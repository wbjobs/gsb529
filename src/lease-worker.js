import {
  WAITER_TTL_MS,
  acquire,
  cancelWaiter,
  createResource,
  findSelfRequest,
  makeClock,
  reconcile,
  releaseOwner,
  renewOwner,
  touchWaiter,
} from './lock-engine.js';
import { createClockEstimator } from './clock-sync.js';

const DB_NAME = 'multi-tab-lease-locks';
const DB_VERSION = 2;
const STORE_NAME = 'resources';
const RESOURCE_ID = 'shared-document';
const CHANNEL_NAME = 'multi-tab-lease-lock-v1';
const TICK_MS = 200;
const HEARTBEAT_MS = 700;
const CLOCK_PING_MS = 2_500;
const PEER_TIMEOUT_MS = 4_000;

const tabId =
  globalThis.crypto?.randomUUID?.() ||
  `tab-${Date.now()}-${Math.random().toString(16).slice(2)}`;

let clockSkewMs = Math.round((Math.random() * 2 - 1) * 800);
let zombie = false;
let failNextRenewal = false;
let autoRenew = true;
let leaseTtlMs = 8_000;
let state = null;
let activeRequest = null;
let lastHeartbeatAt = 0;
let lastClockPingAt = 0;
const events = [];
const peers = new Map();

const logicalNow = () => Date.now() + clockSkewMs;
const perfNow = () => performance.now();
const estimator = createClockEstimator(tabId, logicalNow, perfNow);
const channel = new BroadcastChannel(CHANNEL_NAME);

function runtimeClock() {
  return makeClock(tabId, logicalNow, estimator);
}

function openDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        const store = database.createObjectStore(STORE_NAME, {
          keyPath: 'id',
        });
        store.put(createResource(RESOURCE_ID, logicalNow()));
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

const databaseReady = openDatabase();
let databaseChain = Promise.resolve();

function queueDatabase(job) {
  const result = databaseChain.then(() => job());
  databaseChain = result.catch(() => {});
  return result;
}

function transactionDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error);
  });
}

function storeGet(store, id) {
  return new Promise((resolve, reject) => {
    const request = store.get(id);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function mutateResource(mutator) {
  return queueDatabase(async () => {
    const database = await databaseReady;
    const transaction = database.transaction(STORE_NAME, 'readwrite');
    const store = transaction.objectStore(STORE_NAME);
    const persisted =
      (await storeGet(store, RESOURCE_ID)) ||
      createResource(RESOURCE_ID, logicalNow());
    const working = structuredClone(persisted);
    const result = await mutator(working);

    if (result.changed) {
      working.revision += 1;
      store.put(structuredClone(working));
    }

    await transactionDone(transaction);
    state = structuredClone(working);
    return result;
  });
}

function readResource() {
  return queueDatabase(async () => {
    const database = await databaseReady;
    const transaction = database.transaction(STORE_NAME, 'readonly');
    const store = transaction.objectStore(STORE_NAME);
    const resource =
      (await storeGet(store, RESOURCE_ID)) ||
      createResource(RESOURCE_ID, logicalNow());
    await transactionDone(transaction);
    return structuredClone(resource);
  });
}

function postMessageToPage(message) {
  postMessage(message);
}

function sendOverChannel(message) {
  if (zombie) {
    return;
  }
  channel.postMessage({
    ...message,
    tabId,
    logicalAt: logicalNow(),
    perfAt: perfNow(),
    zombie,
  });
}

function addEvent(type, message, detail = {}) {
  events.unshift({
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    at: logicalNow(),
    type,
    message,
    detail,
  });
  events.length = Math.min(events.length, 40);
}

function describeEngineEvent(event) {
  if (event.type === 'owner-expired') {
    return {
      type: 'lease-expired',
      message: `租约超时，持有者 ${event.owner.tabId.slice(0, 8)} 的锁已自动释放`,
    };
  }
  if (event.type === 'lock-acquired') {
    return { type: 'lock-acquired', message: '立即获得锁' };
  }
  if (event.type === 'lock-acquired-after-wait') {
    return {
      type: 'lock-acquired',
      message: `等待 ${Math.round(event.waitedMs)} ms 后按 FIFO 获得锁`,
    };
  }
  if (event.type === 'owner-released') {
    return { type: 'lock-released', message: '持有者主动释放锁' };
  }
  if (event.type === 'waiter-queued') {
    return { type: 'waiting', message: `进入等待队列，位置 ${event.position}` };
  }
  if (event.type === 'waiter-canceled') {
    return { type: 'waiting-canceled', message: '已取消等待' };
  }
  if (event.type === 'lease-renewed') {
    return { type: 'lease-renewed', message: '租约续约成功' };
  }
  return null;
}

function addEngineEvents(engineEvents) {
  for (const engineEvent of engineEvents || []) {
    const described = describeEngineEvent(engineEvent);
    if (described) {
      addEvent(described.type, described.message, engineEvent);
    }
  }
}

function rememberPeer(message) {
  if (!message.tabId || message.tabId === tabId) {
    return;
  }
  peers.set(message.tabId, {
    tabId: message.tabId,
    lastPerfAt: perfNow(),
    logicalAt: message.logicalAt,
    zombie: Boolean(message.zombie),
    revision: message.revision ?? null,
  });
}

channel.onmessage = async (event) => {
  const message = event.data;
  if (!message || message.tabId === tabId) {
    return;
  }

  const wasKnown = peers.has(message.tabId);
  rememberPeer(message);

  if (message.kind === 'hello') {
    if (!wasKnown) {
      sendOverChannel({ kind: 'hello' });
    }
  } else if (message.kind === 'clock-ping') {
    sendOverChannel({
      kind: 'clock-pong',
      target: message.tabId,
      t1: message.logicalAt,
      p1: message.perfAt,
      t2: logicalNow(),
      p2: perfNow(),
    });
  } else if (message.kind === 'clock-pong' && message.target === tabId) {
    estimator.recordPing(
      message.tabId,
      message.t1,
      message.p1,
      message.t2,
      message.p2,
      logicalNow(),
      perfNow(),
    );
  } else if (message.kind === 'heartbeat') {
    if (!state || message.revision > state.revision) {
      await refreshState({ remote: true });
    }
  } else if (message.kind === 'goodbye') {
    peers.delete(message.tabId);
    await refreshState({ remote: true });
  } else if (message.kind === 'state-changed') {
    await refreshState({ remote: true });
  }
};

async function refreshState(options = {}) {
  const previousSelf = state ? findSelfRequest(state, tabId) : null;
  const loaded = await readResource();
  const nextSelf = findSelfRequest(loaded, tabId);
  state = loaded;

  if (options.remote) {
    if (
      previousSelf?.kind === 'owner' &&
      nextSelf?.kind !== 'owner'
    ) {
      activeRequest = null;
      addEvent(
        'lock-fenced',
        '本地持有者状态被更高版本覆盖，已停止写入并退出临界区',
      );
    } else if (
      previousSelf?.kind === 'waiter' &&
      nextSelf?.kind === 'owner'
    ) {
      activeRequest = {
        kind: 'owner',
        requestId: nextSelf.request.requestId,
        fencingToken: nextSelf.request.fencingToken,
        trueDeadline: perfNow() + leaseTtlMs,
      };
      addEvent('lock-acquired', '远端事务确认本队列首部获得锁');
    } else if (previousSelf?.kind === 'waiter' && !nextSelf) {
      activeRequest = null;
    }
  }

  syncActiveRequest(nextSelf);
  emitSnapshot();
}

function syncActiveRequest(selfRequest) {
  if (!activeRequest || selfRequest?.kind !== activeRequest.kind) {
    return;
  }

  activeRequest.requestId = selfRequest.request.requestId;
  if (activeRequest.kind === 'owner') {
    activeRequest.fencingToken = selfRequest.request.fencingToken;
  }
}

function broadcastStateChange() {
  sendOverChannel({
    kind: 'state-changed',
    revision: state?.revision ?? 0,
    fencingToken: state?.owner?.fencingToken ?? null,
  });
}

function addEngineEventsAndFinish(result) {
  addEngineEvents(result.events);
  broadcastStateChange();
  emitSnapshot();
}

function localRequestIdentity() {
  if (!activeRequest) {
    return null;
  }
  const identity = {
    tabId,
    requestId: activeRequest.requestId,
  };
  if (activeRequest.kind === 'owner') {
    identity.fencingToken = activeRequest.fencingToken;
  }
  return identity;
}

async function handleAcquire() {
  if (activeRequest) {
    return;
  }

  const requestId = crypto.randomUUID();
  const result = await mutateResource((working) => {
    const acquired = acquire(
      working,
      {
        tabId,
        requestId,
        ttl: leaseTtlMs,
        waiterTtl: WAITER_TTL_MS,
      },
      runtimeClock(),
    );
    return acquired;
  });

  activeRequest =
    result.outcome === 'granted'
      ? {
          kind: 'owner',
          requestId,
          fencingToken: result.owner.fencingToken,
          trueDeadline: perfNow() + leaseTtlMs,
        }
      : {
          kind: 'waiter',
          requestId,
        };
  addEngineEventsAndFinish(result);
}

async function handleCancel() {
  if (activeRequest?.kind !== 'waiter') {
    return;
  }
  const identity = localRequestIdentity();
  activeRequest = null;
  const result = await mutateResource((working) =>
    cancelWaiter(working, identity, logicalNow()),
  );
  addEngineEventsAndFinish(result);
}

async function handleRelease() {
  if (activeRequest?.kind !== 'owner') {
    return;
  }
  const identity = localRequestIdentity();
  activeRequest = null;
  const result = await mutateResource((working) =>
    releaseOwner(working, identity, logicalNow()),
  );
  addEngineEventsAndFinish(result);
}

async function renewCurrentOwner({ force = false } = {}) {
  if (activeRequest?.kind !== 'owner') {
    return;
  }

  const remaining = activeRequest.trueDeadline - perfNow();
  if (remaining <= 0) {
    const identity = localRequestIdentity();
    activeRequest = null;
    const result = await mutateResource((working) =>
      releaseOwner(working, identity, logicalNow()),
    );
    addEvent('lease-expired', '真实经过时间达到租约上限，锁已自动释放');
    addEngineEvents(result.events.filter((event) => event.type !== 'owner-released'));
    broadcastStateChange();
    emitSnapshot();
    return;
  }

  const shouldRenew =
    force || (autoRenew && remaining <= (leaseTtlMs * 2) / 3);
  if (!shouldRenew || remaining <= 0) {
    return;
  }

  if (failNextRenewal) {
    failNextRenewal = false;
    addEvent('renew-failed', '模拟续约失败：本次 IndexedDB 条件续约被跳过，将自动重试');
    emitSnapshot();
    return;
  }

  const identity = localRequestIdentity();
  const result = await mutateResource((working) =>
    renewOwner(working, identity, logicalNow(), leaseTtlMs),
  );

  if (result.changed) {
    activeRequest.fencingToken = result.owner.fencingToken;
    activeRequest.ttl = leaseTtlMs;
    activeRequest.trueDeadline = perfNow() + leaseTtlMs;
    addEngineEventsAndFinish(result);
  } else {
    activeRequest = null;
    addEvent('renew-rejected', '续约被拒绝：本地持有者已失去锁或栅栏令牌不匹配');
    emitSnapshot();
  }
}

async function tick() {
  if (zombie || !state) {
    return;
  }

  if (activeRequest?.kind === 'owner') {
    await renewCurrentOwner();
    return;
  }

  const identity = localRequestIdentity();
  const result = await mutateResource((working) => {
    const reconciliation = reconcile(working, runtimeClock());
    let changed = reconciliation.changed;
    const events = [...reconciliation.events];

    if (activeRequest?.kind === 'waiter') {
      const waiter = working.waiters.find(
        (item) =>
          item.tabId === tabId &&
          item.requestId === activeRequest.requestId,
      );
      if (
        waiter &&
        waiter.expiresAt - logicalNow() < WAITER_TTL_MS - 1_000
      ) {
        const touched = touchWaiter(
          working,
          identity,
          logicalNow(),
          WAITER_TTL_MS,
        );
        changed = changed || touched.changed;
        events.push(...(touched.events || []));
      }

      if (!working.owner) {
        const promoted = acquire(
          working,
          {
            tabId,
            requestId: activeRequest.requestId,
            ttl: leaseTtlMs,
            waiterTtl: WAITER_TTL_MS,
          },
          runtimeClock(),
        );
        changed = changed || promoted.changed;
        events.push(...(promoted.events || []));
        return { ...promoted, changed, events };
      }
    }

    return { changed, events };
  });

  if (result.outcome === 'granted') {
    activeRequest = {
      kind: 'owner',
      requestId: identity.requestId,
      fencingToken: result.owner.fencingToken,
      ttl: leaseTtlMs,
      trueDeadline: perfNow() + leaseTtlMs,
    };
  } else if (
    activeRequest?.kind === 'waiter' &&
    !state.waiters.some((waiter) => waiter.tabId === tabId)
  ) {
    activeRequest = null;
  }

  if (result.events?.some((event) => event.type === 'owner-expired')) {
    const expiredSelf = result.events.some(
      (event) => event.type === 'owner-expired' && event.owner.tabId === tabId,
    );
    if (expiredSelf) {
      activeRequest = null;
    }
  }

  if (result.changed) {
    addEngineEvents(result.events);
    broadcastStateChange();
  }
  emitSnapshot();
}

async function writeResource(payload) {
  if (activeRequest?.kind !== 'owner') {
    addEvent('write-rejected', '没有有效租约，写入被拒绝');
    emitSnapshot();
    return;
  }

  const identity = localRequestIdentity();
  const result = await mutateResource((working) => {
    if (
      !working.owner ||
      working.owner.tabId !== identity.tabId ||
      working.owner.requestId !== identity.requestId ||
      working.owner.fencingToken !== identity.fencingToken
    ) {
      return { changed: false, events: [] };
    }
    working.payload = {
      value: String(payload),
      fencingToken: identity.fencingToken,
      writtenAt: logicalNow(),
      writerTabId: tabId,
    };
    return { changed: true, events: [] };
  });
  if (!result.changed) {
    activeRequest = null;
    addEvent('write-rejected', '栅栏令牌已失效，临界区写入被拒绝');
  } else {
    addEvent('resource-written', `携带栅栏令牌 ${identity.fencingToken} 写入共享资源`);
    broadcastStateChange();
  }
  emitSnapshot();
}

async function handleShutdown() {
  if (activeRequest?.kind === 'owner') {
    const identity = localRequestIdentity();
    await mutateResource((working) =>
      releaseOwner(working, identity, logicalNow()),
    );
  } else if (activeRequest?.kind === 'waiter') {
    const identity = localRequestIdentity();
    await mutateResource((working) =>
      cancelWaiter(working, identity, logicalNow()),
    );
  }
  activeRequest = null;
  sendOverChannel({ kind: 'goodbye' });
}

onmessage = async (messageEvent) => {
  const { command, value } = messageEvent.data || {};

  try {
    switch (command) {
      case 'acquire':
        await handleAcquire();
        break;
      case 'cancel':
        await handleCancel();
        break;
      case 'release':
        await handleRelease();
        break;
      case 'renew-now':
        await renewCurrentOwner({ force: true });
        break;
      case 'write':
        await writeResource(value);
        break;
      case 'set-auto-renew':
        autoRenew = Boolean(value);
        break;
      case 'set-ttl':
        if (!activeRequest) {
          leaseTtlMs = Number(value);
        }
        break;
      case 'set-skew':
        if (!activeRequest) {
          clockSkewMs = Number(value);
        }
        break;
      case 'set-zombie':
        zombie = Boolean(value);
        if (zombie) {
          addEvent('zombie', '模拟标签页冻结：停止心跳、续约和状态广播');
        } else {
          addEvent('zombie-recovered', '标签页恢复：立即同步并尝试续约');
          sendOverChannel({ kind: 'hello' });
          sendClockPings();
          if (activeRequest?.kind === 'owner') {
            await refreshState({ remote: false });
            await renewCurrentOwner({ force: true });
          }
        }
        break;
      case 'fail-next-renewal':
        failNextRenewal = true;
        addEvent('renew-failure-armed', '下一次续约将模拟失败，随后自动重试');
        break;
      case 'shutdown':
        await handleShutdown();
        break;
      default:
        break;
    }
    emitSnapshot();
  } catch (error) {
    addEvent('worker-error', error?.message || String(error));
    emitSnapshot();
  }
};

function buildLeaseView(owner) {
  if (!owner) {
    return null;
  }

  if (owner.tabId === tabId && activeRequest?.kind === 'owner') {
    const remaining = Math.max(0, activeRequest.trueDeadline - perfNow());
    return {
      ...owner,
      local: true,
      remainingMs: remaining,
      safeRemainingMs: remaining,
      progress: Math.min(1, Math.max(0, remaining / leaseTtlMs)),
      clock: { offset: clockSkewMs, uncertainty: 0, alive: true },
    };
  }

  const clockEstimate = estimator.estimateFor(owner.tabId);
  const remoteNow = clockEstimate.now;
  const safeRemaining = owner.expiresAt - (remoteNow + clockEstimate.uncertainty);
  return {
    ...owner,
    local: false,
    remainingMs: Math.max(0, owner.expiresAt - remoteNow),
    safeRemainingMs: Math.max(0, safeRemaining),
    progress: Math.min(1, Math.max(0, (owner.expiresAt - remoteNow) / owner.ttl)),
    clock: clockEstimate,
  };
}

function buildSnapshot() {
  const self = state ? findSelfRequest(state, tabId) : null;

  for (const [peerId, peer] of peers) {
    if (perfNow() - peer.lastPerfAt > PEER_TIMEOUT_MS) {
      peer.alive = false;
    }
  }

  const queue = (state?.waiters || []).map((waiter, index) => {
    const estimate = estimator.estimateFor(waiter.tabId);
    const safeRemaining =
      waiter.expiresAt - (estimate.now + estimate.uncertainty);
    return {
      ...waiter,
      position: index + 1,
      isSelf: waiter.tabId === tabId,
      remainingMs: Math.max(0, waiter.expiresAt - estimate.now),
      safeRemainingMs: Math.max(0, safeRemaining),
      alive: waiter.tabId === tabId || estimate.alive,
      clock: estimate,
    };
  });

  const peerList = Array.from(peers.values()).map((peer) => {
    const estimate = estimator.estimateFor(peer.tabId);
    return {
      tabId: peer.tabId,
      shortId: peer.tabId.slice(0, 8),
      alive: perfNow() - peer.lastPerfAt <= PEER_TIMEOUT_MS,
      zombie: peer.zombie,
      ageMs: perfNow() - peer.lastPerfAt,
      offsetMs: estimate.offset,
      uncertaintyMs: estimate.uncertainty,
      rttMs: estimate.rtt,
    };
  });

  return {
    tabId,
    shortTabId: tabId.slice(0, 8),
    now: localNow,
    revision: state?.revision ?? 0,
    role: self?.kind || 'idle',
    activeRequest: activeRequest
      ? {
          ...activeRequest,
          remainingMs:
            activeRequest.kind === 'owner'
              ? Math.max(0, activeRequest.trueDeadline - perfNow())
              : null,
        }
      : null,
    owner: buildLeaseView(state?.owner),
    queue,
    peers: [
      {
        tabId,
        shortId: tabId.slice(0, 8),
        alive: !zombie,
        zombie,
        isSelf: true,
        offsetMs: clockSkewMs,
        uncertaintyMs: 0,
        rttMs: 0,
      },
      ...peerList,
    ],
    payload: state?.payload || null,
    config: {
      ttlMs: leaseTtlMs,
      autoRenew,
      clockSkewMs,
      failNextRenewal,
      zombie,
    },
    events: events.slice(0, 20),
  };
}

function emitSnapshot() {
  postMessageToPage({ kind: 'snapshot', snapshot: buildSnapshot() });
}

function sendHeartbeat(force = false) {
  if (zombie) {
    return;
  }
  const now = perfNow();
  if (!force && now - lastHeartbeatAt < HEARTBEAT_MS) {
    return;
  }
  lastHeartbeatAt = now;
  sendOverChannel({
    kind: 'heartbeat',
    revision: state?.revision ?? 0,
  });
}

function sendClockPings() {
  if (zombie || perfNow() - lastClockPingAt < CLOCK_PING_MS) {
    return;
  }
  lastClockPingAt = perfNow();
  sendOverChannel({ kind: 'clock-ping' });
}

setInterval(() => {
  tick().catch((error) => {
    addEvent('tick-error', error?.message || String(error));
  });
}, TICK_MS);

setInterval(() => {
  sendHeartbeat();
  sendClockPings();
  emitSnapshot();
}, 500);

queueDatabase(async () => {
  await databaseReady;
  state = await readResource();
  sendOverChannel({ kind: 'hello' });
  sendHeartbeat(true);
  sendClockPings();
  emitSnapshot();
});
