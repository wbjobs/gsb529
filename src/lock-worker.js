import {
  PEER_TIMEOUT_MS,
  UNKNOWN_CLOCK_DRIFT,
  leaseView,
  planGrant,
  planRelease,
  planRenew,
} from './engine.js';

const DB_NAME = 'lease-lock-db';
const DB_VERSION = 1;
const RESOURCE_STORE = 'resources';
const REQUEST_STORE = 'requests';
const SNAPSHOT_INTERVAL_MS = 250;
const PRESENCE_INTERVAL_MS = 500;
const TIME_SYNC_INTERVAL_MS = 2000;
const MIN_LEASE_MS = 1000;
const MAX_LEASE_MS = 60_000;

const state = {
  db: null,
  clientId: null,
  clientName: null,
  resource: 'orders/42',
  clockBiasMs: 0,
  peers: new Map(),
  localRequest: null,
  autoRenew: true,
  autoRenewTimer: null,
  maintainTimer: null,
  snapshotTimer: null,
  presenceTimer: null,
  timeSyncTimer: null,
};

const channel = new BroadcastChannel('fair-lease-lock-v1');

const nowWall = () => Date.now() + state.clockBiasMs;

function openDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(RESOURCE_STORE)) {
        db.createObjectStore(RESOURCE_STORE, { keyPath: 'resource' });
      }
      if (!db.objectStoreNames.contains(REQUEST_STORE)) {
        const store = db.createObjectStore(REQUEST_STORE, { keyPath: 'seq', autoIncrement: true });
        store.createIndex('resourceStatus', ['resource', 'status']);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function transaction(storeNames, mode, callback) {
  return new Promise((resolve, reject) => {
    const tx = state.db.transaction(storeNames, mode);
    let result;
    tx.oncomplete = () => resolve(result);
    tx.onabort = () => reject(tx.error || new Error('IndexedDB transaction aborted'));
    tx.onerror = () => reject(tx.error || new Error('IndexedDB transaction failed'));
    Promise.resolve(callback(tx)).then((value) => {
      result = value;
    }).catch(reject);
  });
}

function requestStore(tx) {
  return tx.objectStore(REQUEST_STORE);
}

function resourceStore(tx) {
  return tx.objectStore(RESOURCE_STORE);
}

function idbRequest(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function getAll(tx, storeName) {
  return idbRequest(tx.objectStore(storeName).getAll());
}

function getResource(tx, resourceName) {
  return idbRequest(resourceStore(tx).get(resourceName));
}

function getRequestsForResource(tx, resourceName) {
  return idbRequest(requestStore(tx).index('resourceStatus').getAll([resourceName, 'waiting']));
}

function deleteRequest(tx, seq) {
  if (!Number.isFinite(seq)) return;
  requestStore(tx).delete(seq);
}

function selfClockPeer() {
  return {
    clientId: state.clientId,
    clientName: state.clientName,
    offsetMs: 0,
    rttMs: 0,
    lastSeenAgeMs: 0,
    lastSeenWallMs: nowWall(),
  };
}

function effectivePeers(now = nowWall()) {
  const peers = Object.fromEntries(state.peers.entries());
  peers[state.clientId] = selfClockPeer();
  for (const peer of Object.values(peers)) {
    peer.lastSeenAgeMs = now - peer.lastSeenWallMs;
  }
  return peers;
}

function upsertPeer(message) {
  const clientId = message.senderId || message.clientId;
  if (!clientId || clientId === state.clientId) return;
  const current = state.peers.get(clientId) || {
    clientId,
    samples: [],
    offsetMs: 0,
    rttMs: UNKNOWN_CLOCK_DRIFT,
  };

  current.clientName = message.clientName || current.clientName || clientId;
  if (Number.isFinite(message.offsetMs)) current.offsetMs = message.offsetMs;
  if (Number.isFinite(message.rttMs)) current.rttMs = message.rttMs;
  current.lastSeenWallMs = nowWall() - (message.ageMs || 0);
  state.peers.set(clientId, current);
}

function send(message) {
  channel.postMessage({
    ...message,
    senderId: state.clientId,
    clientName: state.clientName,
    sentWallMs: nowWall(),
    ageMs: 0,
  });
}

function sendPresence() {
  send({ type: 'presence' });
}

function sendTimePing() {
  send({ type: 'time-ping', t1WallMs: nowWall() });
}

function applyTimeReply(message) {
  if (message.targetId !== state.clientId) return;
  const t4 = nowWall();
  const rtt = t4 - message.t1WallMs;
  if (!Number.isFinite(rtt) || rtt < 0) return;

  const peer = state.peers.get(message.senderId);
  if (!peer) return;

  const offset = message.t2WallMs - (message.t1WallMs + t4) / 2;
  peer.samples.push({ offset, rtt });
  peer.samples = peer.samples.slice(-5);
  const sorted = [...peer.samples].sort((a, b) => a.offset - b.offset);
  peer.offsetMs = sorted[Math.floor(sorted.length / 2)].offset;
  peer.rttMs = Math.min(...peer.samples.map((sample) => sample.rtt));
  state.peers.set(message.senderId, peer);
}

function applyPlanToTransaction(tx, plan) {
  for (const seq of plan.purgeSeqs) {
    deleteRequest(tx, seq);
  }

  if (plan.releaseHolder || plan.grant) {
    deleteRequest(tx, plan.nextResource.holderRequestSeq);
    resourceStore(tx).put(plan.nextResource);
    return plan.nextResource;
  }

  if (plan.changed && !plan.grant) {
    resourceStore(tx).put(plan.nextResource);
  }
  return null;
}

function post(type, payload = {}) {
  self.postMessage({ type, ...payload });
}

function postEvent(level, message, details = {}) {
  post('event', { event: { id: crypto.randomUUID(), at: Date.now(), level, message, details } });
}

function scheduleMaintain(delay = 0) {
  clearTimeout(state.maintainTimer);
  state.maintainTimer = setTimeout(() => {
    maintain().catch((error) => {
      postEvent('error', '维护周期失败', { error: String(error.message || error) });
    });
  }, Math.max(0, delay));
}

function notifyGrant(grantedResource, request) {
  const isLocal = request.clientId === state.clientId;
  if (isLocal) {
    state.localRequest = {
      ...request,
      status: 'held',
      grantedResource: grantedResource,
    };
    clearTimeout(state.autoRenewTimer);
    if (state.autoRenew && grantedResource.leaseMs >= MIN_LEASE_MS) {
      state.autoRenewTimer = setTimeout(() => {
        renew().catch((error) => postEvent('error', '自动续约失败', { error: String(error.message || error) }));
      }, Math.max(MIN_LEASE_MS / 2, grantedResource.leaseMs * 0.4));
    }
    post('acquired', { request: state.localRequest, resource: grantedResource });
    postEvent('info', '获得锁', {
      resource: request.resource,
      leaseMs: request.leaseMs,
      fencingToken: grantedResource.fencingToken,
    });
  } else {
    postEvent('info', '队首获得锁', {
      resource: request.resource,
      clientId: request.clientId,
      clientName: request.clientName,
      fencingToken: grantedResource.fencingToken,
    });
  }
}

async function maintain(resourceName = state.localRequest?.resource) {
  if (!resourceName) return;

  const currentNow = nowWall();
  let previousHolderSeq = null;
  let granted = null;
  let released = false;
  const purged = [];

  await transaction([RESOURCE_STORE, REQUEST_STORE], 'readwrite', (tx) =>
    Promise.all([getResource(tx, resourceName), getRequestsForResource(tx, resourceName)])
      .then(([resource, waiting]) => {
        previousHolderSeq = resource?.holderRequestSeq || null;
        const plan = planGrant({
          resource,
          waitingRequests: waiting,
          nowWallMs: currentNow,
          peers: effectivePeers(currentNow),
        });

        purged.push(...plan.purgeSeqs);
        released = plan.releaseHolder;
        granted = plan.grant ? { resource: plan.nextResource, request: plan.grant } : null;
        applyPlanToTransaction(tx, plan);
      })
  );

  if (purged.length || released || granted) {
    send({ type: 'state-changed', resource: resourceName });
  }

  for (const seq of purged) {
    if (state.localRequest?.seq === seq) {
      state.localRequest = null;
      clearTimeout(state.autoRenewTimer);
      const reason = released && seq === previousHolderSeq ? 'lease-expired' : 'waiter-timeout';
      post('request-ended', { seq, reason });
      postEvent('warn', reason === 'lease-expired' ? '租约超时，锁自动释放' : '等待超时，退出队列', { resource: resourceName });
    }
  }

  if (granted) {
    notifyGrant(granted.resource, granted.request);
  }

  scheduleNextMaintain(resourceName);
}

function scheduleNextMaintain(resourceName) {
  let delay = state.localRequest ? 250 : 1000;
  if (state.localRequest?.status === 'held') {
    const remainingMs = state.localRequest.grantedResource.expiresWallAt - nowWall();
    delay = Math.max(100, remainingMs + 25);
  } else if (state.localRequest?.status === 'waiting') {
    delay = 250;
  }
  scheduleMaintain(Math.min(5000, delay));
}

async function acquire({ resource, leaseMs, waitTimeoutMs }) {
  if (state.localRequest) {
    post('acquire-error', { error: '当前标签已有活动请求，请先释放或取消' });
    return;
  }

  const normalizedLeaseMs = Math.min(MAX_LEASE_MS, Math.max(MIN_LEASE_MS, Number(leaseMs) || 8000));
  const normalizedWaitMs = waitTimeoutMs === null || waitTimeoutMs === undefined || Number.isNaN(Number(waitTimeoutMs))
    ? null
    : Math.max(0, Number(waitTimeoutMs));
  const currentNow = nowWall();
  const request = {
    resource,
    clientId: state.clientId,
    clientName: state.clientName,
    status: 'waiting',
    leaseMs: normalizedLeaseMs,
    enqueuedWallAt: currentNow,
    aliveUntilWallMs: currentNow + PRESENCE_INTERVAL_MS * 2.2,
    waitDeadlineWallMs: normalizedWaitMs === null ? null : currentNow + normalizedWaitMs,
    nowWallMs: currentNow,
  };

  let seq;
  let grantResult = null;

  await transaction([RESOURCE_STORE, REQUEST_STORE], 'readwrite', async (tx) => {
    seq = await idbRequest(requestStore(tx).add(request));
    const [existingResource, waiting] = await Promise.all([
      getResource(tx, resource),
      getRequestsForResource(tx, resource),
    ]);
    const added = waiting.find((item) => item.seq === seq);
    if (!added) throw new Error('新请求未提交成功');

    const plan = planGrant({
      resource: existingResource,
      waitingRequests: waiting,
      nowWallMs: currentNow,
      peers: effectivePeers(currentNow),
    });
    applyPlanToTransaction(tx, plan);
    if (plan.grant?.seq === seq) {
      grantResult = { resource: plan.nextResource, request: plan.grant };
    }
  });

  state.localRequest = { ...request, seq };
  post('queued', { request: state.localRequest });
  send({ type: 'state-changed', resource });

  if (grantResult) {
    notifyGrant(grantResult.resource, grantResult.request);
  } else {
    postEvent('info', '进入 FIFO 等待队列', { resource, seq, waitTimeoutMs: normalizedWaitMs });
  }

  scheduleMaintain(0);
}

async function release() {
  const local = state.localRequest;
  if (!local) return;

  let response = { ok: false, reason: 'not-owner' };
  if (local.status === 'held') {
    await transaction([RESOURCE_STORE, REQUEST_STORE], 'readwrite', (tx) =>
      getResource(tx, local.resource).then((resource) => {
        const plan = planRelease(resource, local.seq);
        response = plan;
        if (plan.ok) {
          resourceStore(tx).put(plan.resource);
          deleteRequest(tx, local.seq);
        }
      })
    );
  } else {
    await transaction(REQUEST_STORE, 'readwrite', (tx) => {
      deleteRequest(tx, local.seq);
      response = { ok: true, canceled: true };
    });
  }

  if (response.ok) {
    clearTimeout(state.autoRenewTimer);
    state.localRequest = null;
    send({ type: 'state-changed', resource: local.resource });
    post('released', { seq: local.seq, canceled: Boolean(response.canceled) });
    postEvent('info', response.canceled ? '已取消等待' : '已释放锁', { resource: local.resource });
    scheduleMaintain(0);
  } else {
    post('release-error', { error: response.reason });
  }
}

async function renew() {
  const local = state.localRequest;
  if (!local || local.status !== 'held') {
    post('renew-error', { error: '当前未持有锁' });
    return;
  }

  let result;
  await transaction(RESOURCE_STORE, 'readwrite', (tx) =>
    getResource(tx, local.resource).then((resource) => {
      const estimatedNow = nowWall();
      result = planRenew(resource, local.seq, estimatedNow);
      if (result.ok) resourceStore(tx).put(result.resource);
    })
  );

  if (result.ok) {
    state.localRequest = { ...state.localRequest, grantedResource: result.resource };
    send({ type: 'state-changed', resource: local.resource });
    post('renewed', { resource: result.resource });
    postEvent('info', '续约成功', {
      resource: local.resource,
      renewCount: result.resource.renewCount,
      expiresWallAt: result.resource.expiresWallAt,
    });

    clearTimeout(state.autoRenewTimer);
    if (state.autoRenew) {
      state.autoRenewTimer = setTimeout(() => {
        renew().catch((error) => postEvent('error', '自动续约失败', { error: String(error.message || error) }));
      }, Math.max(MIN_LEASE_MS / 2, result.resource.leaseMs * 0.4));
    }
  } else {
    clearTimeout(state.autoRenewTimer);
    state.localRequest = null;
    send({ type: 'state-changed', resource: local.resource });
    post('renew-error', { error: result.reason });
    postEvent('warn', '续约失败，停止访问资源', { resource: local.resource, reason: result.reason });
    scheduleMaintain(0);
  }
}

async function touchWaitingLease() {
  const local = state.localRequest;
  if (!local || local.status !== 'waiting') return;

  await transaction(REQUEST_STORE, 'readwrite', async (tx) => {
    const record = await idbRequest(requestStore(tx).get(local.seq));
    if (!record || record.clientId !== state.clientId) return;
    record.aliveUntilWallMs = nowWall() + PRESENCE_INTERVAL_MS * 2.2;
    requestStore(tx).put(record);
  });
}

async function readSnapshot() {
  return transaction([RESOURCE_STORE, REQUEST_STORE], 'readonly', async (tx) => {
    const [resources, requests] = await Promise.all([
      getAll(tx, RESOURCE_STORE),
      getAll(tx, REQUEST_STORE),
    ]);
    const currentNow = nowWall();
    const peers = effectivePeers(currentNow);
    const resource = resources.find((item) => item.resource === state.resource) || { resource: state.resource };
    const holderPeer = resource.holderClientId ? peers[resource.holderClientId] || null : null;
    const waitingRequests = requests
      .filter((item) => item.resource === state.resource && item.status === 'waiting')
      .sort((a, b) => a.seq - b.seq);

    return {
      resource,
      lease: leaseView(resource, holderPeer, currentNow),
      waitingRequests,
      peers: Object.values(peers).map((peer) => ({
        clientId: peer.clientId,
        clientName: peer.clientName,
        offsetMs: peer.offsetMs,
        rttMs: peer.rttMs,
        lastSeenAgeMs: peer.lastSeenAgeMs,
        online: peer.lastSeenAgeMs <= PEER_TIMEOUT_MS,
      })),
      localRequest: state.localRequest,
      nowWallMs: currentNow,
      realMs: Date.now(),
      generatedAt: new Date().toISOString(),
    };
  });
}

async function publishSnapshot() {
  try {
    const snapshot = await readSnapshot();
    post('snapshot', { snapshot });
  } catch (error) {
    postEvent('error', '读取可视化状态失败', { error: String(error.message || error) });
  }
}

function onChannelMessage(event) {
  const message = event.data;
  if (!message || message.senderId === state.clientId) return;

  if (message.type === 'hello' || message.type === 'presence') {
    upsertPeer(message);
    if (message.type === 'hello') {
      sendPresence();
      sendTimePing();
    }
    if (message.resource && state.localRequest?.resource === message.resource) {
      scheduleMaintain(0);
    }
    return;
  }

  if (message.type === 'time-ping') {
    upsertPeer(message);
    send({
      type: 'time-reply',
      targetId: message.senderId,
      t1WallMs: message.t1WallMs,
      t2WallMs: nowWall(),
    });
    return;
  }

  if (message.type === 'time-reply') {
    upsertPeer({ ...message, ageMs: 0 });
    applyTimeReply(message);
    return;
  }

  if (message.type === 'state-changed' && message.resource === state.resource) {
    scheduleMaintain(0);
  }
}

async function initialize(message) {
  state.clientId = message.clientId;
  state.clientName = message.clientName || state.clientId.slice(0, 8);
  state.resource = message.resource || state.resource;
  state.clockBiasMs = Number(message.clockBiasMs) || 0;
  state.autoRenew = message.autoRenew !== false;
  state.db = await openDatabase();

  channel.addEventListener('message', onChannelMessage);
  send({ type: 'hello', resource: state.resource });
  sendTimePing();

  state.presenceTimer = setInterval(() => {
    sendPresence();
    touchWaitingLease().catch((error) => {
      postEvent('error', '等待心跳失败', { error: String(error.message || error) });
    });
  }, PRESENCE_INTERVAL_MS);
  state.timeSyncTimer = setInterval(sendTimePing, TIME_SYNC_INTERVAL_MS);
  state.snapshotTimer = setInterval(() => publishSnapshot(), SNAPSHOT_INTERVAL_MS);

  post('ready', { clientId: state.clientId, resource: state.resource });
  await publishSnapshot();
  await maintain(state.resource);
}

self.onmessage = async (event) => {
  const message = event.data || {};

  try {
    if (message.type === 'init') {
      await initialize(message);
      return;
    }

    if (!state.db) {
      throw new Error('Worker 尚未初始化');
    }

    switch (message.type) {
      case 'set-resource':
        if (state.localRequest) {
          post('resource-blocked', { error: '活动请求结束前不能切换资源' });
          break;
        }
        state.resource = message.resource;
        send({ type: 'hello', resource: state.resource });
        await publishSnapshot();
        break;
      case 'set-clock-bias':
        state.clockBiasMs = Number(message.clockBiasMs) || 0;
        await publishSnapshot();
        break;
      case 'set-auto-renew':
        state.autoRenew = Boolean(message.enabled);
        if (!state.autoRenew) clearTimeout(state.autoRenewTimer);
        break;
      case 'acquire':
        await acquire({
          resource: state.resource,
          leaseMs: message.leaseMs,
          waitTimeoutMs: message.waitTimeoutMs,
        });
        break;
      case 'release':
      case 'cancel':
        await release();
        break;
      case 'renew':
        await renew();
        break;
      default:
        break;
    }
  } catch (error) {
    post('fatal', { error: String(error.message || error) });
  }
};
