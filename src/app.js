const STORAGE_KEY = 'fair-lease-lock-tab';

const $ = (id) => document.getElementById(id);
const elements = {
  openTab: $('openTab'),
  lockPanel: $('lockPanel'),
  lockState: $('lockState'),
  lockBadge: $('lockBadge'),
  leaseBar: $('leaseBar'),
  remainingTime: $('remainingTime'),
  uncertainty: $('uncertainty'),
  resourceName: $('resourceName'),
  holderName: $('holderName'),
  fencingToken: $('fencingToken'),
  renewCount: $('renewCount'),
  resourceInput: $('resourceInput'),
  leaseInput: $('leaseInput'),
  waitInput: $('waitInput'),
  clockInput: $('clockInput'),
  autoRenewInput: $('autoRenewInput'),
  acquireBtn: $('acquireBtn'),
  renewBtn: $('renewBtn'),
  releaseBtn: $('releaseBtn'),
  queueCount: $('queueCount'),
  queueList: $('queueList'),
  peerList: $('peerList'),
  eventLog: $('eventLog'),
  clearLog: $('clearLog'),
};

const identity = getIdentity();
let snapshot = null;
let worker;
const params = new URLSearchParams(location.search);
const runnerMode = params.get('runner') === '1';
const runnerChannelName = params.get('channel') || 'fair-lease-runner';
let runnerChannel = null;

function getIdentity() {
  const queryClientId = new URLSearchParams(location.search).get('clientId');
  const existing = sessionStorage.getItem(STORAGE_KEY);
  const queryName = new URLSearchParams(location.search).get('clientName');
  if (queryClientId) {
    const identity = { clientId: queryClientId, clientName: queryName || queryClientId.slice(0, 8) };
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(identity));
    return identity;
  }

  if (existing) {
    const parsed = JSON.parse(existing);
    return queryName ? { ...parsed, clientName: queryName } : parsed;
  }

  const identity = {
    clientId: crypto.randomUUID(),
    clientName: queryName || `标签 ${Math.floor(1000 + Math.random() * 9000)}`,
  };
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(identity));
  return identity;
}

function post(message) {
  worker.postMessage(message);
}

function reportRunner(payload) {
  if (!runnerChannel) return;
  runnerChannel.postMessage({
    ...payload,
    clientId: identity.clientId,
    clientName: identity.clientName,
    at: Date.now(),
  });
}

function formatMs(value) {
  if (!Number.isFinite(value)) return '∞';
  if (value <= 0) return '0 ms';
  if (value < 1000) return `${Math.ceil(value)} ms`;
  return `${(value / 1000).toFixed(1)} s`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    "'": '&#39;',
    '"': '&quot;',
  }[char]));
}

function addEvent(event) {
  const item = document.createElement('li');
  item.className = event.level === 'warn' ? 'warn' : event.level === 'error' ? 'error' : '';
  const time = new Date(event.at).toLocaleTimeString('zh-CN', { hour12: false });
  item.innerHTML = `<time>${time}</time><div>${escapeHtml(event.message)}</div>`;
  elements.eventLog.prepend(item);
  while (elements.eventLog.children.length > 80) {
    elements.eventLog.lastElementChild.remove();
  }
}

function renderLock(snap) {
  const resource = snap.resource;
  const isHeld = Boolean(resource.holderClientId);
  const isLocalHeld = resource.holderClientId === identity.clientId;
  const elapsed = Date.now() - snap.realMs;
  const simulatedNow = snap.nowWallMs + elapsed;
  const remaining = isHeld ? resource.expiresWallAt - simulatedNow : Infinity;
  const progress = isHeld ? Math.max(0, Math.min(100, (remaining / resource.leaseMs) * 100)) : 100;

  elements.lockPanel.classList.toggle('held', isHeld);
  elements.lockPanel.classList.toggle('local-held', isLocalHeld);
  elements.lockState.textContent = isHeld ? (isLocalHeld ? '本标签持有锁' : '其他标签持有锁') : '锁空闲';
  elements.lockBadge.textContent = isHeld ? (isLocalHeld ? 'OWNER' : 'HELD') : 'FREE';
  elements.lockBadge.className = `badge ${isLocalHeld ? 'local' : isHeld ? 'held' : 'free'}`;
  elements.leaseBar.style.width = `${progress}%`;
  elements.remainingTime.textContent = isHeld ? formatMs(remaining) : '∞';
  elements.uncertainty.textContent = isHeld ? `± ${formatMs(snap.lease.uncertaintyMs)}` : '—';
  elements.resourceName.textContent = resource.resource;
  elements.holderName.textContent = resource.holderName || '—';
  elements.fencingToken.textContent = resource.fencingToken || '0';
  elements.renewCount.textContent = resource.renewCount || '0';
}

function renderQueue(snap) {
  const requests = snap.waitingRequests;
  elements.queueCount.textContent = requests.length;
  if (!requests.length) {
    elements.queueList.className = 'queue-list empty';
    elements.queueList.textContent = '暂无等待请求';
    return;
  }

  elements.queueList.className = 'queue-list';
  elements.queueList.replaceChildren(...requests.map((request, index) => {
    const item = document.createElement('div');
    item.className = 'queue-item';
    const isLocal = request.clientId === identity.clientId;
    const deadline = request.waitDeadlineWallMs
      ? `等待截止：${new Date(request.waitDeadlineWallMs).toLocaleTimeString('zh-CN', { hour12: false })}`
      : '无等待超时';
    item.innerHTML = `
      <span class="queue-position">${index + 1}</span>
      <span class="queue-main">
        <strong>${escapeHtml(request.clientName)}${isLocal ? '（本标签）' : ''}</strong>
        <span class="muted">#${request.seq} · 租约 ${formatMs(request.leaseMs)} · ${deadline}</span>
      </span>
    `;
    return item;
  }));
}

function renderPeers(snap) {
  const peers = [...snap.peers].sort((a, b) => Number(a.clientId !== identity.clientId) - Number(b.clientId !== identity.clientId));
  if (!peers.length) {
    elements.peerList.className = 'peer-list empty';
    elements.peerList.textContent = '等待心跳...';
    return;
  }

  elements.peerList.className = 'peer-list';
  elements.peerList.replaceChildren(...peers.map((peer) => {
    const item = document.createElement('div');
    item.className = 'peer-item';
    const label = peer.clientId === identity.clientId ? `${peer.clientName}（本标签）` : peer.clientName;
    const offset = peer.clientId === identity.clientId ? Number(elements.clockInput.value) || 0 : peer.offsetMs;
    item.innerHTML = `
      <span class="peer-main">
        <strong><span class="dot ${peer.online ? 'on' : 'off'}"></span>${escapeHtml(label)}</strong>
        <span class="muted">offset ${Math.round(offset)} ms · RTT ${Math.round(peer.rttMs || 0)} ms · ${peer.online ? `${Math.round(peer.lastSeenAgeMs)} ms ago` : '离线'}</span>
      </span>
    `;
    return item;
  }));
}

function renderControls() {
  const local = snapshot?.localRequest;
  const busy = Boolean(local);
  const held = local?.status === 'held';
  elements.acquireBtn.disabled = busy;
  elements.renewBtn.disabled = !held;
  elements.releaseBtn.disabled = !busy;
  elements.releaseBtn.textContent = held ? '释放锁' : '取消等待';
  elements.resourceInput.disabled = busy;
}

function render() {
  if (!snapshot) return;
  renderLock(snapshot);
  renderQueue(snapshot);
  renderPeers(snapshot);
  renderControls();
}

function startWorker() {
  if (runnerMode) document.body.classList.add('runner-frame');
  worker = new Worker('./src/lock-worker.js', { type: 'module' });
  worker.onmessage = (event) => {
    const message = event.data;
    if (message.type === 'snapshot') {
      snapshot = message.snapshot;
      render();
      reportRunner({ kind: 'snapshot', snapshot: message.snapshot });
      return;
    }
    if (message.type === 'event') {
      addEvent(message.event);
      return;
    }
    if (message.type === 'acquired' || message.type === 'released' || message.type === 'request-ended') {
      reportRunner({
        kind: message.type,
        resource: message.resource,
        request: message.request,
        seq: message.seq,
        reason: message.reason,
      });
    }
    if (message.type === 'fatal') {
      addEvent({ at: Date.now(), level: 'error', message: message.error });
      return;
    }
    if (message.type.endsWith('error') || message.type === 'resource-blocked') {
      addEvent({ at: Date.now(), level: 'error', message: message.error || '操作失败' });
    }
  };

  post({
    type: 'init',
    clientId: identity.clientId,
    clientName: identity.clientName,
    resource: params.get('resource') || elements.resourceInput.value.trim() || 'orders/42',
    clockBiasMs: Number(params.get('clockBias') ?? elements.clockInput.value) || 0,
    autoRenew: params.get('autoRenew') === null ? elements.autoRenewInput.checked : params.get('autoRenew') === '1',
  });
}

function setupRunner() {
  if (!runnerMode) return;
  runnerChannel = new BroadcastChannel(runnerChannelName);
  const resource = params.get('resource') || 'runner/fifo';
  const lease = params.get('lease') || '2500';
  elements.resourceInput.value = resource;
  elements.leaseInput.value = lease;
  elements.autoRenewInput.checked = params.get('autoRenew') === '1';
  runnerChannel.addEventListener('message', (event) => {
    const message = event.data;
    if (message?.kind === 'start' && message.clientId === identity.clientId) {
      post({ type: 'acquire', leaseMs: Number(lease), waitTimeoutMs: null });
    }
  });
  reportRunner({ kind: 'ready' });
}

elements.acquireBtn.addEventListener('click', () => {
  post({
    type: 'acquire',
    leaseMs: Number(elements.leaseInput.value),
    waitTimeoutMs: elements.waitInput.value === '' ? null : Number(elements.waitInput.value),
  });
});

elements.renewBtn.addEventListener('click', () => post({ type: 'renew' }));
elements.releaseBtn.addEventListener('click', () => post({ type: 'release' }));

elements.resourceInput.addEventListener('change', () => {
  const resource = elements.resourceInput.value.trim();
  if (resource) post({ type: 'set-resource', resource });
});

elements.clockInput.addEventListener('change', () => {
  post({ type: 'set-clock-bias', clockBiasMs: Number(elements.clockInput.value) || 0 });
});

elements.autoRenewInput.addEventListener('change', () => {
  post({ type: 'set-auto-renew', enabled: elements.autoRenewInput.checked });
});

elements.clearLog.addEventListener('click', () => elements.eventLog.replaceChildren());
elements.openTab.addEventListener('click', () => window.open(location.href, '_blank', 'noopener'));

setInterval(render, 100);
setupRunner();
startWorker();
