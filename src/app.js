const worker = new Worker(new URL('./lease-worker.js', import.meta.url), {
  type: 'module',
});

const elements = {
  tabId: document.querySelector('#tabId'),
  role: document.querySelector('#role'),
  lockState: document.querySelector('#lockState'),
  leaseRing: document.querySelector('#leaseRing'),
  ownerId: document.querySelector('#ownerId'),
  fenceToken: document.querySelector('#fenceToken'),
  leaseRemaining: document.querySelector('#leaseRemaining'),
  safeRemaining: document.querySelector('#safeRemaining'),
  acquireBtn: document.querySelector('#acquireBtn'),
  releaseBtn: document.querySelector('#releaseBtn'),
  renewBtn: document.querySelector('#renewBtn'),
  cancelBtn: document.querySelector('#cancelBtn'),
  writeBtn: document.querySelector('#writeBtn'),
  payloadInput: document.querySelector('#payloadInput'),
  payloadView: document.querySelector('#payloadView'),
  ttlRange: document.querySelector('#ttlRange'),
  ttlValue: document.querySelector('#ttlValue'),
  skewRange: document.querySelector('#skewRange'),
  skewValue: document.querySelector('#skewValue'),
  autoRenew: document.querySelector('#autoRenew'),
  failRenewBtn: document.querySelector('#failRenewBtn'),
  zombieBtn: document.querySelector('#zombieBtn'),
  queueTitle: document.querySelector('#queueTitle'),
  queue: document.querySelector('#queue'),
  peers: document.querySelector('#peers'),
  events: document.querySelector('#events'),
};

function send(command, value) {
  worker.postMessage({ command, value });
}

function formatMs(ms) {
  if (ms === null || ms === undefined || Number.isNaN(ms)) {
    return '—';
  }
  return `${Math.ceil(ms).toLocaleString('zh-CN')} ms`;
}

function short(id) {
  return id ? id.slice(0, 8) : '—';
}

function setRole(role, zombie) {
  elements.role.className = `role ${zombie ? 'zombie' : role}`;
  if (zombie) {
    elements.role.textContent = '冻结';
  } else if (role === 'owner') {
    elements.role.textContent = '持有锁';
  } else if (role === 'waiter') {
    elements.role.textContent = '等待中';
  } else {
    elements.role.textContent = '空闲';
  }
}

function renderLease(snapshot) {
  const owner = snapshot.owner;
  const isOwner = snapshot.role === 'owner';

  if (!owner) {
    elements.lockState.textContent = '未锁定，队首请求可立即获得';
    elements.ownerId.textContent = '—';
    elements.fenceToken.textContent = '—';
    elements.leaseRemaining.textContent = '—';
    elements.safeRemaining.textContent = '—';
    elements.leaseRing.className = 'ring free';
    elements.leaseRing.style.setProperty('--deg', '0deg');
    elements.leaseRing.textContent = 'FREE';
    return;
  }

  const progress = Math.round(owner.progress * 100);
  const tone =
    owner.safeRemainingMs < 1200 ? 'danger' : owner.safeRemainingMs < 3000 ? 'warning' : '';
  elements.lockState.textContent = isOwner
    ? '本标签页持有锁，可进入临界区'
    : `${short(owner.tabId)} 持有锁，队首等待租约释放`;
  elements.ownerId.textContent = `${short(owner.tabId)}${owner.local ? '（本页）' : ''}`;
  elements.fenceToken.textContent = `#${owner.fencingToken}`;
  elements.leaseRemaining.textContent = formatMs(owner.remainingMs);
  elements.safeRemaining.textContent = owner.local
    ? '单调时钟精确计时'
    : `≥ ${formatMs(owner.safeRemainingMs)}（±${Math.round(owner.clock.uncertainty)} ms）`;
  elements.leaseRing.className = `ring ${tone}`.trim();
  elements.leaseRing.style.setProperty('--deg', `${progress * 3.6}deg`);
  elements.leaseRing.textContent = `${progress}%`;
}

function renderQueue(snapshot) {
  elements.queueTitle.textContent = `${snapshot.queue.length} 个等待者`;
  if (snapshot.queue.length === 0) {
    elements.queue.className = 'queue empty';
    elements.queue.textContent = '暂无等待请求';
    return;
  }

  elements.queue.className = 'queue';
  elements.queue.replaceChildren(
    ...snapshot.queue.map((waiter) => {
      const item = document.createElement('div');
      item.className = `queue-item${waiter.position === 1 ? ' head' : ''}`;
      item.innerHTML = `
        <span class="ticket">#${waiter.ticket}</span>
        <div>
          <p>${short(waiter.tabId)}${waiter.isSelf ? '（本页）' : ''}</p>
          <small>队首心跳剩余 ≥ ${formatMs(waiter.safeRemainingMs)}</small>
        </div>
        <span class="role ${waiter.position === 1 ? 'owner' : 'waiting'}">
          ${waiter.position === 1 ? 'NEXT' : `P${waiter.position}`}
        </span>
      `;
      return item;
    }),
  );
}

function renderPeers(snapshot) {
  elements.peers.replaceChildren(
    ...snapshot.peers.map((peer) => {
      const row = document.createElement('tr');
      const stateClass = peer.zombie ? 'zombie' : peer.alive ? '' : 'dead';
      row.innerHTML = `
        <td>${peer.shortId}${peer.isSelf ? '（本页）' : ''}</td>
        <td><span class="dot ${stateClass}">${
          peer.zombie ? '冻结' : peer.alive ? '在线' : '超时'
        }</span></td>
        <td>${Math.round(peer.offsetMs).toLocaleString('zh-CN')} ms</td>
        <td>${Math.round(peer.uncertaintyMs)} ms</td>
        <td>${peer.rttMs === null ? '—' : `${Math.round(peer.rttMs)} ms`}</td>
      `;
      return row;
    }),
  );
}

function renderEvents(snapshot) {
  elements.events.replaceChildren(
    ...snapshot.events.map((event) => {
      const item = document.createElement('li');
      const time = new Date(event.at).toLocaleTimeString('zh-CN', {
        hour12: false,
      });
      item.innerHTML = `<strong>${time}</strong> · ${event.message}`;
      return item;
    }),
  );
}

function render(snapshot) {
  elements.tabId.textContent = snapshot.shortTabId;
  setRole(snapshot.role, snapshot.config.zombie);
  renderLease(snapshot);
  renderQueue(snapshot);
  renderPeers(snapshot);
  renderEvents(snapshot);

  elements.acquireBtn.disabled = Boolean(snapshot.activeRequest);
  elements.releaseBtn.disabled = snapshot.role !== 'owner';
  elements.renewBtn.disabled = snapshot.role !== 'owner';
  elements.cancelBtn.disabled = snapshot.role !== 'waiter';
  elements.writeBtn.disabled = snapshot.role !== 'owner';
  elements.failRenewBtn.disabled =
    snapshot.role !== 'owner' || snapshot.config.failNextRenewal;
  elements.zombieBtn.textContent = snapshot.config.zombie
    ? '恢复本标签页'
    : '冻结本标签页';
  elements.zombieBtn.classList.toggle('warning', !snapshot.config.zombie);
  elements.ttlValue.textContent = `${snapshot.config.ttlMs} ms`;
  elements.skewValue.textContent = `${snapshot.config.clockSkewMs} ms`;
  elements.ttlRange.value = snapshot.config.ttlMs;
  elements.skewRange.value = snapshot.config.clockSkewMs;
  elements.autoRenew.checked = snapshot.config.autoRenew;
  elements.skewRange.disabled = Boolean(snapshot.activeRequest);

  if (snapshot.payload) {
    elements.payloadView.textContent =
      `资源值：${snapshot.payload.value} · 写入者 ${short(snapshot.payload.writerTabId)} · ` +
      `栅栏 #${snapshot.payload.fencingToken}`;
  } else {
    elements.payloadView.textContent = '共享资源尚无临界区写入';
  }
}

worker.addEventListener('message', (event) => {
  if (event.data?.kind === 'snapshot') {
    render(event.data.snapshot);
  }
});

elements.acquireBtn.addEventListener('click', () => send('acquire'));
elements.releaseBtn.addEventListener('click', () => send('release'));
elements.renewBtn.addEventListener('click', () => send('renew-now'));
elements.cancelBtn.addEventListener('click', () => send('cancel'));
elements.writeBtn.addEventListener('click', () =>
  send('write', elements.payloadInput.value),
);
elements.autoRenew.addEventListener('change', () =>
  send('set-auto-renew', elements.autoRenew.checked),
);
elements.ttlRange.addEventListener('change', () =>
  send('set-ttl', Number(elements.ttlRange.value)),
);
elements.skewRange.addEventListener('change', () =>
  send('set-skew', Number(elements.skewRange.value)),
);
elements.failRenewBtn.addEventListener('click', () =>
  send('fail-next-renewal'),
);
elements.zombieBtn.addEventListener('click', () =>
  send('set-zombie', elements.zombieBtn.textContent === '冻结本标签页'),
);

window.addEventListener('pagehide', () => send('shutdown'));
