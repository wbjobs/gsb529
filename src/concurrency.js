const PARTICIPANT_COUNT = 4;
const LEASE_MS = 3000;
const RESOURCE = `runner/${Date.now()}`;
const channelName = `fair-lease-runner-${Date.now()}`;

const participantIds = Array.from({ length: PARTICIPANT_COUNT }, (_, index) => crypto.randomUUID());
const ready = new Set();
const acquisitions = [];
const renewObserved = { value: false };
let started = false;

const $ = (id) => document.getElementById(id);
const channel = new BroadcastChannel(channelName);

for (let index = 0; index < PARTICIPANT_COUNT; index += 1) {
  const frame = document.createElement('iframe');
  const autoRenew = index === PARTICIPANT_COUNT - 1 ? 1 : 0;
  const clientName = encodeURIComponent(`竞争者 ${index + 1}`);
  const clockBias = index === 1 ? 300 : 0;
  frame.src = `index.html?runner=1&clientId=${participantIds[index]}&channel=${encodeURIComponent(channelName)}&resource=${encodeURIComponent(RESOURCE)}&lease=${LEASE_MS}&autoRenew=${autoRenew}&clientName=${clientName}&clockBias=${clockBias}`;
  frame.title = `竞争者 ${index + 1}`;
  $('frames').append(frame);
}

function log(message) {
  const item = document.createElement('div');
  item.textContent = `${new Date().toLocaleTimeString('zh-CN', { hour12: false })}  ${message}`;
  $('timeline').prepend(item);
}

function uniqueHolders() {
  return new Set(acquisitions.map((item) => item.clientId)).size;
}

function gapsValid() {
  return acquisitions.every((item, index) => {
    if (index === 0) return true;
    const previous = acquisitions[index - 1];
    return item.at - previous.at >= LEASE_MS * 0.72;
  });
}

function evaluate() {
  const expected = PARTICIPANT_COUNT;
  const enoughAcquisitions = acquisitions.length >= expected;
  const allUnique = uniqueHolders() === expected;
  const seqs = acquisitions.map((item) => item.request?.seq || item.request?.holderRequestSeq);
  const isFifo = seqs.every((seq, index) => index === 0 || seq > seqs[index - 1]);
  const lastRenewed = renewObserved.value || acquisitions.some((item) => item.resource?.renewCount >= 1);
  const serialGap = gapsValid();
  const pass = enoughAcquisitions && allUnique && isFifo && lastRenewed && serialGap;

  const verdict = $('verdict');
  verdict.textContent = pass
    ? '通过：无重叠持有、FIFO 公平、自动续约有效，租约到期后自动交接'
    : `运行中或失败：交接 ${acquisitions.length}/${expected}，唯一持有者 ${uniqueHolders()}，FIFO=${isFifo}，续约=${lastRenewed}，串行间隔=${serialGap}`;
  verdict.className = pass ? 'verdict' : 'verdict fail';

  $('metrics').innerHTML = `
    <div><dt>完成交接</dt><dd>${acquisitions.length}</dd></div>
    <div><dt>唯一持有者数量</dt><dd>${uniqueHolders()}</dd></div>
    <div><dt>FIFO</dt><dd>${isFifo ? '通过' : '失败'}</dd></div>
    <div><dt>自动续约</dt><dd>${lastRenewed ? '通过' : '等待'}</dd></div>
    <div><dt>无重叠交接间隔</dt><dd>${serialGap ? '通过' : '等待'}</dd></div>
  `;

  if (pass) $('startBtn').disabled = true;
}

channel.addEventListener('message', (event) => {
  const message = event.data;
  if (!message) return;

  if (message.kind === 'ready') {
    ready.add(message.clientId);
    log(`${message.clientName} 就绪`);
    if (!started && ready.size === PARTICIPANT_COUNT) {
      started = true;
      log('广播并发开始');
      for (const clientId of ready) {
        channel.postMessage({ kind: 'start', clientId });
      }
    }
  }

  if (message.kind === 'acquired') {
    acquisitions.push(message);
    log(`${message.clientName} 获得锁，#${message.request?.seq}，token=${message.resource?.fencingToken}，续约=${message.resource?.renewCount || 0}`);
    evaluate();
  }

  if (message.kind === 'snapshot' && message.snapshot?.resource?.holderClientId) {
    if (message.snapshot.resource.renewCount >= 1) renewObserved.value = true;
  }
});

$('startBtn').addEventListener('click', () => {
  if (started) {
    $('verdict').textContent = '验收已启动，等待租约交接完成';
    return;
  }
  if (ready.size !== PARTICIPANT_COUNT) {
    $('verdict').textContent = `参与者仍在初始化：${ready.size}/${PARTICIPANT_COUNT}`;
    return;
  }
  started = true;
  for (const clientId of ready) channel.postMessage({ kind: 'start', clientId });
});

setInterval(evaluate, 500);
