globalThis.self = globalThis;
globalThis.BroadcastChannel = class BroadcastChannel {
  addEventListener() {}
  postMessage() {}
};
await import('../src/engine.js');
await import('../src/lock-worker.js');
