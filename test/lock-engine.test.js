import test from 'node:test';
import assert from 'node:assert/strict';

import {
  acquire,
  cancelWaiter,
  createResource,
  isOwnerActive,
  makeClock,
  reconcile,
  releaseOwner,
  renewOwner,
  touchWaiter,
} from '../src/lock-engine.js';

function clock(tabId, current, estimates) {
  let nowValue = current;
  return {
    clock: makeClock(tabId, () => nowValue, estimates),
    advance(ms) {
      nowValue += ms;
    },
  };
}

test('first requester acquires and later requests queue in ticket order', () => {
  const { clock: current, advance } = clock('tab-a', 100);
  const state = createResource('shared-document', 100);

  const first = acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  const second = acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );
  const third = acquire(
    state,
    { tabId: 'tab-c', requestId: 'req-c', ttl: 500 },
    current,
  );

  assert.equal(first.outcome, 'granted');
  assert.equal(second.outcome, 'queued');
  assert.equal(third.outcome, 'queued');
  assert.deepEqual(
    state.waiters.map((waiter) => waiter.tabId),
    ['tab-b', 'tab-c'],
  );

  advance(600);
  reconcile(state, current);
  const promoted = acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );

  assert.equal(promoted.outcome, 'granted');
  assert.equal(state.owner.tabId, 'tab-b');
  assert.equal(state.fencingToken, 2);
});

test('owner renews only with matching request and fencing token', () => {
  const { clock: current, advance } = clock('tab-a', 0);
  const state = createResource('resource', 0);
  acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  advance(400);

  const renewed = renewOwner(
    state,
    { tabId: 'tab-a', requestId: 'req-a', fencingToken: 1 },
    current.now(),
    500,
  );
  const wrongFence = renewOwner(
    state,
    { tabId: 'tab-a', requestId: 'req-a', fencingToken: 999 },
    current.now(),
    500,
  );

  assert.equal(renewed.changed, true);
  assert.equal(state.owner.expiresAt, 900);
  assert.equal(wrongFence.changed, false);
});

test('owner release is conditional and exposes next waiter', () => {
  const current = makeClock('tab-a', () => 0);
  const state = createResource('resource', 0);
  acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );

  assert.equal(
    releaseOwner(
      state,
      { tabId: 'tab-c', requestId: 'req-c', fencingToken: 1 },
      0,
    ).changed,
    false,
  );
  assert.equal(
    releaseOwner(
      state,
      { tabId: 'tab-a', requestId: 'req-a', fencingToken: 1 },
      0,
    ).changed,
    true,
  );
  assert.equal(state.owner, null);
  assert.equal(state.waiters[0].tabId, 'tab-b');
});

test('canceled waiter cannot bypass later waiters', () => {
  const current = makeClock('tab-a', () => 0);
  const state = createResource('resource', 0);
  acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-c', requestId: 'req-c', ttl: 500 },
    current,
  );
  cancelWaiter(
    state,
    { tabId: 'tab-b', requestId: 'req-b' },
    0,
  );
  releaseOwner(
    state,
    { tabId: 'tab-a', requestId: 'req-a', fencingToken: 1 },
    0,
  );

  const promotion = acquire(
    state,
    { tabId: 'tab-c', requestId: 'req-c', ttl: 500 },
    current,
  );
  assert.equal(promotion.outcome, 'granted');
  assert.equal(state.owner.fencingToken, 2);
});

test('dead waiter heartbeat is ignored', () => {
  const { clock: current, advance } = clock('tab-b', 0);
  const state = createResource('resource', 0);
  acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );
  advance(5_000);
  reconcile(state, current);

  assert.equal(state.waiters.length, 0);
  assert.equal(
    touchWaiter(
      state,
      { tabId: 'tab-b', requestId: 'req-b' },
      current.now(),
    ).changed,
    false,
  );
});

test('remote owner expires using conservative synchronized clock uncertainty', () => {
  const localNow = 10_000;
  const estimates = new Map([
    [
      'tab-b',
      {
        now: localNow,
        offset: 200,
        uncertainty: 300,
        alive: true,
      },
    ],
  ]);
  const current = makeClock('tab-a', () => localNow, estimates);
  const state = createResource('resource', 0);
  state.owner = {
    tabId: 'tab-b',
    requestId: 'req-b',
    fencingToken: 7,
    acquiredAt: 0,
    lastRenewedAt: 0,
    expiresAt: 9_600,
    ttl: 10_250,
  };

  assert.equal(isOwnerActive(state, current), false);
});

test('new requests do not jump ahead of an existing FIFO queue', () => {
  const current = makeClock('tab-a', () => 0);
  const state = createResource('resource', 0);
  acquire(
    state,
    { tabId: 'tab-a', requestId: 'req-a', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );
  acquire(
    state,
    { tabId: 'tab-c', requestId: 'req-c', ttl: 500 },
    current,
  );
  releaseOwner(
    state,
    { tabId: 'tab-a', requestId: 'req-a', fencingToken: 1 },
    0,
  );

  const late = acquire(
    state,
    { tabId: 'tab-d', requestId: 'req-d', ttl: 500 },
    current,
  );
  const next = acquire(
    state,
    { tabId: 'tab-b', requestId: 'req-b', ttl: 500 },
    current,
  );

  assert.equal(late.outcome, 'queued');
  assert.equal(next.outcome, 'granted');
  assert.equal(state.owner.tabId, 'tab-b');
  assert.equal(state.waiters[0].tabId, 'tab-c');
  assert.equal(state.waiters[1].tabId, 'tab-d');
});

test('reaper removes a dead head so the lock cannot remain permanently blocked', () => {
  const current = makeClock('tab-c', () => 10_000);
  const state = createResource('resource', 0);
  state.owner = {
    tabId: 'tab-a',
    requestId: 'req-a',
    fencingToken: 1,
    acquiredAt: 0,
    lastRenewedAt: 0,
    expiresAt: 1_000,
    ttl: 1_000,
  };
  state.waiters = [
    {
      tabId: 'tab-b',
      requestId: 'req-b',
      ticket: 1,
      enqueuedAt: 0,
      lastSeenAt: 0,
      expiresAt: 1_000,
    },
  ];
  state.nextTicket = 2;

  const acquired = acquire(
    state,
    { tabId: 'tab-c', requestId: 'req-c', ttl: 500 },
    current,
  );

  assert.equal(acquired.outcome, 'granted');
  assert.equal(state.waiters.length, 0);
  assert.equal(state.owner.tabId, 'tab-c');
});
