import test from 'node:test';
import assert from 'node:assert/strict';
import {
  clearHolder,
  leaseView,
  planGrant,
  planRelease,
  planRenew,
} from '../src/engine.js';

const selfPeer = { offsetMs: 0, rttMs: 0, lastSeenAgeMs: 0 };

function request(seq, clientId, overrides = {}) {
  return {
    seq,
    resource: 'resource-a',
    clientId,
    clientName: clientId,
    status: 'waiting',
    leaseMs: 4000,
    enqueuedWallAt: 1000,
    aliveUntilWallMs: 10_000,
    waitDeadlineWallMs: null,
    ...overrides,
  };
}

function heldResource(overrides = {}) {
  return {
    resource: 'resource-a',
    holderClientId: 'owner-a',
    holderName: 'owner-a',
    holderRequestSeq: 1,
    fencingToken: 7,
    leaseMs: 4000,
    acquiredWallAt: 1000,
    lastRenewWallAt: 1000,
    expiresWallAt: 5000,
    renewCount: 0,
    ...overrides,
  };
}

test('grants the oldest live waiting request in FIFO order', () => {
  const plan = planGrant({
    resource: null,
    nowWallMs: 2000,
    peers: { a: selfPeer, b: selfPeer },
    waitingRequests: [request(2, 'b'), request(1, 'a')],
  });

  assert.equal(plan.grant.seq, 1);
  assert.equal(plan.nextResource.holderClientId, 'a');
  assert.equal(plan.nextResource.fencingToken, 1);
});

test('does not grant while the lease is certainly active', () => {
  const plan = planGrant({
    resource: heldResource(),
    nowWallMs: 4999,
    peers: { 'owner-a': selfPeer, a: selfPeer },
    waitingRequests: [request(2, 'a')],
  });

  assert.equal(plan.grant, null);
  assert.equal(plan.nextResource.holderClientId, 'owner-a');
});

test('releases an expired lease and hands the lock to the queue head', () => {
  const plan = planGrant({
    resource: heldResource(),
    nowWallMs: 5001,
    peers: { 'owner-a': selfPeer, waiter: selfPeer },
    waitingRequests: [request(2, 'waiter')],
  });

  assert.deepEqual(plan.purgeSeqs, [1]);
  assert.equal(plan.grant.seq, 2);
  assert.equal(plan.nextResource.holderClientId, 'waiter');
  assert.equal(plan.nextResource.fencingToken, 8);
});

test('uses clock uncertainty to avoid premature takeover', () => {
  const peer = { offsetMs: 0, rttMs: 100, lastSeenAgeMs: 0 };
  const view = leaseView(heldResource({ expiresWallAt: 5050 }), peer, 5010);
  assert.equal(view.remainingMs, 40);
  assert.equal(view.active, true);

  const plan = planGrant({
    resource: heldResource({ expiresWallAt: 5050 }),
    nowWallMs: 5010,
    peers: { 'owner-a': peer, waiter: selfPeer },
    waitingRequests: [request(2, 'waiter')],
  });
  assert.equal(plan.grant, null);
});

test('purges dead and timed-out waiters without breaking FIFO fairness', () => {
  const plan = planGrant({
    resource: null,
    nowWallMs: 6000,
    peers: { live: selfPeer, dead: selfPeer },
    waitingRequests: [
      request(1, 'dead', { aliveUntilWallMs: 5000 }),
      request(2, 'live', { enqueuedWallAt: 2000 }),
    ],
  });

  assert.deepEqual(plan.purgeSeqs, [1]);
  assert.equal(plan.grant.seq, 2);
});

test('renewal extends expiry and keeps the fencing token stable', () => {
  const result = planRenew(heldResource(), 1, 4000);
  assert.equal(result.ok, true);
  assert.equal(result.resource.expiresWallAt, 8000);
  assert.equal(result.resource.renewCount, 1);
  assert.equal(result.resource.fencingToken, 7);
});

test('renewal fails after lease expiry or ownership change', () => {
  assert.equal(planRenew(heldResource(), 1, 5000).ok, false);
  assert.equal(planRenew(heldResource(), 99, 4000).reason, 'not-owner');
});

test('release is conditional and preserves fencing monotonicity', () => {
  const denied = planRelease(heldResource(), 99);
  assert.equal(denied.ok, false);

  const released = planRelease(heldResource(), 1);
  assert.equal(released.ok, true);
  assert.equal(released.resource.holderClientId, null);
  assert.equal(released.resource.fencingToken, 7);
  assert.equal(clearHolder(heldResource()).fencingToken, 7);
});
