export const WAITER_TTL_MS = 4000;
export const MAX_CLOCK_SKEW_MS = 1500;

export function createResource(id, now = 0) {
  return {
    id,
    fencingToken: 0,
    nextTicket: 1,
    revision: 0,
    owner: null,
    waiters: [],
    createdAt: now,
  };
}

export function makeClock(selfTabId, now, estimates = new Map()) {
  return {
    selfTabId,
    now,
    estimateFor(tabId) {
      if (tabId === selfTabId) {
        return { now: now(), offset: 0, uncertainty: 0, alive: true };
      }
      return (
        estimates.get(tabId) || {
          now: now(),
          offset: 0,
          uncertainty: MAX_CLOCK_SKEW_MS,
          alive: false,
        }
      );
    },
    earliestFor(tabId) {
      const estimate = this.estimateFor(tabId);
      return estimate.now - estimate.uncertainty;
    },
  };
}

function earliest(clock, tabId) {
  if (tabId === clock.selfTabId) {
    return clock.now();
  }
  return clock.earliestFor(tabId);
}

export function isOwnerActive(state, clock) {
  if (!state.owner) {
    return false;
  }
  return earliest(clock, state.owner.tabId) < state.owner.expiresAt;
}

export function isWaiterActive(waiter, clock) {
  return earliest(clock, waiter.tabId) < waiter.expiresAt;
}

export function reconcile(state, clock) {
  const events = [];
  let changed = false;

  if (state.owner && !isOwnerActive(state, clock)) {
    events.push({
      type: 'owner-expired',
      at: clock.now(),
      owner: { ...state.owner },
    });
    state.owner = null;
    changed = true;
  }

  const activeWaiters = [];
  for (const waiter of state.waiters) {
    if (isWaiterActive(waiter, clock)) {
      activeWaiters.push(waiter);
    } else {
      events.push({
        type: 'waiter-expired',
        at: clock.now(),
        waiter: { ...waiter },
      });
      changed = true;
    }
  }
  state.waiters = activeWaiters;

  return { changed, events };
}

function grant(state, input, now, waiter = null, priorEvents = []) {
  const fencingToken = state.fencingToken + 1;
  const owner = {
    tabId: input.tabId,
    requestId: input.requestId,
    fencingToken,
    acquiredAt: now,
    lastRenewedAt: now,
    expiresAt: now + input.ttl,
    ttl: input.ttl,
  };
  state.fencingToken = fencingToken;
  state.owner = owner;
  state.waiters = state.waiters.filter(
    (waiterItem) => waiterItem.requestId !== input.requestId,
  );

  return {
    outcome: 'granted',
    owner: { ...owner },
    changed: true,
    events: [
      ...priorEvents,
      {
        type: waiter ? 'lock-acquired-after-wait' : 'lock-acquired',
        at: now,
        owner: { ...owner },
        waiter: waiter ? { ...waiter } : null,
        waitedMs: waiter ? now - waiter.enqueuedAt : 0,
      },
    ],
  };
}

export function acquire(state, input, clock) {
  const reconciliation = reconcile(state, clock);
  const now = clock.now();
  const owner = state.owner;
  const ownWaiter = state.waiters.find(
    (waiter) => waiter.requestId === input.requestId,
  );
  const waiterForTab = state.waiters.find(
    (waiter) => waiter.tabId === input.tabId,
  );

  if (owner && owner.tabId === input.tabId) {
    return {
      ...reconciliation,
      outcome:
        owner.requestId === input.requestId ? 'already-owner' : 'tab-busy',
    };
  }

  if (waiterForTab && waiterForTab.requestId !== input.requestId) {
    return { ...reconciliation, outcome: 'tab-busy' };
  }

  if (ownWaiter) {
    if (!owner && state.waiters[0]?.requestId === input.requestId) {
      return {
        ...reconciliation,
        ...grant(state, input, now, ownWaiter, reconciliation.events),
      };
    }
    return { ...reconciliation, outcome: 'already-waiting' };
  }

  if (!owner) {
    if (state.waiters.length === 0) {
      return {
        ...reconciliation,
        ...grant(state, input, now, null, reconciliation.events),
      };
    }

    if (state.waiters[0].requestId === input.requestId) {
      return {
        ...reconciliation,
        ...grant(
          state,
          input,
          now,
          state.waiters[0],
          reconciliation.events,
        ),
      };
    }
  }

  const waiter = {
    tabId: input.tabId,
    requestId: input.requestId,
    ticket: state.nextTicket,
    enqueuedAt: now,
    lastSeenAt: now,
    expiresAt: now + (input.waiterTtl || WAITER_TTL_MS),
  };
  state.nextTicket += 1;
  state.waiters.push(waiter);

  return {
    ...reconciliation,
    outcome: 'queued',
    waiter: { ...waiter },
    changed: true,
    events: [
      ...reconciliation.events,
      {
        type: 'waiter-queued',
        at: now,
        waiter: { ...waiter },
        position: state.waiters.length,
      },
    ],
  };
}

export function touchWaiter(state, identity, now, ttl = WAITER_TTL_MS) {
  const waiter = state.waiters.find(
    (item) =>
      item.tabId === identity.tabId &&
      item.requestId === identity.requestId,
  );
  if (!waiter) {
    return { changed: false, events: [] };
  }

  waiter.lastSeenAt = now;
  waiter.expiresAt = now + ttl;
  return {
    changed: true,
    events: [{ type: 'waiter-heartbeat', at: now, waiter: { ...waiter } }],
  };
}

export function renewOwner(state, identity, now, ttl) {
  const owner = state.owner;
  if (
    !owner ||
    owner.tabId !== identity.tabId ||
    owner.requestId !== identity.requestId ||
    (identity.fencingToken !== undefined &&
      owner.fencingToken !== identity.fencingToken)
  ) {
    return { changed: false, owner: null, events: [] };
  }

  owner.lastRenewedAt = now;
  owner.expiresAt = now + ttl;
  owner.ttl = ttl;

  return {
    changed: true,
    owner: { ...owner },
    events: [{ type: 'lease-renewed', at: now, owner: { ...owner } }],
  };
}

export function releaseOwner(state, identity, now) {
  const owner = state.owner;
  if (
    !owner ||
    owner.tabId !== identity.tabId ||
    owner.requestId !== identity.requestId ||
    (identity.fencingToken !== undefined &&
      owner.fencingToken !== identity.fencingToken)
  ) {
    return { changed: false, events: [] };
  }

  state.owner = null;
  return {
    changed: true,
    events: [
      {
        type: 'owner-released',
        at: now,
        owner: { ...owner },
      },
    ],
  };
}

export function cancelWaiter(state, identity, now) {
  const index = state.waiters.findIndex(
    (waiter) =>
      waiter.tabId === identity.tabId &&
      waiter.requestId === identity.requestId,
  );
  if (index === -1) {
    return { changed: false, events: [] };
  }

  const waiter = state.waiters[index];
  state.waiters.splice(index, 1);
  return {
    changed: true,
    events: [
      {
        type: 'waiter-canceled',
        at: now,
        waiter: { ...waiter },
      },
    ],
  };
}

export function findSelfRequest(state, tabId) {
  if (state.owner?.tabId === tabId) {
    return { kind: 'owner', request: state.owner };
  }
  const waiter = state.waiters.find((item) => item.tabId === tabId);
  return waiter ? { kind: 'waiter', request: waiter } : null;
}
