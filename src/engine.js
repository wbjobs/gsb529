export const PEER_TIMEOUT_MS = 1500;
export const UNKNOWN_CLOCK_DRIFT = 5000;
export const CLOCK_RATE_SKEW_PPM = 100;

export function peerClock(peer) {
  if (!peer || !Number.isFinite(peer.offsetMs) || peer.lastSeenAgeMs > PEER_TIMEOUT_MS) {
    return { offsetMs: 0, uncertaintyMs: UNKNOWN_CLOCK_DRIFT };
  }

  const roundTripMs = Number.isFinite(peer.rttMs) ? peer.rttMs : UNKNOWN_CLOCK_DRIFT;
  const rateSkewMs = peer.lastSeenAgeMs * CLOCK_RATE_SKEW_PPM / 1_000_000;
  return {
    offsetMs: peer.offsetMs,
    uncertaintyMs: roundTripMs / 2 + rateSkewMs,
  };
}

export function leaseView(resource, holderPeer, nowWallMs) {
  if (!resource || !resource.holderClientId) {
    return {
      active: false,
      remainingMs: Number.POSITIVE_INFINITY,
      uncertaintyMs: 0,
      holderNowMs: nowWallMs,
    };
  }

  const clock = peerClock(holderPeer);
  const holderNowMs = nowWallMs + clock.offsetMs;
  const remainingMs = resource.expiresWallAt - holderNowMs;

  return {
    active: remainingMs + clock.uncertaintyMs > 0,
    remainingMs,
    uncertaintyMs: clock.uncertaintyMs,
    holderNowMs,
  };
}

function requestDeadlinePassed(request, peer, nowWallMs, deadlineKey) {
  const deadline = request[deadlineKey];
  if (!Number.isFinite(deadline)) return false;
  const clock = peerClock(peer);
  const ownerNowMs = nowWallMs + clock.offsetMs;
  return ownerNowMs - clock.uncertaintyMs > deadline;
}

export function planGrant({ resource, waitingRequests = [], nowWallMs, peers = {} }) {
  const purgeSeqs = new Set();
  const liveRequests = [];

  for (const request of [...waitingRequests].sort((a, b) => a.seq - b.seq)) {
    const ownerPeer = peers[request.clientId] || null;
    const isDead = requestDeadlinePassed(request, ownerPeer, nowWallMs, 'aliveUntilWallMs');
    const timedOut = requestDeadlinePassed(request, ownerPeer, nowWallMs, 'waitDeadlineWallMs');
    if (isDead || timedOut) {
      purgeSeqs.add(request.seq);
    } else {
      liveRequests.push(request);
    }
  }

  const holderPeer = resource ? peers[resource.holderClientId] || null : null;
  const lease = leaseView(resource, holderPeer, nowWallMs);
  if (lease.active) {
    return {
      changed: purgeSeqs.size > 0,
      purgeSeqs: [...purgeSeqs],
      releaseHolder: false,
      grant: null,
      nextResource: resource,
      lease,
    };
  }

  if (resource && resource.holderClientId) {
    purgeSeqs.add(resource.holderRequestSeq);
  }

  const grant = liveRequests[0] || null;
  if (!grant) {
    const cleared = clearHolder(resource);
    return {
      changed: purgeSeqs.size > 0 || Boolean(resource?.holderClientId),
      purgeSeqs: [...purgeSeqs].filter(Boolean),
      releaseHolder: Boolean(resource?.holderClientId),
      grant: null,
      nextResource: cleared,
      lease,
    };
  }

  const grantedAtMs = grant.nowWallMs ?? nowWallMs;
  const nextResource = {
    ...clearHolder(resource),
    resource: grant.resource,
    holderClientId: grant.clientId,
    holderName: grant.clientName,
    holderRequestSeq: grant.seq,
    fencingToken: (resource?.fencingToken || 0) + 1,
    leaseMs: grant.leaseMs,
    acquiredWallAt: grantedAtMs,
    lastRenewWallAt: grantedAtMs,
    expiresWallAt: grantedAtMs + grant.leaseMs,
    renewCount: 0,
  };

  return {
    changed: true,
    purgeSeqs: [...purgeSeqs].filter((seq) => seq !== grant.seq),
    releaseHolder: Boolean(resource?.holderClientId),
    grant,
    nextResource,
    lease,
  };
}

export function clearHolder(resource) {
  if (!resource) return null;
  return {
    ...resource,
    holderClientId: null,
    holderName: null,
    holderRequestSeq: null,
    acquiredWallAt: null,
    lastRenewWallAt: null,
    expiresWallAt: null,
    renewCount: 0,
  };
}

export function planRelease(resource, requestSeq) {
  if (!resource || resource.holderRequestSeq !== requestSeq) {
    return { ok: false, reason: 'not-owner', resource };
  }
  return { ok: true, resource: clearHolder(resource), deleteRequestSeq: requestSeq };
}

export function planRenew(resource, requestSeq, nowWallMs) {
  if (!resource || resource.holderRequestSeq !== requestSeq) {
    return { ok: false, reason: 'not-owner', resource };
  }
  if (nowWallMs >= resource.expiresWallAt) {
    return { ok: false, reason: 'lease-expired', resource };
  }

  return {
    ok: true,
    resource: {
      ...resource,
      lastRenewWallAt: nowWallMs,
      expiresWallAt: nowWallMs + resource.leaseMs,
      renewCount: resource.renewCount + 1,
    },
  };
}
