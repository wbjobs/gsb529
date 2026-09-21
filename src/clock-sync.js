export const MAX_REMOTE_CLOCK_SKEW_MS = 1500;

export function createClockEstimator(selfTabId, logicalNow, perfNow) {
  const samplesByTab = new Map();

  function samples(tabId) {
    if (!samplesByTab.has(tabId)) {
      samplesByTab.set(tabId, []);
    }
    return samplesByTab.get(tabId);
  }

  return {
    recordPing(tabId, t1, p1, t2, p2, t3, p3) {
      const rtt = Math.max(0, p3 - p1);
      const theta = t2 - (t1 + t3) / 2;
      const history = samples(tabId);
      history.push({ rtt, theta, at: p3 });
      while (history.length > 8) {
        history.shift();
      }
      while (history.length > 1 && p3 - history[0].at > 30_000) {
        history.shift();
      }
      return this.estimateFor(tabId);
    },

    estimateFor(tabId) {
      if (tabId === selfTabId) {
        return {
          now: logicalNow(),
          offset: 0,
          uncertainty: 0,
          rtt: 0,
          alive: true,
          hasSample: true,
        };
      }

      const history = samplesByTab.get(tabId) || [];
      const fresh = history.filter((sample) => perfNow() - sample.at < 15_000);
      if (fresh.length === 0) {
        return {
          now: logicalNow(),
          offset: 0,
          uncertainty: MAX_REMOTE_CLOCK_SKEW_MS,
          rtt: null,
          alive: false,
          hasSample: false,
        };
      }

      const best = fresh.reduce((left, right) =>
        left.rtt <= right.rtt ? left : right,
      );
      return {
        now: logicalNow() + best.theta,
        offset: best.theta,
        uncertainty: Math.max(5, best.rtt / 2),
        rtt: best.rtt,
        alive: true,
        hasSample: true,
      };
    },
  };
}
