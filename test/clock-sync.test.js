import test from 'node:test';
import assert from 'node:assert/strict';

import { createClockEstimator } from '../src/clock-sync.js';

test('clock estimator derives offset from Cristian-style exchange', () => {
  let logical = 10_000;
  let perf = 5_000;
  const estimator = createClockEstimator(
    'tab-a',
    () => logical,
    () => perf,
  );

  perf = 5_010;
  logical = 10_010;
  estimator.recordPing(
    'tab-b',
    10_000,
    5_000,
    10_045,
    5_005,
    logical,
    perf,
  );

  const estimate = estimator.estimateFor('tab-b');
  assert.equal(estimate.offset, 40);
  assert.equal(estimate.rtt, 10);
  assert.equal(estimate.uncertainty, 5);
  assert.equal(estimate.now, 10_050);
});

test('stale clock samples fall back to conservative skew protection', () => {
  let perf = 0;
  const estimator = createClockEstimator(
    'tab-a',
    () => 1_000,
    () => perf,
  );
  estimator.recordPing('tab-b', 0, 0, 10, 0, 10, 10);
  perf = 20_000;

  const estimate = estimator.estimateFor('tab-b');
  assert.equal(estimate.hasSample, false);
  assert.equal(estimate.alive, false);
  assert.equal(estimate.uncertainty, 1_500);
});
