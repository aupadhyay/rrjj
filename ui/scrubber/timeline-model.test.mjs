import assert from 'node:assert/strict';
import test from 'node:test';

import {
  appendLiveEvent,
  appendSseOverflow,
  createLiveTimeline,
  describeEvent,
  isRrjjStatus,
  parseNdjson,
  sameOriginEventsUrl,
} from './timeline-model.mjs';

test('uses same-origin events and recognizes rrjj health status', () => {
  assert.equal(sameOriginEventsUrl(), '/events');
  assert.equal(isRrjjStatus({ session_id: 'demo', seq: 0 }), true);
  assert.equal(isRrjjStatus({ session_id: 'demo' }), false);
  assert.equal(isRrjjStatus({ session_id: '', seq: 0 }), false);
});

test('parses NDJSON and ignores blank lines', () => {
  assert.deepEqual(
    parseNdjson(
      '{"seq":0,"session_id":"s"}\n\n{"seq":1,"session_id":"s"}\n',
    ),
    [
      { seq: 0, session_id: 's' },
      { seq: 1, session_id: 's' },
    ],
  );
});

test('rejects durable sequence gaps and session changes', () => {
  assert.throws(
    () =>
      parseNdjson(
        '{"seq":0,"session_id":"s"}\n{"seq":2,"session_id":"s"}\n',
      ),
    /expected 1, received 2/,
  );
  assert.throws(
    () =>
      parseNdjson(
        '{"seq":0,"session_id":"s"}\n{"seq":1,"session_id":"other"}\n',
      ),
    /Session changed/,
  );
});

test('describes snapshot, flush, mark, and schema overflow events', () => {
  assert.deepEqual(
    describeEvent({
      type: 'snapshot',
      data: {
        op: 'op:2',
        tree: 't:2',
        stats: { added: 2, removed: 1 },
        truncated: true,
        changes: [{ kind: 'added', path: 'new.txt' }],
      },
    }),
    {
      title: 'Snapshot · 3 changed paths',
      detail: 'op:2 → t:2 · listing truncated',
      paths: ['added: new.txt'],
    },
  );
  assert.deepEqual(describeEvent({ type: 'flush', data: { op: 'op:2' } }), {
    title: 'Durable flush',
    detail: 'op:2',
  });
  assert.deepEqual(
    describeEvent({
      type: 'mark',
      data: { label: 'build', ref_op: 'op:2' },
    }),
    { title: 'Mark · build', detail: 'op:2' },
  );
  assert.deepEqual(
    describeEvent({
      type: 'overflow',
      data: { source: 'watcher', recovery: 'full_scan_snapshot' },
    }),
    {
      title: 'Watcher overflow',
      detail: 'watcher · recovery: full_scan_snapshot',
    },
  );
});

test('live model detects gaps and blocks events until durable resync', () => {
  const initial = createLiveTimeline();
  const live = appendLiveEvent(initial, {
    seq: 7,
    session_id: 's',
    type: 'mark',
    data: {},
  });
  const gap = appendLiveEvent(live, {
    seq: 9,
    session_id: 's',
    type: 'mark',
    data: {},
  });
  assert.equal(gap.requiresResync, true);
  assert.equal(gap.events.at(-1).type, 'sequence_gap');
  assert.match(gap.events.at(-1).data.detail, /Expected sequence 8/);
  assert.strictEqual(
    appendLiveEvent(gap, {
      seq: 8,
      session_id: 's',
      type: 'mark',
      data: {},
    }),
    gap,
  );
});

test('SSE overflow is visible and requires durable resync', () => {
  const overflow = appendSseOverflow(createLiveTimeline(), {
    message: 'lagged by 3',
  });
  assert.equal(overflow.requiresResync, true);
  assert.deepEqual(overflow.events, [
    {
      type: 'sse_overflow',
      data: {
        detail: 'lagged by 3',
        recovery: 'Load manifest.events_object to resynchronize.',
      },
    },
  ]);
});

test('reports the invalid NDJSON line', () => {
  assert.throws(() => parseNdjson('{"seq":0}\nnope\n'), /line 2/);
});

test('describes touched operations without treating them as snapshots', () => {
  assert.deepEqual(
    describeEvent({
      type: 'touched_paths',
      data: {
        raw_events: 4,
        paths: [{ path: 'src/main.rs', operations: ['modify', 'rename'] }],
      },
    }),
    {
      title: '1 touched path',
      detail: '4 watcher events · modify, rename',
      paths: ['src/main.rs'],
    },
  );
});
