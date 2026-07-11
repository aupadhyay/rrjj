export function sameOriginEventsUrl() {
  return '/events';
}

export function isRrjjStatus(value) {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    typeof value.session_id === 'string' &&
    value.session_id.length > 0 &&
    Number.isSafeInteger(value.seq) &&
    value.seq >= 0
  );
}

export function parseNdjson(text) {
  const events = text
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .map((line, index) => {
      try {
        return JSON.parse(line);
      } catch (error) {
        throw new Error(`Invalid NDJSON on line ${index + 1}: ${error.message}`);
      }
    });
  validateTimeline(events);
  return events;
}

export function validateTimeline(events) {
  let sessionId = null;
  events.forEach((event, index) => {
    if (!event || typeof event !== 'object' || Array.isArray(event)) {
      throw new Error(`Invalid event on NDJSON record ${index + 1}`);
    }
    if (!Number.isSafeInteger(event.seq) || event.seq < 0) {
      throw new Error(`Invalid sequence on NDJSON record ${index + 1}`);
    }
    if (event.seq !== index) {
      throw new Error(
        `Sequence gap on NDJSON record ${index + 1}: expected ${index}, received ${event.seq}`,
      );
    }
    if (typeof event.session_id !== 'string' || !event.session_id) {
      throw new Error(`Missing session_id on NDJSON record ${index + 1}`);
    }
    if (sessionId !== null && event.session_id !== sessionId) {
      throw new Error(`Session changed on NDJSON record ${index + 1}`);
    }
    sessionId = event.session_id;
  });
}

export function createLiveTimeline(events = []) {
  const last = events.at(-1);
  return {
    events: [...events],
    lastSeq: last?.seq ?? null,
    sessionId: last?.session_id ?? null,
    requiresResync: false,
  };
}

export function appendLiveEvent(timeline, event) {
  if (timeline.requiresResync) return timeline;
  const validEnvelope =
    event &&
    typeof event === 'object' &&
    !Array.isArray(event) &&
    Number.isSafeInteger(event.seq) &&
    event.seq >= 0 &&
    typeof event.session_id === 'string' &&
    event.session_id;
  if (!validEnvelope) {
    return appendStreamNotice(timeline, 'Invalid live event envelope');
  }
  const sequenceMatches =
    timeline.lastSeq === null || event.seq === timeline.lastSeq + 1;
  const sessionMatches =
    timeline.sessionId === null || event.session_id === timeline.sessionId;
  if (!sequenceMatches || !sessionMatches) {
    const detail = !sessionMatches
      ? `Session changed from ${timeline.sessionId} to ${event.session_id}`
      : `Expected sequence ${timeline.lastSeq + 1}, received ${event.seq}`;
    return appendStreamNotice(timeline, detail);
  }
  return {
    events: [...timeline.events, event],
    lastSeq: event.seq,
    sessionId: event.session_id,
    requiresResync: timeline.requiresResync,
  };
}

export function appendSseOverflow(timeline, data = {}) {
  const detail =
    data.message ??
    'The live subscriber lagged and records may have been dropped.';
  return appendStreamNotice(timeline, detail, 'sse_overflow');
}

function appendStreamNotice(timeline, detail, type = 'sequence_gap') {
  return {
    ...timeline,
    events: [
      ...timeline.events,
      {
        type,
        data: {
          detail,
          recovery: 'Load manifest.events_object to resynchronize.',
        },
      },
    ],
    requiresResync: true,
  };
}

export function describeEvent(event) {
  const data = event.data ?? {};
  switch (event.type) {
    case 'session_start':
      return {
        title: 'Session started',
        detail: `${event.session_id} · ${data.roots?.join(', ') ?? ''}`,
      };
    case 'touched_paths': {
      const paths = data.paths ?? [];
      const operations = [
        ...new Set(paths.flatMap((path) => path.operations ?? [])),
      ];
      return {
        title: `${paths.length} touched path${paths.length === 1 ? '' : 's'}`,
        detail: `${data.raw_events ?? 0} watcher events · ${operations.join(', ') || 'no operation category'}`,
        paths: paths.map((path) => path.path),
      };
    }
    case 'snapshot': {
      const stats = data.stats ?? {};
      const changed = Object.values(stats).reduce(
        (sum, value) => sum + (Number(value) || 0),
        0,
      );
      return {
        title: `Snapshot · ${changed} changed path${changed === 1 ? '' : 's'}`,
        detail: `${data.op ?? ''} → ${data.tree ?? ''}${data.truncated ? ' · listing truncated' : ''}`,
        paths: (data.changes ?? []).map(
          (change) => `${change.kind}: ${change.path}`,
        ),
      };
    }
    case 'mark':
      return {
        title: `Mark · ${data.label ?? ''}`,
        detail: data.ref_op ?? '',
      };
    case 'overflow':
      return {
        title: 'Watcher overflow',
        detail: `${data.source ?? ''} · recovery: ${data.recovery ?? 'unknown'}`,
      };
    case 'sse_overflow':
      return {
        title: 'Live stream overflow',
        detail: `${data.detail} ${data.recovery}`,
      };
    case 'sequence_gap':
      return {
        title: 'Live sequence discontinuity',
        detail: `${data.detail} ${data.recovery}`,
      };
    case 'flush':
      return { title: 'Durable flush', detail: data.op ?? '' };
    case 'session_end':
      return { title: 'Session ended', detail: data.reason ?? '' };
    default:
      return { title: event.type ?? 'unknown', detail: JSON.stringify(data) };
  }
}
