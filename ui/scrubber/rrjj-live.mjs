import {
  appendLiveEvent,
  appendSseOverflow,
  createLiveTimeline,
  describeEvent,
  isRrjjStatus,
  parseNdjson,
  sameOriginEventsUrl,
} from './timeline-model.mjs';

const template = document.createElement('template');
template.innerHTML = `
  <style>
    :host { color: #e8edf2; display: block; font: 14px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; }
    .bar { align-items: center; background: #151a21; border: 1px solid #303844; border-radius: 10px; display: flex; flex-wrap: wrap; gap: 10px; padding: 12px; }
    input[type="url"] { background: #0d1117; border: 1px solid #3b4552; border-radius: 6px; color: inherit; flex: 1; min-width: 260px; padding: 8px; }
    button, .file { background: #263140; border: 1px solid #46566b; border-radius: 6px; color: inherit; cursor: pointer; padding: 7px 10px; }
    .file input { display: none; }
    .status { margin-left: auto; }
    .status::before { background: #8b949e; border-radius: 50%; content: ""; display: inline-block; height: 8px; margin-right: 7px; width: 8px; }
    .status[data-state="live"]::before { background: #3fb950; }
    .status[data-state="warning"]::before { background: #f0a43c; }
    .status[data-state="error"]::before { background: #f85149; }
    ol { list-style: none; margin: 18px 0 0; padding: 0 0 0 26px; position: relative; }
    ol::before { background: #30363d; bottom: 0; content: ""; left: 8px; position: absolute; top: 0; width: 2px; }
    li { background: #111720; border: 1px solid #2d3744; border-radius: 8px; margin: 0 0 10px; padding: 10px 12px; position: relative; }
    li::before { background: #58a6ff; border: 3px solid #0d1117; border-radius: 50%; content: ""; height: 10px; left: -24px; position: absolute; top: 14px; width: 10px; }
    .meta { color: #8b949e; font-size: 12px; }
    .title { color: #f0f6fc; font-weight: 700; margin: 3px 0; }
    .paths { color: #b1bac4; margin: 7px 0 0; max-height: 7em; overflow: auto; padding-left: 18px; }
    .empty { color: #8b949e; padding: 32px; text-align: center; }
  </style>
  <div class="bar">
    <input class="url" type="url" value="/events" aria-label="SSE events URL">
    <button class="connect">Connect</button>
    <label class="file">Load NDJSON<input type="file" accept=".ndjson,application/x-ndjson,application/json"></label>
    <span class="status" data-state="idle">idle</span>
  </div>
  <div class="empty">Connect to a daemon or load an NDJSON event file.</div>
  <ol aria-live="polite"></ol>
`;

export class RrjjLive extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' }).append(template.content.cloneNode(true));
    this.timeline = createLiveTimeline();
    this.source = null;
    this.initialized = false;
    this.autoProbe = 0;
  }

  connectedCallback() {
    if (this.initialized) return;
    this.initialized = true;
    this.shadowRoot.querySelector('.connect').addEventListener('click', () => {
      this.autoProbe += 1;
      this.connect();
    });
    this.shadowRoot.querySelector('input[type="file"]').addEventListener(
      'change',
      async (event) => {
        const [file] = event.target.files;
        if (!file) return;
        this.autoProbe += 1;
        this.disconnect();
        try {
          this.timeline = createLiveTimeline(parseNdjson(await file.text()));
          this.render();
          this.setStatus(
            'idle',
            `durable NDJSON · ${this.timeline.events.length} events · resynchronized`,
          );
        } catch (error) {
          this.setStatus('error', error.message);
        }
      },
    );
    if (this.hasAttribute('src')) {
      this.shadowRoot.querySelector('.url').value = this.getAttribute('src');
      this.connect();
    } else {
      this.shadowRoot.querySelector('.url').value = sameOriginEventsUrl();
      this.connectIfServedByRrjj();
    }
  }

  disconnectedCallback() {
    this.autoProbe += 1;
    this.disconnect();
  }

  async connectIfServedByRrjj() {
    const probe = ++this.autoProbe;
    this.setStatus('warning', 'checking same-origin daemon…');
    try {
      const response = await fetch('/health', {
        headers: { accept: 'application/json' },
      });
      const status = response.ok ? await response.json() : null;
      if (
        probe === this.autoProbe &&
        this.isConnected &&
        isRrjjStatus(status)
      ) {
        this.connect();
        return;
      }
    } catch {
      // A standalone static server has no rrjj health endpoint.
    }
    if (probe === this.autoProbe && this.isConnected) {
      this.setStatus('idle', 'idle · enter an events URL or load NDJSON');
    }
  }

  connect() {
    if (this.timeline.requiresResync) {
      this.setStatus(
        'error',
        'durable NDJSON resync required before reconnecting',
      );
      return;
    }
    this.disconnect();
    const url = this.shadowRoot.querySelector('.url').value;
    this.setStatus('warning', 'connecting…');
    const source = new EventSource(url);
    this.source = source;
    source.onopen = () => {
      if (this.source === source) this.setStatus('live', 'live');
    };
    source.addEventListener('event', (message) => {
      if (this.source !== source) return;
      try {
        this.timeline = appendLiveEvent(
          this.timeline,
          JSON.parse(message.data),
        );
        this.render();
        if (this.timeline.requiresResync) {
          this.disconnect();
          this.setStatus(
            'error',
            'sequence gap · load manifest.events_object to resync',
          );
        }
      } catch (error) {
        this.disconnect();
        this.timeline = appendLiveEvent(this.timeline, null);
        this.render();
        this.setStatus('error', `invalid event: ${error.message}`);
      }
    });
    source.addEventListener('overflow', (message) => {
      if (this.source !== source) return;
      const data = (() => {
        try {
          return JSON.parse(message.data);
        } catch {
          return { message: message.data };
        }
      })();
      this.timeline = appendSseOverflow(this.timeline, data);
      this.render();
      this.disconnect();
      this.setStatus(
        'error',
        'live overflow · load manifest.events_object to resync',
      );
    });
    source.onerror = () => {
      if (this.source === source) {
        this.setStatus('warning', 'disconnected · one reconnect pending…');
      }
    };
  }

  disconnect() {
    this.source?.close();
    this.source = null;
  }

  setStatus(state, text) {
    const status = this.shadowRoot.querySelector('.status');
    status.dataset.state = state;
    status.textContent = text;
  }

  render() {
    const timeline = this.shadowRoot.querySelector('ol');
    const empty = this.shadowRoot.querySelector('.empty');
    empty.hidden = this.timeline.events.length > 0;
    timeline.replaceChildren(
      ...this.timeline.events.map((event) => {
        const description = describeEvent(event);
        const item = document.createElement('li');
        const paths = description.paths?.length
          ? `<ul class="paths">${description.paths
              .map((path) => `<li>${escapeHtml(path)}</li>`)
              .join('')}</ul>`
          : '';
        item.innerHTML = `
          <div class="meta">#${event.seq ?? '?'} · ${escapeHtml(event.ts ?? '')} · ${escapeHtml(event.type ?? '')}</div>
          <div class="title">${escapeHtml(description.title)}</div>
          <div>${escapeHtml(description.detail)}</div>${paths}`;
        return item;
      }),
    );
  }
}

function escapeHtml(value) {
  const node = document.createElement('span');
  node.textContent = String(value);
  return node.innerHTML;
}

customElements.define('rrjj-live', RrjjLive);
