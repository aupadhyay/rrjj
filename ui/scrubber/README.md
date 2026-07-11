# rrjj scrubber

Serve this directory instead of opening it as `file://` so browser module and
file-loading behavior are consistent:

```sh
python3 -m http.server 8000 --directory ui/scrubber
```

Open <http://127.0.0.1:8000>, then enter the daemon's events URL or load a
session's durable NDJSON file referenced by `manifest.json` as `events_object`.
The standalone server has no `/health`, so the component remains idle. When
served by `rrjj daemon --http ...`, it detects same-origin `/health` and
automatically connects to same-origin `/events`. The `<rrjj-live>` Web Component
can also be reused with an explicit URL:

```html
<script type="module" src="/rrjj-live.mjs"></script>
<rrjj-live src="http://127.0.0.1:8787/events"></rrjj-live>
```

An SSE overflow or sequence discontinuity is appended to the visible timeline.
Because the live endpoint has no replay buffer, the component then closes the
connection and requires loading the manifest's durable `events_object` before
another connection can start. Ordinary transport errors use EventSource's
single built-in reconnect path; stale callbacks and parallel reconnects are
ignored.

Run the dependency-free model tests with:

```sh
node --test ui/scrubber/timeline-model.test.mjs
```
