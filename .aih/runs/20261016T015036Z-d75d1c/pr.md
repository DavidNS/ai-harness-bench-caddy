## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

> **Run ABORTED.** This branch holds what was committed before the run stopped, plus the run's artifacts. It is evidence, not a finished change.
>
> `run.aborted`: review.changes_requested: the reviewer requests 2 change(s): ACC1: the reviewer's probe `modules/caddyhttp/reverseproxy/aih_probe_test.go` fails on the change [proven: `modules/caddyhttp/reverseproxy/aih_probe_test.go` fails] [proven: `modules/caddyhttp/reverseproxy/aih_probe_test.go` fails]

### Plan

One PR: add a process-wide detached-stream registry keyed by upstream, stream_detached and stream_logs options, hijack-safe recorder/encode/intercept, stream metrics, docs, and unit plus integration tests. Docs live in Go doc comments (generated docs), no separate doc files.

### Request

Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy
PR #7649 scope).

Long-lived upgraded connections through the reverse proxy (WebSocket and other `Upgrade`
tunnels, HTTP/2 extended CONNECT) keep the whole handler and config alive across reloads, so a
server that reloads often holds on to old configs, and the current behaviour on reload (close every
upgraded stream, optionally after a delay) is the only option. Middleware and response writers also
misbehave once a connection has been hijacked, and operators cannot see what their streams are
doing.

Required behaviour:

- A new opt-in `reverse_proxy` setting, `stream_detached`: when set, upgraded streams are detached
  from the config lifecycle and survive a reload; on reload only the streams whose upstream was
  removed from the new config are closed. The default stays as today: upgraded streams are closed on
  reload, honouring the existing close-delay behaviour.
- A `stream_logs` block with a `skip_handshake` option to suppress access logging of the upgrade
  handshake (default: logged), with stream close logs available in the access log via
  `logger_name access`, and a configurable log level (default DEBUG).
- Upgraded streams must keep working through the other handlers of a route: `encode`, `intercept`
  with `handle_response`, access logging. Once a connection is hijacked, nothing may write to the
  response again; a 101 is the final response for an upgrade; the proxy copies bytes on the raw
  connection, not through the recording response writer.
- HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, as do HTTP/1.1 upgrades in detached
  mode.
- Stream observability metrics in `reverse_proxy`: active streams, streams closed by result,
  stream duration, bytes to and from the backend, each stream accounted exactly once.
- Caddyfile and JSON config both expose the new options; document them.

Add integration tests in `caddytest/integration` that run a real server and real clients: streams
surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through
`encode` and `intercept`, detached streams with access logging. Add unit tests for the pieces you
change. The existing suites must stay green. Do not change unrelated behaviour.

### Intents

- `INTENT1` Add an opt-in `stream_detached` setting to `reverse_proxy` (JSON and Caddyfile) so that upgraded streams are detached from the config lifecycle and survive a reload, and on reload only streams whose upstream was removed from the new config are closed. (change)
- `INTENT2` Without `stream_detached`, upgraded streams are still closed on reload, honouring the existing stream close-delay behaviour unchanged. (change)
- `INTENT3` Add a `stream_logs` block (JSON and Caddyfile) with a `skip_handshake` option that suppresses access logging of the upgrade handshake (default: the handshake is logged), a configurable log level (default DEBUG), and stream close logs available in the access log via `logger_name access`. (change)
- `INTENT4` Upgraded streams must work through the other handlers of a route (`encode`, `intercept` with `handle_response`, access logging); once a connection is hijacked nothing writes to the response again, a 101 is the final response for an upgrade, and the proxy copies bytes on the raw connection rather than through the recording response writer. (change)
- `INTENT5` HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, and HTTP/1.1 upgrades keep working in detached mode. (change)
- `INTENT6` Add `reverse_proxy` stream metrics: active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once. (change)
- `INTENT7` Document the new Caddyfile and JSON options (`stream_detached`, `stream_logs`, `skip_handshake`, log level). (change)
- `INTENT8` Add integration tests in `caddytest/integration` using a real server and real clients covering: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT9` Add unit tests for the pieces changed, and keep the existing test suites green. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedReload` (covered, probe failed) | pending |
| ACC2 | behavior | T1 | `modules/caddyhttp/reverseproxy/caddyfile_test.go::TestStreamOptionsCaddyfile` (covered, probe verified) | pending |
| ACC3 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamCloseDelayReload` (covered, probe verified) | pending |
| ACC4 | behavior | T1 | `modules/caddyhttp/reverseproxy/caddyfile_test.go::TestStreamOptionsCaddyfile` (covered, probe verified) | pending |
| ACC5 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedAccessLog` (covered, probe verified) | pending |
| ACC6 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamThroughEncode` (covered, probe verified) | pending |
| ACC7 | behavior | T1 | `modules/caddyhttp/responsewriter_test.go::TestResponseRecorderSizeIgnoresRawConn` (covered, probe verified) | pending |
| ACC8 | behavior | T1 | `caddytest/integration/stream_test.go::TestH2ExtendedConnectWebSocket` (covered, probe verified) | pending |
| ACC9 | behavior | T1 | `modules/caddyhttp/reverseproxy/metrics_test.go::TestStreamMetrics` (covered, probe verified) | pending |
| ACC10 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/intercept_test.go, caddytest/integration/stream_reload_test.go, caddytest/integration/stream_test.go, modules/caddyhttp/encode/encode_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/reverseproxy/caddyfile_test.go, modules/caddyhttp/reverseproxy/detached_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | pending |
| ACC11 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedReload` (covered, probe verified) | pending |
| ACC12 | behavior | T1 | `modules/caddyhttp/encode/encode_test.go::TestResponseWriterHijackStopsWrites` (covered, probe verified) | pending |

### Tasks

- `T1` Order: (1) hijack safety in caddyhttp recorder, encode and intercept; (2) new stream registry (detached.go) plus StreamDetached/StreamLogs fields, Provision/Cleanup and handleUpgradeResponse using raw conn; (3) metrics; (4) Caddyfile parsing and docs; (5) unit tests then integration tests. Default non-detached path keeps closeConnections/StreamCloseDelay unchanged. (1 attempt(s), commit ``)
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/reverseproxy.go:489`: Cleanup only closes detached streams for h.Upstreams; streams to dynamic upstreams are never closed on reload, and no test covers that case.
  - reviewer note [pass 4] `modules/caddyhttp/reverseproxy/detached.go:78`: streamLevel re-parses the level string on every call. This is harmless, but a level parsed once at Provision time would be simpler.

### Gate failures along the way

- plan (plan, attempt 1): plan.evidence_quote_not_found
- review.approved (T1, attempt 1): review.changes_requested
- review.approved (T1, attempt 1): review.changes_requested
- review.approved (T1, attempt 1): review.changes_requested

### Cost (client-side estimate)

- sessions: 9; turns: 255; USD: 6.67; attempts: 1

_Generated by aih run `20261016T015036Z-d75d1c`._
