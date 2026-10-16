## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

### Plan

One releasable change: add opt-in `stream_detached` plus a `stream_logs` block (skip_handshake, level) and stream metrics to reverse_proxy. Make the response-writer chain (recorder, encode, intercept) treat a 101 as final and write nothing after hijack. Detached streams are tracked in a package-level registry keyed by upstream and, on handler Cleanup, only those whose upstream is absent from the live config are closed. Docs are the Caddyfile doc comment and JSON field comments, since the repo has no separate docs tree.

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

- `INTENT1` Add an opt-in `reverse_proxy` setting `stream_detached` (Caddyfile and JSON) that detaches upgraded streams from the config lifecycle so they survive a reload, closing on reload only streams whose upstream was removed from the new config. (change)
- `INTENT2` Keep the default (stream_detached off) as today: upgraded streams are closed on reload, honouring the existing close-delay behaviour. (change)
- `INTENT3` Add a `stream_logs` block (Caddyfile and JSON) with a `skip_handshake` option that suppresses access logging of the upgrade handshake (default: logged). (change)
- `INTENT4` Emit stream close logs that are available in the access log via `logger_name access`, with a configurable log level defaulting to DEBUG. (change)
- `INTENT5` Make upgraded streams work through `encode`, `intercept` with `handle_response`, and access logging, so that nothing writes to the response after hijack, a 101 is the final response for an upgrade, and the proxy copies bytes on the raw connection rather than through the recording response writer. (change)
- `INTENT6` Keep HTTP/2 extended CONNECT (RFC 8441) upgrades working, and HTTP/1.1 upgrades working in detached mode. (change)
- `INTENT7` Add `reverse_proxy` stream metrics: active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once. (change)
- `INTENT8` Document the new options (`stream_detached`, `stream_logs`, `skip_handshake`, log level) in the config surface for both Caddyfile and JSON. (change)
- `INTENT9` Add integration tests in `caddytest/integration` using a real server and real clients covering: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT10` Add unit tests for each piece of code changed. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestDetachedStreamFollowsItsUpstream` (covered, probe verified) | committed |
| ACC2 | behavior | T1 | `modules/caddyhttp/reverseproxy/stream_options_test.go::TestStreamOptionsCaddyfile` (covered, probe verified) | committed |
| ACC3 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamIsClosedAfterCloseDelay` (covered, probe verified) | committed |
| ACC4 | behavior | T1 | `caddytest/integration/stream_upgrade_test.go::TestStreamLogsSkipHandshake` (covered, probe verified) | committed |
| ACC5 | behavior | T1 | `caddytest/integration/stream_upgrade_test.go::TestDetachedStreamCloseLogAfterReload` (covered, probe verified) | committed |
| ACC6 | behavior | T1 | `modules/caddyhttp/responsewriter_test.go::TestResponseRecorderNoWritesAfterHijack` (covered, probe verified) | committed |
| ACC7 | behavior | T1 | `caddytest/integration/stream_upgrade_test.go::TestUpgradeThroughIntercept` (covered, probe verified) | committed |
| ACC8 | behavior | T1 | `caddytest/integration/stream_upgrade_test.go::TestExtendedConnectWebsocketOverH2` (covered, probe verified) | committed |
| ACC9 | behavior | T1 | `modules/caddyhttp/reverseproxy/metrics_test.go::TestStreamMetricsAccountEachStreamOnce` (covered, probe verified) | committed |
| ACC10 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/stream_reload_test.go, caddytest/integration/stream_upgrade_test.go, modules/caddyhttp/encode/encode_test.go, modules/caddyhttp/intercept/intercept_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/stream_options_test.go, modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/streams_test.go | committed |
| ACC11 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamIsClosedOnReload` (covered, probe verified) | committed |
| ACC12 | behavior | T1 | `modules/caddyhttp/reverseproxy/streams_test.go::TestStreamRegistryOrphans` (covered, probe verified) | committed |

### Tasks

- `T1` Order: (1) in caddyhttp, make responseRecorder treat 101 as final/unbuffered and no-op writes after Hijack; add the Server accessor for access loggers; (2) same hijack/101 hardening in encode and intercept; (3) add reverseproxy streams registry (package-level, keyed by upstream dial, live-upstream refcount maintained in Provision/Cleanup), StreamDetached, StreamLogs{SkipHandshake, Level} fields, Caddyfile parsing, and cleanupConnections branching (detached: close only streams whose upstream is no longer live; default path unchanged); (4) rewrite handleUpgradeResponse to account each stream once via a single finalizer (metrics, close log to access loggers) and set LogSkipVar when skip_handshake; (5) add metrics; (6) tests and caddyfile_adapt fixture, then docs in comments. (1 attempt(s), commit `50c9d7ae19`)
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/streams.go:140`: Streams to dynamic upstreams (keyed by di.String(), never in the live set) are never closed by a reload in detached mode; documented in the field comment but not tested.
  - reviewer note [pass 4] `modules/caddyhttp/reverseproxy/streams.go:143`: removeLive deletes r.open[u] only for orphaned upstreams, and remove() leaves empty inner maps behind for live upstreams, a small leak that is harmless in practice.

### Cost (client-side estimate)

- sessions: 4; turns: 182; USD: 4.29; attempts: 1

_Generated by aih run `20261016T033330Z-673f8e`._
