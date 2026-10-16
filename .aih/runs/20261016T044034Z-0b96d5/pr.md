## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

### Plan

Single PR: add stream_detached and stream_logs to reverse_proxy, per-upstream stream tracking so reloads close only streams whose upstream was removed, hijack-safe response writers (recorder/encode/intercept), stream metrics and close logs, with unit and integration tests. Docs are the JSON struct-field doc comments and Caddyfile syntax comments, since the repo has no separate docs tree.

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

- `INTENT1` Add an opt-in `stream_detached` setting on `reverse_proxy` so upgraded streams detach from the config lifecycle and survive a reload, and on reload only streams whose upstream was removed from the new config are closed. (change)
- `INTENT2` With `stream_detached` unset, upgraded streams are still closed on reload and the existing `stream_close_delay` behaviour is unchanged. (already_satisfied)
- `INTENT3` Add a `stream_logs` block with a `skip_handshake` option (default false, so the handshake is logged) that suppresses access logging of the upgrade handshake. (change)
- `INTENT4` Stream close logs are available in the access log via `logger_name access`, with a configurable log level that defaults to DEBUG. (change)
- `INTENT5` After a connection is hijacked nothing writes to the response again, a 101 is the final response for an upgrade, and the proxy copies bytes on the raw connection rather than through the recording response writer. (change)
- `INTENT6` Upgraded streams keep working through the other handlers of a route: `encode`, `intercept` with `handle_response`, and access logging. (change)
- `INTENT7` HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, as do HTTP/1.1 upgrades in detached mode. (change)
- `INTENT8` Add `reverse_proxy` metrics for active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once. (change)
- `INTENT9` Expose the new options in both Caddyfile and JSON config and document them. (change)
- `INTENT10` Add integration tests in `caddytest/integration` using a real server and real clients, covering streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT11` Add unit tests for the pieces that are changed. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedSurvivesReload` (covered, probe verified / base does_not_build) | committed |
| ACC2 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedClosesRemovedUpstream` (covered, probe verified / base does_not_build) | committed |
| ACC3 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDefaultClosedOnReload` (covered, probe verified / base does_not_build) | committed |
| ACC4 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamLogsSkipHandshake` (covered, probe verified / base does_not_build) | committed |
| ACC5 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamCloseLogAccessLogger` (covered, probe verified / base does_not_build) | committed |
| ACC6 | behavior | T1 | `modules/caddyhttp/hijack_test.go::TestResponseRecorderNoWriteAfterHijack` (covered, probe verified / base does_not_build) | committed |
| ACC7 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestUpgradeThroughEncodeAndIntercept` (covered, probe verified / base does_not_build) | committed |
| ACC8 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedHTTP11Upgrade` (covered, probe verified / base does_not_build) | committed |
| ACC9 | behavior | T1 | `modules/caddyhttp/reverseproxy/metrics_test.go::TestStreamMetricsAccountedOnce` (covered, probe verified / base does_not_build) | committed |
| ACC10 | behavior | T1 | `modules/caddyhttp/reverseproxy/stream_detached_test.go::TestStreamOptionsCaddyfile` (covered, probe verified / base does_not_build) | committed |
| ACC11 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/stream_reload_test.go, modules/caddyhttp/hijack_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | committed |
| ACC12 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/stream_reload_test.go, modules/caddyhttp/hijack_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | committed |
| ACC13 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/stream_reload_test.go, modules/caddyhttp/hijack_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | committed |

### Tasks

- `T1` Order: (1) add the config fields (StreamDetached, StreamLogs) and the Caddyfile and JSON parsing; (2) add the hijack-safety guards in the response recorder, encode, intercept and access logging; (3) add the per-upstream stream registry, detached Cleanup semantics, stream close accounting and the access-logger close log in handleUpgradeResponse; (4) add the metrics; (5) write the unit tests, the adapt test and the integration tests. The default close path (closeConnections, cleanupConnections, stream_close_delay) must not change when stream_detached is unset. (2 attempt(s), commit `e835145f2a`)
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/streaming.go:153`: Detached streams to dynamic upstreams are keyed by an empty upstream string and never closed on removal, and no test covers that case or stream_timeout combined with detached; the first is documented in the field comment.
  - reviewer note [pass 4] `modules/caddyhttp/reverseproxy/streaming.go:337`: StreamLogs.skipHandshake() and the nil-receiver handling are thin wrappers that could be inlined.

### Gate failures along the way

- review.approved (T1, attempt 1): review.changes_requested
- tests.selected_green (T1, attempt 1): tests.selected_failed

### Cost (client-side estimate)

- sessions: 8; turns: 192; USD: 5.62; attempts: 2

_Generated by aih run `20261016T044034Z-0b96d5`._
