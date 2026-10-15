## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

### Plan

One releasable change: opt-in detached upgraded streams in reverse_proxy, with stream logs, hijack-safety fixes in the response-writer chain, stream metrics, config/docs and tests. Docs are limited to the Caddyfile doc comment and JSON struct field comments, as the intents require.

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

- `INTENT1` Add an opt-in `stream_detached` setting to `reverse_proxy` so that upgraded streams (HTTP/1.1 Upgrade, HTTP/2 extended CONNECT) are detached from the config lifecycle and survive a reload. (change)
- `INTENT2` In detached mode, on reload close only the streams whose upstream was removed from the new config and leave all other streams running. (change)
- `INTENT3` Without `stream_detached`, upgraded streams are still closed on reload and still honour the existing close-delay behaviour. (change)
- `INTENT4` Add a `stream_logs` block with a `skip_handshake` option that suppresses access logging of the upgrade handshake (default: the handshake is logged). (change)
- `INTENT5` Make stream close logs available in the access log through `logger_name access`, with a configurable log level that defaults to DEBUG. (change)
- `INTENT6` After a connection is hijacked nothing may write to the response again; a 101 is the final response for an upgrade; the proxy copies bytes on the raw connection and not through the recording response writer. (change)
- `INTENT7` Upgraded streams keep working through the other handlers of a route: `encode`, `intercept` with `handle_response`, and access logging. (change)
- `INTENT8` HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, and HTTP/1.1 upgrades keep working in detached mode. (change)
- `INTENT9` Add reverse_proxy stream metrics for active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once. (change)
- `INTENT10` Expose the new options in both Caddyfile and JSON config and document them. (change)
- `INTENT11` Add integration tests in `caddytest/integration` that use a real server and real clients: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT12` Add unit tests for the pieces that change. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamDetachedSurvivesReload` (covered) | committed |
| ACC2 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamDetachedClosesRemovedUpstream` (covered) | committed |
| ACC3 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamCloseDelayOnReload` (covered) | committed |
| ACC4 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamDetachedAccessLogs` (covered) | committed |
| ACC5 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamDetachedAccessLogs` (covered) | committed |
| ACC6 | behavior | T1 | `modules/caddyhttp/responsewriter_test.go::TestResponseRecorderAfterHijack` (covered) | committed |
| ACC7 | behavior | T1 | `caddytest/integration/intercept_test.go::TestInterceptUpgrade` (covered) | committed |
| ACC8 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamExtendedConnectH2` (covered) | committed |
| ACC9 | behavior | T1 | `modules/caddyhttp/reverseproxy/stream_detached_test.go::TestStreamAccountExactlyOnce` (covered) | committed |
| ACC10 | other | T1 | caddytest/integration/intercept_test.go, caddytest/integration/stream_detached_test.go, caddytest/integration/stream_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | committed |
| ACC11 | behavior | T1 | `caddytest/integration/stream_detached_test.go::TestStreamDetachedSurvivesReload` (covered) | committed |
| ACC12 | behavior | T1 | `modules/caddyhttp/reverseproxy/stream_detached_test.go::TestStreamRegistry` (covered) | committed |

### Tasks

- `T1` Order: (1) add config fields and Caddyfile parsing plus docs; (2) add stream metrics and a stream-logs/access-logger path; (3) add a global detached-stream registry keyed by upstream, used in handleUpgradeResponse and Cleanup/reload logic, leaving the default path unchanged; (4) fix hijack safety in caddyhttp responseRecorder/encode/intercept (no writes after hijack, copy on raw conn); (5) write unit then integration tests, updating existing streaming tests and adapt fixtures if they break. (1 attempt(s), commit `4277379953`)
  - reviewer note [pass 2] `caddytest/integration/stream_detached_test.go:0`: No integration test covers an h2 extended CONNECT stream with stream_detached across a reload, or detached streams with stream_close_delay set. The doc comment says the delay does not apply and that h2 streams keep the request.
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/streaming.go:520`: A stream whose upgrade finishes after the old handler's Cleanup has run is registered too late and is never closed on removal of its upstream. No test covers this reload race.
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/stream_detached_test.go:0`: No end-to-end test for stream_timeout expiring on a detached stream, or for stream_logs with JSON config. Both are covered only at unit level.
  - reviewer note [pass 4] `modules/caddyhttp/reverseproxy/streaming.go:538`: streamRegistry.count is used only by tests.
  - reviewer note [pass 4] `modules/caddyhttp/intercept/intercept.go:0`: intercept.go is declared for the task but unchanged. The intercept 101 handling relies entirely on the responseRecorder change, so the declaration is unused.

### Cost (client-side estimate)

- sessions: 4; turns: 165; USD: 4.36; attempts: 1

_Generated by aih run `20261015T203734Z-b8ae6d`._
