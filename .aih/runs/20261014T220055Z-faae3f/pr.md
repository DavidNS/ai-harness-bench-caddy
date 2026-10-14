## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

> **Run ABORTED.** This branch holds what was committed before the run stopped, plus the run's artifacts. It is evidence, not a finished change.
>
> `run.aborted`: review.changes_requested: the reviewer requests 2 change(s): TestMain uses syscall.Exec, which is undefined on Windows, so the integration package fails to compile there (CI runs Windows). Because the tests skip when the exec fails, the h2 extended CONNECT criterion (ACC8) is also only met on Linux and macOS. Use a build-tagged helper, or set the GODEBUG setting in code, instead.

### Plan

The whole request ships as one PR in the reverseproxy module, with small supporting edits in caddyhttp (recorder, server logging), encode and intercept. Docs live in Go doc comments, because the repo has no separate docs files for these options. I did not trace the access-log skip mechanism or how `logRequest` handles hijacked connections, so the implementer should read those first.

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

- `INTENT1` Add an opt-in `stream_detached` setting to `reverse_proxy`: when set, upgraded streams are detached from the config lifecycle and survive a reload, and on reload only streams whose upstream was removed from the new config are closed. (change)
- `INTENT2` With `stream_detached` unset, upgraded streams are still closed on reload, honouring the existing close-delay behaviour, as today. (change)
- `INTENT3` Add a `stream_logs` block with a `skip_handshake` option that suppresses access logging of the upgrade handshake (default: the handshake is logged). (change)
- `INTENT4` Make stream close logs available in the access log via `logger_name access`, with a configurable log level that defaults to DEBUG. (change)
- `INTENT5` Upgraded streams must keep working through the other handlers of a route: `encode`, `intercept` with `handle_response`, and access logging. (change)
- `INTENT6` Once a connection is hijacked nothing may write to the response again. A 101 is the final response for an upgrade. The proxy copies bytes on the raw connection, not through the recording response writer. (change)
- `INTENT7` HTTP/2 extended CONNECT (RFC 8441) upgrades keep working, and so do HTTP/1.1 upgrades in detached mode. (change)
- `INTENT8` Add `reverse_proxy` stream metrics: active streams, streams closed by result, stream duration, and bytes to and from the backend. Each stream is accounted exactly once. (change)
- `INTENT9` Expose the new options in both Caddyfile and JSON config, and document them. (change)
- `INTENT10` Add integration tests in `caddytest/integration` that run a real server and real clients, covering: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT11` Add unit tests for the pieces that are changed. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedSurvivesReload` (covered) | pending |
| ACC2 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedClosesOnlyRemovedUpstream` (covered) | pending |
| ACC3 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamCloseDelayKeepsStreamUntilDelay` (covered) | pending |
| ACC4 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamLogsSkipHandshakeAndLevel` (covered) | pending |
| ACC5 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamLogsDefault` (covered) | pending |
| ACC6 | behavior | T1 | `caddytest/integration/intercept_test.go::TestInterceptUpgradeThroughReverseProxy` (covered) | pending |
| ACC7 | behavior | T1 | `modules/caddyhttp/responsewriter_test.go::TestResponseRecorderNoWritesAfterHijack` (covered) | pending |
| ACC8 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamExtendedConnectOverH2` (partial) | pending |
| ACC9 | behavior | T1 | `modules/caddyhttp/reverseproxy/metrics_test.go::TestStreamAccounting` (covered) | pending |
| ACC10 | behavior | T1 | `modules/caddyhttp/reverseproxy/caddyfile_test.go::TestUnmarshalCaddyfileStreamOptions` (covered) | pending |
| ACC11 | other | T1 | caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest, caddytest/integration/intercept_test.go, caddytest/integration/stream_reload_test.go, caddytest/integration/stream_test.go, modules/caddyhttp/encode/encode_conformance_test.go, modules/caddyhttp/intercept/intercept_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/reverseproxy/caddyfile_test.go, modules/caddyhttp/reverseproxy/hopheaders_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/streams_test.go | pending |
| ACC12 | behavior | T1 | `caddytest/integration/stream_reload_test.go::TestStreamDetachedWithAccessLogging` (covered) | pending |
| ACC13 | behavior | T1 | `modules/caddyhttp/encode/encode_conformance_test.go::TestEncodeUpgradeAndHijack` (covered) | pending |

### Tasks

- `T1` Implement the whole feature as one PR. Order: (1) add StreamDetached/StreamLogs fields, Caddyfile parsing and docs; (2) add metrics and a once-only per-stream accounting struct; (3) add a global detached-stream registry keyed by upstream, with Cleanup closing only streams whose upstream is gone from the new config, leaving the non-detached path and close-delay untouched; (4) make 101 final and writes after hijack no-ops in the caddyhttp recorder and encode, hijack on the raw connection in handleUpgradeResponse, and send close logs to the access logger with skip_handshake in server logging; (5) fix intercept for upgrades; (6) update existing tests and add unit, caddyfile-adapt and integration tests. (2 attempt(s), commit ``)
  - reviewer note [pass 1] `caddytest/integration/stream_reload_test.go:170`: TestStreamNotDetachedClosedOnReload only asserts the close within 5s, so it can't show the stream closes immediately when stream_close_delay is 0.
  - reviewer note [pass 2] `caddytest/integration/stream_reload_test.go:0`: No test covers a stream_timeout expiry or a backend-initiated close producing the right result label in the metrics or close log (integration or unit).
  - reviewer note [pass 3] `modules/caddyhttp/reverseproxy/streaming_test.go:208`: The only skip I found here is the existing GOOS skip in TestHandlerUpgradedStreamHalfClose. I saw no weakened assertions, but I couldn't see the diff for this file.
  - reviewer note [pass 4] `modules/caddyhttp/reverseproxy/streams.go:204`: streamRegistry.count() appears to be used only by tests.

### Gate failures along the way

- tests.selected_green (T1, attempt 1): tests.selected_failed
- review.approved (T1, attempt 2): review.changes_requested
- review.approved (T1, attempt 2): review.changes_requested
- review.approved (T1, attempt 2): review.changes_requested

### Cost (client-side estimate)

- sessions: 9; turns: 250; USD: 6.22; attempts: 2

_Generated by aih run `20261014T220055Z-faae3f`._
