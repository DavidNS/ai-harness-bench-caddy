## Harden upgraded-stream handling in `reverse_proxy` across config reloads (caddyserver/caddy

> **Run ABORTED.** This branch holds what was committed before the run stopped, plus the run's artifacts. It is evidence, not a finished change.
>
> `run.aborted`: review.changes_requested: the reviewer requests 11 change(s): `ctx   caddy.Context` is misaligned with `logger`/`events` below it (the original was aligned), so gofmt and golangci-lint will fail; run gofmt.

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

- `INTENT1` Add an opt-in reverse_proxy setting `stream_detached` so that, when set, upgraded streams are detached from the config lifecycle, survive a reload, and on reload only streams whose upstream was removed from the new config are closed. (change)
- `INTENT2` Keep the default (stream_detached unset) behaviour unchanged: upgraded streams are closed on reload, honouring the existing close-delay behaviour. (change)
- `INTENT3` Add a `stream_logs` block with a `skip_handshake` option (default: handshake logged) that suppresses access logging of the upgrade handshake, and emit stream close logs available in the access log via `logger_name access`, with a configurable log level defaulting to DEBUG. (change)
- `INTENT4` Ensure upgraded streams work through other handlers in the route (`encode`, `intercept` with `handle_response`, access logging): after a connection is hijacked nothing writes to the response again, 101 is treated as the final response, and the proxy copies bytes on the raw connection rather than through the recording response writer. (change)
- `INTENT5` Keep HTTP/2 extended CONNECT (RFC 8441) upgrades working, and HTTP/1.1 upgrades working in detached mode. (change)
- `INTENT6` Add reverse_proxy stream metrics for active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once. (change)
- `INTENT7` Expose the new options in both Caddyfile and JSON config and document them. (change)
- `INTENT8` Add integration tests in caddytest/integration running a real server and real clients covering: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging. (change)
- `INTENT9` Add unit tests for the pieces changed. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC12 | behavior | T2 | `caddytest/integration/stream_test.go::TestStreamDetachedSurvivesReload` (covered) | pending |
| ACC13 | behavior | T2 | `caddytest/integration/stream_test.go::TestStreamDetachedClosesRemovedUpstream` (covered) | pending |
| ACC14 | behavior | T2 | `caddytest/integration/stream_test.go::TestStreamDefaultClosedOnReload` (covered) | pending |
| ACC15 | behavior | T2 | `caddytest/integration/stream_test.go::TestDetachedStreamAccessLog` (covered) | pending |
| ACC16 | behavior | T2 | `modules/caddyhttp/responsewriter_test.go::TestResponseRecorderNoWriteAfterHijack` (covered) | pending |
| ACC17 | behavior | T2 | `caddytest/integration/intercept_test.go::TestUpgradeThroughIntercept` (covered) | pending |
| ACC18 | behavior | T2 | `caddytest/integration/stream_test.go::TestExtendedConnectH2` (covered) | pending |
| ACC19 | behavior | T2 | `modules/caddyhttp/reverseproxy/streaming_test.go::TestStreamFinishOnce` (covered) | pending |
| ACC20 | behavior | T2 | `modules/caddyhttp/reverseproxy/caddyfile_test.go::TestUnmarshalCaddyfileStreamDetached` (covered) | pending |
| ACC21 | other | T2 | caddytest/integration/intercept_test.go, caddytest/integration/stream_test.go, modules/caddyhttp/encode/encode_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/reverseproxy/caddyfile_test.go, modules/caddyhttp/reverseproxy/streaming_test.go | pending |
| ACC22 | behavior | T2 | `modules/caddyhttp/reverseproxy/streaming_test.go::TestDetachedRegistryPrune` (covered) | pending |

### Tasks

- `T2` Add stream_detached and stream_logs to reverse_proxy with a process-wide registry of detached streams that is pruned on reload by upstream. Add stream metrics with once-only accounting. Make response writers hijack-safe (no writes after hijack, 101 final, raw-conn copy). Caddyfile parsing is tested by a Go unit test in the reverseproxy package; no .caddyfiletest data file is added. Order of work: (1) config fields and Caddyfile parsing; (2) registry and metrics in streaming.go/metrics.go; (3) wire Provision/Cleanup; (4) writer fixes in responsewriter/encode/intercept and logging; (5) unit tests; (6) integration tests. (1 attempt(s), commit ``)
  - reviewer note [pass 2] `modules/caddyhttp/reverseproxy/reverseproxy.go:206`: The documented rule that streams to dynamic upstreams are never closed by a reload (empty upstream key) has no test.
  - reviewer note [pass 2] `caddytest/integration/stream_test.go:0`: No integration test covers detached streams with stream_timeout, or the shutdown of detached streams when Caddy stops.
  - reviewer note [pass 4] `modules/caddyhttp/responsewriter_test.go:225`: The `equalInts` helper duplicates `slices.Equal`.

### Gate failures along the way

- change.implemented (T1, attempt 1): change.replan_requested
- review.approved (T2, attempt 1): review.changes_requested
- review.approved (T2, attempt 1): review.changes_requested
- review.approved (T2, attempt 1): review.changes_requested

### Cost (client-side estimate)

- sessions: 11; turns: 368; USD: 9.40; attempts: 2

_Generated by aih run `20261014T204522Z-5acf37`._
