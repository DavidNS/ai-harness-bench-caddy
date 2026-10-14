You are implementing one task of an approved plan inside an isolated worktree. You can read and
edit files. The only command you can run is `aih check <test files...>`, which runs those tests
(or `aih check --build`, which only compiles, when the project has a build step): use it as often
as you need; every other shell command is rejected. When you try to finish, the harness runs the
task's tests again and gives you the result. Edits outside the files listed for this task are
rejected by a hook; do not work around it.

## Original request

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

## Plan summary

The whole request ships as one PR in the reverseproxy module, with small supporting edits in caddyhttp (recorder, server logging), encode and intercept. Docs live in Go doc comments, because the repo has no separate docs files for these options. I did not trace the access-log skip mechanism or how `logRequest` handles hijacked connections, so the implementer should read those first.

## Your task: T1 (T1)

Implement the whole feature as one PR. Order: (1) add StreamDetached/StreamLogs fields, Caddyfile parsing and docs; (2) add metrics and a once-only per-stream accounting struct; (3) add a global detached-stream registry keyed by upstream, with Cleanup closing only streams whose upstream is gone from the new config, leaving the non-detached path and close-delay untouched; (4) make 101 final and writes after hijack no-ops in the caddyhttp recorder and encode, hijack on the raw connection in handleUpgradeResponse, and send close logs to the access logger with skip_handshake in server logging; (5) fix intercept for upgrades; (6) update existing tests and add unit, caddyfile-adapt and integration tests.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: With stream_detached set, an established upgraded stream keeps passing bytes after a config reload that keeps its upstream.
- ACC2 [behavior]: With stream_detached set, a reload that removes the stream's upstream closes that stream while streams to retained upstreams stay open.
- ACC3 [behavior]: Without stream_detached, upgraded streams are closed on reload, immediately when stream_close_delay is 0 and after the delay otherwise.
- ACC4 [behavior]: stream_logs skip_handshake suppresses the access log entry for the upgrade request; by default the handshake is logged.
- ACC5 [behavior]: A stream close log is written to the access logger (logger_name access) at the configured level, DEBUG by default, including with detached streams.
- ACC6 [behavior]: WebSocket/Upgrade streams work end to end through encode, through intercept with handle_response, and with access logging enabled.
- ACC7 [behavior]: After Hijack, the response recorder and encode writer never write header or body to the underlying writer; 101 is treated as a final status; the proxy copies on the raw connection rather than the recorder's wrapper.
- ACC8 [behavior]: An RFC 8441 extended CONNECT websocket over h2 works through the proxy, and an HTTP/1.1 upgrade works in detached mode.
- ACC9 [behavior]: Stream metrics (active gauge, closed-by-result counter, duration histogram, bytes to/from backend) are updated and each stream is counted exactly once regardless of how many close paths fire.
- ACC10 [behavior]: Caddyfile stream_detached and stream_logs { skip_handshake, level } adapt to the JSON fields stream_detached and stream_logs; invalid input errors; defaults are omitted from JSON.
- ACC11 [other]: New options are documented in the struct field comments and the Caddyfile syntax comment in caddyfile.go.
- ACC12 [behavior]: Integration tests exist for reload survive/close in both modes, h2 extended CONNECT, encode, intercept, and detached with access logging.
- ACC13 [behavior]: Unit tests cover the stream registry, metrics accounting, Caddyfile parsing, recorder/encode hijack safety.

Files you may edit (anything else is blocked):
- modules/caddyhttp/reverseproxy/reverseproxy.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming.go (production, modify)
- modules/caddyhttp/reverseproxy/caddyfile.go (production, modify)
- modules/caddyhttp/reverseproxy/metrics.go (production, modify)
- modules/caddyhttp/reverseproxy/streams.go (production, create)
- modules/caddyhttp/responsewriter.go (production, modify)
- modules/caddyhttp/server.go (production, modify)
- modules/caddyhttp/logging.go (production, modify)
- modules/caddyhttp/encode/encode.go (production, modify)
- modules/caddyhttp/intercept/intercept.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming_test.go (test, modify)
- modules/caddyhttp/reverseproxy/hopheaders_test.go (test, modify)
- modules/caddyhttp/reverseproxy/streams_test.go (test, create)
- modules/caddyhttp/reverseproxy/metrics_test.go (test, create)
- modules/caddyhttp/reverseproxy/caddyfile_test.go (test, create)
- modules/caddyhttp/responsewriter_test.go (test, modify)
- modules/caddyhttp/encode/encode_conformance_test.go (test, modify)
- modules/caddyhttp/intercept/intercept_test.go (test, create)
- caddytest/integration/stream_reload_test.go (test, create)
- caddytest/integration/stream_test.go (test, modify)
- caddytest/integration/intercept_test.go (test, modify)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test, create)

Tests that must pass when you finish: modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/hopheaders_test.go, modules/caddyhttp/reverseproxy/streams_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/reverseproxy/caddyfile_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/encode/encode_conformance_test.go, modules/caddyhttp/intercept/intercept_test.go, caddytest/integration/stream_reload_test.go, caddytest/integration/stream_test.go, caddytest/integration/intercept_test.go, caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest

## Rules

- Write or update the tests listed above so that they fail without your production change and
  pass with it. The harness verifies this against the base commit: a test that also passes
  without the change does not count.
- Every branch you add (a new `if`/`else`, an `except`, an early return, a new error status)
  must be exercised by a test in this task's test files before you finish. An independent
  reviewer blocks the task for each new branch without a test; write them first.
- Do not modify existing tests other than the ones listed for this task.
- Do not add features, docs or refactors the task does not ask for.
- If the task cannot be done without touching a file that is not listed, or a criterion is
  impossible as written, do not improvise: stop and return `replan_request` with the reason and the
  files you need. That re-plans instead of failing you.

## Final answer

Return JSON matching the schema you were given: a short `summary`, one `criteria_notes` entry per
criterion with the test that proves it, and `replan_request` (`null` unless you need a re-plan).
