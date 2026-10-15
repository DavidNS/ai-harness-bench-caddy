You are implementing one task of an approved plan inside an isolated worktree. You can read and
edit files. The only command you can run is `aih check`, as often as you need; every other shell
command is rejected. `aih check <test files...>` runs those tests; `aih check --build` only
compiles (when the project has a build step); `--race` runs them under the race detector and
`--run <pattern>` restricts them to matching tests, when the project supports it; and
`aih check --probe <file>` runs a scratch test file you wrote and deletes it afterwards, so you can
try a hypothesis without it becoming part of the change (name such files `aih_probe*`; they are
the only files you may create outside the task's list). When you try to finish, the harness runs the
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

One releasable change: opt-in detached upgraded streams in reverse_proxy, with stream logs, hijack-safety fixes in the response-writer chain, stream metrics, config/docs and tests. Docs are limited to the Caddyfile doc comment and JSON struct field comments, as the intents require.

## Your task: T1 (T1)

Order: (1) add config fields and Caddyfile parsing plus docs; (2) add stream metrics and a stream-logs/access-logger path; (3) add a global detached-stream registry keyed by upstream, used in handleUpgradeResponse and Cleanup/reload logic, leaving the default path unchanged; (4) fix hijack safety in caddyhttp responseRecorder/encode/intercept (no writes after hijack, copy on raw conn); (5) write unit then integration tests, updating existing streaming tests and adapt fixtures if they break.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: With stream_detached set (Caddyfile and JSON), an established WebSocket/Upgrade stream stays open and passes data after a config reload.
- ACC2 [behavior]: In detached mode, after a reload that removes an upstream, streams to that upstream are closed while streams to retained upstreams keep working.
- ACC3 [behavior]: Without stream_detached, upgraded streams are closed on reload, and with stream_close_delay they stay open until the delay elapses.
- ACC4 [behavior]: stream_logs { skip_handshake } suppresses the access log entry for the upgrade handshake; by default it is logged. Caddyfile parses to JSON correctly.
- ACC5 [behavior]: Stream close is logged via the access logger when logger_name access is configured, at a configurable level defaulting to DEBUG.
- ACC6 [behavior]: After hijack, writes and WriteHeader on the recording writer are no-ops or errors and do not reach the connection; 101 is final; the proxy copies on the raw conn, not the hijackedConn wrapper.
- ACC7 [behavior]: WebSocket upgrades work through encode and through intercept with handle_response in the same route, and with access logging.
- ACC8 [behavior]: HTTP/2 extended CONNECT websocket works end to end over h2, and HTTP/1.1 upgrades work in detached mode.
- ACC9 [behavior]: Metrics for active streams, closed streams by result, duration, and bytes to/from the backend are recorded, each stream exactly once, including on timeout, error and reload close.
- ACC10 [other]: stream_detached and stream_logs appear in JSON struct fields and Caddyfile parsing, with doc comments in caddyfile.go and reverseproxy.go.
- ACC11 [behavior]: Integration tests cover reload survival/closure in both modes, h2 extended CONNECT, encode and intercept upgrades, and detached streams with access logging.
- ACC12 [behavior]: Unit tests cover the detached registry, Caddyfile parsing of new options, metrics accounting, and post-hijack writer behaviour.

Files you may edit (anything else is blocked):
- modules/caddyhttp/reverseproxy/reverseproxy.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming.go (production, modify)
- modules/caddyhttp/reverseproxy/caddyfile.go (production, modify)
- modules/caddyhttp/reverseproxy/metrics.go (production, modify)
- modules/caddyhttp/responsewriter.go (production, modify)
- modules/caddyhttp/server.go (production, modify)
- modules/caddyhttp/encode/encode.go (production, modify)
- modules/caddyhttp/intercept/intercept.go (production, modify)
- modules/caddyhttp/logging.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming_test.go (test, modify)
- modules/caddyhttp/reverseproxy/stream_detached_test.go (test, create)
- modules/caddyhttp/responsewriter_test.go (test, modify)
- caddytest/integration/stream_test.go (test, modify)
- caddytest/integration/stream_detached_test.go (test, create)
- caddytest/integration/intercept_test.go (test, modify)

Tests that must pass when you finish: modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/responsewriter_test.go, caddytest/integration/stream_test.go, caddytest/integration/stream_detached_test.go, caddytest/integration/intercept_test.go

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
