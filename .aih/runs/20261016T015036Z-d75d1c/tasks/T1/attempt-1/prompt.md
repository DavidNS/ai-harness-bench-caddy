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

One PR: add a process-wide detached-stream registry keyed by upstream, stream_detached and stream_logs options, hijack-safe recorder/encode/intercept, stream metrics, docs, and unit plus integration tests. Docs live in Go doc comments (generated docs), no separate doc files.

## Your task: T1 (T1)

Order: (1) hijack safety in caddyhttp recorder, encode and intercept; (2) new stream registry (detached.go) plus StreamDetached/StreamLogs fields, Provision/Cleanup and handleUpgradeResponse using raw conn; (3) metrics; (4) Caddyfile parsing and docs; (5) unit tests then integration tests. Default non-detached path keeps closeConnections/StreamCloseDelay unchanged.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: With stream_detached, an established Upgrade stream keeps passing data after a config reload; when the reload removes its upstream that stream is closed while streams to retained upstreams stay open.
- ACC2 [behavior]: Caddyfile and JSON stream_detached both set Handler.StreamDetached and survive Caddyfile-to-JSON adaptation.
- ACC3 [behavior]: Without stream_detached an upgraded stream is closed on reload; with stream_close_delay it stays open until the delay elapses, then closes.
- ACC4 [behavior]: stream_logs with skip_handshake, level and logger_name parse from Caddyfile and JSON; defaults are handshake logged and level DEBUG; skip_handshake suppresses the access log entry for the upgrade request.
- ACC5 [behavior]: With logger_name access, a stream close log entry appears in the access log at the configured level, including duration and byte counts.
- ACC6 [behavior]: WebSocket upgrades through encode and through intercept with handle_response work end to end; the client receives exactly one 101 and no writes happen to the response after hijack.
- ACC7 [behavior]: The proxy copies stream bytes on the raw hijacked connection, so the recorder size is not inflated by stream bytes.
- ACC8 [behavior]: HTTP/2 extended CONNECT WebSocket through reverse_proxy transfers data, and an HTTP/1.1 upgrade works in detached mode.
- ACC9 [behavior]: Active-streams, closed-by-result, duration and bytes-to/from-backend metrics are updated, and each stream is counted exactly once even when both directions end, time out or are closed by cleanup.
- ACC10 [other]: Doc comments on the Handler JSON fields and the Caddyfile syntax block describe stream_detached, stream_logs, skip_handshake and the log level.
- ACC11 [behavior]: Integration tests with a real server and clients cover reload survive/close in both modes, h2 extended CONNECT, upgrades via encode and intercept, and detached streams with access logging.
- ACC12 [behavior]: Unit tests cover the registry, config parsing, metrics, recorder hijack behaviour, and encode and intercept post-hijack behaviour; existing suites stay green.

Files you may edit (anything else is blocked):
- modules/caddyhttp/reverseproxy/streaming.go (production, modify)
- modules/caddyhttp/reverseproxy/reverseproxy.go (production, modify)
- modules/caddyhttp/reverseproxy/caddyfile.go (production, modify)
- modules/caddyhttp/reverseproxy/metrics.go (production, modify)
- modules/caddyhttp/reverseproxy/detached.go (production, create)
- modules/caddyhttp/responsewriter.go (production, modify)
- modules/caddyhttp/encode/encode.go (production, modify)
- modules/caddyhttp/intercept/intercept.go (production, modify)
- modules/caddyhttp/server.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming_test.go (test, modify)
- modules/caddyhttp/reverseproxy/detached_test.go (test, create)
- modules/caddyhttp/reverseproxy/caddyfile_test.go (test, create)
- modules/caddyhttp/reverseproxy/metrics_test.go (test, create)
- modules/caddyhttp/responsewriter_test.go (test, modify)
- modules/caddyhttp/encode/encode_test.go (test, modify)
- caddytest/integration/stream_test.go (test, modify)
- caddytest/integration/stream_reload_test.go (test, create)
- caddytest/integration/intercept_test.go (test, modify)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test, create)

Tests that must pass when you finish: modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/detached_test.go, modules/caddyhttp/reverseproxy/caddyfile_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/encode/encode_test.go, caddytest/integration/stream_test.go, caddytest/integration/stream_reload_test.go, caddytest/integration/intercept_test.go, caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest

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
