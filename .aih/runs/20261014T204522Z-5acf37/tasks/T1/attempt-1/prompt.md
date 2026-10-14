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



## Your task: T1 (T1)

Add stream_detached and stream_logs to reverse_proxy with a process-wide registry of detached streams that is pruned on reload by upstream. Add stream metrics with once-only accounting. Make response writers hijack-safe (no writes after hijack, 101 final, raw-conn copy). Order of work: (1) config fields and Caddyfile parsing; (2) registry and metrics in streaming.go/metrics.go; (3) wire Provision/Cleanup; (4) writer fixes in responsewriter/encode/intercept and logging; (5) unit tests; (6) integration tests.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: With stream_detached set, an established WebSocket stream stays open and keeps echoing after a config reload that keeps its upstream. TestStreamDetachedSurvivesReload.
- ACC2 [behavior]: With stream_detached set, a reload that removes a stream's upstream closes only the streams of that upstream; streams on retained upstreams stay open. TestStreamDetachedClosesRemovedUpstream.
- ACC3 [behavior]: Without stream_detached, a reload closes upgraded streams, immediately or after stream_close_delay. TestStreamDefaultClosedOnReload.
- ACC4 [behavior]: stream_logs parses in the Caddyfile and JSON (skip_handshake, level, default DEBUG). skip_handshake suppresses the handshake access log entry, and the stream close log is emitted with logger_name access. TestStreamLogsConfig and TestDetachedStreamAccessLog.
- ACC5 [behavior]: After Hijack, the response recorder and the encode writer drop any WriteHeader or Write; a 101 is treated as final. TestResponseRecorderNoWriteAfterHijack, TestEncodeUpgradeFinal101.
- ACC6 [behavior]: A WebSocket upgrade works through the encode handler and through intercept with handle_response, and the proxy copies on the raw connection, not through the recorder's hijackedConn. TestUpgradeThroughEncode, TestUpgradeThroughIntercept.
- ACC7 [behavior]: HTTP/2 extended CONNECT WebSocket works over h2 with a real h2 client, and HTTP/1.1 upgrades work in detached mode. TestExtendedConnectH2, TestDetachedHTTP11Upgrade.
- ACC8 [behavior]: Metrics report active streams, closed streams by result, duration and bytes in each direction. Each stream is counted exactly once even when several close paths race. TestStreamMetricsAccountedOnce.
- ACC9 [behavior]: Caddyfile stream_detached and stream_logs { skip_handshake; level } adapt to the expected JSON (caddyfiletest).
- ACC10 [other]: Doc comments for JSON fields and the Caddyfile syntax block describe stream_detached and stream_logs.
- ACC11 [behavior]: Unit tests cover the detached registry and its upstream-removal pruning, and the stream finish-once accounting. TestDetachedRegistryPrune, TestStreamFinishOnce.

Files you may edit (anything else is blocked):
- modules/caddyhttp/reverseproxy/reverseproxy.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming.go (production, modify)
- modules/caddyhttp/reverseproxy/caddyfile.go (production, modify)
- modules/caddyhttp/reverseproxy/metrics.go (production, modify)
- modules/caddyhttp/responsewriter.go (production, modify)
- modules/caddyhttp/encode/encode.go (production, modify)
- modules/caddyhttp/intercept/intercept.go (production, modify)
- modules/caddyhttp/server.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming_test.go (test, modify)
- modules/caddyhttp/responsewriter_test.go (test, modify)
- modules/caddyhttp/encode/encode_test.go (test, modify)
- caddytest/integration/stream_test.go (test, modify)
- caddytest/integration/intercept_test.go (test, modify)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test, create)

Tests that must pass when you finish: modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/responsewriter_test.go, modules/caddyhttp/encode/encode_test.go, caddytest/integration/stream_test.go, caddytest/integration/intercept_test.go, caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest

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
