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

Single PR: add stream_detached and stream_logs to reverse_proxy, per-upstream stream tracking so reloads close only streams whose upstream was removed, hijack-safe response writers (recorder/encode/intercept), stream metrics and close logs, with unit and integration tests. Docs are the JSON struct-field doc comments and Caddyfile syntax comments, since the repo has no separate docs tree.

## Your task: T1 (T1)

Order: (1) add the config fields (StreamDetached, StreamLogs) and the Caddyfile and JSON parsing; (2) add the hijack-safety guards in the response recorder, encode, intercept and access logging; (3) add the per-upstream stream registry, detached Cleanup semantics, stream close accounting and the access-logger close log in handleUpgradeResponse; (4) add the metrics; (5) write the unit tests, the adapt test and the integration tests. The default close path (closeConnections, cleanupConnections, stream_close_delay) must not change when stream_detached is unset.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: With stream_detached set, an upgraded stream stays open and keeps echoing after a config reload that keeps its upstream. Test: TestStreamDetachedSurvivesReload.
- ACC2 [behavior]: With stream_detached set, a reload that removes the stream's upstream closes that stream, and streams on retained upstreams stay open. Test: TestStreamDetachedClosesRemovedUpstream.
- ACC3 [behavior]: With stream_detached unset, a reload closes the upgraded stream, honouring stream_close_delay (immediately when 0, after the delay otherwise). Test: TestStreamDefaultClosedOnReload.
- ACC4 [behavior]: stream_logs skip_handshake suppresses the access log entry for the upgrade handshake; the default logs it. Test: TestStreamLogsSkipHandshake.
- ACC5 [behavior]: A stream close entry appears under logger_name access at the configured level, DEBUG by default. Test: TestStreamCloseLogAccessLogger.
- ACC6 [behavior]: After Hijack, the response recorder and encode writers ignore WriteHeader, Write and Flush; the 101 is written once to the raw conn; proxy copying uses the raw conn. Test: TestResponseRecorderNoWriteAfterHijack.
- ACC7 [behavior]: WebSocket-style upgrades succeed and echo through encode, and through intercept with handle_response, with access logging enabled. Test: TestUpgradeThroughEncodeAndIntercept.
- ACC8 [behavior]: An h2 extended CONNECT stream works end to end, and an HTTP/1.1 upgrade works in detached mode. Test: TestExtendedConnectH2 and TestStreamDetachedHTTP11Upgrade.
- ACC9 [behavior]: Metrics record active streams, closed-by-result, duration and bytes to/from the backend, each stream counted exactly once even with concurrent close paths. Test: TestStreamMetricsAccountedOnce.
- ACC10 [behavior]: The Caddyfile parses stream_detached and stream_logs { skip_handshake; level } into the expected JSON; invalid input errors. Test: the reverse_proxy_stream_detached.caddyfiletest adapt test and TestStreamOptionsCaddyfile.
- ACC11 [other]: The new JSON fields and Caddyfile syntax are documented in struct doc comments and the UnmarshalCaddyfile syntax comment.
- ACC12 [other]: Integration tests exist in caddytest/integration using a real server and real clients, covering the scenarios listed in the intent.
- ACC13 [other]: Unit tests accompany each changed package: reverseproxy (registry, metrics, caddyfile) and caddyhttp (hijack guard).

Files you may edit (anything else is blocked):
- modules/caddyhttp/reverseproxy/reverseproxy.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming.go (production, modify)
- modules/caddyhttp/reverseproxy/caddyfile.go (production, modify)
- modules/caddyhttp/reverseproxy/metrics.go (production, modify)
- modules/caddyhttp/responsewriter.go (production, modify)
- modules/caddyhttp/server.go (production, modify)
- modules/caddyhttp/logging.go (production, modify)
- modules/caddyhttp/encode/encode.go (production, modify)
- modules/caddyhttp/intercept/intercept.go (production, modify)
- modules/caddyhttp/reverseproxy/streaming_test.go (test, modify)
- modules/caddyhttp/reverseproxy/stream_detached_test.go (test, create)
- modules/caddyhttp/reverseproxy/metrics_test.go (test, create)
- modules/caddyhttp/hijack_test.go (test, create)
- caddytest/integration/stream_reload_test.go (test, create)
- caddytest/integration/stream_test.go (test, modify)
- caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest (test, create)

Tests that must pass when you finish: modules/caddyhttp/reverseproxy/streaming_test.go, modules/caddyhttp/reverseproxy/stream_detached_test.go, modules/caddyhttp/reverseproxy/metrics_test.go, modules/caddyhttp/hijack_test.go, caddytest/integration/stream_reload_test.go, caddytest/integration/caddyfile_adapt/reverse_proxy_stream_detached.caddyfiletest

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
