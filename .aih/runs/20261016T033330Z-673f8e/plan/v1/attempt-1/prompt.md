You are planning a change in this repository. You can read files but not edit them or run
commands. Another session will implement your plan, one task at a time, each in a fresh context, and a
harness will check the result against it mechanically. Explore the repository as you see fit;
only your final JSON answer is used.

## Request

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

## Intents (use these ids exactly)

- `INTENT1` (stream-detached-option): Add an opt-in `reverse_proxy` setting `stream_detached` (Caddyfile and JSON) that detaches upgraded streams from the config lifecycle so they survive a reload, closing on reload only streams whose upstream was removed from the new config.
- `INTENT2` (default-close-on-reload): Keep the default (stream_detached off) as today: upgraded streams are closed on reload, honouring the existing close-delay behaviour.
- `INTENT3` (stream-logs-block): Add a `stream_logs` block (Caddyfile and JSON) with a `skip_handshake` option that suppresses access logging of the upgrade handshake (default: logged).
- `INTENT4` (stream-close-logging): Emit stream close logs that are available in the access log via `logger_name access`, with a configurable log level defaulting to DEBUG.
- `INTENT5` (hijack-safe-handlers): Make upgraded streams work through `encode`, `intercept` with `handle_response`, and access logging, so that nothing writes to the response after hijack, a 101 is the final response for an upgrade, and the proxy copies bytes on the raw connection rather than through the recording response writer.
- `INTENT6` (h2-extended-connect): Keep HTTP/2 extended CONNECT (RFC 8441) upgrades working, and HTTP/1.1 upgrades working in detached mode.
- `INTENT7` (stream-metrics): Add `reverse_proxy` stream metrics: active streams, streams closed by result, stream duration, and bytes to and from the backend, with each stream accounted exactly once.
- `INTENT8` (document-options): Document the new options (`stream_detached`, `stream_logs`, `skip_handshake`, log level) in the config surface for both Caddyfile and JSON.
- `INTENT9` (integration-tests): Add integration tests in `caddytest/integration` using a real server and real clients covering: streams surviving and being closed across reloads in each mode, extended CONNECT over h2, upgrades through `encode` and `intercept`, and detached streams with access logging.
- `INTENT10` (unit-tests): Add unit tests for each piece of code changed.

## Constraints

- The default behaviour (upgraded streams closed on reload, existing close-delay honoured) must stay unchanged.
- Existing test suites must stay green.
- Do not change unrelated behaviour.
- stream_detached is opt-in; stream handshake logging default is logged; stream close log level default is DEBUG.

## Repository facts

- Test files are those matching: **/*_test.go, caddytest/integration/caddyfile_adapt/*.caddyfiletest
- Tests are run by the harness, not by the implementer.


## What the plan must contain

Return JSON matching the schema you were given.

- `intents`: every intent id above, exactly once, with a `resolution`:
  - `change`: something must change. `evidence` is optional but welcome.
  - `already_satisfied`: the repository already does it. Requires `evidence` that proves it.
  - `out_of_scope`: you propose not doing it. Requires a `rationale`; a human will decide.
- `evidence`: `path` relative to the repository root, `start`/`end` line numbers (1-based,
  inclusive) and a short `quote` that appears literally within those lines. The harness checks
  the file, the range and the quote; a wrong quote fails the plan.
- `criteria`: acceptance criteria, each tied to an `intent`. `kind` is `behavior` when a test
  can prove it, `other` otherwise (documentation, naming). Write them so that a test can be
  named for each `behavior` criterion.
- `tasks`: the units of work. **A task is an increment that could be released on its own: the
  change that would ship as one pull request.** A request that ships as one PR is ONE task, however
  many files it touches; splitting it into steps costs a fresh context per step, loses the design
  decisions between them and produces nothing releasable in between. Use several tasks only when
  the request contains pieces that could be merged and released independently of each other
  (then `depends_on` says which must land first). Never split by layer, by file or by phase.
  - `files`: every file the task may touch, with `role` (`test` if it matches the test globs,
    `production` otherwise) and `action`. The implementer is blocked from touching anything
    else. Declaring a file you will not need is cheap; forgetting one costs a replan.
  - `tests`: the test files (existing or to be created) that prove this task's criteria. Every
    `behavior` criterion must be covered by at least one task with at least one test. **Every
    path in `tests` must also appear in that task's `files`** (with `role: test`); a test path
    missing from `files` fails the plan.
  - `criteria`: the criteria keys this task satisfies.
  - `depends_on`: keys of tasks that must be committed first. No cycles. Independent tasks must
    not share a production file.
  - Existing tests that assert the old behaviour must be listed in the `files` (role `test`) and
    `tests` of the task that changes that behaviour; the implementer is blocked from touching any
    test file the task does not declare.
  - In the task `summary`, state the order of work the implementer should follow when it matters
    (add the new thing, migrate its callers and their tests, deprecate or remove the old thing
    last: many suites turn warnings into errors).
- `summary` on the plan and on each task: one or two sentences for a human reviewer.

Do not include anything the user did not ask for (docs, refactors, extra features) unless an
intent requires it; if you think it is needed, say so in the plan summary instead.
