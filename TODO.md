# TODO

Possible, interesting, and deliberately deferred engineering or operational
improvements. Completed behaviour belongs in `CHANGELOG.md`; this file is the
forward-looking backlog.

- [ ] Record collect progress so a crashed `--collectonly` run can resume.
  `--collectafter` already starts listing after a given key, but nothing
  records the last key listed, so after a crash the operator cannot tell what
  to pass and restarts from the beginning. Write the last listed key
  durably (for example in the run log) at each progress step.
- [ ] Add an opt-in `dev_cluster` GCS end-to-end test. It must require explicit
  environment configuration, create an isolated tiered-policy fixture, run
  `collect → dry-run → delete → verify`, clean up ClickHouse and object-storage
  fixtures, and remain excluded from CI.
- [ ] Require the `Container / test` GitHub Actions check before pull-request
  merges in the repository branch-protection settings. `master` has no branch
  protection or ruleset today; this needs a repository admin.
- [ ] Surface a DDL `TIMEOUT_EXCEEDED` (159) instead of `IncompleteRead`. The
  server sends `200 OK` before a distributed DDL times out, so the HTTP client
  hides the cause. The Replicated-database preflight covers a replica that is
  already down; a replica that dies after the preflight still surfaces as
  `IncompleteRead` after `distributed_ddl_task_timeout`.
- [ ] Consider a `TTL` on the run-log table. Volume is a few hundred rows per
  run so it is not urgent, but it grows without bound across many cleanups.
  The run log is the durable record of what was deleted, so a TTL works
  against its purpose: keep it off by default, or make it long (a year or
  more).
