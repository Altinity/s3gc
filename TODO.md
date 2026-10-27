# TODO

Deferred engineering and operational work. Record completed changes in
`CHANGELOG.md` and remove or update the item here.

- [ ] Record the last listed key so an interrupted collect can resume.
  `--collect-after` can start after a key, but collect does not save that key.
  Write it durably at each progress step, for example in the run log.
- [ ] Add an opt-in `dev_cluster` GCS end-to-end test. It must require explicit
  environment configuration, create an isolated tiered-policy fixture, run
  `collect → dry-run → delete → verify`, clean up ClickHouse and object-storage
  fixtures, and remain excluded from CI.
- [ ] Have a repository admin require the `Container / test` GitHub Actions
  check before pull-request merges. This item was recorded when `master` had
  no branch protection or ruleset. Verify the current settings before changing them.
- [ ] Surface a DDL `TIMEOUT_EXCEEDED` (159) instead of `IncompleteRead`. The
  server sends `200 OK` before a distributed DDL times out, so the HTTP client
  hides the cause. The Replicated-database preflight catches a replica already
  reported down. A replica that dies after the check can still cause
  `IncompleteRead` after `distributed_ddl_task_timeout`.
- [ ] Consider a run-log `TTL`. Each run adds a few hundred rows, and the table
  grows across cleanups. It is the durable deletion record, so keep any TTL
  disabled by default or retain at least a year of history.
