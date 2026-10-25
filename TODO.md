# TODO

Possible, interesting, and deliberately deferred engineering or operational
improvements. Completed behaviour belongs in `CHANGELOG.md`; this file is the
forward-looking backlog.

- [ ] In a separate documentation PR, document every CLI parameter: its purpose,
  accepted values, default, aliases, exact `S3GC_*` environment variable name,
  and interactions or constraints. Prefer existing kebab-case aliases in
  examples, explain CLI/environment/configuration precedence, and include
  examples for collect, dry-run, delete and verify. Keep existing option names
  and aliases; a naming overhaul is unnecessary.
- [ ] Add resumable `--collectonly` collection checkpoints. A crash currently
  restarts listing from the beginning; `--collectafter` would allow a large
  bucket collection to resume safely.
- [ ] Add an opt-in `dev_cluster` GCS end-to-end test. It must require explicit
  environment configuration, create an isolated tiered-policy fixture, run
  `collect → dry-run → delete → verify`, clean up ClickHouse and object-storage
  fixtures, and remain excluded from CI.
- [ ] Require the `Container / test` GitHub Actions check before pull-request
  merges in the repository branch-protection settings.
- [ ] Surface a DDL `TIMEOUT_EXCEEDED` (159) instead of `IncompleteRead`. The
  server sends `200 OK` before a distributed DDL times out, so the HTTP client
  hides the cause. The Replicated-database preflight covers a replica that is
  already down; a replica that dies after the preflight still surfaces as
  `IncompleteRead` after `distributed_ddl_task_timeout`.
- [ ] Consider a `TTL` on the run-log table. Volume is a few hundred rows per
  run so it is not urgent, but it grows without bound across many cleanups.
- [ ] Drop the redundant top-level `preflight_cluster()` call in `do_use()`.
  Since the per-sample re-check landed there are two call sites, and the
  top-level one only buys failing about one query earlier. It also makes the
  M7 mutation ("remove the preflight call") survive the suite, because removing
  either site alone is covered by the other. Removing site A and retargeting M7
  at the per-sample call restores a clean 11/11. Not a defect — removing both
  sites still fails two tests.
