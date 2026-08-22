# TODO

Possible, interesting, and deliberately deferred engineering or operational
improvements. Completed behaviour belongs in `CHANGELOG.md`; this file is the
forward-looking backlog.

- [ ] Add resumable `--collectonly` collection checkpoints. A crash currently
  restarts listing from the beginning; `--collectafter` would allow a large
  bucket collection to resume safely.
- [ ] Add an opt-in `dev_cluster` GCS end-to-end test. It must require explicit
  environment configuration, create an isolated tiered-policy fixture, run
  `collect → dry-run → delete → verify`, clean up ClickHouse and object-storage
  fixtures, and remain excluded from CI.
- [ ] Require the `Container / test` GitHub Actions check before pull-request
  merges in the repository branch-protection settings.
- [ ] Quote `--useafter` as a SQL string literal (strict `xfail` in the suite).
- [ ] Consider re-checking cluster topology per sample, not once per run, so a
  replica lost mid-run cannot widen the deletion scope.
- [ ] `print()` output is block-buffered because the image sets no
  `PYTHONUNBUFFERED` and stdout is a pipe under Kubernetes. Logger records are
  flushed per line, but the bare prints — including the closing `s3gc: OK` —
  are lost when `activeDeadlineSeconds` fires and the kubelet sends SIGTERM.
  The durable run log now covers the evidence case; this remains a stdout
  fidelity gap, and it also makes the `dev-automation` phase markers interleave
  wrongly against the shell's unbuffered `echo`.
- [ ] Consider a `TTL` on the run-log table. Volume is a few hundred rows per
  run so it is not urgent, but it grows without bound across many cleanups.
