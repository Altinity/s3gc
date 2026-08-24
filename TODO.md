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
- [ ] Decide what `USEAGE_HOURS=0` should do. It disables the age window that
  is the only guard against deleting a part between its blob upload and its
  registration in `system.remote_data_paths`. Options: reject it in
  `render.py`, warn loudly in `s3gc.py`, or leave it and document it. Covered
  today only by a test that documents the hazard.
- [ ] Quote `--useafter` as a SQL string literal (strict `xfail` in the suite).
- [ ] Consider re-checking cluster topology per sample, not once per run, so a
  replica lost mid-run cannot widen the deletion scope.
