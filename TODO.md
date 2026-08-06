# TODO

Unshipped engineering and operational follow-ups. Completed behaviour belongs
in `CHANGELOG.md`; this file is the forward-looking backlog.

- [ ] Add resumable `--collectonly` collection checkpoints. A crash currently
  restarts listing from the beginning; `--collectafter` would allow a large
  bucket collection to resume safely.
- [ ] Add an opt-in `dev_cluster` GCS end-to-end test. It must require explicit
  environment configuration, create an isolated tiered-policy fixture, run
  `collect → dry-run → delete → verify`, clean up ClickHouse and object-storage
  fixtures, and remain excluded from CI.
- [ ] Make the `ghcr.io/altinity/s3gc` package public in the organisation's
  package settings after its first publish, so Kubernetes pulls need no
  registry credentials.
