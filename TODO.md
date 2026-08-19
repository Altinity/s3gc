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
- [ ] Make the `ghcr.io/altinity/s3gc` package public in the organisation's
  package settings after its first publish, so Kubernetes pulls need no
  registry credentials.
- [ ] Require the `Container / test` GitHub Actions check before pull-request
  merges in the repository branch-protection settings.
- [ ] Publish releases from a semver tag. `.github/workflows/container.yml`
  triggers on `tags: ['v*.*.*']`, but the tags in the repository are `v0.5`,
  `v_0.1` and `v_0.2` — none can ever match, so the release job has never run
  and no versioned or `latest` image exists. Every deployment so far has pinned
  a hand-built `dev-*` digest carrying no
  `org.opencontainers.image.revision`, which makes a running image impossible to
  map back to a commit without guessing from log message formats. Tag `vX.Y.Z`
  and let `type=semver` and `type=sha,format=long` do the rest.
