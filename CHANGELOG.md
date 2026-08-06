# Changelog

Notable changes to `s3gc`, newest first.

This file records **what changed and why**, with enough context that someone
picking the repository up later — human or agent — can tell a deliberate design
decision from an accident. Defects found by running the tool against real
clusters carry their evidence, because the reasoning is usually the expensive
part to reconstruct.

Format loosely follows [Keep a Changelog](https://keepachangelog.com/).
Customer names, cluster identifiers and credentials never appear here (see
`CLAUDE.md`, rule 3); findings are described in terms of the behaviour they
expose.

## [Unreleased]

Changes below are on `feature/kubernetes-job-runner` and not yet released.

### Fixed

- **Boolean options were unusable from the environment, and one of them hung a
  production Job indefinitely.** Flags declared with `action="store_true"` are
  populated by `jsonargparse` from the environment as the **raw string**, and
  every non-empty string is truthy in Python — so `S3GC_S3USEIAM=false` meant
  *true*. The affected Job selected `IamAwsProvider()` instead of the static
  keys in its Secret and wedged in the IMDS credential loop: no error, no
  exception, no log line, zero rows after five minutes, no `:443` connection
  ever opened, 0.34 s of CPU — only `activeDeadlineSeconds` ended it. Removing
  the variable made the same image, Secret and manifest work immediately at
  ~3,500 objects/s.

  Thirteen flags shared the defect and four are set by `job.yaml.tmpl`
  (`S3GC_S3USEIAM`, `S3GC_S3SECURE_FLAG`, `S3GC_ORDER_BY_OBJPATH`,
  `S3GC_VERBOSE_FLAG`); two were correct only because `"true"` happens to be
  truthy, and `S3GC_S3SECURE_FLAG=false` would have silently stayed on TLS.

  *Why not `type=bool`:* it raises on `0`, `1` and empty values, and it would
  force bare flags such as `--collectonly` to take an argument, which
  `docker/kubernetes-entrypoint.sh` and every documented invocation rely on.
  Instead all boolean options are coerced once after parsing through the
  existing `strtobool` helper, accepting `true/false`, `yes/no`, `on/off`,
  `1/0`, empty and unset. The `--order-by-objpath-flag` twin argument, a
  previous one-off workaround for this same defect, is retired.

- **`--age` silently collected nothing for anything older than a day.** The
  filter used `timedelta.seconds`, the sub-day remainder (0..86399), so computed
  age never exceeded 23 h: a 30 d 5 h old object reported **5**. Harmless at the
  default `age=0`, which is why it went unnoticed — but `--age 24`, the natural
  choice by analogy with `--useage 24`, would produce an empty auxiliary table
  and a dry-run reporting a clean bucket. Now uses `total_seconds()`.

- **`--usecollected` against a missing or empty auxiliary table exited 0**,
  which is indistinguishable from success. That is exactly what a load-balanced
  ClickHouse Service produces, because the auxiliary table is a *node-local*
  `ReplacingMergeTree`: collect writes it on one replica and a later phase looks
  for it on another. It now fails loudly and explains the replica-pinning
  requirement.

- **GCS endpoints now fall back to per-object deletion automatically.** Google
  Cloud Storage's S3-compatible API has no batch `DeleteObjects`, so
  `remove_objects()` fails there. Detected from the endpoint, with a warning
  that the per-object path is markedly slower.

### Added

- **Warning when `--samples` disagrees with the auxiliary table's
  `PARTITION BY`.** The table is created as `PARTITION BY CRC32(objpath) %
  <samples>` at collect time, so a different value during the use phase loses
  partition pruning. Measured on production-scale data: ~2 min per sample when
  matched against ~26 min when not.

- **Cumulative deletion total alongside the per-attempt one.** The closing
  `N objects … are removed` line counts only the process that printed it, which
  understated one resumed run by 16.61 TiB. It now says "in this attempt" and
  logs the auxiliary table's cumulative tombstone count.

- **30 regression tests** covering the boolean matrix (per flag, per spelling,
  plus bare-CLI compatibility), the age filter, the fail-loud path, the samples
  warning, the GCS fallback, and renderer pull-secret handling.

### Changed

- **Images are published publicly to `ghcr.io/altinity/s3gc`** instead of a
  private Docker Hub repository, matching `altinity-mcp` and
  `altinity-sql-browser`. CI authenticates with the automatic `GITHUB_TOKEN`,
  so there is no registry secret to manage or rotate.

  *Why it matters operationally:* a private image forces whoever runs a Job to
  copy a registry credential into the target namespace as an `imagePullSecret`
  and remember to delete it afterwards. A public image removes that step
  entirely.

- **`IMAGE_PULL_SECRET` is now optional.** `render.py` omits the
  `imagePullSecrets` block when the value is empty, rather than emitting a
  meaningless `- name: ""`. Set it only for a private mirror.

- **Multi-architecture builds are mandatory, not advisory.** ClickHouse node
  pools are frequently arm64 — one observed pool was 5× arm64 and 1× amd64,
  where an amd64-only image can only ever schedule on a sixth of the capacity.
  CI builds `linux/amd64,linux/arm64` in a single step.

- **CI validates the rendered manifest** with `kubectl apply --dry-run=client`
  in addition to running the tests and the renderer.

### Documentation

- `CHHOST` must be a **per-replica** Service, never the load-balanced one, and
  every phase of a cleanup must use the same host.
- The minimum ClickHouse grant set: `SELECT ON system.*`;
  `SELECT, INSERT, CREATE TABLE ON <db>.*`; `REMOTE ON *.*`. Notably **not**
  `S3 ON *.*` — `s3gc` lists buckets with its own client, not the `s3()` table
  function — and not `TRUNCATE` unless `--keepdata` is omitted.
- Sharding recipe for `--collectonly`, which has no resume: a crash re-lists
  from the beginning, which is expensive on multi-million-object buckets.
  Re-running a shard is safe because the auxiliary table is a
  `ReplacingMergeTree` keyed on `objpath`.
- Per-cluster values that cause silent failure when wrong: `S3PATH` may
  legitimately be empty (bucket-root layouts); the disk is not always named
  `s3` (GCS-backed clusters commonly use `gcs`, which changes both the
  anti-join scope and the auxiliary table name); a `*_cache` disk is a
  filesystem cache over the same blobs, not a second reference scope.
- Buildx builder containers cache `/etc/resolv.conf` at creation, so a builder
  left running across a network change fails with
  `lookup registry-1.docker.io: i/o timeout` while the host resolves fine.

### Known gaps

- **`--collectonly` still has no resume.** The sharding recipe covers it
  operationally; a `--collectafter` checkpoint would remove the need.
- **The GHCR package must be marked public once** in the organisation's package
  settings after the first publish, otherwise pulls still require
  authentication and the pull-secret benefit is not realised.

## v0.2 — 2025-01-31

- Added an option to avoid batch deletion for services such as GCS.

## v0.1 — 2024-06-12

- Added object last-modified timestamps to the auxiliary table.
- Added the object age option.
