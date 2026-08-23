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

### Changed

- **`--useage` now has a hard floor of 24 hours, and defaults to 24** instead
  of 0. Below the floor a run is refused outright rather than warned about, for
  `--dry-run` as well as delete.

  Why a floor and not a warning: the age window is the *only* thing standing
  between a run and live data. ClickHouse uploads a part's blobs to S3 and
  registers them in `system.remote_data_paths` a moment later; in that window a
  live blob is absent from the reference table and looks orphaned, and there is
  no per-object re-check before the S3 delete. `--useage 0` silently removed
  that protection, the default *was* 0, and `render.py` accepted it — so the
  dangerous configuration was also the out-of-the-box one for anyone who did
  not set it.

  Why a floor and not a hardcoded constant: the parameter is only dangerous
  downward. Upward it is the "be more careful" lever — a cluster with slow
  merges or long-running mutations may legitimately want 72 hours or a week.
  Removing it would forfeit that and buy nothing the floor does not already
  give.

  Refused for `--dry-run` too, deliberately: a preview computed over a wider
  set than the delete would honour is worse than no preview, because the
  reviewed number is the one the customer approves.

  The floor is enforced in **both** `s3gc.py` and `render.py`. The renderer
  catches it before a Job is applied; the tool catches a direct CLI run, which
  never passes through the renderer at all.

  **Development escape hatch, scoped to the one non-production phase.**
  `PHASE=dev-automation` seeds and deletes its own fixtures within minutes, so
  a 24 hour window would make it find nothing and "succeed" vacuously — worse
  than failing. That phase, and only that phase, passes
  `--dev-allow-short-useage`; the `collect`, `dry-run` and `delete` branches of
  the entrypoint never do, and a test asserts it. A run that uses it logs a
  warning and writes a `warning` row to the durable run log, so it can never be
  mistaken for a normal one. The entrypoint passes the explicit
  `--dev-allow-short-useage=true` value required by the boolean parser.

### Added

- **A durable run log in ClickHouse**, `<COLLECTTABLEPREFIX><disk>_log`, written
  beside the auxiliary table and **never truncated**.

  Why: pod logs are not a record. The kubelet rotates container output (10Mi
  over 5 files by default), so `kubectl logs` cannot return the beginning of a
  long run, and `ttlSecondsAfterFinished` deletes the Job and its pods along
  with everything they printed. A cleanup that reclaimed terabytes left no
  evidence of what it did once that window closed — the reported symptom was
  simply "the job does not have all the log output".

  ClickHouse is the sink rather than a volume or the bucket: the connection,
  credentials and grants already exist, a `readOnlyRootFilesystem` container
  cannot write a file, and an object written under `S3PATH` would be listed by
  the *next* collect, found absent from `system.remote_data_paths`, and become
  a deletion candidate — s3gc would garbage-collect its own logs.

  Rows are self-describing evidence, not just text: `run_id`, `phase`, `event`,
  `message`, running `objects`/`bytes`, and the scope the run was pointed at
  (bucket, prefix, disk, cluster, dry-run, ClickHouse host, container
  hostname). Events are phase start, throttled collect progress, per-sample
  start, **per-delete-batch checkpoint**, finish with attempt totals, warnings
  and errors. `S3GC_RUNID` defaults to a generated timestamped id and the Job
  template passes `JOB_NAME`, so a row traces back to the Job that wrote it.

  Three constraints, each with a test, because this is bookkeeping attached to
  an irreversible operation:

  1. **Writes go on `ch_writer`, never `ch_client`.** `do_use()` holds
     `ch_client`'s session for the whole anti-join stream, and a second query on
     a held session is `SESSION_IS_LOCKED` (373) — the 0.6.0 defect, which
     would now fire mid-delete at the worst possible moment.
  2. **A logging failure never fails the run.** One failure disables the run log
     for the remainder of the process rather than retrying every batch, and a
     missing `CREATE TABLE` grant degrades to stdout only with one warning.
  3. **Messages are redacted** through the existing `LogFormatter._filter`
     before insert, so a credential cannot reach a table that outlives the run.

  Opt out with `--runlog false` / `S3GC_RUNLOG_FLAG=false`, or `RUNLOG=false`
  in the renderer. Default on: durability is the point.

- **Collect now reports progress at INFO**, throttled to every 100 000 objects.
  Per-batch progress was logged at DEBUG only, so a multi-hour collect over
  millions of objects emitted about four lines at `--verbose` — while `--debug`
  emits one line *per object*, which on a large bucket exceeds the kubelet's
  rotation limit and destroys the beginning of its own output. The operator's
  real choice was "almost nothing" or "too much to retrieve".


- **Regression tests pinning the deletion scope itself.** The suite already
  proved that a candidate list is deleted, batched and checkpointed correctly,
  but nothing proved the list contained only orphans: the ClickHouse fake
  ignores the anti-join SQL and returns pre-canned blocks, so the one statement
  that forms the entire safety boundary was never asserted on.

  Mutation testing established the gap rather than assuming it. Against the
  65-test suite, **nine of eleven** deliberate breakages of the delete scope
  passed fully green — including turning `LEFT ANTI JOIN` into a plain
  `LEFT JOIN` (every *referenced* object becomes a deletion candidate) and
  making `--dry-run` delete for real. Only the two error-bookkeeping mutations
  were caught.

  The scope is now asserted directly: anti-join semantics, the
  `remote_path = objpath` join key, the `disk_name` predicate, the
  `clusterAllReplicas` fan-out when a cluster is configured, `active=true`,
  the `--useage` window, candidates being drawn only from the auxiliary table,
  that a dry run cannot reach S3 at all, and that the cluster/replica preflight
  gates the delete path. All eleven mutations are now caught.

  Why it is worth the words: the anti-join is correct today, so this changes no
  behaviour. It exists so that a future edit which quietly widens the deletion
  scope fails a test instead of reaching a customer bucket.

### Known defects recorded

- **`USEAGE_HOURS=0` silently removes the only protection against deleting a
  part mid-write.** ClickHouse uploads a part's blobs to S3 and registers them
  in `system.remote_data_paths` a moment later; in that window a live blob is
  absent from the reference table and looks orphaned. There is no per-object
  re-check before the S3 delete, so the `--useage` clause is the whole safety
  margin — and `if args.useage else ""` emits no clause at all for 0, which
  `render.py` accepts (only negatives are rejected). Documented by
  `test_useage_zero_disables_the_age_guard`; the fix (refuse, or warn) is
  deliberately deferred to an explicit decision and tracked in `TODO.md`.

- **`--useafter` is interpolated unquoted**, so the value lands as a bare SQL
  identifier rather than a string literal. It is the only unquoted value in the
  anti-join `WHERE` clause. Fail-closed in practice (unknown identifier), and
  recorded as a strict `xfail` so it flips to a failure the moment it is fixed.

- **The cluster/replica preflight is a point-in-time check**, run once, while
  the anti-join is re-issued per sample over what can be hours. A replica
  dropping out mid-run is not re-detected.

## [0.6.0] - 2026-08-19

The first release published by CI, and the first image whose provenance can be
recovered. Everything before it was a hand-built `dev-*` tag.

Two things had to be fixed to get here, both worth recording because neither is
visible from the code. The release workflow triggers on `tags: ['v*.*.*']`,
while the repository's tags were `v0.5`, `v_0.1` and `v_0.2` — none can match,
so the publish job had never run for a release. And `ghcr.io/altinity/s3gc`
already existed from the manual pushes; a GHCR package created by a user push is
not linked to its repository, so `GITHUB_TOKEN` was refused with
`denied: permission_denied: write_package` until the package's *Manage Actions
access* granted the repository the Write role. A renamed or new package will
need that grant again.

Images from this release carry `org.opencontainers.image.revision`, so a running
container maps back to a commit. Identifying which build produced an earlier
production run previously required comparing log-message formats between runs.

### Added

- **`USETOTAL` is now settable from the Kubernetes Job template**, so a use
  phase can be capped at a few thousand objects. `--usetotal` already existed on
  the command line and, through `env_prefix="S3GC"`, in the environment; it was
  simply unreachable for anyone deploying with the renderer, whose only option
  was an unbounded multi-hour run.

  This is the missing safety rail for a first delete against a newly published
  image or an unfamiliar cluster: a bounded run exercises anti-join, S3 deletion
  and tombstone write-back end to end in minutes. The session-lock defect above
  would have been caught by one, for the price of minutes instead of a run that
  died partway with objects already removed.

  The key is optional and renders no environment variable when empty, because
  `S3GC_USETOTAL` is parsed as an integer and an empty string would fail the run
  at startup. Existing environment files render unchanged.

### Fixed

- **The delete phase died on its first batch with `SESSION_IS_LOCKED` (ClickHouse
  error 373), after the objects were already gone from S3.** `connect_to_ch()`
  built a single `clickhouse_connect` client, which the driver gives an
  auto-generated `session_id`, and ClickHouse permits one query at a time per
  session. `do_use()` holds that session for the entire anti-join while it
  consumes `query_row_block_stream`, and `insert()` issues its own
  `DESCRIBE TABLE` before writing — a second concurrent query on the held
  session. The server rejected it, the job exited non-zero, and up to
  `--deletebatchsize` objects were deleted with no tombstone recorded, so a
  resumed run could not know they were done.

  Tombstone writes now go to a second client created in the same call. The
  connection is deliberate, not incidental: reverting to a shared client
  reintroduces the defect silently, because it only fires once a *real* deletion
  succeeds and the write-back is attempted.

  Evidence and why it went unnoticed: every earlier delete that reclaimed data
  ran with `--order-by-objpath`, which makes the server sort the whole result
  before streaming it, and ran against ClickHouse 25.x. The first run without
  global ordering — the documented default for Kubernetes Jobs — hit the lock 78
  minutes in, on the first block the anti-join produced. Whether ordering or the
  server version is what previously masked it was not established; the fix does
  not depend on the answer, and relying on either to keep the session free was
  accidental rather than designed.


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

- **S3 authentication modes — `--s3auth=static|aws|iam`**, merging
  [PR #2](https://github.com/Altinity/s3gc/pull/2) by **@realyota**, which added
  `--s3auth=aws`, `--s3profile` and `--s3-session-token` so credentials can come from
  the boto3 chain (AWS SSO profiles) or as explicit temporary credentials.

  That PR replaced the workload-identity path outright. Both are kept instead,
  because they are not interchangeable: `iam` uses MinIO's own provider, needs no
  `boto3`, and is what the validated Kubernetes deployments use — it hands MinIO the
  *provider* rather than frozen keys so credentials refresh across a long collect or
  delete. `aws` resolves through boto3 and suits a workstation with SSO.

  | Mode | Credentials | boto3 |
  |---|---|---|
  | `static` (default) | explicit keys, optional session token | no |
  | `aws` | boto3 chain / `--s3profile` | **yes** |
  | `iam` | MinIO workload identity (IRSA/IMDS/ECS) | no |

  `--s3profile` implies `aws`. Contradictory combinations are rejected rather
  than silently resolved, so nobody ends up authenticating with an identity they
  did not ask for.

- **Operator-facing S3 listing errors**, also from PR #2: a failed listing now names
  the required permission (`s3:ListBucket` on the bucket ARN, **even for
  `--dry-run`**) and prints the `aws sts get-caller-identity` /
  `aws s3api list-objects-v2` commands to verify the same credentials. `UserVisibleError`
  reports such failures without a traceback.

- **Wider log-secret filtering** (PR #2): `s3accesskey` and `s3sessiontoken` are now
  redacted alongside `chpass` and `s3secretkey`.

- **Kubernetes wiring for the new auth**, which PR #2 did not include: `S3AUTH` and
  `S3PROFILE` are exposed by `job.yaml.tmpl`, required and validated by `render.py`
  (`S3PROFILE` without `S3AUTH=aws` is an error), and default to `iam` in
  `example.env`. Without this the flags were unreachable from a Job. The session token
  stays out of the non-secret env file — it belongs in the credentials Secret.

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

- **`boto3` is a pinned runtime dependency** (`boto3==1.43.65`), required by
  `--s3auth=aws`. It is imported lazily, so the other modes never load it. PR #2 added
  it unpinned; the pinned set is kept per `CLAUDE.md` rule 5 — unpinned `jsonargparse`
  resolves to 4.50.x and fails at import.

- **`IMAGE_PULL_SECRET` is now optional.** `render.py` omits the
  `imagePullSecrets` block when the value is empty, rather than emitting a
  meaningless `- name: ""`. Set it only for a private mirror.

- **Multi-architecture builds are mandatory, not advisory.** ClickHouse node
  pools are frequently arm64 — one observed pool was 5× arm64 and 1× amd64,
  where an amd64-only image can only ever schedule on a sixth of the capacity.
  CI builds `linux/amd64,linux/arm64` in a single step.

- **CI validates the rendered manifest without a Kubernetes cluster.** Strict,
  digest-pinned Kubeconform schema validation replaces client-side `kubectl`,
  which attempts OpenAPI discovery against a nonexistent API server on GitHub
  runners.

- **CI explicitly excludes `dev_cluster` tests.** The pull-request suite stays
  offline even after opt-in environment-dependent coverage is added.

- Removed the deprecated S3 IAM selector. `S3AUTH=static|aws|iam` is now the
  only supported authentication interface.

### Documentation

- `CHHOST` must be a **per-replica** Service, never the load-balanced one, and
  every phase of a cleanup must use the same host.
- S3 authentication guidance now keeps static, AWS SSO/profile, and workload
  identity modes together; the Kubernetes guide calls out dedicated ClickHouse
  user provisioning and only the required table and system-table grants.
- Contributor guidance now requires test-driven, focused coverage for every
  feature, defect, and material operational behaviour change.
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

### Migration notes

- **Pull secrets are registry-scoped.** Moving from Docker Hub to GHCR silently
  invalidates an existing `imagePullSecret` even though its *name* still looks
  right: a secret holding `index.docker.io` credentials does not apply to
  `ghcr.io`, so the kubelet falls back to an anonymous token and the pod sits in
  `ImagePullBackOff` with `failed to fetch anonymous token: 401 Unauthorized`.
  With the public image the correct action is to **remove** the secret reference
  (leave `IMAGE_PULL_SECRET` empty), not to repoint it.

- **The GHCR package must be made public once**, in the organisation's package
  settings. It cannot be done through the REST API — the visibility endpoint
  returns 404 and the standard token lacks `write:packages`. Until it is flipped,
  every pull still needs credentials and the benefit above is not realised.

### Verified

All three phases were exercised end to end against a development ClickHouse cluster using the
image built from this branch, pulled anonymously from the public registry with no
`imagePullSecret`: `collect` (188 objects), `dry-run` (exactly the 16 seeded orphan
fixtures), `delete` (cluster preflight, per-batch checkpoints, cumulative total) and a
verifying `dry-run` reporting zero. `auth=iam` in the log confirms workload identity still
resolves after the credential-resolution rewrite, and the referenced tables were untouched.
Unit tests cover the `static`/`aws` modes; the GCS per-object fallback is still only
unit-tested, pending a real GCS endpoint.

## v0.2 — 2025-01-31

- Added an option to avoid batch deletion for services such as GCS.

## v0.1 — 2024-06-12

- Added object last-modified timestamps to the auxiliary table.
- Added the object age option.
