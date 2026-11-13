# TODO

Deferred engineering and operational work. Record completed changes in
`CHANGELOG.md` and remove or update the item here.

## Shared buckets and large cleanups

Proposed release work, ordered by safety dependencies. Each item needs a
reviewed specification and offline regression tests before implementation.
The existing manual shared-bucket workflow, replica preflight, checkpoints,
run log, IAM mode, and explicit approval remain in force until replacements
are implemented and validated.

- [ ] **Cover every consumer of a shared prefix.** Define a registry of
  independent clusters, replicas, disk-to-bucket mappings, and non-ClickHouse
  consumers. Collect or import complete live and frozen references with source
  identity, timestamps, counts, and hashes; subtract their union from the
  listing. Fail closed on missing or incomplete sources, unexpected topology,
  and ambiguous scope or key normalization. A configured ClickHouse cluster
  alone does not cover independent consumers.
- [ ] **Gate on freshness and coordination.** Bind source snapshots to a
  reviewed manifest and an explicit validity window. Check every consumer at
  the deletion boundary and after pauses; stop on expired evidence or missing
  consumers. Specify how changes to references, restores, backups, and key
  reuse are prevented during deletion. An operator assertion is not an
  enforceable lock; expose that distinction and support coordination hooks
  where practical.
- [ ] **Protect object identity.** Design and test provider-specific handling
  of keys replaced or reused after listing, including versioned buckets,
  metadata checks, and supported conditional operations. Document S3 API and
  compatible-provider limits, and fail closed when the requested protection
  cannot be provided. Object age alone cannot establish safety.
- [ ] **Bind preview and approval to an immutable manifest.** Record the
  bucket, prefix, source coverage, snapshot time, age bounds and inclusivity,
  object counts, bytes, and cryptographic digests. Bind execution, image and
  configuration, dry-run output, and approval to that manifest; reject
  mismatches. Cover direct-prefix objects and frozen-only references, and
  distinguish listing-time age from age at deletion.
- [ ] **Prevent long result streams from stalling during deletion.** A server
  HTTP socket write timeout can surface later as a client stream-decoding
  failure while synchronous S3 deletion and checkpoint writes occur between
  reads. Backpressure is a plausible cause, not yet proven for every failure.
  Evaluate draining each sample to bounded private storage or another design
  that keeps the stream moving. Test capacity exhaustion, corruption, cleanup,
  interrupted reads, reference freshness, and non-root Kubernetes operation
  with a read-only root filesystem. Do not assume increasing a client timeout
  changes the server's socket timeout.
- [ ] **Reconcile interrupted deletion safely.** Distinguish confirmed S3
  successes, per-key failures, unknown in-flight outcomes, and persisted
  tombstones. Define idempotent resume across an S3 success followed by a
  failed inventory write, including safeguards against overwritten or reused
  keys. Preserve explicit operator control over destructive retries. Test
  process interruption before and after each response and checkpoint, plus
  partial batch failures; an S3 success response alone does not prove a key
  previously existed.
- [ ] **Add a global work limit and practical recovery.** `--usetotal` is
  currently per sample. Add an unambiguous global limit and bounded,
  checkpoint-safe cursor or partition strategy with predictable job duration.
  Make attempt lineage and cumulative progress clear across restarts.
- [ ] **Scale reference processing.** Measure repeated
  `system.remote_data_paths` scans and anti-joins; evaluate verified staging or
  one scan with partitioned output. Preserve fresh reference checks and frozen
  path coverage, bound resource use, and expose incomplete exports, network
  interruptions, and credential expiry without relaxing managed limits.
- [ ] **Strengthen audit durability and visibility.** Existing run-log rows
  record selected events and batch totals; a write failure warns and falls
  back to stdout. Offer explicit fail-closed behavior when durable audit is
  required, alert on lost logging, and add attempt lineage, batch identifiers,
  classified errors, and exportable reports. Report per-source and per-sample
  progress, elapsed time, rates, last progress time, and stalled phases without
  exposing credentials or unnecessary object paths.
- [ ] **Reconcile completion and verify object-store results.** Compare the
  approved manifest, attempt checkpoints, inventory and replica state. Avoid
  presenting sequential live replica reads as a simultaneous snapshot. Design
  a separately authorized object-store verification mode with explicit
  coverage, provider/versioning semantics, and limitations; tombstones alone
  are not independent proof of physical object removal. Review safe inventory
  retention defaults (`--keepdata` currently controls retention).
- [ ] **Make shared-bucket operation reproducible.** Provide a supported
  multi-consumer manifest import, preview, recovery and verification workflow.
  Test independent clusters sharing keys, frozen-only references, direct
  prefix objects, incomplete listings, unavailable replicas, key replacement,
  partial S3 failures, interrupted streams and lost audit writes at realistic
  scale; include non-root Kubernetes deployments.
- [ ] **Improve workload-identity and operator ergonomics.** IAM mode already
  exists: document scoped identity verification and make a ClickHouse-only
  Secret with a workload-identity ServiceAccount straightforward. Improve
  phase summaries, image/config provenance, age and limit descriptions,
  renderer diagnostics, and retention/export of pod logs without exposing
  credentials or confirmation values.

## Other deferred work

- [ ] Record the last listed key so an interrupted collect can resume.
  `--collect-after` can start after a key, but collect does not save that key.
  Write it durably with scope and completeness checks so interrupted
  collection can resume without silently skipping or duplicating objects.
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
