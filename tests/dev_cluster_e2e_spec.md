# Dev-cluster E2E spec

Add an opt-in test that validates s3gc's Replicated-database preflight and
cross-replica inventory checkpoints against an isolated development ClickHouse
cluster and S3 prefix. The test must never run in the default pytest suite.

Acceptance criteria:

- The test requires explicit opt-in, a named ClickHouse client connection, a
  two-replica cluster, the S3 bucket and region, and IAM-backed S3 access.
- It creates a unique Replicated database and
  `ReplicatedReplacingMergeTree` inventory, seeds only uniquely prefixed test
  objects, and reuses the table across collect, dry-run, delete and verify on
  both replicas with synchronization between phases.
- It deletes only the seeded fixture keys, confirms the dry-run candidate set
  matches them, verifies replicated tombstones and missing S3 keys, and cleans
  up only its own database and objects on success or failure.
- It remains marked `dev_cluster`, excluded from CI, and does not store or log
  credentials or target-specific configuration.
