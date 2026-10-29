# Dev-cluster E2E spec

Historical acceptance criteria for the test now implemented in
[test_s3gc_dev_cluster.py](test_s3gc_dev_cluster.py). See its explicit opt-in
requirements before using an isolated development cluster. These criteria are
not instructions to run a live test as part of the default suite.

Add an opt-in test that validates s3gc's Replicated-database preflight and
cross-replica inventory checkpoints against an isolated development ClickHouse
cluster and S3 prefix. The test must never run in the default pytest suite.

Acceptance criteria:

- The test requires explicit opt-in, ClickHouse credentials from a named
  client connection or Kubernetes Secret environment, two distinct replica
  endpoints, the S3 bucket and region, and IAM-backed S3 access.
- It creates a unique Replicated database and
  `ReplicatedReplacingMergeTree` inventory, seeds only uniquely prefixed test
  objects, collects on one replica, then runs dry-run and delete on the other.
  Synchronization and verification check the inventory on both replicas.
- It deletes only the seeded fixture keys, confirms the dry-run candidate set
  matches them, verifies replicated tombstones and missing S3 keys, and cleans
  up only its own database and objects on success or failure.
- It remains marked `dev_cluster`, excluded from CI, and does not store or log
  credentials or target-specific configuration.
