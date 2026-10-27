---
name: altinity-clickhouse-s3gc
description: Safely investigate, plan, review, and execute cleanup of orphaned objects from ClickHouse S3 or compatible object-storage disks with s3gc. Use for S3 garbage collection, orphan-object cleanup, s3gc dry runs, reviewed deletion Jobs, or post-delete verification.
---

# ClickHouse S3 Garbage Collection

Use this skill to find, review, and remove approved orphaned objects with s3gc.
It compares an inventory with ClickHouse `system.remote_data_paths` for the
selected disk and cluster. Use the Kubernetes runbook for production.

## Non-negotiable rules

- Deletion is irreversible. Follow **collect → dry-run → explicit human approval
  → delete → verify** for customer and production work.
- Limit the bucket and prefix to the intended scope. A wrong prefix can hide
  orphans or include objects you must not delete.
- Set `S3DISKNAME` to the underlying object disk, such as `s3` or `gcs`.
  Never use its `*_cache` wrapper.
- Pin `CHHOST` to one per-replica ClickHouse Service for every phase. The
  default inventory is node-local. Keep the same pinned host even when using
  a manually created replicated inventory.
- Require the actual cluster name and expected replica count. Delete must
  pass the cluster/replica preflight before deleting any objects.
- Use a unique `COLLECTTABLEPREFIX` for each cleanup scope. Keep it after a
  partial delete so a replacement Job can use the confirmed checkpoints.
- Keep `USEAGE_HOURS` at 24 or more for production. This protects objects
  uploaded before ClickHouse registers their paths.
- An agent must not supply `DELETE_ORPHANS`, create a delete manifest with that
  token, or bypass dry-run review. A human must review the immediate dry-run
  output and explicitly authorize deletion.
- Never record credentials. Keep customer configuration, rendered manifests,
  bucket names, cluster identifiers, and their output outside Git. Use a private
  run directory and the authorized support record for run evidence.
  Provide credentials through Kubernetes Secrets or workload identity.

## Workflow

### 1. Establish the cleanup scope

Record the purpose, disk, endpoint, bucket, prefix, cluster, expected replicas,
namespace, and pinned host in the private run record. Confirm the authentication
mode and required ClickHouse and S3 permissions. Prefer `iam` for in-cluster AWS
work. Supply static credentials only through a runtime Secret.

Read the [Kubernetes runbook](../../deploy/kubernetes/README.md). Copy
[example.env](../../deploy/kubernetes/example.env) to the private run directory.
Use a multi-architecture image pinned by digest and an existing ServiceAccount
configured for the intended identity. The Job does not need Kubernetes API access.

If independent clusters share the scope, follow the
[manual inventory workflow](../../docs/cli-reference.md#manual-inventory-for-a-bucket-shared-by-independent-clusters).
Check references from every cluster, replica, relevant disk, and other consumer.
Import only reviewed candidates. s3gc's configured-cluster check cannot see
independent clusters. Stop if the review cannot cover every consumer.

### 2. Collect and review

Run `collect`, then `dry-run`, against the same host and inventory. Collection
does not delete objects. Dry-run reports the candidate count and size. Review
the bucket and prefix too. Store the configuration location and Job/log evidence
in the authorized support record without copying credentials or artifacts into Git.

For a manually imported inventory, skip collect and start with dry-run.
`--use-collected` reads the existing table without listing S3 again. Keep the
all-consumer reference review current through deletion, as the manual workflow
requires.

Investigate a missing or empty inventory before proceeding. Check the prefix,
disk, and pinned host. For large buckets, collect separate prefix shards.
Interrupted collection has no checkpoint. Rerun only the failed shard before
deletion, not while recovering a delete.

### 3. Require approval and delete

Present the candidate count, size, exact scope, and any uncertainty to the
authorized operator. Do not proceed without explicit approval.

After the human supplies the token in the private configuration, render a new
delete Job with the same host, scope, and inventory. Monitor it to completion.
Keep the durable run log enabled unless stdout is the approved record.

A failed Job is not rerun automatically. Individual S3 requests can retry within
the process. Inspect the failure and confirm that approval still applies before
starting a replacement Job. Preserve the inventory and checkpoints. Do not
re-collect during recovery. A changed scope needs a new inventory, review, and
approval.

### 4. Verify and hand off

Run a final dry-run against the same host and inventory with `USETOTAL` cleared.
It must report zero candidates before the cleanup is complete. That result
covers the reviewed inventory and age window, not newly uploaded objects.

Record the approved scope, image digest, preflight result, candidate and deleted
totals, verification result, and recovery steps in the authorized handoff record.

## References

- [Kubernetes Job runner](../../deploy/kubernetes/README.md) — authoritative production procedure,
  permissions, phase inputs, and safety behavior.
- [s3gc README](../../README.md) — tool model, authentication modes, direct read-only usage, and
  local development checks.
- [`deploy/kubernetes/example.env`](../../deploy/kubernetes/example.env) — non-secret renderer
  template. Copy it only to a private run directory.
- [CLI reference](../../docs/cli-reference.md) — parameter details and manual inventory preparation.
