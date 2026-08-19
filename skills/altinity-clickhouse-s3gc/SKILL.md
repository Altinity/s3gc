---
name: altinity-clickhouse-s3gc
description: Safely investigate, plan, review, and execute cleanup of orphaned objects from ClickHouse S3 or compatible object-storage disks with s3gc. Use for S3 garbage collection, orphan-object cleanup, s3gc dry runs, reviewed deletion Jobs, or post-delete verification.
---

# ClickHouse S3 Garbage Collection

Use this skill for the `s3gc` workflow only: identifying objects in a configured object-store
bucket/prefix that are absent from ClickHouse `system.remote_data_paths`, reviewing candidates,
and removing approved orphaned objects. It is not a generic S3 cleanup procedure.

## Non-negotiable rules

- Deletion is irreversible. For customer and production work, use the Kubernetes Job workflow:
  **collect → dry-run → explicit customer approval → delete → verify**.
- Scope the bucket and prefix as narrowly as possible. A wrong prefix can produce a misleading
  clean result or expand deletion scope.
- Use the underlying object disk (for example `s3` or `gcs`) for `S3DISKNAME`, never its
  `*_cache` wrapper. The cache does not represent a separate object-reference scope.
- Pin `CHHOST` to a per-replica ClickHouse Service, and use the same host for every phase. The
  auxiliary inventory table is node-local; a load-balanced Service can send phases to different
  replicas and invalidate the result.
- For clustered cleanup, require the actual cluster name and expected replica count. The delete
  phase must pass `s3gc`'s cluster/replica preflight before any S3 call.
- Use a unique `COLLECTTABLEPREFIX` for each bucket/prefix cleanup. Preserve it after a partial
  delete: confirmed deletions are checkpointed, and a replacement delete Job resumes from that
  state without re-collecting.
- An agent must not supply `DELETE_ORPHANS`, create a delete manifest with that token, or bypass
  the reviewed dry-run gate. A human must review the immediate dry-run output and explicitly
  authorize deletion.
- Never commit or record credentials, customer `.env` files, rendered manifests, bucket names,
  cluster identifiers, or command output containing them. Keep run material outside this repo and
  use Kubernetes Secrets or workload identity.

## Workflow

### 1. Establish the cleanup scope

Before collecting, record the purpose, affected disk, endpoint, bucket, exact prefix, ClickHouse
cluster, expected replica count, namespace, and per-replica `CHHOST`. Confirm the intended S3
authentication mode and the minimum ClickHouse/S3 permissions. Prefer workload identity (`iam`)
for an in-cluster AWS cleanup; use static credentials only from a runtime Secret.

Read [the Kubernetes operator guide](../../deploy/kubernetes/README.md) and start from
[`deploy/kubernetes/example.env`](../../deploy/kubernetes/example.env) copied to a private run
directory outside the repository. Use an immutable multi-architecture image digest, not a tag.

### 2. Collect and review

Render and run a `collect` Job, then a `dry-run` Job against the same `CHHOST` and collection
table. Collect does not delete; dry-run reports the candidate count and size. Capture the rendered
configuration location and the non-secret Job/log evidence in the active support record without
copying credentials or customer artifacts into Git.

Treat an empty or missing collected inventory as a failure to investigate, not proof that the
bucket is clean. Verify the prefix, disk name, and replica-pinned service first. For large buckets,
shard collection by prefix; interrupted collection has no checkpoint and must be rerun only for
the affected shard.

### 3. Require approval and delete

Present the dry-run candidate count, size, exact bucket/prefix scope, and any uncertainty to the
authorized customer/operator. Do not proceed without explicit approval.

After the human supplies the delete confirmation in the private runtime configuration, render a
new delete Job using the same collection-table prefix. Monitor it to completion. A failed delete
Job has no automatic retry; start a replacement delete Job with the same prefix so the confirmed
deletion checkpoints are preserved. Do not re-collect unless the scope itself changed.

### 4. Verify and hand off

Run the final `dry-run` phase against the same host and inventory. It must report zero remaining
candidates before declaring the cleanup complete. Record the approved scope, image digest,
cluster/replica preflight result, candidate and deleted totals, verification result, and any
partial-failure recovery steps in the active ticket's handoff record.

## References

- [Kubernetes Job runner](../../deploy/kubernetes/README.md) — authoritative production procedure,
  permissions, phase inputs, and safety behavior.
- [s3gc README](../../README.md) — tool model, authentication modes, direct read-only usage, and
  local development checks.
- [`deploy/kubernetes/example.env`](../../deploy/kubernetes/example.env) — non-secret renderer
  template; copy it only to a private run directory.
