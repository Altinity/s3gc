# Kubernetes Job runner

Run `s3gc` as a one-shot Job in the ClickHouse namespace. For customer and
production work, always run:

```text
collect → dry-run → approved delete → verify
```

The Job reaches ClickHouse through an in-cluster Service; no laptop tunnel is
needed.

> **`CHHOST` must be a PER-REPLICA Service, never the load-balanced one.**
> The auxiliary table is a node-local `ReplacingMergeTree`, not a Replicated
> table. A load-balanced Service round-robins, so `collect` can write the table
> on one replica while `dry-run`/`delete` land on another and find nothing.
> Use `chi-<chi>-<cluster>-0-0` (replica 0), not `clickhouse-<cluster>`, and use
> the **same** host for every phase of a cleanup.

## Before the first Job

- Use an immutable, multi-architecture image digest:
  `ghcr.io/altinity/s3gc@sha256:<digest>`. CI prints the exact `IMAGE=` line in
  its job summary. Pin by digest, never by tag — tags get re-pushed.
- The image must be multi-arch. ClickHouse node pools are often arm64 (one
  customer cluster is 5x arm64 + 1x amd64), and an amd64-only image simply will
  not schedule there.
- Create or reuse a namespace-local ServiceAccount.
- Create a runtime Secret named by `CREDENTIALS_SECRET`:
  - `S3AUTH=static`: `S3GC_CHUSER`, `S3GC_CHPASS`, `S3GC_S3ACCESSKEY`,
    `S3GC_S3SECRETKEY`, plus `S3GC_S3SESSIONTOKEN` for temporary credentials.
  - `S3AUTH=iam`: only `S3GC_CHUSER` and `S3GC_CHPASS`, with an
    identity-enabled ServiceAccount. Note the template sets
    `automountServiceAccountToken: false`, so `iam` also needs a ServiceAccount
    that actually projects a token.
- Confirm the ClickHouse Service name, cluster macro, expected replica count,
  S3 bucket/prefix, and disk name. Use a unique `COLLECTTABLEPREFIX` per
  bucket/prefix cleanup.

### Choosing `S3AUTH`

| `S3AUTH` | Credentials | Needs boto3 | Use when |
|---|---|---|---|
| `iam` (default here) | MinIO workload identity — IRSA, IMDS, ECS task role | no | the normal Kubernetes case |
| `static` | `S3GC_S3ACCESSKEY`/`S3GC_S3SECRETKEY` (+ optional `S3GC_S3SESSIONTOKEN`) from the Secret | no | no workload identity available |
| `aws` | boto3 chain, optionally `S3PROFILE` | **yes** | rarely in-cluster; this is a workstation SSO path |

`S3PROFILE` requires `S3AUTH=aws` and the renderer rejects other combinations.

Prefer `iam`: it hands MinIO the credential provider, so temporary credentials
refresh during a long collect or delete instead of expiring mid-run.

### Minimum ClickHouse grants

Create or provision a dedicated ClickHouse user for `s3gc`, then grant it:

```sql
GRANT SELECT ON system.*                        TO s3gc;  -- remote_data_paths, one, disks, tables
GRANT SELECT, INSERT, CREATE TABLE ON <db>.*    TO s3gc;  -- auxiliary + run-log tables
```

### Values that vary per cluster, and bite when wrong

- **`S3PATH` may legitimately be empty** — some buckets keep blobs at the root.
  A wrong prefix silently lists nothing and reports a clean bucket.
- **The disk is not always called `s3`** — GCS-backed clusters commonly use
  `gcs`. `S3DISKNAME` sets both the anti-join scope and the aux table name, so
  the wrong value makes *every* blob look orphaned.
- **A `*_cache` disk is a filesystem cache over the same blobs**, not a second
  reference scope; scope the anti-join to the underlying object disk.
- **`SAMPLES` must match the value used at collect time** — the aux table is
  `PARTITION BY CRC32(objpath) % SAMPLES`, and a mismatch loses partition
  pruning (measured: ~2 min vs ~26 min per sample). s3gc now warns on mismatch.
- **GCS has no batch delete** — s3gc detects a `storage.googleapis.com` endpoint
  and falls back to one request per object, which is markedly slower.

Never commit credentials, rendered customer manifests, or customer `.env`
files to this repository.

## Local run directory

Keep generated files outside this repository:

```bash
export S3GC_RUN_DIR=/path/to/private-s3gc-runs/customer-cluster
mkdir -p "$S3GC_RUN_DIR"
cp deploy/kubernetes/example.env "$S3GC_RUN_DIR/s3gc.env"
```

```text
$S3GC_RUN_DIR/
├── s3gc.env
├── collect.yaml
├── dry-run.yaml
├── delete.yaml
└── verify.yaml
```

Fill `s3gc.env` from `example.env`. For production, start with
`SAMPLES=4`, `USEAGE_HOURS=24`, `ORDER_BY_OBJPATH=false`, and a 12-hour
deadline.

> **`USEAGE_HOURS` has a hard floor of 24 and the renderer enforces it.**
> ClickHouse uploads a part's blobs to S3 and registers them in
> `system.remote_data_paths` a moment later. In that window a live blob looks
> orphaned, and nothing re-checks it before the delete — so this window is the
> only thing protecting a part that is still being written. Raise it if a
> cluster has slow merges or long mutations; you cannot lower it. The one
> exception is `PHASE=dev-automation`, which seeds and deletes its own fixtures
> and is already documented as non-production.

Before the first *full* delete against a newly published image or a cluster you
have not deleted from before, run one bounded delete with `USETOTAL` set to a
few thousand. It exercises the whole path — anti-join, S3 deletion, tombstone
write-back — in minutes, and a failure costs you that instead of a multi-hour
run that dies partway with objects already removed. Clear `USETOTAL` for the
real run.

## Run each phase

For each phase, update only `PHASE`, `JOB_NAME`, and (for delete)
`DELETE_CONFIRMATION` in `s3gc.env`, then render and apply:

```bash
python3 deploy/kubernetes/render.py "$S3GC_RUN_DIR/s3gc.env" \
  > "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply --dry-run=server -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl -n <namespace> logs -f job/<job-name>
```

| Phase | Required values | Result |
|---|---|---|
| `collect` | `PHASE=collect` | Lists S3 objects into the auxiliary ClickHouse table. No deletion. |
| `dry-run` | `PHASE=dry-run` | Reports candidates and total size. Review this result. |
| `delete` | `PHASE=delete`, `DELETE_CONFIRMATION=DELETE_ORPHANS` | Checks cluster/replicas, deletes candidates, and checkpoints confirmed progress. |
| `verify` | `PHASE=dry-run` | Must report zero candidates. |

### Development automation only

`PHASE=dev-automation` runs `collect → dry-run → delete` in one Job. It always
starts with a fresh auxiliary table and requires
`DELETE_CONFIRMATION=DELETE_ORPHANS`, `CLUSTERNAME`, and `EXPECTED_REPLICAS`.
Any failed stage stops the Job and later stages do not run; successful delete
batches remain checkpointed. Do not use this phase for customer or production
work because it removes the manual dry-run approval gate.

## Durable run history

Pod logs are **not** a record. The kubelet rotates container output, so
`kubectl logs` cannot return the start of a long run, and
`ttlSecondsAfterFinished` deletes the Job and its pods along with everything
they printed. `kubectl logs -f` is for watching, not for evidence.

So each run also appends to `<COLLECTTABLEPREFIX><S3DISKNAME>_log` in
ClickHouse, beside the auxiliary table. That table is **never truncated**, and
`S3GC_RUNID` is set to the Job name, so a row traces back to the Job that wrote
it long after the pod is gone.

What a delete actually did, on the same replica-pinned `CHHOST`:

```sql
SELECT event_time, phase, event, objects, bytes, message
FROM   <db>.<prefix><disk>_log
WHERE  run_id = '<job-name>'
ORDER BY event_time;
```

How far a *failed* delete got before it died, which is what tells you whether to
start a replacement delete Job:

```sql
SELECT max(objects) AS deleted, max(bytes) AS reclaimed
FROM   <db>.<prefix><disk>_log
WHERE  run_id = '<job-name>' AND event = 'checkpoint';
```

Every phase of a cleanup, newest first:

```sql
SELECT run_id, min(event_time) AS started, max(event_time) AS ended,
       anyIf(message, event = 'error') AS error
FROM   <db>.<prefix><disk>_log
GROUP BY run_id ORDER BY started DESC;
```

Set `RUNLOG=false` to opt out. A missing `CREATE TABLE` grant degrades to
stdout only with one warning rather than failing the run, and a run-log write
that fails mid-delete disables the log instead of stopping the deletion.

## Safety

- Delete checks the local cluster macro and expected replica count before S3
  calls.
- Confirmed deletions are tombstoned in the auxiliary table. If a delete Job
  fails, create a new delete Job name with the same table prefix; do **not**
  re-collect.
- No Job retries automatically (`backoffLimit: 0`).
- Do not run delete until the customer explicitly approves the dry-run result.
