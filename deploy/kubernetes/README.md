# Kubernetes Job runner

Run `s3gc` as a one-shot Job in the ClickHouse namespace. This guide is the
production procedure.

## Prerequisite

Use Python 3.11 from the repository root. Create the project virtual
environment before you render a Job:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
```

`render.py` uses only the Python standard library. The runbook still uses
`.venv/bin/python` so every command runs with the supported Python version.

## Runbook

### 1. Prepare a private run directory

Keep generated files outside this repository:

```bash
export S3GC_RUN_DIR=/path/to/private-s3gc-runs/customer-cluster
mkdir -p "$S3GC_RUN_DIR"
cp deploy/kubernetes/example.env "$S3GC_RUN_DIR/s3gc.env"
```

The directory will contain the non-secret configuration and one rendered
manifest for each phase:

```text
$S3GC_RUN_DIR/
├── s3gc.env
├── collect.yaml
├── dry-run.yaml
├── delete.yaml
└── verify.yaml
```

Do not commit this directory. It identifies a target environment even when it
contains no credentials.

### 2. Confirm the cleanup scope

Set the values in `s3gc.env`, then check these five items before rendering:

1. Set `CHHOST` to one per-replica ClickHouse Service and use the same host for every phase. The inventory table is node-local; a load-balanced Service can send a later phase to a replica without that table.
2. Set the exact bucket, prefix, and underlying object-disk name. `S3PATH` may be empty. GCS commonly uses `gcs`; never use a `*_cache` disk.
3. Set the actual `CLUSTERNAME` and `EXPECTED_REPLICAS` for clustered cleanup. Delete fails closed if this preflight does not match.
4. Choose a unique, database-qualified `COLLECTTABLEPREFIX`, such as `s3gc.s3gc_<run>_`. Keep it after a partial delete so the replacement Job can use the deletion checkpoints. A bare prefix uses the ClickHouse user's current database, which can differ from `default` and cause a grant failure.
5. Set `USEAGE_HOURS` to 24 or more. Raise it for slow merges or long mutations; production phases cannot lower it.

For an installation-wide cleanup, set `S3PATH` to the parent prefix that
contains every replica's objects, such as `clickhouse/<installation>/`, not a
single `<installation>/<replica>/` prefix. Collect it once into one inventory
table on the pinned host. With `CLUSTERNAME` set, dry-run and delete compare
the inventory with `system.remote_data_paths` on all replicas, so an object
that any replica still references is never a candidate. A single-replica
prefix misses the other replicas' orphans. Collect per-replica prefixes only
when you intend to, and give each one its own `COLLECTTABLEPREFIX`.

Use an immutable multi-architecture image digest in `IMAGE`. CI prints the
exact value after publishing. Do not use an image tag.

Collection has no resume checkpoint. For a large bucket, collect separate
prefix shards and rerun only a failed shard. Re-listing a shard is safe because
the auxiliary table replaces rows by object path.

### 3. Provide credentials and identity

Create or reuse the Kubernetes Secret named by `CREDENTIALS_SECRET`:

| `S3AUTH` | Secret values |
| --- | --- |
| `static` | `S3GC_CHUSER`, `S3GC_CHPASS`, `S3GC_S3ACCESSKEY`, `S3GC_S3SECRETKEY`, and an optional `S3GC_S3SESSIONTOKEN` |
| `iam` | `S3GC_CHUSER` and `S3GC_CHPASS` |
| `aws` | `S3GC_CHUSER`, `S3GC_CHPASS`, plus a credential source available to boto3 |

Set `SERVICE_ACCOUNT` to an existing namespace-local ServiceAccount that your
cluster configures for the chosen workload-identity mode. The Job keeps
`automountServiceAccountToken: false` because it does not call the Kubernetes
API. For `iam`, the platform must inject or otherwise provide the workload
identity credentials. Existing ClickHouse or ClickHouse-backup ServiceAccounts
are suitable when they meet that cluster policy.

Create a dedicated auxiliary database and ClickHouse user before the first run.
Replace `<cluster-name>`, `<auxiliary-database>`, and the password placeholder.
Use the same `<auxiliary-database>.` prefix in `COLLECTTABLEPREFIX`.

```sql
CREATE DATABASE IF NOT EXISTS <auxiliary-database>
ON CLUSTER <cluster-name>;

CREATE USER IF NOT EXISTS s3gc
ON CLUSTER <cluster-name>
IDENTIFIED WITH sha256_password BY '<strong-password>';

GRANT ON CLUSTER <cluster-name>
    SELECT ON system.* TO s3gc;

GRANT ON CLUSTER <cluster-name>
    SELECT, INSERT, CREATE TABLE ON <auxiliary-database>.* TO s3gc;

GRANT ON CLUSTER <cluster-name>
    REMOTE ON *.* TO s3gc;
```

The database grant covers the auxiliary inventory and durable run-log tables.
`REMOTE` permits the `clusterAllReplicas` reads used by clustered dry-run and
delete preflight. Do not grant ClickHouse's `S3` source privilege: s3gc uses
its own S3 client and the Secret or workload identity instead.

For `S3AUTH=static`, create the Secret from a private, shell-style credential
file. Keep this file outside the repository, restrict it to its owner, and
never print or commit it. s3gc does not read `AWS_ACCESS_KEY_ID` or
`AWS_SECRET_ACCESS_KEY` in static mode. Store those values under the s3gc
names instead:

| AWS variable | Secret key |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | `S3GC_S3ACCESSKEY` |
| `AWS_SECRET_ACCESS_KEY` | `S3GC_S3SECRETKEY` |
| `AWS_SESSION_TOKEN` | `S3GC_S3SESSIONTOKEN` (optional) |

The file must export these values:

```bash
export S3GC_CHUSER=s3gc
export S3GC_CHPASS='<ClickHouse password>'
export S3GC_S3ACCESSKEY='<S3 access key>'
export S3GC_S3SECRETKEY='<S3 secret key>'
```

Create the Secret. This command creates it once and refuses to overwrite an
existing Secret:

```bash
set -a
. /private/path/s3gc-secrets.env
set +a

kubectl -n <namespace> create secret generic <credentials-secret-name> \
  --from-literal=S3GC_CHUSER="$S3GC_CHUSER" \
  --from-literal=S3GC_CHPASS="$S3GC_CHPASS" \
  --from-literal=S3GC_S3ACCESSKEY="$S3GC_S3ACCESSKEY" \
  --from-literal=S3GC_S3SECRETKEY="$S3GC_S3SECRETKEY"

unset S3GC_CHUSER S3GC_CHPASS S3GC_S3ACCESSKEY S3GC_S3SECRETKEY
```

The S3 principal needs `s3:ListBucket` on the scoped bucket/prefix for
collection. A delete Job additionally needs `s3:DeleteObject` on the scoped
object keys. Keep delete permission separate until an approved delete phase.

### 4. Run each phase

For each phase, edit only the values shown below, render the Job, server-side
validate it, apply it, and follow its logs:

```bash
.venv/bin/python deploy/kubernetes/render.py "$S3GC_RUN_DIR/s3gc.env" \
  > "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply --dry-run=server -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl -n <namespace> logs -f job/<job-name>
```

| Phase | Change in `s3gc.env` | Required outcome |
| --- | --- | --- |
| Collect | Set `PHASE=collect` and a new `JOB_NAME`. | Inventory S3 objects. No deletion occurs. |
| Review | Set `PHASE=dry-run` and a new `JOB_NAME`. | Review the candidate count, bytes, bucket, and prefix. |
| Delete | After written human approval, set `PHASE=delete`, a new `JOB_NAME`, and `DELETE_CONFIRMATION=DELETE_ORPHANS`. | Delete and checkpoint confirmed batches. |
| Verify | Set `PHASE=dry-run`, a new `JOB_NAME`, and clear `DELETE_CONFIRMATION`. | Report zero candidates. |

Do not run delete from an unreviewed dry-run. Before the first full delete for
a new image or cluster, set `USETOTAL` to a few thousand objects for one
bounded delete. Clear it before the full run.

### 5. Recover or verify

If a delete Job fails, create a replacement delete Job with a new `JOB_NAME`
and the same `COLLECTTABLEPREFIX`. Do not re-collect unless the scope changed.
`backoffLimit: 0` prevents automatic retries.

After a successful delete, run the verification dry-run. Do not declare the
cleanup complete until it reports zero candidates.

## Configuration reference

`render.py` reads a non-secret `KEY=VALUE` file. It rejects missing required
values, unpinned images, invalid phases, invalid booleans, and unsafe age
windows. Start with [example.env](example.env).

| Group | Required values | Notes |
| --- | --- | --- |
| Job | `JOB_NAME`, `NAMESPACE`, `IMAGE`, `IMAGE_PULL_SECRET`, `SERVICE_ACCOUNT`, `CREDENTIALS_SECRET` | `JOB_NAME` is a DNS label and becomes the durable run ID. Leave `IMAGE_PULL_SECRET` empty for the public image. |
| ClickHouse | `CHHOST`, `CHPORT`, `CLUSTERNAME`, `EXPECTED_REPLICAS`, `COLLECTTABLEPREFIX` | Pin `CHHOST` to one replica for the entire cleanup. |
| Object store | `S3IP`, `S3PORT`, `S3BUCKET`, `S3PATH`, `S3REGION`, `S3SECURE_FLAG`, `S3DISKNAME`, `S3AUTH`, `S3PROFILE` | `S3PROFILE` requires `S3AUTH=aws`. |
| Limits | `SAMPLES`, `DELETE_BATCH_SIZE`, `USEAGE_HOURS`, `ACTIVE_DEADLINE_SECONDS`, `TTL_SECONDS_AFTER_FINISHED`, `MEMORY_REQUEST`, `MEMORY_LIMIT` | Keep `SAMPLES` unchanged after collect for partition pruning. |
| Behavior | `PHASE`, `DELETE_CONFIRMATION`, `ORDER_BY_OBJPATH`, `VERBOSE` | The renderer requires the confirmation token for delete and development automation. |

`USETOTAL` is optional and limits one use phase. `RUNLOG` defaults to `true`;
set it to `false` only when stdout is an adequate operational record.

## Durable run history

Pod logs are for monitoring, not durable evidence. The kubelet rotates them,
and the Job TTL removes the pod. By default, `s3gc` writes run events to
`<COLLECTTABLEPREFIX><S3DISKNAME>_log` in ClickHouse.

Use the same replica-pinned host to inspect a run:

```sql
SELECT event_time, phase, event, objects, bytes, message
FROM   <db>.<prefix><disk>_log
WHERE  run_id = '<job-name>'
ORDER BY event_time;
```

For a failed delete, inspect the latest `checkpoint` event before starting its
replacement Job. A run-log write failure falls back to stdout and does not stop
the cleanup.

## Development automation only

`PHASE=dev-automation` runs collect, dry-run, and delete in one Job. It starts
with a fresh inventory and requires `DELETE_CONFIRMATION=DELETE_ORPHANS`,
`CLUSTERNAME`, and `EXPECTED_REPLICAS`.

Use it only for isolated development fixtures. It bypasses the manual review
and approval gate, and it alone may use an age window below 24 hours.

## Preserved safeguards

- The manifest runs as a non-root user with a read-only root filesystem and no Linux capabilities.
- Credentials come from `CREDENTIALS_SECRET` or workload identity, never the image or environment file.
- The renderer requires a digest-pinned image and validates delete confirmation before the Job reaches the cluster.
- Delete requires the cluster/replica preflight, records checkpoints, and never retries automatically.
