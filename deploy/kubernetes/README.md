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

Use `.venv/bin/python` for these commands. The renderer itself needs only the
Python standard library.

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

Set these values in `s3gc.env` before rendering:

| Setting | What to check |
| --- | --- |
| `CHHOST` | Use one per-replica ClickHouse Service for every phase. The default inventory is node-local. A load-balanced Service can reach a replica without it. |
| `S3BUCKET`, `S3PATH`, `S3DISKNAME` | Match the exact bucket, object prefix, and underlying disk. An empty `S3PATH` selects the bucket root. Use `gcs` for a disk named `gcs`, never a `*_cache` wrapper. |
| `CLUSTERNAME`, `EXPECTED_REPLICAS` | Match the actual cluster and total replica count. Delete stops if the preflight result differs. |
| `COLLECTDATABASE`, `COLLECTTABLEPREFIX` | Use the auxiliary database and a unique prefix such as `s3gc_<run>_`. Keep both unchanged during recovery. |
| `USEAGE_HOURS` | Use 24 or more. Raise it for slow merges or long mutations. Production phases reject a lower value. |

Without `COLLECTDATABASE`, the inventory uses the ClickHouse user's current
database. That database may differ from the one you granted access to.
The older `db.prefix_` form still works in `COLLECTTABLEPREFIX`, but its database
must agree with `COLLECTDATABASE` when both are set.

For a cleanup across one installation, collect the parent prefix that holds all
replicas' objects, such as `clickhouse/<installation>/`. Set `CLUSTERNAME` so
the comparison reads references from every configured replica. Collecting only
`<installation>/<replica>/` misses the other replicas' orphans.

The selected prefix must contain objects for only the chosen disk. References
are matched by disk name, so another disk's objects could look orphaned. Use a
disk-specific prefix when possible. For intentional per-replica runs, give each
prefix its own inventory table.

If independent clusters or other consumers share the selected scope, follow
the [manual inventory workflow](../../docs/cli-reference.md#manual-inventory-for-a-bucket-shared-by-independent-clusters).
Check every consumer before importing candidates. The configured cluster check
cannot see references in other clusters. After importing the reviewed inventory,
start with the dry-run phase and skip collection.

Use an immutable multi-architecture image digest in `IMAGE`. CI prints the
exact value after publishing. Do not use an image tag.

Collection has no resume checkpoint. For a large bucket, collect separate
prefix shards and rerun only a failed shard. The inventory replaces rows with
the same object path. Do this before deletion, not while recovering a delete.

### 3. Provide credentials and identity

Create or reuse the Kubernetes Secret named by `CREDENTIALS_SECRET`:

| `S3AUTH` | Secret values |
| --- | --- |
| `static` | `S3GC_CHUSER`, `S3GC_CHPASS`, `S3GC_S3ACCESSKEY`, `S3GC_S3SECRETKEY`, and an optional `S3GC_S3SESSIONTOKEN` |
| `iam` | `S3GC_CHUSER` and `S3GC_CHPASS` |
| `aws` | `S3GC_CHUSER`, `S3GC_CHPASS`, plus a credential source available to boto3 |

Set `SERVICE_ACCOUNT` to an existing ServiceAccount in the namespace. For `iam`,
the platform must provide workload-identity credentials for that account.
The Job keeps `automountServiceAccountToken: false` because it does not call the
Kubernetes API. An existing ClickHouse or ClickHouse-backup ServiceAccount is
suitable if it meets the cluster's identity policy.

Create a dedicated auxiliary database and ClickHouse user before the first run.
Replace `<cluster-name>`, `<auxiliary-database>`, and the password placeholder.
Set the same `<auxiliary-database>` as `COLLECTDATABASE`.

```sql
CREATE DATABASE IF NOT EXISTS <auxiliary-database>
ON CLUSTER <cluster-name>;

CREATE USER IF NOT EXISTS s3gc
ON CLUSTER <cluster-name>
IDENTIFIED WITH sha256_password BY '<strong-password>';

GRANT ON CLUSTER <cluster-name>
    SELECT ON system.remote_data_paths,
    SELECT ON system.one,
    SELECT ON system.clusters TO s3gc;

GRANT ON CLUSTER <cluster-name>
    SELECT, INSERT, CREATE TABLE ON <auxiliary-database>.* TO s3gc;

GRANT ON CLUSTER <cluster-name>
    REMOTE ON *.* TO s3gc;
```

The system-table grants are by name, so the user cannot read `system.query_log`
or other tables that may hold restricted data. `system.remote_data_paths` is the
reference s3gc compares the inventory with, `system.one` backs the replica-count
preflight of the delete phase, and `system.clusters` backs the replica-liveness
check for a Replicated auxiliary database. s3gc also reads `system.databases`
and `system.tables`; on the version this was checked on (26.8) any user can
read those two, limited to the objects it has access to, so they need no grant.

The database grant covers the auxiliary inventory and durable run-log tables.
`REMOTE` permits the `clusterAllReplicas` reads used by clustered dry-run and
delete preflight. These grants support the normal phases, which preserve the
inventory after delete. Optional DROP or TRUNCATE operations need their own
grants.

For a Replicated auxiliary database, every phase checks replica liveness before
DDL. If ClickHouse cannot report liveness or returns no replicas, s3gc warns and
skips that check. An optional delayed re-check uses the Keeper session timeout
reported by the cluster. See the [CLI preflight options](../../docs/cli-reference.md#clickhouse-connection-and-topology).
The optional delayed re-check also reads `system.zookeeper_connection`. Grant
`SELECT ON system.zookeeper_connection` only if you pass
`--replica-liveness-recheck`; without the grant that flag warns and skips the
re-check.

s3gc uses its own S3 client. It does not need ClickHouse's `S3` source privilege.

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
# export S3GC_S3SESSIONTOKEN='<S3 session token>'  # only for temporary credentials
```

Create the Secret. This command creates it once and refuses to overwrite an
existing Secret:

```bash
set -a
. /private/path/s3gc-secrets.env
set +a

literals=(
  --from-literal=S3GC_CHUSER="$S3GC_CHUSER"
  --from-literal=S3GC_CHPASS="$S3GC_CHPASS"
  --from-literal=S3GC_S3ACCESSKEY="$S3GC_S3ACCESSKEY"
  --from-literal=S3GC_S3SECRETKEY="$S3GC_S3SECRETKEY"
)
if [ -n "${S3GC_S3SESSIONTOKEN:-}" ]; then
  literals+=(--from-literal=S3GC_S3SESSIONTOKEN="$S3GC_S3SESSIONTOKEN")
fi

kubectl -n '<namespace>' create secret generic '<credentials-secret-name>' \
  "${literals[@]}"

unset S3GC_CHUSER S3GC_CHPASS S3GC_S3ACCESSKEY S3GC_S3SECRETKEY S3GC_S3SESSIONTOKEN
```

The S3 principal needs `s3:ListBucket` on the scoped bucket/prefix for
collection. A delete Job additionally needs `s3:DeleteObject` on the scoped
object keys. Keep delete permission separate until an approved delete phase.

### 4. Run each phase

For each phase, change the values in the table below. Render the Job, validate
it against the API server, apply it, and follow its logs. Replace the
placeholders before running these commands:

```bash
.venv/bin/python deploy/kubernetes/render.py "$S3GC_RUN_DIR/s3gc.env" \
  > "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply --dry-run=server -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl apply -f "$S3GC_RUN_DIR/<phase>.yaml"
kubectl -n '<namespace>' logs -f 'job/<job-name>'
```

| Phase | Change in `s3gc.env` | Required outcome |
| --- | --- | --- |
| Collect | Set `PHASE=collect` and a new `JOB_NAME`. | Inventory S3 objects. No deletion occurs. |
| Review | Set `PHASE=dry-run` and a new `JOB_NAME`. | Review the candidate count, bytes, bucket, and prefix. |
| Delete | After written human approval, set `PHASE=delete`, a new `JOB_NAME`, and `DELETE_CONFIRMATION=DELETE_ORPHANS`. | Delete and checkpoint confirmed batches. |
| Verify | Set `PHASE=dry-run`, a new `JOB_NAME`, and clear `DELETE_CONFIRMATION`. | Report zero candidates. |

Do not run delete from an unreviewed dry-run. Before the first full delete for
a new image or cluster, set `USETOTAL` to a few thousand objects per sample
for a bounded trial delete. Clear it before the full run and verification.
The use-phase query applies this limit separately to each sample, so it is not
a global cap on the Job.

The entrypoint selects the CLI flags for each phase:

| Phase | CLI flags |
| --- | --- |
| Collect | `--collectonly --keepdata` |
| Dry-run and verify | `--usecollected --dry-run` |
| Delete | `--usecollected --keepdata --non-interactive`, after the token and cluster-input gates |

See the [CLI reference](../../docs/cli-reference.md) for parameter details.

### 5. Recover or verify

If a delete Job fails, create a replacement delete Job with a new `JOB_NAME`
and the same host, scope, and inventory settings. Inspect the failure and confirm
that the approval still applies first. Do not re-collect or drop the inventory
during recovery. A changed scope needs a new inventory, review, and approval.
`backoffLimit: 0` prevents automatic Job reruns. Individual S3 requests can still
retry within the running process.

After a successful delete, run the verification dry-run. Do not declare the
cleanup complete until it reports zero candidates with `USETOTAL` cleared.
That result covers the reviewed inventory and age window. It does not check
new objects absent from the inventory or prove the bucket is empty.

## Configuration reference

`render.py` reads a non-secret `KEY=VALUE` file. It rejects missing required
values, unpinned images, invalid phases, invalid booleans, and unsafe age
windows. Start with [example.env](example.env).

Renderer keys differ from CLI environment names. For example, `USEAGE_HOURS`
becomes `S3GC_USEAGE`, and `RUNLOG` becomes `S3GC_RUNLOG_FLAG`.

| Group | Required values | Notes |
| --- | --- | --- |
| Job | `JOB_NAME`, `NAMESPACE`, `IMAGE`, `IMAGE_PULL_SECRET`, `SERVICE_ACCOUNT`, `CREDENTIALS_SECRET` | `JOB_NAME` is a DNS label and becomes the durable run ID. Leave `IMAGE_PULL_SECRET` empty for the public image. |
| ClickHouse | `CHHOST`, `CHPORT`, `CLUSTERNAME`, `EXPECTED_REPLICAS`, `COLLECTTABLEPREFIX` | Pin `CHHOST` to one replica for the entire cleanup. `COLLECTDATABASE` (optional) qualifies the auxiliary/run-log tables' database. |
| Object store | `S3IP`, `S3PORT`, `S3BUCKET`, `S3PATH`, `S3REGION`, `S3SECURE_FLAG`, `S3DISKNAME`, `S3AUTH`, `S3PROFILE` | `S3PROFILE` requires `S3AUTH=aws`. |
| Limits | `SAMPLES`, `DELETE_BATCH_SIZE`, `USEAGE_HOURS`, `ACTIVE_DEADLINE_SECONDS`, `TTL_SECONDS_AFTER_FINISHED`, `MEMORY_REQUEST`, `MEMORY_LIMIT` | Keep `SAMPLES` unchanged after collect for partition pruning. |
| Behavior | `PHASE`, `DELETE_CONFIRMATION`, `ORDER_BY_OBJPATH`, `VERBOSE` | The renderer requires the confirmation token for delete and development automation. |

`USETOTAL` is optional and limits each sample in a use phase. `RUNLOG` defaults
to `true`. Set it to `false` only when stdout is an adequate operational record.

## Durable run history

The kubelet rotates pod logs, and the Job TTL removes the pod. By default,
`s3gc` also writes durable run events to
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

Use it only for isolated development fixtures. It runs all three phases without
a pause for manual review. The renderer allows an age window below 24 hours
only for this phase, and the entrypoint passes the development override.
Never use it for customer or production data.

## Preserved safeguards

- The manifest runs as a non-root user with a read-only root filesystem and no Linux capabilities.
- Credentials come from `CREDENTIALS_SECRET` or workload identity, never the image or environment file.
- The renderer requires a digest-pinned image and validates delete confirmation before the Job reaches the cluster.
- A delete Job requires the cluster/replica preflight and records checkpoints. A failed Job is not rerun automatically.
