# CLI and configuration reference

`s3gc` collects an object inventory and compares it with ClickHouse references.
For production, follow the [Kubernetes runbook](../deploy/kubernetes/README.md):
collect → dry-run → explicit human approval → delete → verify. This reference
describes the CLI, including options that the Kubernetes renderer does not expose.

## Main parameters

Start with these settings for a cleanup. The linked tables describe their
aliases, defaults, and environment variables.

- [ClickHouse host, cluster, and expected replicas](#clickhouse-connection-and-topology) identify the reference scope and gate a clustered delete.
- [Bucket, object prefix, and disk name](#object-store-and-authentication) identify the object-store scope and the ClickHouse disk to compare.
- [Collection database, table prefix, and samples](#collection-and-inventory) identify the inventory. Use `--collect-only` to build it and `--use-collected` to reuse it.
- [Dry-run, use age, and deletion limits](#candidate-processing-and-deletion) control the review and use phases.

Boolean environment and explicit-value options accept `true`/`false`,
`yes`/`no`, `on`/`off`, `1`/`0`, and the shorter `y`/`n` and `t`/`f`, ignoring
case. A bare flag such as `--dry-run` sets its value to true. Use its
explicit-value alias, such as `--dry-run-flag false`, to set it false. Where
aliases share a destination, the later CLI assignment wins. An unset optional
value is shown as `none`. An empty string default is shown as `empty`.

The following tables give every application parameter. The first spelling is
the preferred CLI form where a kebab-case alias exists. All other accepted
aliases are listed in the same cell. Environment variables use the destination
name, which can differ from the flag spelling. String parameters take a string.
Integer parameters take an integer. Defaults shown are CLI defaults, not values
from the Kubernetes example or renderer.

## ClickHouse connection and topology

| Option and aliases | Environment / config key | Default and purpose |
| --- | --- | --- |
| <a id="option-chhost"></a>`--ch-host`, `--chhost` | `S3GC_CHHOST` / `chhost` | `localhost`. String host. Pin every phase to the same replica, especially with the default node-local inventory. |
| <a id="option-chport"></a>`--ch-port`, `--chport` | `S3GC_CHPORT` / `chport` | `8123`. ClickHouse HTTP port. |
| <a id="option-chuser"></a>`--ch-user-name`, `--chuser`, `--chusername` | `S3GC_CHUSER` / `chuser` | `default`. ClickHouse user. |
| <a id="option-chpass"></a>`--ch-password`, `--chpass`, `--ch-pass` | `S3GC_CHPASS` / `chpass` | empty. ClickHouse password. Supply securely. |
| <a id="option-chtimeout"></a>`--ch-timeout`, `--chtimeout`, `--send-receive-timeout`, `--ch-send-receive-timeout` | `S3GC_CHTIMEOUT` / `chtimeout` | `1800`. Integer ClickHouse send/receive timeout in seconds. |
| <a id="option-clustername"></a>`--cluster-name`, `--cluster`, `--clustername` | `S3GC_CLUSTERNAME` / `clustername` | empty. String cluster whose replicas are queried for references. Empty uses the connected server only. It does not cover independent clusters. |
| <a id="option-expected_replicas"></a>`--expected-replicas` | `S3GC_EXPECTED_REPLICAS` / `expected_replicas` | none. Integer expected replica count. With a nonzero value, delete checks the cluster macro and reachable count before use and each sample. It requires a cluster name. Set a positive value for production. |
| <a id="option-replica_liveness_recheck"></a>`--replica-liveness-recheck` | `S3GC_REPLICA_LIVENESS_RECHECK` / `replica_liveness_recheck` | `false`. Bare boolean flag. For a Replicated auxiliary database, wait past the Keeper session timeout and re-check registered replica liveness before DDL. |
| <a id="option-replica_liveness_margin_ms"></a>`--replica-liveness-margin-ms` | `S3GC_REPLICA_LIVENESS_MARGIN_MS` / `replica_liveness_margin_ms` | `1000`. Nonnegative integer milliseconds added to that optional re-check. |

The Replicated-database preflight is distinct from the delete-time topology
preflight. If ClickHouse cannot report database liveness, or returns no replica
rows, it logs a warning and skips that database check. The Kubernetes delete
entrypoint requires both cluster name and expected replica count. A direct CLI
run does not impose that requirement when the count is unset.

## Object store and authentication

| Option and aliases | Environment / config key | Default and purpose |
| --- | --- | --- |
| <a id="option-s3ip"></a>`--s3-ip`, `--s3ip` | `S3GC_S3IP` / `s3ip` | `127.0.0.1`. String S3-compatible API hostname or IP. |
| <a id="option-s3port"></a>`--s3-port`, `--s3port` | `S3GC_S3PORT` / `s3port` | `9001`. API port. |
| <a id="option-s3bucket"></a>`--s3-bucket`, `--s3bucket` | `S3GC_S3BUCKET` / `s3bucket` | `root`. String bucket name. |
| <a id="option-s3path"></a>`--s3-path`, `--s3path` | `S3GC_S3PATH` / `s3path` | `data/`. String listing prefix. Empty selects the bucket root. Set the scope consistently across phases. |
| <a id="option-s3diskname"></a>`--s3-disk-name`, `--s3diskname` | `S3GC_S3DISKNAME` / `s3diskname` | `s3`. String ClickHouse disk name used in the reference join and inventory table name. Select the underlying object disk, not its cache disk. |
| <a id="option-s3auth"></a>`--s3-auth`, `--s3auth` | `S3GC_S3AUTH` / `s3auth` | `static`. `static`, `aws`, or `iam` (case-insensitive). Static uses explicit keys or anonymous access if both are empty. AWS uses boto3. IAM uses MinIO's refreshable workload-identity provider. |
| <a id="option-s3accesskey"></a>`--s3-access-key`, `--s3accesskey` | `S3GC_S3ACCESSKEY` / `s3accesskey` | empty. Static access key. Supply with the secret key. |
| <a id="option-s3secretkey"></a>`--s3-secret-key`, `--s3secretkey` | `S3GC_S3SECRETKEY` / `s3secretkey` | empty. Static secret key. Supply securely. |
| <a id="option-s3sessiontoken"></a>`--s3-session-token`, `--s3sessiontoken` | `S3GC_S3SESSIONTOKEN` / `s3sessiontoken` | empty. Optional static session token. Requires both static keys. |
| <a id="option-s3profile"></a>`--s3-profile`, `--s3profile` | `S3GC_S3PROFILE` / `s3profile` | empty. AWS profile name. A profile selects `aws` and conflicts with `iam`. AWS mode rejects explicit S3 keys. |
| <a id="option-s3region"></a>`--s3-region`, `--s3region` | `S3GC_S3REGION` / `s3region` | none. Optional string region. AWS mode can use its session region. |
| <a id="option-s3secure_flag"></a>`--s3-secure`, `--s3secure`. Explicit-value forms: `--s3-secure-flag`, `--s3secureflag` | `S3GC_S3SECURE_FLAG` / `s3secure_flag` | `false`. Boolean HTTPS selection. The current HTTP client is configured with certificate verification disabled. Do not interpret this option as a certificate-verification guarantee. |
| <a id="option-s3sslcertfile"></a>`--s3-ssl-cert-file`, `--s3sslcertfile` | `S3GC_S3SSLCERTFILE` / `s3sslcertfile` | empty. String path assigned to `SSL_CERT_FILE` when secure mode is enabled. See the certificate-verification limitation above. |
| <a id="option-s3_connect_timeout"></a>`--s3-connect-timeout` | `S3GC_S3_CONNECT_TIMEOUT` / `s3_connect_timeout` | `15`. Integer S3 connection timeout in seconds. |
| <a id="option-s3_read_timeout"></a>`--s3-read-timeout` | `S3GC_S3_READ_TIMEOUT` / `s3_read_timeout` | `120`. Integer S3 read timeout in seconds. |
| <a id="option-s3_retries"></a>`--s3-retries` | `S3GC_S3_RETRIES` / `s3_retries` | `3`. Integer HTTP retry count for transient S3 errors. |

HTTP retries happen within a running process. Batch deletion can also reconnect
and retry once after a transport failure. These retries are separate from
Kubernetes Job reruns, which the manifest disables.

Static mode does not automatically read the standard `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` variables. Supply the corresponding `S3GC_*` values
through a secret manager, or choose an appropriate AWS/IAM mode. Collection
requires listing permission. A dry-run using an existing inventory does not list
S3. A delete requires object-deletion permission.

## Collection and inventory

| Option and aliases | Environment / config key | Default and purpose |
| --- | --- | --- |
| <a id="option-collectonly_flag"></a>`--collect-only`, `--collectonly`. Explicit-value forms: `--collect-only-flag`, `--collectonlyflag` | `S3GC_COLLECTONLY_FLAG` / `collectonly_flag` | `false`. Boolean. List objects and populate the inventory without running the use phase. |
| <a id="option-usecollected_flag"></a>`--use-collected`, `--usecollected`. Explicit-value forms: `--use-collected-flag`, `--usecollectedflag` | `S3GC_USECOLLECTED_FLAG` / `usecollected_flag` | `false`. Boolean. Skip listing and use the existing inventory. See [Using a collected inventory](#using-a-collected-inventory). |
| <a id="option-collecttableprefix"></a>`--collect-table-prefix`, `--collecttableprefix` | `S3GC_COLLECTTABLEPREFIX` / `collecttableprefix` | `s3objects_for_`. String prefix for the inventory table. The disk name is appended. A `database.prefix_` form can qualify it. Use a unique prefix per cleanup scope. |
| <a id="option-collectdatabase"></a>`--collect-database`, `--collectdatabase` | `S3GC_COLLECTDATABASE` / `collectdatabase` | empty. Optional database for inventory and run log. If the prefix embeds a database, both names must agree. Without either, the connection's current database is used. |
| <a id="option-collectbatchsize"></a>`--collect-batch-size`, `--collectbatchsize` | `S3GC_COLLECTBATCHSIZE` / `collectbatchsize` | `1024`. Integer listing rows considered per insert batch. Use a positive value. |
| <a id="option-total"></a>`--collect-total`, `--total`, `--collecttotal`, `--total-num` | `S3GC_TOTAL` / `total` | none. Optional integer target for collected rows. The code subtracts whole batches and stops on exactly zero, so this is not a reliable strict limit when a batch overshoots. |
| <a id="option-collectafter"></a>`--collect-after`, `--collectafter` | `S3GC_COLLECTAFTER` / `collectafter` | none. Optional string key passed as the listing start-after marker. Collect does not persist a resume marker. |
| <a id="option-age"></a>`--age-hours`, `--age`, `--hours`, `--collectage`, `--collecthours` | `S3GC_AGE` / `age` | `0`. Integer hours. Collection includes listed objects whose age is at least this value. Separate from the use-phase age floor. |
| <a id="option-samples"></a>`--samples` | `S3GC_SAMPLES` / `samples` | `4`. Integer number of inventory partitions and use-phase samples. Use a positive value and keep it consistent across phases for partition pruning. |
| <a id="option-createdatabase_flag"></a>`--create-database`, `--createdatabase`. Explicit-value forms: `--create-database-flag`, `--createdatabase-flag` | `S3GC_CREATEDATABASE_FLAG` / `createdatabase_flag` | `false`. Boolean. Create the qualified inventory database during collection. A database name must be supplied. |
| <a id="option-drop_collecttable_flag"></a>`--drop-collecttable`, `--dropcollecttable`. Explicit-value forms: `--drop-collecttable-flag`, `--dropcollecttable-flag` | `S3GC_DROP_COLLECTTABLE_FLAG` / `drop_collecttable_flag` | `false`. Boolean. Drop and recreate the inventory during collection, losing prior inventory and checkpoints. Do not use when resuming a delete. |
| <a id="option-keepdata_flag"></a>`--keep-data`, `--keepdata`. Explicit-value forms: `--keep-data-flag`, `--keepdataflag` | `S3GC_KEEPDATA_FLAG` / `keepdata_flag` | `false`. Boolean. Preserve the inventory after a successful delete. Without it, a successful non-dry use truncates the table. Dry-run does not truncate it. |

## Candidate processing and deletion

| Option and aliases | Environment / config key | Default and purpose |
| --- | --- | --- |
| <a id="option-dryrun_flag"></a>`--dry-run`, `--dryrun`. Explicit-value forms: `--dry-run-flag`, `--dryrun-flag`, `--dryrunflag` | `S3GC_DRYRUN_FLAG` / `dryrun_flag` | `false`. Boolean. Report candidates without deleting objects. Without `--use-collected`, collection still runs and writes the inventory. |
| <a id="option-useage"></a>`--useage-hours`, `--useage`, `--usehours` | `S3GC_USEAGE` / `useage` | `24`. Integer hours. Use selects inventory rows older than this age. Below 24 is refused for preview and delete except with the development-only override. |
| <a id="option-useafter"></a>`--use-after`, `--useafter` | `S3GC_USEAFTER` / `useafter` | none. Optional string. Process inventory keys lexically greater than this marker. |
| <a id="option-usetotal"></a>`--use-total`, `--usetotal` | `S3GC_USETOTAL` / `usetotal` | none. Optional integer SQL limit **per sample**, not a global run limit. A positive value can bound a trial. Unset it for a full run and verification. |
| <a id="option-deletebatchsize"></a>`--delete-batch-size`, `--deletebatchsize` | `S3GC_DELETEBATCHSIZE` / `deletebatchsize` | `1000`. Positive integer objects per deletion/checkpoint batch. |
| <a id="option-order_by_objpath"></a>`--order-by-objpath`. Explicit-value forms: `--order-by-objpath-flag` | `S3GC_ORDER_BY_OBJPATH` / `order_by_objpath` | `false`. Boolean. Order candidates by key within each sample. Sorting can be expensive. |
| <a id="option-use_remove_objects"></a>`--use-remove-objects`, `--useremoveobjects` | `S3GC_USE_REMOVE_OBJECTS` / `use_remove_objects` | `true`. Boolean. This option requires a value. Use batch S3 deletion when true, per-object deletion when false. The recognized GCS endpoint switches to per-object deletion automatically. |
| <a id="option-interactive_flag"></a>`--non-interactive`, `--noninteractive`. Explicit-value forms: `--interactive-flag` | `S3GC_INTERACTIVE_FLAG` / `interactive_flag` | `true`. Boolean controls the CLI prompt. `--non-interactive` sets false. The prompt appears only when both stdin and stdout are terminals. A non-terminal run does not prompt even with the default. The Kubernetes token gate is separate. |
| <a id="option-dev_allow_short_useage"></a>`--dev-allow-short-useage` | `S3GC_DEV_ALLOW_SHORT_USEAGE` / `dev_allow_short_useage` | `false`. Boolean. This option requires a value. Permits a use age below 24 hours for isolated development fixtures only. Never use for customer or production data. |

## Using a collected inventory

`--use-collected` skips the S3 listing and reads an existing inventory table.
Use it after a normal collect or after importing a reviewed candidate list.
The [manual shared-bucket example](#manual-inventory-for-a-bucket-shared-by-independent-clusters)
shows how to prepare that list when independent clusters share a bucket.

Each use phase checks the inventory against current ClickHouse references for
the configured cluster and disk. It also applies the age filter to the stored
timestamp. It does not refresh S3 metadata or check independent clusters. New
keys are missing from an old inventory, and overwritten keys can retain old
timestamps. A missing or empty inventory causes the run to fail.

Use the same pinned ClickHouse host and cleanup settings for every phase. The
default inventory table is node-local. Its name is
`<collecttableprefix><s3diskname>` in the chosen database. Start a new scope
with a unique table because collection adds rows to an existing table.

These examples are for an isolated development environment. Set the scope as
shown in the [README](../README.md#local-setup-and-read-only-preview) and
provide credentials securely. Use the [Kubernetes runbook](../deploy/kubernetes/README.md)
for production.

```bash
# 1. Collect into a new, unique inventory table.
.venv/bin/python s3gc.py --collect-only --keep-data --verbose

# 2. Review candidates in that inventory.
.venv/bin/python s3gc.py --use-collected --dry-run --verbose

# 3. Run from a terminal after review and explicit approval.
.venv/bin/python s3gc.py --use-collected --keep-data --verbose

# 4. Verify the same inventory, without use-total or use-after limits.
.venv/bin/python s3gc.py --use-collected --dry-run --verbose
```

The CLI prompts only when stdin and stdout are terminals. Get explicit human
approval before a non-interactive delete too. Keep the table after delete so
its checkpoints remain available for recovery and verification. If delete
fails, inspect the run log and confirm that the scope and approval still apply
before retrying. Do not re-collect or drop the table during recovery.

A zero-candidate verification covers only this inventory and its filters. It
does not check objects absent from the inventory or prove the bucket is empty.

## Manual inventory for a bucket shared by independent clusters

When different ClickHouse clusters share an overlapping bucket/prefix, use a
manually reviewed inventory. A normal clustered s3gc comparison covers the
configured cluster, not other independent clusters. Putting the full shared
listing directly into the s3gc inventory would let objects referenced only by
another cluster appear orphaned.

First export the listing and compare it with references from every consumer.
Import only the reviewed candidates into the inventory table. Then use
`--use-collected` to preview, delete, and verify that table without listing S3
again. The steps below show the complete workflow.

The outline below uses an AWS-compatible listing, CSV staging, and a dedicated
replicated candidate table. Adapt endpoints, authentication, names, and SQL to
the installation. It requires a complete view of every cluster, replica, disk,
and any other consumer of the shared scope. If that view cannot be established,
stop. Coordinate all consumers so that references cannot change between the
final cross-cluster review and deletion. This coordination is outside s3gc.
Work in a private directory outside Git, use least-privilege credentials, and
keep the raw listing separate from the candidate table.

### 1. Export the full scoped listing

For an AWS-compatible endpoint, the following example uses the installed boto3
dependency and its paginated `list_objects_v2` API. Configure the SDK's
credentials and region outside the script. For another S3-compatible endpoint,
configure the session/endpoint as appropriate. Set `S3GC_RUN_DIR` to an existing
private directory outside the repository, and set `S3GC_S3BUCKET` and
`S3GC_S3PATH` to the reviewed scope. An empty prefix lists the bucket root.
The CSV has one row per object and preserves the object-store size and UTC
last-modified time. Do not fabricate or backdate timestamps: s3gc's age guard
uses the imported value.

```bash
.venv/bin/python - > "$S3GC_RUN_DIR/listing.csv" <<'PY'
import csv, datetime, os, sys
import boto3

writer = csv.writer(sys.stdout)
writer.writerow(['objpath', 'size', 'last_modified'])
s3 = boto3.client('s3')
pages = s3.get_paginator('list_objects_v2').paginate(
    Bucket=os.environ['S3GC_S3BUCKET'], Prefix=os.environ['S3GC_S3PATH'])
for page in pages:
    for obj in page.get('Contents', []):
        modified = obj['LastModified'].astimezone(datetime.timezone.utc)
        writer.writerow([obj['Key'], obj['Size'], modified.strftime('%Y-%m-%d %H:%M:%S')])
PY
```

Check that the command succeeded, pagination completed, and the row count,
bucket, prefix, object keys, sizes, and timestamps are credible. The export is
a point-in-time listing, not an atomic snapshot of all consumers. Preserve its
provenance and obtain a new listing if the scope or assumptions change.

### 2. Stage the listing and collect all references

The examples use a pre-existing `s3gc` database and a private, per-replica
ClickHouse connection configured in your client. Use unique staging names for
the run. Create staging tables on the chosen host. These are **not** the table
that s3gc will use:

```sql
CREATE TABLE s3gc.manual_listing
    (objpath String, size Int64, last_modified DateTime('UTC'))
    ENGINE = MergeTree ORDER BY objpath;
CREATE TABLE s3gc.manual_references (objpath String)
    ENGINE = MergeTree ORDER BY objpath;
```

Import the listing into staging through the same pinned host:

```bash
clickhouse-client --host '<pinned-replica>' \
  --query 'INSERT INTO s3gc.manual_listing FORMAT CSVWithNames' \
  < "$S3GC_RUN_DIR/listing.csv"
```

For **each** independent cluster, connect to that cluster and export references
from every replica, using the disk names that map to this bucket/prefix on that
cluster. The names can differ between clusters. For example, after replacing
the placeholders with the cluster's actual names:

```bash
clickhouse-client --host '<cluster-replica>' \
  --query "SELECT DISTINCT remote_path AS objpath
           FROM clusterAllReplicas('<cluster-name>', system.remote_data_paths)
           WHERE disk_name IN ('<object-disk-name>')
           SETTINGS skip_unavailable_shards = 0 FORMAT CSVWithNames" \
  > "$S3GC_RUN_DIR/references-cluster-a.csv"
```

Check the expected replica count, successful query completion, disk-to-bucket
mapping, and key format for each cluster. Include every relevant disk and
non-ClickHouse consumer in the review. If a replica or consumer is unavailable,
the exports are incomplete. Stop rather than treating missing references as
orphans. Import each verified reference CSV into `manual_references` on the
pinned staging host, using a distinct file per source:

```bash
clickhouse-client --host '<pinned-replica>' \
  --query 'INSERT INTO s3gc.manual_references FORMAT CSVWithNames' \
  < "$S3GC_RUN_DIR/references-cluster-a.csv"
```

Repeat for every cluster. Add references from other consumers in the same
one-column `objpath` format. Confirm that keys are comparable to the S3 listing
and verify import counts. Duplicate references are harmless for the anti-join.

### 3. Review candidates and load a replicated inventory

Generate a candidate CSV from the staging tables. Only rows absent from the
union of all reference exports enter it. Preserve listing metadata and mark rows
active. Review keys, counts, total bytes, timestamps, scope, and the source
exports before importing. Exclude anything whose ownership is uncertain.

```bash
clickhouse-client --host '<pinned-replica>' \
  --query "SELECT l.objpath, l.size, l.last_modified, true AS active
           FROM s3gc.manual_listing AS l
           LEFT ANTI JOIN s3gc.manual_references AS r ON l.objpath = r.objpath
           FORMAT CSVWithNames" > "$S3GC_RUN_DIR/reviewed-candidates.csv"
```

Create a **new, empty** auxiliary table with the schema and replacement key
expected by s3gc. This example uses `S3GC_COLLECTDATABASE=s3gc`,
`S3GC_COLLECTTABLEPREFIX=manual_cleanup_`, `S3GC_S3DISKNAME=s3`, and
`S3GC_SAMPLES=4`. Use a unique Keeper path and a cluster with correctly
configured `{shard}` and `{replica}` macros. Review the DDL for the installation.

Check the database engine before choosing how to create the table:

```sql
SELECT engine FROM system.databases WHERE name = 's3gc';
```

The `CREATE TABLE` example below uses `ON CLUSTER` for an Atomic database.
If `s3gc` uses `ENGINE = Replicated`, omit `ON CLUSTER '<cleanup-cluster>'`.
That database distributes table DDL itself. See the
[Replicated database documentation](https://clickhouse.com/docs/engines/database-engines/replicated).

The table engine replicates data within a shard. It does not automatically
share rows across independent shards or clusters. Table replication is separate
from a Replicated database's DDL replication. See ClickHouse's
[replicated table engine documentation](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication).

```sql
CREATE TABLE s3gc.manual_cleanup_s3 ON CLUSTER '<cleanup-cluster>'
(
    objpath String,
    size Int64,
    last_modified DateTime('UTC'),
    active Bool
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/s3gc_manual_cleanup_s3', '{replica}')
PARTITION BY CRC32(objpath) % 4
ORDER BY objpath;
```

Import only the reviewed candidate CSV on the pinned execution replica:

```bash
clickhouse-client --host '<pinned-replica>' \
  --query 'INSERT INTO s3gc.manual_cleanup_s3 FORMAT CSVWithNames' \
  < "$S3GC_RUN_DIR/reviewed-candidates.csv"
```

Compare the imported keys, metadata, count, and total bytes with the reviewed
CSV. Confirm replication has caught up where the table will be read. Replication
is asynchronous, so keep all s3gc phases pinned to the import replica.
Resolve any partial import before proceeding. An empty candidate table cannot
be passed to the use phase. If the review found no candidates, record that
result instead of starting delete.

The normal s3gc collector creates a node-local table. You must create the
replicated table yourself for this workflow.

### 4. Preview, approve, delete, and verify

Keep the same database, table prefix, disk, bucket, object prefix, execution
cluster, expected replicas, sample count, and age window. Refresh references
from **all** consumers immediately before delete, and verify that candidate
objects still have the reviewed metadata. Maintain the agreed period in which
references cannot change and candidate keys cannot be overwritten or reused.
Then obtain explicit approval for the reviewed candidate set. If any
cross-cluster reference, candidate metadata, or scope changes, stop and rebuild
and re-review the candidate inventory. s3gc rechecks only the configured
cluster during use. It does not recheck other clusters or S3 metadata for you.

Run the Kubernetes `dry-run`, approved `delete`, and `dry-run` verification
phases against the existing table. They already select `--usecollected`. Do not
run the Kubernetes collect phase or set the fresh-run drop flag on this table.
Keep the same pinned host and inventory for recovery and verification. A zero
result applies to the reviewed imported inventory and current configured-cluster
filters, not to a newly generated bucket listing. Preserve review evidence and
clean up private staging artifacts according to your retention policy.

## Advanced: configuration files and precedence

The tables above list each parameter's configuration key. Use those keys in a
JSON or YAML file passed with `--cfg`. For example, this file selects a replica
and enables a preview:

```json
{
  "chhost": "replica.example.invalid",
  "collectdatabase": "s3gc",
  "collecttableprefix": "example_",
  "dryrun_flag": true
}
```

Pass `--cfg /path/to/config.json`. The parser starts with defaults, applies
environment values, then processes command-line arguments in order. A `--cfg`
file is applied at its position on the command line: it can override an earlier
CLI option, and a later CLI option can override the file. Multiple files and
options follow the same ordering. `S3GC_CFG` can also select a config file.
The parser applies other environment variables after that environment-selected
file. Keep secrets outside config files and shell history.

`--help` (`-h`) prints CLI help. `--print_config` prints the effective parsed
configuration and exits. **Its output can contain credentials**, so do not
share or commit it. `--list-options` (`--listoptions`) prints environment-style
options and redacts configured credentials. These inspection options do not
contact ClickHouse or S3. `--cfg` has environment variable `S3GC_CFG`. The help
and print-config actions have no documented environment equivalent. The
list-options variable is `S3GC_LISTOPTIONS` (boolean, default `false`).


## Advanced: logging and inspection

| Option and aliases | Environment / config key | Default and purpose |
| --- | --- | --- |
| <a id="option-verbose_flag"></a>`--verbose`. Explicit-value forms: `--verbose-flag`, `--verboseflag` | `S3GC_VERBOSE_FLAG` / `verbose_flag` | `false`. Boolean. Enable informational logs. |
| <a id="option-debug_flag"></a>`--debug`. Explicit-value forms: `--debug-flag`, `--debugflag` | `S3GC_DEBUG_FLAG` / `debug_flag` | `false`. Boolean. Enable more detailed logs, including per-object output. |
| <a id="option-silent_flag"></a>`--silent`. Explicit-value forms: `--silent-flag`, `--silentflag` | `S3GC_SILENT_FLAG` / `silent_flag` | `false`. Boolean. Restrict console logging to critical messages. |
| <a id="option-runlog_flag"></a>`--run-log`, `--runlog` | `S3GC_RUNLOG_FLAG` / `runlog_flag` | `true`. Boolean. This option requires a value. Write durable run events alongside the inventory. Logging failure warns and falls back to stdout. |
| <a id="option-runid"></a>`--run-id`, `--runid` | `S3GC_RUNID` / `runid` | empty. String identifier for run-log rows. Empty generates a timestamped ID. Kubernetes supplies the Job name. |
| <a id="option-listoptions"></a>`--list-options`, `--listoptions` | `S3GC_LISTOPTIONS` / `listoptions` | `false`. Boolean. Print environment-style options with credentials redacted and exit. |
| <a id="option-cfg"></a>`--cfg` | `S3GC_CFG` / `cfg` | none. Configuration file path. Place before CLI overrides. |
| <a id="option-help"></a>`--help`, `-h` | none | Print help and exit. |
| <a id="option-print_config"></a>`--print_config` | none | Print effective configuration and exit. Output may contain secrets. |
