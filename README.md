# s3gc

`s3gc` finds orphaned objects on a ClickHouse S3 disk or compatible object
store. It collects an inventory into ClickHouse, compares it with
`system.remote_data_paths`, and reports objects that ClickHouse does not
reference.

For customer and production work, use the Kubernetes runbook:

```text
collect → dry-run → explicit human approval → delete → verify
```

Read the [Kubernetes Job runner](deploy/kubernetes/README.md) before you run a
cleanup. It is the authoritative production procedure.

## Safety boundary

Deleting an object is irreversible. `s3gc` preserves the following controls:

- A delete Job needs the `DELETE_ORPHANS` confirmation token.
- A clustered delete checks the configured cluster and expected replica count
  before it calls S3.
- A failed delete Job does not retry automatically. Confirmed batches remain
  checkpointed, so a replacement Job can resume with the same collection-table
  prefix.
- `USEAGE` must be at least 24 hours outside development automation. This age
  window protects a part that S3 has received before ClickHouse registers it.
- The Kubernetes renderer requires a digest-pinned image and keeps credentials
  outside the manifest.

For an installation-wide cleanup, collect the parent prefix that holds every
replica's objects and set the cluster name, so the comparison covers all
replicas. A single-replica prefix misses the other replicas' orphans.

Use a unique collection-table prefix for each bucket and prefix. Never commit
credentials, rendered manifests, target-cluster details, or run output.

## Local setup and read-only preview

Install the pinned dependencies and inspect the available options:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python s3gc.py --help
```

Set configuration with flags or `S3GC_*` environment variables. A minimum
non-secret setup looks like this:

```bash
export S3GC_CHHOST='<per-replica-clickhouse-host>'
export S3GC_CHPORT=8123
export S3GC_CHUSER='<clickhouse-user>'
export S3GC_S3IP='s3.eu-central-1.amazonaws.com'
export S3GC_S3PORT=443
export S3GC_S3BUCKET='<bucket>'
export S3GC_S3PATH='<target-prefix>/'
export S3GC_S3REGION='eu-central-1'
export S3GC_S3SECURE_FLAG=true
export S3GC_S3DISKNAME=s3
export S3GC_CLUSTERNAME='<clickhouse-cluster>'
export S3GC_EXPECTED_REPLICAS=2
export S3GC_COLLECTTABLEPREFIX='s3gc_example_'
export S3GC_USEAGE=24
```

Inject passwords and keys from a secret manager or your shell. Do not save them
in a file or command history. Then run a preview:

```bash
.venv/bin/python s3gc.py --verbose --dry-run
```

Use direct deletion only for controlled development work. The Kubernetes
runbook separates collection, review, approval, deletion, and verification.

## Authentication and object stores

Choose one S3 authentication mode with `S3GC_S3AUTH`:

| Mode | Credentials | Use it for |
| --- | --- | --- |
| `static` | `S3GC_S3ACCESSKEY` and `S3GC_S3SECRETKEY`; optional session token | explicit credentials from a secret manager |
| `aws` | boto3 chain; optional `S3GC_S3PROFILE` | AWS SSO or a named workstation profile |
| `iam` | MinIO workload-identity provider | EKS IRSA, EC2 instance profiles, or ECS task roles |

Static mode does not read `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY`. Pass
those values as `S3GC_S3ACCESSKEY` and `S3GC_S3SECRETKEY`.

`S3GC_S3PROFILE` selects `aws`; the tool rejects contradictory settings. An
`aws` preview still needs `s3:ListBucket` for the configured bucket and prefix.

GCS uses HMAC interoperability keys and usually names the object disk `gcs`.
It does not support S3 batch deletion, so `s3gc` falls back to slower
per-object deletion. Set `S3GC_S3DISKNAME=gcs`; do not use a `*_cache` disk as
the reference scope.

## Kubernetes deployment

The Kubernetes runner is in [deploy/kubernetes](deploy/kubernetes/). It uses a
one-shot Job, a non-secret environment file, and a Kubernetes Secret or
workload identity for credentials.

Start with these links:

1. [Run the production workflow](deploy/kubernetes/README.md#runbook)
2. [Configure the Job](deploy/kubernetes/README.md#configuration-reference)
3. [Copy the non-secret example](deploy/kubernetes/example.env)

Released images are public at `ghcr.io/altinity/s3gc`. Always use a digest:

```bash
docker pull ghcr.io/altinity/s3gc@sha256:<digest>
```

CI publishes the exact `IMAGE=` value in its job summary. The renderer rejects
mutable image tags.

## Durable run history

By default, each run writes structured events to
`<COLLECTTABLEPREFIX><S3DISKNAME>_log` in ClickHouse. The table records phase
starts, collect progress, deletion checkpoints, totals, warnings, and errors.
It outlives the Job and rotated pod logs.

Query one run on the same replica-pinned ClickHouse host:

```sql
SELECT event_time, phase, event, objects, bytes, message
FROM   <db>.<prefix><disk>_log
WHERE  run_id = '<job-name>'
ORDER BY event_time;
```

Set `RUNLOG=false` only when stdout is an adequate record. A run-log failure
falls back to stdout and does not stop a cleanup.

## AI-agent skill

[`skills/altinity-clickhouse-s3gc`](skills/altinity-clickhouse-s3gc/) gives an
agent the same safety boundaries as the Kubernetes runbook. Install it by
symlinking or copying the directory into the agent's skills directory. For
Codex:

```bash
mkdir -p ~/.codex/skills
ln -s /path/to/s3gc/skills/altinity-clickhouse-s3gc \
      ~/.codex/skills/altinity-clickhouse-s3gc
```

The skill never supplies the delete confirmation. An authorized human must
approve the reviewed dry-run result.

## AI coding workflow

`CLAUDE.md` (full guide) and `AGENTS.md` (short pointer) tell AI coding
agents how to work here. For a new feature, bug fix, or behaviour change:

1. **Spec Intake** — if the request is underspecified, the agent asks what
   should happen instead, how to trigger it, whether it touches the delete
   lifecycle, and known edge cases.
2. **Spec** — a short problem statement and acceptance criteria, in the
   commit that starts the change.
3. **TDD** — a failing test per acceptance criterion, then made to pass.

Editorial changes (docs, comments) skip this. See `CLAUDE.md` for the full
rules.

## Development checks

Run offline tests:

```bash
.venv/bin/python -m pytest -v -m "not dev_cluster"
```

Render and validate the example Job without contacting a cluster:

```bash
.venv/bin/python deploy/kubernetes/render.py deploy/kubernetes/example.env > /tmp/s3gc-job.yaml
docker run --rm --entrypoint /kubeconform -v /tmp:/tmp:ro \
  ghcr.io/yannh/kubeconform@sha256:85dbef6b4b312b99133decc9c6fc9495e9fc5f92293d4ff3b7e1b30f5611823c \
  -strict -summary /tmp/s3gc-job.yaml
```

See [CLAUDE.md](CLAUDE.md) for contributor requirements,
[CHANGELOG.md](CHANGELOG.md) for operational history, and [TODO.md](TODO.md)
for deferred work.
