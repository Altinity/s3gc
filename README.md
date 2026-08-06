# s3gc

`s3gc` finds and removes orphaned objects from ClickHouse S3 disks and other
S3-compatible storage. An object is a candidate only when it exists under the
configured bucket/prefix but is absent from ClickHouse
`system.remote_data_paths` for the configured disk.

## How it works

1. Collect object names, sizes, and timestamps into an auxiliary ClickHouse
   table.
2. Anti-join that inventory with `system.remote_data_paths` (or all replicas of
   a configured cluster).
3. Report candidates in dry-run mode, or delete them in batches and record
   confirmed deletion checkpoints in the auxiliary table.

The command-line script supports these actions directly. For Kubernetes, the
repository supplies a one-shot Job runner that separates collection, review,
and deletion.

## Safety

Deleting an object is irreversible. Always run and review a dry-run before
deletion, and scope the configured bucket and prefix as narrowly as possible.

- Use a unique collection-table prefix for each cleanup.
- For clustered ClickHouse, use the cluster name and expected replica count.
- A failed delete Job does not automatically retry. Successfully deleted
  batches remain checkpointed, so a replacement delete Job can resume safely.
- Never put credentials, customer manifests, or target-cluster details in Git.

## Requirements

- Python 3.11 for local development; the container image also uses Python 3.11.
- Network access to ClickHouse and the target S3-compatible endpoint.
- A ClickHouse user that can read `system.remote_data_paths` and manage the
  auxiliary table.
- S3 permissions appropriate to the action: list for collection, plus delete
  for deletion.

## Quick start

Create a local environment and inspect the available options:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python s3gc.py --help
```

Configuration can be supplied as command-line arguments or `S3GC_*`
environment variables. Set the ClickHouse connection, S3 endpoint/bucket/prefix,
region, disk name, and either static S3 keys or workload identity. Keep secrets
in your approved secret manager or environment, not in command history.

Run a dry-run first:

```bash
.venv/bin/python s3gc.py --verbose --dry-run
```

For a production or customer cleanup, use the Kubernetes procedure below rather
than a one-line delete command.

## Direct script examples

The following is a non-secret target configuration. Replace every
`<placeholder>` value and do not commit this environment to Git:

```bash
export S3GC_CHHOST='<clickhouse-host>'
export S3GC_CHPORT=8123
export S3GC_CHUSER='<clickhouse-user>'
export S3GC_S3IP='s3.eu-central-1.amazonaws.com'
export S3GC_S3PORT=443
export S3GC_S3BUCKET='<bucket>'
export S3GC_S3PATH='<only-the-target-prefix>/'
export S3GC_S3REGION='eu-central-1'
export S3GC_S3SECURE_FLAG=true
export S3GC_S3DISKNAME=s3
export S3GC_CLUSTERNAME='<clickhouse-cluster>'
export S3GC_EXPECTED_REPLICAS=2
export S3GC_COLLECTTABLEPREFIX='s3gc_example_'
export S3GC_AGE=24
export S3GC_USEAGE=24
```

For static S3 credentials, inject the following values from a secret manager
or interactive shell rather than saving them in a file:

```bash
export S3GC_CHPASS='<clickhouse-password>'
export S3GC_S3ACCESSKEY='<s3-access-key>'
export S3GC_S3SECRETKEY='<s3-secret-key>'
export S3GC_S3USEIAM=false
```

Every `S3GC_*` boolean accepts `true/false`, `yes/no`, `on/off`, `1/0`, or an
empty value for false. Unset also means false.

### S3 authentication modes

Select one with `S3GC_S3AUTH` (or `--s3auth`):

| Mode | Credentials | Needs boto3 | Typical use |
|---|---|---|---|
| `static` (default) | `S3GC_S3ACCESSKEY` + `S3GC_S3SECRETKEY`, optionally `S3GC_S3SESSIONTOKEN` | no | long-lived keys, or explicit temporary credentials |
| `aws` | boto3 credential chain, optionally `S3GC_S3PROFILE` | **yes** | AWS SSO / named profiles on a workstation |
| `iam` | MinIO workload identity provider | no | EKS IRSA, EC2 instance profile, ECS task role |

`S3GC_S3PROFILE` implies `aws`. `S3GC_S3USEIAM=true` is a **deprecated alias**
for `S3GC_S3AUTH=iam` — it still works and logs a deprecation warning.
Contradictory combinations are rejected rather than silently resolved.

Prefer `iam` over `aws` inside Kubernetes: it refreshes temporary credentials
through MinIO's provider and keeps `boto3` out of the request path.

#### AWS SSO or a named profile

Authenticate with the AWS CLI first, then let `s3gc` resolve temporary
credentials through the boto3 chain:

```bash
aws sso login --profile my-sso-profile

export S3GC_S3AUTH=aws
export S3GC_S3PROFILE=my-sso-profile
export S3GC_S3IP=s3.amazonaws.com
export S3GC_S3PORT=443
export S3GC_S3REGION=us-east-1
export S3GC_S3SECURE_FLAG=true
.venv/bin/python ./s3gc.py --verbose --dry-run
```

`S3GC_S3ACCESSKEY` and `S3GC_S3SECRETKEY` are unused in `aws` mode, and setting
them is an error rather than a silent override. The resolved credentials must
allow `s3:ListBucket` on the bucket for that prefix **even for `--dry-run`** —
collection lists objects. On failure `s3gc` prints the required permission and
the commands to verify it:

```bash
aws sts get-caller-identity --profile my-sso-profile
aws s3api list-objects-v2 --bucket <bucket> --prefix <prefix> --max-keys 1 --profile my-sso-profile
```

#### Workload identity (EKS/IRSA, EC2, ECS)

```bash
export S3GC_CHPASS='<clickhouse-password>'
export S3GC_S3AUTH=iam        # or the deprecated S3GC_S3USEIAM=true
```

#### GCS and other stores without batch delete

GCS has no batch `DeleteObjects`. `s3gc` detects a `storage.googleapis.com`
endpoint and falls back to per-object deletion automatically, warning that it is
slower; `--use-remove-objects=false` sets it explicitly. Note the disk name is
usually `gcs`, not `s3`, and GCS needs **HMAC/interop** keys:

```bash
export S3GC_S3ACCESSKEY='GOOG1...'
export S3GC_S3SECRETKEY='...'
export S3GC_S3IP=storage.googleapis.com
export S3GC_S3PORT=443
export S3GC_S3SECURE_FLAG=true
export S3GC_S3DISKNAME=gcs
.venv/bin/python ./s3gc.py --verbose --use-remove-objects=false
```

### Collect has no resume — shard large buckets

A crashed or interrupted `--collectonly` restarts its listing from the
beginning; there is no checkpoint. On a multi-million-object bucket that can
cost hours, and long runs are exactly where a rotating password or a dropped
connection tends to strike.

Shard the listing by prefix and re-run only the shards that failed. This is safe
to repeat: the auxiliary table is a `ReplacingMergeTree` keyed on `objpath`, so
re-listing a shard is idempotent.

```bash
# buckets laid out as <prefix>/<3-char hash>/<blob>
for shard in 0 1 2 3 4 5 6 7 8 9 a b c d e f g h i j k l m n o p q r s t u v w x y z; do
  S3GC_S3PATH="<prefix>/${shard}" ./s3gc.py --collectonly --keepdata || \
    echo "shard ${shard} FAILED — re-run just this one"
done
```

### IAM role support

With `S3GC_S3USEIAM=true`, `s3gc` uses MinIO's AWS IAM credential provider.
It obtains and refreshes temporary credentials from one of these environments:

- an EKS Pod using IRSA/workload identity (`AWS_WEB_IDENTITY_TOKEN_FILE` and
  `AWS_ROLE_ARN`);
- an EC2 instance with an attached instance profile; or
- an ECS task with task-role credentials.

Setting `S3GC_S3USEIAM=true` on an ordinary workstation is not enough. The
current provider does **not** read AWS CLI profiles, `aws sso login` state,
`~/.aws/config`, or `AWS_PROFILE`. For a direct local run, use static S3 keys
or run the script from an identity-enabled EC2/EKS/ECS environment.

The static-key path accepts an access key and secret key only; it does not yet
accept an AWS session token. Therefore, do not copy temporary
`aws sts assume-role` credentials into the static-key variables.

Run the safe, split workflow directly. Collection makes an auxiliary table;
the second command reads it and reports candidates without deleting objects:

```bash
.venv/bin/python s3gc.py --collectonly --keepdata
.venv/bin/python s3gc.py --usecollected --dry-run
```

The same variables can be passed as flags (for example,
`--ch-host` or `--s3-bucket`). Run `.venv/bin/python s3gc.py --help` for the
complete flag and environment-variable reference. Avoid direct deletion for
customer or production work; use the reviewed Kubernetes workflow instead.

## Container image

Released images are **public** at `ghcr.io/altinity/s3gc`, so Kubernetes needs no
`imagePullSecret`. Always reference them **by digest**, never by tag — tags get
re-pushed and stop reproducing what you tested:

```bash
docker pull ghcr.io/altinity/s3gc@sha256:<digest>
```

CI prints the exact `IMAGE=` line in its job summary; paste that into your
`.env`. `render.py` refuses anything not digest-pinned.

Build locally for a quick check:

```bash
docker build -f docker/Dockerfile -t s3gc:local .
```

To publish by hand, **both architectures are mandatory** — ClickHouse node pools
are frequently arm64, and an amd64-only image will not schedule there:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile -t ghcr.io/altinity/s3gc:<tag> --push .
docker buildx imagetools inspect ghcr.io/altinity/s3gc:<tag>   # expect amd64 AND arm64
```

> Buildx builder containers cache `/etc/resolv.conf` at creation time. A builder
> left running across a network or VPN change fails with
> `lookup registry-1.docker.io: i/o timeout` while the host resolves fine.
> Recreate the builder, or create one with `--driver-opt network=host`.

The CI workflow runs tests on every pull request and publishes from pushes to
`master` and version tags, authenticating to GHCR with the automatic
`GITHUB_TOKEN`.

## Kubernetes

The Kubernetes runner lives in [`deploy/kubernetes/`](deploy/kubernetes/). It
uses a digest-pinned image and external Kubernetes Secrets; it does not create
or store credentials in the repository.

For customer and production work, follow:

```text
collect → dry-run → approved delete → verify
```

The concise operator procedure, Secret requirements, and renderer configuration
are in [deploy/kubernetes/README.md](deploy/kubernetes/README.md). A guarded
`dev-automation` phase is available only for non-production testing; it runs
collect, dry-run, and delete in one Job and still requires an explicit delete
confirmation.

## Testing

Run all isolated unit tests:

```bash
.venv/bin/python -m pytest -v
```

Run only the development-automation tests:

```bash
.venv/bin/python -m pytest -v -k dev_automation
```

Validate that the example Kubernetes configuration renders without creating a
cluster resource:

```bash
python3 deploy/kubernetes/render.py deploy/kubernetes/example.env > /tmp/s3gc-job.yaml
kubectl apply --dry-run=client -f /tmp/s3gc-job.yaml
```

The unit suite does not contact ClickHouse, S3, or Kubernetes. The reserved
`dev_cluster` pytest marker is excluded from CI; any future tests using it must
be selected explicitly with `.venv/bin/python -m pytest -m dev_cluster` after
reviewing their fixture scope. The collect/dry-run/delete exercise is manual
because it can intentionally delete development objects.

## Repository layout

- `s3gc.py` — collection, anti-join, and deletion logic.
- `docker/` — Python 3.11 container image and Kubernetes entrypoint.
- `deploy/kubernetes/` — plain Job template, renderer, example configuration,
  and operator guide.
- `tests/` — pytest safety, renderer, and entrypoint tests.

## History and roadmap

See [`CHANGELOG.md`](CHANGELOG.md) for the full history, including why each
change was made and the evidence behind defects found in production use.

Planned: concurrency and asynchronous collection/deletion; a `--collectafter`
checkpoint so an interrupted collect can resume instead of re-listing.
