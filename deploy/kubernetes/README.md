# Kubernetes Job runner

This directory runs `s3gc` as a one-shot Kubernetes Job in the same namespace
as ClickHouse. It uses the in-cluster ClickHouse Service directly, so it does
not depend on a laptop or `kubectl port-forward`.

The runner has three separate phases: `collect`, `dry-run`, and `delete`.
Run them in that order. Only the delete phase changes S3.

## Safety rules

- Use a unique auxiliary-table prefix for each bucket/prefix cleanup.
- Never delete by S3 age or prefix heuristic. Delete only after a successful
  cluster-wide dry-run has been reviewed and approved.
- `delete` requires `DELETE_CONFIRMATION=DELETE_ORPHANS`, a target cluster
  name, and the expected replica count.
- The delete Job checks the local cluster macro and reachable replica count
  before deleting. It records confirmed deletion tombstones after every
  `DELETE_BATCH_SIZE` objects.
- Jobs have `backoffLimit: 0`: a failed Job never retries automatically. A
  replacement Job with a new name and the same auxiliary table resumes safely.
- The Job has no Kubernetes API token and no in-pod Kubernetes RBAC.

## 1. Publish and pin the image

Merge the reviewed code through protected `master`. The publishing workflow
creates a private multi-architecture image for `linux/amd64` and `linux/arm64`.
Copy the immutable digest from the workflow summary:

```text
altinity/s3gc@sha256:<published-digest>
```

Use this digest in deployment configuration. Do not use `latest` or a mutable
tag.

## 2. Identify the customer target

Use the explicit customer kubeconfig and namespace; do not rely on your default
Kubernetes context.

```bash
KUBECONFIG=/secure/customer.kubeconfig \
  kubectl -n <namespace> get svc
```

Before rendering a Job, determine and review:

- `CHHOST`: the in-namespace ClickHouse Service name, not `localhost`.
- `CLUSTERNAME`: the exact ClickHouse `cluster` macro.
- `EXPECTED_REPLICAS`: the number of replicas expected to be reachable during
  deletion.
- S3 endpoint, port, bucket, prefix, region, TLS setting, and ClickHouse disk
  name.
- a unique `COLLECTTABLEPREFIX` for this bucket/prefix pair.

The ClickHouse user must be able to read `system.remote_data_paths` across the
target cluster and create, insert into, select from, and truncate the auxiliary
table.

## 3. Create Kubernetes prerequisites

These resources are namespace-local and intentionally not created by this
repository.

Create or reuse a dedicated ServiceAccount. Static S3 credentials require no
Kubernetes RBAC:

```bash
kubectl -n <namespace> create serviceaccount s3gc
```

The published image is private, so create an image-pull Secret:

```bash
kubectl -n <namespace> create secret docker-registry altinity-s3gc-pull \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username='<registry user>' \
  --docker-password='<registry token>'
```

### Static S3 credentials

For customers without workload identity, create the runtime Secret with four
keys. Provide actual values through your approved secret manager or an
interactive terminal; never commit a Secret manifest or `.env` file containing
credentials.

```bash
kubectl -n <namespace> create secret generic s3gc-runtime \
  --from-literal=S3GC_CHUSER='<ClickHouse user>' \
  --from-literal=S3GC_CHPASS='<ClickHouse password>' \
  --from-literal=S3GC_S3ACCESSKEY='<S3 access key>' \
  --from-literal=S3GC_S3SECRETKEY='<S3 secret key>'
```

Set `S3USEIAM=false` in the deployment config. `s3gc` then uses the access and
secret keys directly and does not attempt workload-identity authentication.

### Workload identity

For EKS/IRSA or another workload-identity setup, annotate or reuse the
identity-enabled ServiceAccount, set `S3USEIAM=true`, and provide only
`S3GC_CHUSER` and `S3GC_CHPASS` in the runtime Secret.

## 4. Create a non-secret deployment config

Copy the template to a secure location outside the repository:

```bash
cp deploy/kubernetes/example.env /secure/s3gc-customer.env
```

Set the target values. This static-S3 example shows the important fields:

```dotenv
JOB_NAME=s3gc-customer-collect
NAMESPACE=<namespace>
IMAGE=altinity/s3gc@sha256:<published-digest>
IMAGE_PULL_SECRET=altinity-s3gc-pull
CREDENTIALS_SECRET=s3gc-runtime
SERVICE_ACCOUNT=s3gc

CHHOST=<in-namespace-clickhouse-service>
CHPORT=8123
CLUSTERNAME=<exact-cluster-macro>
EXPECTED_REPLICAS=<actual-replica-count>
COLLECTTABLEPREFIX=s3gc_customer_20260804_

S3IP=<s3-endpoint>
S3PORT=443
S3BUCKET=<bucket>
S3PATH=<only-the-required-prefix>
S3REGION=<region>
S3SECURE_FLAG=true
S3DISKNAME=<disk-name>
S3USEIAM=false

SAMPLES=4
DELETE_BATCH_SIZE=1000
USEAGE_HOURS=24
ORDER_BY_OBJPATH=false
ACTIVE_DEADLINE_SECONDS=43200
TTL_SECONDS_AFTER_FINISHED=604800
MEMORY_REQUEST=1Gi
MEMORY_LIMIT=4Gi
VERBOSE=true
```

For a large production cleanup, begin with `SAMPLES=4`, `USEAGE_HOURS=24`, and
a 12-hour deadline. `ORDER_BY_OBJPATH=false` avoids an unnecessary global sort
that can consume substantial ClickHouse memory and CPU.

## 5. Collect the S3 inventory

Set these values in the secure config:

```dotenv
PHASE=collect
DELETE_CONFIRMATION=
JOB_NAME=s3gc-customer-collect
```

Render, validate, apply, and follow the Job log:

```bash
python3 deploy/kubernetes/render.py /secure/s3gc-customer.env \
  > /secure/s3gc-customer-collect.yaml
kubectl apply --dry-run=server -f /secure/s3gc-customer-collect.yaml
kubectl apply -f /secure/s3gc-customer-collect.yaml
kubectl -n <namespace> logs -f job/s3gc-customer-collect
```

The collect phase creates and populates the auxiliary ClickHouse table. It does
not delete S3 objects.

## 6. Dry-run the cluster-wide anti-join

Keep every target field and `COLLECTTABLEPREFIX` identical. Change only:

```dotenv
PHASE=dry-run
DELETE_CONFIRMATION=
JOB_NAME=s3gc-customer-dry-run
```

Render and apply a new manifest using the commands above. The final Job log
reports a line such as:

```text
<N> objects of total size <bytes> would be removed but for dryrun flag
```

Review the count and size with the customer. Do not continue based only on a
successful Job exit status.

## 7. Delete only after explicit approval

After the dry-run result has been approved, keep all target fields identical
and change only:

```dotenv
PHASE=delete
DELETE_CONFIRMATION=DELETE_ORPHANS
JOB_NAME=s3gc-customer-delete
```

Render, server-dry-run, and apply a new manifest. Follow the logs:

```bash
kubectl -n <namespace> logs -f job/s3gc-customer-delete
```

Before deleting, the Job verifies `CLUSTERNAME` and `EXPECTED_REPLICAS`. During
deletion, logs show durable checkpoints such as:

```text
delete checkpoint: 1000 objects / <bytes> removed so far
```

If the Job fails, do not re-run collect. Diagnose the failure, then render a
new delete Job name using the same auxiliary-table prefix. Objects already
deleted successfully are tombstoned and are not selected again.

## 8. Verify and retain evidence

Run one final dry-run with:

```dotenv
PHASE=dry-run
DELETE_CONFIRMATION=
JOB_NAME=s3gc-customer-verify
```

It must report zero candidates. Retain the auxiliary table and completed Job
logs for audit. Do not reuse that table for another bucket or prefix.
