# Kubernetes Job runner

The runner executes `s3gc` in the same namespace as ClickHouse so it reaches the
cluster service directly and does not depend on a `kubectl port-forward`.

## Safety model

- Render and apply exactly one phase at a time: `collect`, `dry-run`, or `delete`.
- `delete` requires `DELETE_CONFIRMATION=DELETE_ORPHANS`, a cluster name, and an
  expected replica count. The program validates the local macro and the number of
  reachable replicas before it deletes anything.
- Jobs never retry automatically. Successful S3 deletions are tombstoned in the
  auxiliary table after every `DELETE_BATCH_SIZE` objects, with an accumulated
  progress line in the Job log, so a manually rerun Job resumes safely.
- The Job does not receive a Kubernetes API token and receives no in-pod RBAC.

## Credentials

Create or reference a namespaced Secret outside this repository. For static S3
credentials it must contain these four keys, which are passed unchanged to
`s3gc`:

```
S3GC_CHUSER
S3GC_CHPASS
S3GC_S3ACCESSKEY
S3GC_S3SECRETKEY
```

Do not put secret values in the config file or rendered manifest. The secret
provisioning mechanism is intentionally environment-owned.

For EKS/IRSA or another AWS workload-identity setup, set `S3USEIAM=true` and
set `SERVICE_ACCOUNT` to the identity-enabled ServiceAccount. In that mode the
Secret only needs `S3GC_CHUSER` and `S3GC_CHPASS`; static S3 keys are not used.

## Render and validate

Copy `example.env` outside the repository and fill the target values. Use an
immutable image digest after publishing the image:

```bash
python3 deploy/kubernetes/render.py /secure/path/s3gc.env > /tmp/s3gc-job.yaml
kubectl apply --dry-run=client -f /tmp/s3gc-job.yaml
```

`IMAGE_PULL_SECRET` is a namespaced `kubernetes.io/dockerconfigjson` Secret for
the registry containing the pinned image. It is separate from the runtime
credentials Secret and must be created by the environment owner.

Apply only after a separate explicit approval for the target environment:

```bash
kubectl apply -f /tmp/s3gc-job.yaml
kubectl logs -f job/<job-name>
```

For a fresh run, use `PHASE=collect` and a unique `COLLECTTABLEPREFIX`. The
collect Job retains the table. Render `PHASE=dry-run` next, inspect its result,
then render `PHASE=delete` plus the required delete confirmation only when
approved. Never reuse an auxiliary table for a different bucket/prefix.

Set `VERBOSE=true` for the Job logs to include its connection, collection, and
anti-join totals.

`DELETE_BATCH_SIZE` controls delete progress granularity (use `1000` normally;
use a smaller value only when more frequent progress checkpoints are useful).
