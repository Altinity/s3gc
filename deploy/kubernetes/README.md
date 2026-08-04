# Kubernetes Job runner

Run `s3gc` as a one-shot Job in the ClickHouse namespace. For customer and
production work, always run:

```text
collect → dry-run → approved delete → verify
```

The Job reaches ClickHouse through its in-cluster Service; no laptop tunnel is
needed.

## Before the first Job

- Use an immutable, multi-architecture image digest:
  `altinity/s3gc@sha256:<digest>`.
- Create or reuse a namespace-local ServiceAccount and registry pull Secret.
- Create a runtime Secret named by `CREDENTIALS_SECRET`:
  - static S3: `S3GC_CHUSER`, `S3GC_CHPASS`, `S3GC_S3ACCESSKEY`, and
    `S3GC_S3SECRETKEY`; set `S3USEIAM=false`.
  - workload identity: only `S3GC_CHUSER` and `S3GC_CHPASS`; set
    `S3USEIAM=true` and use an identity-enabled ServiceAccount.
- Confirm the ClickHouse Service name, cluster macro, expected replica count,
  S3 bucket/prefix, and disk name. Use a unique `COLLECTTABLEPREFIX` per
  bucket/prefix cleanup.

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

## Safety

- Delete checks the local cluster macro and expected replica count before S3
  calls.
- Confirmed deletions are tombstoned in the auxiliary table. If a delete Job
  fails, create a new delete Job name with the same table prefix; do **not**
  re-collect.
- No Job retries automatically (`backoffLimit: 0`).
- Do not run delete until the customer explicitly approves the dry-run result.
