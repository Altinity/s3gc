# Contributor guide — s3gc

`s3gc` is a Python command-line tool and Kubernetes Job workflow for finding
and deleting orphaned objects from ClickHouse S3 disks and compatible object
storage. It is an operationally destructive tool: quality is held by offline
tests, explicit delete controls, and conservative deployment defaults.

## Hard rules

1. **The deletion lifecycle is non-negotiable.** The normal production flow is
   `collect → dry-run → explicit customer approval → delete → verify`. Preserve
   the delete confirmation token, ClickHouse cluster and expected-replica
   preflight, deletion checkpoints, and the rule that a failed delete job does
   not retry automatically. A behavior change in this path needs regression
   tests and matching README/deployment documentation.

2. **Tests stay offline by default.** Install both requirements files, then
   run `pytest -v` for relevant changes. Tests must use fakes, local
   subprocesses, and manifest dry-runs; they must not contact live ClickHouse,
   S3-compatible storage, or delete objects. The `dev_cluster` marker is
   explicitly environment-dependent and never runs in CI. Do not add live
   credentials or a live-delete test path to the default suite.

3. **No secrets or customer artifacts in Git.** Do not commit S3 keys,
   ClickHouse passwords, customer `.env` files, rendered customer manifests,
   target-cluster details, or command output containing them. Keep credentials
   in an approved secret manager, Kubernetes Secret, or workload identity.
   `deploy/kubernetes/example.env` is a non-secret template only.

4. **Kubernetes deployment stays immutable and least-privileged.**
   `deploy/kubernetes/render.py` must continue to reject unpinned images and
   invalid phase/confirmation input. Use image digests, never mutable tags.
   Preserve the Job template's non-root user and read-only root filesystem;
   do not embed credentials in the image or manifest.

5. **Dependencies are deliberate and reproducible.** Runtime dependencies are
   pinned in `requirements.txt`; test-only dependencies are pinned in
   `requirements-dev.txt`. Use both files when preparing a development or CI
   environment. Add or update a dependency only when it is necessary for the
   requested capability, and test the resulting workflow.

## Repository map

| Path | Purpose |
| --- | --- |
| `s3gc.py` | CLI arguments, ClickHouse inventory/anti-join, S3 collection and deletion, safety preflight, and checkpoints. |
| `tests/` | Pytest regression tests using fakes and local subprocesses. |
| `docker/Dockerfile` | Minimal Python 3.11 production image. |
| `docker/kubernetes-entrypoint.sh` | Phase dispatcher and delete/dev-automation confirmation gate. |
| `deploy/kubernetes/render.py` | Validates a non-secret environment file and renders the Job manifest. |
| `deploy/kubernetes/job.yaml.tmpl` | Kubernetes Job template with security context and environment wiring. |
| `deploy/kubernetes/example.env` | Non-secret rendering example; copy it outside the repository for a real run. |
| `.github/workflows/container.yml` | CI test, render, manifest validation, and container publication workflow. |

## Required checks

For a change to Python, shell, renderer, manifest, dependencies, or deployment
workflow, run the relevant checks after installing the pinned development
requirements:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest -v
.venv/bin/python deploy/kubernetes/render.py deploy/kubernetes/example.env > /tmp/s3gc-job.yaml
kubectl apply --dry-run=client -f /tmp/s3gc-job.yaml
```

Add a regression test in the same change as each behavior or safety fix. Cover
failure paths as well as the intended path, especially delete confirmation,
cluster/replica preflight, S3 delete errors, checkpointing, boolean environment
parsing, and renderer input validation. Do not claim or impose a coverage
percentage until coverage tooling and an enforceable threshold are introduced.

## Working discipline

- Keep the command-line and `S3GC_*` environment interfaces compatible unless
  the task explicitly authorizes a breaking operational change.
- Treat the renderer, entrypoint, README, and Kubernetes guide as part of the
  same operator-facing contract. Update the affected documentation in the same
  change as an operational behavior change.
- Surface out-of-scope safety defects rather than silently changing them. State
  the file and risk, and defer the fix unless it is necessary to keep the
  current task safe.
- When using subagents for discovery or review, make them read-only unless the
  task explicitly authorizes writes. Inspect the working tree after any agent
  batch before continuing.
