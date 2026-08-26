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

2. **Spec-first TDD is mandatory.** For every new feature, bug fix, or
   material operational behaviour change, start with a short spec in the
   commit that begins the change: one or two sentences on the problem or
   intent, plus a short bullet list of acceptance criteria — what must be true
   when it's done. Then use TDD: for each acceptance criterion, write the
   failing test that covers it before making it pass. Add a regression test
   for every defect. Purely editorial changes are exempt from both the spec
   and TDD.

3. **Tests stay offline by default.** Install both requirements files, then
   run `pytest -v` for relevant changes. Tests must use fakes, local
   subprocesses, and manifest dry-runs; they must not contact live ClickHouse,
   S3-compatible storage, or delete objects. The `dev_cluster` marker is
   explicitly environment-dependent and never runs in CI. Do not add live
   credentials or a live-delete test path to the default suite.

4. **No secrets or customer artifacts in Git.** Do not commit S3 keys,
   ClickHouse passwords, customer `.env` files, rendered customer manifests,
   target-cluster details, or command output containing them. Keep credentials
   in an approved secret manager, Kubernetes Secret, or workload identity.
   `deploy/kubernetes/example.env` is a non-secret template only.

5. **Kubernetes deployment stays immutable and least-privileged.**
   `deploy/kubernetes/render.py` must continue to reject unpinned images and
   invalid phase/confirmation input. Use image digests, never mutable tags.
   Preserve the Job template's non-root user and read-only root filesystem;
   do not embed credentials in the image or manifest.

6. **Dependencies are deliberate and reproducible.** Runtime dependencies are
   pinned in `requirements.txt`; test-only dependencies are pinned in
   `requirements-dev.txt`. Use both files when preparing a development or CI
   environment. Add or update a dependency only when it is necessary for the
   requested capability, and test the resulting workflow.

7. **Read project history and backlog before substantive changes.** Review
   `CHANGELOG.md` for recent behaviour and operational evidence, then `TODO.md`
   for possible, interesting, and deliberately deferred improvements. Do not
   treat a TODO item as already implemented or use the changelog as a backlog.

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
| `CHANGELOG.md` | Shipped behaviour and **why**, including evidence for defects found in production use. Update it in the same change as any behaviour, safety, or deployment change. |
| `TODO.md` | Possible, interesting, and deliberately deferred engineering or operational improvements. Move completed work to the changelog when it lands. |

## Required checks

For a change to Python, shell, renderer, manifest, dependencies, or deployment
workflow, run the relevant checks after installing the pinned development
requirements:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest -v -m "not dev_cluster"
.venv/bin/python deploy/kubernetes/render.py deploy/kubernetes/example.env > /tmp/s3gc-job.yaml
docker run --rm --entrypoint /kubeconform -v /tmp:/tmp:ro \
  ghcr.io/yannh/kubeconform@sha256:85dbef6b4b312b99133decc9c6fc9495e9fc5f92293d4ff3b7e1b30f5611823c \
  -strict -summary /tmp/s3gc-job.yaml
```

Add a regression test in the same change as each behavior or safety fix. Cover
failure paths as well as the intended path, especially delete confirmation,
cluster/replica preflight, S3 delete errors, checkpointing, boolean environment
parsing, and renderer input validation. Do not claim or impose a coverage
percentage until coverage tooling and an enforceable threshold are introduced.

## Working discipline

- Keep the command-line and `S3GC_*` environment interfaces compatible unless
  the task explicitly authorizes a breaking operational change.
- Record behaviour, safety and deployment changes in `CHANGELOG.md` as part of
  the same change. Write down *why*, and keep the evidence for defects found in
  production — the reasoning is the expensive part to reconstruct later. Never
  put customer names, cluster identifiers or credentials there.
- Record possible, interesting, or pending engineering and operational
  improvements in `TODO.md`, not in the changelog. Remove or update the TODO
  item when the work lands.
- Treat the renderer, entrypoint, README, and Kubernetes guide as part of the
  same operator-facing contract. Update the affected documentation in the same
  change as an operational behavior change.
- Surface out-of-scope safety defects rather than silently changing them. State
  the file and risk, and defer the fix unless it is necessary to keep the
  current task safe.
- When using subagents for discovery or review, make them read-only unless the
  task explicitly authorizes writes. Inspect the working tree after any agent
  batch before continuing.
