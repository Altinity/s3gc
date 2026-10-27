# Contributor guide — s3gc

`s3gc` finds and deletes orphaned objects from ClickHouse S3 disks and compatible
object storage. It provides a Python CLI and a Kubernetes Job workflow. Deletion
is irreversible, so preserve the offline tests and operational safeguards.

## Hard rules

1. **Preserve the deletion lifecycle.** The normal production flow is
   `collect → dry-run → explicit customer approval → delete → verify`. Preserve
   the delete confirmation token, ClickHouse cluster and expected-replica
   preflight, deletion checkpoints, and the rule that a failed delete Job
   is not rerun automatically. A behavior change in this path needs regression
   tests and matching README and deployment documentation. Job reruns are
   separate from HTTP retries within a running process.

2. **Spec-first TDD is mandatory.** For every new feature, bug fix, or
   material operational behaviour change, start with a short spec in the
   commit that begins the change. State the problem in one or two sentences
   and list the acceptance criteria. Run Spec Intake below if details are
   missing. For each criterion, write a failing test before implementing the
   change. Add a regression test for every defect. Purely editorial changes
   are exempt from both the spec and TDD.

3. **Tests stay offline by default.** Install both requirements files, then
   run the relevant offline suite with `-m "not dev_cluster"`. Default tests
   use fakes, local subprocesses, and manifest dry-runs. They must not contact
   live ClickHouse or object storage, or delete objects. Tests marked
   `dev_cluster` require explicit opt-in and an isolated development environment.
   They never run in CI. Do not add live credentials or live-delete paths to
   the default suite.

4. **No secrets or customer artifacts in Git.** Do not commit S3 keys,
   ClickHouse passwords, customer `.env` files, rendered customer manifests,
   target-cluster details, or command output containing them. Keep credentials
   in an approved secret manager, Kubernetes Secret, or workload identity.
   `deploy/kubernetes/example.env` is a non-secret template only.

5. **Kubernetes deployment stays immutable and least-privileged.**
   `deploy/kubernetes/render.py` must continue to reject unpinned images and
   invalid phase/confirmation input. Use image digests, never mutable tags.
   Preserve the Job template's non-root user and read-only root filesystem.
   Do not embed credentials in the image or manifest.

6. **Dependencies are deliberate and reproducible.** Runtime dependencies are
   pinned in `requirements.txt`. Test-only dependencies are pinned in
   `requirements-dev.txt`. Use both files when preparing a development or CI
   environment. Add or update a dependency only when it is necessary for the
   requested capability, and test the resulting workflow.

7. **Read the history and backlog before substantive changes.** Read
   `CHANGELOG.md` for behavior changes and operational evidence, then `TODO.md`
   for deferred work. A TODO item is not implemented. The changelog records
   completed work, not the backlog.

## Spec Intake

When someone reports a bug or asks for a feature without enough detail to
write a spec, ask:

1. What's happening, and what should happen instead?
2. How do you trigger it (steps or command)?
3. Does this touch the delete lifecycle (collect → dry-run → delete → verify)
   or a safety check?
4. Any known edge cases, or things this must not break?

Use the answers to write the spec's acceptance criteria. If the report
already answers these, skip straight to the spec.

## Repository map

| Path | Purpose |
| --- | --- |
| `s3gc.py` | CLI arguments, ClickHouse inventory/anti-join, S3 collection and deletion, safety preflight, and checkpoints. |
| `docs/cli-reference.md` | Parameter details, configuration, phase examples, and manual shared-bucket inventory. |
| `tests/` | Pytest regression tests using fakes and local subprocesses. |
| `docker/Dockerfile` | Minimal Python 3.11 production image. |
| `docker/kubernetes-entrypoint.sh` | Phase dispatcher and delete/dev-automation confirmation gate. |
| `deploy/kubernetes/render.py` | Validates a non-secret environment file and renders the Job manifest. |
| `deploy/kubernetes/job.yaml.tmpl` | Kubernetes Job template with security context and environment wiring. |
| `deploy/kubernetes/example.env` | Non-secret rendering example. Copy it outside the repository for a real run. |
| `.github/workflows/container.yml` | CI test, render, manifest validation, and container publication workflow. |
| `CHANGELOG.md` | Completed changes and their reasons, including production evidence. Update it with behavior, safety, and deployment changes. |
| `TODO.md` | Deferred engineering and operational work. Move completed items to the changelog. |

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
- Record behavior, safety, and deployment changes in `CHANGELOG.md` with the
  change itself. Explain why and preserve production evidence without customer
  names, cluster identifiers, or credentials.
- Put deferred work in `TODO.md`. Remove or update the item when it lands.
- Keep the renderer, entrypoint, README, CLI reference, and Kubernetes guide in
  agreement. Update the affected docs with each operational behavior change.
- Surface out-of-scope safety defects rather than silently changing them. State
  the file and risk, and defer the fix unless it is necessary to keep the
  current task safe.
- When using subagents for discovery or review, make them read-only unless the
  task explicitly authorizes writes. Inspect the working tree after any agent
  batch before continuing.
