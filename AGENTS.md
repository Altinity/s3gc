# Codex guide — s3gc

Read `CLAUDE.md`, `CHANGELOG.md`, and `TODO.md` before making substantive
changes in this repository. `CLAUDE.md` is the full contributor guide and the
primary source of truth. The changelog explains completed changes, and the TODO
file records deferred work.

This file is intentionally short so agent tooling can find the critical rules
quickly, then defer to `CLAUDE.md` for complete repository guidance.

## Critical rules

1. **Read `CLAUDE.md` first, then `CHANGELOG.md` and `TODO.md`.** Treat them
   as required repository context, not optional background reading.
2. **Protect destructive-operation safeguards.** `s3gc` deletes orphaned S3
   objects only after collection, dry-run review, explicit confirmation, and
   the applicable ClickHouse cluster/replica preflight. Do not weaken these
   controls without explicit approval and matching tests and documentation.
3. **Spec-first TDD and required checks are mandatory.** For every feature,
   bug fix, or material behaviour change, start with a short spec (problem +
   acceptance criteria) in the starting commit. If the request doesn't already
   answer that, run Spec Intake first (see `CLAUDE.md`). Then use test-driven
   development to satisfy each criterion. Run the relevant offline pytest
   suite and Kubernetes renderer/manifest dry-run checks described in
   `CLAUDE.md`. Purely editorial changes are exempt from spec and TDD.
   Default tests must stay offline and must not delete objects. Tests marked
   `dev_cluster` require explicit opt-in and an isolated development environment.
   They never run in CI.
4. **No secrets or customer data in Git.** Never commit credentials, customer
   configuration, target-cluster details, or rendered customer manifests. Use
   Kubernetes Secrets or workload identity for production credentials.
5. **Keep deployments immutable and least-privileged.** Preserve digest-pinned
   images, non-root/read-only container settings, and renderer validation.
6. **Keep dependencies deliberate.** Pin production dependencies in
   `requirements.txt` and testing-only dependencies in `requirements-dev.txt`.
   Add or update one only when the task needs it, and verify the workflow.

## Working rule

When `AGENTS.md` and `CLAUDE.md` differ, update them to match, but follow the
more complete guidance in `CLAUDE.md` for the current task.
