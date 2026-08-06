# Codex guide — s3gc

Read `CLAUDE.md`, `CHANGELOG.md`, and `TODO.md` before making substantive
changes in this repository. `CLAUDE.md` is the full contributor guide and the
primary source of truth; the changelog provides recent historical context and
the TODO file records unshipped follow-ups.

This file is intentionally short so agent tooling can find the critical rules
quickly, then defer to `CLAUDE.md` for complete repository guidance.

## Critical rules

1. **Read `CLAUDE.md` first, then `CHANGELOG.md` and `TODO.md`.** Treat them
   as required repository context, not optional background reading.
2. **Protect destructive-operation safeguards.** `s3gc` deletes orphaned S3
   objects only after collection, dry-run review, explicit confirmation, and
   the applicable ClickHouse cluster/replica preflight. Do not weaken these
   controls without explicit approval and matching tests and documentation.
3. **Required checks are offline.** Run the relevant pytest suite and
   Kubernetes renderer/manifest dry-run checks described in `CLAUDE.md`.
   Automated tests must not contact live ClickHouse or object storage, or
   delete objects.
4. **No secrets or customer data in Git.** Never commit credentials, customer
   configuration, target-cluster details, or rendered customer manifests. Use
   Kubernetes Secrets or workload identity for production credentials.
5. **Keep deployments immutable and least-privileged.** Preserve digest-pinned
   images, non-root/read-only container settings, and renderer validation.
6. **Keep dependencies deliberate.** Production dependencies belong in
   `requirements.txt`; testing-only dependencies belong in
   `requirements-dev.txt`. Do not add either casually.

## Working rule

When `AGENTS.md` and `CLAUDE.md` differ, update them to match, but follow the
more complete guidance in `CLAUDE.md` for the current task.
