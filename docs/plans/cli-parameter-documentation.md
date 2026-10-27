# Plan: complete CLI parameter documentation

Status: implemented on `docs/cli-parameter-reference`.
Planning branch: `docs/cli-reference-plan`.
Proposed implementation branch: `docs/cli-parameter-reference`.

## Problem and intent

The README introduces common settings, but operators need a complete reference
that explains each parameter and how parameters work together across a cleanup.
Complete the first documentation item in `TODO.md`, with particular attention to
`--usecollected` and the separation of collection, review, deletion, and verification.

This is an editorial change. Preserve CLI names, aliases, defaults, environment
variables, runtime behavior, deployment settings, and deletion safeguards. Exclude
the deleted manifest feature. Do not fix runtime defects as part of this work.

## Reference style

Use Altinity's [clickhouse-backup configurable parameters](https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md#configurable-parameters)
as a presentation reference: group related settings and put environment names,
value meanings, defaults, and constraints near their explanations. Its
[CLI manual](https://github.com/Altinity/clickhouse-backup/blob/master/Manual.md)
also puts usage, aliases, and option descriptions together. References inspected
on 2026-10-27.

Adapt that structure to s3gc's existing interfaces. Distinguish illustrative
values from actual defaults and provide operational explanations beyond help text.

## Deliverables

| File | Planned change |
| --- | --- |
| `docs/cli-reference.md` | Complete parameter reference, configuration rules, phase walkthrough, and examples. |
| `README.md` | Link the reference beside local setup and introduce the collect/use distinction briefly. |
| `deploy/kubernetes/README.md` | Cross-link the reference and explain how entrypoint phases select CLI flags; retain the production runbook. |
| `deploy/kubernetes/example.env` | Clarify that the documented use-phase limit applies to each sample. |
| `CHANGELOG.md` | Record the completed documentation under Unreleased. |
| `TODO.md` | Remove the completed documentation item only after its acceptance criteria are met. |

## Implementation sequence

1. Read `CLAUDE.md`, `CHANGELOG.md`, `TODO.md`, and the existing operator guides.
   Inventory every argument declaration in `s3gc.py`, including aliases, shared
   destinations, explicit-value boolean forms, and parser-provided help/config
   options. Consult runtime consumers, the entrypoint, renderer, and template to
   distinguish declared defaults from effective behavior. Use local sources;
   consult the clickhouse-reference skill if a ClickHouse claim needs verification.
2. Build the complete reference, grouped into ClickHouse connection/topology,
   object storage/authentication, collection/inventory, candidate processing and
   deletion, timeouts/retries, and logging/configuration/development controls.
   Give each setting a stable anchor. For each setting document purpose, all
   aliases, value type and accepted values, units, actual default, exact
   `S3GC_*` variable, affected phases, and interactions. State explicitly when a
   value has no default or a parser utility has no environment equivalent.
   Document enforced restrictions separately from operational recommendations.
3. Explain configuration files through `--cfg`, configuration destination names,
   booleans, and precedence among defaults, environment, files, and CLI arguments.
   Verify precedence and shared-destination alias behavior with the pinned
   jsonargparse version using offline parser probes; do not assume precedence
   matches clickhouse-backup. Include a non-secret configuration example and
   explain `--help`, `--print_config`, and the redacted `--list-options` output.
4. Write the collect/use explanation and examples described below. Explain
   defaults that can start collection and deletion in one invocation; distinguish
   auxiliary-table writes during preview from S3 object deletion. Check CLI
   confirmation behavior separately from the Kubernetes confirmation token.
5. Add README/runbook links, remove unnecessary duplicate explanations, and
   reconcile any conflicting guidance against the implementation. Preserve the
   production approval sequence and existing runbook requirements. Finish the
   changelog and TODO edits after validation.

## Required explanation of `--usecollected`

- Document `--usecollected`, its `--use-collected` alias, the explicit-value forms
  `--usecollectedflag` and `--use-collected-flag`, default `false`, and the exact
  variable `S3GC_USECOLLECTED_FLAG`.
- Explain that it skips collection and reads the existing auxiliary inventory.
  It re-evaluates references and the age filter during the use phase; the
  inventory is not a saved final deletion decision and is not refreshed by an
  S3 listing. New keys are absent until another collect, and overwritten keys
  can retain stale metadata.
- Show the distinction between `--collect-only`, `--use-collected --dry-run`,
  and a delete using the collected inventory. Explain the effect of omitting
  `--use-collected`, and the interaction with `--keep-data`, tombstones, table
  truncation, `--drop-collecttable`, and retrying an interrupted delete.
- Explain how the selected database, table prefix, disk, and pinned ClickHouse
  host identify the inventory. Cover missing/empty inventory errors and why
  every phase must use the same replica and cleanup scope.
- Keep collection age (`--age`) separate from use age (`--useage`), collection
  limits/markers separate from use limits/markers, and document sampling and
  ordering constraints from the code. Describe actual limit behavior rather
  than promising a global cap without checking it.
- Map Kubernetes collect, dry-run, delete, and dev-automation phases to their
  actual entrypoint arguments. Keep renderer keys distinct from `S3GC_*`
  variables; do not imply all CLI options are exposed by the renderer.

## Manual inventory for a shared bucket

Document the preferred manual workflow when independent clusters share an
overlapping bucket/prefix: export the full scoped listing to CSV, gather live
references from every participating cluster and replica, and exclude those
references before approving candidates. Include example listing, CSV import,
reference-export, and comparison commands. A cluster's own replicas can normally
be covered with `--cluster-name`; independent clusters require this additional
cross-cluster review because s3gc checks only its configured reference scope.

Import the reviewed candidates into a dedicated replicated auxiliary table with
the schema, naming, replacement semantics, and partitioning expected by s3gc.
Distinguish table data replication from a Replicated database's DDL replication,
and keep the execution host pinned. The full raw listing belongs in a separate
staging table so unrelated objects cannot become deletion candidates. Explain
metadata provenance, complete reference exports, replication readiness, and
freshness checks before delete. Use `--use-collected` for preview, approved
delete, recovery, and verification; do not re-collect into the reviewed table.

## Examples and safety requirements

Provide one consistent placeholder configuration and separate examples for
collect, dry-run, approved delete, and verification. Prefer existing kebab-case
aliases. Preserve inventory through the delete/verify sequence as required by
the implementation. State prerequisites next to the commands, including the
same host and scope, cluster and expected replicas, and use age of at least
24 hours. Keep human review and explicit approval before deletion; explain the
actual confirmation mechanism for each execution path.

Include an interrupted-delete recovery example using the same inventory and a
brief contrast with restarting collection. Verification must explain what a
zero-candidate result establishes for that inventory and time window. Keep
production commands separate from the development-only age override. Use
placeholders and externally injected credentials throughout. Do not execute
any live example while writing or validating the documentation.

## Acceptance criteria and validation

- Every current CLI argument and alias appears in the reference, with the fields
  above checked against parser declarations and runtime consumers. Review an
  inventory-to-documentation checklist to catch omissions, including boolean
  destinations ending in `_FLAG` and parser utility options.
- Configuration precedence and representative boolean/alias combinations are
  demonstrated with isolated offline probes of the installed pinned parser.
  Clear inherited `S3GC_*` values for probes and use dummy configuration only;
  do not print the real environment or call `main()`.
- A reader can explain when `--use-collected` is needed, what it reuses, what
  gets checked again, and how to preserve the inventory for recovery and verify.
- The shared-bucket example covers full CSV listing/import, all-cluster reference
  review, and a replicated candidate table. It states that `--use-collected`
  does not discover other clusters or make an unfiltered shared listing safe.
- All four workflow examples use existing options, consistent scope, and the
  existing approval, age, topology, and checkpoint safeguards. Kubernetes
  explanations agree with the renderer, template, and entrypoint.
- Relative links and anchors resolve, Markdown tables/code blocks render, and
  `git diff --check` passes. Run help/config inspection offline only. This
  editorial change needs no new runtime tests or live-system validation.
  If executable files enter scope, split that work out and apply the repository's
  spec-first TDD and required offline pytest/render/Kubeconform checks.
- Only the planned documentation files change; no secrets, customer artifacts,
  dependencies, generated configuration dumps, or rendered manifests enter Git.

## Handoff

The planning stage ended when implementation was requested. The implementation
branch is `docs/cli-parameter-reference`; carry this plan with the documentation.
Keep the TODO item open until implementation is done. Publishing a PR or merging
is outside this documentation implementation request.
