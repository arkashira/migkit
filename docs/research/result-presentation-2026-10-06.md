# Result and report presentation plan

Status: proposed, not implemented. Owner clarification, 2026-10-06. Extends P0-RESULT-SCOPE and P0-VALIDATE.results; do not replace their research requirements.

## Required reading order

1. Overall outcome and completeness, with actual scope and observation window.
2. One row per database: database, checks completed/planned, outcome, concrete target-use problems, remaining work.
3. Selected database: grouped actionable issues with affected objects and practical consequences.
4. Expandable evidence and technical diagnostics, linked to the exact issue or check.

Do not mix operational progress, issue severity and verification outcome into one status. Avoid long prose, unexplained counters, raw logs or generic recommendations in the initial view. Healthy databases remain visible in the summary without repeated success paragraphs.

## Result contract

Store outcome (pass, issue, incomplete, unable to check), execution state (queued, running, completed, interrupted), coverage (planned, attempted, verified, skipped, unsupported, failed) and issue severity separately. Scope includes actual objects, filters, exclusions, mappings, method, guarantee and observation window. Define not-applicable explicitly; exclude it from required coverage only with a recorded reason.

A successful process or partial check cannot yield a complete pass. Unsupported/skipped/failed checks remain visible. A detected issue and unfinished work may coexist. Aggregate structured fields, never parse human finding text. Stable database/object/check identifiers connect summary, details and evidence without exposing customer identifiers to product telemetry.

## Shared presentation

CLI, web, structured exports and document reports consume the same versioned result envelope and aggregation rules. Preserve current export compatibility during transition. Use short plain labels and consistent grouping; color is supplemental, never the only signal. Users can filter problems or unfinished work while retaining a clear indication of hidden databases and total scope.

Each issue explains what differs, where, its practical effect and the confidence of that conclusion. Recommendations are optional and separate from verified facts. The default report does not advertise wrapped tools or assign vendor blame. Sensitive evidence stays local or in explicitly authorized destinations.

## Delivery and acceptance

- Inventory current Result, verdict, evidence, report, render, CLI and web paths and their consumers before changing contracts.
- Define examples for fully healthy, confirmed differences, mixed incomplete results, unsupported engine checks, timeout/retry, changing source data and interrupted/resumed runs.
- Prototype a compact database summary and one issue detail using existing validation findings with confidential values kept private.
- Implement the shared result envelope and aggregation, then connect all presentations. Preserve resumable evidence and existing output consumers.
- Test meaningful outcome/coverage invariants and representative end-to-end report scenarios under repository test cadence.
- Acceptance: a reader identifies affected databases, practical problems and unfinished checks within 30 seconds without reading logs; all tested incomplete/unsupported/failure cases remain distinguishable from complete pass; the same facts and scope agree across all output surfaces; details are available without overwhelming the first view.

Open-source and commercial scope/result presentation research remains tracked in P0-RESULT-SCOPE.commercial and P0-RESULT-SCOPE.unmatched. This clarification is not a claim that all comparisons are complete.

## Run artifact hierarchy and entry point

Observed private validation run: 119 top-level entries, mixing 63 JSON files, 23 logs, 20 helper scripts, 3 locks, 4 Markdown files and 6 directories. This includes operational workarounds and document tooling, not solely native product output. The current folder is difficult to navigate; do not use its layout as the product contract.

Proposed run layout:

```text
runs/<run>/
  index.html             readable entry point: scope, outcome, progress, database links
  summary.json           versioned machine-readable result envelope
  databases/<safe-id>/    database summary and links to applicable issues/evidence
  evidence/              immutable check evidence indexed by manifest
  diagnostics/           local logs, attempts, errors and performance details
  internal/              checkpoints, locks, query caches and recovery records
  exports/               requested reports and documents
```

A run manifest records artifact type, database/check association, attempt, authoritative/superseded status, sensitivity and schema version. Display human-readable database names in the index; use safe opaque paths to avoid collisions, unsafe names or accidental sensitive-name export. Never infer the latest valid result by filename sorting. Aggregate valid retry evidence through an explicit manifest, keeping failed attempts available in diagnostics. Normal users start at the index, not JSON or logs. State and credentials must not be mixed into shareable exports. Do not rename, move or remove live checkpoints/locks while a run is active. Provide a compatibility loader and a non-destructive indexed view for legacy runs before any cleanup.

Acceptance: one obvious entry point; every database discoverable from it; no need to browse internal files to understand outcome; all evidence links resolve; current and superseded attempts are distinguishable; sharing an export excludes internal state and secrets; resume remains correct after layout migration.

Platform-wide rule: readability applies across configuration, CLI/help, workflow progress, ETA, errors/recovery, reports, artifacts, exports, docs and source navigation. P0-READABILITY-ALL coordinates existing owners. Preserve deep technical evidence on demand; do not simplify by hiding incomplete work or guarantees.

## First-class artifacts

Owner clarification, 2026-10-06: artifacts are typed products of every migration workflow, not arbitrary files. An artifact record contains a stable identity, kind/schema version, producer run/task/attempt, scope association, creation/observation window, completeness/validity, provenance and dependency links, local location, integrity metadata, sensitivity/access class and retention policy. Kinds include plan, assessment, conversion output, schema proposal, move/check evidence, checkpoint, issue, report and diagnostic bundle. Do not include customer-value fingerprints in outbound product telemetry.

Reuse existing evidence/state/audit owners through one manifest contract rather than a second storage system. Publish a completed artifact reference atomically after durable content; distinguish partial, valid, superseded, invalid and unavailable artifacts. A latest pointer is not a completeness verdict. Relate derived artifacts to their inputs and invalidate reuse when scope, schema, source window or method guarantees change. Reuse evidence only when validity conditions are satisfied.

Artifacts are discoverable by run, database, task and kind through the readable index. Required state/evidence cannot be garbage-collected while active, pinned or needed for authorized resume/rollback/audit. Exports use an explicit sensitivity allowlist and strip internal state/credentials. Storage is local by default; cloud storage/sharing is separate scope. User-facing artifacts never authorize applying SQL or making database changes.

Acceptance includes crash during publication, stale references, reruns, superseded attempts, scope changes, retention, restore, missing content and safe exports. Provide a non-destructive legacy importer. Integrity metadata establishes content identity, not independent proof that the database is correct.

Required incident acceptance: per-column timeout followed by a successful partial deep result must display incomplete coverage, failed units and remaining scope. No pass/readiness claim without every required unit or an explicit unsupported guarantee. Track implementation and sandbox regression in P0-VALIDATE.no-silent-skip.
