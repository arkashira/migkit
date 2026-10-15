# The autonomous workflow, mechanism by mechanism (P0-AUTONOMOUS-MIGRATION)

Grounded in the existing research; nothing here is new reading:
[AWS] `mechanisms-aws-azure-google-snowflake-2026-09-28.md`,
[DTS] `mechanisms-dts-tidb-vitess-molt-voyager-2026-09-28.md`,
[GG] `mechanisms-goldengate-qlik-hvr-ibm-shareplex-2026-09-28.md`.
Each row says the established mechanism, migkit's equivalent as
`module.function`, or the gap. No row claims migkit is better: that needs
the measured scenarios at the end, which are written and not yet run.

| Mechanism | Established products | migkit | Gap |
|---|---|---|---|
| Preflight | AWS DMS premigration assessment runs, individual assessments to S3 [AWS 1.8]; Tencent DTS precheck, warnings vs errors [DTS 2.3] | `ready.items` / `cli.assess`, run as the first unit of `autonomous.run`; a failure for one database holds that one only (`cli._Steps.assess`) | the per-database split asks the quick gates per database; the full engine assessment is hop-wide |
| Restart / resume | DMS `RecoveryCheckpoint` and native start points [AWS 1.6]; GoldenGate Extract read/recovery checkpoints, Replicat checkpoint table [GG 1.2, 3.4]; DTS resumable export [DTS 2.5] | the move's own checkpoints (`checkpoint.py`, `ranges.plan`); the flow's units kept in `workflow.json` (`autonomous.Workflow.load` / `again`): done work kept, a catch-up proved again before the proof that depends on it | none for the flow's own state; per engine see P0-RESUME-ALL |
| Error classification | DMS task error handling per category (table, apply, data) [AWS 1.4]; DTS task states [DTS 2.5] | `decide.operational` (a way failing to run) vs migkit's own stop (`SystemExit`, never retried) vs a verdict (`decide.Verdict`) vs a fault (held, said "stopped unexpectedly") in `autonomous._unit`; a stop that needs the operator mapped to its typed question (`autonomous.question_for`) | no per-category counts or thresholds like DMS's |
| Backoff and retry | DTS automatic retry within a 5-720 minute window [DTS 2.5]; GoldenGate restart of Extract from the recovery checkpoint [GG 1.2] | `decide.attempt` over `autonomous.ladder`: as planned, again from the last position after `autonomous.backoff` (the failed attempt's own time, doubled, bounded), the copy in smaller pieces; each rung once, given-up rungs kept beside the unit (`workflow-<unit>-ways.json`) | no time-window budget; concurrency is not rebalanced by this ladder (the move's own throttle does it) |
| Conflict handling | GoldenGate HANDLECOLLISIONS, HVR resilient integrate [GG 3.3]; DTS ReportError/Ignore/Cover/ConditionCover [DTS 2.4] | idempotent apply (upsert, delete whether or not there) and `twoway` policies [DTS 1.3, 2.4 rows]; a target already holding rows is a typed business question (`target_rows`) | none new in this item |
| Lag-aware confirmation | Veridata confirm out-of-sync after replication latency [GG 5.1]; DMS `RecordFailureDelayInMinutes` [AWS 1.7]; GoldenGate heartbeat table [GG 6.1] | the follow unit waits for the target to apply what the source held (`Engine.fence_wait`, `cli._Steps.follow`) before the proof; suspects re-read behind a fence by the check's confirm pass [AWS 1.7 migkit row] | no heartbeat table across hops |
| State and evidence | DMS validation control table [AWS 1.7]; Veridata OOS files [GG 5.1] | `durable.write_json` of the units and every recovery decision; `migkit migrate --why` (`autonomous.evidence`); `decide.record` for the ways chosen | none |
| Reporting | DMS table states; DTS task progress | one progress line per unit with the engine's forecast (`Engine.forecast`, `cli._move_said`); a database-level outcome (`autonomous.outcome`) | the report page does not yet show the flow's units |

## Scenarios that would prove a difference (written, not run)

`tests/test_autonomous_scenarios.py` (sandbox, PostgreSQL): a healthy copy
in one command; a fault injected mid-copy completed by the next run with
no duplicate keys; an unreachable target stopped safely with nothing
claimed; evidence on demand. Still owed for a comparison: the same
scenarios against a managed service's measured inputs and interventions,
completion time and retry waste, on equal scope and resources.
