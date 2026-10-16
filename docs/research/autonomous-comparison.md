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
| Restart / resume | DMS `RecoveryCheckpoint` and native start points [AWS 1.6]; GoldenGate Extract read/recovery checkpoints, Replicat checkpoint table [GG 1.2, 3.4]; DTS resumable export [DTS 2.5] | the move's own checkpoints (`checkpoint.py`, `ranges.plan`); before any step runs again, the operations a run before left open are read on the target (`autonomous.settle`: `attempts.Ledger.uncertain`, `faults.landed`) - one that cannot be read is a blocker with its next step, never replayed from a kept position blind; the flow's units kept in `workflow.json` (`autonomous.Workflow.load` / `again`): done work kept, a catch-up proved again before the proof that depends on it | none for the flow's own state; per engine see P0-RESUME-ALL |
| Error classification | DMS task error handling per category (table, apply, data) [AWS 1.4]; DTS task states [DTS 2.5] | a failed unit told by its class (`faults.classify`: the engine's code first, then what it did - a write lost after it was sent is an outcome not known) and recovered as `decide.recovery` chooses from what `faults.CATALOGUE` admits for that class, under the one ladder (`faults.retried`, wrapped by `autonomous._unit`); migkit's own stop (`SystemExit`) never retried; a verdict (`decide.Verdict`) never retried; a stop that needs the operator mapped to its typed question (`autonomous.question_for`) | no per-category counts or thresholds like DMS's |
| Backoff and retry | DTS automatic retry within a 5-720 minute window [DTS 2.5]; GoldenGate restart of Extract from the recovery checkpoint [GG 1.2] | `decide.recovery` per failure: reconcile an effect not known before anything repeats it (`autonomous.reconcile`), proceed on one read as committed, pause when the same class failed the unit again, throttle (longer wait, half the size, the units left re-planned: `autonomous.replan`), retry from the last durable position after `autonomous.backoff`, probe, replan, isolate or stop; a sizing cause retuned (`tuning.sized`), never the same work again; a unit makes as many runs as its forecast work pays for at the measured time of one attempt (`autonomous.tries_for`), the ladder's own bound where nothing is measured; a throttle never cuts below one worker of the measured budget (`autonomous.scale_floor`); the flow hands each copy its share of the measured budget (`Plan.workers`, `decide.worker_budget`, `model.Model.budget`), and a plain move asks the same rung; each choice, its postcondition and whether it held kept in `workflow.json` | the try budget is the forecast's time, not a window the operator sets as DTS's is |
| Conflict handling | GoldenGate HANDLECOLLISIONS, HVR resilient integrate [GG 3.3]; DTS ReportError/Ignore/Cover/ConditionCover [DTS 2.4] | idempotent apply (upsert, delete whether or not there) and `twoway` policies [DTS 1.3, 2.4 rows]; a target already holding rows is a typed business question (`target_rows`) | none new in this item |
| Lag-aware confirmation | Veridata confirm out-of-sync after replication latency [GG 5.1]; DMS `RecordFailureDelayInMinutes` [AWS 1.7]; GoldenGate heartbeat table [GG 6.1] | the follow unit waits for the target to apply what the source held (`Engine.fence_wait`, `cli._Steps.follow`) before the proof; suspects re-read behind a fence by the check's confirm pass [AWS 1.7 migkit row] | no heartbeat table across hops |
| State and evidence | DMS validation control table [AWS 1.7]; Veridata OOS files [GG 5.1] | `durable.write_json` of the units and every recovery decision; `migkit migrate --why` (`autonomous.evidence`); `decide.record` for the ways chosen | none |
| Reporting | DMS table states; DTS task progress | one progress line per unit with the engine's forecast (`Engine.forecast`, `cli._move_said`); after an isolate or a throttle the scope left, its order (shortest forecast first) and the measured work left said (`autonomous.replan`); a database-level outcome (`autonomous.outcome`) | the report page does not yet show the flow's units |
| Cutover switch | DMS stops the task, the application is pointed at the target by the operator [AWS 1.6]; DTS switch with a reverse task offered [DTS 2.5]; GoldenGate switchover with a reverse Extract [GG 3.4] | proposed only when every database is proved ready in this run, as the typed question `switch` with the evidence (`autonomous.propose_switch`); made only on the operator's yes - `--switch` given up front is that yes, `--go` only writes - through the cutover itself (`cli._cutover`); a run again proves catch-up and readiness again before it asks | none in the flow; the switch's own drain and point of no return are `cutover.run`'s |

## Measured scenarios

`tests/test_autonomous_scenarios.py` (sandbox, MySQL and PostgreSQL, 20000
rows) measures each scenario the same way and writes one JSON line a run
to `MIGKIT_MEASURE_OUT`: inputs (arguments of the first command),
interventions (runs the operator had to start again), completed (exit 0
and every key once on the target), recovered (recoveries whose
postcondition held, of all), retry waste (recoveries that failed again),
seconds. `autonomous.comparison` fills this table from those lines. The
inputs are the arguments of the first command (`migrate --hop t --goal copy
--go`: 6), the same for every scenario, so they are written; the cells below are
written from the run of 2026-10-16 (docker sandbox: mysql-rowmeta pair,
mysql 8.4.11; pg pair, postgres 16): no copy completed on either engine -
board notes W15J-PG16 and W15J-MYBULK, strict xfails in the harness -
so a row's "no" is that run's measured outcome; a row no run reached says
"to be measured"; a
managed service column needs the same scenario on equal scope and
resources and is not claimed.

| Scenario | Engine | Inputs | Interventions | Completed | Recovered | Retry waste | Seconds | Managed service |
|---|---|---|---|---|---|---|---|---|
| healthy | MySQL | 6 | 0 | no | 0% | 1 | 7.58 | to be measured |
| healthy | PostgreSQL | 6 | to be measured | to be measured | to be measured | to be measured | to be measured | to be measured |
| fault mid-copy, recovered in the run | MySQL | 6 | 0 | no | 0% | 1 | 7.15 | to be measured |
| fault mid-copy, recovered in the run | PostgreSQL | 6 | to be measured | to be measured | to be measured | to be measured | to be measured | to be measured |
| crash mid-copy, run again | MySQL | 6 | 1 | no | 0% | 2 | 10.47 | to be measured |
| crash mid-copy, run again | PostgreSQL | 6 | to be measured | to be measured | to be measured | to be measured | to be measured | to be measured |
| refused target (unrecoverable) | MySQL | 6 | 0 | no | none needed | 0 | 3.95 | to be measured |
| refused target (unrecoverable) | PostgreSQL | 6 | 0 | no | none needed | 0 | 62.64 | to be measured |

The recovery choices themselves are proved on fakes in
`tests/test_autonomous_workflow.py`: reconcile before a repeat, a write
read as committed not run again, one that cannot be read not run again,
a sizing cause retuned, a refused account stopped at once, a throttle
re-planning the units left, the shortest forecast first after an
isolate, bounded tries, the switch asked and made only on a yes.
