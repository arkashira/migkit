# User-reported migration needs: evidence, mechanisms and acceptance backlog

Research date: 2026-10-17 Asia/Bangkok. Current checkout HEAD observed: d333f5fb; working-tree changes exist. This addendum extends the earlier official-document research with **28 concrete testable backlog children**, linked to the earlier 72 entries. Total proposed integration: **100 entries**.

## Evidence discipline

69 distinct additional URLs are screened here. Primary issue reports are direct evidence that someone reported a problem for a stated version/configuration. They do not prove all users have it, that the reporter's root-cause theory is correct, or that the problem persists in the latest release. Closed does not automatically mean fixed. Some reports are historical (2014-2024), others are 2025-2026; all remain version-bounded.

Community questions and migration stories are qualitative demand signals, not representative market surveys or engineering proof. Votes, repeated search results and syndicated copies are not independent prevalence measurements. Technical remedies are our proposed design and acceptance tests, grounded where possible in project/vendor docs and code; a community workaround is not automatically safe or adopted. Vendor advice that reduces durability, drops protections, truncates LOBs or skips validation must still satisfy the migkit contract.

The original 55-product/workflow report remains the official-document capability baseline. This addendum adds schema-deployment concerns from Liquibase/Flyway because they intersect live application compatibility and rollback; it does not conflate schema deployments with whole-database relocation. No new runtime defect in migkit is asserted solely from an external issue.

## Why migkit is beyond a script

Observed architecture includes typed choices/capability constraints, a cost model and resource scheduler, durable workflow/effect state, recovery/reconciliation, shared verification verdicts, repair strategies and code-execution proof. That is platform architecture. Kit can remain the product name; maturity should be earned per engine/flow from installed-command proof, not renamed into existence.

## Changes observed since the prior review

The checkout advanced from the earlier 750b38a5 baseline to d333f5fb. The working index now labels several earlier enterprise/autopilot items DONE. These claims were not independently rerun in this documentation task. Earlier bug repros/test results remain historical baseline evidence and are marked for revalidation; new entries extend acceptance coverage instead of deleting existing safeguards. Current tailtables already checks scope changes, and PostgreSQL applied_lsn already distinguishes target application from source acknowledgements. The corresponding requests below extend consistency across all adapters; they are not claims those ideas are absent.

## Demand-to-mechanism records


### NEED01.resume-scope — Persist filter and mapping identity across every restart

**P1; OPEN extension.** Parent: SMART-N16, RCH17, attempts, tailtables.

**Evidence:** A version-specific MongoDB incremental-snapshot report says resumed chunk queries lost additional-conditions; the issue is closed, not proof of a current defect.

**migkit mechanism:** Keep scope/filter/mapping/tenant predicates in durable unit identity; reject incompatible checkpoint reuse. Current tailtables already checks changed scope, so extend this to every adapter and boot-snapshot path.

**Acceptance:** Crash at each chunk with a nontrivial row filter; no out-of-scope row is read/written and skipped in-scope work remains pending.

Sources: [P067](https://github.com/debezium/dbz/issues/1807).


### NEED02.snapshot-state — Preserve pending incremental work through boot snapshots

**P1; OPEN extension.** Parent: RCH11, SMART-N17, autonomous, checkpoint.

**Evidence:** An issue reports that a boot-time snapshot can replace a restored offset context and discard pending incremental work silently.

**migkit mechanism:** Represent full snapshot, incremental snapshot and CDC state as separate durable sub-workflows with explicit transfer invariants; a phase may not reset another phase's obligations.

**Acceptance:** Restart all supported snapshot modes at 30 percent completion; pending units remain accounted for and coverage totals never silently shrink.

Sources: [P065](https://github.com/debezium/dbz/issues/2325).


### NEED03.cancel-recovery — Make invalid snapshot requests and cancellation recoverable

**P1; OPEN extension.** Parent: SMART-N21, RCH37, attempts.

**Evidence:** A user reports invalid snapshot metadata blocking subsequent requests and requiring manual offset edits.

**migkit mechanism:** Validate initialization before publishing live state; transactional initialize/commit/cancel states retain diagnostic evidence while removing only owned invalid work.

**Acceptance:** Invalid key/filter, cancellation and process death cannot strand the workflow or require destructive global offset reset.

Sources: [P066](https://github.com/debezium/dbz/issues/1533).


### NEED04.ack-lattice — Distinguish read, durably retained, applied and proved positions

**P1; OPEN extension.** Parent: RCH08, RCH16, postgres.applied_lsn, pipeline.

**Evidence:** A historical closed issue describes source LSN acknowledgement preceding successful destination loading.

**migkit mechanism:** Maintain separate monotone positions and use each only for the guarantee it establishes; source log release requires durable reconstructability, cutover requires target committed-prefix plus required proof. Existing applied_lsn already distinguishes target application from source ack; extend shared contracts.

**Acceptance:** Crash after read/spool/apply/ack independently; retained data reconstructs every released change, and ack alone cannot authorize cutover.

Sources: [P002](https://github.com/airbytehq/airbyte/issues/15214).


### NEED05.quiet-source — Manage WAL on low-volume or heavily filtered sources

**P1; OPEN extension.** Parent: RCH16, SMART-N19, postgres, model.

**Evidence:** Troubleshooting and issue reports describe retained WAL when few selected events flow or acknowledgements lag.

**migkit mechanism:** Model safe keepalive/heartbeat and progress for filtered intervals separately from business events; select source-read-only methods first and make any heartbeat write an explicit source effect.

**Acceptance:** A quiet database plus noisy excluded tables advances safe retention state without falsely claiming target-applied business writes or unbounded disk growth.

Sources: [P001](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/postgres/postgres-troubleshooting.md), [P003](https://github.com/airbytehq/airbyte/issues/15040), [P004](https://github.com/airbytehq/airbyte/issues/70887).


### NEED06.offset-disagreement — Diagnose offset-store versus slot disagreement without silent skipping

**P1; OPEN extension.** Parent: SMART-N17, RCH15, reconciliation.

**Evidence:** A closed enhancement request asks for flexible recovery when offset and replication-slot LSNs disagree.

**migkit mechanism:** Classify missing-log, already-retained, already-applied and unknown intervals from evidence; automatic recovery needs an interval reconstruction certificate, not a choose-larger-LSN rule.

**Acceptance:** Offset ahead/behind slot, failover and stale backup combinations either reconstruct every interval or explicitly require bounded reseed.

Sources: [P006](https://github.com/debezium/dbz/issues/1146).


### NEED07.handoff-overlap — Prove gap-free snapshot and CDC composition after interruptions

**P1; OPEN extension.** Parent: RCH11, RCH08, movers.

**Evidence:** Historical pgcopydb and SQL Server CDC reports describe resume-phase confusion, copy-to-follow gaps or overlapping snapshot/stream events.

**migkit mechanism:** Persist the snapshot lifetime, lower/upper capture fences and dedup identity as one handoff contract; no manual two-step path can omit an interval.

**Acceptance:** Writers run throughout seed, handoff and crash/restart; compare every source commit and duplicate multiplicity, not only final keyed upserts.

Sources: [P040](https://github.com/dimitri/pgcopydb/issues/659), [P041](https://github.com/dimitri/pgcopydb/issues/263), [P018](https://github.com/debezium/dbz/issues/1336).


### NEED08.identity-semantics — Preserve distinct keys across target collation and bulk dedup

**P1; OPEN extension.** Parent: SMART-N14, RCH07, canon, hetero.

**Evidence:** Closed destination reports describe case-sensitive identifiers merging under case-insensitive collation and duplicates during bulk loading.

**migkit mechanism:** Define identity equivalence before mapping or dedup; detect non-injective target key representation and choose a supported lossless key strategy or refuse. Bulk and row paths share the same identity contract.

**Acceptance:** Case-only IDs, Unicode normalization variants and replayed bulk batches never merge distinct source entities or duplicate one effect.

Sources: [P020](https://github.com/airbytehq/airbyte/issues/60828), [P021](https://github.com/airbytehq/airbyte/issues/65938).


### NEED09.backfill-race — Catch column backfills that precede schema discovery

**P1; OPEN extension.** Parent: RCH09, RCH10, schema_drift, tailtables.

**Evidence:** A 2026 user report asks for targeted repair when backfill events occur before the connector refreshes schema.

**migkit mechanism:** Record relation/schema epochs and observed gaps; a newly accepted column has an explicit historical-coverage obligation. Choose selective backfill or shadow resnapshot and reconcile buffered events.

**Acceptance:** ADD COLUMN -> UPDATE old rows -> delayed schema discovery -> restart converges without an unnecessary whole-database reload.

Sources: [P068](https://github.com/airbytehq/airbyte/issues/80920).


### NEED10.partial-images — Treat absent unchanged LOB/TOAST fields differently from NULL

**P1; OPEN extension.** Parent: RCH02, RCH17, engines/postgres, canon.

**Evidence:** Vendor engineering describes logical-decoding schema and unchanged-TOAST pitfalls.

**migkit mechanism:** Use a typed absent/unchanged/null/value representation; choose before-image lookup/cache/re-fetch only with a valid row version. Never overwrite an unchanged large value with a placeholder.

**Acceptance:** UPDATE only a small column, concurrent LOB updates and reconnect preserve exact LOB bytes and SQL NULL semantics.

Sources: [P059](https://blog.peerdb.io/overcoming-pitfalls-of-postgres-logical-decoding).


### NEED11.lob-transaction — Keep LOB fetching compatible with constraints and commit semantics

**P1; OPEN extension.** Parent: RCH02, RCH03, pieces.

**Evidence:** A Google forum user reports a non-null CLOB apply failure; its proposed root cause is only a hypothesis. AWS docs separately explain LOB size/memory constraints.

**migkit mechanism:** Probe the actual adapter's LOB materialization and constraint behavior, price inline/streamed paths and retain transaction identity through delayed fetching. Do not relax NOT NULL silently.

**Acceptance:** Non-null empty/large/mutating CLOBs and failure between row/LOB steps either commit valid state atomically or remain unexposed and recoverable.

Sources: [P029](https://discuss.google.dev/t/error-during-migrate-to-cloud-sql-for-postgresql-from-oracle-using-data-migration-service-dms/163387), [P032](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.LOBSupport.html).


### NEED12.decompression-budget — Budget uncompressed transaction size and streaming decode

**P1; OPEN extension.** Parent: SMART-N20, RCH03, tuning, spool.

**Evidence:** A version-specific report describes inability to decode compressed transaction payloads above 2GB uncompressed.

**migkit mechanism:** Track compressed bytes, expanded bytes, nested allocations, transaction spill and decoder limits; select bounded streaming/spill paths while preserving transaction atomicity.

**Acceptance:** Highly compressible huge transactions stay within process budgets or are refused/reseeded safely without acknowledging partial effects.

Sources: [P042](https://github.com/debezium/dbz/issues/1503).


### NEED13.process-memory — Bound total memory across migkit, drivers and external movers

**P1; OPEN extension.** Parent: SMART-N23, RCH35, sizing, runner.

**Evidence:** Historical pgloader reports and build guidance show that row batches alone do not capture runtime/heap limits.

**migkit mechanism:** Budget child-process heaps, decoder buffers, driver prefetch, compression, queues and concurrent proofs; distinguish OS kill, driver error and throughput stall. Reconcile target state before changing mover.

**Acceptance:** Constrained-memory installed run with large rows and concurrent phases cannot exceed the global budget indefinitely or restart from an unverified position.

Sources: [P035](https://github.com/dimitri/pgloader/issues/1237), [P037](https://github.com/dimitri/pgloader/issues/64), [P038](https://github.com/dimitri/pgloader/issues/399), [P039](https://github.com/dimitri/pgloader/blob/main/INSTALL.md).


### NEED14.progress-cause — Explain blocked progress and name the actual prerequisite

**P2; OPEN extension.** Parent: SMART-N15, SMART-N21, guide, session.

**Evidence:** Users report migrations remaining at restoring or failing with opaque process errors; these are specific reports, not established current provider defects.

**migkit mechanism:** Expose causal phase states: waiting backup, lock, snapshot, log drain, network route, permission or child process. Use observed checkpoints for liveness and separate ETA uncertainty from a hidden required user action.

**Acceptance:** Stalled restore and failed child-process scenarios identify the blocking operation and a precise safe next step; no generic increasing percent pretends progress.

Sources: [P024](https://learn.microsoft.com/en-us/answers/questions/1188523/azure-data-studios-azure-sql-migration-hangs-with), [P025](https://learn.microsoft.com/en-us/answers/questions/1388527/during-database-migration-in-online-mode-using-azu), [P028](https://discuss.google.dev/t/dms-mysql-migration-fails-with-waitid-no-child-processes-recurring-across-different-configs/390779).


### NEED15.lock-impact — Predict and enforce a live workload lock budget

**P1; OPEN extension.** Parent: AUTO.deferred-index, RCH25, schema, locks.

**Evidence:** Community questions seek safe large-table schema evolution under running queries; technical design documents discuss online DDL.

**migkit mechanism:** Probe lock/rewrite behavior per operation/version; schedule short lock acquisition, cancellation and expand/backfill/contract alternatives within the authorized scope.

**Acceptance:** Long-lived transaction plus index/type/constraint change cannot exceed the lock budget or publish partially restored constraints; practice on representative concurrency.

Sources: [P045](https://www.reddit.com/r/PostgreSQL/comments/1grykyv), [P049](https://www.reddit.com/r/SQL/comments/1whl5jc/how_do_you_handle_schema_migrations_on_a_live/), [P050](https://github.com/yugabyte/yugabyte-db/blob/master/architecture/design/online-schema-migrations.md).


### NEED16.control-upgrade — Upgrade migkit state without forcing data resync

**P1; OPEN extension.** Parent: SMART-N22, durable, config, packaging.

**Evidence:** A control-plane upgrade report asks to avoid wiping configuration and resyncing every connection; vendor upgrade guidance emphasizes retaining history.

**migkit mechanism:** Version durable state and adapter checkpoint formats; stage migrations on a copy, validate invariants, atomic swap and supported downgrade/export. Data-plane state survives an interrupted control-plane upgrade.

**Acceptance:** Upgrade/kill/downgrade tests preserve hop definitions, pending work and exact resume positions without full resync unless incompatibility is explicitly established.

Sources: [P060](https://github.com/airbytehq/airbyte/issues/80770), [P064](https://airbyte.com/blog/introducing-airbyte-destinations-v2-typing-deduping).


### NEED17.lock-capability — Probe coordination primitives on compatible-looking services

**P1; OPEN extension.** Parent: SMART-N9, RCH15, fencing, capabilities.

**Evidence:** Issue reports show PG-compatible endpoints missing advisory-lock/catalog/protocol features expected by migration tools.

**migkit mechanism:** Discover required lock/catalog/replication primitives by safe behavioral probe; choose an equivalent fenced lease where supported, not a brand-name fallback.

**Acceptance:** A PG-wire endpoint without advisory locks cannot accidentally run two migrators; stale-owner takeover is fenced and missing primitive support stays explicit.

Sources: [P063](https://github.com/flyway/flyway/issues/4022), [P023](https://github.com/cockroachdb/cockroach/issues/84505).


### NEED18.cost-envelope — Forecast and enforce complete migration operating cost

**P2; OPEN extension.** Parent: REV-D4, SMART-N6, machine, jointplan.

**Evidence:** Anecdotal discussions ask for predictable billing and less connector-maintenance burden; they do not establish current list prices or representative market prevalence.

**migkit mechanism:** Account for source/target compute, egress, staging, scans, changed-row billing, resyncs, verification and post-migration idle resources. Use timestamped user/provider rates with uncertainty; price unknowns remain unknown.

**Acceptance:** A targeted-refresh plan can be compared to full reload under the same proof contract; actual spend and forecast error recorded, no stale community price treated as current pricing.

Sources: [P011](https://www.reddit.com/r/dataengineering/comments/1u81nqa/evaluating_dlt_vs_fivetran_for_a_small_team_that/), [P012](https://www.reddit.com/r/dataengineering/comments/1ii4ry5), [P013](https://www.reddit.com/r/dataengineering/comments/1jwrfit/thinking_of_migrating_from_fivetran_to_hevo_would/), [P014](https://www.reddit.com/r/dataengineering/comments/1uk2cl7/portable_vs_fivetran_anyone_make_the_swap/).


### NEED19.private-proof — Verify masked/restricted data without exposing source values

**P1; OPEN extension.** Parent: RCH17, RCH19, transforms, evidence.

**Evidence:** A first-person migration story describes owner-only PII and inaccessible data; AWS documents migration masking workflows.

**migkit mechanism:** Use owner-executed collectors, local type-aware proof/masking and privacy-preserving evidence where supported. Preserve FK-consistent masking and exact authorized scope; do not claim equivalence when transforms are unverifiable.

**Acceptance:** Secrets/PII stay out of logs, counterexamples, remote model requests and support bundles; missing access yields explicit proof debt rather than inferred success.

Sources: [P046](https://www.reddit.com/r/dataengineering/comments/1mwvz6p/what_is_the_most_painful_data_migration_project/), [P053](https://aws.amazon.com/blogs/database/mask-pii-data-using-aws-dms-and-amazon-macie-during-migration/).


### NEED20.exit-portability — Keep output mappings and checkpoints portable

**P2; OPEN extension.** Parent: SMART-N11, SMART-N22, config, manifests.

**Evidence:** Historical user complaints cite renamed fields, forced object selection and costly reprocessing; these remain anecdotes, not general product facts.

**migkit mechanism:** Export machine-readable schema/mapping lineage, configuration, tool versions, data manifests and supported checkpoint semantics. Preserve original names with collision-aware mappings; compatible engine upgrade avoids forced history reload.

**Acceptance:** A clean migkit installation can inspect/export the project and resume compatible supported state without proprietary hidden metadata or indiscriminate source rescan.

Sources: [P017](https://www.reddit.com/r/dataengineering/comments/11xbpjy), [P064](https://airbyte.com/blog/introducing-airbyte-destinations-v2-typing-deduping).


### NEED21.app-rollback — Coordinate application versions with reversible schema evolution

**P1; OPEN extension.** Parent: RCH12, RCH13, SMART-N18.

**Evidence:** A release-management discussion asks how to roll back app/schema when a git revert removes down-migration files; rollback docs warn about evolving data.

**migkit mechanism:** Keep immutable operation/recovery artefacts independent of the application commit; model old/new app compatibility, expand-contract windows and irreversible data changes. Choose forward-fix versus reverse migration from current writes, not git state.

**Acceptance:** Rollback after new-version writes either preserves them through a proved reverse path or refuses incompatible app rollback before traffic shifts.

Sources: [P015](https://github.com/werf/werf/discussions/5749), [P062](https://docs.liquibase.com/community/reference-guide-5-0/init-update-and-rollback-commands/what-are-rollback-commands).


### NEED22.rollback-set — Define targeted rollback by effect identity and dependencies

**P1; OPEN extension.** Parent: RCH17, repairlog, attempts.

**Evidence:** A version-specific issue reports changed tag-boundary behavior; current docs describe targeted rollback with retained history.

**migkit mechanism:** Resolve a rollback to an immutable set of owned effects and dependency constraints, not a mutable tag or file ordering alone. Check target postconditions before undo.

**Acceptance:** Changes before/at/after a tag, unrelated later work and concurrent target edits are handled deterministically without removing the wrong operation.

Sources: [P054](https://github.com/liquibase/liquibase/issues/6728), [P056](https://docs.liquibase.com/secure/user-guide-5-1/what-are-targeted-rollbacks).


### NEED23.support-reproducer — Generate bounded secret-safe diagnostic reproductions

**P2; OPEN extension.** Parent: RCH34, RCH39, diagnostics, evidence.

**Evidence:** An old issue was closed for insufficient reproduction data; opaque provider-process failures show why configuration alone is insufficient.

**migkit mechanism:** Capture tool/engine versions, contract/model/fact epochs, phase timing, redacted logs and minimal synthetic failure fixtures. Preserve all effect/position relations but no original credentials or sensitive rows.

**Acceptance:** A support bundle reproduces selected failure classes offline and passes a secrets/PII scan; unknown proprietary internals remain explicitly unknown.

Sources: [P036](https://github.com/dimitri/pgloader/issues/371), [P028](https://discuss.google.dev/t/dms-mysql-migration-fails-with-waitid-no-child-processes-recurring-across-different-configs/390779).


### NEED24.partial-access — Handle restricted or nonstandard exports with honest coverage

**P1; OPEN extension.** Parent: SMART-N12, SMART-N14, saas, scope.

**Evidence:** A user describes a SaaS migration without a public API and data accessible only to owners; this is one scenario, not support for arbitrary extraction.

**migkit mechanism:** Accept explicitly provided approved export/manifests or registered authorized collectors, validate completeness/version/schema and retain source lineage. Do not broaden account/network access or invent records to fill inaccessible data.

**Acceptance:** Partial exports cannot produce a whole-source same verdict; normalized target mappings preserve records and reconciliation identifies absent source evidence.

Sources: [P046](https://www.reddit.com/r/dataengineering/comments/1mwvz6p/what_is_the_most_painful_data_migration_project/).


### NEED25.adaptive-bounds — Validate adaptive limits and routing across iterative cutovers

**P1; OPEN extension.** Parent: SMART-N20, RCH30, sizing, topology.

**Evidence:** Voyager release notes describe corrected hangs with inconsistent parallelism bounds and lost custom CDC routing across iterations.

**migkit mechanism:** Validate min/start/max as one feasible envelope; persist routing/partition rules across every iteration and recheck after topology changes. Settings cannot be silently defaulted after restart.

**Acceptance:** max below start refuses clearly; tablet/shard split and repeated cutover preserve routing, progress and worker budgets.

Sources: [P008](https://docs.yugabyte.com/stable/yugabyte-voyager/release-notes/).


### NEED26.canonical-oracle — Prove canonicalization independently of rendered strings

**P1; OPEN extension.** Parent: RCH07, RCH36, canon.

**Evidence:** A DVT issue demonstrates decimal-to-string differences; changelog documents later normalization fixes. VDiff docs describe collation-induced comparison ordering.

**migkit mechanism:** Use type/version-aware canonical semantics with exact scale/precision policy and independent test vectors. Separate display, identity sorting and equality; broad trimming/lowercasing is not automatically valid.

**Acceptance:** Decimal leading zeros/scale, signed zero, trailing spaces, Unicode/collation and reordered keys detect real drift without hiding semantic differences.

Sources: [P033](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1134), [P034](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/CHANGELOG.md), [P022](https://vitess.io/docs/archive/14.0/reference/vreplication/vdiff/).


### NEED27.lineage-evidence — Expose unknown dynamic lineage instead of inferring completeness

**P2; OPEN extension.** Parent: RCH18, RCH21, storedcode/appscan, capabilities.

**Evidence:** Community questions seek SQL lineage; a Voyager request asks for target-version-specific extension checks.

**migkit mechanism:** Combine syntax-level references, approved runtime traces and versioned catalogues with explicit unresolved dynamic SQL/extension behavior. Scope percentages must include unavailable dependencies.

**Acceptance:** A routine with dynamic identifier construction or unsupported extension cannot appear fully assessed just because static parsing succeeded.

Sources: [P047](https://www.reddit.com/r/dataengineering/comments/pmecrp), [P052](https://github.com/yugabyte/yb-voyager/issues/2420).


### NEED28.demand-benchmark — Evaluate endpoint-only migration against real pain scenarios

**P1; OPEN extension.** Parent: RCH40, SMART-N25, autonomous comparison.

**Evidence:** Users ask how to validate migrations, reduce operator skill burden and deal with access/normalization constraints; these are qualitative demand signals.

**migkit mechanism:** Add the failure scenarios above to installed-command tests under stored intent and authorization policies. Measure manual technical interventions, verified completion, source impact, waste, cost and honest blockers; separate schema deployment from database relocation.

**Acceptance:** Publish reproducible results per pair with injected compound faults; neither feature count nor sampled anecdotes establishes best-in-market or universally zero-intervention operation.

Sources: [P069](https://www.reddit.com/r/SQL/comments/1qk44yi/database_migration/), [P011](https://www.reddit.com/r/dataengineering/comments/1u81nqa/evaluating_dlt_vs_fivetran_for_a_small_team_that/), [P046](https://www.reddit.com/r/dataengineering/comments/1mwvz6p/what_is_the_most_painful_data_migration_project/).


## Product direction inferred from these needs

These are scenario forecasts, not vendor promises. Users want fewer fragile reset/resync procedures, lower manual skill burden, predictable all-in costs, explicit progress explanations, privacy-preserving operation and proof that the application still works. Connector state and checkpoint upgrades are a migration problem themselves. A useful lead for migkit is a durable task contract with exact coverage/effect identity, self-diagnosis from evidence and an explanation of the smallest missing prerequisite. Predictive/self-healing behavior should preserve information and obligations rather than hide errors by reset, coercion or dropping data.

Priorities: first NEED01-12 (scope, handoff, identity and data integrity); then NEED13-17/21-22/25-26 (operational correctness and recovery); then NEED18-20/23-24/27-28 (economics, privacy, portability, user effort and proof breadth). These are child scenarios of existing RCH/SMART work; do not build 28 unrelated engines.

## Measurement and completion gates

For each applicable pair run an installed-command migration under an explicit stored intent policy. Inject the scenario, including compound faults. Record scope units expected/completed, loss/duplicate effects, exact proof coverage, source impact, retry waste, peak total memory/spool, actual billed work where available, progress/ETA calibration and manual technical interventions. Simulations and fake adapters cannot stand in for real-engine acceptance. Do not infer best-in-market from a feature count or a collection of anecdotes.

## Additional source register

Page-retrieved records were fetched; excerpt-only entries are labelled. A technical requirement needs the relevant official document or controlled reproduction, not solely the anecdote.


- **P001** — [airbyte/docs/integrations/sources/postgres/postgres-troubleshooting.md at master · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/postgres/postgres-troubleshooting.md); official-technical-documentation; search-excerpt.


- **P002** — [postgres cdc lsn commit on source side should happen after loading data into the destination · Issue #15214 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/15214); first-person-issue-or-forum-report; page-retrieved.


- **P003** — [Investigate usage of heartbeat property in debezium for CDC · Issue #15040 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/15040); first-person-issue-or-forum-report; search-excerpt.


- **P004** — [[source-postgresql] CDC sync is only allowing WAL to be emptied every two syncs · Issue #70887 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/70887); first-person-issue-or-forum-report; page-retrieved.


- **P005** — [debezium/documentation/modules/ROOT/pages/connectors/postgresql.adoc at main · debezium/debezium · GitHub](https://github.com/debezium/debezium/blob/main/documentation/modules/ROOT/pages/connectors/postgresql.adoc); official-technical-documentation; search-excerpt.


- **P006** — [Introduce flexible recovery from LSN desynchronisation [DBZ-9688] · Issue #1146 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/1146); first-person-issue-or-forum-report; page-retrieved.


- **P007** — [Pull requests · PeerDB-io/peerdb · GitHub](https://github.com/PeerDB-io/peerdb/pulls); vendor-or-project-publication; search-excerpt.


- **P008** — [What's new in YugabyteDB Voyager | YugabyteDB Docs](https://docs.yugabyte.com/stable/yugabyte-voyager/release-notes/); official-technical-documentation; page-retrieved.


- **P009** — [PostgreSQL: Re: Database migration to RDS issues permissions](https://www.postgresql.org/message-id/042619bb-06b5-2b3f-1bb3-0c436bd91ab1%40aklaver.com); official-technical-documentation; search-excerpt.


- **P010** — [streaming replication on-premise posgresql(src) -> aws rds postgresql (dst) | AWS re:Post](https://www.repost.aws/questions/QUoNntBzyEQMiN18US16KQzg/streaming-replication-on-premise-posgresql-src-aws-rds-postgresql-dst); first-person-issue-or-forum-report; search-excerpt.


- **P011** — [Evaluating DLT vs Fivetran for a small team that lives in SQL, not Python](https://www.reddit.com/r/dataengineering/comments/1u81nqa/evaluating_dlt_vs_fivetran_for_a_small_team_that/); community-anecdote; page-retrieved.


- **P012** — [Fivetran Pricing](https://www.reddit.com/r/dataengineering/comments/1ii4ry5); community-anecdote; search-excerpt.


- **P013** — [Thinking of Migrating from Fivetran to Hevo — Would Love Your Input](https://www.reddit.com/r/dataengineering/comments/1jwrfit/thinking_of_migrating_from_fivetran_to_hevo_would/); community-anecdote; page-retrieved.


- **P014** — [Portable vs Fivetran, anyone make the swap?](https://www.reddit.com/r/dataengineering/comments/1uk2cl7/portable_vs_fivetran_anyone_make_the_swap/); community-anecdote; search-excerpt.


- **P015** — [Release rollback which contains database migrations via git revert · werf werf · Discussion #5749 · GitHub](https://github.com/werf/werf/discussions/5749); first-person-issue-or-forum-report; search-excerpt.


- **P016** — [streaming replication on-premise posgresql(src) -> aws rds postgresql (dst) | AWS re:Post](https://www.repost.aws/es/questions/QUoNntBzyEQMiN18US16KQzg/streaming-replication-on-premise-posgresql-src-aws-rds-postgresql-dst); vendor-or-project-publication; search-excerpt.


- **P017** — [Beware of Fivetran and other ELT tools.](https://www.reddit.com/r/dataengineering/comments/11xbpjy); community-anecdote; search-excerpt.


- **P018** — [Duplicate events from the snapshot and streaming phases [DBZ-8704] · Issue #1336 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/1336); first-person-issue-or-forum-report; page-retrieved.


- **P019** — [airbyte/docs/platform/using-airbyte/core-concepts/sync-modes/incremental-append-deduped.md at master · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/blob/master/docs/platform/using-airbyte/core-concepts/sync-modes/incremental-append-deduped.md); official-technical-documentation; search-excerpt.


- **P020** — [[destination-mssql] Case-Insensitive Collation Causes Incorrect Deduplication of Case-Sensitive IDs · Issue #60828 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/60828); first-person-issue-or-forum-report; page-retrieved.


- **P021** — [MSSQL Bulk Loader De-duplication Bug: Duplicate Records with Identical _airbyte_raw_id Values · Issue #65938 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/65938); first-person-issue-or-forum-report; page-retrieved.


- **P022** — [The Vitess Docs | VDiff](https://vitess.io/docs/archive/14.0/reference/vreplication/vdiff/); vendor-or-project-publication; search-excerpt.


- **P023** — [migrations: Support AWS DMS as a source · Issue #84505 · cockroachdb/cockroach · GitHub](https://github.com/cockroachdb/cockroach/issues/84505); first-person-issue-or-forum-report; search-excerpt.


- **P024** — [Azure Data Studios Azure SQL Migration hangs with Restoring Migration Status - Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/1188523/azure-data-studios-azure-sql-migration-hangs-with); first-person-issue-or-forum-report; page-retrieved.


- **P025** — [During database migration in online mode using Azure data studio status is restoring only on sqlmi. - Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/1388527/during-database-migration-in-online-mode-using-azu); first-person-issue-or-forum-report; search-excerpt.


- **P026** — [Tutorial: Migrate SQL Server to SQL Server on an Azure Virtual Machine with Azure DMS (Online) - Azure Database Migration Service | Microsoft Learn](https://learn.microsoft.com/en-us/data-migration/sql-server/virtual-machines/database-migration-service-online); vendor-or-project-publication; search-excerpt.


- **P027** — [Tutorial: Migrate SQL Server to SQL Server on an Azure Virtual Machine with Azure DMS (Offline) - Azure Database Migration Service | Microsoft Learn](https://learn.microsoft.com/en-us/data-migration/sql-server/virtual-machines/database-migration-service-offline); vendor-or-project-publication; search-excerpt.


- **P028** — [DMS MySQL migration fails with "waitid: no child processes" — recurring across different configs - Database - Google Developer forums](https://discuss.google.dev/t/dms-mysql-migration-fails-with-waitid-no-child-processes-recurring-across-different-configs/390779); first-person-issue-or-forum-report; page-retrieved.


- **P029** — [Error during migrate to Cloud SQL for PostgreSQL from Oracle using Data Migration Service (DMS) - Database - Google Developer forums](https://discuss.google.dev/t/error-during-migrate-to-cloud-sql-for-postgresql-from-oracle-using-data-migration-service-dms/163387); first-person-issue-or-forum-report; page-retrieved.


- **P030** — [Troubleshoot data mismatch between AWS DMS source and target databases | AWS re:Post](https://www.repost.aws/knowledge-center/dms-source-target-data-mismatch); official-technical-documentation; search-excerpt.


- **P031** — [DMS serverless replication stuck on single small table with LOBs | AWS re:Post](https://repost.aws/questions/QU_9nRuyEDQfGO5W6xgfXUxw/dms-serverless-replication-stuck-on-single-small-table-with-lobs); first-person-issue-or-forum-report; search-excerpt.


- **P032** — [Setting LOB support for source databases in an AWS DMS task - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.LOBSupport.html); official-technical-documentation; page-retrieved.


- **P033** — [Row validation failing for TD DECIMAL(15,2) and BQ NUMERIC(15,2) · Issue #1134 · GoogleCloudPlatform/professional-services-data-validator](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1134); first-person-issue-or-forum-report; page-retrieved.


- **P034** — [professional-services-data-validator/CHANGELOG.md at develop · GoogleCloudPlatform/professional-services-data-validator · GitHub](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/CHANGELOG.md); official-technical-documentation; search-excerpt.


- **P035** — [Sqlite to postgres load killed · Issue #1237 · dimitri/pgloader · GitHub](https://github.com/dimitri/pgloader/issues/1237); first-person-issue-or-forum-report; search-excerpt.


- **P036** — [Couldn't write to #<SB-SYS:FD-STREAM Broken pipe · Issue #371 · dimitri/pgloader · GitHub](https://github.com/dimitri/pgloader/issues/371); first-person-issue-or-forum-report; search-excerpt.


- **P037** — [pgloader consumes memory until it gets killed · Issue #64 · dimitri/pgloader · GitHub](https://github.com/dimitri/pgloader/issues/64); first-person-issue-or-forum-report; search-excerpt.


- **P038** — [Heap exhausted migrating using docker · Issue #399 · dimitri/pgloader · GitHub](https://github.com/dimitri/pgloader/issues/399); first-person-issue-or-forum-report; search-excerpt.


- **P039** — [pgloader/INSTALL.md at main · dimitri/pgloader · GitHub](https://github.com/dimitri/pgloader/blob/main/INSTALL.md); official-technical-documentation; search-excerpt.


- **P040** — [pgcopydb_v0.15 resume ERRORS with copy follow --split-tables-larger-than --resume · Issue #659 · dimitri/pgcopydb · GitHub](https://github.com/dimitri/pgcopydb/issues/659); first-person-issue-or-forum-report; page-retrieved.


- **P041** — [Missing data when copying the DB in the "manual" 2-steps mode · Issue #263 · dimitri/pgcopydb · GitHub](https://github.com/dimitri/pgcopydb/issues/263); first-person-issue-or-forum-report; page-retrieved.


- **P042** — [Connector cannot handle uncompressed transaction payloads beyond 2GB · Issue #1503 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/1503); first-person-issue-or-forum-report; page-retrieved.


- **P043** — [airbyte/docs/integrations/sources/postgres.md at master · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/postgres.md?plain=1); official-technical-documentation; search-excerpt.


- **P044** — [Best practices for very large databases ?](https://www.reddit.com/r/PostgreSQL/comments/145y391); community-anecdote; search-excerpt.


- **P045** — [DB migrations at scale](https://www.reddit.com/r/PostgreSQL/comments/1grykyv); community-anecdote; search-excerpt.


- **P046** — [What is the most painful data migration project that you ever faced?](https://www.reddit.com/r/dataengineering/comments/1mwvz6p/what_is_the_most_painful_data_migration_project/); community-anecdote; page-retrieved.


- **P047** — [SQL Parsing and Data Lineage](https://www.reddit.com/r/dataengineering/comments/pmecrp); community-anecdote; search-excerpt.


- **P048** — [We took production down for 20 minutes because of a DB migration, how do you prevent this?](https://www.reddit.com/r/devops/comments/1swbj6e/we_took_production_down_for_20_minutes_because_of/); community-anecdote; search-excerpt.


- **P049** — [How do you handle schema migrations on a live production database without downtime?](https://www.reddit.com/r/SQL/comments/1whl5jc/how_do_you_handle_schema_migrations_on_a_live/); community-anecdote; search-excerpt.


- **P050** — [yugabyte-db/architecture/design/online-schema-migrations.md at master · yugabyte/yugabyte-db · GitHub](https://github.com/yugabyte/yugabyte-db/blob/master/architecture/design/online-schema-migrations.md); official-technical-documentation; search-excerpt.


- **P051** — [Pull requests · yugabyte/yb-voyager · GitHub](https://github.com/yugabyte/yb-voyager/pulls); vendor-or-project-publication; search-excerpt.


- **P052** — [Version specific detection for PG extension in assessment phase · Issue #2420 · yugabyte/yb-voyager · GitHub](https://github.com/yugabyte/yb-voyager/issues/2420); first-person-issue-or-forum-report; page-retrieved.


- **P053** — [Mask PII data using AWS DMS and Amazon Macie during migration | AWS Database Blog](https://aws.amazon.com/blogs/database/mask-pii-data-using-aws-dms-and-amazon-macie-during-migration/); vendor-or-project-publication; page-retrieved.


- **P054** — [Rollback removes changeset with target tag · Issue #6728 · liquibase/liquibase · GitHub](https://github.com/liquibase/liquibase/issues/6728); first-person-issue-or-forum-report; search-excerpt.


- **P055** — [DB is left in an inconsitent state after connection to database is lost · Issue #1498 · liquibase/liquibase · GitHub](https://github.com/liquibase/liquibase/issues/1498); first-person-issue-or-forum-report; page-retrieved.


- **P056** — [Secure 5.1: What are targeted rollbacks?](https://docs.liquibase.com/secure/user-guide-5-1/what-are-targeted-rollbacks); official-technical-documentation; page-retrieved.


- **P057** — [Drift analysis - Redgate Flyway - Product Documentation](https://documentation.red-gate.com/flyway/flyway-concepts/drift-analysis); official-technical-documentation; page-retrieved.


- **P058** — [Got Drift? Redgate Flyway now helps you resolve it quicker - Redgate Flyway - Product Documentation](https://documentation.red-gate.com/flyway/flyway-blog/got-drift-redgate-flyway-now-helps-you-resolve-it-quicker); official-technical-documentation; search-excerpt.


- **P059** — [Overcoming Pitfalls of Postgres Logical Decoding](https://blog.peerdb.io/overcoming-pitfalls-of-postgres-logical-decoding); vendor-or-project-publication; search-excerpt.


- **P060** — [abctl: silent migration failure from v0.14.1 -> v0.30.4 · Issue #80770 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/80770); first-person-issue-or-forum-report; page-retrieved.


- **P061** — [Issues · liquibase/liquibase · GitHub](https://github.com/liquibase/liquibase/issues); vendor-or-project-publication; search-excerpt.


- **P062** — [Community 5.0: What are rollback commands?](https://docs.liquibase.com/community/reference-guide-5-0/init-update-and-rollback-commands/what-are-rollback-commands); official-technical-documentation; page-retrieved.


- **P063** — [Flyway CLI does not seem to work with Postgres Aurora Limitless database · Issue #4022 · flyway/flyway](https://github.com/flyway/flyway/issues/4022); first-person-issue-or-forum-report; page-retrieved.


- **P064** — [Introducing Airbyte Destinations V2 - Typing & Deduping | Airbyte](https://airbyte.com/blog/introducing-airbyte-destinations-v2-typing-deduping); vendor-or-project-publication; search-excerpt.


- **P065** — [Pending incremental snapshot is silently discarded when a boot-time snapshot runs (snapshot.mode=always/configuration_based) · Issue #2325 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/2325); first-person-issue-or-forum-report; page-retrieved.


- **P066** — [Invalid incremental snapshot leave Connectors stuck; stop-snapshot ineffective and requires manual offset modification · Issue #1533 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/1533); first-person-issue-or-forum-report; page-retrieved.


- **P067** — [MongoDB conditional incremental snapshot state lost on interruption · Issue #1807 · debezium/dbz · GitHub](https://github.com/debezium/dbz/issues/1807); first-person-issue-or-forum-report; page-retrieved.


- **P068** — [[source-postgres] CDC sync loses backfilled values for newly added columns if the backfill occurs before schema refresh · Issue #80920 · airbytehq/airbyte · GitHub](https://github.com/airbytehq/airbyte/issues/80920); first-person-issue-or-forum-report; page-retrieved.


- **P069** — [Database Migration: post-migration schema/data validation question](https://www.reddit.com/r/SQL/comments/1qk44yi/database_migration/); community-anecdote; search-excerpt.
