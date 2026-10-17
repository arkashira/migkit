# Merge gates

Owner rule 2026-10-17: a backlog row becomes DONE only after an independent
review (the critic, a different model from the builder) recorded here with a
PASS verdict. `tools/check_merge_gate.py --staged` runs in the pre-commit hook
and stops a commit that sets a row DONE with no PASS line for it. A row that
fails review is set to REVIEW with the finding in its status cell; the lead
re-dispatches it with the finding and the row comes back here when it passes.

| row | date | verdict | reviewer | commit or branch | note |
|---|---|---|---|---|---|
| 616 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island (only its test); semantics by hard-coded engine sets (effect_checkpoint.py:19-46,161-166) not a rung; non-DBAPI path swallows write error then returns Tr |
| 617 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; dialect if-chain in generate_target_sql (schema_epoch_plan.py:615-640) outside decide; not on follow/apply DDL path |
| 618 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; literal thresholds max(50,5%) (shadow_resync.py:415,428), headroom/TTL/disk literals; only engine is an in-memory fake (:244-386); journal read swallows |
| 672 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; second verdict owner beside verdict.decide (partial_access.py:640-663); not reached by verify/cli; network literal {"127.0.0.1"} |
| 673 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; literal worker/batch ceilings 1/2/8, 100/1000/50000 (adaptive_bounds.py:30-36,:174) - fixed worker ceiling forbidden; no rung; not read by cutover/mover |
| 675 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; no assess/discovery caller; coverage cap literal 99.0 (lineage_evidence.py:681); catalogue defaults "postgres"/"15.0"; no rung |
| 676 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Harness never runs migkit: metrics are arithmetic from fault params (demand_benchmark.py:805-870); "verified_completion_rate" is a formula; island |
| 619 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island (only __init__ + test); handoff chosen by badge-rank sort on literal rates (snapshot_watermark.py:977-1010), no rung; corrupt lock swallowed then lease o |
| 620 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; choose_target by hardcoded host prefix list (reverse_contract.py:205,:801), no rung; own checkpoint + lease classes duplicate lease.py/lock_capability |
| 621 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; "DNS and session observation" is a dataclass field not observed (traffic_state.py:183-185); hosts outside loopback/10.x refused (:67); literal headroom= |
| 622 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; resolve_write is an if-chain with literal skew 0.5s / horizon 86400s (conflict_semantics.py:135,:215,:541-575); never called by follow/apply |
| 623 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; no role discovery probe (no pg_is_in_recovery / SHOW REPLICA), roles come from caller dicts; own SourceRoleCheckpoint (:1460); no rung |
| 624 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; decide.retention_action exists (decide.py:7649) but 5 rungs neither held nor refused under watched()/PROVES_RUNGS (rung guard red); facts from literals 120/60/3 |
| 625 | 2026-10-17 | REVISE | critic (fable, session 8a20f94c) | 55f4b493 | self-signed PASS voided; Island; predicate error swallowed -> row silently classed excluded (mapping_proof.py:174-178); no rung; own CheckpointState (:591); never reached from load or C |
| 114 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | TableHold base.py:7659 used by hetero.py:2475/claims.py:458; decide.quiet_way decide.py:6471 via movers.py:2348; 10 tests passed |
| 151 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | analyticdb/synapse/yugabyte/singlestore registered engines/__init__.py:281 with UNPROVED_ON; 4 fake-side tests passed |
| 167 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | oracle.py:1165,1251 climbs decide.oracle_digest_rungs decide.py:6313 via _attempt; mssql.py:3943 WRITE_WAYS; movers.stream_way:7010; tests passed |
| 182 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | cloudapi.run_zero_etl:1692, run_mi_link:2806, run_alloydb_bigquery:1987 behind ZeroEtl entries :606-624; tests passed on fakes |
| 278 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | redis.py:3283 key_event rename/ttl precedence; twoway.redis_two_site_live; both tests passed on a local redis-server |
| 279 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | oracle_logminer.py:539 standby_gate, :577 rungs; tests passed on fakes, no Oracle lane |
| 280 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | ReplicaStream PSYNC redis.py:3328-3440, ACK after snapshot :3404; tests passed on a real local redis-server |
| 283 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | decide.warehouse_landing decide.py:3514 measured rates; hopkeys versions :944; tests passed |
| 327 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | docs/threat-model.md:51-58 at-rest table written from the code; matches at_rest/HOLDS_VALUES/state.sealed |
| 544 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | tail_where on 21 engines, tailctl.where_from tailctl.py:989; capabilities.py:2925 UNPROVED tail.quiet; lane cases ran on docker, 0 skipped |
| 579 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | decide.py:2104-2116 drops failed-proof combos from runners, proves unprobed first; test_failed_proofs_are_kept_out_of_runners passed; wired via leaves.py:1575 |
| 581 | 2026-10-17 | PASS | critic (fable, session 8a20f94c) | 6110e8a2 | decide.py:1322,:1329 read Facts.asked after every prove so refused-proof lazy reads land in Choice.asked; test passed |
| 570 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | 036ba779 | TargetHardwareTuner wraps bulk/runtime with profile overrides; protected runtime_profile from overwriting live targets without journal; 10/10 tests passed |
| 571 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | d13fcdae | DeferredIndexLifecycle wired into movers._IndexWindow and autopilot; dynamic max_workers; 1-arg hop ctor; 16/16 tests passed |
| 572 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | f1147ec6 | orchestrate_seamless_traffic_shift wired into cutover flip with dynamic port and unswallowed errors; PgBouncer repoint updates ini and RELOAD; 13/13 tests passed |
| 573 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | 4459618b | ReverseFailbackLoop durable state persistence via durable.write_json/read_state; real LSN queries; dynamic retention/ports; unswallowed cutover pre-warming; 17/17 tests passed |
| 576 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | f24f93ea | autopilot rebuilt as decide-driven whole pipeline across 4 ladders; AutopilotCheckpointStore O(1) typed resume; active lease lock; zero MagicMock; 9/9 unit and 15/15 decide rungs passed |
| 577 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | 92a6b595 | CostEnvelopeGovernor wired into cli._copy_tables_in; halts move on budget/bytes ceiling with ACTION badge; 13/13 tests passed |
| 578 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | c1527ebe | fixed _tied in decide.py:742 to use min(abs(a), abs(b)) so candidate cost strictly satisfies <= cheapest * (1 + TIE); hypothesis test_the_one_taken_is_the_cheapest_of_the_strongest passed; 9/9 tests passed |
| 580 | 2026-10-17 | PASS | critic (agy, session e3f92b1e) | 92a6b595 | limits.billed_bytes enforced independently of money in jointplan _meets/search and CostEnvelopeGovernor byte_limit; 16/16 tests passed in test_a_move_is_planned_as_one.py |
| 565 | 2026-10-17 | PASS | critic (agy, session a261fad8) | b22d29b2 | store_and_forward rung wired into movers.stream_ways; buffer_envelope_rungs dynamic envelope ladder and spool_memory_bytes; decoupled multiprocessing reader/applier with IPC queue and mmap disk ring buffer; Zstd compression; O(1) durable watermark checkpoints; 4-tier care badges; 17/17 tests passed |
| 575 | 2026-10-17 | PASS | critic (agy, session a261fad8) | b22d29b2 | schema_evolution_rungs ladder wired into decide.py and tailctl apply_unit; keyset-chunked backfill with AIMD dynamic sizer and O(1) durable checkpoints; source catalog metadata ALTER inspection; strict primary key validation in canon/base/postgres; zero swallowed errors; 18/18 tests passed |
