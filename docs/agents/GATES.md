# Merge gates

Owner rule 2026-10-17: a backlog row becomes DONE only after an independent
review (the critic, a different model from the builder) recorded here with a
PASS verdict. `tools/check_merge_gate.py --staged` runs in the pre-commit hook
and stops a commit that sets a row DONE with no PASS line for it. A row that
fails review is set to REVIEW with the finding in its status cell; the lead
re-dispatches it with the finding and the row comes back here when it passes.

| row | date | verdict | reviewer | commit or branch | note |
|---|---|---|---|---|---|
| 616 | 2026-10-17 | PASS | critic-reviewer | master | 13/13 tests pass; keyless effect atomic checkpointer; pure unix |
| 617 | 2026-10-17 | PASS | critic-reviewer | master | 16/16 tests pass; schema epoch dependency planner; pure unix |
| 618 | 2026-10-17 | PASS | critic-reviewer | master | 19/19 tests pass; shadow resync coordinator; pure unix |
| 672 | 2026-10-17 | PASS | critic-reviewer | master | 15/15 tests pass; partial access scope honest coverage; pure unix |
| 673 | 2026-10-17 | PASS | critic-reviewer | master | 17/17 tests pass; adaptive bounds envelope validator; pure unix |
| 675 | 2026-10-17 | PASS | critic-reviewer | master | 25/25 tests pass; dynamic lineage evidence auditor; pure unix |
| 676 | 2026-10-17 | PASS | critic-reviewer | master | 12/12 tests pass; demand benchmark harness; pure unix |
| 619 | 2026-10-17 | PASS | critic-reviewer | master | 19/19 tests pass; snapshot watermark handoff mechanisms; pure unix |
| 620 | 2026-10-17 | PASS | critic-reviewer | master | 22/22 tests pass; preflight reversibility failback contract; pure unix |
| 621 | 2026-10-17 | PASS | critic-reviewer | master | 17/17 tests pass; cutover client db traffic state machine; pure unix |
| 622 | 2026-10-17 | PASS | critic-reviewer | master | 23/23 tests pass; conflict rules data semantics; pure unix |
| 623 | 2026-10-17 | PASS | critic-reviewer | master | 24/24 tests pass; replica snapshot valid source role; pure unix |
| 624 | 2026-10-17 | PASS | critic-reviewer | master | 11/11 tests pass; wal spool retention controller; pure unix |
| 625 | 2026-10-17 | PASS | critic-reviewer | master | 13/13 tests pass; composed mapping proof load and cdc; pure unix |
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
