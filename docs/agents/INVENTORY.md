# migkit source inventory

Written by `tools/gen_codemap.py` from the code; the pre-commit hook stops a commit that leaves it behind. Every file under `migkit/` and `tools/` with the responsibility package a table in the generator gives it (`_BY_NAME`, `_BY_PREFIX`), the owner domain where it holds one of the one-place readings (`OWNERS`), its size, the modules that import it and the packages it reaches (module-level and inside functions alike), the wrapped programs and libraries it registers or is read for, and the tests that reach it. The layering rule these packages are held to is [LAYERS.md](LAYERS.md); where things are by module is [CODEMAP.md](CODEMAP.md).

## Unassigned

- `migkit/convert.py`

## Packages

| package | rank | files | lines | responsibility |
|---|---|---|---|---|
| configuration | 1 | 11 | 10522 | what a hop says: its keys, the environment, the endpoints and the addresses it is built from |
| scope | 2 | 5 | 3151 | which objects and rows a hop carries |
| persistence | 3 | 18 | 9196 | state that outlives a process: checkpoints, the audit record, the run's own files, the undo |
| safety | 4 | 12 | 12204 | who may write, what must not leak, what keeps a mover from writing twice |
| execution | 5 | 17 | 11093 | the running of work: processes, ranges, retries, pacing, stalls, sizing of what runs at once |
| conversion | 6 | 69 | 47978 | a value or an object as another engine takes it: canonical text, transforms, types, stored code |
| reporting | 7 | 14 | 10832 | what the operator reads: the verdict, the result shape, the log, the metrics, the notices |
| decision | 8 | 36 | 36979 | choosing: the rungs, the cost model, the joint plan, what each engine can do |
| engines | 9 | 80 | 128670 | the contract every engine fills and each engine's own adapter |
| integrations | 9 | 23 | 24368 | the programs, libraries and provider APIs migkit wraps, and the other tools it meets |
| workflows | 10 | 111 | 149804 | a move's phases and what they do around the move: copy, follow, prove, repair, cut over, recover |
| cli | 11 | 4 | 10464 | the command line and the guide that asks for two addresses |
| web | 11 | 30 | 6451 | the web view and its API |
| tools | - | 13 | 7195 | the generators, the full suite and the checks run by the maintainers; they import migkit and migkit never imports them |
| data | - | 5 | 10810 | the files migkit ships as resources |

## configuration (11 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/__init__.py` | - | 110 | 53: avrostream, barriers, binlog_marks, capabilities, cli (+48) | - | - | 866: conftest.py, holdkit.py, large_values_rig.py (+863) |
| `migkit/addresses.py` | - | 529 | 10: config, diagnostics, engineat, guide, hopkeys (+5) | configuration 2, engines 4 | - | 5: a_hop_file_is_checked_whole_at_load, a_hop_from_two_addresses, copybook_files_as_a_side_of_a_pair (+2) |
| `migkit/clouds.py` | - | 337 | 13: case, cli, cloudseed, engines.kafka, engines.mysql (+8) | configuration 3 | - | 11: a_hop_from_two_addresses, a_managed_mysql_says_how_long_its_log_is_kept, a_managed_side_is_read_and_held_by_what_its_admin_may_do (+8) |
| `migkit/config.py` | scope: Hop.excluded, Hop.carries, Hop.target_table_in | 2478 | 61: approvals, backupseed, carriers, changetopic, cli (+56) | configuration 14, conversion 1, engines 1, execution 13, integrations 4, persistence 3, reporting 1, safety 4 | - | 840: bigquery_fake.py, cosmos_fake.py, dynamodb_on_moto.py (+837) |
| `migkit/engineat.py` | - | 648 | 2: estate, guide | configuration 5, decision 3, engines 6 | - | 3: init_reads_the_engine_off_the_server, the_engine_finding_rungs_hold_and_refuse, the_estate_is_discovered_sized_and_priced |
| `migkit/envkeys.py` | environment variables | 521 | 52: approvals, assist, audit, bulkways, cli (+47) | - | - | 3: every_env_var_is_declared, mysql_grants_from_the_tables, the_sweeps_last_leftovers_decide_on_facts |
| `migkit/exit_portability.py` | - | 749 | 0 | persistence 2, scope 2 | - | 1: exit_portability_checkpoints |
| `migkit/hopkeys.py` | hop keys: HOP_KEYS, HOP_OPTIONS | 3397 | 24: addresses, case, charsets, cli, config (+19) | configuration 21, conversion 3, decision 5, engines 10, integrations 1, safety 3, scope 1, web 2, workflows 13 | - | 43: a_hop_file_is_checked_whole_at_load, a_hop_from_a_dts_job, a_hop_from_another_clouds_job (+40) |
| `migkit/init.py` | - | 3 | 0 | cli 1 | - | 1: every_module_is_reached |
| `migkit/managed.py` | - | 1132 | 13: capabilities, cloud_readiness, clouds, cutover, dba (+8) | cli 2, configuration 1, decision 18, engines 2, integrations 3, persistence 1, safety 4, workflows 1 | - | 15: a_managed_brand_is_told_by_what_its_server_says, a_managed_side_is_read_and_held_by_what_its_admin_may_do, a_managed_source_is_held_on_a_server_like_its_service (+12) |
| `migkit/variants.py` | - | 618 | 6: engineat, engines.base, engines.kafka, engines.mongodb, engines.redis (+1) | configuration 2, decision 2, integrations 1, safety 1 | - | 13: a_cutover_proof_on_postgres_reads_every_table, a_managed_brand_is_told_by_what_its_server_says, a_redis_snapshot_valkey_refuses_is_said_before_a_move (+10) |

## scope (5 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/case.py` | - | 594 | 2: cli, ready | configuration 2, decision 1, safety 2, workflows 15 | - | 6: a_fork_change_is_held_first_mysql_mariadb, a_fork_change_is_held_first_redis_valkey, a_version_down_is_held_first_pg (+3) |
| `migkit/partial_access.py` | - | 663 | 0 | - | - | 1: partial_access_honest_coverage |
| `migkit/result_scope.py` | - | 704 | 3: cli, report, verdict | reporting 1, safety 2 | - | 3: cdc_watermark_inflight_fencing, operator_readability_triage, result_scope_presentation |
| `migkit/resume_scope.py` | - | 627 | 2: cutover, exit_portability | decision 1, persistence 2, scope 1 | - | 2: exit_portability_checkpoints, resume_scope_identity |
| `migkit/rowfilter.py` | row filter | 563 | 6: engines.base, engines.hetero, engines.mongodb, hopkeys, resume_scope (+1) | execution 1 | - | 1: one_row_filter_reads_alike_everywhere |

## persistence (18 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/anchor.py` | - | 719 | 5: cli, export, session, telemetry, tooluse | decision 1, integrations 2, persistence 6, reporting 3, safety 4 | boto3-s3-anchor | 3: a_run_writes_every_step_back_as_it_ends, telemetry_carries_no_customer_data, the_audit_record_is_anchored_in_a_locked_bucket |
| `migkit/audit.py` | - | 247 | 20: anchor, approvals, cli, cutover, drainmark (+15) | configuration 1, persistence 2 | - | 8: a_run_keeps_its_own_log, a_step_waits_for_its_approvers, nothing_leaves_where_nothing_may (+5) |
| `migkit/checkpoint.py` | - | 616 | 26: application_readiness, autopilot, bulkways, cli, complete_path (+21) | decision 9, execution 1, persistence 6 | - | 12: a_begun_range_is_found_again_after_a_reload_whatever_its_key, a_postgres_check_salts_each_run, adaptive_aimd_batch_sizing (+9) |
| `migkit/claims.py` | - | 720 | 5: cli, engines.hetero, fleet, ha, reshard | decision 4, execution 4, persistence 1, safety 4, workflows 7 | - | 7: a_standby_mover_takes_over_and_fences, every_sandbox_target_holds_the_fencing_number_its_own_way, mysql_mssql_and_claims_share_the_hold (+4) |
| `migkit/control_upgrade.py` | - | 682 | 0 | persistence 2 | - | 1: control_plane_upgrade_resume |
| `migkit/digests.py` | - | 277 | 9: engines.hetero, engines.mysql, engines.postgres, engines.sqlite, export (+4) | conversion 3, execution 1, persistence 5 | - | 5: a_run_keeps_its_own_log, a_seed_is_shipped_on_media, every_answer_and_position_keeps_its_scope (+2) |
| `migkit/durable.py` | - | 357 | 117: ack_lattice, agent_control, ai_proof_loop, anchor, apply_contract (+112) | persistence 4 | - | 21: a_copier_reconciles_and_retunes, a_mysql_cutover_can_be_rolled_back, change_windows_name_when_a_tail_diverges (+18) |
| `migkit/effect_checkpoint.py` | - | 677 | 1: tailctl | configuration 1, decision 6 | - | 1: keyless_effect_atomic_checkpoint |
| `migkit/evidence.py` | - | 404 | 11: config, durable, engines.couchbase, engines.postgres, export (+6) | configuration 1, persistence 2 | pyrage | 9: a_programs_files_go_where_at_rest_says, a_seed_is_shipped_on_media, postgres_proves_json_on_the_server (+6) |
| `migkit/kept.py` | - | 1009 | 4: engines.hetero, engines.mysql, engines.postgres, ha | conversion 3, engines 1, persistence 1, workflows 5 | - | 3: the_proof_is_kept_as_the_data_moves, the_proof_is_kept_on_servers, the_tail_pays_once_what_it_can |
| `migkit/repairlog.py` | - | 567 | 9: cli, engines.base, engines.generic, engines.mongodb, engines.mssql (+4) | conversion 5, persistence 2 | - | 11: a_batch_that_is_not_a_page_is_the_decisions, a_repair_is_recorded_statement_by_statement, cassandra_rows_are_repaired_and_undone (+8) |
| `migkit/revert.py` | - | 123 | 2: engines.mysql, engines.postgres | execution 1 | - | 2: a_check_does_not_write_to_the_target, revert |
| `migkit/runlog.py` | - | 463 | 8: cli, diagnostics, digests, egress, export (+3) | execution 1, persistence 4, safety 1 | - | 8: a_run_keeps_its_own_log, every_table_digest_takes_the_chosen_hash, mysql_digests_json_on_the_server (+5) |
| `migkit/session.py` | - | 239 | 11: audit, cli, decide, digests, egress (+6) | configuration 2, decision 1, persistence 3, reporting 6 | - | 9: a_run_keeps_its_own_log, migkit_works_inside_the_teams_tools, nothing_leaves_where_nothing_may (+6) |
| `migkit/setaside.py` | - | 144 | 9: cutover, engines.cassandra, engines.mongodb, engines.mssql, engines.opensearch (+4) | persistence 1, safety 2, workflows 2 | - | 3: a_cutover_proof_on_postgres_reads_every_table, a_load_holds_the_targets_ttl_indexes_from_deleting_what_it_writes, every_tail_goes_round_one_loop |
| `migkit/shared.py` | - | 546 | 6: claims, cli, ha, reshard, state (+1) | configuration 1, decision 3, engines 3, integrations 1, persistence 6 | boto3-s3-conditional | 6: a_standby_takes_the_move_over_where_it_stood, one_table_is_copied_across_machines, one_writer_at_a_time_is_proved_from_what_was_kept (+3) |
| `migkit/snapshot_state.py` | - | 728 | 1: cutover | decision 1, persistence 1 | - | 1: snapshot_subworkflow_state |
| `migkit/state.py` | - | 678 | 10: cli, cutover, engines.hetero, export, interlock (+5) | configuration 2, integrations 1, persistence 14, workflows 5 | boto3-s3-conditional | 13: a_hop_file_is_checked_whole_at_load, a_lease_carries_a_number_the_next_holder_goes_above, a_mysql_cutover_can_be_rolled_back (+10) |

## safety (12 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/approvals.py` | - | 424 | 6: archive, cli, ship, sourcechange, storedcode.convert (+1) | configuration 3, persistence 1, reporting 2, web 2, workflows 2 | - | 9: a_repair_never_writes_what_the_hop_leaves_out, a_source_change_is_offered_never_assumed, a_step_waits_for_its_approvers (+6) |
| `migkit/egress.py` | - | 252 | 15: anchor, assist, cloud_readiness, cloudapi, engines.kafka (+10) | configuration 1, persistence 2, reporting 2, safety 1 | - | 7: a_managed_brand_is_told_by_what_its_server_says, every_provider_call_goes_through_the_egress_gate, nothing_leaves_where_nothing_may (+4) |
| `migkit/fencing.py` | - | 910 | 18: capabilities, claims, crossmove, engines.base, engines.cassandra (+13) | configuration 1, decision 1 | - | 5: a_standby_mover_takes_over_and_fences, a_writer_that_lost_its_hold_is_refused_by_the_target, cockroachdb_and_dsql_carry_counters_and_say_what_a_move_will_meet (+2) |
| `migkit/freeze.py` | - | 6958 | 15: claims, cli, cutover, drainmark, engines.base (+10) | configuration 10, decision 59, engines 37, integrations 1, persistence 6, workflows 10 | - | 42: holdkit.py, a_cassandra_source_is_held_and_given_back, a_cutover_proof_on_postgres_reads_every_table (+39) |
| `migkit/interlock.py` | - | 566 | 13: case, cli, cutover, drainmark, engines.mongodb (+8) | configuration 1, persistence 4, reporting 1, safety 1 | - | 11: a_managed_side_is_read_and_held_by_what_its_admin_may_do, a_slow_oracle_reader_stops_the_start, cassandra_cell_times_are_checked (+8) |
| `migkit/lease.py` | - | 458 | 27: agent_control, application_readiness, apply_contract, cancel_recovery, claims (+22) | configuration 1, persistence 2, workflows 1 | - | 14: conftest.py, a_check_holds_a_lease, a_hops_write_lease (+11) |
| `migkit/leftovers.py` | - | 582 | 10: cli, config, drift, engines.mongodb, engines.mysql (+5) | configuration 1, decision 2, persistence 3, safety 2 | pg_dump | 9: holdkit.py, a_programs_files_go_where_at_rest_says, a_providers_sync_back_table_is_not_your_data (+6) |
| `migkit/lock_capability.py` | - | 775 | 0 | - | - | 1: lock_capability_probes |
| `migkit/masking.py` | - | 318 | 14: assist, charsets, cli, engines.base, export (+9) | configuration 4, persistence 2 | - | 4: a_shown_hash_is_keyed, the_drilldown_can_be_masked, the_second_reading_is_used_whole (+1) |
| `migkit/ownership.py` | - | 78 | 6: engines.clickhouse, engines.mssql, engines.mysql, engines.oracle_deep, engines.postgres (+1) | - | - | 1: ownership |
| `migkit/redact.py` | - | 218 | 12: anchor, diagnostics, egress, engines.postgres, export (+7) | configuration 1 | - | 3: a_masked_column_is_proved_as_written, one_redactor_hides_every_secret, the_clouds_own_sign_in_is_minted_and_refreshed |
| `migkit/tls.py` | - | 665 | 4: capabilities, cli, config, engines.base | - | - | 12: a_connection_is_verified_where_it_can_be, a_leg_says_what_it_verified, a_mysql_server_is_asked_for_its_certificate (+9) |

## execution (17 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/ack_lattice.py` | - | 445 | 1: cutover | decision 1, persistence 1 | - | 1: ack_position_lattice |
| `migkit/attempts.py` | - | 1024 | 14: agent_control, autonomous, claims, cli, engines.base (+9) | decision 3, engines 2, execution 4, persistence 3, workflows 2 | - | 16: a_copier_reconciles_and_retunes, a_failure_is_told_by_what_it_did, a_lost_answer_closes_only_its_session (+13) |
| `migkit/cancel_recovery.py` | - | 613 | 1: cutover | decision 1, safety 1 | - | 1: cancel_recovery_transaction |
| `migkit/decomp_budget.py` | - | 707 | 0 | - | - | 1: decompression_budget_spill |
| `migkit/failpoint.py` | - | 60 | 16: claims, cli, crossmove, cutover, engines.cassandra (+11) | configuration 1, decision 1 | - | 22: a_copier_reconciles_and_retunes, a_cross_engine_copy_goes_server_to_server, a_cutover_without_a_position_drains_by_a_mark (+19) |
| `migkit/keepalive.py` | - | 130 | 3: config, engines.mysql, engines.postgres | - | - | 1: ssm_tunnel_keepalive_armor |
| `migkit/locks.py` | - | 1124 | 2: engines.postgres, revert | - | - | 5: constraint_repair_pg, lock_impact_budget, locks (+2) |
| `migkit/process_memory.py` | - | 553 | 0 | - | - | 1: process_memory_budget_governor |
| `migkit/ranges.py` | ranges | 1958 | 16: checkpoint, claims, cli, crossmove, engines.base (+11) | conversion 1, decision 5, execution 5, safety 1, workflows 4 | - | 42: a_begun_range_is_found_again_after_a_reload_whatever_its_key, a_cross_engine_copy_goes_server_to_server, a_fault_in_each_copier_is_recovered_on_a_server (+39) |
| `migkit/runner.py` | - | 943 | 14: cli, dsbulk, engines.my_pass, fromsnapshot, mirror (+9) | decision 1, execution 5, integrations 4, persistence 2, reporting 2, safety 1 | - | 13: a_fallback_says_why_and_a_refusal_stops, a_stopped_command_ends_inside_its_bound, mysql_tls_on_every_path (+10) |
| `migkit/runners/__init__.py` | - | 0 | 0 | - | - | 1: every_module_is_reached |
| `migkit/runners/second_reader.py` | - | 149 | 0 | configuration 1 | dvt | 1: every_module_is_reached |
| `migkit/schedule.py` | - | 323 | 7: cli, engines.hetero, integrations, movers, tailctl (+2) | persistence 1, reporting 3, safety 2 | - | 7: a_follow_ends_when_caught_up_or_asked, a_hop_runs_on_its_schedule, a_schedule_holds_its_slot_by_lease (+4) |
| `migkit/stall.py` | - | 99 | 8: engines.my_pass, engines.mysql, engines.pg_pass, engines.postgres, legs (+3) | configuration 1 | - | 2: one_runner_for_every_program, the_wrapped_programs_run_one_way |
| `migkit/throttle.py` | - | 305 | 10: cli, engines.generic, engines.hetero, engines.kafka, engines.mongodb (+5) | decision 2 | - | 8: a_native_replica_is_proved_two_ways, calibrated_eta, generic_reladiff (+5) |
| `migkit/tunnel.py` | - | 1203 | 7: autopilot, engines, engines.postgres, legs, model (+2) | decision 1, safety 1 | - | 5: a_server_behind_a_bastion_is_reached, built_in_auto_bastion_tunnels, the_cloud_native_services_as_sides (+2) |
| `migkit/util.py` | - | 1457 | 47: attempts, cli, config, cutover, digests (+42) | configuration 1, decision 6, engines 2, execution 1, persistence 5, reporting 1, safety 2 | reladiff | 36: a_failure_is_told_by_what_it_did, a_lost_answer_closes_only_its_session, a_mysql_server_is_asked_for_its_certificate (+33) |

## conversion (69 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/bsontype.py` | - | 341 | 2: canon, engines.base | conversion 10 | - | 1: type_fidelity_classes |
| `migkit/canon.py` | - | 4814 | 93: applyway, archive, avrostream, bsontype, changetopic (+88) | conversion 46, decision 1, engines 2 | - | 103: a_bigquery_pair_follows_the_hops_mapping, a_column_renders_as_canon_renders, a_cross_engine_copy_goes_server_to_server (+100) |
| `migkit/canon_oracle.py` | - | 755 | 1: canon | - | - | 1: canonical_oracle |
| `migkit/charsets.py` | - | 1873 | 4: cli, engines.base, hopkeys, tableengine | configuration 1, conversion 10, decision 3, engines 5, safety 1, workflows 2 | - | 2: a_character_set_change_is_planned_and_proved, the_move_way_rungs_hold_and_refuse |
| `migkit/commercial.py` | - | 639 | 5: canon, engines.base, engines.hetero, engines.mssql, engines.oracle | conversion 9, engines 1 | - | 3: a_column_renders_as_canon_renders, type_fidelity_classes, types_only_commercial_engines_have |
| `migkit/copybook.py` | - | 1617 | 2: engines.copybook, engines.db2i | - | - | 2: copybook_files_move_into_postgresql, copybook_records_read_as_ibm_documents |
| `migkit/ddl.py` | - | 148 | 1: engines.base | engines 1 | - | 1: ddl_that_cannot_be_applied |
| `migkit/ddl_shield.py` | - | 1144 | 2: schema_drift, schema_epoch_plan | conversion 2 | - | 4: ddl_shield_and_query_guard_wiring, online_ddl_ast_mutation_shield, schema_drift_auto_resolution (+1) |
| `migkit/described.py` | - | 102 | 2: engines.bigtable, engines.firestore | configuration 1, conversion 2, persistence 3, workflows 1 | - | 2: large_values_on_google_and_aws_native_services, the_cloud_native_services_as_sides |
| `migkit/geo.py` | - | 397 | 6: bsontype, canon, commercial, engines.mssql, engines.oracle (+1) | - | - | 3: sql_server_shapes_are_read_as_their_binary, type_fidelity_classes, types_only_commercial_engines_have |
| `migkit/grants.py` | - | 612 | 0 | - | - | 1: users_grants_parity_orchestrator |
| `migkit/identity_semantics.py` | - | 359 | 0 | - | - | 1: identity_semantics_collation |
| `migkit/intervals.py` | - | 174 | 8: canon, engines.duckdb, engines.keyspaces, engines.oracle, engines.pgwire (+3) | - | - | 4: duckdb_intervals_and_vectors_are_read_whole, oracle_redo_is_read_back_exactly, teradata_as_a_side_of_a_pair (+1) |
| `migkit/lineage_evidence.py` | - | 696 | 0 | - | - | 1: lineage_evidence_dynamic_sql |
| `migkit/loadtext.py` | - | 282 | 1: crossmove | conversion 2 | - | 1: a_cross_engine_table_goes_the_way_measured_fastest |
| `migkit/merkle.py` | - | 821 | 1: ddl_shield | - | - | 2: ddl_shield_and_query_guard_wiring, in_database_merkle_tree_folding |
| `migkit/nanotime.py` | - | 110 | 17: bsontype, changetopic, commercial, engines.clickhouse, engines.db2 (+12) | - | - | 7: a_column_renders_as_canon_renders, oracle_is_a_whole_side, oracle_redo_is_read_back_exactly (+4) |
| `migkit/partial_images.py` | - | 549 | 0 | workflows 1 | - | 1: partial_images_toast |
| `migkit/pieces.py` | - | 961 | 37: canon, decide, engines.analyticdb, engines.ase, engines.base (+32) | conversion 4, decision 2, engines 3 | - | 18: large_values_rig.py, a_batch_that_is_not_a_page_is_the_decisions, a_cosmosdb_deep_check_reads_the_items_and_the_container_properties (+15) |
| `migkit/private_proof.py` | - | 693 | 0 | - | - | 1: private_proof_masked_data |
| `migkit/render.py` | - | 104 | 5: canon, engines.saas, kept, keyless_proof, leaves | conversion 3 | - | 2: a_column_renders_as_canon_renders, wide_values_are_hashed_where_they_lie |
| `migkit/rowtext.py` | - | 213 | 13: canon, digests, engines.mysql, engines.opensearch, engines.oracle (+8) | conversion 2 | - | 16: a_column_renders_as_canon_renders, charset_encoding_mysql, every_ad_hoc_switch_goes_through_the_decision (+13) |
| `migkit/schema.py` | - | 33 | 1: autopilot | workflows 14 | - | 1: deferred_index_and_non_blocking_fk |
| `migkit/schema_drift.py` | - | 1452 | 4: connector_capability, decide, schema_epoch_plan, shard_ddl | conversion 6, decision 3, persistence 1 | - | 1: schema_drift_auto_resolution |
| `migkit/schema_epoch_plan.py` | - | 1079 | 0 | conversion 11, decision 3 | - | 1: schema_epoch_fenced_plan |
| `migkit/schemaformats.py` | - | 1108 | 3: engines.kafka, streamout, verifiers | configuration 1, conversion 2, engines 1, execution 1, integrations 2, workflows 2 | - | 4: nosql_engines_compare_schemas_one_way, probes_accumulate_and_schemas_compare_one_way, registry_messages_in_json_schema_and_protobuf (+1) |
| `migkit/storedcode/__init__.py` | - | 8 | 0 | - | - | 17: collations_agree_on_the_columns_own_values, constraints_accept_and_refuse_the_same_rows, conversion_breadth_mysql_to_postgres (+14) |
| `migkit/storedcode/appscan.py` | - | 507 | 3: storedcode.builtins, storedcode.prove, tooluse | conversion 4, integrations 1 | ast-grep-py | 1: the_applications_sql_is_found_and_held_to_the_source |
| `migkit/storedcode/bench.py` | - | 37 | 1: storedcode.prove | conversion 1 | - | 2: collations_agree_on_the_columns_own_values, constraints_accept_and_refuse_the_same_rows |
| `migkit/storedcode/builtins.py` | - | 472 | 3: storedcode.convert, storedcode.prove, storedcode.statements | conversion 15, engines 1 | - | 1: where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/collations.py` | - | 1454 | 1: storedcode.prove | decision 1, engines 2, safety 1, scope 1 | - | 2: collations_agree_on_the_columns_own_values, the_stored_code_rungs_hold_and_refuse |
| `migkit/storedcode/concolic.py` | - | 175 | 1: storedcode.prove | conversion 3 | - | 1: stored_code_candidates_are_ranked_by_what_proved |
| `migkit/storedcode/constraints.py` | - | 2513 | 3: storedcode.prove, storedcode.scalar, storedcode.sizing | conversion 4, engines 4 | - | 1: constraints_accept_and_refuse_the_same_rows |
| `migkit/storedcode/convert.py` | - | 1149 | 6: convert, engines.hetero, storedcode.prove, storedcode.rungs, storedcode.sandbox (+1) | configuration 1, conversion 45, engines 2, integrations 1, persistence 2, safety 1, workflows 5 | - | 3: oracle_code_runs_in_mariadbs_oracle_mode, stored_code_fixes_drafted_through_proof, where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/corpus.py` | - | 324 | 2: storedcode.prove, workload | conversion 8, engines 3, safety 1 | - | 1: the_applications_sql_is_found_and_held_to_the_source |
| `migkit/storedcode/effort.py` | - | 160 | 3: cli, storedcode.convert, storedcode.programs | - | - | 1: the_proof_reads_history_clock_and_cuts |
| `migkit/storedcode/harness.py` | - | 1789 | 13: storedcode.bench, storedcode.builtins, storedcode.constraints, storedcode.corpus, storedcode.history (+8) | conversion 3 | - | 5: constraints_accept_and_refuse_the_same_rows, stored_code_candidates_are_ranked_by_what_proved, the_applications_sql_is_found_and_held_to_the_source (+2) |
| `migkit/storedcode/history.py` | - | 436 | 2: storedcode.corpus, storedcode.prove | conversion 1, engines 1, safety 1 | - | 1: the_proof_reads_history_clock_and_cuts |
| `migkit/storedcode/indexadvice.py` | - | 272 | 3: storedcode.partitions, tooluse, workload | conversion 2, integrations 1 | hypopg | 1: where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/inputs.py` | - | 271 | 5: storedcode.prove, storedcode.reference, storedcode.scalar, storedcode.statements, storedcode.trace | conversion 1, persistence 1 | - | 1: stored_code_candidates_are_ranked_by_what_proved |
| `migkit/storedcode/keep.py` | - | 85 | 2: storedcode.prove, storedcode.reference | conversion 2 | - | through storedcode.prove, storedcode.reference |
| `migkit/storedcode/learn.py` | - | 83 | 1: storedcode.convert | persistence 1 | - | 1: stored_code_candidates_are_ranked_by_what_proved |
| `migkit/storedcode/model.py` | - | 109 | 2: storedcode.convert, storedcode.rungs | conversion 3, integrations 3, safety 2 | - | 1: nothing_leaves_where_nothing_may |
| `migkit/storedcode/mutants.py` | - | 131 | 1: storedcode.convert | - | - | 2: stored_code_candidates_are_ranked_by_what_proved, stored_code_is_proved_by_running_it |
| `migkit/storedcode/mysql_pg.py` | - | 1915 | 9: storedcode.concolic, storedcode.convert, storedcode.harness, storedcode.keep, storedcode.pg_mysql (+4) | conversion 10 | - | 6: migkits_own_rules_convert_procedural_code, postgresql_code_converted_to_mysql_by_rules, stored_code_candidates_are_ranked_by_what_proved (+3) |
| `migkit/storedcode/packages.py` | - | 143 | 1: storedcode.prove | conversion 4, engines 2 | - | 1: plsql_rules_carry_packages |
| `migkit/storedcode/partitions.py` | - | 325 | 1: storedcode.prove | conversion 4, engines 2 | - | 2: conversion_breadth_mysql_to_postgres, where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/pg_mysql.py` | - | 3863 | 3: storedcode.convert, storedcode.reduce, storedcode.trace | conversion 8 | - | 1: postgresql_code_converted_to_mysql_by_rules |
| `migkit/storedcode/plpgsql.py` | - | 125 | 9: storedcode.convert, storedcode.model, storedcode.mysql_pg, storedcode.pg_mysql, storedcode.plsql_pg (+4) | - | - | 2: plsql_rules_carry_packages, tsql_rules_at_depth |
| `migkit/storedcode/plsql_pg.py` | - | 1905 | 2: storedcode.convert, storedcode.packages | conversion 9 | - | 1: plsql_rules_carry_packages |
| `migkit/storedcode/programs.py` | - | 252 | 5: export, storedcode.convert, storedcode.model, storedcode.rungs, tooluse | conversion 3, integrations 4 | ora2pg | 1: stored_code_candidates_are_ranked_by_what_proved |
| `migkit/storedcode/prove.py` | - | 1757 | 6: engines.hetero, storedcode.convert, storedcode.reduce, storedcode.reference, storedcode.sequences (+1) | conversion 49, engines 3 | - | 8: collations_agree_on_the_columns_own_values, constraints_accept_and_refuse_the_same_rows, conversion_breadth_mysql_to_postgres (+5) |
| `migkit/storedcode/reduce.py` | - | 266 | 2: storedcode.convert, storedcode.trace | conversion 8 | - | 2: the_proof_reads_history_clock_and_cuts, where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/reference.py` | - | 198 | 1: storedcode.prove | conversion 5, engines 1, safety 1, workflows 1 | - | 2: conversion_breadth_mysql_to_postgres, where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/routines.py` | - | 64 | 15: engines.bigquery_checks, engines.cockroachdb, engines.mongodb, engines.mssql, engines.mysql (+10) | - | - | 9: migkits_own_rules_convert_procedural_code, plsql_rules_carry_packages, postgresql_code_converted_to_mysql_by_rules (+6) |
| `migkit/storedcode/rungs.py` | - | 209 | 2: storedcode.convert, tools.gen_docs | conversion 8, decision 1, persistence 1 | - | 4: stored_code_candidates_are_ranked_by_what_proved, stored_code_fixes_drafted_through_proof, the_stored_code_rungs_hold_and_refuse (+1) |
| `migkit/storedcode/sandbox.py` | - | 849 | 3: cli, storedcode.convert, storedcode.prove | configuration 3, conversion 2, engines 4, execution 1 | - | 12: collations_agree_on_the_columns_own_values, constraints_accept_and_refuse_the_same_rows, doctor_says_the_stored_code_check_rung (+9) |
| `migkit/storedcode/scalar.py` | - | 131 | 1: storedcode.prove | conversion 5 | - | 1: where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/sequences.py` | - | 253 | 2: storedcode.convert, storedcode.prove | conversion 6 | - | through storedcode.convert, storedcode.prove |
| `migkit/storedcode/sizing.py` | - | 276 | 2: engines.hetero, storedcode.prove | conversion 3, engines 1 | - | 1: the_proof_reads_history_clock_and_cuts |
| `migkit/storedcode/statements.py` | - | 942 | 5: storedcode.appscan, storedcode.builtins, storedcode.corpus, storedcode.prove, workload | conversion 7, decision 1, engines 1, workflows 1 | - | 1: the_applications_sql_is_found_and_held_to_the_source |
| `migkit/storedcode/trace.py` | - | 495 | 1: storedcode.prove | conversion 13 | - | 1: where_two_versions_part_and_what_holds_for_every_input |
| `migkit/storedcode/translate.py` | - | 150 | 20: storedcode.builtins, storedcode.concolic, storedcode.constraints, storedcode.convert, storedcode.indexadvice (+15) | - | - | 9: migkits_own_rules_convert_procedural_code, nothing_leaves_where_nothing_may, plsql_rules_carry_packages (+6) |
| `migkit/storedcode/tsql_pg.py` | - | 1617 | 5: engines.mssql, storedcode.convert, storedcode.prove, storedcode.reduce, storedcode.trace | conversion 8 | - | 2: migkits_own_rules_convert_procedural_code, tsql_rules_at_depth |
| `migkit/storedcode/views.py` | - | 50 | 1: storedcode.prove | conversion 2 | - | 1: a_postgresql_source_carries_its_views |
| `migkit/tally.py` | - | 128 | 3: engines.mysql, engines.postgres, model | - | - | 4: a_digest_is_not_a_security_hash, a_mysql_copy_passes_rows_server_to_server, a_same_engine_copy_passes_rows_server_to_server (+1) |
| `migkit/transforms.py` | column transform | 785 | 6: config, decide, engines.generic, engines.hetero, hopkeys (+1) | configuration 2, decision 2, persistence 1, safety 1 | - | 4: a_masked_column_is_proved_as_written, a_transform_in_flight_is_verified_as_transformed, the_mysql_bulk_rungs_run (+1) |
| `migkit/vectors.py` | - | 138 | 2: bsontype, canon | - | - | 1: type_fidelity_classes |
| `migkit/zones.py` | - | 91 | 2: engines.mysql, engines.postgres | engines 1 | - | 1: the_time_zone_the_data_is_written_in |

## reporting (14 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/demand_benchmark.py` | - | 965 | 0 | - | - | 1: demand_benchmark_fault_scenarios |
| `migkit/diagnostics.py` | probes: ProbeAccumulator | 437 | 7: cli, cutover, engines.mssql, engines.mysql, engines.postgres (+2) | configuration 3, decision 1, persistence 4, safety 3 | - | 8: a_problem_can_be_reported_without_handing_anything_over, engine_probes_reach_the_accumulator, one_redactor_hides_every_secret (+5) |
| `migkit/export.py` | - | 1272 | 12: anchor, cli, egress, session, ship (+7) | configuration 4, conversion 2, engines 2, integrations 2, persistence 15, reporting 5, safety 4, workflows 5 | - | 13: a_run_keeps_its_own_log, a_seed_is_shipped_on_media, an_orphan_whose_parent_the_hop_does_not_take (+10) |
| `migkit/notify.py` | - | 668 | 11: approvals, cli, cutover, schedule, session (+6) | configuration 6, execution 1, integrations 1, reporting 1, safety 3, workflows 1 | apprise | 7: alerts_and_notifications, nothing_leaves_where_nothing_may, one_redactor_hides_every_secret (+4) |
| `migkit/progress_cause.py` | - | 757 | 0 | - | - | 1: progress_cause_diagnostics |
| `migkit/query_guard.py` | - | 1011 | 0 | configuration 1, decision 1, persistence 1 | - | 2: ddl_shield_and_query_guard_wiring, query_optimizer_parity_guard |
| `migkit/report.py` | - | 990 | 4: cli, cutover, export, guide | configuration 1, decision 4, engines 1, execution 5, reporting 7, safety 2, scope 2, workflows 2 | - | 15: a_hop_that_keeps_some_changes_is_held_to_them, a_run_writes_every_step_back_as_it_ends, drain_and_readiness_are_wired (+12) |
| `migkit/servicecheck.py` | - | 335 | 4: cli, imports, report, verdict | configuration 1 | - | 2: a_hop_from_a_dts_job, what_the_service_said_held_against_migkit |
| `migkit/support_reproducer.py` | - | 611 | 0 | configuration 2, safety 2 | - | 1: support_diagnostic_reproducer |
| `migkit/telemetry.py` | - | 1649 | 8: autonomous, cli, faults, model, planner (+3) | cli 1, configuration 6, decision 6, engines 2, integrations 1, persistence 11, reporting 8, safety 2, workflows 1 | - | 2: telemetry_carries_no_customer_data, telemetry_segments_are_sealed_pushed_once_and_received_checked |
| `migkit/traces.py` | - | 309 | 4: cli, cutover, session, tooluse | configuration 3, integrations 1, reporting 1, safety 3 | opentelemetry | 2: migkit_works_inside_the_teams_tools, nothing_leaves_where_nothing_may |
| `migkit/troubleshooting.py` | - | 235 | 1: tools.gen_docs | engines 5 | - | 4: every_module_is_reached, every_write_passes_one_guard, the_manual_is_generated_from_the_code (+1) |
| `migkit/verdict.py` | verdict: verdict.decide | 1247 | 35: agent_control, approvals, catalog, cli, config (+30) | configuration 6, conversion 2, decision 1, engines 5, execution 1, persistence 2, reporting 2, safety 1, scope 2 | - | 39: a_cosmosdb_tail_follows_each_container_on_its_own, a_dynamodb_tail_follows_each_table_on_its_own, a_hop_that_keeps_deletes_on_mysql (+36) |
| `migkit/wording.py` | - | 346 | 37: autonomous, backupseed, cli, costmodel, cutover (+32) | execution 1, persistence 1, safety 1 | - | 8: a_pooler_between_migkit_and_the_database, one_progress_vocabulary, one_redactor_hides_every_secret (+5) |

## decision (36 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/adaptive_bounds.py` | - | 646 | 0 | - | - | 1: adaptive_bounds_cutover_envelope |
| `migkit/advisors.py` | - | 178 | 1: cli | decision 1 | - | 3: advise_follows_the_service_the_hop_names, every_command_ends_in_its_own_words, the_move_way_rungs_hold_and_refuse |
| `migkit/bulkways.py` | - | 142 | 1: movers | configuration 1, decision 4, integrations 5, persistence 1, workflows 2 | - | 1: the_non_sql_tools_are_used_whole |
| `migkit/capabilities.py` | - | 3824 | 11: cli, decide, diagnostics, engines.base, report (+6) | configuration 4, decision 3, engines 8, safety 3, workflows 6 | - | 36: a_document_store_assessment_names_versions_writers_and_what_the_target_lacks, a_document_store_carries_its_indexes_views_and_functions, a_keyed_read_is_paged_by_what_it_measured (+33) |
| `migkit/changedepth.py` | - | 200 | 8: engines.cassandra, engines.kafka, engines.mongodb, engines.mysql, engines.postgres (+3) | decision 1 | - | 6: a_copy_goal_is_not_held_by_the_change_depth, a_mongodb_replica_set_is_followed_by_its_oplog, a_mongodb_standalone_is_followed_by_buckets (+3) |
| `migkit/cloud_readiness.py` | - | 256 | 1: ready | configuration 1, decision 2, integrations 1, safety 1 | - | 3: a_keyed_read_is_paged_by_what_it_measured, cloud_readiness_topology, the_cloud_side_of_readiness_is_audited_before_the_move |
| `migkit/cost_envelope.py` | - | 608 | 1: cli | - | - | 1: cost_envelope_enforcement |
| `migkit/costmodel.py` | - | 781 | 10: decide, engines.generic, incumbent_switch, legs, marks (+5) | configuration 2, decision 2, engines 4, persistence 2, reporting 1, workflows 1 | - | 12: a_managed_services_own_operation_is_ranked_by_its_eligibility, a_mysql_copy_passes_rows_server_to_server, a_way_is_costed_by_what_the_unit_is (+9) |
| `migkit/dba.py` | - | 1502 | 6: cli, guide, managed, physical, ready (+1) | configuration 3, decision 1, engines 8, integrations 1 | - | 11: a_managed_mysql_says_how_long_its_log_is_kept, a_managed_side_is_read_and_held_by_what_its_admin_may_do, a_source_change_is_offered_never_assumed (+8) |
| `migkit/decide.py` | decision: every choice is a rung | 7774 | 204: ack_lattice, action_compiler, advisors, agent_control, ai_proof_loop (+199) | configuration 1, conversion 5, decision 34, engines 5, persistence 4 | - | 257: rung_branches.py, a_batch_that_is_not_a_page_is_the_decisions, a_clone_plugin_offer_on_mysql (+254) |
| `migkit/factbook.py` | - | 483 | 5: cli, decide, fact_validity, jointplan, waysaid | - | - | 1: every_fact_is_declared |
| `migkit/faults.py` | - | 801 | 17: autonomous, causal_diagnosis, cli, engines.generic, engines.hetero (+12) | decision 2, execution 3, reporting 1 | - | 10: a_copier_reconciles_and_retunes, a_failed_bulk_copy_falls_back_to_the_range_copier_on_mysql, a_failure_is_told_by_what_it_did (+7) |
| `migkit/guarantees.py` | - | 682 | 10: apply_contract, cli, decide, engines.base, execution_contract (+5) | - | - | 8: a_finished_tables_indexes_build_while_the_next_copies, a_move_keeps_what_it_must, every_way_is_weighed (+5) |
| `migkit/handwork.py` | - | 162 | 17: engines.bigquery_checks, engines.cassandra, engines.clickhouse, engines.cosmosdb, engines.dynamodb (+12) | - | - | 6: handwork, handwork_mongo, handwork_mysql (+3) |
| `migkit/jointplan.py` | - | 1316 | 6: cli, incumbent_switch, movers, sizing, tableway (+1) | decision 10, execution 1, persistence 1, reporting 4, workflows 3 | - | 7: a_finished_tables_indexes_build_while_the_next_copies, a_move_is_planned_as_one, a_move_keeps_what_it_must (+4) |
| `migkit/keyless.py` | - | 288 | 3: engines.mysql, engines.postgres, scanplan | decision 1 | - | 1: no_pk_ctid_page_slicing |
| `migkit/lacks.py` | - | 1019 | 3: backupseed, case, editions | engines 2, workflows 7 | - | 1: the_case_between_two_servers_is_held_first |
| `migkit/legs.py` | - | 1470 | 6: cli, engines.postgres, machine, movers, physical (+1) | configuration 1, decision 20, engines 8, execution 5, persistence 2, reporting 5, workflows 6 | - | 8: doctor_names_the_ways_a_hop_stands_on, moves_use_overlap_compression_and_parts, phases_overlap_and_legs_are_measured (+5) |
| `migkit/machine.py` | - | 1006 | 9: cli, decide, estate, hopkeys, jointplan (+4) | configuration 3, decision 4, engines 1, integrations 3, persistence 2, reporting 2, workflows 2 | - | 6: every_answer_and_position_keeps_its_scope, placement_advice_topology_sizing, the_cloud_side_of_readiness_is_audited_before_the_move (+3) |
| `migkit/model.py` | - | 1146 | 12: claims, cli, guide, ha, jointplan (+7) | configuration 2, conversion 2, decision 10, engines 2, execution 1, persistence 1, reporting 3 | - | 19: large_values_rig.py, a_move_is_planned_as_one, a_rehearsal_forecasts_the_real_run (+16) |
| `migkit/offset_diag.py` | - | 901 | 0 | - | - | 1: offset_disagreement_diagnosis |
| `migkit/planner.py` | - | 1437 | 20: autonomous, backupseed, cli, cloudseed, decide (+15) | configuration 2, decision 22, execution 1, persistence 2, reporting 3, workflows 8 | - | 43: a_move_is_planned_as_one, a_physical_seed_is_taken_only_where_it_holds, a_provider_operation_is_offered_by_its_facts (+40) |
| `migkit/ready.py` | - | 755 | 3: cli, cloud_readiness, managed | configuration 4, decision 12, engines 1, reporting 1, safety 2, scope 1, workflows 7 | - | 11: a_copy_goal_is_not_held_by_the_change_depth, a_fork_change_is_held_first_mysql_mariadb, a_slow_oracle_reader_stops_the_start (+8) |
| `migkit/rehearsal.py` | - | 440 | 8: cli, cutover, engines.mongodb, model, placement_advice (+3) | configuration 1, decision 5, persistence 2 | - | 4: a_rehearsal_forecasts_the_real_run, every_answer_and_position_keeps_its_scope, everything_a_goal_needs_is_known_first (+1) |
| `migkit/repairways.py` | - | 860 | 2: cli, engines.base | configuration 1, conversion 1, decision 3, persistence 3, reporting 1, safety 2, workflows 3 | - | 6: a_repair_leaves_what_the_hop_keeps, repair_is_a_ladder, repair_ladder_on_postgres_and_mysql (+3) |
| `migkit/scanplan.py` | - | 1072 | 6: engines.base, engines.mssql, engines.mysql, engines.postgres, keyless (+1) | configuration 1, decision 6, engines 1, execution 6, persistence 4, workflows 2 | - | 10: a_copier_reconciles_and_retunes, a_tables_checks_are_read_in_one_plan, adaptive_merkle_bucket_diff (+7) |
| `migkit/scenarios.py` | - | 1342 | 7: autonomous, cli, consolidate, hopkeys, physical (+2) | configuration 5, decision 18, engines 1, workflows 3 | - | 11: a_managed_services_own_operation_is_ranked_by_its_eligibility, a_provider_operation_is_offered_by_its_facts, consolidation_follows_every_source (+8) |
| `migkit/sizing.py` | - | 1199 | 26: checkpoint, claims, cli, decide, engines.base (+21) | configuration 1, conversion 1, decision 3, execution 1, reporting 1 | - | 25: large_values_rig.py, a_copier_reconciles_and_retunes, a_drivers_requests_in_flight_are_decided (+22) |
| `migkit/sourcechange.py` | - | 1106 | 16: archive, cli, dba, engines.mssql, engines.mysql (+11) | configuration 5, decision 6, persistence 2, safety 1 | - | 10: a_clone_plugin_offer_on_mysql, a_copy_never_starts_blind_to_commits_under_way, a_managed_side_is_read_and_held_by_what_its_admin_may_do (+7) |
| `migkit/tableway.py` | - | 897 | 19: cli, costmodel, engines.bigtable, engines.cassandra, engines.cockroachdb (+14) | decision 8, engines 3, execution 1, reporting 1 | - | 9: a_mysql_copy_passes_rows_server_to_server, a_same_engine_copy_passes_rows_server_to_server, every_resource_in_one_model (+6) |
| `migkit/topology.py` | - | 512 | 7: cli, consolidate, fleet_coordination, hopkeys, planner (+2) | configuration 1, decision 2, workflows 1 | - | 8: each_source_of_a_consolidation_is_verified_at_its_own_position, fleet_coordination_capacity, many_node_topologies_are_decided (+5) |
| `migkit/tune.py` | - | 296 | 2: autopilot, managed | configuration 1, persistence 1 | - | 1: automated_target_tuning_flip |
| `migkit/tuning.py` | - | 1085 | 14: autonomous, canon, costmodel, decide, engines.base (+9) | conversion 11, decision 8, execution 2, persistence 1 | - | 9: a_copier_reconciles_and_retunes, a_fault_in_each_copier_is_recovered_on_a_server, autotune_every_dimension (+6) |
| `migkit/userways.py` | - | 59 | 1: users | decision 1 | - | 2: the_password_of_an_account_is_chosen_by_what_each_side_allows, the_userways_rungs_hold_and_refuse |
| `migkit/waysaid.py` | - | 175 | 2: cli, report | decision 4, engines 1, workflows 3 | - | 1: doctor_names_the_ways_a_hop_stands_on |
| `migkit/workload.py` | - | 559 | 7: engines.hetero, engines.mssql, engines.mysql, engines.postgres, storedcode.statements (+2) | conversion 5, engines 1, safety 1, workflows 2 | - | 10: conversion_breadth_mysql_to_postgres, deep_checks_read_only_what_the_hop_takes, oracle_packages_converted_and_proved (+7) |

## engines (80 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/binlog_marks.py` | - | 151 | 2: binlog_payload, engines.mysql | configuration 1 | - | 1: two_way_rungs_are_ranked_by_measurement |
| `migkit/binlog_payload.py` | - | 297 | 1: engines.mysql | engines 1 | - | 2: a_compressed_binlog_is_read_or_stops_the_tail, mariadb_compressed_events_are_read_or_stop_the_tail |
| `migkit/catalog.py` | - | 274 | 6: drift, engines.mssql, engines.postgres, engines.sqlcatalog, engines.sqlite (+1) | reporting 1 | - | 8: a_catalogue_is_one_shape, a_move_keeps_what_it_must, drift_compares_whole_definitions (+5) |
| `migkit/engines/__init__.py` | - | 449 | 55: addresses, archive, attempts, autonomous, canon (+50) | engines 38, execution 1 | - | 171: bigquery_fake.py, a_clone_goes_on_exactly_mysql, a_continuous_hop_feeds_a_warehouse (+168) |
| `migkit/engines/analyticdb.py` | - | 506 | 3: engines.dbapi, engines.singlestore, engines.synapse | configuration 1, conversion 1, decision 1, engines 3, integrations 1 | - | 2: analyticdb_mysql_is_a_side_on_a_fake, analyticdb_postgres_is_a_side_on_a_fake |
| `migkit/engines/ase.py` | - | 699 | 2: engines, tools | conversion 1, decision 4, engines 9, persistence 1 | - | 3: large_values_on_sap_ase, sap_ase_against_a_server, sap_ase_takes_params_users_statistics_and_a_snapshot_on_fakes |
| `migkit/engines/base.py` | scope: Engine.in_scope, Engine.takes | 7965 | 88: applyway, archive, capabilities, charsets, cli (+83) | cli 1, configuration 13, conversion 29, decision 35, engines 2, execution 17, integrations 1, persistence 6, reporting 4, safety 9, scope 3, workflows 31 | - | 143: a_batch_that_is_not_a_page_is_the_decisions, a_character_set_change_is_planned_and_proved, a_check_says_what_changed_since_the_move (+140) |
| `migkit/engines/bigquery_changes.py` | - | 401 | 1: engines.warehouse | conversion 1, engines 1 | - | through engines.warehouse |
| `migkit/engines/bigquery_checks.py` | - | 1518 | 1: engines.warehouse | conversion 3, decision 1, engines 2, reporting 1 | - | through engines.warehouse |
| `migkit/engines/bigtable.py` | - | 1285 | 2: engines.keyspaces, tooluse | conversion 7, decision 9, engines 10, integrations 1 | google-cloud-bigtable | 5: bigtable_as_a_side_of_a_pair, large_values_on_google_and_aws_native_services, the_cloud_native_services_as_sides (+2) |
| `migkit/engines/cassandra.py` | - | 3133 | 4: engines, engines.geminidb, engines.keyspaces, users | configuration 3, conversion 10, decision 9, engines 5, execution 2, integrations 1, persistence 3, reporting 3, safety 3, workflows 9 | - | 20: cassandra_fake.py, a_cassandra_source_is_held_and_given_back, a_cluster_target_is_loaded_as_it_spreads_its_rows (+17) |
| `migkit/engines/clickhouse.py` | - | 3194 | 2: engines, users | conversion 10, decision 7, engines 7, execution 2, persistence 2, reporting 2, safety 4 | - | 13: a_target_is_kept_on_its_server, clickhouse_catalogue_checks_without_a_server, clickhouse_holds_the_hops_scope_without_a_server (+10) |
| `migkit/engines/cockroachdb.py` | - | 860 | 2: engines, engines.tidb | conversion 4, decision 4, engines 4 | - | 10: pgwire_rig.py, a_target_is_kept_on_its_server, a_tls_connection_is_verified_on_the_wire_engines_and_files (+7) |
| `migkit/engines/copybook.py` | - | 780 | 1: engines | conversion 10, decision 2, engines 3, persistence 1 | - | 1: copybook_files_as_a_side_of_a_pair |
| `migkit/engines/cosmosdb.py` | - | 1549 | 2: engines, tooluse | configuration 1, conversion 7, decision 5, engines 15, integrations 2 | azure-cosmos | 7: cosmos_fake.py, a_change_feed_is_read_in_commit_order, a_cosmosdb_tail_follows_each_container_on_its_own (+4) |
| `migkit/engines/couchbase.py` | - | 1181 | 4: engines, engines.cosmosdb, engines.firestore, tooluse | configuration 1, conversion 5, decision 5, engines 5, integrations 1, persistence 1 | couchbase | 3: couchbase_as_a_side_of_a_pair, large_values_on_document_services, the_change_topic_engines_bulk_users_and_snapshots_on_fakes |
| `migkit/engines/db2.py` | - | 1120 | 4: engines, engines.db2i, engines.db2zos, tools | conversion 5, decision 4, engines 12, persistence 1 | - | 8: db2_as_one_side_of_a_pair, db2_bulk_move_and_snapshot_are_decided_from_the_catalogue, db2_changes_are_read_from_its_log_on_a_transcript (+5) |
| `migkit/engines/db2_log.py` | - | 380 | 1: engines.db2 | configuration 1, conversion 1, decision 3 | - | 2: db2_bulk_move_and_snapshot_are_decided_from_the_catalogue, db2_changes_are_read_from_its_log_on_a_transcript |
| `migkit/engines/db2i.py` | - | 1167 | 4: engines, tools, tooluse, users | conversion 7, engines 7, integrations 1 | ibmi-odbc, mapepire | 3: db2_changes_are_read_from_its_log_on_a_transcript, db2_for_i_as_a_side_of_a_pair, db2_takes_sequences_params_users_and_statistics_on_fakes |
| `migkit/engines/db2zos.py` | - | 455 | 1: engines | engines 8 | - | 4: a_quiet_tail_is_told_on_every_source, db2_changes_are_read_from_its_log_on_a_transcript, db2_for_i_as_a_side_of_a_pair (+1) |
| `migkit/engines/dbapi.py` | - | 1348 | 32: dba, editions, engines.ase, engines.cockroachdb, engines.db2 (+27) | conversion 16, decision 6, engines 5, execution 1 | - | 12: a_quiet_tail_is_told_on_every_source, db2_as_one_side_of_a_pair, every_ad_hoc_switch_goes_through_the_decision (+9) |
| `migkit/engines/distmysql.py` | - | 609 | 3: engines.analyticdb, engines.oceanbase, engines.singlestore | configuration 2, engines 5, workflows 1 | - | 3: singlestore_is_a_side_on_a_fake, tdsql_is_followed_set_by_set, the_cloud_native_services_as_sides |
| `migkit/engines/dsql.py` | - | 574 | 1: engines | conversion 4, engines 5, integrations 2 | - | 9: pgwire_rig.py, a_tls_connection_is_verified_on_the_wire_engines_and_files, cockroachdb_and_dsql_carry_counters_and_say_what_a_move_will_meet (+6) |
| `migkit/engines/duckdb.py` | - | 414 | 1: engines | conversion 4, engines 6 | - | 5: duckdb_findings_name_their_table, duckdb_intervals_and_vectors_are_read_whole, duckdb_to_duckdb_keeps_what_a_row_copy_leaves_behind (+2) |
| `migkit/engines/dynamodb.py` | - | 1845 | 2: ddbexport, engines | configuration 1, conversion 10, decision 10, engines 6, execution 1, integrations 5, workflows 5 | - | 9: dynamodb_on_moto.py, a_dynamodb_tail_follows_each_table_on_its_own, dynamodb_copied_through_its_export (+6) |
| `migkit/engines/enterprise_connectors.py` | - | 178 | 5: cli, engines.generic, engines.hetero, engines.mongodb, engines.postgres | conversion 1, decision 1 | - | 5: a_mongodb_replica_set_is_followed_by_its_oplog, a_postgres_replica_wal_is_followed_by_its_records, a_refused_grant_is_said_as_the_grant (+2) |
| `migkit/engines/firestore.py` | - | 1403 | 1: tooluse | conversion 10, decision 6, engines 19, integrations 2, workflows 4 | google-cloud-datastore, google-cloud-firestore | 5: firestore_as_a_side_of_a_pair, large_values_on_google_and_aws_native_services, the_cloud_native_services_as_sides (+2) |
| `migkit/engines/gaussdb.py` | - | 771 | 2: engines.analyticdb, engines.yugabyte | conversion 1, decision 7, engines 6 | - | 5: gaussdb_as_a_side_of_a_pair, the_cloud_native_services_as_sides, the_cloud_native_services_own_bulk_paths (+2) |
| `migkit/engines/gcp.py` | - | 53 | 3: engines.bigtable, engines.firestore, engines.spanner | integrations 1 | - | 3: bigtable_as_a_side_of_a_pair, firestore_as_a_side_of_a_pair, large_values_on_google_and_aws_native_services |
| `migkit/engines/geminidb.py` | - | 210 | 0 | engines 5 | - | 2: geminidb_on_its_apis_stand_ins, the_cloud_native_services_as_sides |
| `migkit/engines/generic.py` | - | 2673 | 3: engines, engines.dbapi, leaves | conversion 3, decision 17, engines 6, execution 5, persistence 2, workflows 7 | reladiff | 21: a_repair_never_writes_what_the_hop_leaves_out, every_check_runs, every_engine_declares_its_gaps (+18) |
| `migkit/engines/hetero.py` | - | 6126 | 15: archive, capabilities, charsets, cli, consolidate (+10) | configuration 2, conversion 39, decision 12, engines 10, execution 16, persistence 14, reporting 3, safety 2, scope 2, workflows 36 | - | 169: a_batch_the_target_committed_is_never_applied_twice, a_bigquery_pair_follows_the_hops_mapping, a_clone_goes_on_exactly_mysql (+166) |
| `migkit/engines/hooks.py` | - | 121 | 40: changetopic, drainmark, drift, engines.ase, engines.bigtable (+35) | - | - | 1: an_engine_asks_its_workflow_through_a_hook |
| `migkit/engines/kafka.py` | - | 4486 | 4: changetopic, engines, mirror, users | configuration 3, conversion 3, decision 7, engines 9, execution 3, integrations 23, persistence 1, reporting 2, safety 3, workflows 23 | - | 26: kafka_fake.py, a_kafka_cluster_copied_through_the_cluster_mirror, a_kafka_cluster_that_asks_for_a_password (+23) |
| `migkit/engines/keyspaces.py` | - | 505 | 1: tooluse | conversion 3, engines 3, integrations 3 | cassandra-sigv4 | 3: a_drivers_requests_in_flight_are_decided, keyspaces_on_a_cassandra_stand_in, the_cloud_native_services_as_sides |
| `migkit/engines/kinesis.py` | - | 140 | 2: changetopic, engines | engines 2, integrations 1 | - | 1: changes_delivered_to_kinesis |
| `migkit/engines/lake.py` | - | 897 | 2: engines, tooluse | configuration 1, conversion 8, decision 1, engines 8 | - | 1: one_engine_is_asked_of_its_own_gaps |
| `migkit/engines/lindorm.py` | - | 287 | 0 | conversion 2, engines 5 | - | 1: the_cloud_native_services_as_sides |
| `migkit/engines/mongodb.py` | - | 5948 | 3: engines, engines.geminidb, movers | configuration 3, conversion 10, decision 18, engines 23, execution 10, persistence 8, reporting 5, safety 6, scope 1, workflows 25 | - | 65: mongo_fake.py, a_cluster_target_is_loaded_as_it_spreads_its_rows, a_collection_copied_document_for_document (+62) |
| `migkit/engines/mssql.py` | - | 5155 | 6: engines, engines.ase, engines.db2, engines.synapse, storedcode.corpus (+1) | conversion 14, decision 19, engines 16, execution 6, integrations 2, persistence 3, reporting 4, safety 1, workflows 9 | - | 51: mssql_stub.py, a_copier_reconciles_and_retunes, a_masked_column_is_proved_as_written (+48) |
| `migkit/engines/mssql_log.py` | - | 415 | 1: engines.mssql | conversion 1, decision 3 | - | 1: sql_server_changes_are_read_from_its_log_and_its_backups |
| `migkit/engines/my_pass.py` | - | 298 | 2: engines.mysql, ship | engines 3, execution 3 | - | 1: mysql_tls_on_every_path |
| `migkit/engines/mysql.py` | - | 11902 | 9: costmodel, crossmove, dba, engines, engines.distmysql (+4) | configuration 11, conversion 32, decision 52, engines 34, execution 19, persistence 12, reporting 5, safety 4, workflows 45 | reladiff | 205: cutover_harness.py, mysql_pair.py, a_batch_that_is_not_a_page_is_the_decisions (+202) |
| `migkit/engines/neo4j.py` | - | 1078 | 1: engines | conversion 2, decision 10, engines 7, workflows 3 | - | 5: engine_ladders_hold_and_refuse, neo4j_compares_settings_and_settles_statistics_on_a_server, qdrant_and_neo4j_close_their_loose_ends (+2) |
| `migkit/engines/neptune.py` | - | 1229 | 2: engines.neo4j, tooluse | conversion 1, decision 10, engines 5, integrations 5 | gremlinpython, neptunedata | 5: engine_ladders_hold_and_refuse, every_engine_is_titled_and_its_hop_is_named, neptune_on_gremlin_and_sparql_servers (+2) |
| `migkit/engines/oceanbase.py` | - | 476 | 0 | conversion 1, decision 2, engines 7, workflows 1 | - | 4: oceanbase_as_a_side_of_a_pair, the_cloud_native_accounts_are_carried, the_cloud_native_services_as_sides (+1) |
| `migkit/engines/opensearch.py` | - | 2778 | 2: engines, users | configuration 2, conversion 8, decision 10, engines 8, execution 7, integrations 2, persistence 2, reporting 2, safety 3 | - | 19: opensearch_fake.py, a_target_is_kept_on_its_server, an_opensearch_source_is_held_and_given_back (+16) |
| `migkit/engines/oracle.py` | - | 2596 | 6: engines, engines.oceanbase, engines.oracle_deep, engines.oracle_redo, storedcode.constraints (+1) | configuration 1, conversion 22, decision 9, engines 25, safety 2, workflows 1 | - | 24: oracle_catalogue.py, a_partition_that_arrived_empty_is_found_on_sql_server_oracle_and_tidb, a_quiet_tail_is_told_on_every_source (+21) |
| `migkit/engines/oracle_catalog.py` | - | 305 | 1: engines.oracle | decision 1, engines 1 | - | through engines.oracle |
| `migkit/engines/oracle_deep.py` | - | 849 | 1: engines.oracle | engines 4, reporting 2, safety 1 | - | through engines.oracle |
| `migkit/engines/oracle_logminer.py` | - | 1654 | 4: dba, engines.oracle, engines.oracle_pump, engines.oracle_redo | conversion 4, decision 18, integrations 2 | - | 10: a_slow_oracle_reader_stops_the_start, engine_ladders_hold_and_refuse, oracle_follows_and_resumes_on_its_lane (+7) |
| `migkit/engines/oracle_pump.py` | - | 233 | 1: engines.oracle | cli 1, configuration 1, decision 4, engines 2 | - | 4: engine_ladders_hold_and_refuse, oracle_is_a_whole_side, oracle_redo_is_read_back_exactly (+1) |
| `migkit/engines/oracle_redo.py` | - | 625 | 4: dba, engines.oracle, tools, tooluse | configuration 1, conversion 2, decision 4, engines 2, integrations 5 | OpenLogReplicator | 6: a_slow_oracle_reader_stops_the_start, every_dependency_may_ship_with_migkit, every_wrapped_option_is_used_or_explained (+3) |
| `migkit/engines/parquet.py` | - | 609 | 2: engines, ship | conversion 4, decision 1, engines 4 | - | 3: a_cross_engine_copy_goes_server_to_server, a_file_database_moves_by_its_own_means, a_lake_table_is_read_through_its_log |
| `migkit/engines/pg_pass.py` | - | 519 | 4: crossmove, engines.postgres, model, ship | decision 2, engines 3, execution 1, workflows 1 | - | 10: a_fallback_says_why_and_a_refusal_stops, a_pass_is_given_up_one_way, a_resumed_range_and_a_seam_leave_their_record (+7) |
| `migkit/engines/pgwire.py` | - | 1607 | 5: engines.analyticdb, engines.cockroachdb, engines.dsql, engines.gaussdb, engines.yugabyte | conversion 9, decision 2, engines 10, reporting 2 | - | 5: cockroachdb_and_dsql_carry_counters_and_say_what_a_move_will_meet, cockroachdb_and_dsql_hold_a_hop_to_what_it_carries_and_keeps, cockroachdb_and_dsql_make_and_read_a_schema (+2) |
| `migkit/engines/postgres.py` | - | 15736 | 10: cutover, engines, engines.mysql, engines.pg_pass, engines.pgwire (+5) | configuration 19, conversion 25, decision 63, engines 26, execution 31, persistence 14, reporting 10, safety 8, workflows 66 | pg_dump, pg_restore | 260: cutover_harness.py, a_batch_that_is_not_a_page_is_the_decisions, a_cloud_copy_seeds_the_target (+257) |
| `migkit/engines/provedsql.py` | - | 128 | 2: engines.distmysql, engines.gaussdb | conversion 4, decision 3 | - | 2: engine_ladders_hold_and_refuse, the_cloud_native_services_as_sides |
| `migkit/engines/pubsub.py` | - | 148 | 1: engines | engines 2, integrations 1 | - | 1: changes_delivered_to_pubsub |
| `migkit/engines/qdrant.py` | - | 897 | 1: engines | configuration 1, conversion 1, decision 10, engines 3 | - | 6: engine_ladders_hold_and_refuse, qdrant_and_neo4j_close_their_loose_ends, qdrant_and_neo4j_go_deeper (+3) |
| `migkit/engines/redis.py` | - | 4216 | 4: engines, engines.geminidb, movers, redisshake | configuration 5, conversion 1, decision 7, engines 11, execution 6, persistence 2, reporting 1, safety 2, workflows 10 | - | 37: redis_local.py, a_connection_is_verified_where_it_can_be, a_fork_change_is_held_first_redis_valkey (+34) |
| `migkit/engines/saas.py` | - | 1471 | 4: addresses, engines, engines.saas_salesforce, tooluse | conversion 6, decision 1, engines 3, execution 1, integrations 3, persistence 1 | airbyte-connectors, dlt, singer-tap | 2: saas_sources_as_programs, salesforce_changes_through_its_events |
| `migkit/engines/saas_salesforce.py` | - | 688 | 2: engines.saas, tooluse | conversion 2, decision 2, engines 3, integrations 1 | salesforce-api | 2: salesforce_changes_through_its_events, the_ways_chosen_as_they_run_hold_and_refuse |
| `migkit/engines/sapodp.py` | - | 1217 | 4: cli, engines, ready, tooluse | conversion 2, decision 2, engines 4, integrations 1, persistence 1 | pyodata | 3: an_sap_systems_database_and_subscriptions, sap_application_data_through_odp, the_ways_chosen_as_they_run_hold_and_refuse |
| `migkit/engines/singlestore.py` | - | 137 | 0 | decision 1, engines 3 | - | 1: singlestore_is_a_side_on_a_fake |
| `migkit/engines/spanner.py` | - | 1810 | 3: engines.firestore, engines.timestream, tooluse | conversion 9, decision 9, engines 10, integrations 2 | google-cloud-spanner | 7: engine_ladders_hold_and_refuse, large_values_on_google_and_aws_native_services, spanner_as_a_side_of_a_pair (+4) |
| `migkit/engines/sqlcatalog.py` | - | 1329 | 1: engines.db2 | conversion 3, engines 7, reporting 2, safety 1 | - | through engines.db2 |
| `migkit/engines/sqlite.py` | - | 1222 | 1: engines | conversion 7, decision 1, engines 11, persistence 1 | - | 39: a_carrier_is_held_to_its_limits, a_copy_holds_a_read_not_a_table, a_key_that_holds_null (+36) |
| `migkit/engines/synapse.py` | - | 197 | 0 | decision 1, engines 4, workflows 1 | - | 1: synapse_is_a_side_on_a_fake |
| `migkit/engines/teradata.py` | - | 2745 | 3: engines, tooluse, users | conversion 10, decision 7, engines 7, integrations 1, persistence 2, reporting 2, safety 2 | teradatasql | 6: teradata_systems.py, assess_says_what_each_engine_reaches, deep_checks_read_only_what_the_hop_takes (+3) |
| `migkit/engines/tidb.py` | - | 1058 | 1: engines | conversion 9, decision 6, engines 8, safety 1 | - | 17: a_partition_that_arrived_empty_is_found_on_sql_server_oracle_and_tidb, a_sql_server_and_tidb_table_comes_back_from_where_it_left, a_sql_server_and_tidb_table_joins_a_running_tail_at_an_exact_seam (+14) |
| `migkit/engines/timestream.py` | - | 681 | 1: tooluse | conversion 2, decision 7, engines 8, integrations 3 | timestream | 5: engine_ladders_hold_and_refuse, every_engine_is_titled_and_its_hop_is_named, every_provider_call_goes_through_the_egress_gate (+2) |
| `migkit/engines/warehouse.py` | - | 4365 | 5: engines, engines.bigquery_checks, engines.lake, engines.synapse, tooluse | configuration 3, conversion 11, decision 4, engines 13, integrations 7, safety 1 | deltalake, google-cloud-bigquery, google-cloud-bigquery-storage, pyiceberg, redshift_connector, snowflake-connector-python, snowpipe-streaming | 13: bigquery_fake.py, a_bigquery_pair_follows_the_hops_mapping, a_continuous_hop_feeds_a_warehouse (+10) |
| `migkit/engines/warehouse_sql.py` | - | 839 | 4: engines.duckdb, engines.lake, engines.spanner, engines.warehouse | conversion 2, decision 3 | - | 6: engine_ladders_hold_and_refuse, large_values_in_pieces, the_cloud_native_services_as_sides (+3) |
| `migkit/engines/yugabyte.py` | - | 106 | 0 | decision 1, engines 2 | - | 1: yugabytedb_is_a_side_on_a_fake |
| `migkit/logddl.py` | - | 320 | 3: ddl, engines.mysql, engines.oracle | - | - | 4: a_ddl_is_read_at_its_place_in_the_log, a_statement_binlog_is_followed_as_reloads_or_stops_the_tail, oracle_follows_tables_that_come_and_go_and_change_shape (+1) |
| `migkit/pgslot.py` | - | 532 | 3: engines.gaussdb, engines.postgres, tailctl | configuration 1, conversion 3 | - | 9: a_ddl_is_read_at_its_place_in_the_log, pgslot, pgslot_live (+6) |
| `migkit/quiet_source.py` | - | 391 | 0 | - | - | 1: quiet_source_wal_advance |
| `migkit/standby.py` | - | 81 | 1: engines.postgres | - | - | 1: standby |
| `migkit/tailwhere.py` | - | 76 | 20: engines.bigquery_changes, engines.bigtable, engines.clickhouse, engines.cosmosdb, engines.couchbase (+15) | engines 1 | - | through engines.bigtable, engines.clickhouse (+17) |

## integrations (23 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/assist.py` | - | 197 | 3: cli, storedcode.convert, storedcode.model | configuration 1, safety 2 | - | 2: help_from_a_model_any_provider, nothing_leaves_where_nothing_may |
| `migkit/avrostream.py` | - | 166 | 2: engines.kafka, schemaformats | configuration 1, conversion 1, integrations 1 | - | 1: changes_go_as_avro_through_a_registry |
| `migkit/backupseed.py` | - | 454 | 1: physical | configuration 1, decision 2, reporting 1, workflows 10 | - | 3: the_carrier_and_seed_rungs_hold_and_refuse, the_case_between_two_servers_is_held_first, the_scope_reads_alike_everywhere |
| `migkit/blockseed.py` | - | 283 | 1: ship | persistence 1 | - | 1: a_seed_from_a_block_snapshot_carries_only_changed_blocks |
| `migkit/carriers.py` | - | 1492 | 4: engines.analyticdb, engines.oracle_logminer, hopkeys, ship | configuration 2, decision 1, integrations 3, persistence 1 | azure-storage-blob, boto3-s3, google-cloud-storage | 8: a_carrier_is_held_to_its_limits, a_seed_is_shipped_on_media, a_seed_on_media_between_mysql_servers (+5) |
| `migkit/cloudapi.py` | - | 6667 | 27: anchor, carriers, cloud_readiness, cloudseed, config (+22) | configuration 7, decision 2, integrations 2, safety 2 | alibabacloud-sdk, aws-sdk, azure-sdk, google-api-client, huaweicloud-sdk, ibm-cloud-sdk-core, oci-sdk, tencentcloud-sdk | 15: a_continuous_hop_feeds_a_warehouse, a_keyed_read_is_paged_by_what_it_measured, a_managed_services_own_operation_is_ranked_by_its_eligibility (+12) |
| `migkit/cloudseed.py` | - | 1110 | 1: physical | configuration 2, decision 1, integrations 6, workflows 6 | - | 2: a_cloud_copy_seeds_the_target, the_carrier_and_seed_rungs_hold_and_refuse |
| `migkit/ddbexport.py` | - | 434 | 2: engines.dynamodb, tooluse | engines 1, integrations 4, persistence 1 | dynamodb-s3 | 3: dynamodb_copied_through_its_export, the_engines_table_way_rungs_hold_and_refuse, the_wrapped_programs_run_one_way |
| `migkit/dsbulk.py` | - | 358 | 2: engines.cassandra, tooluse | execution 2, integrations 4 | dsbulk | 2: cassandra_copied_by_the_bulk_loader, the_non_sql_tools_are_used_whole |
| `migkit/fromsnapshot.py` | - | 338 | 2: engines.opensearch, tooluse | execution 3, integrations 4 | reindex-from-snapshot | 2: opensearch_copied_from_a_snapshot, the_wrapped_programs_run_one_way |
| `migkit/huawei_sdk.py` | - | 122 | 0 | - | - | 2: each_cloud_is_asked_through_its_own_api, every_module_is_reached |
| `migkit/imports.py` | - | 3356 | 1: guide | configuration 2, engines 3, reporting 1, workflows 1 | - | 5: a_hop_from_a_dts_job, a_hop_from_another_clouds_job, a_hop_from_another_tools_task (+2) |
| `migkit/integrations.py` | - | 245 | 0 | cli 1, configuration 1, execution 1, integrations 1, persistence 1, reporting 2 | - | 4: every_module_is_reached, migkit_works_inside_the_teams_tools, one_verdict_one_exit_code (+1) |
| `migkit/kafkaclient.py` | - | 906 | 1: engines.kafka | decision 5 | - | 2: the_kafka_client_and_keyspace_copy_rungs_hold_and_refuse, the_stream_client_and_codec_are_chosen_by_facts |
| `migkit/mirror.py` | - | 699 | 4: bulkways, engines.kafka, movers, tooluse | configuration 1, decision 1, engines 2, execution 7, integrations 6, persistence 1, reporting 3 | connect-mirror-maker.sh | 4: a_kafka_cluster_copied_through_the_cluster_mirror, the_non_sql_tools_are_used_whole, the_stream_client_and_codec_are_chosen_by_facts (+1) |
| `migkit/redisshake.py` | - | 727 | 3: bulkways, movers, tooluse | configuration 1, decision 5, engines 6, execution 4, integrations 4, reporting 3, workflows 1 | redis-shake | 5: redis_copied_by_the_replica_protocol, the_hop_takes_only_what_it_includes, the_kafka_client_and_keyspace_copy_rungs_hold_and_refuse (+2) |
| `migkit/registry.py` | - | 243 | 3: avrostream, engines.kafka, schemaformats | - | - | 4: a_kafka_registry_schema_drift_is_found_by_field_type, a_secret_is_read_once_where_it_is_used, changes_go_as_avro_through_a_registry (+1) |
| `migkit/seedtools.py` | - | 187 | 1: tooluse | integrations 1 | azure-storage-blob, boto3-s3, google-cloud-storage, pyrage, zstandard | through tooluse |
| `migkit/ship.py` | - | 2686 | 2: cli, machine | cli 2, conversion 4, decision 8, engines 11, execution 5, integrations 3, persistence 13, reporting 5, safety 2, workflows 3 | pyrage, zstandard | 14: a_carrier_is_held_to_its_limits, a_managed_mysql_says_how_long_its_log_is_kept, a_repair_never_writes_what_the_hop_leaves_out (+11) |
| `migkit/tools.py` | - | 2937 | 17: cli, cloudapi, engines.mssql, engines.oracle_redo, engines.saas (+12) | configuration 10, decision 2, engines 8, execution 10, integrations 4, persistence 3, reporting 1, workflows 3 | - | 25: a_cross_engine_table_goes_the_way_measured_fastest, a_drivers_requests_in_flight_are_decided, a_migration_completes_inside_a_closed_network (+22) |
| `migkit/tooluse.py` | - | 317 | 37: anchor, cloudapi, ddbexport, dsbulk, engines.bigtable (+32) | conversion 3, engines 16, integrations 8, persistence 2, reporting 2, web 1, workflows 4 | - | 16: a_native_replica_is_proved_two_ways, each_cloud_is_asked_through_its_own_api, every_verifier_feature_is_used_or_explained (+13) |
| `migkit/toolversion.py` | - | 268 | 5: engines.base, movers, runner, tools, unchanged | execution 4, integrations 1 | - | 6: handwork_pg, one_runner_for_every_program, the_install_is_pinned (+3) |
| `migkit/workflow_exporter.py` | - | 176 | 1: integrations | configuration 1 | - | 1: the_folded_ha_and_workflow_ways |

## workflows (111 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/action_compiler.py` | - | 1298 | 0 | decision 1 | - | 1: action_compiler_causal_plan |
| `migkit/agent_control.py` | - | 1714 | 1: cutover | decision 1, execution 1, persistence 1, reporting 1, safety 1, workflows 1 | - | 1: agent_control_constrained_ops |
| `migkit/ai_proof_loop.py` | - | 1401 | 1: repair_fence | decision 1, persistence 1 | - | 1: ai_proof_loop_executable_evidence |
| `migkit/anti_corruption.py` | - | 266 | 2: cutover, verify_method | decision 1 | - | 1: anti_corruption_verifier_blind_spots |
| `migkit/application_readiness.py` | - | 959 | 0 | decision 1, persistence 1, safety 1, workflows 2 | - | 2: application_readiness_behavioral, query_regression_performance_envelope |
| `migkit/apply_contract.py` | - | 1487 | 0 | decision 2, persistence 1, safety 1 | - | 1: apply_contract_costing |
| `migkit/applyway.py` | - | 359 | 1: engines.base | conversion 1, decision 1, engines 1 | - | 2: the_apply_way_climbs_to_columns, the_move_way_rungs_hold_and_refuse |
| `migkit/appversions.py` | - | 458 | 6: cli, cutover, engines.base, engines.mysql, engines.postgres (+1) | - | - | 1: a_migration_run_on_one_side_only_is_found |
| `migkit/archive.py` | - | 678 | 2: cli, hopkeys | conversion 2, decision 6, engines 3, safety 2, workflows 1 | - | 2: old_rows_are_archived_then_removed_only_once_proved, the_move_way_rungs_hold_and_refuse |
| `migkit/autonomous.py` | - | 1233 | 4: agent_control, cli, telemetry, tools.measures_table | decision 7, engines 1, execution 1, persistence 1, reporting 2, workflows 1 | - | 10: agent_control_constrained_ops, autonomous_scenarios, autonomous_workflow (+7) |
| `migkit/autopilot.py` | - | 826 | 1: cli | conversion 1, decision 3, execution 1, persistence 2, workflows 5 | - | 2: ddl_shield_and_query_guard_wiring, migkit_autopilot_one_command |
| `migkit/backfill_race.py` | - | 680 | 0 | persistence 1 | - | 1: backfill_race_epoch_reconcile |
| `migkit/barriers.py` | - | 483 | 6: engines.dynamodb, engines.kafka, engines.mongodb, engines.redis, freeze (+1) | configuration 13, workflows 6 | - | 3: the_mark_travels_in_the_atomic_unit_on_kafka, tombstones_and_barriers, transaction_dag_barrier_scheduler |
| `migkit/cache_semantics.py` | - | 1593 | 2: cutover, tailctl | decision 1 | - | 1: cache_semantics_expiry_eviction |
| `migkit/capability_discovery.py` | - | 1434 | 0 | decision 1 | - | 1: capability_discovery_contracts |
| `migkit/causal_diagnosis.py` | - | 886 | 0 | decision 2 | - | 1: causal_diagnosis_discriminating_evidence |
| `migkit/changetopic.py` | - | 1398 | 2: described, engines.firestore | configuration 6, conversion 4, decision 1, engines 3 | - | 7: cosmos_fake.py, a_change_feed_is_read_in_commit_order, cockroachdb_follows_through_its_changefeed (+4) |
| `migkit/closed_loop_proof.py` | - | 1397 | 0 | decision 1, persistence 1 | - | 1: closed_loop_proof_replay |
| `migkit/complete_path.py` | - | 1755 | 0 | decision 1, persistence 1, safety 1 | - | 1: complete_path_optimization |
| `migkit/conflict_semantics.py` | - | 1333 | 1: tailctl | decision 4 | - | 2: conflict_rules_data_semantics, conflict_semantics_rungs |
| `migkit/connection_only.py` | - | 1008 | 0 | configuration 1, decision 1 | - | 1: connection_only_reusable_intent |
| `migkit/connector_capability.py` | - | 1810 | 1: cutover | conversion 3, decision 1, persistence 1 | - | 1: connector_capability_schema_drift |
| `migkit/consolidate.py` | - | 1316 | 6: archive, charsets, cli, hopkeys, reshard (+1) | configuration 2, conversion 3, decision 11, engines 8, reporting 1, workflows 3 | - | 7: a_consolidations_counters_are_of_the_tables_it_takes, a_split_and_a_consolidation_are_followed_and_cut_over_as_one, consolidation_follows_every_source (+4) |
| `migkit/context_learning.py` | - | 1503 | 0 | decision 1, persistence 1 | - | 1: context_learning_transfer |
| `migkit/continuous_scope.py` | - | 1745 | 0 | decision 1 | - | 1: continuous_scope_lifecycle |
| `migkit/conversion_ledger.py` | - | 1148 | 1: storedcode.convert | decision 1, persistence 1 | - | 1: conversion_ledger_scope_accounting |
| `migkit/crossmove.py` | - | 1101 | 3: engines.hetero, tools, tools.gen_codemap | configuration 1, conversion 10, decision 13, engines 3, execution 2, safety 1, workflows 1 | - | 6: a_cross_engine_copy_goes_server_to_server, a_cross_engine_table_goes_the_way_measured_fastest, a_fault_in_each_copier_is_recovered_on_a_server (+3) |
| `migkit/cutover.py` | - | 3190 | 11: approvals, cli, flight_simulator, freeze, managed (+6) | configuration 9, decision 10, engines 5, execution 5, persistence 12, reporting 9, safety 16, scope 1, workflows 46 | - | 27: a_cutover_proof_on_postgres_reads_every_table, a_delta_reads_what_the_hop_takes_pg, a_kafka_cutover_drains_to_the_end_offsets (+24) |
| `migkit/dag_scheduler.py` | - | 748 | 2: barriers, transaction_dag | - | - | 2: transaction_dag_barrier_scheduler, transaction_dag_parallel_apply |
| `migkit/decision_compute.py` | - | 1381 | 0 | decision 1 | - | 1: decision_compute_budget |
| `migkit/discovery_closure.py` | - | 1406 | 0 | decision 1 | - | 1: discovery_closure_scope |
| `migkit/document_model.py` | - | 2509 | 1: cutover | decision 1 | - | 1: document_model_transform_verification |
| `migkit/drainmark.py` | - | 338 | 3: capabilities, cli, cutover | engines 1, persistence 1, safety 2, workflows 2 | - | 4: a_resumed_range_and_a_seam_leave_their_record, drain_and_readiness_are_wired, drain_to_position_and_the_approval_gate (+1) |
| `migkit/drift.py` | - | 385 | 10: cli, cutover, engines.hetero, engines.mssql, engines.mysql (+5) | engines 5, persistence 1, safety 1 | - | 9: a_check_says_what_changed_since_the_move, a_move_keeps_what_it_must, an_online_rewrites_working_tables_are_left_alone (+6) |
| `migkit/dynamic_sql_tracer.py` | - | 1592 | 2: cutover, ready | decision 1 | - | 1: dynamic_sql_trace_app_dependencies |
| `migkit/editions.py` | - | 1077 | 3: backupseed, case, upgrade | decision 4, engines 1, reporting 1, workflows 3 | - | 2: an_edition_change_on_sql_server, the_case_between_two_servers_is_held_first |
| `migkit/end_to_end_bar.py` | - | 193 | 0 | configuration 2 | - | 1: end_to_end_bar_superiority_claims |
| `migkit/etl_bi_waves.py` | - | 1251 | 2: cutover, jointplan | decision 1, persistence 1 | - | 1: etl_bi_wave_planner |
| `migkit/execution_contract.py` | - | 1947 | 0 | decision 2, persistence 1, safety 1 | - | 1: execution_contract_intent_compilation |
| `migkit/fact_validity.py` | - | 1151 | 0 | decision 2 | - | 1: fact_validity_bounded_context |
| `migkit/failure_budget.py` | - | 1369 | 1: cutover | decision 2, persistence 1 | - | 1: failure_budget_recovery_allocation |
| `migkit/fleet.py` | - | 423 | 5: attempts, claims, ha, lease, ranges | execution 1, persistence 2, safety 1, workflows 1 | - | 4: a_standby_mover_takes_over_and_fences, every_sandbox_target_holds_the_fencing_number_its_own_way, takeover_is_used_and_topology_reads_the_hop (+1) |
| `migkit/fleet_coordination.py` | - | 1363 | 0 | decision 2, safety 1 | - | 1: fleet_coordination_capacity |
| `migkit/flight_simulator.py` | - | 415 | 3: autopilot, cli, cutover | persistence 1, workflows 1 | - | 1: shadow_cutover_flight_simulator |
| `migkit/ha.py` | - | 1140 | 8: claims, cli, engines.kafka, export, fleet (+3) | cli 1, configuration 1, decision 4, engines 2, execution 1, persistence 13, safety 2, workflows 8 | - | 6: a_standby_takes_the_move_over_where_it_stood, one_writer_at_a_time_is_proved_from_what_was_kept, takeover_is_used_and_topology_reads_the_hop (+3) |
| `migkit/handoff_overlap.py` | - | 519 | 0 | workflows 2 | - | 1: handoff_overlap_contract |
| `migkit/history_contract.py` | - | 2122 | 2: cutover, tailctl | decision 1 | - | 1: history_contract_semantics |
| `migkit/incumbent_switch.py` | - | 1641 | 1: cutover | decision 3 | - | 1: incumbent_switch_transition_costs |
| `migkit/indexes.py` | - | 906 | 5: engines.postgres, movers, schema, setaside, target_tuning_contract | decision 2, engines 4, execution 2, persistence 2, workflows 3 | - | 9: a_cutover_proof_on_postgres_reads_every_table, deferred_index_and_non_blocking_fk, foreign_keys_go_back_or_the_move_stops (+6) |
| `migkit/information_budget.py` | - | 1248 | 0 | decision 1, persistence 1 | - | 1: information_budget_voI |
| `migkit/jobs.py` | - | 531 | 3: capabilities, cutover, engines.mssql | configuration 2, engines 1, persistence 2, safety 1 | - | 7: clickhouse_catalogue_checks_without_a_server, oracle_jobs_run_on_one_side_at_a_time, sql_server_agent_jobs_run_on_one_side_at_a_time (+4) |
| `migkit/keyless_proof.py` | - | 1605 | 0 | conversion 3, decision 1 | - | 1: keyless_proof_multiset_scale |
| `migkit/leaves.py` | - | 2577 | 11: crossmove, engines.base, engines.hetero, engines.mongodb, export (+6) | configuration 2, conversion 3, decision 5, engines 2, execution 4, persistence 7, workflows 7 | reladiff | 27: a_batch_that_is_not_a_page_is_the_decisions, a_follow_and_a_ship_keep_what_they_chose, a_keyless_table_resumes_by_its_rows_hash (+24) |
| `migkit/live_replan.py` | - | 1467 | 0 | decision 4, persistence 1, safety 1 | - | 1: live_replan_priced_transitions |
| `migkit/lob_policy.py` | - | 1599 | 0 | conversion 1, decision 1 | - | 1: lob_policy_no_truncation |
| `migkit/lob_transaction.py` | - | 522 | 0 | - | - | 1: lob_transaction_semantics |
| `migkit/mapping_proof.py` | - | 1282 | 2: movers, tailctl | decision 1, persistence 1 | - | 2: composed_mapping_proof_load_and_cdc, mapping_proof_rungs |
| `migkit/marks.py` | - | 784 | 12: barriers, cli, engines.cassandra, engines.dynamodb, engines.hetero (+7) | decision 7, workflows 4 | - | 11: one_way_batches_are_numbered_on_the_target, the_mark_travels_in_the_atomic_unit_on_cassandra, the_mark_travels_in_the_atomic_unit_on_dynamodb (+8) |
| `migkit/movers.py` | - | 9739 | 32: autonomous, bulkways, capabilities, cli, cutover (+27) | configuration 15, conversion 2, decision 63, engines 64, execution 20, integrations 14, persistence 8, reporting 30, safety 1, workflows 17 | debezium, mydumper, myloader, mysqlsh, pg_dump, pg_restore, pgcopydb, redpanda | 126: a_batch_that_is_not_a_page_is_the_decisions, a_connection_is_verified_where_it_can_be, a_cross_engine_table_goes_the_way_measured_fastest (+123) |
| `migkit/online_confirmation.py` | - | 1384 | 0 | decision 1, persistence 1 | - | 1: online_confirmation_divergence |
| `migkit/operation_experiments.py` | - | 982 | 0 | decision 1, persistence 1, safety 1 | - | 1: operation_experiments_whole_runs |
| `migkit/physical.py` | - | 2142 | 11: backupseed, case, charsets, cli, cloudseed (+6) | configuration 5, decision 14, engines 2, execution 5, integrations 6, persistence 1, reporting 1, workflows 7 | - | 19: a_clone_goes_on_exactly_mysql, a_clone_plugin_offer_on_mysql, a_cloud_copy_seeds_the_target (+16) |
| `migkit/placement_advice.py` | - | 1415 | 1: cutover | decision 3, persistence 1 | - | 1: placement_advice_topology_sizing |
| `migkit/principal_migration.py` | - | 1437 | 2: application_readiness, cutover | decision 1 | - | 1: principal_security_migration |
| `migkit/progress_liveness.py` | - | 1674 | 1: autopilot | decision 1, persistence 1 | - | 2: migkit_autopilot_one_command, progress_liveness_detection |
| `migkit/proof_planning.py` | - | 1487 | 0 | decision 1, persistence 1 | - | 1: proof_planning_coverage_dag |
| `migkit/provider_path.py` | - | 1421 | 0 | decision 1, persistence 1, safety 1 | - | 1: provider_path_differentiation |
| `migkit/proxy.py` | - | 802 | 2: cutover, reverse | configuration 2 | - | 2: seamless_proxy_pooler_traffic_shifting, zero_loss_reverse_failback_loop |
| `migkit/query_regression.py` | - | 1273 | 2: application_readiness, cutover | decision 1, persistence 1 | - | 1: query_regression_performance_envelope |
| `migkit/ratchet.py` | - | 386 | 3: cutover, engines.mysql, engines.postgres | configuration 2 | - | 1: pre_syncback_headroom_ratchet |
| `migkit/release_intelligence.py` | - | 1978 | 1: cutover | decision 1 | - | 1: release_intelligence_falsifiable |
| `migkit/repair_fence.py` | - | 1438 | 0 | decision 1, persistence 1, workflows 2 | - | 2: ai_proof_loop_executable_evidence, repair_fence_cdc_coordination |
| `migkit/replicasum.py` | - | 606 | 2: engines.mysql, tooluse | decision 2, engines 1, execution 6, integrations 3, workflows 2 | pt-table-checksum | 3: a_native_replica_is_proved_two_ways, an_untimed_program_gets_one_bounded_trial, the_pieces_repair_and_replica_checksum_rungs_hold_and_refuse |
| `migkit/reshard.py` | - | 2748 | 7: cli, consolidate, hopkeys, scenarios, shard_ddl (+2) | configuration 1, conversion 3, decision 9, engines 11, execution 1, persistence 4, safety 6, workflows 22 | - | 7: a_cluster_target_is_loaded_as_it_spreads_its_rows, a_split_and_a_consolidation_are_followed_and_cut_over_as_one, decided_scenarios (+4) |
| `migkit/resource_ownership.py` | - | 971 | 0 | decision 1, persistence 1, safety 1 | - | 1: resource_ownership_lifecycle |
| `migkit/retention_controller.py` | - | 679 | 1: tailctl | decision 1, persistence 1, workflows 2 | - | 1: wal_spool_retention_controller |
| `migkit/reverse.py` | - | 777 | 3: autopilot, cli, cutover | persistence 2, workflows 15 | - | 2: preflight_reversibility_failback_contract, zero_loss_reverse_failback_loop |
| `migkit/reverse_contract.py` | - | 1312 | 1: reverse | decision 1, persistence 1, safety 2 | - | 1: preflight_reversibility_failback_contract |
| `migkit/ring_buffer.py` | - | 1279 | 2: movers, retention_controller | configuration 1, decision 4, persistence 1 | - | 1: store_and_forward_ring_buffer |
| `migkit/robust_scheduling.py` | - | 1398 | 0 | decision 1, persistence 1, safety 1 | - | 1: robust_scheduling_tail_risk |
| `migkit/rollback.py` | - | 449 | 0 | - | - | 1: app_rollback_coordination |
| `migkit/rollback_set.py` | - | 488 | 0 | configuration 1 | - | 1: targeted_rollback_set |
| `migkit/rule_generalization.py` | - | 1400 | 2: convert, rules | decision 1 | - | 1: rule_generalization_narrow_scoping |
| `migkit/rules.py` | - | 150 | 1: engines.base | conversion 1, engines 1, workflows 3 | - | 1: business_rules |
| `migkit/second_reader.py` | - | 816 | 7: cli, engines.base, engines.hetero, engines.mysql, engines.postgres (+2) | configuration 1, decision 2, engines 1, execution 3, safety 1 | datacompy, dvt | 10: a_run_writes_every_step_back_as_it_ends, every_verifier_feature_is_used_or_explained, second_reader (+7) |
| `migkit/semantic_negotiation.py` | - | 1570 | 0 | decision 1, persistence 1, safety 1 | - | 1: semantic_negotiation_cross_engine |
| `migkit/shadow_resync.py` | - | 1193 | 1: repair_fence | decision 4, persistence 1 | - | 2: shadow_resnapshot_atomic_swap, shadow_resync_lifecycle |
| `migkit/shard_ddl.py` | - | 1947 | 1: cutover | conversion 6, decision 7, workflows 1 | - | 1: shard_ddl_convergence |
| `migkit/snapshot_watermark.py` | - | 1212 | 1: movers | decision 1, persistence 1, workflows 3 | - | 2: snapshot_watermark_rungs, snapshot_watermark_stream_handoff |
| `migkit/source_role.py` | - | 1861 | 0 | decision 1, persistence 1 | - | 2: replica_snapshot_valid_source_role, source_role_discovery |
| `migkit/split.py` | - | 400 | 3: cli, hopkeys, reshard | configuration 1, conversion 2, engines 3, workflows 1 | - | 1: one_database_is_split_every_row_once |
| `migkit/storage_protocol.py` | - | 1489 | 1: cutover | decision 1 | - | 1: storage_protocol_commit_semantics |
| `migkit/stream_metadata.py` | - | 2137 | 1: cutover | decision 1 | - | 1: stream_metadata_consumers_and_schemas |
| `migkit/streamout.py` | - | 126 | 2: engines.kafka, schemaformats | configuration 1, conversion 2, engines 1 | - | through engines.kafka, schemaformats |
| `migkit/subset.py` | - | 820 | 3: cli, hopkeys, ready | conversion 5, decision 1, engines 3, safety 1, workflows 1 | - | 3: a_batch_that_is_not_a_page_is_the_decisions, a_subset_is_complete_and_masked, the_literal_pages_are_the_decisions |
| `migkit/tableengine.py` | - | 1128 | 7: cli, engines.base, engines.mysql, engines.postgres, hopkeys (+2) | configuration 1, conversion 2, decision 9, engines 5, reporting 1 | - | 4: a_storage_engine_change_is_made_in_the_schema_step, decided_scenarios, the_table_engine_and_tail_table_rungs_hold_and_refuse (+1) |
| `migkit/tailctl.py` | - | 1080 | 32: approvals, cli, consolidate, drainmark, engines.base (+27) | decision 6, engines 5, execution 7, persistence 3, reporting 1, safety 1, workflows 6 | - | 68: a_batch_the_target_committed_is_never_applied_twice, a_copier_reconciles_and_retunes, a_cosmosdb_tail_follows_each_container_on_its_own (+65) |
| `migkit/tailtables.py` | - | 2232 | 8: engines.hetero, engines.mongodb, engines.postgres, ha, hopkeys (+3) | cli 1, configuration 3, conversion 3, decision 3, engines 1, execution 3, persistence 6, reporting 1, workflows 6 | - | 30: a_batch_the_target_committed_is_never_applied_twice, a_cosmosdb_tail_follows_each_container_on_its_own, a_dynamodb_tail_follows_each_table_on_its_own (+27) |
| `migkit/target_tuning_contract.py` | - | 2265 | 2: autopilot, cutover | decision 2, persistence 1, workflows 1 | - | 1: target_tuning_durability_contract |
| `migkit/time_aware_control.py` | - | 1381 | 0 | decision 1, persistence 1, safety 1 | - | 1: time_aware_control_windows |
| `migkit/topicstates.py` | - | 413 | 2: capabilities, engines.kafka | workflows 2 | - | 2: each_engine_says_what_it_lacks_from_the_code, the_kafka_tail_keeps_each_topic_in_a_state |
| `migkit/traffic_state.py` | - | 994 | 2: autopilot, cutover | decision 1, safety 1 | - | 1: cutover_client_db_traffic_state |
| `migkit/transaction_dag.py` | - | 2024 | 0 | decision 1, persistence 1, workflows 5 | - | 1: transaction_dag_parallel_apply |
| `migkit/twoway.py` | - | 1781 | 12: cli, engines.base, engines.dynamodb, engines.hetero, engines.mysql (+7) | conversion 1, decision 1, engines 1, persistence 6, reporting 1, safety 1, workflows 1 | - | 12: a_batch_the_target_committed_is_never_applied_twice, redis_live_follow_is_reachable_and_reads_the_snapshot, redis_tail_is_lossless_and_two_site_rules_hold (+9) |
| `migkit/uncertain_effects.py` | - | 1092 | 0 | decision 2, execution 1, safety 1 | - | 1: uncertain_effects_reconciliation |
| `migkit/unchanged.py` | - | 495 | 5: engines.mssql, engines.mysql, engines.postgres, ranges, scanplan | integrations 1 | - | 7: a_cutover_proof_on_postgres_reads_every_table, a_table_is_skipped_only_on_counters_that_hold, chunk_edges_come_from_statistics (+4) |
| `migkit/upgrade.py` | - | 1648 | 11: case, cli, cloudseed, editions, engines.mysql (+6) | decision 4, integrations 4, reporting 2, safety 1, workflows 13 | initdb, pg_upgrade | 6: an_upgrade_goes_to_the_new_version, an_upgrade_in_place_on_the_target_pg, cloud_readiness_topology (+3) |
| `migkit/users.py` | - | 4548 | 11: capabilities, cli, engines.cassandra, engines.kafka, engines.mongodb (+6) | configuration 6, decision 4, engines 25, reporting 2 | - | 28: mongo_fake.py, a_kafka_check_names_what_a_topic_can_differ_in, a_kafka_hop_reads_only_the_topics_it_takes (+25) |
| `migkit/verifiers.py` | - | 1551 | 17: cli, engines.cassandra, engines.distmysql, engines.dynamodb, engines.firestore (+12) | conversion 5, decision 7, engines 9, execution 5, integrations 4, reporting 6, workflows 4 | datacompy, dvt, pg_dump, pg_restore, reladiff | 12: an_engine_asks_its_workflow_through_a_hook, cdc_watermark_inflight_fencing, change_windows_name_when_a_tail_diverges (+9) |
| `migkit/verify_method.py` | - | 2416 | 1: cutover | decision 1, persistence 1, workflows 1 | - | 1: verify_method_proof_strength |
| `migkit/work_granularity.py` | - | 1420 | 0 | conversion 1, decision 2, safety 1 | - | 1: work_granularity_distributions |

## cli (4 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/cli.py` | - | 7770 | 10: engines.base, engines.oracle_pump, estate, ha, integrations (+5) | cli 8, configuration 28, conversion 5, decision 87, engines 23, execution 31, integrations 4, persistence 36, reporting 52, safety 27, scope 3, web 1, workflows 98 | - | 245: conftest.py, cosmos_fake.py, cutover_harness.py (+242) |
| `migkit/estate.py` | - | 365 | 1: cli | cli 1, configuration 3, decision 4, engines 1, integrations 1, reporting 2 | - | 1: the_estate_is_discovered_sized_and_priced |
| `migkit/guide.py` | - | 905 | 5: cli, init, managed, tools.gen_docs, web.newhop | configuration 19, conversion 1, decision 8, engines 1, integrations 2, persistence 1, reporting 4 | - | 11: drain_to_position_and_the_approval_gate, every_command_ends_in_its_own_words, every_goal_and_mode_is_explained (+8) |
| `migkit/ui.py` | - | 1424 | 4: cli, tools.gen_docs, web.app, web.views | configuration 7, decision 2, execution 1, persistence 5, reporting 4, web 2, workflows 6 | - | 14: a_tail_ends_its_batches_at_commits_and_says_when, alerts_and_notifications, every_resource_in_one_model (+11) |

## web (30 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/web/__init__.py` | - | 25 | 2: cli, web.app | web 1 | - | 11: a_quiet_tail_is_told_on_every_source, a_run_writes_every_step_back_as_it_ends, drain_and_readiness_are_wired (+8) |
| `migkit/web/api.py` | - | 619 | 2: tools.gen_docs, web.app | configuration 2, decision 1, persistence 3, reporting 1, web 22, workflows 4 | - | 2: a_run_writes_every_step_back_as_it_ends, web_pages_show_outcomes_never_a_program |
| `migkit/web/app.py` | - | 884 | 1: web.server | cli 1, configuration 2, decision 1, persistence 1, web 34 | jinja2, werkzeug | 1: web_stand_ins.py |
| `migkit/web/approve.py` | - | 207 | 4: approvals, web.api, web.app, web.views | persistence 3, web 5 | - | through approvals, web.api (+2) |
| `migkit/web/identity.py` | - | 1101 | 3: ui, web.app, web.approve | decision 1, safety 1, web 4 | argon2-cffi, authlib, joserfc, signxml | 5: a_quiet_tail_is_told_on_every_source, the_views_sign_in_decrypts_signs_and_logs_out, web_pages_show_outcomes_never_a_program (+2) |
| `migkit/web/newhop.py` | - | 254 | 1: web.app | cli 1, configuration 5, reporting 1, safety 1, web 1 | - | 1: web_a_hop_is_set_up_in_the_view |
| `migkit/web/roles.py` | - | 284 | 7: hopkeys, tools.gen_docs, ui, web.api, web.app (+2) | configuration 1 | - | 1: web_roles_are_given_by_the_hops_file |
| `migkit/web/runner.py` | - | 70 | 1: web.runs | cli 1, configuration 1, persistence 2, reporting 3 | - | 2: web_a_hop_is_set_up_in_the_view, web_runs_are_the_command_lines_own |
| `migkit/web/runs.py` | - | 422 | 5: tools.gen_docs, web.api, web.app, web.newhop, web.views | configuration 1, persistence 6, reporting 3, safety 1, web 1, workflows 1 | - | 4: drain_and_readiness_are_wired, web_a_hop_is_set_up_in_the_view, web_pages_show_outcomes_never_a_program (+1) |
| `migkit/web/scim.py` | - | 552 | 1: web.app | - | scim2-filter-parser | through web.app |
| `migkit/web/server.py` | - | 62 | 1: web | configuration 2, execution 1, web 2 | cheroot | through web |
| `migkit/web/settings.py` | - | 220 | 2: web.approve, web.server | configuration 3 | - | 2: web_is_safe_by_default, web_stand_ins.py |
| `migkit/web/static/web.css` | - | 72 | 0 | - | - | - |
| `migkit/web/static/web.js` | - | 140 | 0 | - | - | - |
| `migkit/web/store.py` | - | 336 | 3: web.app, web.approve, web.identity | persistence 1 | - | through web.app, web.identity |
| `migkit/web/templates/account.html` | - | 40 | 0 | - | - | - |
| `migkit/web/templates/admin.html` | - | 45 | 0 | - | - | - |
| `migkit/web/templates/api.html` | - | 16 | 0 | - | - | - |
| `migkit/web/templates/base.html` | - | 36 | 0 | - | - | - |
| `migkit/web/templates/decisions.html` | - | 18 | 0 | - | - | - |
| `migkit/web/templates/error.html` | - | 8 | 0 | - | - | - |
| `migkit/web/templates/evidence.html` | - | 33 | 0 | - | - | - |
| `migkit/web/templates/home.html` | - | 28 | 0 | - | - | - |
| `migkit/web/templates/hop.html` | - | 220 | 0 | - | - | - |
| `migkit/web/templates/newhop.html` | - | 53 | 0 | - | - | - |
| `migkit/web/templates/run.html` | - | 22 | 0 | - | - | - |
| `migkit/web/templates/setup.html` | - | 19 | 0 | - | - | - |
| `migkit/web/templates/signin.html` | - | 34 | 0 | - | - | - |
| `migkit/web/uses.py` | - | 101 | 1: tooluse | integrations 1 | argon2-cffi, authlib, cheroot, jinja2, joserfc, scim2-filter-parser, signxml, werkzeug | through tooluse |
| `migkit/web/views.py` | - | 530 | 2: web.api, web.app | cli 1, configuration 8, decision 5, persistence 2, reporting 6, safety 6, web 5, workflows 1 | - | 6: a_run_writes_every_step_back_as_it_ends, drain_and_readiness_are_wired, web_approvals_and_offers_are_answered_in_the_view (+3) |

## tools (13 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `tools/audit_backlog_index.py` | - | 153 | 0 | configuration 2 | - | none |
| `tools/check_merge_gate.py` | - | 74 | 0 | - | - | none |
| `tools/check_no_secrets.py` | - | 107 | 0 | configuration 1 | - | none |
| `tools/ease_measure.py` | - | 280 | 0 | configuration 1 | - | none |
| `tools/final_suite.py` | - | 859 | 0 | configuration 7, integrations 1, tools 1 | - | none |
| `tools/gen_changelog.py` | - | 120 | 1: tools.gen_docs | - | - | none |
| `tools/gen_codemap.py` | - | 2086 | 1: tools.gen_docs | configuration 4, decision 2, engines 1, workflows 1 | - | none |
| `tools/gen_docs.py` | - | 1782 | 0 | cli 6, configuration 25, conversion 2, decision 8, engines 5, reporting 6, tools 2, web 4, workflows 1 | - | none |
| `tools/measures_table.py` | - | 42 | 0 | workflows 1 | - | none |
| `tools/pin_programs.py` | - | 239 | 0 | integrations 1 | - | none |
| `tools/whole_use.py` | - | 353 | 0 | configuration 2, integrations 1 | - | none |
| `tools/whole_use_catalogue.json` | - | 820 | 0 | - | - | - |
| `tools/with_docker_lock.py` | - | 280 | 1: tools.final_suite | configuration 3 | - | none |

## data (5 files)

| file | owner | lines | imported by | reaches | wraps | tests |
|---|---|---|---|---|---|---|
| `migkit/data/instances.json` | - | 204 | 0 | - | - | - |
| `migkit/data/managed.yaml` | - | 3873 | 0 | - | - | - |
| `migkit/data/programs.lock.json` | - | 384 | 0 | - | - | - |
| `migkit/data/requirements.txt` | - | 3676 | 0 | - | - | - |
| `migkit/data/second-reader.txt` | - | 2673 | 0 | - | - | - |
