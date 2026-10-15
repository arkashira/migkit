# Result presentation compared with the paid verification products (2026-10-16)

Research only. Moved out of the report (`result_scope.py`) on 2026-10-16: user-facing
text never names a product or tool, and a comparison hard-coded as "proven" is not
something a run measured. The ratings are from each product's public documentation
at the version named; `present`/`absent` mean documented present or documented absent.
Companion to `result-presentation-2026-10-06.md`.

## Dimensions

- `scope_preview`
- `mappings_filters`
- `database_grouping`
- `progress_vs_correctness`
- `incomplete_unsupported_states`
- `diff_categories`
- `consistency_guarantees`
- `next_actions`
- `structured_export`
- `detail_navigation`
- `unmatched_dimensions`

## Oracle GoldenGate Veridata (12.2.1)

Source: https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDUG/working_with_jobs.htm  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Job and group comparison pairs |
| mappings_filters | present | Partition and column-level filters |
| database_grouping | present | Hierarchy by compare pairs and groups |
| progress_vs_correctness | absent | Job status often mixed with sync status |
| incomplete_unsupported_states | present | Out-of-sync and unconfirmed states reported |
| diff_categories | present | Out-of-sync row records with source and target values |
| consistency_guarantees | present | In-flight confirm step via GoldenGate trail checks |
| next_actions | absent | Manual out-of-sync report review without automated fix hints |
| structured_export | present | XML and CLI report generation |
| detail_navigation | present | Drill-down to out-of-sync column values |
| unmatched_dimensions | absent | No bidirectional drift proofs or schema invariant barriers |

## Informatica Cloud Data Validation (July 2024)

Source: https://docs.informatica.com/content/dam/source/GUID-B/GUID-BE90AF74-E490-4BA6-9EC6-2D9B079C5D7B/7/en/CDV_July2024_Monitor_en.pdf  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Suite and test case scope configuration |
| mappings_filters | present | Table mapping and SQL expression filters |
| database_grouping | absent | Grouped by test suite, not natural database boundaries |
| progress_vs_correctness | present | Execution monitor separated from assertion results |
| incomplete_unsupported_states | present | Unchecked, aborted and skipped tests distinguished |
| diff_categories | present | Matched, unmatched, missing, extra records |
| consistency_guarantees | absent | Point-in-time snapshot query only; no live replication proof |
| next_actions | absent | Requires user investigation in source tools |
| structured_export | present | REST API and PDF/CSV execution summaries |
| detail_navigation | present | Drill-down from test case to row differences |
| unmatched_dimensions | absent | No autoinc headroom, artifact separation, or drift proofs |

## Qlik Qlik Replicate (May 2025)

Source: https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Tutorial/RunMonitorTask_sample.htm  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Table selection list and pattern include/exclude |
| mappings_filters | present | Column transformation rules and record filtering |
| database_grouping | absent | Task-centric table list rather than database-grouped summary |
| progress_vs_correctness | absent | Primarily replication progress; validation verification is separate add-on |
| incomplete_unsupported_states | present | Error, queued, and loading states distinct |
| diff_categories | evidence_unavailable | Verification details require Qlik Enterprise Manager |
| consistency_guarantees | present | Transactional CDC ordering supported |
| next_actions | absent | Error flags without direct repair command generators |
| structured_export | present | Enterprise Manager REST API |
| detail_navigation | present | Table progress screen drilldown |
| unmatched_dimensions | absent | No schema invariant barriers or sequence headroom checks |

## Redgate SQL Data Compare (15.x)

Source: https://documentation.red-gate.com/sdc15/reviewing-the-comparison-results/viewing-the-row-differences  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Object mapping list and WHERE clause filters |
| mappings_filters | present | Custom comparison keys and mapped tables |
| database_grouping | present | Database pair root context |
| progress_vs_correctness | present | Comparison phase progress separated from grid diff |
| incomplete_unsupported_states | present | Identical, different, and unable to compare groups |
| diff_categories | present | Source only, target only, and different row records |
| consistency_guarantees | absent | Static table comparison; no CDC or live drift guarantees |
| next_actions | present | Generates deployment synchronization scripts |
| structured_export | present | CLI XML and HTML export |
| detail_navigation | present | Side-by-side row viewer with highlighted column diffs |
| unmatched_dimensions | absent | No live drift proofs or replication artifact partitioning |

## Devart dbForge Data Compare (5.x)

Source: https://docs.devart.com/data-compare-for-mysql/view-comparison-results/view-comparison-results.html  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Object mapping view with schema filter |
| mappings_filters | present | Include/exclude checkboxes and custom filters |
| database_grouping | present | Target/source schema pair root |
| progress_vs_correctness | present | Comparison wizard completion separate from diff grid |
| incomplete_unsupported_states | present | Objects without keys marked incomparable |
| diff_categories | present | Different, only in source, only in target, identical |
| consistency_guarantees | absent | Point-in-time comparison without replication lock proof |
| next_actions | present | Data synchronization script generation wizard |
| structured_export | present | HTML, Excel, XML, and CSV reports |
| detail_navigation | present | Object grid to bottom row data pane |
| unmatched_dimensions | absent | No live replication barriers or artifact separation |

## Datafold Datafold Cloud (2024)

Source: https://www.datafold.com/blog/what-the-heck-is-data-diffing/  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Model and table diff overview in web UI and CI PR comments |
| mappings_filters | present | Primary key configuration and column selection |
| database_grouping | absent | dbt model/table flat list rather than database boundaries |
| progress_vs_correctness | present | CI check execution state distinct from diff findings |
| incomplete_unsupported_states | present | Unsupported column types and timed-out diffs stated |
| diff_categories | present | Exclusive rows, shared rows with value differences |
| consistency_guarantees | absent | Checksum hashing against warehouse tables; no transactional CDC proof |
| next_actions | absent | Highlights differences without remediation scripts |
| structured_export | present | JSON API, CLI, and GitHub PR comments |
| detail_navigation | present | Interactive web diff with column breakdown |
| unmatched_dimensions | absent | No engine-level sequence headroom or runtime config invariants |

## QuerySurge QuerySurge (2024)

Source: https://www.querysurge.com/product-tour/features  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | QueryPairs organized in test suites |
| mappings_filters | present | Source and target custom SQL queries |
| database_grouping | absent | Project/Suite hierarchy rather than database-first grouping |
| progress_vs_correctness | present | Execution status (pass/fail/killed) distinct from diff counts |
| incomplete_unsupported_states | present | Killed, warning, and syntax error states explicit |
| diff_categories | present | Row count difference, column failure, data mismatch |
| consistency_guarantees | absent | Independent JDBC queries without multi-table lock or proof |
| next_actions | absent | Pure QA testing platform; no automated migration repairs |
| structured_export | present | REST API, CLI, and automated email/HTML reports |
| detail_navigation | present | Drilldown to mismatched cells in tabular grid |
| unmatched_dimensions | absent | No schema invariant DAGs, artifact checks, or drift proofs |

## IBM Data Replication asntdiff (10.2.1)

Source: https://www.ibm.com/docs/en/idr/10.2.1?topic=tables-table-compare-utility-asntdiff  

| dimension | documented | notes |
|---|---|---|
| scope_preview | absent | Runs per subscription or table pair; no global scope preview |
| mappings_filters | present | WHERE clause predicate support |
| database_grouping | absent | Single table comparison execution per command |
| progress_vs_correctness | present | Return codes distinguish execution failure from data mismatch |
| incomplete_unsupported_states | present | Return code 1 indicates differences, 2 indicates error |
| diff_categories | present | Writes differences to a dedicated diff table |
| consistency_guarantees | present | Coordinates with Q Capture commit sequence numbers |
| next_actions | present | Pairs with asntrep for SQL repair generation |
| structured_export | present | Diff stored in SQL table for query extraction |
| detail_navigation | absent | Requires SQL queries against diff table; no unified UI |
| unmatched_dimensions | absent | No automatic cross-engine translation or invariant barriers |

## Amazon Web Services AWS Database Migration Service (3.5+)

Source: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Table mapping JSON preview in console |
| mappings_filters | present | Transformation rules, schemas, and table filters |
| database_grouping | absent | Flat table list under task; no multi-database grouping |
| progress_vs_correctness | present | Validation pending, validating, and completed states separate |
| incomplete_unsupported_states | present | Validation suspended, no primary key, error states |
| diff_categories | present | Mismatched records, records with errors, deleted records |
| consistency_guarantees | absent | Eventual validation during CDC; prone to false positives under write load |
| next_actions | absent | Leaves `awsdms_apply_exceptions` table without auto-repair scripts |
| structured_export | present | AWS CloudWatch metrics and DescribeTableStatistics API |
| detail_navigation | absent | AWS console only shows error counts; requires manual query on target |
| unmatched_dimensions | absent | Does not clean up or isolate its own bookkeeping artifacts |

## Microsoft Azure Azure Database Migration Service (2024)

Source: https://learn.microsoft.com/en-us/azure/dms/tutorial-sql-server-to-azure-sql  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Database and table selection screen with preflight validation |
| mappings_filters | present | Database name remapping and table selection |
| database_grouping | present | Database level overview under project |
| progress_vs_correctness | present | Migration step state distinct from data validation outcome |
| incomplete_unsupported_states | present | Schema mismatch and unsupported data types flagged in preflight |
| diff_categories | present | Row count mismatches and schema discrepancy reports |
| consistency_guarantees | present | Transaction log restore provides high consistency for SQL Server |
| next_actions | absent | Points to remediation documentation rather than direct SQL fixes |
| structured_export | present | Azure Monitor and Resource Manager REST APIs |
| detail_navigation | present | Activity blade displays database and table summary |
| unmatched_dimensions | absent | No bidirectional drift proofs or auto-increment headroom proofs |

## Google Cloud Database Migration Service (2024)

Source: https://cloud.google.com/database-migration-service/docs  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Connection profile and migration job configuration summary |
| mappings_filters | present | Schema conversion rules and database mapping |
| database_grouping | present | Database level overview for heterogeneous migrations |
| progress_vs_correctness | present | Migration phases (Full Dump, CDC) distinct from verification |
| incomplete_unsupported_states | present | Unsupported flags in preflight assessment |
| diff_categories | absent | Relies on external verification or basic row count comparisons |
| consistency_guarantees | present | AlloyDB/Cloud SQL replication consistency |
| next_actions | present | Schema conversion suggestions provided in workspace |
| structured_export | present | Cloud Logging, Cloud Monitoring, and REST API |
| detail_navigation | present | Migration job details page |
| unmatched_dimensions | absent | No continuous mathematical hash drift proofs |

## Tencent Cloud Data Transmission Service (DTS) (2024)

Source: https://intl.cloud.tencent.com/document/product/571/18765  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Migration object selection with regex and wildcard filtering |
| mappings_filters | present | DB/table renaming and column selection |
| database_grouping | present | Database level tree view with object counts |
| progress_vs_correctness | present | Independent Data Verification task separate from migration task |
| incomplete_unsupported_states | present | Non-comparable tables (no PK) explicitly listed |
| diff_categories | present | Inconsistent row count, content diff, and unverified tables |
| consistency_guarantees | present | Static verification or incremental verification with compensation |
| next_actions | present | Generates data repair SQL scripts for inconsistent rows |
| structured_export | present | Tencent Cloud API and downloadable diff CSV files |
| detail_navigation | present | Object drilldown with primary key values of differing rows |
| unmatched_dimensions | absent | No replication artifact isolation (creates `dts_*` tables) |

## Alibaba Cloud Data Transmission Service (DTS) (2024)

Source: https://www.alibabacloud.com/help/en/dts/user-guide/data-verification  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Object mapping and verification scope configuration |
| mappings_filters | present | Table renaming and conditional WHERE clauses |
| database_grouping | present | Database and schema grouped overview |
| progress_vs_correctness | present | Data verification progress distinct from migration pipeline |
| incomplete_unsupported_states | present | Inconsistent, identical, and failed verification states |
| diff_categories | present | Count inconsistency and content discrepancy |
| consistency_guarantees | present | Dual-sampling verification during incremental CDC |
| next_actions | present | Automatic repair SQL statements generated for download |
| structured_export | present | OpenAPI and console report download |
| detail_navigation | present | Detailed record diff view in DTS console |
| unmatched_dimensions | absent | Leaves tracking tables on target; no schema barrier proofs |

## Huawei Cloud Data Replication Service (DRS) (2024)

Source: https://support.huaweicloud.com/intl/en-us/drs/index.html  

| dimension | documented | notes |
|---|---|---|
| scope_preview | present | Migration flow wizard with object tree selector |
| mappings_filters | present | Database, table and column level mapping rules |
| database_grouping | present | Database-grouped object verification hierarchy |
| progress_vs_correctness | present | Data comparison jobs run alongside or post-migration |
| incomplete_unsupported_states | present | Tables without primary keys or unsupported data types flagged |
| diff_categories | present | Row count difference and row detail mismatch |
| consistency_guarantees | present | Dynamic comparison with CDC log compensation |
| next_actions | present | Exportable repair scripts for mismatched data |
| structured_export | present | REST API and downloadable comparison logs |
| detail_navigation | present | Table and row-level diff drilldown in management console |
| unmatched_dimensions | absent | No bidirectional multiset hash proofs or DDL invalidation barriers |

## What the report says that these products do not

The checks below are reported by migkit as findings of their own; the products
above document no equivalent.

- `bidirectional_drift_proof` (mathematical_proof): Continuous mathematical multiset hash proof proving zero drift in live bidirectional replication without stopping writes.
- `schema_invariant_barriers` (verified_barrier): Topological dependency DAG and cycle barrier verification preventing FK violations and unsafe cutover ordering.
- `replication_artifact_partitioning` (verified_barrier): Strict isolation of mover bookkeeping artifacts (e.g. awsdms_*, debezium_*, dts_*) from application defects with enforced repair guards.
- `sequence_autoinc_headroom` (mathematical_proof): Verification that target sequence next values exceed source high-water marks to guarantee zero duplicate key collisions upon write cutover.
- `routine_event_semantic_parity` (verified_barrier): Dialect-aware semantic and execution compatibility checks for procedures, functions, triggers, and scheduled events.
- `runtime_parameter_invariants` (heuristic_warning): Cross-engine verification of timezones, character encodings, collations, and transaction isolation levels.
- `proof_invalidation_ddl_boundary` (verified_barrier): Detection of schema alterations during verification runs, invalidating prior checksums and preventing false-positive pass verdicts.
