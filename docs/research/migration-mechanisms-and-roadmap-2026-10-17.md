> Morning integration update, 2026-10-17: this report's earlier source inspection and tests are historical. The current checkout advanced; see user-needs-and-acceptance-2026-10-17.md for 28 new user-need scenarios, current baseline and the complete 100-entry integration. Product claims retain their source/version caveats.

# migkit: deep migration-mechanism research and decision-engine roadmap

Research cut-off: 2026-10-17, Asia/Bangkok. Owner goal: configure a hop with database endpoints and credentials, then have migkit own technical discovery, planning, movement, continuous adaptation, verification, recovery and completion within a previously specified intent/authorization policy.

This report integrates the earlier five confirmed baseline defects, two test/coverage debts and 25 intelligence proposals with 40 additional research-derived mechanism work items. **72 actionable backlog entries**, each with acceptance criteria. This is documentation and a patch for review/implementation; it does not implement the features or certify that the gaps are closed.

The source checkout is being modified concurrently by another worker. HEAD observed was 750b38a5; source and backlog-index working changes accumulated during research. Historical bug reproductions and the 244-pass/11-fail focused run remain baseline evidence, not a verdict on subsequent fixes. Revalidate each before implementation. In particular AUTO.target-tune, AUTO.deferred-index and AUTO.proxy-shift changed to DONE in the working index during this pass; research items extend their contracts/proof rather than reimplement them.

## 1. Evidence and bounded completeness

The dossier covers **55 named product/workflow surfaces** across managed migrators, enterprise replication/validation, conversion/modernization, ingestion services and engine-native migration systems. The registry retains **151 distinct retrieved official/vendor URLs**, 67 of which have successful page retrieval records; other entries are explicitly official-search-excerpt evidence. A page retrieval is not a claim that every linked manual page was read. Five failed opens and a legacy documentation redirect are tracked instead of filled in from memory.

D0 = vendor overview/indexed excerpt only; D1 = a specific documented mechanism, possibly versioned; D2 = several workflow/limitation sources cross-checked. No D3 claim (vendor product experimentally reproduced) is made. Some dossiers intentionally remain D0/limited. The exact source and caveat are retained on every dossier.

This is a substantial, bounded pass, **not every commercial product or every private implementation in existence**. Proprietary internal scheduling algorithms, licensed-only manuals, exact provider internals and untested connector/version combinations remain unknown. The goal is an auditable mechanism inventory, not a claim of exhaustive market coverage. Official docs can contradict older pages or differ by region/product generation. Historical versions remain labelled; Preview, Early Access and Planned never mean GA.

Sources were retrieved anew; the repository's September research was used to locate prior work and avoid duplicates, not as sole evidence for current product facts. No benchmark number or vendor marketing speedup is adopted as independent performance evidence.

## 2. What the decision engine already has

Local implementation inspection found: decide.Rung/Need/Facts/climb/space/attempt, correctness-first pruning, explicit guarantee ordering, measured-versus-predicted costs, bounded Trials and retry/fallback; planner/tableway share table choices; costmodel fits unit-shape costs with uncertainty and cross-hop history; jointplan simulates shared resources and selects ways/order/workers; sizing adjusts concurrency; autonomous composes phases and reconciles open effects; repairways selects rows/spans/reload with undo and fresh proof; unchanged refuses doubtful cache markers and does not skip final proof; storedcode/prove executes routines on aligned sandbox inputs and records value/effect/coverage evidence; storedcode/mutants tests the quality of that proof.

These are meaningful foundations. Research must extend them, not create independent optimizers or another autonomously named CLI. P0-MINIMAL-INPUT, P0-WORKFLOW-DEPTH, P0-SMART, P0-DECISION-ALL and P0-RESILIENCE-ALL already own related work. Existing capability/depth tables show substantial engine-specific proof debt. A method with a source-code body or a PROVES declaration is not automatically real-engine proof.

Observed focused test result before concurrent changes: 244 passed, 11 failed, 11 Docker/VM cases deselected across 16 files. Two failures were the selected-guarantee evidence bug; one exposed 27 undeclared engine/shape cells; eight involved MySQL table-way fixtures missing new packet/row-width probe facts. Runtime probes for hard-limit admission, proof facts, failed runner promotion and a missing bytes budget were separately reproduced in-process. The patch retains reproduction detail and requires revalidation.

## 3. The complete decision problem

Optimize **time and cost to a verified usable target**, not raw copy MB/s. Required scope/proof/consistency/idempotence/source impact and authorization are admission constraints. Once admissible, compare total expected completion, calibrated tail risk and operating cost; record tradeoffs and unmeasured assumptions. No strategy can be universally fastest, cheapest and strongest when resource/correctness dimensions conflict. Expose minimum guarantees, retain useful nondominated options and never silently relax the goal.

Decision levels must connect: estate and physical shared resources -> hop/topology -> database/phase -> object -> key range/transaction -> request/batch/row/LOB -> failure and recovery -> verification and final cutover. A decision at one level must carry the same contract to its children. The optimizer's own CPU/probe time is part of the cost.

Every action contract should declare: input/output types; scope; prerequisites; facts and validity epochs; effects and visibility; durability/checkpoint semantics; proof requirements; resources/cost distribution; restart/reconcile/compensation rules; target identity; and the authority under which it can act. A technically possible path is not eligible if it lacks any required property.

## 4. Product and workflow dossiers

Each entry separates documented behavior, limitations/evidence boundary and the proposed migkit response. Product behavior is source-derived; migkit responses are design conclusions, not a vendor roadmap or a claim of current implementation.

### 1. AWS DMS classic — D2

**Observed:** Full-load/CDC tasks, validation and scheduled resync form distinct operations. Resync reads target validation-failure records, re-fetches source keys and applies configured transforms; CDC resync pauses replication/validation.

**Boundary:** Resync documentation requires 3.6.1+, Oracle/SQL Server sources and PostgreSQL/Aurora PostgreSQL targets; keyed-table and scheduling restrictions apply.

**migkit response:** Extend repairways with per-range CDC exclusion/fences, atomic repair provenance and mandatory revalidation. Preserve current strict verdict semantics; do not confuse engine-version support with every pair.

Sources: [S109](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html), [S143](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.DataResync.html), [S144](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html).

### 2. AWS DMS homogeneous — D1

**Observed:** Serverless same-engine migration uses native dump/restore tools and can carry partitions and secondary objects.

**Boundary:** This path explicitly lacks built-in validation; PostgreSQL views may be materialized as tables; ongoing schema handling differs by source engine.

**migkit response:** Cost native movement as one candidate and independently prove object-kind/value/security coverage. A native fast path cannot inherit the generic verifier's unperformed checks.

Sources: [S142](https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html).

### 3. AWS DMS Schema Conversion — D1

**Observed:** Assessment inventories automated versus manual conversion; conversion supports rules, AI assistance, review/export/apply and extension packs.

**Boundary:** Schema conversion is separate from data movement. The presence of converted SQL or emulation packs is not runtime equivalence proof.

**migkit response:** Bind assessment objects, conversion provenance, deployed objects and executable tests in one ledger; admit generated candidates only after the existing storedcode proof harness.

Sources: [S145](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_SchemaConversion.html).

### 4. Azure PostgreSQL migration service — D2

**Observed:** The current flexible-server service provides premigration validation and offline/online workflows; online migration uses pgcopydb follow.

**Boundary:** Current guidance imposes finite migration/cutover lifetimes and asks for manual post-migration data/object checks. Old Azure DMS PostgreSQL pages describe a different service.

**migkit response:** Select by actual service generation; model window expiry, logical-decoding/key prerequisites and independent post-copy proof, not only a completed table counter.

Sources: [S018](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-known-issues-migration-service?source=recommendations), [S089](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/overview-migration-service-postgresql?source=recommendations&tabs=portal%2Coffline), [S019](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql), [S017](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-premigration-migration-service).

### 5. Azure MySQL DMS — D1

**Observed:** Online migration includes data and selected non-table objects with ROW-binlog requirements.

**Boundary:** Documented DEFINER handling can replace source definers with the migration login; schema-only selection and database-rename constraints exist.

**migkit response:** Treat definer/invoker/ownership and application-role execution as semantic parity, not cosmetic metadata. Separate schema scope from row-copy scope.

Sources: [S151](https://learn.microsoft.com/en-us/azure/dms/tutorial-mysql-azure-external-to-flex-online-portal).

### 6. Azure SQL Managed Instance LRS — D2

**Observed:** Log Replay Service restores native SQL Server backup/log chains; DMS and LRS share underlying migration technology/APIs. Explicit final-backup completion ends replay.

**Boundary:** Native restore eligibility, log-chain continuity and service-tier constraints differ from generic SQL row movement.

**migkit response:** Choose physical/log replay from measured eligibility and total recovery/cutover cost; verify chain/restore points and expose independent evidence of a usable target.

Sources: [S091](https://learn.microsoft.com/th-th/Azure/Azure-sql/managed-instance/log-replay-service-overview?view=azuresql), [S092](https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/log-replay-service-migrate?view=azuresql), [S093](https://learn.microsoft.com/cli/azure/sql/midb/log-replay).

### 7. Google DMS data migration — D2

**Observed:** Supports homogeneous and heterogeneous migration flows. The Oracle heterogeneous documentation describes eventual consistency and alignment after source writes stop and buffers drain.

**Boundary:** Users/permissions and active schema changes are outside specified paths; SQL Server instance-level jobs/logins/certificates have exclusions. Transaction consistency cannot be assumed uniformly.

**migkit response:** Make transaction visibility, security objects and DDL explicit per-path contracts; compare at a common fence and include application dependencies.

Sources: [S110](https://docs.cloud.google.com/database-migration/docs/oracle-to-postgresql/known-limitations), [S148](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/known-limitations), [S016](https://docs.cloud.google.com/database-migration/docs/release-notes).

### 8. Google conversion/Gemini — D2

**Observed:** Conversion workspaces combine deterministic conversion with AI-assisted fixes, explanations and quality assessment; fixes can influence similar object suggestions.

**Boundary:** Quality assessments apply only to eligible AI-converted objects, exclude schema objects and do not include performance testing.

**migkit response:** Use AI for proposal/search, existing executable differential/mutation testing for acceptance, and a separate workload-performance gate. Avoid treating an AI score as equivalence.

Sources: [S147](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/code-conversion-with-gemini), [S150](https://docs.cloud.google.com/database-migration/docs/convert-sql-with-dms).

### 9. Google failback — D2

**Observed:** Documented Preview failback prepares reverse CDC before promotion and then reads PostgreSQL changes back toward the original SQL Server.

**Boundary:** No full backfill/new-table replication/schema changes; keyless behavior can be append-only; point-in-time transaction consistency is not maintained by that path.

**migkit response:** Prepare reverse intervals and prove invertible mappings before promotion; disallow blanket zero-loss rollback claims when keys/types/transactions or new objects cannot reverse.

Sources: [S146](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/failback-migration), [S016](https://docs.cloud.google.com/database-migration/docs/release-notes).

### 10. Tencent DTS — D2

**Observed:** Built-in checksum flow writes checksums on the source and replays computation via binlog; independent comparison queries slices on both sides with mapping rules. Release notes add account-conflict policies, GTID checks and replay-position controls.

**Boundary:** Built-in validation excludes advanced objects and target-only writes; source-read-only and DDL restrictions apply. Independent and built-in keyless thresholds differ by path/document.

**migkit response:** Retain source-read-only verification, separate scope/coverage accounting and epoch validity. Add replay-position reconciliation and account-policy semantics rather than copy the brand's consistent label.

Sources: [S094](https://www.tencentcloud.com/document/product/571/42724/), [S096](https://cloud.tencent.com/document/product/571/85797), [S112](https://proxy-hk.tencentcloud.com/ind/document/product/571/42597).

### 11. Alibaba DTS — D2

**Observed:** Offers schema/full/incremental verification. Exactly-once keyless support uses transactional destination metadata in specified synchronization scenarios.

**Boundary:** Keyless verification thresholds differ from keyless write support. Exactly-once has deployment, size/throughput and DDL restrictions; source tables may lock.

**migkit response:** Separate write-idempotence capability from proof coverage; choose atomic checkpoint/effect strategies per engine and bound metadata/retention/locking costs.

Sources: [S010](https://www.alibabacloud.com/help/en/dts/user-guide/enable-data-verification), [S011](https://www.alibabacloud.com/help/en/dts/user-guide/synchronize-tables-without-primary-keys-and-unique-constraints-from-the-source-database), [S013](https://www.alibabacloud.com/help/en/dts/user-guide/view-data-verification-details).

### 12. Huawei DRS — D2

**Observed:** Migration/synchronization/DR paths expose object, row-count and value comparisons; a DR path also documents account comparison.

**Boundary:** Capabilities depend on direction/database and comparison type. Cold/off-peak comparison advice does not establish a common snapshot by itself.

**migkit response:** Extend the coverage contract to objects/accounts as well as data; choose exact comparison and temporal alignment from workload instead of fixed count-then-assume logic.

Sources: [S014](https://support.huaweicloud.com/intl/en-us/realtimesyn-drs/drs_10_0012.html), [S097](https://support.huaweicloud.com/intl/en-us/realtimedr-drs/drs_02_0033.html), [S015](https://support.huaweicloud.com/intl/en-us/productdesc-drs/drs_01_0301.html).

### 13. Oracle GoldenGate — D2

**Observed:** Integrated apply builds logical records from trails, computes constraint-derived dependencies and parallelizes independent transactions with DDL barriers; ACDR uses before images and database-maintained metadata.

**Boundary:** Integrated apply is Oracle-specific and not an initial-loader; auto-CDR and parallelism capabilities differ by mode/target. Index/logging prerequisites matter.

**migkit response:** Extend existing transaction DAG and conflict-policy work with explicit native versus generic guarantees, commit-prefix visibility and targeted dependency probes.

Sources: [S001](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/replicat-integrated-replicat.html), [S002](https://docs.oracle.com/en/middleware/goldengate/core/21.3/oracle-db/configuring-oracle-goldengate-apply.html), [S003](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/administer-automatic-cdr.html?source=%3Aso%3Ach%3Aor%3Aawr%3A%3A%3A).

### 14. Oracle Veridata — D2

**Observed:** Two-stage comparison hashes normalized values, queues possible divergence, then confirms out-of-sync rows while transactions/replication continue.

**Boundary:** Comparison parallelism consumes server/memory resources. A first-pass mismatch during replication is not automatically a persistent defect.

**migkit response:** Extend current convergence logic with position-aware confirmation and independently scheduled proof, measuring source cost and time to definitive results.

Sources: [S098](https://docs.oracle.com/en/database/goldengate/core/26/gvdug/how-oracle-goldengate-veridata-works.html), [S004](https://docs.oracle.com/en/middleware/goldengate/veridata/23/gvdug/oracle-goldengate-veridata.pdf).

### 15. Qlik Replicate — D2

**Observed:** Captures changes while tables load, then applies cached changes. Transactional and batch apply have different ordering/integrity and endpoint eligibility. May 2026 notes expand endpoints and native handling.

**Boundary:** Keyless rows and unsupported targets constrain batching; recovery and target constraints affect correctness. Faster batching is not a universal drop-in mode.

**migkit response:** Make mode-specific guarantees and complete-path costs first-class; test target constraint absence, cached-event replay and barrier handling.

Sources: [S007](https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm), [S008](https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Introduction/FullCDCProcesses.htm), [S080](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm).

### 16. Fivetran HVR — D2

**Observed:** Compare includes checksums/row-wise differences, online handling, transformations and directional target-type coercion. Refresh offers bulk/row methods and excludes concurrent Integrate work.

**Boundary:** Target semantic coercion may intentionally merge distinctions; refresh blocks and recovery controls are durable workflow concerns.

**migkit response:** Expose equivalence direction/lossiness and schedule repair/load exclusion from facts, with crash-safe release of blockers and proof of the resumed CDC interval.

Sources: [S005](https://fivetran.com/docs/hvr6/getting-started/concepts/compare), [S006](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh).

### 17. IBM CDC/IDR — D2

**Observed:** Adaptive apply can change conflicting insert/update operations; standard mapping permits throughput optimizations such as grouped/array operations.

**Boundary:** Adaptive/CDR modes can disable optimizations. Mapping normalization is not automatically permission to overwrite unexpected target state.

**migkit response:** Cost semantic apply policies separately; preserve explicit conflict intent, audit decisions and verify resulting effects under restart.

Sources: [S104](https://www.ibm.com/docs/vi/SSTRGZ_11.3.3/com.ibm.cdcdoc.mcadminguide.doc/concepts/mappingadaptiveapply.html), [S106](https://www.ibm.com/support/pages/node/1102497), [S009](https://www.ibm.com/docs/en/iirfz/11.3.0?topic=processing-adaptive-apply).

### 18. Quest SharePlex — D1-limited

**Observed:** Vendor documentation describes hidden-drift compare/repair and peer-to-peer conflict routines.

**Boundary:** Some full guide opens failed; Oracle-focused compare/repair and timestamp/identity assumptions must not be generalized to every target.

**migkit response:** Retain compare independent of touched CDC rows, policy-driven conflict semantics and repair/load cost accounting. Recheck exact-version manuals before implementing mode details.

Sources: [S020](https://www.quest.com/products/shareplex), [S021](https://support.quest.com/technical-documents/doc2094948), [S022](https://support.quest.com/technical-documents/shareplex/11.4/administration-guide/34), [S023](https://support.quest.com/technical-documents/doc1629965).

### 19. Informatica CDIR — D1-limited

**Observed:** Retrieved documentation describes recovery positions in files or a target recovery table and configurable schema-change responses.

**Boundary:** Full manual/page retrieval failed in this pass; available excerpts are version-specific and distinguish application versus database ingestion.

**migkit response:** Test initial/no-checkpoint restart explicitly; catalogue schema responses by connector and mode rather than inherit a generic self-healing claim.

Sources: [S025](https://docs.informatica.com/content/dam/source/GUID-5/GUID-564A497D-5045-4D3A-92A2-F1C5D0D1B030/50/en/CMI_November2024_DatabaseIngestionAndReplication_en.pdf), [S024](https://docs.informatica.com/integration-cloud/data-ingestion-and-replication/current-version/application-ingestion-and-replication/application-ingestion-and-replication/handling-source-schema-changes.html).

### 20. Precisely Connect CDC/SQData — D1

**Observed:** Apply-engine documentation separates transport/frameworks, normalization, exception processing and replication with compatible descriptions.

**Boundary:** Automatic relational replication relies on compatible field descriptions, name/sequence/key conditions; cross-datastore structural differences require mapping.

**migkit response:** Register description/encoding/identity prerequisites and probes for mainframe/nonrelational adapters; broad connector names alone earn no semantic support.

Sources: [S027](https://help.precisely.com/r/t/1027124403/2023-12-31/Connect-CDC-SQData/pub/Latest/en-US/Connect-CDC-SQData-Apply-engine/Replication?contentId=2sJaR~97vXktdxbV~br72A).

### 21. EDB migration stack — D1

**Observed:** Portal assessment, Toolkit snapshot movement, Replication Server and LiveCompare serve different migration phases; reverse replication supports fallback scenarios.

**Boundary:** Toolchain availability and subscriptions differ; compatibility assessment or schema-load success is not complete application proof.

**migkit response:** Compose phase-specific components with one scope/evidence model and combine code, data, role and application testing without hiding manual service dependencies.

Sources: [S028](https://www.enterprisedb.com/docs/migrating/oracle/edb_migration_tools/).

### 22. Snowflake SnowConvert/AIM — D2

**Observed:** Current docs span deterministic/AI conversion, reusable fixes, ETL conversion, data validation and BI repointing, with resumable staged migration guidance.

**Boundary:** Submitted/assessment/conversion scopes differ. Capability tables explicitly mark some systems/features Planned; scope validation is not data/runtime validation.

**migkit response:** Extend code-scope accounting and dependency waves; retain deterministic proof gates. Treat announced plans as direction evidence, never as shipped capability.

Sources: [S035](https://docs.snowflake.com/en/migrations/README), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill), [S034](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/review-results/snowconvert-scopes), [S031](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/validation/README).

### 23. Snowflake Openflow — D1

**Observed:** The PostgreSQL connector creates destination structures and journal tables with schema-versioned naming and a replication lifecycle.

**Boundary:** Journal state is operational state; destination rows/files alone cannot prove which changes are durably published.

**migkit response:** Add journal/manifest-aware warehouse commit contracts and crash reconciliation to the existing staged/streaming path chooser.

Sources: [S087](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/connectors/postgres/about).

### 24. Databricks Lakeflow Connect — D2

**Observed:** SQL Server ingestion documents incremental/SCD2 support and specific schema-evolution behavior; new-table handling depends on schema scope.

**Boundary:** Rename/type-change/backfill behavior differs from add-column support; inactive columns and refresh requirements matter. Ingestion is not application cutover.

**migkit response:** Use connector/version/mode matrices and information-content contracts; prove historical backfill and failover-position portability separately.

Sources: [S100](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview), [S101](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq), [S036](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits).

### 25. Datafold Migration Agent — D1-vendor-engineering

**Observed:** Vendor engineering describes frozen aligned source inputs, translate/diff/repeat and delivery with value-level validation.

**Boundary:** The engineering description is not an independently measured speedup or proof of all stored-code side effects.

**migkit response:** Extend existing sandbox differential proof into a bounded candidate/fix loop; use independent effect/coverage/mutation evidence, not only table-output parity.

Sources: [S099](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/).

### 26. MongoDB Relational Migrator — D2

**Observed:** The 1.18+ verifier checks mappings at field-name/type/path/value level, flattening destination documents for comparison; snapshot verification is on by default.

**Boundary:** Assumes the source does not change after job start and destination contains migrated data only; snapshot support differs from continuous modes.

**migkit response:** Extend mapping-aware continuous proof for document identity, embedded updates/deletes, multiplicity and concurrent epochs; account for verification time in method choice.

Sources: [S042](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/), [S041](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/use-data-verification/), [S043](https://www.mongodb.com/docs/relational-migrator/jobs/creating-jobs/).

### 27. Redis Software/Cloud/RDI — D2

**Observed:** Replica Of, CRDT active-active and database-to-cache CDC solve distinct problems. RDI supports configured transformations.

**Boundary:** Active-active is eventually consistent and does not itself reroute applications; full resync can replace destination state and compression can hurt replication.

**migkit response:** Select by TTL/type/conflict/routing requirements and measured CPU/network tradeoffs; fence before promotion and prevent destructive post-promotion resync.

Sources: [S045](https://redis.io/docs/latest/operate/rc/databases/active-active/), [S046](https://redis.io/docs/latest/integrate/redis-data-integration/), [S047](https://redis.io/docs/latest/operate/rs/7.8/databases/import-export/replica-of/).

### 28. Confluent Cluster Linking — D2

**Observed:** Mirrors preserve topic data/offsets with metadata/consumer-offset/ACL synchronization capabilities; migration and promotion workflows coordinate consumers.

**Boundary:** Automatic failover documentation is Early Access, not production-GA proof. Consumer/schema/auth state is distinct from copied topic bytes.

**migkit response:** Treat offsets, registry/schema, ACL and clients as cutover dependencies; require a retained-position certificate and explicit promotion state.

Sources: [S083](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html), [S085](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/cluster-links-cc.html), [S082](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-automatic-failover-tutorial.html), [S038](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/mirror-topics-cp.html).

### 29. Striim/Validata — D2

**Observed:** Recovery tracks processing positions; validation offers method choices including compact signatures and per-record hashes.

**Boundary:** Recovery can restrict parallelism for some targets. Vector validation documents a blind spot for perfect duplicate records.

**migkit response:** Cost recovery and proof method jointly. Duplicate-sensitive exact escalation and verified checkpoint/effect coupling are required before a strong verdict.

Sources: [S073](https://www.striim.com/docs/validata/en/validation-types.html), [S075](https://striim.com/docs/platform/en/recovering-applications.html).

### 30. Airbyte — D2

**Observed:** CDC connections have snapshot/state dependencies and selectable schema-propagation policies; breaking key/cursor changes pause connections.

**Boundary:** New fields/streams may need snapshots/refresh; table/connector-specific CDC limitations apply. Replicated current state is not necessarily historical event completeness.

**migkit response:** Automate only compatible scoped drift with proof, choose shadow refresh when needed and retain explicit incomplete states for untracked intervals.

Sources: [S107](https://docs.airbyte.com/platform/understanding-airbyte/cdc), [S108](https://docs.airbyte.com/platform/using-airbyte/schema-change-management).

### 31. Fivetran managed connectors — D1

**Observed:** History mode records SCD2-style state with source-method-dependent fidelity and explicit resync behavior.

**Boundary:** Polling/CT/Teleport can omit intermediate versions; history begins at a defined start, and resync changes history metadata.

**migkit response:** Separate final-state, change-history and audit requirements. Migration goals requiring all events cannot choose a mechanism that never observes them.

Sources: [S039](https://fivetran.com/docs/core-concepts/sync-modes/history-mode).

### 32. Hevo — D1

**Observed:** Schema management uses metadata and policies for compatible changes, names and destination evolution.

**Boundary:** Some discovery is data-driven; schema renames and empty new structures require care. Generic automatic evolution does not imply historical backfill.

**migkit response:** Probe metadata independently of DML when feasible, detect lossy name collisions and prove the data interval behind every newly admitted field/table.

Sources: [S040](https://docs.hevodata.com/edge/pipelines/object-and-schema-management/pipeline-schema-management/).

### 33. Matillion CDC — D1

**Observed:** CDC agents stage change data in cloud storage and shared jobs feed target processing; docs discuss schema drift.

**Boundary:** Staging, target application and data visibility are separate phases; destination-specific drift semantics require further adapter checks.

**migkit response:** Model upload/manifest/apply/visible positions and recovery obligations as an executable storage protocol rather than treat uploaded bytes as migrated.

Sources: [S077](https://docs.matillion.com/data-productivity-cloud/cdc/docs/cdc-schema-drift/), [S078](https://docs.matillion.com/data-productivity-cloud/cdc/docs/cdc-shared-jobs/).

### 34. Rivery/Boomi — D0-partial

**Observed:** Indexed vendor schema-drift documentation describes automatic detection and handling.

**Boundary:** Opening the legacy URL redirected to a generic Boomi integration page with no usable body. Current product-specific technical semantics were not established.

**migkit response:** Keep as a research-coverage gap. Do not claim connector/protocol parity until current authoritative manuals or a licensed environment are available.

Sources: [S081](https://docs.rivery.io/docs/schema-drift).

### 35. Ispirer SQLWays — D1

**Observed:** Assessment spans database/application objects; CLI options include assessment/statistics and generated test scripts.

**Boundary:** Vendor feature claims and test-script generation are not execution coverage or behavioral equivalence proof.

**migkit response:** Extend extraction/assessment scope and consumer lineage; generated tests enter the existing sandbox harness and must expose uncovered branches.

Sources: [S065](https://wiki.ispirer.com/sqlways/command-line/options), [S063](https://www.ispirer.com/products/database-migration), [S061](https://www.ispirer.com/products/sqlways-database-migration).

### 36. SQLines Data/Converter — D2

**Observed:** Native bulk APIs, schema/constraint/index options and row-count/full-comparison modes expose distinct migration stages.

**Boundary:** Equal counts are weak evidence; code conversion and data movement are separate products/functions.

**migkit response:** Compare native movement with current paths under the same complete proof contract; select counts only for triage and retain exact value verification.

Sources: [S066](https://www.sqlines.com/sqldata_validation), [S067](https://www.sqlines.com/sqldata_cfg), [S068](https://sqlines.com/sqldata).

### 37. DBConvert Streams — D2

**Observed:** Log-based CDC and optional initial snapshot are distinct from Load mode; load review shows per-table target effects before writing.

**Boundary:** Verification documentation describes totals-first and row comparison where totals differ, which cannot alone detect every equal-count mutation.

**migkit response:** Preserve useful effect previews while ensuring equal totals never suppress required exact checks; version each plan against the actual target identity.

Sources: [S069](https://streams.dbconvert.com/docs/integration/what-is-cdc), [S070](https://streams.dbconvert.com/docs/streams/verify-load).

### 38. Datometry Hyper-Q — D1-historical

**Observed:** Official architecture describes real-time SQL emulation/translation between applications and a different warehouse.

**Boundary:** The retrieved documentation is historical; this is an architectural pattern, not a verified current universal compatibility matrix.

**migkit response:** Consider an optional registered compatibility boundary for otherwise unconvertible clients, with measured latency, semantic proof and an explicit long-term dependency cost.

Sources: [S071](https://gcp.docs.datometry.com/en/7086-about-hyper-q.html).

### 39. TiDB DM/Lightning ecosystem — D1

**Observed:** DM documents optimistic/pessimistic shard DDL coordination and a restart-safe mode for replayed incremental operations.

**Boundary:** Optimistic DDL has restrictions; safe mode and key/transaction assumptions are separate from exact proof. Lightning details were not freshly exhaustively reviewed.

**migkit response:** Extend existing shard decision logic and atomic position/effect tests. Preserve per-shard schema compatibility and final keyspace-placement proof.

Sources: [S051](https://docs.pingcap.com/tidb/v7.5/feature-shard-merge-optimistic/), [S052](https://docs.pingcap.com/tidb/dev/dm-safe-mode/).

### 40. Vitess MoveTables/VDiff — D1

**Observed:** A documented workflow creates/copies, checks with VDiff, switches traffic and completes cleanup; reverse migration is integrated with topology workflows.

**Boundary:** Routing ownership and supported Vitess topology are part of the mechanism; generic endpoints do not inherit it automatically.

**migkit response:** Treat routing rules, tablet roles, writes, reverse readiness and cleanup as one durable topology-dependent cutover plan.

Sources: [S054](https://vitess.io/docs/24.0/reference/vreplication/movetables/).

### 41. PeerDB/ClickPipes — D2

**Observed:** Vendor engineering describes pg_export_snapshot plus parallel CTID-based initial load; lifecycle creates a replication slot during snapshot and re-enters snapshot on resync/new tables.

**Boundary:** Long initial loads retain WAL; CTID range movement is meaningful only within the valid snapshot. Published benchmark numbers are vendor measurements.

**migkit response:** Choose shared-snapshot parallel reads by tested engine constraints and include log/disk/snapshot-age budgets; resync must reconcile concurrent deletes and schema versions.

Sources: [S058](https://clickhouse.com/blog/practical-postgres-migrations-at-scale-peerdb), [S056](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/clickpipes/postgres/lifecycle.md).

### 42. YugabyteDB Voyager — D2

**Observed:** Assessment uses metadata/workload statistics for target recommendations; live migration offers fallback and a third-database fall-forward design.

**Boundary:** Assessment/connector features have documented preview/GA distinctions; reverse replication can modify the original source.

**migkit response:** Rehearse target layout and reverse-compatible paths, and verify new-target write intervals before calling rollback safe.

Sources: [S149](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/assess-migration/), [S048](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-back/), [S049](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-forward/).

### 43. CockroachDB MOLT/Continuum — D0-vendor-overview

**Observed:** Current vendor overview describes bulk movement, continuous replication and failback support for migration workflows.

**Boundary:** This pass did not establish detailed current per-source/version failback semantics from technical manuals; no internals or speedup treated as verified.

**migkit response:** Keep candidate capability/probe tasks, not assumed support. Independently establish keys/types/positions/transaction semantics before accepting an adapter.

Sources: [S050](https://www.cockroachlabs.com/migrations/).

### 44. Google DVT — D1

**Observed:** Official repository provides repeatable heterogeneous column/row/schema/custom-query validation.

**Boundary:** Aggregate checks, row hashes and custom queries have different proof strengths; tool availability does not imply every requested backend works.

**migkit response:** migkit already wraps a second reader. Extend per-engine execution proof and common canonicalization coverage rather than add another unconnected validator.

Sources: [S088](https://github.com/GoogleCloudPlatform/professional-services-data-validator).

### 45. Debezium — D1-versioned

**Observed:** The retrieved versioned PostgreSQL manual documents incremental snapshots triggered through signals and connector state.

**Boundary:** Signaling may write to the source; versioned connector behavior and log retention matter. No claim this 3.0 manual is the newest release.

**migkit response:** Select registered snapshot/signal methods under actual source-write policy, with atomic watermark/state semantics and no implicit source mutation.

Sources: [S086](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html).

### 46. Oracle Zero Downtime Migration — D1-versioned

**Observed:** ZDM distinguishes online/offline and physical/logical/hybrid workflows, using native mechanisms such as RMAN/Data Guard for eligible paths.

**Boundary:** Downtime and supported source/target/version combinations depend on the selected method; the product name is not a measured zero-second outage guarantee.

**migkit response:** Extend physical versus logical eligibility, restore/switchover contracts and native fallback; measure downtime with client writes and role transitions.

Sources: [S113](https://docs.oracle.com/en/database/oracle/zero-downtime-migration/26.1/zdmug/introduction-to-zero-downtime-migration.html), [S114](https://docs.oracle.com/en/database/oracle/zero-downtime-migration/21.2/zdmug/introduction-to-zero-downtime-migration.html).

### 47. SAP SLT — D1

**Observed:** SLT separates initial load and continuing replication; source logging tables/triggers record changes during active initial load.

**Boundary:** This mechanism changes the source and imposes platform-specific permissions/operational load; generic source-read-only promises do not apply.

**migkit response:** Register trigger/log-table capture as a source-effectful capability with explicit policy, ownership, cleanup and measured source impact.

Sources: [S117](https://help.sap.com/saphelp_slt200/helpdata/en/9b/9b67560889225ae10000000a4450e5/content.htm?no_cache=true), [S118](https://help.sap.com/docs/SAP_LANDSCAPE_TRANSFORMATION_REPLICATION_SERVER/007c373fcacb4003b990c6fac29a26e4/ee2e5f564ca7c54de10000000a441470.html?locale=en-US).

### 48. IBM Q/SQL Replication — D1

**Observed:** Q Capture reads Db2 recovery logs and sends committed transactions through IBM MQ to Q Apply; SQL replication has a different capture/apply architecture.

**Boundary:** Transport durability, transaction boundaries and target applicability belong to the mechanism, not just a Db2 connector label.

**migkit response:** Model queue acknowledgments, retention, apply positions and transaction proof across a native adapter; reconcile queue state during takeover.

Sources: [S119](https://www.ibm.com/docs/en/db2/12.1.x?topic=options-replication-tools-by-component), [S120](https://www.ibm.com/docs/en/idr/11.4.0?topic=po-q-replication).

### 49. Microsoft SSMA — D1

**Observed:** SSMA supports several legacy source families for SQL Server/Azure SQL migration and reports conversion statistics and complexity.

**Boundary:** Assessment/converted T-SQL are separate from full workload behavior and migration completeness.

**migkit response:** Use object/statement scope and explicit residual work in assessment; execute representative converted routines through the same proof contract.

Sources: [S121](https://learn.microsoft.com/en-us/sql/ssma/sql-server-migration-assistant?view=sql-server-ver16), [S122](https://learn.microsoft.com/en-us/sql/ssma/shared/assessment-report?view=sql-server-ver17).

### 50. Spanner migration stack — D1

**Observed:** Official guidance composes assessment/schema tools, CDC/data movement and DVT; reverse pipelines support fallback for documented scenarios.

**Boundary:** Some stages require manual work and support depends on the source; reverse replication is a separate prepared workflow.

**migkit response:** Compose provider tools under one evidence/control plane and prove distributed key/transaction/rollback compatibility instead of equating connectivity with migration support.

Sources: [S125](https://docs.cloud.google.com/spanner/docs/migration-tools), [S126](https://docs.cloud.google.com/spanner/docs/cutover-fallback).

### 51. Alibaba ADAM — D1

**Observed:** Assessment recommends targets, estimates transformation effort and supports schema/application migration for specified Oracle scenarios; the overview says ADAM is free.

**Boundary:** This is a separate assessment/conversion product surface from DTS transport and has a supported source/version matrix.

**migkit response:** Compare target alternatives by measured workload compatibility, residual application effort and verified completion, not a transport-only feature list.

Sources: [S133](https://www.alibabacloud.com/help/en/dts/user-guide/adam-overview), [S128](https://www.alibabacloud.com/help/en/dts/user-guide/database-evaluation/), [S129](https://www.alibabacloud.com/help/en/dts/user-guide/evaluate-a-database).

### 52. Huawei UGO — D1

**Observed:** UGO evaluates source/object complexity, converts DDL/DML/DCL and lets users correct migration risks before object verification.

**Boundary:** Schema verification/deployment and data equality are separate obligations; provider/target-specific syntax coverage applies.

**migkit response:** Keep conversion issues, manual fixes, deployed artefacts and independent executed evidence in the same dependency ledger as DRS-like data movement.

Sources: [S130](https://support.huaweicloud.com/intl/en-us/usermanual-ugo/ugo_03_0006.html), [S131](https://support.huaweicloud.com/intl/en-us/productdesc-ugo/ugo_01_0002.html), [S132](https://support.huaweicloud.com/intl/en-us/qs-ugo/ugo_02_0010.html).

### 53. AWS Fleet Advisor retirement — D1

**Observed:** The official end-of-support notice states Fleet Advisor access ends May 20, 2026.

**Boundary:** Older comparison documents that recommend Fleet Advisor as a current supported component are stale.

**migkit response:** Version support metadata and lifecycle notices; retained local discovery/assessment must not depend on an inaccessible retired service.

Sources: [S127](https://docs.aws.amazon.com/dms/latest/userguide/dms_fleet.advisor-end-of-support.html).

### 54. EDB DMS/LiveCompare current surfaces — D1

**Observed:** Current EDB docs describe cloud snapshot/snapshot-plus-streaming migration and resumable multi-database comparison with a reporting/cache database.

**Boundary:** Support varies by connection technology and replication topology; cache/report-store state is an operational dependency.

**migkit response:** Connect multi-node discovery to authorized scope and evidence lineage, with durable resume state and independent role/privilege/application checks.

Sources: [S134](https://www.enterprisedb.com/docs/edb-postgres-ai/migration-etl/), [S135](https://www.enterprisedb.com/docs/livecompare/latest/), [S136](https://www.enterprisedb.com/docs/livecompare/latest/supported_technologies/), [S137](https://www.enterprisedb.com/docs/livecompare/latest/bdr_support/).

### 55. HVR burst/continuous integration — D1

**Observed:** Continuous applies source commits row-wise; burst coalesces net row changes into staging and set-based application. Documentation says both default modes maintain transactional consistency, with different latency/throughput.

**Boundary:** Any net-change optimization must still match the selected goal's history and visibility requirements; target-specific settings affect supported behavior.

**migkit response:** Cost netting/staging/latency jointly and prove final-state versus event-history contracts independently rather than infer a universal batch correctness penalty.

Sources: [S140](https://fivetran.com/docs/hvr6/action-reference/integrate), [S141](https://fivetran.com/docs/hvr6/faq/expert-notes-best-practices/burst-vs-continuous-integration).

## 5. Lifecycle coverage: the questions every candidate must answer

| Phase | Mechanism-level questions | Proof that closes the gap |
|---|---|---|
| Install/connect | Driver/tool version, network path, auth expiry, TLS, endpoint identity, permissions and provider generation? | Clean-machine installed run, secret-safe evidence, bounded discovery |
| Discover/assess | All selected objects/dependencies, unreadable metadata, workload shape, supported types, source/target conflicts? | Explicit denominators and no guessed absence |
| Convert | Parsed/excluded code, dependency order, type loss, emulation, dynamic SQL, ETL/client consumers? | Deployed and executed scope, aligned inputs, counterexamples and coverage |
| Seed/full load | Native/logical/physical eligibility, snapshot consistency, split/skew, index/constraint windows, LOBs? | Common snapshot/handoff certificate; exact data/object coverage |
| Follow changes | Positions, transaction boundaries, ordering, dedup identity, metadata evolution and backlog retention? | Crash/failover tests covering every acknowledged interval |
| Validate | Current state or event history, canonicalization, multiset duplicates, live divergence, independent reader blind spots? | Exact required proof at a valid epoch; uncertain remains incomplete |
| Repair | Cause understood, row/range/table choice, CDC exclusion, preimages, target writers, atomic effects? | Fresh fenced readback plus recoverable undo/reconcile |
| Replan | Fact expiry, predicted error, load/quota changes, transition cost, source impact, deadline slack? | Conservative improvement over remaining work without oscillation |
| Cutover | Active writers, transactions, proof fence, counters, route/pool/consumer offsets and readiness? | Client-observed completion, durable transition and one writer |
| Reverse/rollback | Invertible mapping, reverse capture start, new objects, original-source safety, checkpoints? | Post-switch writes preserved through a tested reverse path |
| Cleanup/operations | Owned slots/spools/settings/credentials/processes/undo; what evidence must survive? | Interrupt every acquisition/release point; no leaked or prematurely removed state |
| Knowledge/control | Provenance, calibration, drift, deterministic replay, trusted agent calls and support lifecycle? | Reproducible measured quality and versioned capability decisions |

A checkmark for full load, CDC or validation is too coarse to answer these. The detailed work items below identify concrete mechanisms and acceptance scenarios.

## 6. Current direction, public roadmaps and forecasts

**Already documented, not predictions.** AWS 3.6.1 documents validation-driven resync and newer authentication/replica capabilities. Google release notes document 2026 conversion-quality assessments, Preview quick-start/MCP and Preview heterogeneous failback. Tencent 2026 notes include account conflict policy, GTID checks, replay-position configuration and CAM-based authentication. Qlik May 2026 broadens native endpoint/type handling. Snowflake's migration skill combines conversion, reusable fixes, validation, ETL and BI surfaces while explicitly marking some source capabilities Planned. Confluent automatic failover is documented as Early Access. Sources: [S109](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html), [S016](https://docs.cloud.google.com/database-migration/docs/release-notes), [S112](https://proxy-hk.tencentcloud.com/ind/document/product/571/42597), [S080](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill), [S082](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-automatic-failover-tutorial.html).

**Forecasts below are our inference, not vendor commitments.** Qualitative confidence reflects the consistency of these public signals, not a statistical probability. Horizons are scenario-planning windows from the research date, not delivery promises.

| Inferred direction | Confidence / horizon | What migkit should build ahead | What could falsify it |
|---|---|---|---|
| Migration becomes an agent-accessible control plane | High / 6-18 months | Structured operations on existing migrate, operation identity, typed effects and evidence-bound authorization | Vendors keep agents as read-only UI helpers |
| AI conversion expands into fix propagation and execution-aware verification | High / 6-18 months | Candidate synthesis plus independent sandbox differential/mutation proof and reusable verified rules | AI quality stays annotation-only; limited safe code coverage |
| Low-risk modernization includes application/ETL/report dependencies | High / 12-24 months | Scope/lineage wave planner and selected consumer adapters | Conversion tools stay siloed; extraction/consumer access remains unavailable |
| Failback and rehearsal become expected migration capabilities | High / 6-18 months | Reverse contracts and captured-position readiness before promotion | Pair-specific type/transaction constraints keep reverse paths niche |
| Adaptive copy/CDC/proof methods converge into one scheduler | Medium / 12-24 months | Complete-path optimization with typed guarantees, source-impact and transition costs | Providers optimize isolated services rather than joint workflows |
| Continuous independent validation becomes a separately valuable service | High / 6-18 months | Mover-independent proof with common epoch, exact coverage and controlled repair | Customers accept engine-local counts with limited assurance |
| Connector support becomes more granular and constantly evolving | High / ongoing | Version/service/type capability registry, bounded probes and expiry rules | Standardized APIs erase most meaningful differences |
| Open table formats and streaming warehouses demand commit-aware migration | High / 12-24 months | Manifest/journal/offset visibility semantics, not file-count checks | Workloads remain snapshot-only and consumers tolerate duplicates |
| Self-tuning moves toward workload objectives instead of knob defaults | Medium / 12-24 months | Calibrated model, safe experiments, hysteresis and measured whole-run utility | Dynamic overhead/permissions cost more than the benefit |

Do not predict that an LLM will safely infer destructive business intent from credentials, that arbitrary type transformations are invertible, or that heterogeneous exactly-once execution is available without endpoint support. These are constraints on any forecast, not features to promise.

## 7. Forty additional research-derived mechanism actions

These extend the earlier SMART-N1..N25 and existing backlog parents. OPEN means a proposed implementation/proof task. A vendor feature is inspiration, not proof migkit lacks the entire area. Each item names code surfaces, the deeper behavior and an acceptance test. Use stable IDs rather than numeric rows when concurrent index changes shift numbering.

### RCH01.provider-path: Differentiate provider-native and generic paths

**P1; M; OPEN extension.** Parent: SMART-N6, P0-CLOUDS. Code: `movers, managed, cloudapi, decide`.

**Trigger:** AWS classic versus homogeneous; Azure LRS versus logical copy.

**Mechanism:** Choose native backup/log restore, logical replication, dump/restore or generic CDC from actual source/target/version/network/privilege support. Build a per-path coverage and secondary-object contract; add independent validation when the provider-native path lacks it. Include provider resource lifecycle and completion semantics.

**Acceptance:** Prove both native and generic paths for a pair, inject an omitted view/sequence/grant, and ensure native speed never earns unsupported coverage.

Sources: [S142](https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html), [S091](https://learn.microsoft.com/th-th/Azure/Azure-sql/managed-instance/log-replay-service-overview?view=azuresql).

### RCH02.lob-policy: Choose LOB policy without truncation

**P1; M; OPEN extension.** Parent: SMART-N20, READ.chunk-loops. Code: `decide, pieces, engines, tuning`.

**Trigger:** AWS LOB limits; Qlik modes; MySQL pieces.

**Mechanism:** Make inline, streamed, locator, chunked and separately fetched LOB paths candidates under value-size/packet/memory/consistency facts. Reject truncating or stale-locator paths under exact contracts; size probes are bounded and unknown maxima stay conservative.

**Acceptance:** Mixed NULL/empty/binary/multibyte/extreme LOBs plus concurrent updates and failures produce exact final bytes, bounded memory and no premature checkpoint.

Sources: [S109](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html), [S007](https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm).

### RCH03.transaction-dag: Parallelize changes with explicit dependency guarantees

**P1; L; OPEN extension.** Parent: SUPER.dag-barrier, SMART-N8. Code: `barriers, tail_apply, guarantees, jointplan`.

**Trigger:** GoldenGate parallel apply and barriers.

**Mechanism:** Extend existing transaction DAG work with row/unique/FK dependency edges, barriers for DDL/sequence/cutover and explicit handling of huge transactions. Independent transactions may reorder only within the consistency contract. Schedule by measured downstream contention, not fixed worker count.

**Acceptance:** Cross-table FK cascades, unique-key swaps, DDL and kill/restart preserve source transaction atomicity; committed-prefix proof blocks cutover on holes.

Sources: [S001](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/replicat-integrated-replicat.html), [S002](https://docs.oracle.com/en/middleware/goldengate/core/21.3/oracle-db/configuring-oracle-goldengate-apply.html).

### RCH04.apply-contract: Cost transactional and batch apply under different contracts

**P1; M; OPEN extension.** Parent: SMART-N1, SMART-N6. Code: `decide, guarantees, tableway`.

**Trigger:** Qlik transactional/batch and IBM optimization tradeoffs.

**Mechanism:** Declare the integrity, ordering, key and trigger assumptions of each apply mode. Rank batch merging only when required guarantees can be maintained or restored before exposure; keyless/native endpoints need their own contract. Include batch recovery/validation cost.

**Acceptance:** No-PK duplicates, cross-table transactions and a target without unique enforcement cannot silently inherit transactional guarantees from a batch path.

Sources: [S007](https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm), [S106](https://www.ibm.com/support/pages/node/1102497).

### RCH05.repair-fence: Schedule repair against CDC without stale overwrite

**P1; M; OPEN extension.** Parent: repairways existing, SMART-N7. Code: `repairways, tailtables, fencing`.

**Trigger:** AWS resync and HVR refresh exclusion.

**Mechanism:** Extend existing rows/spans/reload repair to establish a repair epoch and drain/hold affected apply work, preserve newer target writes under conflict policy, then reconcile buffered changes and reverify. Choose row/range/table scope by total cost and bounded outage.

**Acceptance:** Delay a repair write while a later CDC update arrives; newest committed source value wins, replay is gap-free, and interrupted repair can undo or reconcile safely.

Sources: [S143](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.DataResync.html), [S006](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh).

### RCH06.online-confirmation: Classify transient versus persistent divergence by positions

**P1; M; OPEN extension.** Parent: existing convergence checks, SMART-N7. Code: `verifiers, delta, planner`.

**Trigger:** Veridata MOOS/COOS; HVR online compare.

**Mechanism:** Extend existing delta/generation/fence logic with position-based confirmation where possible; uncertain rows stay pending, not repaired automatically after an arbitrary sleep. Count all selected proof units and track expiry of online comparison evidence.

**Acceptance:** Continuously changing rows, deleted-on-both rows and delayed transactions settle correctly; lag does not create false corruption and persistent drift is not suppressed.

Sources: [S098](https://docs.oracle.com/en/database/goldengate/core/26/gvdug/how-oracle-goldengate-veridata-works.html), [S005](https://fivetran.com/docs/hvr6/getting-started/concepts/compare).

### RCH07.keyless-proof: Prove keyless multisets at scale

**P1; L; OPEN extension.** Parent: SMART-N7, REV-T1. Code: `canon, verifiers, engines`.

**Trigger:** DTS thresholds; Validata duplicate limitation.

**Mechanism:** Use duplicate-sensitive multiset fingerprints with exact escalation/external sorting and a deterministic row-identity strategy where CDC needs it. Avoid XOR-only acceptance, sample-only same or synthetic-key source mutation. Include count/multiplicity and canonicalization contracts.

**Acceptance:** Perfect duplicates with even-count cancellation, reorder, NULL/empty, equal-count corruption and billion-row spill scenarios cannot falsely pass.

Sources: [S010](https://www.alibabacloud.com/help/en/dts/user-guide/enable-data-verification), [S073](https://www.striim.com/docs/validata/en/validation-types.html).

### RCH08.effect-checkpoint: Bind keyless writes and dedup state atomically

**P1; L; OPEN extension.** Parent: SMART-N17, existing fencing. Code: `attempts, durable, engines, tail_apply`.

**Trigger:** Alibaba exactly-once metadata tables.

**Mechanism:** Per engine, choose atomic target transaction plus position/dedup identity when supported; otherwise reconcile and declare weaker semantics. Metadata ownership and retention are part of the plan. Reconstruct checkpoints without treating absence as a fresh start.

**Acceptance:** Crash after target effect/before ack and after checkpoint boundaries for keyless duplicate rows; no duplicated or missing effects under the declared contract.

Sources: [S011](https://www.alibabacloud.com/help/en/dts/user-guide/synchronize-tables-without-primary-keys-and-unique-constraints-from-the-source-database), [S025](https://docs.informatica.com/content/dam/source/GUID-5/GUID-564A497D-5045-4D3A-92A2-F1C5D0D1B030/50/en/CMI_November2024_DatabaseIngestionAndReplication_en.pdf).

### RCH09.schema-epoch: Propagate compatible DDL through a fenced dependency plan

**P1; L; OPEN extension.** Parent: SUPER.ddl-mutation, AUTO.schema-drift. Code: `schema_guard, tailtables, hetero`.

**Trigger:** Google manual refresh; connector-specific drift support.

**Mechanism:** Extend schema drift support with typed compatibility classification, schema-versioned change decoding and dependent invalidation. Include default backfill, rename identity, constraint changes and transition state. Refuse unrepresentable changes rather than make silent lossy fixes.

**Acceptance:** ADD default, rename/drop/re-add same name, changed PK and DDL during snapshot/restart leave a coherent target and invalidate exactly affected evidence.

Sources: [S110](https://docs.cloud.google.com/database-migration/docs/oracle-to-postgresql/known-limitations), [S148](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/known-limitations), [S101](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq).

### RCH10.shadow-resync: Resnapshot affected objects while keeping validated live state

**P1; L; OPEN extension.** Parent: SMART-N16, SMART-N22. Code: `tailtables, ship, ownership`.

**Trigger:** ClickPipes lifecycle; Airbyte refresh; HVR refresh.

**Mechanism:** Choose fenced range repair, shadow-table resnapshot or whole reload. Buffer changes from a known position, reconcile tombstones, catch up shadow state, validate then atomically swap only where the engine can. Account for temporary disk and old table rollback retention.

**Acceptance:** Delete/update during snapshot, new columns and crash at swap yield one complete version with no duplicated/lost changes; foreign references and application bindings remain valid.

Sources: [S056](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/clickpipes/postgres/lifecycle.md), [S107](https://docs.airbyte.com/platform/understanding-airbyte/cdc), [S006](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh).

### RCH11.snapshot-watermark: Choose consistent snapshot-to-stream handoff mechanisms

**P1; L; OPEN extension.** Parent: SMART-N16, existing shared snapshot. Code: `planner, movers, snapshot, tailtables`.

**Trigger:** PeerDB exported snapshots; Debezium incremental snapshots.

**Mechanism:** Register shared snapshot, range watermark and engine-native handoff as explicit alternatives. Price snapshot lifetime/WAL retention/source impact; declare where signaling writes are required and respect read-only source intent. Detect missing log intervals before accepting copy coverage.

**Acceptance:** Concurrent range mutation, long snapshot and restart across failover demonstrate complete handoff; unavailable watermark support refuses that path rather than faking an epoch.

Sources: [S058](https://clickhouse.com/blog/practical-postgres-migrations-at-scale-peerdb), [S086](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html).

### RCH12.reverse-contract: Preflight reversibility before claiming failback

**P1; L; OPEN extension.** Parent: AUTO.reverse-failback, SMART-N14. Code: `reverse, cutover, transforms`.

**Trigger:** Google failback Preview; Voyager fallback/fall-forward.

**Mechanism:** Build a reverse-capability contract for types, mappings, keys, DDL and commit ordering. Prepare reverse capture at the forward fence before new target writes; refuse zero-loss claims if any write interval/type/object lacks reverse representation. Choose original-source fallback or third-target fall-forward where authorized.

**Acceptance:** Writes immediately after promotion, transformed values, new objects, keyless updates and schema changes are either preserved in reverse or explicitly block failback readiness.

Sources: [S146](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/failback-migration), [S048](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-back/), [S049](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-forward/).

### RCH13.traffic-state: Make cutover a durable client-and-database state machine

**P1; M; OPEN extension.** Parent: AUTO.proxy-shift, SMART-N17. Code: `proxy, cutover, session`.

**Trigger:** Vitess traffic workflow; Confluent offsets; Redis routing limits.

**Mechanism:** Track pause/drain/fence/proof/counter adjustment/routing/resume and client acknowledgment separately. Observe outstanding sessions, stale DNS/connection pools and read/write routing. API success or database promotion alone is not application cutover completion.

**Acceptance:** Clients with cached connections plus network failure after routing update cannot split writes; restart reconciles the actual active writer before continuing.

Sources: [S054](https://vitess.io/docs/24.0/reference/vreplication/movetables/), [S083](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html), [S045](https://redis.io/docs/latest/operate/rc/databases/active-active/).

### RCH14.conflict-semantics: Choose conflict rules by data semantics and evidence

**P1; L; OPEN extension.** Parent: SMART-N14, existing two-way. Code: `twoway, canon, guarantees`.

**Trigger:** GoldenGate ACDR; SharePlex CDR; Redis CRDT.

**Mechanism:** Describe policies by origin identity, before image, version vector/timestamp assumptions, tombstone horizon and permissible merge algebra. Restrict commutative merging to types where it is defined; preserve audit trails and unresolved conflicts. Never infer business precedence from server names.

**Acceptance:** Clock skew, delete/reinsert, concurrent increments, out-of-order delivery and cyclic replication converge only under supported semantics; conflicting noncommutative writes remain visible.

Sources: [S003](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/administer-automatic-cdr.html?source=%3Aso%3Ach%3Aor%3Aawr%3A%3A%3A), [S023](https://support.quest.com/technical-documents/doc1629965), [S045](https://redis.io/docs/latest/operate/rc/databases/active-active/).

### RCH15.source-role: Use replicas only when their snapshots and positions are valid

**P1; M; OPEN extension.** Parent: SMART-N2, SMART-N9. Code: `engines, model, scenarios`.

**Trigger:** AWS PostgreSQL16 replica CDC; Databricks AG resume.

**Mechanism:** Discover primary/replica role and lineage, portable positions, slot failover support and replay lag. Decide where assessment, snapshot, CDC and verification read. Include replica failover/lag in proof freshness and cost, not a blanket source-load optimization.

**Acceptance:** Replica lag and primary failover between snapshot and tail cannot skip changes or compare incompatible timelines; role changes invalidate affected decisions.

Sources: [S109](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html), [S036](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits).

### RCH16.retention-controller: Prevent unbounded WAL and spool risk proactively

**P1; M; OPEN extension.** Parent: SUPER.store-forward, SMART-N19. Code: `model, tailtables, sizing, leftovers`.

**Trigger:** PeerDB initial-load slots; Striim checkpoints.

**Mechanism:** Forecast retained log/spool growth from arrival minus durable-consumption rates, free disk, snapshot age and recovery windows. Speed copy, throttle safe stages, add authorized capacity or stop/reseed from a valid point before irreversible log loss. Finite spool is not a guarantee against unlimited outage.

**Acceptance:** Slow target plus multi-hour outage and snapshot restart stay inside disk limits or stop truthfully without acknowledging unretained source changes.

Sources: [S056](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/clickpipes/postgres/lifecycle.md), [S075](https://striim.com/docs/platform/en/recovering-applications.html).

### RCH17.mapping-proof: Verify composed mappings in both load and CDC

**P1; M; OPEN extension.** Parent: SMART-N14, existing transformed verification. Code: `transforms, canon, engines/hetero`.

**Trigger:** HVR directional coercion; Mongo relational-to-document verification.

**Mechanism:** Represent mapping/filter/mask/embedding as a versioned dataflow with coverage lineage. Verify expected mapped source output against target, preserving one-to-many/many-to-one identity, multiplicity and update/delete semantics. Compare lossiness explicitly rather than silently normalizing differences away.

**Acceptance:** Mask+filter+rename and normalized/embedded documents remain correct after updates/deletes and restart; raw equal counts do not substitute for mapped-value proof.

Sources: [S005](https://fivetran.com/docs/hvr6/getting-started/concepts/compare), [S042](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/).

### RCH18.conversion-ledger: Keep submitted, parsed, converted and executed scope distinct

**P1; M; OPEN extension.** Parent: SMART-N7, existing storedcode PROVES. Code: `storedcode, capabilities, verdict`.

**Trigger:** SnowConvert scope accounting.

**Mechanism:** For code/ETL/app artefacts keep stable identities and separate submitted/extracted/parsed/converted/deployed/executed/covered counts. Unsupported encodings, encrypted code and dynamic SQL stay explicit debt. Report every denominator; no conversion percentage can hide excluded objects.

**Acceptance:** One unparsed critical routine blocks complete readiness even when 100 percent of parsed code converted; scope survives rename/restart.

Sources: [S034](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/review-results/snowconvert-scopes), [S031](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/validation/README).

### RCH19.ai-proof-loop: Propose with AI and accept only independent executable evidence

**P1; L; OPEN extension.** Parent: SMART-N10, existing storedcode proof/mutation. Code: `storedcode/translate, prove, mutants`.

**Trigger:** Google Gemini quality; Datafold align/diff; SnowConvert skills.

**Mechanism:** Extend deterministic conversion with bounded candidate synthesis only where authorized. Freeze aligned inputs, run source and target in isolated sandboxes, compare observable effects and minimize counterexamples; mutation tests assess weakness of fixtures. AI explanations/scores are annotations, not proof; retain provenance and regression fixtures.

**Acceptance:** Plausible wrong NULL/exception/timezone/transaction conversions are rejected; retry budgets terminate; no source/application code or data sent to unapproved model destinations.

Sources: [S147](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/code-conversion-with-gemini), [S099](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill).

### RCH20.rule-generalization: Learn conversion rules without propagating a one-off wrong fix

**P2; M; OPEN extension.** Parent: SMART-N3, SMART-N19 equivalent proof pipeline. Code: `storedcode/learn, translate, inputs`.

**Trigger:** Google pattern propagation; Snowflake reusable fixes.

**Mechanism:** Learn narrowly scoped rewrite rules with preconditions, dialect/version constraints and proof obligations. Apply in shadow, test withheld object families, keep conflicting-rule resolution and rollback. Preserve deterministic baseline and rule provenance.

**Acceptance:** A correction for one decimal/NULL pattern cannot alter unrelated routines; cross-project reuse earns proof before activation and can be revoked independently.

Sources: [S147](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/code-conversion-with-gemini), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill).

### RCH21.dynamic-sql: Trace dynamic SQL and external application dependencies

**P1; L; OPEN extension.** Parent: SMART-N12, SMART-N18. Code: `storedcode/appscan, inputs, guide`.

**Trigger:** Ispirer assessment; SnowConvert ETL/BI scope; Hyper-Q.

**Mechanism:** Extend AST scanning with bounded configuration-aware constant propagation and sandbox/runtime traces where approved. Track queries from ORM mappings, prepared statements, generated SQL, report definitions and external jobs as unresolved or discovered dependencies. Never treat heuristic absence as proof of no use.

**Acceptance:** Parameterized SQL, generated identifiers and encrypted/unavailable code are reflected in readiness coverage; migration waves cannot cut over a dependency whose consumer still points at the old shape.

Sources: [S063](https://www.ispirer.com/products/database-migration), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill), [S071](https://gcp.docs.datometry.com/en/7086-about-hyper-q.html).

### RCH22.etl-bi-waves: Plan database, ETL and report migration waves together

**P2; L; OPEN extension.** Parent: SMART-N13, SMART-N18. Code: `planner, storedcode, scope`.

**Trigger:** Snowflake ETL conversion/report repointing.

**Mechanism:** Add registered parsers/adapters for selected ETL and BI artefacts under explicit scope, compile lineage and wave prerequisites, validate transformed results on aligned inputs and coordinate connection repointing. Keep external deployments separately authorized. Start with one concrete artefact family, not universal format support.

**Acceptance:** A database table used by an ETL job and dashboard cannot be marked whole-workload ready while either dependency is unconverted or unverified.

Sources: [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill), [S028](https://www.enterprisedb.com/docs/migrating/oracle/edb_migration_tools/).

### RCH23.query-regression: Gate app readiness on behavioral and performance envelopes

**P1; L; OPEN extension.** Parent: SUPER.query-guard, SMART-N18. Code: `storedcode/bench, model, readiness`.

**Trigger:** Datafold aligned inputs; EDB testing; Qlik target modes.

**Mechanism:** Use representative approved workloads, parameter distributions, concurrency and app roles on a target sandbox. Compare result semantics plus p50/p95 latency, throughput and contention, separating cold-cache effects and noisy resource changes. Candidate index/statistics fixes must be verified and priced.

**Acceptance:** Identical rows with a changed collation, missing index or regressed execution plan cannot receive an unconditional app-ready verdict; no synthetic benchmark is labelled production performance.

Sources: [S099](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/), [S028](https://www.enterprisedb.com/docs/migrating/oracle/edb_migration_tools/).

### RCH24.principal-migration: Migrate security semantics without indiscriminate grant copying

**P1; M; OPEN extension.** Parent: SMART-N12, existing privilege checks. Code: `users, ownership, managed, guide`.

**Trigger:** Tencent account conflict policies; Azure DEFINER limitations.

**Mechanism:** Model user/role/grant/ownership/default privilege and DEFINER/INVOKER semantics separately from credentials and provider-managed principals. Map identities and approved conflict policy explicitly; test with the application role. Unsupported server logins/certificates become scoped prerequisites.

**Acceptance:** Missing sequence privileges, RLS, definer changes and duplicate account names are discovered and resolved within policy; no unintended privilege widening or silent use of migrator ownership.

Sources: [S112](https://proxy-hk.tencentcloud.com/ind/document/product/571/42597), [S151](https://learn.microsoft.com/en-us/azure/dms/tutorial-mysql-azure-external-to-flex-online-portal), [S148](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/known-limitations).

### RCH25.target-tuning-contract: Treat ingest tuning as a temporary durability contract

**P1; M; OPEN extension.** Parent: AUTO.target-tune, AUTO.deferred-index. Code: `tune, indexes, managed, guarantees`.

**Trigger:** HVR load windows; SQLines constraints; AUTO.target-tune.

**Mechanism:** Extend current tuning implementation with approved durability bounds, observed original settings and restart-safe restore. Price index/constraint/ANALYZE tails and crash recovery, and never publish app-ready while checks are disabled or constraints unvalidated. Do not globally turn off protections merely because a faster mode exists.

**Acceptance:** Crash during load and restoration cannot leave production with weakened durability/disabled constraints; overall measured completion justifies each tuning change.

Sources: [S006](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh), [S067](https://www.sqlines.com/sqldata_cfg).

### RCH26.history-contract: Distinguish current-state migration from complete change history

**P1; M; OPEN extension.** Parent: SMART-N1, SMART-N7. Code: `scope, tailtables, verdict`.

**Trigger:** Fivetran SCD2; SQL Server CT versus CDC.

**Mechanism:** Declare whether the goal requires current final state or every intermediate version/event. Choose CDC, CT, polling or historical snapshots only when their information content satisfies that goal. Carry history cutoffs, deletion/tombstone semantics and retention into proof.

**Acceptance:** Insert-and-delete between polls and multiple updates between syncs cannot be falsely declared complete historical capture; final-state and history verdicts use separate coverage.

Sources: [S039](https://fivetran.com/docs/core-concepts/sync-modes/history-mode), [S100](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview).

### RCH27.stream-metadata: Move event consumers and schemas with stream bytes

**P1; L; OPEN extension.** Parent: SMART-N12, SMART-N13 traffic. Code: `engines/kafka, topology, cutover`.

**Trigger:** Confluent Cluster Linking.

**Mechanism:** Extend Kafka migration proof to topic configs/partition identity, retained ranges, consumer group offsets, ACL policy and schema-registry compatibility. Offset-preserving versus remapped paths are different capabilities; coordinate promotion with fences and active consumers.

**Acceptance:** Consumer restart after promotion has no unaccounted replay/gap, offsets outside retention are refused and topic data equality cannot hide incompatible schema/auth state.

Sources: [S083](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html), [S085](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/cluster-links-cc.html), [S038](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/mirror-topics-cp.html).

### RCH28.document-model: Verify normalized-to-document transformations after updates

**P1; L; OPEN extension.** Parent: RCH17, SMART-N14. Code: `engines/hetero, canon, transforms`.

**Trigger:** MongoDB relational verification.

**Mechanism:** Extend mapping-aware verification for embedding, arrays, path identity, type conversion, ordering and multiplicity. Select indexed lookup/batched fetch/stream compare from measured target shape; verify update/delete propagation for embedded references separately from snapshot equivalence.

**Acceptance:** Equal collection counts with wrong nested paths, missing embedded child or reordered semantically ordered arrays fails; concurrent-source assumptions are explicit.

Sources: [S042](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/), [S043](https://www.mongodb.com/docs/relational-migrator/jobs/creating-jobs/).

### RCH29.cache-semantics: Preserve expiry and eviction behavior across cache migrations

**P1; M; OPEN extension.** Parent: existing Redis ladders, SMART-N14. Code: `engines/redis, decide, cutover`.

**Trigger:** Redis Replica Of and CRDT.

**Mechanism:** Carry TTL as relative/absolute policy with elapsed-time compensation, types, eviction/LFU/LRU metadata where supported and active-active conflict behavior. Separate data sync from client routing. Forbid late full-resync from overwriting a promoted writable target.

**Acceptance:** Near-expiry keys, clock skew, memory pressure and reconnect after cutover preserve approved semantics or declare unavailable fidelity; reverse direction does not flush new writes.

Sources: [S047](https://redis.io/docs/latest/operate/rs/7.8/databases/import-export/replica-of/), [S045](https://redis.io/docs/latest/operate/rc/databases/active-active/), [S046](https://redis.io/docs/latest/integrate/redis-data-integration/).

### RCH30.shard-ddl: Coordinate sharded schema evolution with explicit convergence

**P1; L; OPEN extension.** Parent: SMART-N23, existing reshard. Code: `reshard, schema_guard, topology`.

**Trigger:** TiDB optimistic/pessimistic DDL.

**Mechanism:** Extend topology-aware movement with per-shard schema versions, combined target compatibility and barriers. Choose optimistic progress only when target schema and concurrent writes remain representable; otherwise wait/serialize only affected shards. Validate aggregate keyspace placement.

**Acceptance:** Different shard DDL order, duplicate keys, shard split and coordinator restart neither lose columns nor deadlock silently; every row placed exactly once under the declared rule.

Sources: [S051](https://docs.pingcap.com/tidb/v7.5/feature-shard-merge-optimistic/), [S054](https://vitess.io/docs/24.0/reference/vreplication/movetables/).

### RCH31.placement-advice: Connect assessment to measured target topology and sizing

**P2; L; OPEN extension.** Parent: SMART-N6, SMART-N18. Code: `machine, model, managed, scenarios`.

**Trigger:** Voyager workload/IOPS assessment.

**Mechanism:** Extend machine advice with table/index access patterns, skew, distributed locality and transaction fanout. Recommend candidate colocated/distributed/partitioned target designs and resource sizes from evidence; rehearse alternatives rather than use brand-specific static classes.

**Acceptance:** A design that makes bulk import fast but causes hot shards or expensive cross-partition transactions loses against a better end-to-end workload plan.

Sources: [S149](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/assess-migration/), [S028](https://www.enterprisedb.com/docs/migrating/oracle/edb_migration_tools/).

### RCH32.connector-capability: Model schema drift and limitations per connector version

**P1; M; OPEN extension.** Parent: SMART-N9, RCH09. Code: `capabilities, tools, guide`.

**Trigger:** Lakeflow feature matrices; Airbyte policies; Hevo.

**Mechanism:** Snapshot feature matrices by engine/service/version/connector/mode and distinguish added/deleted/renamed/type-changed columns, old-row backfill and new-table discovery. Unknown or contradictory docs trigger bounded probes and owned debt, not inferred yes.

**Acceptance:** A connector that supports add-column but needs full refresh for rename cannot auto-admit rename; stale connector/version metadata invalidates the decision.

Sources: [S100](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview), [S101](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq), [S108](https://docs.airbyte.com/platform/using-airbyte/schema-change-management), [S040](https://docs.hevodata.com/edge/pipelines/object-and-schema-management/pipeline-schema-management/).

### RCH33.agent-control: Expose resumable constrained migration operations to agents

**P1; M; OPEN extension.** Parent: SMART-N11, existing tooluse. Code: `tooluse, session, autonomous`.

**Trigger:** Google DMS MCP; AWS APIs; Snowflake skills.

**Mechanism:** Build a versioned structured read/plan/execute/reconcile interface over the existing engine, with operation IDs, scope and authorization references. Agents request goals/inspect evidence; deterministic admission and effect tracking remain authoritative. Avoid a second orchestration state machine.

**Acceptance:** Duplicated agent calls, process restart and untrusted database text cannot duplicate writes, widen scope or bypass a refused contract; all outputs map to the same verdict semantics.

Sources: [S016](https://docs.cloud.google.com/database-migration/docs/release-notes), [S145](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_SchemaConversion.html), [S103](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill).

### RCH34.release-intelligence: Keep support and competitor knowledge versioned and falsifiable

**P2; M; OPEN extension.** Parent: SMART-N9, source registry. Code: `tools, capabilities, research metadata`.

**Trigger:** Provider release notes.

**Mechanism:** Keep official URL/version/date/status and changed capability predicates. Documentation updates produce reviewable contract/probe tasks, not automatic installs or production changes. Retain conflicting regional/legacy docs and invalidate unsupported broad claims. Separate GA/Preview/roadmap/marketing/inference.

**Acceptance:** A preview feature never turns into unconditional supported capability, and a vendor rename or redirected documentation does not create fabricated coverage.

Sources: [S109](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html), [S016](https://docs.cloud.google.com/database-migration/docs/release-notes), [S112](https://proxy-hk.tencentcloud.com/ind/document/product/571/42597), [S080](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm).

### RCH35.incumbent-switch: Include transition and rollback costs when replanning

**P1; M; OPEN extension.** Parent: SMART-N5, SMART-N24. Code: `decide, jointplan, costmodel`.

**Trigger:** Striim recovery/parallelism tradeoffs; HVR refresh.

**Mechanism:** Extend current transition pricing with checkpoint granularity, spool drains, transaction barriers, process setup, resource restore and proof regeneration. Keep hysteresis and a validated incumbent; assess p90 savings over remaining work rather than local throughput.

**Acceptance:** A marginally faster path with expensive replay never steals the job; sustained changes cross a reproducible break-even threshold without oscillation.

Sources: [S075](https://striim.com/docs/platform/en/recovering-applications.html), [S006](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh).

### RCH36.verify-method: Select validation methods by what each can actually prove

**P1; M; OPEN extension.** Parent: SMART-N7, existing second readers. Code: `verifiers, scanplan, decide`.

**Trigger:** Validata vectors/record hashes; DVT; SQLines.

**Mechanism:** Give count/aggregate/vector/hash/key-row/external-sort/custom-query methods explicit proof strength, collision/duplicate assumptions, canonicalization and source-cost models. Fast signatures can rule in differences or prioritize exact work; cannot certify stronger claims than their contract.

**Acceptance:** Perfect duplicates, equal-count value changes, floating rounding and hash ambiguity either fail or escalate; count-only unchanged totals cannot produce same.

Sources: [S073](https://www.striim.com/docs/validata/en/validation-types.html), [S088](https://github.com/GoogleCloudPlatform/professional-services-data-validator), [S066](https://www.sqlines.com/sqldata_validation), [S070](https://streams.dbconvert.com/docs/streams/verify-load).

### RCH37.failure-budget: Allocate recovery effort by progress and deadline value

**P1; M; OPEN extension.** Parent: SMART-N15, SMART-N21. Code: `faults, attempts, autonomous`.

**Trigger:** Striim checkpoint costs; IBM apply tradeoffs.

**Mechanism:** Extend classified recovery to allocate retry/probe/reconcile/repair budgets by remaining deadline, irreversibility, progress rate and uncertainty. Treat repeated same-state repairs as a diagnosis failure, not success. Compound faults may invalidate prior cost data.

**Acceptance:** Credential expiry plus WAN partition plus schema drift terminates or recovers with explainable state and bounded waste; no retry loop can outlive all resource validity windows.

Sources: [S075](https://striim.com/docs/platform/en/recovering-applications.html), [S106](https://www.ibm.com/support/pages/node/1102497).

### RCH38.storage-protocol: Validate warehouse and lakehouse commit semantics

**P1; L; OPEN extension.** Parent: SMART-N6, existing warehouse feed. Code: `ship, engines/warehouse, durable`.

**Trigger:** Snowflake Openflow journals; Matillion staging; Lakeflow.

**Mechanism:** Model data-file completeness, journal/manifest positions, atomic publish, partition/schema evolution, tombstones and downstream visibility. Choose staged bulk versus streaming by measured billable work and consistent commit behavior; files present are not committed table state.

**Acceptance:** Crash between object upload, metadata commit and ack yields no orphan visibility or duplicated effects; queryable target coverage matches the source fence.

Sources: [S087](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/connectors/postgres/about), [S078](https://docs.matillion.com/data-productivity-cloud/cdc/docs/cdc-shared-jobs/), [S101](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq).

### RCH39.anti-corruption: Test the verifier against its own blind spots

**P1; L; OPEN extension.** Parent: SMART-N10, existing mutation infrastructure. Code: `canon, storedcode/mutants, tests`.

**Trigger:** Datafold aligned inputs; Mongo mapping verification; Veridata.

**Mechanism:** Extend mutation testing beyond stored code: intentionally remove a filter, duplicate a keyless row, alter canonicalization, reset statistics, truncate a table or omit grants. Independently assert that every claimed coverage class detects its defects. Share no bug-prone parser between test oracle and production path.

**Acceptance:** Mutation score reported per coverage class and engine; surviving mutants are explicit proof debt, not passed tests; false-same regressions block releases.

Sources: [S099](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/), [S042](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/), [S098](https://docs.oracle.com/en/database/goldengate/core/26/gvdug/how-oracle-goldengate-veridata-works.html).

### RCH40.end-to-end-bar: Gate superiority claims on reproducible complete migrations

**P1; L; OPEN extension.** Parent: P0-VALIDATE.unattended.proof, SMART-N25. Code: `autonomous comparison, rehearsal, capabilities`.

**Trigger:** Cross-vendor synthesis.

**Mechanism:** Extend existing endpoint-only acceptance with provider-native/tool baselines under equal proof/scope/load/durability contracts. Record install/setup/manual steps, verified completion p50/p95, source reads, downtime, reverse readiness, actual money/bytes, loss/duplication and forecast calibration. Label simulated/fake/lane/managed-provider evidence separately.

**Acceptance:** At least PG-PG, MySQL-MySQL, PG-MySQL and MySQL-PG whole paths plus compound failure/restart; broader pair claims remain owned gaps until tested. No all-products/all-engines/fastest claims from a feature count.

Sources: [S142](https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html), [S019](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql), [S099](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/).

## 8. Implementation order and evidence gates

**Wave 0 — make existing decisions trustworthy.** Revalidate/fix REV-D1..D5, resolve T1/T2 and retain exact reproductions. Required hard limits and honest guarantee/fact evidence precede more autonomy. Do not infer later fixes from this historical report.

**Wave 1 — contract, facts and effects.** SMART-N1/N2/N10/N11/N17/N21/N22 plus RCH01/07/08/15/16/18/24/33/34/39. Establish scope, per-engine eligibility, secret-safe inputs, exact coverage, durable effect identity and controlled agent calls.

**Wave 2 — movement/proof feedback.** RCH02..11, RCH17, RCH25/26/28/32/35/36/37/38 plus dynamic cost learning and calibrated live replanning. Whole-run tests must combine concurrent source writes, target pressure and interruption.

**Wave 3 — cross-engine/application cutover.** RCH12..14, RCH19..23, RCH27/29/30/31 and deeper consumer/semantic contracts. Do not claim app-ready on data parity alone or failback-ready on a configured reverse job alone.

**Wave 4 — widen engine/service lanes.** Run the same full workflow contract on each actual supported pair. Each added pair earns capabilities and depth through real engine evidence. A narrow but proved pair and a broad but unproved adapter must be labelled differently.

**Release gates:** zero unexplained missing/duplicate effects within the contract; no false same in defect-injection tests; exact scope denominators; bounded source impact; deadline/byte/money admission and runtime checks; deterministic resume; settings/resource cleanup; independent evidence of final readiness; measured verified completion and manual-intervention counts. Speed comparisons hold scope/proof/durability/workload equal, report repeated-run p50/p95 and disclose unavailable baselines. A simulation is not a provider benchmark.

## 9. Correct claims before adding features

- A Merkle root is compact to transfer; building it reads/computes over data, and drilldown depends on disagreement and tree layout. Do not promise constant total work or universal logarithmic repair.
- A finite disk spool cannot absorb an unbounded outage or prevent all source-log growth. Acknowledge only durably retained data and budget retention explicitly.
- EXPLAIN ANALYZE executes statements. Workload checks with side effects need isolated authorized execution; a transaction rollback cannot reverse every external effect.
- Savepoints do not cover every database's DDL, implicit commits, external calls or process/provider changes. Rehearsal cleanup must use each action's real effect model.
- Zero-loss failback requires continuous capture from the exact promotion boundary and a reverse-representable schema/data/transaction model. A configurable standby alone is insufficient.
- Temporarily weaker durability or disabled constraints change failure semantics. A faster ingest mode is admissible only under the explicit contract and restored before readiness.
- Row counts, weak signatures, key-only checks and sampling have different coverage from full exact/mapped-value proof. Same totals never establish same values by themselves.
- The guarantee model's lexicographic winner is not proof it simultaneously attains every dimension's independent maximum. Explicit required minimums must be checked first.
- A product named Zero Downtime, Exactly Once, AI Verification or Self-Healing is a documentation claim with preconditions, not a universal guarantee.
- Research and backlog integration close knowledge/ownership gaps; only implemented and exercised acceptance tests close runtime capability gaps.

## 10. Unresolved research and exclusions

Rivery's legacy docs redirected without usable body; some Informatica and SharePlex opens failed. Their dossiers retain excerpt evidence and require authoritative current manuals before detailed implementation. Cockroach MOLT failback was located at vendor-overview level here; per-source/version reverse semantics remain a research task. Versioned IBM, Debezium, Redis and Ispirer documents are used only for the mechanism/version they describe. SAP/mainframe/provider features were not run. Pricing, legal availability and licensing were not exhaustively audited; product names in the report do not grant redistribution rights or permission to install proprietary components.

The report does not enumerate every private enterprise connector, every release of every product, or all legacy tools. It also does not claim equal proof depth across these dossiers. Continue coverage by source/target/version/scenario and by the lifecycle table, logging missing documents as explicit debt. Do not fill proprietary algorithm gaps with guessed internals.

All source-derived product statements below are paraphrases with URLs; long source manuals were not copied into the deliverable. Local recommendations and acceptance criteria are original engineering analysis.

## 11. Source register

Evidence: page-retrieved means the page was fetched; official-search-excerpt means only a retrieved indexed extract was available. Individual dossier grades govern depth. Duplicate retrieval aliases map to one exact URL. Retrieval date: 2026-10-17 Asia/Bangkok. Some vendor docs are historical and remain marked in the dossier.

- **S001** — [About Integrated Replicat](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/replicat-integrated-replicat.html); page-retrieved.

- **S002** — [Configuring Oracle GoldenGate Replicat](https://docs.oracle.com/en/middleware/goldengate/core/21.3/oracle-db/configuring-oracle-goldengate-apply.html); official-search-excerpt.

- **S003** — [Automatic Conflict Detection and Resolution](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/administer-automatic-cdr.html?source=%3Aso%3Ach%3Aor%3Aawr%3A%3A%3A); official-search-excerpt.

- **S004** — [Veridata 23c (23.5.0.0.0)](https://docs.oracle.com/en/middleware/goldengate/veridata/23/gvdug/oracle-goldengate-veridata.pdf); official-search-excerpt.

- **S005** — [Compare - HVR 6 | Fivetran Documentation](https://fivetran.com/docs/hvr6/getting-started/concepts/compare); page-retrieved.

- **S006** — [Refresh - HVR 6 | Fivetran Documentation](https://fivetran.com/docs/hvr6/getting-started/concepts/refresh); page-retrieved.

- **S007** — [Change Processing Tuning #Change Processing Tuning | Qlik Replicate Help](https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_applychangtunestab.htm); page-retrieved.

- **S008** — [Full Load and CDC processes #Full Load and CDC processes | Qlik Replicate Help](https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Introduction/FullCDCProcesses.htm); page-retrieved.

- **S009** — [Adaptive apply](https://www.ibm.com/docs/en/iirfz/11.3.0?topic=processing-adaptive-apply); official-search-excerpt.

- **S010** — [How to Configure a Data Verification Task - Data Transmission Service - Alibaba Cloud Documentation Center](https://www.alibabacloud.com/help/en/dts/user-guide/enable-data-verification); page-retrieved.

- **S011** — [Sync Tables Without Primary Keys via Exactly-Once Write for Conflict-Free Replication - DTS - Alibaba Cloud](https://www.alibabacloud.com/help/en/dts/user-guide/synchronize-tables-without-primary-keys-and-unique-constraints-from-the-source-database); page-retrieved.

- **S012** — [how to configure a data verification task - Data Transmission Service - Alibaba Cloud Documentation Center](https://www.alibabacloud.com/help/doc-detail/452353.html); official-search-excerpt.

- **S013** — [View DTS data verification task details and results - Data Transmission Service - Alibaba Cloud Documentation Center](https://www.alibabacloud.com/help/en/dts/user-guide/view-data-verification-details); page-retrieved.

- **S014** — [Data Comparison (Comparing Synchronization Items)_Task Management_Real-Time Synchronization_Data Replication Service-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/realtimesyn-drs/drs_10_0012.html); official-search-excerpt.

- **S015** — [Real-Time Migration_Functions and Features_Service Overview_Data Replication Service-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/productdesc-drs/drs_01_0301.html); official-search-excerpt.

- **S016** — [Database Migration Service release notes  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/release-notes); page-retrieved.

- **S017** — [Migration Service - Premigration Validations - Azure Database for PostgreSQL | Microsoft Learn](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-premigration-migration-service); official-search-excerpt.

- **S018** — [Known Issues And Limitations For The Migration Service - Azure Database for PostgreSQL | Microsoft Learn](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/concepts-known-issues-migration-service?source=recommendations); page-retrieved.

- **S019** — [Best Practices to Migrate into Flexible Server - Azure Database for PostgreSQL | Microsoft Learn](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql); page-retrieved.

- **S020** — [Database replication software | SharePlex | Quest](https://www.quest.com/products/shareplex); official-search-excerpt.

- **S021** — [SharePlex 10.2.1 - Administration Guide](https://support.quest.com/technical-documents/doc2094948); official-search-excerpt.

- **S022** — [SharePlex 11.4 - Administration Guide](https://support.quest.com/technical-documents/shareplex/11.4/administration-guide/34); official-search-excerpt.

- **S023** — [SharePlex 10.1 - Administration Guide](https://support.quest.com/technical-documents/doc1629965); official-search-excerpt.

- **S024** — [Handling source schema changes](https://docs.informatica.com/integration-cloud/data-ingestion-and-replication/current-version/application-ingestion-and-replication/application-ingestion-and-replication/handling-source-schema-changes.html); official-search-excerpt.

- **S025** — [Informatica® Data Ingestion and Replication](https://docs.informatica.com/content/dam/source/GUID-5/GUID-564A497D-5045-4D3A-92A2-F1C5D0D1B030/50/en/CMI_November2024_DatabaseIngestionAndReplication_en.pdf); official-search-excerpt.

- **S026** — [Connect CDC (formerly SQData) – Precisely Support](https://support.precisely.com/products/connect-cdc-formerly-sqdata/); official-search-excerpt.

- **S027** — [Replication - connect_cdc_sqdata - Latest](https://help.precisely.com/r/t/1027124403/2023-12-31/Connect-CDC-SQData/pub/Latest/en-US/Connect-CDC-SQData-Apply-engine/Replication?contentId=2sJaR~97vXktdxbV~br72A); page-retrieved.

- **S028** — [EDB Docs - EDB capabilities for the migration journey](https://www.enterprisedb.com/docs/migrating/oracle/edb_migration_tools/); page-retrieved.

- **S029** — [SnowConvert AI - About | Snowflake Documentation](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/about); official-search-excerpt.

- **S030** — [SnowConvert AI - Getting Started | Snowflake Documentation](https://docs.snowflake.com/en/en/migrations/snowconvert-docs/general/getting-started/README); official-search-excerpt.

- **S031** — [SnowConvert AI - Validation | Snowflake Documentation](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/validation/README); page-retrieved.

- **S032** — [SnowConvert AI - Assessment Report | Snowflake Documentation](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/review-results/reports/assessment-report/README); official-search-excerpt.

- **S033** — [How To Accelerate Data Warehouse Migrations With SnowConvert AI](https://www.snowflake.com/en/developers/guides/how-to-accelerate-data-warehouse-migrations-with-snowconvert-ai/); official-search-excerpt.

- **S034** — [SnowConvert AI - Scopes | Snowflake Documentation](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/review-results/snowconvert-scopes); page-retrieved.

- **S035** — [Snowflake Migration Tools | Snowflake Documentation](https://docs.snowflake.com/en/migrations/README); page-retrieved.

- **S036** — [SQL Server connector limitations | Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits); official-search-excerpt.

- **S037** — [Tutorial: Migrate Data with Cluster Linking on Confluent Platform | Confluent Documentation](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/migrate-cp.html); official-search-excerpt.

- **S038** — [Manage Mirror Topics for Cluster Linking on Confluent Platform | Confluent Documentation](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/mirror-topics-cp.html); official-search-excerpt.

- **S039** — [Fivetran History Mode](https://fivetran.com/docs/core-concepts/sync-modes/history-mode); official-search-excerpt.

- **S040** — [Pipeline Schema Management - Hevo Data](https://docs.hevodata.com/edge/pipelines/object-and-schema-management/pipeline-schema-management/); page-retrieved.

- **S041** — [Use Data Verification - Relational Migrator - MongoDB Docs](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/use-data-verification/); official-search-excerpt.

- **S042** — [Data Verification - Relational Migrator - MongoDB Docs](https://www.mongodb.com/docs/relational-migrator/jobs/data-verification/); page-retrieved.

- **S043** — [Create a Migration Job - Relational Migrator - MongoDB Docs](https://www.mongodb.com/docs/relational-migrator/jobs/creating-jobs/); official-search-excerpt.

- **S044** — [Migrate a database to Active-Active | Docs](https://redis.io/docs/latest/operate/rs/databases/import-export/migrate-to-active-active/); official-search-excerpt.

- **S045** — [Active-Active Redis | Docs](https://redis.io/docs/latest/operate/rc/databases/active-active/); page-retrieved.

- **S046** — [Redis Data Integration | Docs](https://redis.io/docs/latest/integrate/redis-data-integration/); page-retrieved.

- **S047** — [Replica Of geo-distributed Redis | Docs](https://redis.io/docs/latest/operate/rs/7.8/databases/import-export/replica-of/); page-retrieved.

- **S048** — [Steps to perform live migration of your database with fall-back using YugabyteDB Voyager | YugabyteDB Docs](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-back/); page-retrieved.

- **S049** — [Steps to perform live migration with fall-forward using YugabyteDB Voyager | YugabyteDB Docs](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-fall-forward/); official-search-excerpt.

- **S050** — [Database Migrations to CockroachDB & Cockroach Continuum by Cockroach Labs](https://www.cockroachlabs.com/migrations/); official-search-excerpt.

- **S051** — [Merge and Migrate Data from Sharded Tables in Optimistic Mode - v7.5 | TiDB Docs](https://docs.pingcap.com/tidb/v7.5/feature-shard-merge-optimistic/); page-retrieved.

- **S052** — [DM Safe Mode - dev | TiDB Docs](https://docs.pingcap.com/tidb/dev/dm-safe-mode/); official-search-excerpt.

- **S053** — [docs/tidb-cloud/migrate-sql-shards.md at master · pingcap/docs · GitHub](https://github.com/pingcap/docs/blob/master/tidb-cloud/migrate-sql-shards.md); official-search-excerpt.

- **S054** — [The Vitess Docs | MoveTables](https://vitess.io/docs/24.0/reference/vreplication/movetables/); official-search-excerpt.

- **S055** — [The Vitess Docs | --- SwitchTraffic](https://vitess.io/docs/archive/11.0/reference/vreplication/switchtraffic/); official-search-excerpt.

- **S056** — [clickhouse-docs/docs/integrations/data-ingestion/clickpipes/postgres/lifecycle.md at main · ClickHouse/clickhouse-docs · GitHub](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/clickpipes/postgres/lifecycle.md); page-retrieved.

- **S057** — [Parallelized Initial Load for CDC-based streaming from Postgres](https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres); official-search-excerpt.

- **S058** — [Making large Postgres migrations practical: 1TB in 2 hours with PeerDB | ClickHouse](https://clickhouse.com/blog/practical-postgres-migrations-at-scale-peerdb); official-search-excerpt.

- **S059** — [Documentation archive [Ispirer Migration Knowledge Base]](https://wiki.ispirer.com/sqlways/documentation-archive); official-search-excerpt.

- **S060** — [Assessment Wizard for Databases [Ispirer Migration Knowledge Base]](https://wiki.ispirer.com/sqlways/migration-assessment/database); official-search-excerpt.

- **S061** — [SQLWays Database Migration Tool | Database Schema Conversion](https://www.ispirer.com/products/sqlways-database-migration); official-search-excerpt.

- **S062** — [Ispirer Migration Knowledge Base [Ispirer Migration Knowledge Base]](https://wiki.ispirer.com/); official-search-excerpt.

- **S063** — [Automated Database Migration Software | Ispirer Toolkit | SQL DB Migration Tool](https://www.ispirer.com/products/database-migration); official-search-excerpt.

- **S064** — [Introduction](https://doc.ispirer.com/sqlways/Output/SQLWays-1-002.html); official-search-excerpt.

- **S065** — [Command-line Options [Ispirer Migration Knowledge Base]](https://wiki.ispirer.com/sqlways/command-line/options); page-retrieved.

- **S066** — [Data Validation - SQLines Tools](https://www.sqlines.com/sqldata_validation); page-retrieved.

- **S067** — [SQLines Data Tool - Configuration File Reference - SQLines Tools](https://www.sqlines.com/sqldata_cfg); page-retrieved.

- **S068** — [SQLines Data - Database Migration and ETL - SQLines Tools](https://sqlines.com/sqldata); page-retrieved.

- **S069** — [Change Data Capture (CDC) Mode | DBConvert Streams Docs](https://streams.dbconvert.com/docs/integration/what-is-cdc); page-retrieved.

- **S070** — [Review and Verify a Load | DBConvert Streams Docs](https://streams.dbconvert.com/docs/streams/verify-load); page-retrieved.

- **S071** — [About Hyper-Q](https://gcp.docs.datometry.com/en/7086-about-hyper-q.html); page-retrieved.

- **S072** — [Datometry Documentation](https://gcp.docs.datometry.com/); official-search-excerpt.

- **S073** — [Validation Types](https://www.striim.com/docs/validata/en/validation-types.html); page-retrieved.

- **S074** — [Product architecture](https://striim.com/docs/validataplatform/en/product-architecture.html); official-search-excerpt.

- **S075** — [Recovering applications](https://striim.com/docs/platform/en/recovering-applications.html); page-retrieved.

- **S076** — [Using Striim Open Processors](https://striim.com/docs/platform/en/using-striim-open-processors.html); official-search-excerpt.

- **S077** — [Schema drift - Matillion ETL Docs](https://docs.matillion.com/data-productivity-cloud/cdc/docs/cdc-schema-drift/); page-retrieved.

- **S078** — [CDC Shared Job - Matillion ETL Docs](https://docs.matillion.com/data-productivity-cloud/cdc/docs/cdc-shared-jobs/); page-retrieved.

- **S079** — [--------------------------------------------------------------------------------](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Guides.htm); official-search-excerpt.

- **S080** — [--------------------------------------------------------------------------------](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm); page-retrieved.

- **S081** — [Schema Drifting](https://docs.rivery.io/docs/schema-drift); page-retrieved.

- **S082** — [Cluster Linking Automatic Failover Tutorial for Active-Passive Deployments | Confluent Documentation](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-automatic-failover-tutorial.html); page-retrieved.

- **S083** — [Migrate data with Cluster Linking on Confluent Cloud | Confluent Documentation](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html); page-retrieved.

- **S084** — [Disaster Recovery and Failover on Confluent Cloud | Confluent Documentation](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html); official-search-excerpt.

- **S085** — [Configure and Manage Cluster Links on Confluent Cloud | Confluent Documentation](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/cluster-links-cc.html); page-retrieved.

- **S086** — [Debezium connector for PostgreSQL :: Debezium Documentation](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html); page-retrieved.

- **S087** — [About Openflow Connector for PostgreSQL | Snowflake Documentation](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/connectors/postgres/about); page-retrieved.

- **S088** — [GitHub - GoogleCloudPlatform/professional-services-data-validator: Utility to compare data between homogeneous or heterogeneous environments to ensure source and target tables match · GitHub](https://github.com/GoogleCloudPlatform/professional-services-data-validator); page-retrieved.

- **S089** — [Migration Service in Azure Database for PostgreSQL flexible server - Azure Database for PostgreSQL | Microsoft Learn](https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/overview-migration-service-postgresql?source=recommendations&tabs=portal%2Coffline); official-search-excerpt.

- **S090** — [Known Issues: Online Migrations from PostgreSQL to Azure Database for PostgreSQL - Azure Database Migration Service | Microsoft Learn](https://learn.microsoft.com/en-us/azure/dms/known-issues-azure-postgresql-online); official-search-excerpt.

- **S091** — [Overview of Log Replay Service - Azure SQL Managed Instance | Microsoft Learn](https://learn.microsoft.com/th-th/Azure/Azure-sql/managed-instance/log-replay-service-overview?view=azuresql); page-retrieved.

- **S092** — [Migrate SQL Server Databases by Using Log Replay Service - Azure SQL Managed Instance | Microsoft Learn](https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/log-replay-service-migrate?view=azuresql); official-search-excerpt.

- **S093** — [az sql midb log-replay | Microsoft Learn](https://learn.microsoft.com/cli/azure/sql/midb/log-replay); official-search-excerpt.

- **S094** — [创建数据一致性校验（MySQL 系）](https://www.tencentcloud.com/document/product/571/42724/); page-retrieved.

- **S095** — [Creating Data Consistency Check Task（MySQL）](https://intl.cloud.tencent.com/ind/document/product/571/42724); official-search-excerpt.

- **S096** — [数据传输服务 创建数据一致性校验_腾讯云](https://cloud.tencent.com/document/product/571/85797); page-retrieved.

- **S097** — [Data Comparison (Comparing DR Items)_Task Management_Real-Time Disaster Recovery_Data Replication Service-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/realtimedr-drs/drs_02_0033.html); page-retrieved.

- **S098** — [How Oracle GoldenGate Veridata Works?](https://docs.oracle.com/en/database/goldengate/core/26/gvdug/how-oracle-goldengate-veridata-works.html); page-retrieved.

- **S099** — [What is the Datafold Migration Agent, and how does it work?](https://www.datafold.com/blog/what-is-the-datafold-migration-agent/); page-retrieved.

- **S100** — [Microsoft SQL Server ingestion connector | Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview); page-retrieved.

- **S101** — [Managed connector FAQs | Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/faq); page-retrieved.

- **S102** — [SnowConvert AI - Conversion | Snowflake Documentation](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/getting-started/running-snowconvert/conversion/README); page-retrieved.

- **S103** — [Snowflake Migration Skill | Snowflake Documentation](https://docs.snowflake.com/ja/migrations/snowconvert-docs/general/user-guide/snowconvert/migration-skill/skill); page-retrieved.

- **S104** — [Mapping using Adaptive Apply](https://www.ibm.com/docs/vi/SSTRGZ_11.3.3/com.ibm.cdcdoc.mcadminguide.doc/concepts/mappingadaptiveapply.html); page-retrieved.

- **S105** — [Suppressing the apply of row operations](https://www.ibm.com/docs/en/idr/11.4.0?topic=operations-suppressing-apply-row); official-search-excerpt.

- **S106** — [IBM Data Replication Change Data Capture (CDC) Best Practices](https://www.ibm.com/support/pages/node/1102497); page-retrieved.

- **S107** — [Change Data Capture (CDC) | Airbyte Docs](https://docs.airbyte.com/platform/understanding-airbyte/cdc); page-retrieved.

- **S108** — [Schema change management | Airbyte Docs](https://docs.airbyte.com/platform/using-airbyte/schema-change-management); page-retrieved.

- **S109** — [AWS DMS release notes - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html); page-retrieved.

- **S110** — [Known limitations and recommendations  |  Database Migration Service for heterogeneous Oracle to Cloud SQL for PostgreSQL  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/oracle-to-postgresql/known-limitations); official-search-excerpt.

- **S111** — [Creating a Data Consistency Check Task](https://www-sg.tencentcloud.com/ind/document/product/571/79989); official-search-excerpt.

- **S112** — [Release Notes](https://proxy-hk.tencentcloud.com/ind/document/product/571/42597); page-retrieved.

- **S113** — [Introduction to Zero Downtime Migration](https://docs.oracle.com/en/database/oracle/zero-downtime-migration/26.1/zdmug/introduction-to-zero-downtime-migration.html); official-search-excerpt.

- **S114** — [Introduction to Zero Downtime Migration](https://docs.oracle.com/en/database/oracle/zero-downtime-migration/21.2/zdmug/introduction-to-zero-downtime-migration.html); official-search-excerpt.

- **S115** — [Migrate Oracle Databases to OCI | Oracle](https://www.oracle.com/database/cloud-migration/); official-search-excerpt.

- **S116** — [Switchover](https://docs.oracle.com/en/database/oracle/oracle-database/26/dgbkr/switchover.html); official-search-excerpt.

- **S117** — [SAP LT Replication Server Cockpit - SAP Documentation](https://help.sap.com/saphelp_slt200/helpdata/en/9b/9b67560889225ae10000000a4450e5/content.htm?no_cache=true); official-search-excerpt.

- **S118** — [Understanding the Data Transfer Process | SAP Help Portal](https://help.sap.com/docs/SAP_LANDSCAPE_TRANSFORMATION_REPLICATION_SERVER/007c373fcacb4003b990c6fac29a26e4/ee2e5f564ca7c54de10000000a441470.html?locale=en-US); official-search-excerpt.

- **S119** — [The IBM Replication Tools by Component](https://www.ibm.com/docs/en/db2/12.1.x?topic=options-replication-tools-by-component); official-search-excerpt.

- **S120** — [Q Replication - Q replication](https://www.ibm.com/docs/en/idr/11.4.0?topic=po-q-replication); official-search-excerpt.

- **S121** — [SQL Server Migration Assistant - SQL Server | Microsoft Learn](https://learn.microsoft.com/en-us/sql/ssma/sql-server-migration-assistant?view=sql-server-ver16); official-search-excerpt.

- **S122** — [Assessment Report - SQL Server | Microsoft Learn](https://learn.microsoft.com/en-us/sql/ssma/shared/assessment-report?view=sql-server-ver17); official-search-excerpt.

- **S123** — [Access to SQL Server: Migration guide - SQL Server | Microsoft Learn](https://learn.microsoft.com/th-th/sql/sql-server/migrate/guides/access-to-sql-server?view=sql-server-ver16); official-search-excerpt.

- **S124** — [Migration tools  |  Spanner  |  Google Cloud Documentation](https://docs.cloud.google.com/spanner/docs/migration-tools?hl=en); official-search-excerpt.

- **S125** — [Migration tools  |  Spanner  |  Google Cloud Documentation](https://docs.cloud.google.com/spanner/docs/migration-tools); official-search-excerpt.

- **S126** — [Configure fallback and cutover  |  Spanner  |  Google Cloud Documentation](https://docs.cloud.google.com/spanner/docs/cutover-fallback); official-search-excerpt.

- **S127** — [AWS DMS Fleet Advisor end of support - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/dms_fleet.advisor-end-of-support.html); official-search-excerpt.

- **S128** — [Database evaluation - Data Transmission Service - Alibaba Cloud Documentation Center](https://www.alibabacloud.com/help/en/dts/user-guide/database-evaluation/); official-search-excerpt.

- **S129** — [How to perform a database assessment and analysis - Data Transmission Service - Alibaba Cloud Documentation Center](https://www.alibabacloud.com/help/en/dts/user-guide/evaluate-a-database); official-search-excerpt.

- **S130** — [Database Evaluation_User Guide_Database and Application Migration UGO-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/usermanual-ugo/ugo_03_0006.html); official-search-excerpt.

- **S131** — [What Is UGO?_Service Overview_Database and Application Migration UGO-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/productdesc-ugo/ugo_01_0002.html); official-search-excerpt.

- **S132** — [Database Migration and Verification_Database Schema Migration_Getting Started_Database and Application Migration UGO-Huawei Cloud](https://support.huaweicloud.com/intl/en-us/qs-ugo/ugo_02_0010.html); official-search-excerpt.

- **S133** — [ADAM Overview: Accelerate Oracle-to-Cloud Migration - Data Transmission Service - Alibaba Cloud](https://www.alibabacloud.com/help/en/dts/user-guide/adam-overview); official-search-excerpt.

- **S134** — [EDB Docs - EDB Postgres AI Migration and ETL](https://www.enterprisedb.com/docs/edb-postgres-ai/migration-etl/); official-search-excerpt.

- **S135** — [EDB Docs - LiveCompare v3](https://www.enterprisedb.com/docs/livecompare/latest/); official-search-excerpt.

- **S136** — [EDB Docs - LiveCompare v3 - Supported technologies](https://www.enterprisedb.com/docs/livecompare/latest/supported_technologies/); official-search-excerpt.

- **S137** — [EDB Docs - LiveCompare v3 - EDB Postgres Distributed support](https://www.enterprisedb.com/docs/livecompare/latest/bdr_support/); official-search-excerpt.

- **S138** — [EDB Docs - Bulk loading data into PGD clusters](https://www.enterprisedb.com/docs/edb-postgres-ai/cloud-service/managing_your_cluster/migration/dha_bulk_migration/); official-search-excerpt.

- **S139** — [Enhancements](https://docs.oracle.com/en/database/goldengate/core/26/release-notes/new-enhancements.html); official-search-excerpt.

- **S140** — [Integrate - HVR 6 | Fivetran Documentation](https://fivetran.com/docs/hvr6/action-reference/integrate); official-search-excerpt.

- **S141** — [When to Use Burst vs. Continuous Integration - HVR 6 | Fivetran Documentation](https://fivetran.com/docs/hvr6/faq/expert-notes-best-practices/burst-vs-continuous-integration); official-search-excerpt.

- **S142** — [Migrating databases to their Amazon RDS equivalents with AWS DMS - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html); page-retrieved.

- **S143** — [AWS DMS data resync - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.DataResync.html); page-retrieved.

- **S144** — [AWS DMS data validation - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html); page-retrieved.

- **S145** — [Converting database schemas using DMS Schema Conversion - AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_SchemaConversion.html); page-retrieved.

- **S146** — [Failback migrations  |  Database Migration Service for heterogeneous SQL Server to Cloud SQL for PostgreSQL  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/failback-migration); page-retrieved.

- **S147** — [Convert SQL Server code and schema with Gemini assistance  |  Database Migration Service for heterogeneous SQL Server to Cloud SQL for PostgreSQL  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/code-conversion-with-gemini); page-retrieved.

- **S148** — [Known limitations and recommendations  |  Database Migration Service for heterogeneous SQL Server to Cloud SQL for PostgreSQL  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/sqlserver-to-csql-pgsql/known-limitations); page-retrieved.

- **S149** — [YB Voyager Migration Assessment | YugabyteDB Docs](https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/assess-migration/); page-retrieved.

- **S150** — [Convert SQL with Database Migration Service  |  Google Cloud Documentation](https://docs.cloud.google.com/database-migration/docs/convert-sql-with-dms); official-search-excerpt.

- **S151** — [Azure MySQL online migration](https://learn.microsoft.com/en-us/azure/dms/tutorial-mysql-azure-external-to-flex-online-portal); official-search-excerpt.

