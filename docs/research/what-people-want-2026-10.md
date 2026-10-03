# What people who migrate databases want (demand side, 2026-10)

**Scope.** These are the voices of people who move, replicate, sync or verify databases, not vendor claims. Sources run from 2023 to 2026. Older sources are marked with their year.

**Limits.** Reddit, G2, TrustRadius and Gartner Peer Insights refused our reader (blocked or HTTP 403), so none of them is used here. The weight moved to Hacker News, AWS re:Post, DBA Stack Exchange and Stack Overflow, GitHub issues, PeerSpot reviews, mailing lists, postmortems and surveys.

Every quote is verbatim and under 15 words. Text without quotation marks is a paraphrase. Vendor material is labeled as vendor material.

---

## 1. Top pain points (ranked by frequency × severity)

### P1. The tool says "green" while data is lost or wrong
**Who:** backend and platform engineers, data engineers, DBAs.

**Evidence:**
- **re:Post (2026).** After a Redshift patch, CDC broke for about 24 Postgres sources while "The DMS tasks are all green stating replication is ongoing". Another user moved workloads away "as DMS is not working reliably atm" ([re:Post](https://repost.aws/questions/QU_jYRLLExSlqHkmRXp05K8A/dms-cdc-to-redshift-three-bugs-making-cdc-non-functional)).
- **Smily (2024).** A run showed "Load Complete", but every table was empty: a 267 GB database arrived as 5.3 MB ([Smily](https://www.smily.com/engineering/rds-database-migration-series---a-horror-story-of-using-aws-dms-with-a-happy-ending)).
- **HN (2024).**
  - DMS "silently corrupted data" ([rjh29](https://news.ycombinator.com/item?id=39049624)).
  - "Silent, unbuggable errors" ([troublebucket](https://news.ycombinator.com/item?id=39050346)).
  - A bug turned every `false` boolean into `true` on the target ([mwarkentin](https://news.ycombinator.com/item?id=39071266)).
- **LootLocker postmortem (2026).** In-flight batches were "dropped and never replayed". They had "No monitoring or alerting on the health or replication lag" ([LootLocker](https://lootlocker.com/blog/db-postmortem-data-loss-during-database-migration)).
- **Debezium.** Events went missing after a long test ([SO 2023](https://stackoverflow.com/questions/75267176)).
- **Cassandra (DBA SE, 2024).** Repair reported the nodes in sync, yet a table counted 140k on the source and 85k on the target ([DBA SE](https://dba.stackexchange.com/questions/343874)).

**Workarounds:** a test record written and watched arriving ([GOV.UK](https://gds.blog.gov.uk/2024/01/17/how-we-migrated-our-postgresql-database-with-11-seconds-downtime/)), home-made scripts, and keeping the source alive until checks pass.

**Where tools fail:** task status reports that the process is alive, not that the data is correct.

### P2. Verification is homegrown, thin, or stops the world
- **Thin checks (2020).** A team "only checked max(id) and did some counts". A bug report later found the missing rows ([pgsql-general](https://www.postgresql.org/message-id/CAMX1ThgHK4i86qRy8aCqGbnW8VJz-wTQg5ugPA1SZaTG3B_UzA%40mail.gmail.com)).
- **Hand-built hashing (2023).** A team built its own MD5-per-row tool for Netezza → Synapse. NVARCHAR hashed differently on the two sides, and floats and BLOBs broke it ([DBA SE](https://dba.stackexchange.com/questions/328757)).
- **Freezing writes to compare (2023).** One team attached physical replicas, paused replay and checksummed both sides for "<1s write downtime" ([AlisdairO](https://news.ycombinator.com/item?id=38628663)).
- **Continuous validation (2026).** A user asked "Is there no demand for continuous data validation". The author replied that a full diff without writes would mean "days or even weeks" of stopped writes ([HN thread](https://news.ycombinator.com/item?id=47037781)).
- **Separate authors (2021).** At Notion, the migration and verification logic were "implemented by different people" ([Notion](https://www.notion.com/blog/sharding-postgres-at-notion)).
- **Vendor checkers hedge.**
  - Tencent DTS calls its consistency check only auxiliary ("辅助"). It skips keyless tables over 50,000 rows, does not see DDL, and fails on any query over 10 minutes ([Tencent docs](https://cloud.tencent.com/document/product/571/62564)).
  - AWS warns that CDC latency and collation cause "false mismatches" ([re:Post KC](https://repost.aws/knowledge-center/dms-task-validation-failed-stuck)).
  - mongosync's verifier needs 10 GB plus 500 MB per million documents, and "cannot be resumed" ([MongoDB docs](https://www.mongodb.com/docs/mongosync/current/reference/verification/embedded/)).
- **Cross-engine hashing errs both ways (2024).** In Google DVT, "two identical time fields fail a row hash", and very different values can pass ([DVT #1301](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1301)).

**What failed them:** data-diff was archived on May 17, 2024 ([repo](https://github.com/datafold/data-diff)). Its fork has open usability and scale issues ([reladiff #68](https://github.com/erezsh/reladiff/issues/68), [#83](https://github.com/erezsh/reladiff/issues/83)).

### P3. The mover leaves things behind: sequences, indexes, constraints, DDL
- **RevenueCat (2022).** Root cause: "state of sequences would not be copied". About 2 hours of failed entitlements followed ([RevenueCat](https://www.revenuecat.com/blog/engineering/postmortem-aurora-postgres-migration)).
- **Sequences again.** "You also need to update the sequences" ([postpawl](https://news.ycombinator.com/item?id=39049421)). The same warning came up for pg_easy_replicate ([jreed91](https://news.ycombinator.com/item?id=36406586)).
- **Smily (2024).** "all secondary indexes were missing!"; foreign keys and sequences too.
- **Logical replication (2025).** One user wishes for "a complete 1:1 copy of the database, including DDL, sequences". Today schema drift gives "no error, no indication" ([briffle](https://news.ycombinator.com/item?id=42878823)).
- **Slot and snapshot by hand (2025).** A DBA aligning a slot with a snapshot manually got primary-key conflicts and a slot that never advanced ([DBA SE](https://dba.stackexchange.com/questions/349367)).

**Workarounds:** `pg_dump` pre-data and post-data sections, plus sequence scripts written in advance.

### P4. Type, LOB and time fidelity across engines
- **LOBs and text types.**
  - DMS "would not migrate text and json types", and AWS support had no answer ([sharadov](https://news.ycombinator.com/item?id=39051712)).
  - Character columns were treated as CLOBs ([troublebucket](https://news.ycombinator.com/item?id=39050346)).
  - Airbyte has sent a placeholder instead of TOAST values since 2023 ([#30518](https://github.com/airbytehq/airbyte/issues/30518)).
- **Silent type change (2024).** Unbounded varchar became varchar(8000) and truncated values (Smily).
- **Time zones (2023).** DMS forced UTC on Eastern-time DB2 timestamps ([DBA SE](https://dba.stackexchange.com/questions/322946)).
- **Renamed column (2025).** A wrong target column name made data fall back to defaults, which caused double processing ([sethammons](https://news.ycombinator.com/item?id=45157366)).

### P5. Schema changes during replication break the pipeline
- "any schema changes totally messed it up" ([tibbon](https://news.ycombinator.com/item?id=39050240)).
- PeerDB at launch "doesn't handle schema changes today". A user asked for documented "day-2" limits ([jmg_](https://news.ycombinator.com/item?id=36899017)).
- **DMS Serverless (2026).** New columns were never picked up. The user asked whether recreating the replication is "the only reliable way" ([re:Post](https://repost.aws/questions/QUaGLeAghcSqKcRH6Wu9V97g/dms-serverless-cdc-task-not-capturing-newly-added-postgresql-columns)).

**Workarounds:** freeze schema migrations, recreate tasks, run snapshots on weekends.

### P6. Opaque failures, restarts and fragility
- **PeerSpot reviewers on DMS.**
  - Errors are "very ambiguous and sometimes misleading" ([PeerSpot](https://www.peerspot.com/products/aws-database-migration-service-pros-and-cons)).
  - Messages do not say which component failed.
  - They want to resume from a checkpoint rather than restart ([PeerSpot Q&A](https://www.peerspot.com/questions/what-needs-improvement-with-aws-database-migration-service)).
- **AWS itself (vendor, 2026).** Users struggle with "interpreting error messages". AWS's answer is Amazon Q ([AWS blog](https://aws.amazon.com/blogs/database/accelerate-your-database-migration-journey-with-ai-powered-aws-dms/)).
- **pgcopydb (2026).** "After fighting crash after crash", a user rewrote it in Go ([tbarbugli](https://news.ycombinator.com/item?id=49549604)).
- **Months lost (2024).** DMS "ate months of time" ([tibbon](https://news.ycombinator.com/item?id=39050236)).
- **Bespoke fixes.** One team "just did logical replication with bespoke fixes" ([Spivak](https://news.ycombinator.com/item?id=39049713)).

### P7. Cutover risk and the way back
- **RevenueCat's lesson:** "replay writes during dry-run tests".
- **Figma (2024):** "Avoid one-way migrations", with reverse replication kept ready ([Figma](https://www.figma.com/blog/how-figmas-databases-team-lived-to-tell-the-scale/)).
- **Rehearsal pays.**
  - A live replica on the new version was "invaluable for verifying our workloads" ([Knock](https://news.ycombinator.com/item?id=38620797)).
  - A dry run "gave us a lot more confidence" ([Ozzie_osman](https://news.ycombinator.com/item?id=39050205)).
- **Lock queuing at cutover.** Queued locks hurt more than the DDL itself. `lock_timeout` plus retry was the fix ([kettlecrisp99](https://news.ycombinator.com/item?id=49854576)). gh-ost cut-overs still hit lock-wait timeouts ([#1364](https://github.com/github/gh-ost/issues/1364)).

### P8. Load and resource cliffs on production
- **Lost positions after upgrades (2023).** Reconnecting Debezium after an upgrade is "always painful" ([TechIsCool](https://news.ycombinator.com/item?id=38621161)).
- **Wrong LSN (2024).** Airbyte committed the wrong LSN at the initial snapshot, which caused repeated failures or full refreshes ([#49803](https://github.com/airbytehq/airbyte/issues/49803)).
- **Snapshot locks (2023).** Locking a large table during the initial snapshot "can be problematic" ([dgea005](https://news.ycombinator.com/item?id=36903040)).
- **Slow catch-up (2023).** Even at about 50 GB, catch-up "may take hours" ([take-five](https://news.ycombinator.com/item?id=36408029)).

### P9. Correct but slower afterwards
- A query went from 300 ms to 4.7 s after MySQL 5.7 → 8.4 ([DBA SE 2025](https://dba.stackexchange.com/questions/346179)).
- A MariaDB → Aurora job became 5–10× slower, with COMMITs taking 150–300 ms ([DBA SE 2026](https://dba.stackexchange.com/questions/349561)).
- After `pg_upgrade` the planner has no statistics ([pilif](https://news.ycombinator.com/item?id=42884750)).

### P10. Cost, re-imports, unpredictable duration
- Re-imports became "a somewhat regular occurrence" ([danpalmer](https://news.ycombinator.com/item?id=36848350)).
- Fivetran's per-active-row pricing "quickly adds up" ([shrikant](https://news.ycombinator.com/item?id=40966039)).
- A full re-sync is "painfully slow" ([oskarpearson](https://news.ycombinator.com/item?id=36899647)).

---

## 2. Unmet needs (explicitly asked for)

1. **Verification that keeps up with CDC until the switch, without stopping writes.**
   - **Asked for:** continuous validation ([willquack](https://news.ycombinator.com/item?id=47087563)). One team built a physical-replica freeze to get it (AlisdairO).
   - **Why tools fall short:**
     - VDiff compares one point in time.
     - DMS validation reports false mismatches under latency.
     - Tencent's check does not see DDL.
     - mongosync's verifier restarts from zero.
2. **A status you can believe.** Correctness, not liveness. P1 shows dashboards green while data stopped flowing. A pipeline engineer summed it up: "I was the last to know" ([mckeco](https://news.ycombinator.com/item?id=48979019), adjacent ETL context).
3. **A complete copy, with a loud error on drift.** Sequences, DDL and indexes included (briffle; Smily). Native logical replication and DMS both leave these to the operator.
4. **Cross-engine comparison that normalizes correctly and shows differences usefully.**
   - One data engineer's list: different SQL dialects, missed rows due to transaction isolation, and format conversions "silently mangling my data" ([NortySpock](https://news.ycombinator.com/item?id=43097107)).
   - On a team that built comparison tools: "the real fun was in displaying the differences" ([LeonB](https://news.ycombinator.com/item?id=36907661)).
   - DVT gives false passes on time fields (#1301).
5. **Resume and repair instead of restart or reload.**
   - DMS reviewers ask for this (PeerSpot).
   - On re:Post, "reloading data seems like a bad way" to clear stuck validation ([re:Post 2022](https://repost.aws/questions/QUkc6XcW4gSSW_cVo7fmmKkg/i-have-lots-of-tables-that-are-stuck-in-the-pending-records-validation-state-and-i-can-t-get-them-to-progress)).
   - mongosync's verifier cannot resume.
6. **"Is it safe?" answers grounded in the live database.**
   - A former Postgres platform lead: "most of the devs just want the answer" ([vira28](https://news.ycombinator.com/item?id=49854800)).
   - Rule-only checkers are "hardly complete", because safety depends on database state ([orf](https://news.ycombinator.com/item?id=49855537)).
7. **Honest, documented limits.** "Calling out limitations ... would go a long way in building confidence" (jmg_).
8. **Rehearsal with real write traffic and measured downtime.** Asked for by RevenueCat, Knock and Ozzie_osman.

---

## 3. What makes practitioners adopt, pay for, or abandon a tool

**Native beats generic for like-to-like moves.**
- AWS's own docs point Postgres → Postgres moves to native tools ([T-Winsnes](https://news.ycombinator.com/item?id=39050460)).
- GOV.UK would "invest more time in trying alternative tools such as pglogical".
- One commenter argues database tooling is generalized for market size, which is why it isn't "as good as they should be" ([yen223](https://news.ycombinator.com/item?id=41306574)).

**They leave after reliability failures.** Users moved from DMS to "another tool" (re:Post) or to Striim ([PeerSpot](https://www.peerspot.com/products/aws-database-migration-service-pros-and-cons)), or wrote their own (tbarbugli, Spivak). Not everyone, though: one DMS user is "not sure I've been more impressed by another AWS service" ([NomDePlum](https://news.ycombinator.com/item?id=39049790)).

**Proof beats claims.**
- One commenter will "instantly distrust a new product claiming an exact number" ([evanelias](https://news.ycombinator.com/item?id=49011429)).
- Another would not rely on a third-party table-equivalence tool without well-defined semantics ([crabbone](https://news.ycombinator.com/item?id=36909248)).

**Open source is valued, but continuity and licence stability are suspect.**
- data-diff was sunset ([Datafold](https://www.datafold.com/blog/sunsetting-open-source-data-diff/)).
- Airbyte's move to ELv2 is called a "rugpull" ([CactusBlue](https://news.ycombinator.com/item?id=46978851)).
- After PeerDB's acquisition, for non-ClickHouse targets "you may see support for it wane" ([mritchie712](https://news.ycombinator.com/item?id=42040948)).

**Price transparency and cost matter.**
- With "Call us for pricing", one user "would have just passed immediately" (danpalmer).
- GoldenGate licensing is "very expensive" ([PeerSpot](https://www.peerspot.com/products/oracle-goldengate-pros-and-cons)).

**Data must stay put.** "Does that mean my schema and data are sent to your OpenAI agents?" ([edoceo](https://news.ycombinator.com/item?id=49013943)). Users also hesitate to grant database access through a curl installer ([docheinestages](https://news.ycombinator.com/item?id=49010224)).

**Multi-engine demand exists.**
- In Redgate's 2025 survey (vendor), 48% want multi-platform support in their migration tools.
- 57% name skills as the main challenge of running several database platforms ([Redgate 2025](https://www.red-gate.com/assets/solutions/sodl/redgate-2025-state-of-the-database-landscape-report.pdf)).

**Support is a recurring complaint.** "Even AWS support could not offer a solution" (sharadov). GoldenGate support: "They don't respond on time" (PeerSpot).

---

## 4. AI and agent features: what practitioners actually say

**Background usage is high.**
- **Joe Reis's 2026 survey:** "82% of you use AI tools daily or more" ([Reis](https://joereis.substack.com/p/the-2026-state-of-data-engineering)).
- **Redgate (vendor surveys):**
  - 15% used AI for database management in the 2025 report.
  - Accuracy concern rose from 37% to 57%, and security concern from 41% to 61%.
  - By 2026, 44% use it and 64% cite security and privacy ([Redgate 2026](https://www.red-gate.com/solutions/state-of-database-landscape/2026/ai-mini-report/)).
- None of these surveys asks about migration specifically.

**For: drafting, mapping, explaining.**
- AI helps with "generating an initial data mapping ... to then review" ([GoatInGrey](https://news.ycombinator.com/item?id=44973823)).
- For SQL in less familiar dialects it is "extremely effective" ([gaploid](https://news.ycombinator.com/item?id=41982820)).
- Explaining stored procedures gave "exceptionally good results" ([tarl0s](https://news.ycombinator.com/item?id=39627702)).
- One engineer moved 150 tables per tenant with an LLM, "all of which I reviewed carefully" ([enraged_camel](https://news.ycombinator.com/item?id=46973309)).
- General LLMs flag a classic unsafe `ADD COLUMN ... NOT NULL` that a checker missed ([peterldowns](https://news.ycombinator.com/item?id=49858725)).

**Against: running the migration, or touching production.**
- Even a builder of AI integration tooling says "We wouldn't trust AI to run that kind of migration". Humans validate every mapping and migrate with deterministic code ([rashidae](https://news.ycombinator.com/item?id=44761966)).
- On DMS task design, a model was "factually wrong and needed correction roughly every other turn" ([nijave](https://news.ycombinator.com/item?id=49851977)).
- LLM translation of stored procedures may work, "assuming you have test coverage" ([pmarreck](https://news.ycombinator.com/item?id=42697621)).
- Another builder: "LLMs alone hallucinate variables or forget context" ([trsthales](https://news.ycombinator.com/item?id=47143340)).
- In a thread with more than 1,000 comments about an agent deleting a production database:
  - "I would never give an agent access to the prod DB" ([infecto](https://news.ycombinator.com/item?id=47920771)).
  - "Impossible without a human in the loop" ([postexitus](https://news.ycombinator.com/item?id=47932067)).
- The working pattern people describe: agents write migration files, which are then deployed "deterministically (after review)" ([semanticc](https://news.ycombinator.com/item?id=49859499)).

**Verdict.** We found no practitioner asking for an autonomous migration agent. The demand is narrower:
- AI that drafts mappings or conversions, or explains a finding.
- Its output checked by deterministic proof.
- Run on a model the user chooses, with no write access to production.

Vendor AI in this space (Amazon Q in DMS) answers the error-message pain in P6. It is not evidence that practitioners want agents.

---

## 5. So what for us

What we already have is judged from the README and the backlog headings only.

| # | Opportunity backed by demand | Evidence | Our direction |
|---|---|---|---|
| 1 | Verification that keeps up with CDC until the switch, with no write freeze, and separates in-flight rows from real differences | P2, Need 1 | **Covered:** delta verify, LSN fence, resumable range checksums. Make it the headline. Measure our false-positive rate against DMS validation and VDiff. |
| 2 | "Don't trust green": an independent correctness watchdog over any mover (DMS, DTS, Debezium, native) for stalled apply, slot lag and schema drift | P1, P8, Need 2 | **Partly:** we read DMS/DTS jobs, run `watch --verify`, check boundaries, and have problems E5/E6. Pushing alerts to where on-call people look is not visible in the headings. |
| 3 | Completeness: sequences, indexes, foreign keys, views, grants, and a loud failure on DDL drift | P3, P5, Need 3 | **Mostly covered:** structural layers, sequence repair, deep checks, and the tail stops at a DDL. Carrying DDL safely, rather than only stopping, is still a gap. |
| 4 | Cross-engine comparison that cannot false-pass, and that says which columns differ and why | P2, P4, Need 4 | **Covered:** column fingerprint, normalization, problems D2–D15. Publish a proof per engine pair. |
| 5 | Rehearsal with real writes, a forecast, and a way back that loses nothing | P7, P10, Need 8 | **Partly:** rehearsal and forecast are done but their tests have not run. Reverse replication exists only for native PostgreSQL. |
| 6 | Resume and repair rather than restart, and root cause stated plainly | P6, Need 5 | **Mostly covered:** crash-resume, resumable verify, repair with undo. Plain-language root cause depends on troubleshooting docs and optional AI. |
| 7 | Depth and honestly stated limits per engine pair | §3, Need 7 | **Not yet:** the capability matrix (R13) is in progress. With 34 engines, breadth is a trust risk: practitioners prefer native or specialist tools for like-to-like moves. Publish tested per-pair limits. |
| 8 | AI only as a proposal, on the user's own or local model, never with write access | §4 | **Covered by design:** item 43 is off by default and proposal-only. The evidence argues against building an autonomous migration agent. |

**Smaller gap:** post-migration slowness (P9). Plan and timing checks cover reads on PostgreSQL and MySQL. The write path (commit latency) is not covered.
