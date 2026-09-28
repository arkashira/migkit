# Kafka cluster-to-cluster migration tools: research report (as of 2026-09-27)

This report used public web sources only; no local files were touched. The web-search budget ran out partway through, so some items were checked only by fetching pages directly, and a few are marked **UNVERIFIED**.

---

## 1. Apache Kafka MirrorMaker 2 (MM2)

### Algorithm
- **Built on Kafka Connect:** MM2 runs three connectors: MirrorSourceConnector, MirrorCheckpointConnector and MirrorHeartbeatConnector. https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/
- **It re-produces records rather than copying the log.** Each `ConsumerRecord` becomes `new SourceRecord(..., targetTopic, record.partition(), OPTIONAL_BYTES key, BYTES value, record.timestamp(), headers)`. The partition number, key/value bytes, timestamp and headers are all passed through, but target offsets are new. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java
- **Offset-syncs topic:** each record is `{topic, partition, upstreamOffset, offset(downstream)}`.
  - It lives on the source cluster by default (`offset-syncs.topic.location=source`). https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSync.java, https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorCheckpointConfig.java
  - Syncs are queued in `commitRecord` through `OffsetSyncWriter.maybeQueueOffsetSyncs`, and delayed syncs are promoted on `commit()` so that low-volume partitions don't lag. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java
  - Since 3.7 (KAFKA-15906), the latest syncs are emitted every `offset.flush.interval.ms`. https://github.com/apache/kafka/pull/14967
  - `offset.lag.max` defaults to 100. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConfig.java
- **How translation works (OffsetSyncStore):**
  - The store keeps 64 syncs per partition ("one offset sync for each bit of the topic offset"), spaced roughly exponentially. `syncs[0]` is the latest and `syncs[63]` the oldest usable one. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSyncStore.java
  - To translate, MM2 finds the nearest sync at or before the upstream offset. It returns `downstream` if the offsets are equal, otherwise `downstreamOffsetAfterSync(downstream)` (i.e. +1). If the sync is newer than the requested offset it returns -1 ("too far in the past"). No sync gives an empty result. Nothing is translated until the store has read the syncs topic to the end. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSync.java
- **KAFKA-14666 (fixed in 3.3.3, 3.4.1, 3.5.0):** previously only the latest sync was kept, so groups behind the replication flow could not be translated. The fix added a bounded in-memory index of older syncs, holding only syncs seen since the task last restarted. Because the index is bounded, checkpoints need monotonicity guards. https://issues.apache.org/jira/browse/KAFKA-14666, https://github.com/apache/kafka/pull/13429
- **Why translation got coarser:**
  - KAFKA-12468 (3.3.3, 3.4.1, 3.5.0) fixed a data-loss bug in which untranslated offsets were copied, which showed up as negative lag. Translation now deliberately picks a sync *before* the offset. https://issues.apache.org/jira/browse/KAFKA-12468, https://lists.apache.org/thread/2726x5pzq6zb31z6tvrjf54mb8t07309
  - The upgrade note says translated offsets "will be earlier than in previous versions… re-deliver more data after failing-over." https://kafka.apache.org/38/getting-started/upgrade/
  - In 3.5.0 and 3.5.1 translation produced offsets earlier than needed (KAFKA-15202, fixed in 3.5.2 and 3.6.0). https://www.mail-archive.com/jira@kafka.apache.org/msg152311.html
  - Interpolating between sparse syncs is proposed in KAFKA-16641 and not yet done. https://www.mail-archive.com/dev@kafka.apache.org/msg139372.html
  - From 3.8 (KAFKA-15905), checkpoints can cover offsets mirrored before the checkpoint task started, but only if MM2 has READ on the checkpoints topic. https://kafka.apache.org/38/getting-started/upgrade/
  - A maintainer expects target lag to be about 2x source lag and recommends 3.8+. https://github.com/orgs/strimzi/discussions/10461
- **Checkpoints topic:** `<src>.checkpoints.internal` on the target. Each record holds `{group, topic, partition, upstreamOffset, downstreamOffset, metadata}`. https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0
  - Checkpoint and group-sync defaults: `emit.checkpoints.enabled=true`, `emit.checkpoints.interval.seconds=60`, `sync.group.offsets.enabled=false`, `sync.group.offsets.interval.seconds=60`, `refresh.groups.interval.seconds=600`. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorCheckpointConfig.java
  - Offset sync into target `__consumer_offsets` happens only while the group has no active consumers on the target. https://kafka.apache.org/31/operations/geo-replication-cross-cluster-data-mirroring/
- **Topic configs:** `sync.topic.configs.enabled=true` with a 600 s interval. **ACLs:** `sync.topic.acls.enabled=true` with a 600 s interval. **Topic refresh:** `refresh.topics.interval.seconds=600`. **Target replication factor:** `replication.factor=2`. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConfig.java
- **ACL sync is limited:**
  - Only TOPIC + LITERAL bindings are synced. ALLOW WRITE is dropped and ALLOW ALL is downgraded to READ.
  - Prefixed, group and cluster ACLs are not synced.
  - It only touches remote topics, and it keeps re-adding READ ACLs even when they already exist.
  - Sources: https://github.com/orgs/strimzi/discussions/5419, https://www.mail-archive.com/users@kafka.apache.org/msg40790.html, https://www.mail-archive.com/users@kafka.apache.org/msg43373.html
- **Topic naming:**
  - `DefaultReplicationPolicy` gives `<alias>.<topic>`, with a configurable separator. https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/
  - `IdentityReplicationPolicy` keeps names unchanged ("useful for migrating from legacy MirrorMaker"). It cannot prevent cycles, so the topology must be acyclic. https://github.com/apache/kafka/blob/trunk/connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/IdentityReplicationPolicy.java
- **Partitions:** remote topics "must have the same number of partitions as their source topics." https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0
- **Exactly-once:**
  - KIP-618 adds worker config `exactly.once.source.support` (disabled → preparing → enabled). It requires distributed mode plus TransactionalId WRITE/DESCRIBE and IdempotentWrite ACLs. https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors
  - MM2 dedicated mode supports it from 3.5.0, which also needs `dedicated.mode.enable.internal.rest=true`. https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/
- **Source consumer:** `enable.auto.commit=false` and `auto.offset.reset=earliest` are hard-coded, and there is no isolation.level override. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorConnectorConfig.java
  - The Java default is `read_uncommitted`, so aborted transactional records get copied as normal data unless you set `source.consumer.isolation.level=read_committed`. https://kafka.apache.org/41/configuration/consumer-configs/

### Licence and distribution
- Apache-2.0, part of the Kafka tarball (`connect-mirror-maker.sh`).
- Docker image `apache/kafka` is published for amd64 and arm64. As of this check, 4.4.0 is still at release-candidate stage (rc2 on 2026-09-16, rc1 on 2026-09-22). https://hub.docker.com/v2/repositories/apache/kafka/tags?page_size=5
- Kafka 4.3.0 was released on 2026-05-22. https://kafka.apache.org/blog/2026/05/22/apache-kafka-4.3.0-release-announcement/

### Kafka 4.x changes
- ZooKeeper was removed; 4.0 is KRaft only.
- Connect and tools need Java 17 (clients need Java 11).
- Old protocol versions were removed, so brokers must be 2.1 or newer before you use 4.0 Java clients, including Connect/MM2. **4.x MM2 cannot read from brokers older than 2.1.** https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
- MM1 was removed (KIP-720 / KAFKA-14262), as was the `use.incremental.alter.configs` setting. https://kafka.apache.org/41/getting-started/upgrade/, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/177046121/KIP-720+Deprecate+MirrorMaker+v1
- 4.3 (KIP-1280) deprecates the old MM2 metric names (removal planned for 5.0). New names are opt-in via `metric.names.formats`. https://www.confluent.io/blog/apache-kafka-4-3-release/

### Surface and progress reporting
- **JMX:** group `kafka.connect.mirror`. The source connector exposes `record-count`, `record-rate`, `replication-latency-ms`, `byte-rate` (with -min/-max/-avg) per source/target/topic/partition. The checkpoint connector exposes `checkpoint-latency-ms`. https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/
- **Java API:** `RemoteClusterUtils.translateOffsets / replicationHops / upstreamClusters / checkpointTopics`, which reads the checkpoints topic. https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0
- **For Python:** read the checkpoints topic directly and decode it the same way.

### Upcoming native replacement: KIP-1279 "Cluster Mirroring"
- **What it does:** mirroring runs inside the broker, using follower-style fetches. It copies batches byte for byte, so offsets, compression and topic IDs are preserved.
- **Groups:** group offsets are clamped to `max(dest LSO, min(dest LEO, src))`, and ACLs and configs are synced.
- **Tooling:** CLI `kafka-cluster-mirrors.sh`; source brokers can be 2.1+.
- **Limits:** asynchronous only, no exactly-once (uncommitted data becomes visible), no active-active, Streams internal topics excluded.
- Sources: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1279:+Cluster+Mirroring, https://developers.redhat.com/articles/2026/09/22/data-liberation-apache-kafka-native-cluster-mirroring
- **Status conflict:** the KIP page summary says "Accepted", but an Aug-2026 newsletter says it was still under discussion and missed both the 4.3 and 4.4 cuts. It is **not in any GA release as of Sept 2026**. https://getkafkanated.substack.com/p/get-kafka-nated-espresso-august-2026

### Known MM2 failure modes
- Lag inflates, or translation freezes, after checkpoint-task restarts on versions before 3.8 (KAFKA-15905). https://kafka.apache.org/38/getting-started/upgrade/
- Group lag can exceed `offset.lag.max` (KAFKA-14797). https://issues.apache.org/jira/browse/KAFKA-14797
- Offset sync is incorrect when the target partition is empty (KAFKA-12635). https://issues.apache.org/jira/browse/KAFKA-12635
- Group offsets are not written while the group is active on the target. https://kafka.apache.org/31/operations/geo-replication-cross-cluster-data-mirroring/
- With the Strimzi User Operator, ACL sync is impossible because the operator overwrites the ACLs. https://github.com/strimzi/strimzi-kafka-operator/discussions/5419

---

## 2. Confluent Replicator
- **Licence:** commercial. It is part of Confluent Platform Enterprise, runs as a Connect connector on Connect Enterprise 5.3+, and has a trial. Confluent now says "the recommended approach is to use Cluster Linking and Schema Linking over Replicator." https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/index.html
- **Features:** preserves partition count, replication factor and config overrides; renames via `topic.rename.format`; uses provenance headers to prevent loops; translates schemas. https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/index.html
- **Offset translation:**
  - Consumers run `interceptor.classes=io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor`, which writes the timestamp of each committed offset to `__consumer_timestamps` on the source.
  - Replicator maps those timestamps to target offsets and writes them to the target `__consumer_offsets`, but only while the group is inactive on the target. https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-failover.html
- **Limits:**
  - Java consumers only.
  - The interceptor "does not support transactions."
  - Identical timestamps cause ambiguous mapping.
  - It needs WRITE/DESCRIBE on `__consumer_timestamps`.
  - Source: https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-failover.html

## 3. uReplicator (Uber)
- **Licence:** Apache-2.0.
- **Status:** not archived, but dormant; the last commit was 2021-06-07. https://github.com/uber/uReplicator, https://github.com/uber/uReplicator/commits/master
- **Architecture:**
  - A Helix controller statically assigns partitions to workers, each running a DynamicKafkaConsumer. This avoids MM1-style full rebalances.
  - Coordination uses ZooKeeper.
  - It commits only after the destination has persisted the data.
  - Offsets are not translated.
  - Sources: https://www.uber.com/blog/ureplicator-apache-kafka-replicator/, https://github.com/uber/uReplicator
- **API:** REST `POST <controller>/topics/<topic>`. In federated mode a manager handles routes with `?src=&dst=`. https://github.com/uber/uReplicator/wiki/uReplicator-User-Guide

## 4. kcat (formerly kafkacat)
- **Licence:** BSD-2-Clause. https://github.com/edenhill/kcat/blob/master/LICENSE
- **Status:** effectively unmaintained.
  - The last GitHub release is 1.7.0 ("renamed to kcat", Aug 2021).
  - The Docker image is `edenhill/kcat:1.7.1`.
  - The last commit was 2022-11-17.
  - Sources: https://github.com/edenhill/kcat/releases, https://github.com/edenhill/kcat/commits/master, https://github.com/edenhill/kcat
- **Features useful for migration:**
  - Modes: `-C` consume, `-P` produce, `-L` metadata, `-Q` offsets by timestamp (`-Q -t mytopic:3:2389238523`).
  - Offsets: `-o s@<ms> -o e@<ms>` gives a time range; `-e` exits at the end of the partition.
  - Output and input: `-J` JSON envelope, `-K` key delimiter, `-H` headers, format tokens `%o %p %T %h`.
  - `-X` passes librdkafka properties; Avro via Schema Registry is supported; idempotent and transactional producers are supported.
  - Source: https://github.com/edenhill/kcat
  - **UNVERIFIED:** I could not confirm the exact field list of the `-J` envelope in this pass.
- **Install:** `brew install kcat`, apt, Docker. **UNVERIFIED:** arm64 availability of the Docker image.

## 5. Python clients

| | kafka-python | confluent-kafka | aiokafka |
|---|---|---|---|
| Latest | 3.0.11 (2026-08-16); 3.0.0 on 2026-06-11 rewrote the networking layer (`kafka.net`, async) and generates the protocol from Kafka JSON schemas | 2.15.1 (2026-09-10), bundles librdkafka 2.15.1 | 0.14.0 (2026-04-29) |
| Licence | Apache-2.0; maintainers dpkp, mumrah | Apache-2.0 | Apache-2.0 (aio-libs) |
| Wheels | pure Python | macOS x86-64/arm64; Linux x86-64/arm64 (glibc 2.28+); Python 3.8–3.14 | macOS arm64, Linux arm64; Python ≥3.10 |
| Default partitioner | murmur2 (Java-compatible) | **consistent_random (CRC32)** | murmur2 (Java-compatible) |
| Transactions | yes | yes | yes |

Sources for the table:
- kafka-python: https://pypi.org/project/kafka-python/, https://github.com/dpkp/kafka-python/blob/master/CHANGES.md, https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
- confluent-kafka: https://pypi.org/project/confluent-kafka/, https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
- aiokafka: https://pypi.org/project/aiokafka/, https://aiokafka.readthedocs.io/en/stable/api.html

Further details:
- **kafka-python 3.0 admin client** adds `describe_acls/create_acls` and `list_group_offsets/reset_group_offsets`, and supports brokers 0.8 through 4.3. https://github.com/dpkp/kafka-python/blob/master/CHANGES.md, https://pypi.org/project/kafka-python/
- **confluent-kafka AdminClient** covers:
  - `list_consumer_group_offsets`, `alter_consumer_group_offsets`
  - `describe_acls`, `create_acls`, `delete_acls`
  - `describe_user_scram_credentials`, `alter_user_scram_credentials`
  - `list_offsets` (earliest, latest, by timestamp), `delete_records`
  - `incremental_alter_configs`, `describe_configs`, `create_partitions`, `describe_topics`, `describe_cluster`, `elect_leaders`
  - Transactions: `init_transactions`, `begin_transaction`, `send_offsets_to_transaction`, `commit_transaction`, `abort_transaction`
  - Source: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- **Recent confluent-kafka additions:** AIOProducer (beta in 2.12), async context managers (2.14), "deterministic partitioner functions" exposed (2.13), ShareConsumer (2.15). https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md
- **librdkafka defaults to watch:**
  - `enable.idempotence=false` (Java defaults to `true`).
  - `max.in.flight=1000000`, automatically capped at 5 when idempotence is on.
  - `isolation.level=read_committed` (Java defaults to `read_uncommitted`).
  - `linger.ms=5`, `message.timeout.ms=300000`.
  - Source: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
- **SCRAM caveat:** Describe returns only the mechanism and iteration count, never the salt or hash. AlterUserScramCredentials does accept a pre-salted `Salt` + `SaltedPassword`, but you cannot copy users between clusters without knowing their passwords. https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API

**Throughput benchmarks.** None of them are current or directly comparable.

| Benchmark | Setup | Result |
|---|---|---|
| Activision, 2016 | 1M × 100 B, acks=1, one partition | confluent-kafka producer 183,456 msg/s, consumer 261,408 msg/s; kafka-python far slower |
| Derosiaux, 2026 | tuned, single CPU | confluent-kafka 540K msg/s produce, 657K consume, vs Java 1.6M / 2.6M |
| abhishekray07 (undated) | 100k × 100 B | confluent-kafka 22,284 msg/s, kafka-python 11,413, aiokafka 45,016 — method suspect (possibly flushing per message) |

Sources: https://activisiongamescience.github.io/2016/06/15/Kafka-Client-Benchmarking/, https://sderosiaux.medium.com/i-benchmarked-java-vs-python-kafka-clients-with-bayesian-optimization-java-was-3x-faster-9d89ca843607, https://github.com/abhishekray07/kafka-client-benchmarks

## 6. Redpanda
- **Licences:**
  - The core is under BSL 1.1, which converts to Apache-2.0 four years after each merge. Enterprise features are under the Redpanda Community License (RCL) and need a paid licence, with a 30-day trial.
  - Enterprise features include **Tiered Storage, Remote Read Replicas and Shadowing**.
  - Source: https://docs.redpanda.com/current/get-started/licensing/overview/
- **rpk:** BSL (per the header in `main.go`). https://github.com/redpanda-data/redpanda/blob/dev/src/go/rpk/cmd/rpk/main.go
  - Binaries: `rpk-darwin-arm64.zip`, `rpk-linux-arm64.zip`, or brew `redpanda-data/tap/redpanda`. https://docs.redpanda.com/current/get-started/rpk-install/
- **Shadowing (v25.3+, Enterprise licence required on both clusters):**
  - Broker-native and asynchronous. Copies byte for byte, preserving offsets and timestamps, plus group offsets, ACLs and schemas.
  - The shadow cluster is read-only until failover.
  - **The source can be Apache Kafka, Confluent Platform or Confluent Cloud.** Replicating the `_schemas` topic byte for byte needs a Redpanda source; other sources use API mode.
  - No active-active; one shadow link per shadow cluster.
  - Group offsets are copied only for topics being shadowed.
  - Sources: https://docs.redpanda.com/streaming/current/manage/disaster-recovery/shadowing/overview/, https://www.redpanda.com/blog/25-3-enterprise-disaster-recovery
  - CLI: `rpk shadow config|create|describe|status|failover|update|list|delete`. https://docs.redpanda.com/streaming/current/reference/rpk/rpk-shadow/rpk-shadow/
  - Metric example: `redpanda_shadow_link_client_errors`. https://docs.redpanda.com/streaming/25.3/manage/disaster-recovery/shadowing/monitor.md
- **Redpanda Connect `redpanda_migrator` (unified input/output, 4.67.x, status stable):**
  - Source files carry an **Apache-2.0** header, and `migrator.go` has no licence check. https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/migrator_groups.go, https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/migrator.go
  - Enterprise connectors in Redpanda Connect generally are RCL and gated at runtime. https://docs.redpanda.com/redpanda-connect/get-started/licensing/
  - **Group-offset algorithm:**
    1. Read the source record at `committed-1` and take its timestamp.
    2. Call `ListOffsetsAfterMilli(ts)` on the destination.
    3. Add 1 when the timestamps match exactly.
    4. Refine using the `redpanda-migrator-offset` header (the source offset embedded in each copied record), with up to 5 reads, and cache results to prevent rewinds.
  - Offsets outside `(start, end]` are skipped. All groups except Dead are eligible, or only Empty ones with `only_empty`.
  - Sources: https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/migrator_groups.go, https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/README.md
  - Offset translation is described as "best-effort" and needs identical partition counts.
  - ACLs: ALLOW WRITE is dropped and ALLOW ALL becomes READ (same rule as MM2).
  - Schema Registry: IDs are preserved by default (`translate_ids` changes that). The destination registry must be in READWRITE or IMPORT mode. Schemas resync every 5 minutes.
  - Source for the last three points: https://docs.redpanda.com/redpanda-connect/components/outputs/redpanda_migrator/
  - **Metrics:** `redpanda_migrator_cg_offsets_translated_total`, `..._cg_offset_translation_errors_total`, `..._topics_created_total`, `..._sr_schemas_created_total`, `redpanda_lag`, and `input_redpanda_migrator_lag{topic,partition}`. https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/migrator.go, https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda_migrator/
  - **Distribution:** `rpk connect`, brew, Docker `docker.redpanda.com/redpandadata/connect`. https://docs.redpanda.com/redpanda-connect/get-started/quickstarts/rpk/
  - **UNVERIFIED:** the status of the older `redpanda_migrator_offsets` / `redpanda_migrator_bundle` components.

## 7. Other offset-preserving options
- **Confluent Cluster Linking (commercial):**
  - Mirror topics are byte for byte and read-only, with consistent offsets. Consumer offsets are synced via `consumer.offset.sync.enable`, and ACLs and configs are synced too.
  - The destination must be Confluent Server (CP 7.8+). Current docs list Apache Kafka 3.8.x+ as the minimum source version.
  - Source: https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.html
- **Confluent Schema Linking (Enterprise):** schema exporters run inside Schema Registry, use contexts, and the destination uses IMPORT mode. CLI is `schema-exporter --create/--pause/--reset`. https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html
- **AutoMQ Kafka Linking:** commercial edition only; byte-to-byte, offset-consistent, zero-downtime. The open-source edition uses MM2. https://docs.automq.com/automq/migration/overview
- **WarpStream Orbit (proprietary):** offset-identical copy including headers. `copy_offsets_enabled` copies group offsets, but only for Orbit-managed topics. `irreversible_disable_orbit_management` handles producer cutover. https://docs.warpstream.com/warpstream/kafka/orbit
- **Aiven Klaw (Apache-2.0):** a governance portal. It syncs metadata (topics, ACLs, schemas, connectors) but does not replicate data. https://github.com/Aiven-Open/klaw

## 8. Techniques and pitfalls for a Python copier
- **Timestamps:**
  - With `message.timestamp.type=CreateTime` (the default) the producer's timestamp is kept; with LogAppendTime the broker overwrites it.
  - In 4.x, `message.timestamp.after.max.ms` defaults to 1 hour (future timestamps are rejected) and `before.max.ms` is unlimited.
  - Source: https://kafka.apache.org/41/configuration/topic-configs/
  - Time-based retention uses the **largest timestamp in each segment**, so old CreateTime records copied into a topic with a short `retention.ms` can be deleted almost at once. https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index
  - Always pass `timestamp=` (confluent-kafka) or `timestamp_ms=` (kafka-python) explicitly.
- **Partitioner mismatch:** librdkafka's default `consistent_random` (CRC32) ≠ Java's murmur2. Use `murmur2_random` to match Java. Empty keys are handled differently too: `consistent_random` spreads them randomly, `murmur2_random` hashes them. https://github.com/confluentinc/librdkafka/issues/4510, https://www.confluent.io/blog/standardized-hashing-across-java-and-non-java-producers/, https://www.conduktor.io/blog/librdkafka-vs-java-client
  - For migration, **set `partition=` to the source partition explicitly** and never rely on a partitioner. This also means the target partition count must be ≥ the source count, which is what MM2 assumes. https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0
- **Ordering:** Java idempotence is on by default and requires `max.in.flight ≤ 5`, `retries > 0`, `acks=all`. With idempotence off and in-flight > 1, retries can reorder messages. https://kafka.apache.org/41/configuration/producer-configs/
  - In librdkafka, set `enable.idempotence=true` explicitly. https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
- **Transactions:**
  - Read the source with `read_committed`, otherwise aborted records are copied as live data.
  - Commit markers take up offsets, so target offsets drift from source offsets even without compaction.
  - `read_committed` stops at the LSO while a transaction is open.
  - Source: https://kafka.apache.org/41/configuration/consumer-configs/
- **Compacted topics:** re-producing packs the offsets densely and loses compaction gaps. Only byte-copy tools preserve the gaps (KIP-1279, Cluster Linking, Shadowing). https://developers.redhat.com/articles/2026/09/22/data-liberation-apache-kafka-native-cluster-mirroring
  - Tombstones (null value) must be produced as `value=None`.
  - Under KIP-1279, tombstones beyond the replication watermark at failover are never applied. https://cwiki.apache.org/confluence/display/KAFKA/KIP-1279:+Cluster+Mirroring
  - Target `delete.retention.ms` defaults to 1 day. https://kafka.apache.org/41/configuration/topic-configs/
- **Headers:** MM2 passes them through. https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java
  - Embedding the source offset in a header, as the Redpanda migrator does, enables exact offset translation later. https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/README.md
- **Schema Registry:** schema IDs embedded in message payloads must still resolve on the target. Either keep IDs by using IMPORT mode (Redpanda migrator default) or rewrite them (`translate_ids`, Schema Linking contexts). https://docs.redpanda.com/redpanda-connect/components/outputs/redpanda_migrator/, https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html

**Note on sources:** KIP-1279's status conflicts between the KIP page ("Accepted") and the August newsletter (still under discussion, not in any GA release). The benchmark numbers are old or methodologically weak, so a custom benchmark is needed before relying on any throughput figure.
