# Redis/Valkey migration tools for a Python wrapper (research as of 2026-09-27)

A few points below are my own inference and are marked "(inference)". Everything else has a source link next to it.

## Summary
- **RedisShake v4** (MIT, Go) is the only maintained tool that covers everything: PSYNC, SCAN, RDB and AOF input, a status endpoint, and prebuilt binaries for darwin/linux/windows on amd64 and arm64. Its weak points: it does not reconnect if the PSYNC link drops, and it panics if the cluster topology changes ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html)).
- **RIOT is archived.** Its successor RIOT-X is under the **BSL**, and production use is allowed only with Redis CE, Redis Cloud or Redis Software. Using it to migrate into Valkey or ElastiCache-Valkey is a licensing problem ([LICENSE](https://github.com/redis/riotx-dist/blob/main/LICENSE.md)).
- **redis-full-check** (Apache-2.0 since v1.4.10) is the reference verifier that compares in several rounds. Its only release binary is linux-amd64 ([releases](https://github.com/tair-opensource/RedisFullCheck/releases)).
- **No maintained pure-Python RDB parser handles RDB 10 or newer.** Maintained parsers exist in Go (HDT3213/rdb) and C (librdb).
- **Redis and Valkey RDB formats have split.** Redis unstable is at RDB_VERSION 16 ([rdb.h](https://github.com/redis/redis/blob/unstable/src/rdb.h)). Valkey 9 uses version 80. The Valkey migration guide says RDB files from Redis CE 7.4 and later are not compatible ([valkey](https://valkey.io/topics/migration/)).

---

## 1. RedisShake v4 (tair-opensource/RedisShake)

**Status and distribution**
- MIT licence, not archived, default branch `v4`, last push 2026-09-11 ([api](https://api.github.com/repos/tair-opensource/RedisShake)).
- Latest release is v4.6.2 (2026-08-17). Assets are `redis-shake-v4.6.2-{darwin,linux,windows}-{amd64,arm64}.tar.gz` ([releases](https://github.com/tair-opensource/RedisShake/releases)).
- v4.6.0 added Redis 8.4 and Valkey 9.x command specs. v4.5.0 added Redis 8.0 and Valkey 9.0 ([releases](https://github.com/tair-opensource/RedisShake/releases)).
- The README claims support for Redis 2.8 to 8.4.x and Valkey 8 to 9.x, in standalone, master-replica, sentinel and cluster setups ([README](https://github.com/tair-opensource/RedisShake)).
- It is not on PyPI; you would wrap the binary.

**Readers**
- **sync_reader** pretends to be a replica ([docs](https://tair-opensource.github.io/RedisShake/en/reader/sync_reader.html), [source](https://github.com/tair-opensource/RedisShake/blob/v4/internal/reader/sync_standalone_reader.go)):
  - It sends `REPLCONF listening-port <status_port>`.
  - It sends `REPLCONF CAPA EOF` only when `try_diskless=true`.
  - It sends `REPLCONF rdb-only 1` when `sync_aof=false`.
  - It then sends `PSYNC ? -1`, which always forces a full sync (`SYNC` on sources older than 2.8). It does not advertise `capa psync2` and never resumes partially.
  - The RDB arrives either length-prefixed (read in 32 MB chunks) or diskless with a 40-byte EOF marker. It is parsed into commands, then the AOF stream is forwarded as is.
  - It sends `REPLCONF ACK <offset>` every **100 ms**. There is no reconnect logic.
  - With `cluster=true` it discovers nodes from any address. `prefer_replica=true` pulls from a replica ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
  - The docs warn against using sync_reader on a Sentinel-managed master ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html)).
- **scan_reader** uses `SCAN`, then `DUMP` on the source and `RESTORE` on the target ([scan](https://tair-opensource.github.io/RedisShake/en/reader/scan_reader.html)):
  - `ksn=true` adds `psubscribe __keyevent@*__:*` and requires `notify-keyspace-events` to contain `AE`.
  - It cannot see `FLUSHALL`/`FLUSHDB`. You may need to raise `client-output-buffer-limit pubsub`.
  - SCAN can miss keys or return deleted ones, and the progress figure is only approximate.
  - In the maintainers' test, DUMP raised source CPU from 47% to 91%.
  - Options: `dbs`, `count` (default 1), and `skip_unknown_type` ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- **rdb_reader** takes a `filepath`. **aof_reader** takes a `filepath` and a `timestamp` for point-in-time replay ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).

**Writers**
- `redis_writer`: `cluster`, `tls`, and `off_reply` (turns off server replies).
- `file_writer`: `type = cmd|aof|json` ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).

**How the RDB is converted** ([rdb.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/rdb/rdb.go))
- **TTL:** each key is written with `RESTORE key <relative pttl> <payload>`. The TTL is computed as `expire − now()` at parse time, without ABSTTL. Keys that have already expired are clamped to **1 ms**, so they appear briefly on the target.
- **Existing keys:** `REPLACE` is added only when `rdb_restore_command_behavior="rewrite"`. The options are `panic` (default), `rewrite` and `skip`. This setting applies to the RDB phase and to scan_reader, but not to the AOF phase ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- **Big keys:** if the DUMP size exceeds `target_redis_proto_max_bulk_len` (default 512 000 000), the key is rebuilt with per-type commands plus `PEXPIRE` ([rdb.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/rdb/rdb.go)). Setting it to `0` turns every key into plain commands, which is the documented fix for migrating to an older version ([version](https://tair-opensource.github.io/RedisShake/en/others/version.html)).
- **Unknown module types:** `MODULE_2` values can only go through RESTORE. A value larger than the limit (including when the limit is 0) fails with an explicit error ([PR #1062](https://github.com/tair-opensource/RedisShake/pull/1062)).
- **Functions:** the RDB `FUNCTION2` opcode is replayed as `FUNCTION LOAD REPLACE`. Module aux data is parsed and then dropped ([rdb.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/rdb/rdb.go)). The parser has an `isValkey` flag for the diverging type numbers ([rdb.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/rdb/rdb.go)).
- **Bloom filters:** `target_mbbloom_version` exists because the BF.LOADCHUNK format differs between versions ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).

**Filters and Lua**
- Allow/block lists for keys, key prefix, suffix and regex, db, command, and command group ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- `function = """lua"""` exposes `DB`, `CMD`, `GROUP`, `KEYS`, `KEY_INDEXES`, `SLOTS` and `ARGV`, plus `shake.call(db, argv)` (can be called several times to split a command) and `shake.log()`. If the script never calls `shake.call`, the data is silently dropped ([function](https://tair-opensource.github.io/RedisShake/en/filter/function.html)).
- **Multiple databases into a cluster:** you get `ERR SELECT is not allowed in cluster mode`. Fix it with `allow_db=[0]`, or remap the db in Lua ([#869](https://github.com/tair-opensource/RedisShake/issues/869)).

**Tuning, progress and completion**
- Tuning knobs: `pipeline_count_limit=1024`, `target_redis_max_qps=300000` (token bucket), `target_redis_client_max_querybuf_len=1GB`, `empty_db_before_sync` (target flushes before the full sync), and log rotation settings. There is no JSON log option in shake.toml ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- **Progress:** set `status_port` to get an HTTP JSON endpoint ([status.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/status/status.go)). The response contains `start_time`, `consistent`, `total_entries_count{read_count,read_ops,write_count,write_ops}`, `per_cmd_entries_count`, `reader` and `writer` ([pkg doc](https://pkg.go.dev/github.com/wangwenjie2500/RedisShake/pkg/status)).
- The reader status has RDB file size, received and sent bytes, and AOF received and sent offsets ([sync src](https://github.com/tair-opensource/RedisShake/blob/v4/internal/reader/sync_standalone_reader.go)).
- **Cutover signal:** `consistent = lastConsistent && reader && writer`. For sync_reader, "consistent" means `AofReceivedOffset != 0 && == AofSentOffset && queue empty` ([status.go](https://github.com/tair-opensource/RedisShake/blob/v4/internal/status/status.go), [sync src](https://github.com/tair-opensource/RedisShake/blob/v4/internal/reader/sync_standalone_reader.go)). My reading is that it must hold across two consecutive samples (inference).

**Failure modes**
- A dropped PSYNC link is not reconnected. There is no checkpoint or resume in 4.x. Failover, scaling or slot migration causes a panic ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html), [agents](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/README_FOR_AGENTS.md)).
- "Target key name is busy" panics by default ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- Going from a higher to a lower version can fail on binary encodings and on commands the target doesn't know ([version](https://tair-opensource.github.io/RedisShake/en/others/version.html)).
- On clouds that block PSYNC you get `ERR unknown command 'psync'` ([#423](https://github.com/alibaba/RedisShake/issues/423)).

---

## 2. RIOT and RIOT-X

**RIOT** ([GitHub](https://github.com/redis/riot), [releases](https://github.com/redis/riot/releases), [docs](https://redis.github.io/riot/))
- Apache-2.0. **Archived and unmaintained since 2025-10-29**, superseded by RIOT-X.
- Last release v4.3.0 (2025-04-01), with `riot-standalone` zips for linux, linux_musl, osx and windows on x86_64 and aarch64.
- Needs Java 11+ unless you use the standalone build, which bundles a runtime.
- **Replication types:** DUMP plus PTTL, written as RESTORE plus EXPIRE (the default), or `--struct`, which uses type-specific read and write commands. `--struct` is for targets without RESTORE or with an incompatible RDB format.
- **Modes:** `scan` (snapshot), `liveonly` (keyspace notifications only) and `live` (both in parallel).
- **Compare:** `quick` (the default) checks type and TTL. `full` checks type, TTL and value. It runs after the scan, or in live mode once notifications go idle. There is also a standalone `compare` command.
- Compare options: `--ttl-tolerance` and `--show-diffs`. The report counts missing keys and keys whose type, value or TTL differ.
- **Filters:** `--key-pattern`, `--key-type`, `--key-include`/`--key-exclude` (client side), and `--mem-limit`. **Progress:** `--progress ascii|log|none`.
- **Limits:** live mode "does not guarantee data consistency", because pub/sub is not reliable, and big keys can overflow its queue. Active-Active targets need `--no-stream-id`.

**RIOT-X**
- Licence is **Business Source License**, not open source. Production use is allowed only "in connection with" Redis Community Edition, Redis Cloud or Redis Software, and it converts to MIT after four years ([LICENSE](https://github.com/redis/riotx-dist/blob/main/LICENSE.md)).
- v1.15.1 released 2026-09-25, with `riotx-standalone` for linux, linux_musl, osx and windows on x86_64 and aarch64 ([releases](https://github.com/redis/riotx-dist/releases)).
- Install with `brew install redis/tap/riotx` or scoop ([install](https://redis.io/docs/latest/integrate/riot/install/)).
- `--mode scan|liveonly|live`. Live mode needs `notify-keyspace-events KEA` ([modes](https://redis.github.io/riotx/replication/modes.html)).
- A third-party summary says it exposes Prometheus `/metrics` on port 8080 ([DeepWiki](https://deepwiki.com/redis/riotx-dist)).
- A Microsoft Q&A answer (AI-generated) says Azure Redis Enterprise cannot enable keyspace notifications, so live mode is unavailable there ([MS Q&A](https://learn.microsoft.com/en-us/answers/questions/5770259/is-there-a-way-to-riotx-live-replicate-when-the-so)).

---

## 3. RDB parsers

- **rdbtools (Python):**
  - MIT, not archived. The latest PyPI release is still **0.1.15 from 2020-06-28** ([PyPI](https://pypi.org/pypi/rdbtools/json), [api](https://api.github.com/repos/sripathikrishnan/redis-rdb-tools)).
  - It fails with `Invalid RDB version number 10`, so it cannot read Redis 7.0 or later. Issues #185, #198 and #202 are still open ([#187](https://github.com/sripathikrishnan/redis-rdb-tools/issues/187), [#198](https://github.com/sripathikrishnan/redis-rdb-tools/issues/198)).
  - Python forks: `rdb-cli` 0.2.0 from about October 2022 ([PyPI](https://pypi.org/project/rdb-cli/)) and `rdbtools3` ([PyPI](https://pypi.org/project/rdbtools3/)). I found no PyPI parser that clearly supports RDB 11 or 12.
- **HDT3213/rdb (Go):**
  - Apache-2.0 ([repo](https://github.com/HDT3213/rdb)). Commands: `json`, `aof` (RESP output for replay), `memory`, `bigkey`, `hotkey`, `prefix` and `flamegraph`.
  - v1.3.2 (2026-04-19) added Valkey 9 **RDB 80** and Hash2 field TTLs, and parses LFU/LRU metadata ([releases](https://github.com/HDT3213/rdb/releases)).
  - Assets are darwin-amd64, darwin-arm64, linux-amd64 and windows-amd64. **There is no linux-arm64 binary.**
- **redis/librdb and its `rdb-cli` (C, official):**
  - MIT. Outputs `json`, `resp`, or `redis` (loads straight into a live server), with filters `-k/-K` (key regex), `-t` (type) and `-d` (db) ([repo](https://github.com/redis/librdb)).
  - v2.2.0 (2026-03-01) added RDB v13. v2.3.0 (2026-06-03) added RDB v14 ([releases](https://github.com/redis/librdb/releases)). No prebuilt binaries; you build from source.
- **Java:**
  - leonchen83/redis-rdb-cli and redis-replicator: parse, split, merge and sync ([repo](https://github.com/leonchen83/redis-rdb-cli)).
  - jwhitbeck/java-rdb-parser: RDB 1 to 13, but no streams or modules ([repo](https://github.com/jwhitbeck/java-rdb-parser)).
- **Redis vs Valkey formats:**
  - Redis unstable is at RDB 16. New types 26 to 33 cover stream IDMP and XNACK, ARRAY, hash templates and GCRA ([rdb.h](https://github.com/redis/redis/blob/unstable/src/rdb.h)).
  - Valkey pinned RDB 11 for Redis 7.2 compatibility ([#665](https://github.com/valkey-io/valkey/pull/665)).
  - Valkey 8.1 added `rdb-version-check strict|relaxed`, which also applies to replication and RESTORE ([#1604](https://github.com/valkey-io/valkey/pull/1604)). Relaxed mode accepts foreign versions but errors out on any type it doesn't know ([#2543](https://github.com/valkey-io/valkey/pull/2543)).
  - Valkey saves volatile-field hashes as `RDB_TYPE_HASH_2` triplets ([#4651](https://github.com/valkey-io/valkey/issues/4651)). Redis HFE uses types 24 and 25 ([rdb.h](https://github.com/redis/redis/blob/unstable/src/rdb.h)).

## 4. RedisGears (short)
- Licence RSALv2 or SSPLv1 ([LICENSE](https://github.com/RedisGears/RedisGears/blob/master/LICENSE.txt)).
- Gears v1 (Python/JVM, KeysReader and StreamReader) is deprecated ([docs](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/deprecated-features/gears-v1/)).
- Gears 2 "triggers and functions" was discontinued before GA. TFCALL and TFUNCTION will return errors, JS libraries are removed, and Lua is unaffected ([deprecated](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/deprecated-features/)).
- The migration pattern was a KeysReader that XADDs events into a stream, consumed by a StreamReader ([StreamReader](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/deprecated-features/gears-v1/jvm/classes/readers/streamreader/)). I don't recommend it.
- Redis core itself is RSALv2/SSPLv1 for 7.4 to 7.8, and RSALv2/SSPLv1/**AGPLv3** from 8.0 ([licenses](https://redis.io/legal/licenses/)).

## 5. DUMP, RESTORE and MIGRATE
- **The DUMP payload:**
  - It uses the RDB encoding plus an **RDB version and a 64-bit checksum**, and has **no TTL** (read PTTL separately) ([DUMP](https://redis.io/docs/latest/commands/dump/)).
  - RESTORE checks the version and checksum and errors if they don't match ([RESTORE](https://redis.io/docs/latest/commands/restore/)).
  - LRU/LFU metadata is written as separate RDB opcodes (`IDLE` and `FREQ`), so it is not in the payload ([rdb.c](https://github.com/redis/redis/blob/unstable/src/rdb.c)). Carry it over with `RESTORE ... IDLETIME s` or `FREQ n` (inference).
- **RESTORE:** `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]`. A ttl of 0 means no expiry. Without REPLACE you get "Target key name is busy". It is not supported on Active-Active except for module keys ([RESTORE](https://redis.io/docs/latest/commands/restore/)).
- **Streams:** DUMP keeps consumer groups, the PEL, consumers and `entries_read`, because they are part of the stream type ([rdb.c](https://github.com/redis/redis/blob/unstable/src/rdb.c)).
- **Hash field TTLs:** these are stored inside value types 24 and 25, so DUMP carries them to another Redis (inference). They are not portable between Redis and Valkey ([#4651](https://github.com/valkey-io/valkey/issues/4651)).
- **Size limit:** a single RESTORE payload is capped by `proto-max-bulk-len`, 512 MB by default ([shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
- **MIGRATE:** `MIGRATE host port ""|key db timeout [COPY] [REPLACE] [AUTH|AUTH2] [KEYS ...]` ([MIGRATE](https://redis.io/docs/latest/commands/migrate/)).
  - Internally it runs DUMP, then RESTORE, then DEL, and **blocks both instances** while it runs.
  - The `KEYS` form (Redis 3.0.6 and later) pipelines many keys in one call.
  - On `IOERR` the key may exist on both sides. `NOKEY` is not an error.
  - It is not supported on Redis Cloud or Redis Software, and ElastiCache and Azure block it ([EC](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html)).
- **MEMORY USAGE:** `MEMORY USAGE key [SAMPLES n]` defaults to 5 samples; 0 means sample everything ([MEMORY USAGE](https://redis.io/docs/latest/commands/memory-usage/)).

## 6. Keyspace notifications and CLIENT TRACKING as change feeds
- **Keyspace notifications** ([docs](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)):
  - Pub/sub is **fire-and-forget**: events sent while you are disconnected are lost.
  - They are **per node** in a cluster, so you must subscribe to every node.
  - `expired` fires when the key is actually deleted, not when its TTL reaches zero.
  - Only real modifications emit events. The feature costs "some CPU power".
  - Flags: `KEA`. The `n`, `m`, `o` and `c` events are not included in `A`.
- **CLIENT TRACKING BCAST** ([docs](https://redis.io/docs/latest/develop/reference/client-side-caching/)):
  - Sends key names only, with no operation type and **no DB number** (one namespace for all DBs). FLUSH sends `null`.
  - No state is kept on the server. CPU cost grows with the number of prefixes, and prefixes may not overlap.
  - Under RESP2 you need `REDIRECT` plus `__redis__:invalidate`. If the connection drops, you have to treat all data as stale.
  - I assume it is per node in a cluster, like notifications (inference).
- **ElastiCache Serverless** blocks `psubscribe` and `client tracking` ([EC](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html)).

## 7. PSYNC from a client, and managed services
- **How a replica works:** it keeps a replication ID plus an offset. It falls back to a full resync when the backlog is too small or the ID is unknown. Masters turn expiries and evictions into `DEL`. Replicas ACK once a second ([replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)). RedisShake ACKs every 100 ms (§1).
- **ElastiCache:**
  - `sync`, `psync`, `migrate`, `config`, `replicaof`, `bgsave` and `debug` are restricted ([EC](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html)).
  - RedisShake says PSYNC is off by default and can be enabled by support ticket, which gives you a renamed PSYNC command for `aws_psync` ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html), [shake.toml](https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml)).
  - Native "Online Migration" makes ElastiCache a replica of a **self-hosted source on EC2** only ([online](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/OnlineMigration.html)). The source must have no AUTH, and the target no in-transit encryption. Target must be Redis OSS 5.0.6 or later or Valkey, and shard and db counts must match. Serverless and r6gd are not supported ([prepare](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Migration-Prepare.html)).
- **MemoryDB:** no PSYNC, so use scan or RDB ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html)).
- **Alibaba Tair:** PSYNC works with an account that has replication rights ([mode](https://tair-opensource.github.io/RedisShake/en/guide/mode.html)).
- **Azure Cache for Redis:** MIGRATE, PSYNC, REPLICAOF, REPLCONF, SYNC and SAVE are disabled. Keyspace events are set in the portal ([Azure](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/cache-configure)). On Azure Managed Redis, keyspace notifications are in preview ([AMR](https://learn.microsoft.com/en-us/azure/redis/enable-redis-keyspace-notifications)).
- **Tencent:** sync, psync, config set, bgsave and others are not supported ([Tencent cmd ref](https://staticintl.cloudcachetci.com/doc/pdf/product/pdf/239_48362_en.pdf)). Tencent's own DTS requires the source to support SYNC/PSYNC ([DTS](https://www.tencentcloud.com/document/product/239/31941)).
- **Why vendors block it:** proxies in front of Redis, fork memory spikes, and discouraging customers from moving to another cloud ([Huawei](https://support.huaweicloud.com/intl/en-us/redisug-nosql/redis_faq_0216.html)).
- **Redis Software "Replica Of":** a target can have up to 32 sources. It does an initial load, then streams writes, with Syncing/Synced/Sync-stopped states. A restart flushes the target and starts over, and cross-slot errors stop the sync ([Replica Of](https://redis.io/docs/latest/operate/rs/databases/import-export/replica-of/)).

## 8. Python clients
- **redis-py 8.1.0** (MIT, Python 3.10+) ([PyPI](https://pypi.org/pypi/redis/json)). `RedisCluster.pipeline()` groups commands per node, runs the nodes in parallel, and returns replies in the original order. `transaction=True` requires all keys in one slot. Only key-based commands are supported ([docs](https://redis.readthedocs.io/en/stable/advanced_features.html)).
- **valkey-glide 2.5.3** (Apache-2.0, Python 3.9+) ([PyPI](https://pypi.org/pypi/valkey-glide/json)):
  - Wheels for cp39 to cp313 on macOS arm64 and x86_64, and manylinux aarch64 and x86_64.
  - `Batch` and `ClusterBatch` take `is_atomic`. Non-atomic cluster batches are split across slots and nodes automatically. Atomic batches must stay in one slot. Options include `raise_on_error`, timeout and retry settings ([batch](https://glide.valkey.io/how-to/send-batch-commands/)).
  - Its compatibility table lists Redis OSS 6.2 to 7.2 and Valkey 7.2 and later, and **does not list Redis 8.0+** ([repo](https://github.com/valkey-io/valkey-glide)).
- **coredis 6.9.0** (MIT, Python 3.10+): async and fully typed, with cluster and sentinel support ([PyPI](https://pypi.org/pypi/coredis/json)).

## 9. Other tools
- **redis-full-check** ([repo](https://github.com/tair-opensource/RedisFullCheck)): Apache-2.0 since v1.4.10 (GPLv3 before that). v1.4.11 released 2026-03-12, **linux-amd64 only** ([releases](https://github.com/tair-opensource/RedisFullCheck/releases)).
  - **Round 1:** SCAN the source. For each batch (`--batchcount`, default 256; `--parallel` default 5; `--qps` default 15000), fetch the keys from both sides. Classify each as `lack_target`, `type` or `value`. Small collections are read whole (HGETALL, SMEMBERS, ZRANGE WITHSCORES, LRANGE). Collections over the threshold are read with H/S/ZSCAN in batches and compared field by field ([Aliyun](https://developer.aliyun.com/article/690463)).
  - **Storage:** conflicts go into SQLite tables `key(id,key,type,conflict_type,db,source_len,target_len)` and `field(id,field,conflict_type,key_id)` ([repo](https://github.com/tair-opensource/RedisFullCheck)).
  - **Rounds 2 to N** (`--comparetimes` default 3): sleep `--interval` (default 5 s), then re-fetch **only the keys and fields that conflicted in the previous round**. Keys that have caught up drop out, which filters out differences caused by writes still in flight. Each round's snapshot is kept as `key_N`/`field_N`, and the last round is the result ([Aliyun](https://developer.aliyun.com/article/690463)).
  - **`--comparemode`:** 1 = full value, 2 = length only (default), 3 = key existence only, 4 = full value but length only for big keys (`--bigkeythreshold`).
  - **Output:** `--result` writes TSV (`db, diff-type, key, field`). `--metric` writes a metrics file. `--filterlist` takes patterns like `a*|b`.
  - **Limits:** it checks one direction only (source to target). It supports Redis 2.x to 7.x, without modules. It works with standalone, cluster, and the Aliyun and Tencent proxies ([repo](https://github.com/tair-opensource/RedisFullCheck)). TTL comparison is not documented (inference: it is not compared).
- **redis-port (CodisLabs):** MIT. Modes decode, restore, dump and sync (acts as a replica via SYNC/PSYNC). Last push 2018-04-27, effectively dead ([api](https://api.github.com/repos/CodisLabs/redis-port), [repo](https://github.com/CodisLabs/redis-port)).
