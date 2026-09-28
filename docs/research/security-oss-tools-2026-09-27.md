# Security facts for 18 migration tools, one line per factor with sources

Every tool below has an entry for all 10 factors. Two things affected coverage:
- **Search quota.** It ran out partway through (200 of 200 searches used across this run), so the later facts come from fetching official docs, READMEs, source files and GitHub security/release pages directly. "Not documented" means I didn't find it in the pages I checked, not that the feature is definitely absent.
- **How the work was split.** Two helper agents covered pgcopydb, pgloader, mydumper, MySQL Shell, gh-ost, Percona Toolkit, Vitess and TiDB. I did the rest. I've passed on their findings as they reported them and did not re-check them.

Each section starts with a key for its sources; the numbers 1–10 match your factor list.

---

## pgcopydb
Sources: clone = https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html · src = https://github.com/dimitri/pgcopydb/blob/main/src/bin/pgcopydb/pgsql.c · follow = https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_follow.html · stream = https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_stream.html · compare = https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_compare.html · rel = https://github.com/dimitri/pgcopydb/releases · #241 = https://github.com/dimitri/pgcopydb/issues/241 · #624 = https://github.com/dimitri/pgcopydb/issues/624 · pol = https://github.com/dimitri/pgcopydb/security/policy

1. **TLS:** Connection strings are handed unchanged to the PostgreSQL client library (`PQconnectdb`), so its sslmode options apply. pgcopydb's own docs don't cover sslmode, certificates or CRL. [src]
2. **Secrets:** The connection string goes in argv or in `PGCOPYDB_SOURCE_PGURI` / `PGCOPYDB_TARGET_PGURI`. The password is split out of the URI (`parse_and_scrub_connection_string` / `safeURI`), and logs show `password=****`. `.pgpass` use is not documented, and there is no vault. [clone][src][#241]
3. **At rest:** The work dir (`--dir`, default `${TMPDIR}/pgcopydb`) holds schema dumps and SQLite catalogs. Change-data files go in `--dir/cdc`. No encryption. [clone][stream][rel]
4. **Writes on source/target:** Source gets an exported snapshot and a logical slot (default name `pgcopydb`). Target gets a replication origin `pgcopydb`. Versions up to about 0.14 also created schema `pgcopydb` and table `pgcopydb.sentinel` on the source; newer versions keep that state in SQLite. `stream cleanup` removes these. Superuser is needed to read `pg_authid`, otherwise use `--no-role-passwords`. [follow][#241][#624][clone]
5. **Audit:** `PGCOPYDB_LOG_FILENAME` and `PGCOPYDB_LOG_JSON` control logging. `compare data` hashes every row (hashtext, then MD5). This checks integrity; it is not tamper evidence. [clone][compare]
6. **Masking:** Only `--filters` and the `--no-acl` / `--no-owner` switches. Masking is not documented.
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. Releases are source archives only, with no signatures, checksums or SBOM. [pol][rel]
10. **FIPS:** not documented.

## pgloader (3.6.9)
Sources: cmd = https://pgloader.readthedocs.io/en/latest/command.html · pg = https://pgloader.readthedocs.io/en/latest/pgloader.html · my = https://pgloader.readthedocs.io/en/latest/ref/mysql.html · #1219 = https://github.com/dimitri/pgloader/issues/1219 · #308 = https://github.com/dimitri/pgloader/issues/308

1. **TLS:** v3 `sslmode` accepts only disable, allow, prefer or require; `verify-full` is rejected when the command is parsed. The client-certificate environment variable `PGSSLCERT` is not honoured. v4 (a pre-release) takes SSL settings in the JDBC URL. [cmd][#1219][#308][pg]
2. **Secrets:** `user:password@` goes in the URI, on argv or in a load file. Without that, it falls back to `PGPASSWORD`, then `.pgpass` / `PGPASSFILE`. Mustache `{{VAR}}` pulls values from the environment. No redaction is documented. [cmd]
3. **At rest:** Everything goes under `/tmp/pgloader`: the log, plus `reject.dat` / `reject.log`, which contain rejected row data in plaintext. No encryption. [pg]
4. **Writes:** Creates target DDL. `BEFORE` / `AFTER LOAD DO` runs arbitrary SQL. Nothing written on the source is documented, and privileges are not documented. [my]
5. **Audit:** `--logfile`, `--summary`, `--dry-run`. No checksums. [pg]
6. **Transform:** CAST rules, Lisp `USING` transform functions, materialized views, and include/exclude filters. No built-in masking. [my]
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. The ghcr image is built on every push. Signing and SBOM are not documented. https://github.com/dimitri/pgloader
10. **FIPS:** not documented.

## mydumper / myloader
Sources: use = https://mydumper.github.io/mydumper/docs/html/mydumper_usage.html · conn = https://github.com/mydumper/mydumper/blob/master/src/connection.c · ept = https://mydumper.github.io/mydumper/docs/html/exec_per_thread.html · locks = https://mydumper.github.io/mydumper/docs/html/locks.html · req = https://mydumper.github.io/mydumper/docs/html/requirements.html · files = https://mydumper.github.io/mydumper/docs/html/files.html · mask = https://mydumper.github.io/mydumper/docs/html/masquerade.html · rel = https://github.com/mydumper/mydumper/releases

1. **TLS:** `--ssl-mode` accepts DISABLED, PREFERRED, REQUIRED, VERIFY_CA or VERIFY_IDENTITY. Also `--ca`, `--capath`, `--cipher`, `--tls-version`, and client certs via `--cert` / `--key`. No CRL option. [use]
2. **Secrets:** `--password`, `--ask-password` (interactive prompt), `--defaults-file` (default `/etc/mydumper.cnf`) and `--defaults-extra-file`. `hide_password()` overwrites the password in argv with X's. Vault is not documented. [use][conn]
3. **At rest:** No built-in encryption. The documented recipe is `--exec-per-thread` with `openssl enc -aes-256-cbc -pbkdf2 -pass file:$KEY`, with that key wrapped by RSA-OAEP; myloader reverses it. Compression is gzip or zstd, and `--masquerade-filename` disguises file names. [ept]
4. **Locks/grants:** `--sync-thread-lock-mode` defaults to FTWRL; other modes are LOCK_ALL, NO_LOCK, SAFE_NO_LOCK and GTID. It takes `LOCK INSTANCE FOR BACKUP` unless told not to. mydumper needs FLUSH_TABLES, PROCESS, REPLICATION CLIENT, SELECT, SHOW VIEW, EVENT, TRIGGER and SHOW_ROUTINE. myloader needs SESSION_VARIABLES_ADMIN, plus SET_USER_ID + SYSTEM_USER unless `--skip-definer`. No helper schema. [locks][req]
5. **Audit:** A `metadata` file (renamed from `.partial` when the dump finishes) records start/end times, binlog position and per-kind checksums. `--checksum-all` on both sides. The checksums are not signed. [files]
6. **Masking:** Masking functions: `random_string`, `random_int`, `random_uuid`, `random_format`, `apply`, `constant`. Row filters via `--where` and per-table where/limit. [mask]
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. The release page lists SHA256/SHA1/MD5 per package, with no signatures or SBOM. [rel]
10. **FIPS:** not documented.

## MySQL Shell (dumpInstance / loadDump / copyInstance)
Sources: sh = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysqlsh.html · enc = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-encrypted-connections.html · files = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-connection-using-files.html · store = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-pluggable-password-store.html · dump = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utilities-dump-instance-schema.html · load = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utilities-load-dump.html · copy = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utils-copy.html · ssh = https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-connection-ssh.html

1. **TLS:** `--ssl-mode`, `--ssl-ca` / `--ssl-capath`, `--ssl-cert` / `--ssl-key`, `--ssl-crl` / `--ssl-crlpath`, `--tls-version`, `--tls-ciphersuites`. Default is REQUIRED, or VERIFY_CA when a CA is given. The utilities inherit the session's SSL settings. [sh][enc][load]
2. **Secrets:** The docs call `--password=` on the command line "insecure". Alternatives: a prompt, `--passwords-from-stdin`, `--defaults-file` / `--defaults-extra-file`, `--login-path`. Pluggable password stores (login-path, macOS Keychain, Windows Credential Manager) can save passwords. [sh][files][store]
3. **At rest:** Compression is zstd (default), gzip or none. Cloud targets: OCI (API key, instance or resource principal), S3 and Azure (SAS). No client-side encryption or SSE option is documented. The docs warn that anyone holding a pre-authenticated request URL can use it. [dump]
4. **Locks/grants:** `consistent:true` takes FTWRL (or falls back to LOCK TABLES), then a consistent-snapshot transaction, then LOCK INSTANCE FOR BACKUP. The dump needs EVENT, RELOAD, SELECT, SHOW VIEW, TRIGGER, plus REPLICATION CLIENT for the binlog position. Load needs `local_infile=ON` on the target; `skipBinlog` turns off binary logging. Progress goes to `load-progress.<uuid>.json`. The copy utilities send only the predetermined chunks and ignore other file requests from the server. [dump][load][copy]
5. **Audit:** `@.json`, `@.done.json`, and `@.checksums.json` (written with `checksum:true`, checked on load). Logging via `--log-file` / `--log-level`. Not signed. [dump][load]
6. **Masking/transform:** `where` / `partitions` filters and compatibility options such as strip_definers. The helper agent also reported an `allowDataMasking` option that relates to server-side masking policies; I haven't confirmed that one. No per-column transform. [dump]
7. **Network:** Built-in SSH tunnelling with `--ssh user@host:port`, `--ssh-identity-file` and `--ssh-config-file`. It checks `~/.ssh/known_hosts`. [ssh]
8. **RBAC:** none beyond MySQL grants.
9. **Supply chain:** SECURITY.md sends reports to secalert_us@oracle.com. MySQL packages are GPG-signed, but the page doesn't say that covers Shell. SBOM and SLSA are not documented. https://github.com/mysql/mysql-shell/security/policy · https://dev.mysql.com/doc/refman/8.4/en/verifying-package-integrity.html
10. **FIPS:** No `--ssl-fips-mode` in the option reference. Not documented.

## gh-ost
Sources: flags = https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md · req = https://github.com/github/gh-ost/blob/master/doc/requirements-and-limitations.md · ic = https://github.com/github/gh-ost/blob/master/doc/interactive-commands.md · hooks = https://github.com/github/gh-ost/blob/master/doc/hooks.md

1. **TLS:** Off by default. `--ssl`, `--ssl-ca` (only this CA is trusted), `--ssl-cert` / `--ssl-key` for mTLS, and `--ssl-allow-insecure` to skip verification. No CRL. [flags]
2. **Secrets:** `--password` goes on argv; the docs recommend `--ask-pass` or `--conf` (a `[client]` option file). Hooks don't receive the password. No redaction or vault documented. [flags][hooks]
3. **At rest:** No spill files; all state lives in the database. Encrypted binlogs are not supported. [req]
4. **Writes:** Tables `_<tbl>_gho` (ghost), `_<tbl>_ghc` (changelog and heartbeat) and `_<tbl>_del` (the old table). Needs ROW binlog with FULL row image. Grants: ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE, plus SUPER or REPLICATION CLIENT, plus REPLICATION SLAVE. `--switch-to-rbr` changes the replica's binlog format and does not change it back. [flags][req]
5. **Audit:** Hooks (`on-status`, `on-interactive-command` with `GH_OST_COMMAND`) can feed an audit trail. No tamper evidence. [hooks]
6. **Masking:** not documented.
7. **Network:** No SSH. The Unix socket is always on, and `--serve-tcp-port` is optional. Neither is authenticated, yet they accept `panic`, `unpostpone` (which triggers cut-over) and throttle commands. [ic]
8. **Approvals:** `--postpone-cut-over-flag-file` holds cut-over until removed. A non-zero exit from `gh-ost-on-before-cut-over` acts as a gate. [flags][hooks]
9. **Supply chain:** `SHA256SUMS` published, plus an "attest the release artifacts" workflow (v1.1.11). No GPG or cosign signatures. SECURITY.md sends reports to opensource-security@github.com. https://github.com/github/gh-ost/security/policy
10. **FIPS:** not documented.

## Percona Toolkit (pt-online-schema-change, pt-table-checksum, pt-table-sync)
Sources: osc = https://docs.percona.com/percona-toolkit/pt-online-schema-change.html · tc = https://docs.percona.com/percona-toolkit/pt-table-checksum.html · ts = https://docs.percona.com/percona-toolkit/pt-table-sync.html · ssl = https://www.percona.com/blog/unlocking-secure-connections-ssl-tls-support-in-percona-toolkit/

1. **TLS:** DSN key `s` (3.7.0) and `--mysql_ssl` (3.7.1). CA, cert and cipher only go in an option file. Verification modes and CRL are not documented. [ssl][osc]
2. **Secrets:** DSN `p=` / `--password` on argv, `--ask-pass`, `--defaults-file` (DSN key `F=`), `--config`. No redaction documented. [osc]
3. **At rest:** not applicable; no spill files.
4. **Writes:**
   - pt-osc creates `_<tbl>_new` plus triggers on the original table. It needs PROCESS, SUPER, REPLICATION SLAVE plus DML/DDL/TRIGGER grants. [osc]
   - pt-table-checksum writes `percona.checksums` and switches the session to `binlog_format=STATEMENT`. [tc]
   - pt-table-sync "changes data!", including on the source with `--sync-to-master`. [ts]
5. **Audit:** pt-osc `--history` writes `percona.pt_osc_history`. No tamper evidence. [osc]
6. **Masking:** Only `--where` row filters. No masking. [osc][ts]
7. **Network:** No SSH. `--version-check` is on by default and contacts Percona's servers. [osc]
8. **Approvals:** Requires an explicit `--execute`; `--dry-run` and `--print` show what would happen. [osc][ts]
9. **Supply chain:** No SECURITY.md in the repo; company-level disclosure policy instead. GPG signing and checksums are not mentioned in the install docs. https://www.percona.com/security · https://docs.percona.com/percona-toolkit/installation.html
10. **FIPS:** not documented.

## MongoDB mongosync (Cluster-to-Cluster Sync)
Sources: bin = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/mongosync/ · cfg = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/configuration/ · perm = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/permissions/ · start = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/api/start/ · lim = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/limitations/ · ver = https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/verification/embedded/ · auth = https://www.mongodb.com/docs/mongosync/current/reference/authentication/

1. **TLS:** Set through standard connection-string options. The pages I checked have no mongosync-specific TLS section. OIDC (Workload Identity Federation) has been supported since 1.8.1. [auth]
2. **Secrets:** The docs warn that a password in `--cluster0` / `--cluster1` can be visible to `ps`, and recommend `--config` with a YAML file instead. Log redaction is not documented. [bin][cfg]
3. **At rest:** No intermediate files (direct cluster-to-cluster copy). Not applicable.
4. **Writes/grants:**
   - Roles on both clusters: backup, clusterManager, clusterMonitor, readWriteAnyDatabase, restore. The source also needs `find` / `collStats` on `local.oplog.rs`.
   - Write-blocking needs `bypassWriteBlockingMode` and `setUserWriteBlockMode`.
   - The destination must not already contain the `__mdb_internal_mongosync` database.
   - Users and roles are not synced. Client-side field-level encryption and Queryable Encryption are unsupported.
   - [perm][lim]
5. **Audit:** `logPath`, `verbosity` (default DEBUG), metrics logging. The embedded verifier is on by default (documents; from 1.15 also metadata, indexes and views). [cfg][ver]
6. **Transform:** Namespace-level `includeNamespaces` / `excludeNamespaces` only. No document-level filtering or transform. [start]
7. **Network:** HTTP API on port 27182. No SSH or proxy documented. [cfg]
8. **RBAC:** The docs say mongosync "does not protect the start endpoint", relying on it binding to localhost only by default. [start]
9. **Supply chain:** Signing and SBOM are not documented on the install pages.
10. **FIPS:** not documented.

## mongodump / mongorestore
Sources: md = https://www.mongodb.com/docs/database-tools/mongodump/ · mr = https://www.mongodb.com/docs/database-tools/mongorestore/ · gpg = https://www.mongodb.com/docs/database-tools/verify/gpg/ · repo = https://github.com/mongodb/mongo-tools

1. **TLS:** `--ssl`, `--sslCAFile`, `--sslPEMKeyFile` (client cert), `--sslCRLFile`, `--sslAllowInvalidCertificates` / `--sslAllowInvalidHostnames` (docs say avoid). Auth mechanisms: SCRAM, X.509, MONGODB-AWS, GSSAPI, PLAIN. [md]
2. **Secrets:** The docs warn that `--password` or a password in `--uri` can be visible to `ps`. Leaving out `--password` gives a prompt. `--config` (a YAML file with password, uri and sslPEMKeyPassword; added in 100.3.0) is "the recommended way", with a note to "secure this file with appropriate filesystem permissions". sslPEMKeyPassword is redacted from all output. [md][mr]
3. **At rest:** `--gzip` / `--archive` compress only; nothing is encrypted. Collections using client-side field-level encryption or Queryable Encryption can't be dumped. [md]
4. **Writes:** mongodump is read-only. mongorestore `--drop` on `admin` replaces every user with those in the dump. There are also `--restoreDbUsersAndRoles` and `--bypassDocumentValidation`. [mr]
5. **Audit:** `--dryRun`, `--verbose`. No tamper evidence. [mr]
6. **Transform:** `--nsInclude` / `--nsExclude` and `--nsFrom` / `--nsTo` (rename). No masking. [mr]
7. **Network:** No SSH.
8. **RBAC:** Only MongoDB roles.
9. **Supply chain:** GPG `.sig` files signed with the key at `pgp.mongodb.com/server-Tools.asc`, plus RPM, macOS and Windows signing. `cyclonedx.sbom.json` and `SARIF.json` sit in the repo. Issues go to Jira TOOLS; there is no SECURITY.md. [gpg][repo]
10. **FIPS:** `--sslFIPSMode` is not in the current docs. Not documented.

## redis-shake (RedisShake v4)
Sources: toml = https://raw.githubusercontent.com/tair-opensource/RedisShake/v4/shake.toml · sync = https://tair-opensource.github.io/RedisShake/en/reader/sync_reader.html · fn = https://tair-opensource.github.io/RedisShake/en/filter/function.html · pol = https://github.com/tair-opensource/RedisShake/security/policy

1. **TLS:** Only `tls = true`. The docs say there is "no need to configure a certificate because RedisShake does not verify the server certificate". No CA or client-cert options. [sync][toml]
2. **Secrets:** Plaintext `username` (ACL) and `password` in the TOML file. Environment variables and redaction are not documented. [toml]
3. **At rest:** `dir = "data"` holds the RDB and AOF files copied from the source, unencrypted. [toml][sync]
4. **Writes:** Connects to the source as a replica (full RDB, then the AOF stream). `empty_db_before_sync` wipes the destination. `rdb_restore_command_behavior` is panic, rewrite or skip. Required ACL commands are not documented. [sync][toml]
5. **Audit:** `log_file`, `log_level`, `status_port`, `pprof_port` (0 disables them). No tamper evidence. [toml]
6. **Transform:** `allow_*` / `block_*` filters, then a Lua function. A command is dropped unless the script calls `shake.call`. No sandboxing is documented. [fn]
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. The release assets page failed to load, so signing and checksums could not be checked. [pol]
10. **FIPS:** not documented.

## Kafka MirrorMaker 2
Sources: geo = https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/ · cfg = https://kafka.apache.org/43/configuration/mirrormaker-configs/ · code = https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConnector.java · dl = https://kafka.apache.org/community/downloads/ · cve = https://kafka.apache.org/community/cve-list/

1. **TLS:** Per-cluster prefixed settings such as `us-east.security.protocol=SSL` with truststore and keystore paths. SASL is also supported. [geo]
2. **Secrets:** Kafka config providers (FileConfigProvider, DirectoryConfigProvider, EnvVarConfigProvider) keep secrets out of the config file. CVE-2024-31141 (a privilege issue in those providers) is fixed in 3.7.1 / 3.8.0. [cve]
3. **At rest:** Handled by the brokers. Not applicable.
4. **Writes:**
   - Internal topics: heartbeats, checkpoints and offset-syncs; offset-syncs lives on the source by default.
   - `sync.topic.acls.enabled` is on by default. The ACL sync skips ALLOW WRITE and downgrades ALLOW ALL to READ.
   - `sync.topic.configs.enabled` is on by default, with a `config.properties.exclude` list.
   - `sync.group.offsets.enabled` (off by default) writes to the target's `__consumer_offsets`.
   - [cfg][code]
5. **Audit:** not documented.
6. **Transform:** `topics` / `topics.exclude` and `groups` / `groups.exclude` regexes, and a pluggable `replication.policy.class`. [cfg]
7. **Network:** none.
8. **RBAC:** Relies on broker ACLs. In dedicated mode with `dedicated.mode.enable.internal.rest`, the docs say it is "highly recommended to secure the REST servers". [geo]
9. **Supply chain:** `.asc` signatures, `.sha512` checksums and a KEYS file; no SBOM. Connect CVE history includes CVE-2023-25194 and CVE-2025-27818 (remote code execution via JAAS config). [dl][cve]
10. **FIPS:** not documented.

## DataStax DSBulk
Sources: drv = https://docs.datastax.com/en/dsbulk/reference/driver-options.html · log = https://docs.datastax.com/en/dsbulk/reference/logging-options.html · chg = https://github.com/datastax/dsbulk/blob/1.x/changelog/README.md

1. **TLS:** `DefaultSslEngineFactory` with truststore and keystore (mTLS). `hostname-validation` is on by default. `cipher-suites` is configurable. The Astra secure connect bundle is `-b`. [drv]
2. **Secrets:** `-p` goes on argv. DataStax "recommends specifying credentials in a configuration file instead of on the command line". It prompts when no password is configured (1.6.0, DAT-472). Kerberos via GSSAPI. [drv][chg]
3. **At rest:** Unload compression via `connector.csv.compression`. `./logs` holds `*.bad` files with the original source records when `--log.sources` is on. [log]
4. **Writes:** not documented.
5. **Audit:** `operation.log`. The default `log.stmt.level=EXTENDED` prints bound values, so data can end up in logs. No redaction is documented. [log]
6. **Transform:** Mapping options exist, but I didn't check them in detail. No masking.
7. **Network:** not documented.
8. **RBAC:** Only Cassandra permissions; proxy-auth `authorization-id` on DSE. [drv]
9. **Supply chain:** No SECURITY.md. The changelog records dependency upgrades for CVEs (for example 1.11.2). https://github.com/datastax/dsbulk/security/policy
10. **FIPS:** not documented.

## Cassandra Data Migrator (CDM)
Sources: readme = https://github.com/datastax/cassandra-data-migrator · props = https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties

1. **TLS:** `spark.cdm.connect.{origin|target}.tls.enabled` (default false), truststore and keystore paths and passwords (mTLS). The default `enabledAlgorithms` value is `TLS_RSA_WITH_AES_128_CBC_SHA,...`, an older cipher suite. [props]
2. **Secrets:** In the properties file or `--conf` (spark-submit argv). Default credentials are `cassandra` / `cassandra`. No vault. [props][readme]
3. **At rest:** not documented.
4. **Writes:** Tables `cdm_run_info` / `cdm_run_details` in the target keyspace when `trackRun` is on. `autocorrect.missing` / `autocorrect.mismatch` write to the target. [props]
5. **Audit:** Run tracking and the DiffData validation job. No tamper evidence. [readme]
6. **Transform:** Filters: `filter.cassandra.whereCondition`, token percent, writetime, column. Transforms: custom writetime/ttl, `extractJson`, `constantColumns`, `explodeMap`. A `guardrail.colSizeInKB` check. No masking. [props]
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. https://github.com/datastax/cassandra-data-migrator/security/policy
10. **FIPS:** not documented.

## clickhouse-backup (Altinity)
Sources: rm = https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md · rel = https://github.com/Altinity/clickhouse-backup/releases · pol = https://github.com/Altinity/clickhouse-backup/security/policy

1. **TLS:** `clickhouse.secure`, `skip_verify` (default false), `tls_key` / `tls_cert` / `tls_ca` (mTLS). The REST API has its own TLS: `api.secure` with `api.ca_cert_file` / `private_key_file` / `certificate_file`. [rm]
2. **Secrets:** In `config.yml`, and every key can be overridden by an environment variable (unless `general.disable_environment_override`). No vault. [rm]
3. **At rest:** Server-side encryption on S3 via `s3.sse` (AES256 or aws:kms), `s3.sse_kms_key_id`, or customer-provided keys (`sse_customer_algorithm` / `sse_customer_key`). GCS `encryption_key` (customer-supplied). Azure `azblob.sse_key`. No client-side archive encryption; "custom" storage via rclone, restic or kopia can add it. Compression: tar, gzip, zstd, lz4 and others. [rm]
4. **Writes:** Must run on the ClickHouse host with filesystem access. Uses `ALTER TABLE ... FREEZE` / `ATTACH PART`. Optional integration tables (`create_integration_tables`). RBAC objects are backed up (`--rbac`, `rbac_conflict_resolution`). [rm]
5. **Audit:** `log_level`, `log_sql_queries: true`. No tamper evidence. [rm]
6. **Masking:** not documented.
7. **Network:** SFTP storage with `sftp.key` and `known_hosts_file`. No SSH tunnel to ClickHouse. [rm]
8. **RBAC:** The API binds to `127.0.0.1:7171` by default, with basic auth via `api.username` / `api.password`. [rm]
9. **Supply chain:** A SHA256 per asset; no signatures or SBOM. SECURITY.md sends reports to security@altinity.com. [rel][pol]
10. **FIPS:** Ships separate `*-fips` builds for linux and darwin, amd64 and arm64. The README doesn't state a certification level. [rel]

## elasticdump
Sources: rm = https://github.com/elasticsearch-dump/elasticsearch-dump · pol = https://github.com/elasticsearch-dump/elasticsearch-dump/security/policy

1. **TLS:** `--input-ca` / `--output-ca`, client certs via `--input-cert` / `--input-key` / `--input-pass`, `--tlsAuth` for client authentication. Setting `NODE_TLS_REJECT_UNAUTHORIZED=0` disables verification. [rm]
2. **Secrets:** Basic auth embedded in the URL (argv), or the environment variables `ELASTICDUMP_{INPUT,OUTPUT}_{USERNAME,PASSWORD}`. AWS via `--awsChain` or an ini profile, or raw `--awsAccessKeyId` / `--awsSecretAccessKey`. No redaction documented. [rm]
3. **At rest:** `--fsCompress` (gzip) for files. S3 server-side encryption via `--s3ServerSideEncryption` and `--s3SSEKMSKeyId`. Local files are not encrypted. [rm]
4. **Writes:** not documented.
5. **Audit:** not documented.
6. **Transform:** `--transform` runs JavaScript on each document, and `@module` transforms support anonymisation. [rm]
7. **Network:** SOCKS5 via `--inputSocksProxy` / `--outputSocksProxy`, plus custom `--headers`. [rm]
8. **RBAC:** n/a.
9. **Supply chain:** No SECURITY.md. The release commit is GPG-verified, but no asset signatures or SBOM. [pol]
10. **FIPS:** not documented.

## OpenSearch Migration Assistant
Sources: cli = https://docs.opensearch.org/latest/migration-assistant/workflow-cli/ · gs = https://docs.opensearch.org/latest/migration-assistant/workflow-cli/getting-started/ · repo = https://github.com/opensearch-project/opensearch-migrations · sec = https://github.com/opensearch-project/opensearch-migrations/blob/main/SECURITY.md

1. **TLS:** Verification and insecure options are not documented in the pages I checked.
2. **Secrets:** Basic-auth credentials live in Kubernetes secrets (`authConfig.basic.secretName`); SigV4 (AWS signing) is the alternative. The docs note that only the console pod holds AWS credentials. [gs]
3. **At rest:** not documented in the pages I checked (snapshot buckets on S3 or GCS).
4. **Writes:** Snapshot on the source. The capture proxy records live source traffic into Kafka, so sensitive request data is stored there. [repo]
5. **Audit:** `workflow log all | filter`. [cli]
6. **Transform:** Metadata and type-mapping transforms, plus JSON / Jinja / JavaScript transformers. [repo][gs]
7. **Network:** not documented.
8. **Approvals:** Built-in approval gates: `workflow approve step <STEP>` and the `workflow manage` terminal UI. [cli]
9. **Supply chain:** SECURITY.md sends reports to security@opensearch.org. Image signing is not documented. [sec]
10. **FIPS:** not documented.

## Vitess (VReplication / MoveTables)
Sources: tls = https://vitess.io/docs/24.0/user-guides/configuration-advanced/transport-security-model/ · vtt = https://vitess.io/docs/24.0/reference/programs/vttablet/ · vr = https://vitess.io/docs/24.0/reference/vreplication/vreplication/ · mt = https://vitess.io/docs/24.0/reference/vreplication/movetables/ · authz = https://vitess.io/docs/24.0/user-guides/configuration-advanced/authorization/

1. **TLS:** vttablet to MySQL uses `--db-ssl-mode` (up to verify_identity), `--db-ssl-ca` and a minimum of TLS 1.2. gRPC mTLS via `--grpc-ca`, with a CRL via `--grpc-crl`. Tablet-to-tablet VReplication needs `--tablet-grpc-ca` / `--tablet-grpc-server-name`. The docs call TLS "all-or-nothing". [tls][vtt]
2. **Secrets:** `--db-credentials-server` is either a file (reloaded on SIGHUP) or Vault (AppRole, `VAULT_ROLEID` / `VAULT_SECRETID`). vtgate's static auth file can store hashed passwords. [vtt] · https://vitess.io/docs/24.0/user-guides/configuration-advanced/static-auth/
3. **At rest:** not documented.
4. **Writes:** Sidecar database tables `_vt.vreplication`, `_vt.copy_state`, `_vt.vreplication_log`. Needs ROW binlog, FULL row image and GTID. Complete / Cancel drops the source tables unless `--keep-data` or `--rename-tables`. [vr][mt]
5. **Audit:** `_vt.vreplication_log`. No tamper evidence. [vr]
6. **Transform:** Filter rules are SQL SELECTs, so WHERE clauses and expressions can mask or reshape data in flight. MoveTables adds `--tables` / `--exclude-tables`. [vr][mt]
7. **Network:** No SSH. SECURITY.md says to restrict the vttablet gRPC endpoint to trusted vtgates. https://raw.githubusercontent.com/vitessio/vitess/main/SECURITY.md
8. **RBAC:** Table ACLs (`--table-acl-config`, strict mode), static gRPC auth with mTLS allowed-substrings, `--security-policy` (empty means allow all), VTAdmin `rbac.yaml`. Whether table ACLs cover VReplication is not documented. [authz] · https://vitess.io/docs/24.0/reference/vtadmin/role-based-access-control/
9. **Supply chain:** Reports go to cncf-vitess-maintainers@lists.cncf.io. Audited by Cure53 (2019) and Ada Logics (2023; 2 moderate CVEs, SLSA provenance still "in progress"). The v24.0.3 release has no checksums or signatures. https://vitess.io/blog/2023-06-05-vitess-security-audit/
10. **FIPS:** not documented.

## TiDB DM and TiDB Lightning
Sources: dmtls = https://docs.pingcap.com/tidb/stable/dm-enable-tls · src = https://docs.pingcap.com/tidb/stable/dm-manage-source · dmm = https://docs.pingcap.com/tidb/stable/dm-master-configuration-file · task = https://docs.pingcap.com/tidb/stable/task-configuration-file-full · lcfg = https://docs.pingcap.com/tidb/stable/tidb-lightning-configuration · lreq = https://docs.pingcap.com/tidb/stable/tidb-lightning-requirements · lerr = https://docs.pingcap.com/tidb/stable/tidb-lightning-error-resolution

1. **TLS:** DM uses `ssl-ca` / `ssl-cert` / `ssl-key` on master and worker, and `cert-allowed-cn` checks the caller's certificate name (mTLS). Upstream and downstream each have their own `security` block. Lightning's `tidb.tls` accepts `false`, `cluster` or `skip-verify`. [dmtls][lcfg]
2. **Secrets:** DM's `dmctl encrypt` encrypts passwords. Since v8.0 `secret-key-path` is required (an AES-256 key), but plaintext passwords are still accepted. Lightning passwords are plaintext or Base64, which is only encoding. No Vault/KMS. [src][dmm][lcfg]
3. **At rest:** DM's relay log and dump dir are not encrypted (`clean-dump-file` removes the dump). Lightning's `sorted-kv-dir` is not encrypted. [task]
4. **Writes/grants:**
   - DM upstream needs RELOAD, REPLICATION SLAVE, REPLICATION CLIENT, SELECT; the downstream needs DML/DDL plus `ALL ON dm_meta.*`.
   - Lightning physical mode needs SUPER. It writes `lightning_task_info` and `tidb_lightning_checkpoint`.
   - https://docs.pingcap.com/tidb/stable/dm-precheck · [lreq]
5. **Audit:** Lightning's error tables (`type_error_v1`, `conflict_error_v3`, `conflict_records`) store the actual row data. DM audit logging is not documented. [lerr]
6. **Transform:** DM binlog event filter, row-level `expression-filter`, block-allow lists and routes. No masking. [task]
7. **Network:** No SSH. The DM OpenAPI and Lightning's `status-addr` document no authentication. https://docs.pingcap.com/tidb/stable/dm-open-api
8. **RBAC:** Only the certificate-name allow-list between DM components.
9. **Supply chain:** Reports go to security@pingcap.com (tidb) or security@tidb.io (tiflow, where DM lives). TiUP mirror metadata is signed with threshold keys in `root.json`, and packages carry sha256 / sha512. https://docs.pingcap.com/tidb/stable/tiup-mirror-reference
10. **FIPS:** not documented.

## CockroachDB MOLT (Fetch / Verify / Replicator)
Sources: f = https://docs.cockroachlabs.com/docs/molt/molt-fetch · v = https://docs.cockroachlabs.com/docs/molt/molt-verify · r = https://docs.cockroachlabs.com/docs/molt/molt-replicator · rel = https://docs.cockroachlabs.com/docs/releases/molt · utils = https://pkg.go.dev/github.com/cockroachdb/molt/utils

1. **TLS:** Secure connections have been required by default since v0.2.1; `--allow-tls-mode-disable` is the opt-out. Examples use `sslmode=verify-full`. Oracle wallet TLS via `--source-oracle-wallet-location`. Replicator uses `--tlsCertificate` / `--tlsPrivateKey`, and the docs say to avoid `--tlsSelfSigned` and `--disableAuthentication` in production. [rel][f][r]
2. **Secrets:** The password sits in the `--source` / `--target` URL; the docs' examples pass it as `$SOURCE`. `molt escape-password` percent-encodes special characters. Cloud credentials come from environment variables or `--use-implicit-auth` / `--assume-role`. Credentials in `--bucket-path` query parameters are ignored, and storage-URL query parameters are redacted when shown. Redaction of the database connection string is not documented. [f][utils]
3. **At rest:** Intermediate files go to S3, GCS or Azure, or to `--local-path` served from `--local-path-listen-addr`. Encryption is not documented; the docs point to "Cloud storage security best practices". [f]
4. **Writes/grants:**
   - Fetch writes `_molt_fetch_exceptions` on the target and creates a PostgreSQL publication and slot.
   - Replicator keeps a staging schema (for example `_replicator`, with `memo` and `_oracle_checkpoint` tables); Oracle sources need `REPLICATOR_SENTINEL`.
   - Source grants for Fetch: PostgreSQL CONNECT/USAGE/SELECT, MySQL SELECT, Oracle SELECT plus FLASHBACK. Verify needs SELECT only (read-only).
   - [f][r][v]
5. **Audit:** Structured JSON logs and `--log-file`. Verify prints a JSON summary (missing, mismatch, extraneous). Metrics at `127.0.0.1:3030` (Verify) and `/_/varz` (Replicator). Telemetry opt-out via `--opt-out-telemetry`. [v][r]
6. **Transform:** `--transformations-file` for column exclusion and renames. Replicator TypeScript `--userscript` can filter rows or columns and transform values, which is how masking would be done. [f][r]
7. **Network:** not documented.
8. **RBAC:** Replicator has endpoint authentication, which can be switched off with `--disableAuthentication` (docs: not for production). [r]
9. **Supply chain:** Released under the Cockroach Labs product license since v1.0.0. Dependabot keeps Go modules current, and the `version` command reports build details. Signing and SBOM are not documented. The GitHub repo returned 404. [rel][r]
10. **FIPS:** not documented for MOLT.

## YugabyteDB Voyager
Sources: cli = https://docs.yugabyte.com/preview/yugabyte-voyager/reference/yb-voyager-cli/ · cfg = https://docs.yugabyte.com/stable/yugabyte-voyager/reference/configuration-file/ · rn = https://github.com/yugabyte/yb-voyager/blob/main/RELEASE_NOTES.md · live = https://docs.yugabyte.com/stable/yugabyte-voyager/migrate/live-migrate/ · inst = https://docs.yugabyte.com/stable/yugabyte-voyager/install-yb-voyager/

1. **TLS:** `--source-ssl-mode` accepts disable, allow, prefer (default), require, verify-ca or verify-full. Also `--source-ssl-cert` / `-key` / `-root-cert` / `-crl`, with matching `--target-ssl-*` options. Oracle uses a TNS alias with a wallet. [cli][cfg]
2. **Secrets:**
   - `--source-db-password` goes on argv. The `SOURCE_DB_PASSWORD` / `TARGET_DB_PASSWORD` environment variables are the alternative; the release notes say they keep the password out of `ps` and out of config and log files.
   - The config file also has `db-password`, but CLI flags override the config file.
   - A runtime password prompt was not confirmed.
   - [rn][cfg]
3. **At rest:** Export-dir encryption is not documented. [cli]
4. **Writes/grants:** A dedicated `ybvoyager` user and a replication slot. The `yb-voyager-pg-grant-migration-permissions.sql` script sets REPLICA IDENTITY FULL on the tables. `end migration` removes the slot and Voyager's state on the target. [live]
5. **Audit:** not documented. Diagnostics are sent by default (`--send-diagnostics` defaults to true). [inst][cfg]
6. **Masking:** not documented.
7. **Network:** not documented.
8. **RBAC:** none.
9. **Supply chain:** No SECURITY.md. Distributed via yum, apt, brew and Docker, with signing not documented. https://github.com/yugabyte/yb-voyager/security/policy · [inst]
10. **FIPS:** not documented.

---

## Practices a small Python migration CLI could copy

1. **Never take passwords on argv.** Support, in this order: an env var (Voyager's `SOURCE_DB_PASSWORD`), a 0600 config file (mongodump `--config`, DSBulk's recommendation), `--passwords-from-stdin` (MySQL Shell), and a `getpass` prompt (mongodump, mydumper, DSBulk 1.6). mydumper overwrites argv after reading it, but Python can't reliably do that, so reject passwords in argv instead.
2. **Hand credentials to child tools through files, not argv.** Use a temp `PGPASSFILE` or `--defaults-extra-file` created with mode 0600 in a 0700 directory and deleted afterwards; pgloader, mydumper and MySQL Shell already read these.
3. **Redact everywhere.** Scrub connection strings before any log or exception output. Examples: pgcopydb logs `password=****`, MOLT redacts storage-URL query parameters, mongodump redacts sslPEMKeyPassword. Also avoid logging bound values: DSBulk's default EXTENDED statement logging does.
4. **Secure TLS by default.** Default to `verify-full` / `VERIFY_IDENTITY` and require an explicit, loudly named opt-out (MOLT's `--allow-tls-mode-disable`). Expose CA, client cert/key and CRL for both ends (Voyager, MySQL Shell). Avoid redis-shake's "TLS without verification" and CDM's old default cipher.
5. **Encrypt spill and staging files.** Stream them through `age` or `openssl` with a wrapped data key (mydumper's exec-per-thread recipe), use SSE-KMS for object storage (clickhouse-backup, elasticdump), and keep the work dir at 0700 with automatic cleanup. Reject and conflict files (pgloader `reject.dat`, DSBulk `.bad`, Lightning conflict tables) hold real row data, so make them opt-in or encrypt them.
6. **Checksummed manifest with a signature.** Keep a manifest of per-file hashes (MySQL Shell `@.checksums.json`, mydumper `metadata`) and sign it (HMAC or minisign) for tamper evidence; none of these tools signs theirs. Use `hashlib.md5(usedforsecurity=False)` so non-security checksums don't break under FIPS.
7. **Document every footprint and give it a cleanup command.** List what gets written (slots, publications, helper tables, triggers), provide cleanup (`pgcopydb stream cleanup`, Voyager `end migration`), and ship a least-privilege grant script plus a preflight grant check (Voyager guardrail script, DM precheck, mongosync permission tables).
8. **Local control and metrics endpoints bind to 127.0.0.1 and require auth.** clickhouse-backup does both. Avoid mongosync's unprotected start endpoint and gh-ost's unauthenticated socket.
9. **Approval gates for destructive steps.** Read-only by default (MOLT Verify), an explicit `--execute` (pt-osc), a cut-over flag file or hook (gh-ost), or `approve step` (OpenSearch MA). Include a dry-run.
10. **Telemetry and version checks off by default.** In confidential environments they should be opt-in; Voyager diagnostics, mongosync telemetry and pt `--version-check` are all on by default today.
11. **A masking hook per column.** Plain Python functions following the mydumper masking-function pattern (`random_string`, `constant`, `apply`); comparable hooks are redis-shake Lua, MOLT userscripts and elasticdump `--transform`.
12. **Supply chain.** Publish a SECURITY.md (many of these tools don't), SHA256SUMS plus Sigstore signatures (PyPI Trusted Publishing gives PEP 740 attestations; gh-ost uses GitHub artifact attestations), and a CycloneDX SBOM in each release (mongo-tools keeps `cyclonedx.sbom.json` in its repo). Use Dependabot for dependency CVEs, as MOLT does.
