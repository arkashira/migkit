# Security and throughput scorecard: migkit against the 31 tools (2026-09-28)

**Status of this report (written under a stop order; nothing below was cut):**
- A1 migkit security column, 10 factors with file:line: **complete**.
- A2 scorecard (31 tools + migkit x 10 factors) and per-factor verdict: **complete**; tool cells compress the two 2026-09-27 security reports, so their `?` (not verified) cells are inherited, not re-researched.
- A3 gap table (16 fixes with mechanism, library, effort): **complete**.
- B1 migkit's measured baseline: **complete**.
- B2 technique scorecard (22 techniques) and B3 docker recipes: **complete**.
- B4 fresh rates: GoldenGate, Qlik, Vitess, `pg_dump -j` **complete from public pages**; mongosync **partial** (no published rate exists; mechanisms and field reports only). Two primary pages (OCI GoldenGate PDF, PhysicsWallah post) returned 403; their figures come from search extracts and are flagged.
- Not done: re-verification of the earlier reports' `?` cells (Striim 3-10, Qlik audit/FIPS, Alibaba RAM/ActionTrail, Google FIPS); MySQL bulk programs' own TLS flags; no measurement was run (read-only pass).

Two questions, answered factor by factor: is migkit behind, equal or ahead, and what closes each gap.

This builds on four reports written the day before and does not repeat their facts; every tool cell below is a compression of those pages, and the tool sources live there:
- `docs/research/security-oss-tools-2026-09-27.md` (19 open-source tools, 10 factors)
- `docs/research/security-managed-tools-2026-09-27.md` (12 managed and commercial services)
- `docs/research/throughput-published-2026-09-27.md` (published rates of DMS, DTS, pgcopydb, MySQL Shell, mydumper, Lightning)
- `docs/research/throughput-cdc-techniques-2026-09-27.md` (PeerDB, Artie, Estuary, Fivetran, ConnectorX, dlt, ADBC; per-technique numbers)

What is new here: the migkit column, every claim backed by a `file:line` in this repository as of today; the two scorecards; the gap tables with a mechanism, a library and an effort for each; the docker recipe for each throughput technique; and fresh searches for the five rates the earlier report could not find (GoldenGate, Qlik, Vitess, mongosync, `pg_dump -j`).

Method for the migkit column: `grep`/`sed` over `migkit/`, `tools/`, `bench/`, `.github/workflows/`, `pyproject.toml`, `docs/threat-model.md`, `docs/backlog.md`, `docs/scale.md`. No database was opened, no docker run, no test executed. Line numbers are from the working tree on 2026-09-28.

---

## Part A. Security

### A1. migkit, factor by factor, with evidence

**1. TLS: modes, verification, mTLS, CRL**

Has:
- PostgreSQL: the endpoint's `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `sslcrl` reach every driver connection and every libpq program through `PGSSL*` (`migkit/config.py:83-91`; used at `migkit/engines/postgres.py:440,472,849,7578` and in every mover env, e.g. `migkit/movers.py:710,995`). That is all six libpq modes, client certificates and a CRL.
- MySQL: `ssl_ca` turns on certificate and hostname verification (`ssl_verify_identity` default true), `ssl_cert`/`ssl_key` give mTLS (`migkit/config.py:93-108`). No CRL option.
- Kafka: `security_protocol` PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL, SCRAM/PLAIN/AWS_MSK_IAM, `ssl_cafile` (`migkit/engines/kafka.py:20-60`).
- OpenSearch: HTTPS with an explicit `verify: false` opt-out (`migkit/engines/opensearch.py:66-81`).
- `doctor` asks each server what it sees on the wire: `pg_stat_ssl` (`migkit/engines/postgres.py:182-192`), `Ssl_version`/`Ssl_cipher` (`migkit/engines/mysql.py:78-90`), wired at `migkit/cli.py:445`; the base engine answers None (`migkit/engines/base.py:3076-3080`), so only PostgreSQL and MySQL report.

Lacks:
- **The default checks nothing.** Left unset, PostgreSQL goes out as `sslmode=prefer` (`migkit/engines/postgres.py:111-115`; the comment at `migkit/config.py:80-82` says so) and MySQL takes TLS unverified; `ssl: true` on MySQL builds a context with `check_hostname = False` and `CERT_NONE` (`migkit/config.py:109-118`). The backlog records that before R17a no connection took a TLS setting at all (`docs/backlog.md:4297-4306`).
- MongoDB: no TLS options of its own; only what the operator appends through `uri_options` (`migkit/engines/mongodb.py:26-32`). Redis (`migkit/engines/redis.py:31,1094`), SQL Server (`migkit/engines/mssql.py:671`), Cassandra (`migkit/engines/cassandra.py:38`): no TLS parameters. ClickHouse: `secure` only, no CA or verify switch (`migkit/engines/clickhouse.py:38-44`).
- The MySQL bulk programs' own TLS flags are not passed (backlog R17a "still to", `docs/backlog.md:4306-4307`).

**2. Secrets handling**

Has:
- Six reference forms resolved at load: `env:`/`${}`, `file:`, `vault:`, `aws-sm:`, `gcp-sm:`, `azure-kv:` (`migkit/config.py:329-364`); RDS/Aurora IAM tokens re-minted every 10 minutes, cross-account role assumption (`migkit/config.py:121-160`).
- Child programs never see a password on argv: `PGPASSWORD`/`MYSQL_PWD` in the environment (`migkit/movers.py:662,710,2227,2237`, reasoning at `2112-2122`), `PGPASSFILE` at 0600 for pgcopydb (`migkit/movers.py:2888,2949-2952`), a 0600 `[client]` defaults file for DSN-taking tools (`migkit/movers.py:1774-1800`), a private `--config` for mongosync/mongodump (`migkit/movers.py:2410-2418,2539`), atlas URLs through env (`migkit/movers.py:1750-1770`), the relay's password on the far process's stdin (`migkit/engines/postgres.py:7166-7175`). Held by `tests/test_no_program_is_handed_a_password.py`.
- Redaction everywhere a command line is written: `_SECRET_ENV` (`migkit/movers.py:274`) and `_debug` (`371-375`) feed `wording.redact` (`migkit/wording.py:184-219`), so `commands.log` never holds a secret; the diagnostics bundle scrubs keys matching `pass|pwd|secret|token|key|credential|notify|auth` and credentials inside URLs (`migkit/diagnostics.py:23-44`).
- Wrapped programs that phone home are silenced: mongosync `disableTelemetry: true` (`migkit/movers.py:2528-2536`).

Lacks:
- A plaintext `password:` in `hops.yaml` is still accepted (`migkit/config.py:446`); `SECURITY.md` says references are the way, the loader does not enforce it.
- No `--passwords-from-stdin` or `getpass` prompt.
- Secrets read once at load; a rotation mid-run is not re-read (DMS has the same limitation).

**3. Encryption at rest: spill, dump, logs**

Has:
- Every file that holds application values under a hop's report directory (drilldown keys, undo rows, two-way conflicts, 29 call sites) goes through `EvidencePath`, which seals to the hop's `at_rest.recipients` (SSH or age public keys) with pyrage and opens with `MIGKIT_IDENTITY` (`migkit/evidence.py:1-18,42-77,84-135`); a line-appended record is sealed a line at a time (`137-155`). `tests/test_what_holds_values_is_kept_encrypted.py`.
- Restore points and run state leaving the working directory are Fernet-sealed under a PBKDF2-SHA256 key (390,000 iterations, a salt per point) when `MIGKIT_STATE_KEY` is set (`migkit/state.py:85-133`).
- The local dump copy is 0700 and removed however the move ends (`migkit/movers.py:1025`; `docs/threat-model.md:32-35`); the mask salt is 0600 (`migkit/masking.py:69-80`); temp files are created `O_EXCL` 0600 (`migkit/movers.py:1792,2418,3312`).

Lacks:
- The dump files the wrapped programs write (`pg_dump -Fd`, mydumper, mongodump) are not encrypted; the module says so (`migkit/evidence.py:16-18`; backlog R17b "an encrypted disk for now", `docs/backlog.md:4327-4329`).
- Report summaries, `commands.log` (redacted, no values) and mongosync's own logs/metrics in the report directory are plain.
- No KMS-backed master key; recipients are people's keys, not a key service.
- `docs/threat-model.md:89-93` still says the report directory is not encrypted at rest; `evidence.py` has overtaken it.

**4. Source/target footprint and least privilege**

Has:
- The source is read, not written: `_target_only` refuses writes to the source side (`docs/threat-model.md:18-27`), business rules run read-only (`migkit/engines/postgres.py:195-199`).
- The only source objects: a PostgreSQL publication (dropped by `drop_src`, `migkit/engines/postgres.py:7530`), a slot (dropped, `6872`), a MySQL replication user with a fresh random password and `REPLICATION SLAVE` only (`migkit/engines/mysql.py:5709-5714`). Nothing of migkit's in the target except `migkit_origin`, and only when a hop asks for two-way (`migkit/twoway.py:48`; `migkit/engines/postgres.py:1198`; `migkit/engines/mysql.py:705`).
- Triggers and foreign keys are quieted per session, never altered: `session_replication_role=replica` (`migkit/engines/postgres.py:437,749,844`), `foreign_key_checks=0` (`migkit/engines/mysql.py:612`); a role lacking `SET ON PARAMETER` is told the exact grant (`migkit/movers.py:1359`).
- pg_authid is read only where allowed; managed sources fall back to names and a passwords file (`migkit/users.py:91-104`).

Lacks:
- The publication is `FOR ALL TABLES` (`migkit/engines/postgres.py:7521-7523`), the same default the managed report criticised in Debezium.
- No least-privilege grant script per engine and no preflight "does this account hold what the move needs" (Voyager's script, DM's precheck, mongosync's permission table, GoldenGate's `OGG_CAPTURE`/`OGG_APPLY`). `pg_has_role`/`relacl` are read for ownership and grant comparison (`migkit/engines/postgres.py:2523,4989-5006`), not for a preflight.
- `doctor` does not yet name `migkit_origin` as footprint, and the two-way teardown does not drop it (`docs/backlog.md:4657-4662`).

**5. Audit and tamper evidence**

Has:
- `changelog.jsonl` is a SHA-256 hash chain: each entry carries `prev` and its own hash over canonical text (`migkit/audit.py:45-66`); `verify` reports how many are chained and where the chain breaks (`68-98`); wired for every operation (`migkit/cli.py:48-49`), every dashboard action with who took it (`migkit/ui.py:528-533`), every approval (`migkit/approvals.py:101-102`); `history` runs the verification (`migkit/cli.py:2683-2684`).

Lacks:
- No signed checkpoint and no external anchor (R5 "signed checkpoints, optionally anchored", `docs/backlog.md:3878-3880`): the user who owns the file can rewrite the whole chain from the first entry.

**6. Masking**

Has:
- Drilldown keys and values shown as `masked:` + 10 hex of SHA-256(salt || value), salt per hop at 0600, equal values equal (`migkit/masking.py:69-91`); columns and keys selectable (`24-67`). `tests/test_the_drilldown_can_be_masked.py`.
- Row filters push down to the mover (mydumper defaults file per table, `docs/what-a-migration-actually-costs.md:203`); `rules:` are read-only checks, not transforms (`migkit/rules.py:1-20`).

Lacks:
- A salted hash, not a keyed HMAC; 40 bits shown. Display-only: what migkit writes for its own repairs is not masked (`migkit/masking.py:16-18`), and nothing is masked at capture.
- No per-column transform hook (mydumper functions, MOLT userscript, elasticdump `--transform`, redis-shake Lua, Debezium `column.mask.*`).

**7. Network: tunnels, private link, proxy**

Has:
- `tunnel: {ssh: ...}` per endpoint: `ssh -N -L` with `BatchMode`, `ExitOnForwardFailure`, keepalives, `-J` jump hosts, `key`, `port`, `ssh://` URLs (`migkit/tunnel.py:186-208,362`); host keys `accept-new` by default with `host_keys` and `known_hosts` overrides (`194-198`); `tunnel: {command: ...}` for SSM, IAP, cloud SQL proxies (`migkit/tunnel.py:4,181-187`).
- Several ssh legs behind a local splitter when the bastion is far (`migkit/tunnel.py:70-140,288-315`); the PostgreSQL read done beside the source over the same ssh, zstd-compressed (`migkit/engines/postgres.py:7140-7178`). `tests/test_a_server_behind_a_bastion_is_reached.py`.
- The dashboard binds 127.0.0.1 and serves only its own host names (`migkit/ui.py:418-423,547`).

Lacks:
- No reverse tunnel (`-R`) for a source that must dial out, no PrivateLink/PSC guidance, no SOCKS/HTTP proxy, no outbound-only agent.
- `accept-new` is trust-on-first-use; there is no pin-and-revoke record of the fingerprint accepted (Fivetran shows the fingerprint for approval and lets it be revoked).

**8. RBAC and approvals**

Has:
- Approvals signed with approvers' own SSH keys against an `allowed_signers` file, `ssh-keygen -Y find-principals` then `verify`; the request carries hop, step, database, nonce, expiry; the requester's own signature does not count; a count of distinct approvers (`migkit/approvals.py:1-17,42-75,77-115`). `tests/test_a_step_waits_for_its_approvers.py`.
- Dashboard: `access: {operator: [...], viewer: [...]}` shell patterns; a hop not naming the user is not shown (`migkit/ui.py:370-391`); token per start in an `HttpOnly` cookie compared with `hmac.compare_digest`, CSRF as HMAC of the token on every POST, host allowlist, proxy-header identity for a shared view (`migkit/ui.py:425-451,494-521`).
- Read-only by default; anything that writes needs `--go`/`--apply` and saves an undo (`SECURITY.md`, "Scope and design").

Lacks:
- The token is printed in the URL query (`migkit/ui.py:548`), so it lands in browser history and any proxy log before it becomes a cookie.
- No SSO/SCIM/2FA of its own (delegated to the proxy in front); no `approver` role in the view by design (`docs/backlog.md:3851-3853`).

**9. Supply chain**

Has:
- `SECURITY.md` with private reporting; `CODEOWNERS`; `tools/check_no_secrets.py` run on every release (`.github/workflows/release.yml`, job `verify`).
- Release: full suite, `python -m build`, `twine check`, clean-venv install, CycloneDX SBOM of the installed wheel (`cyclonedx-py environment`), `SHA256SUMS`, `actions/attest-build-provenance@v2` and `actions/attest-sbom@v2` (Sigstore) (`.github/workflows/release.yml`, job `build`).
- Weekly matrix of every wrapped program version against the flags migkit passes (`.github/workflows/wrapped-programs.yml`).

Lacks:
- No PyPI publishing, so no Trusted Publishing and no PEP 740 attestations; the release is GitHub-only.
- No Dependabot/Renovate (`.github/dependabot.yml` absent), no `pip-audit` step in `ci.yml`.
- `pyproject.toml` has lower bounds only, no lock file; several dependencies unbounded (`results`, `pandas`, `pyarrow`, `boto3`); actions pinned by tag, not SHA.

**10. FIPS**

Has: nothing documented.

Lacks:
- `hashlib.md5(...)` without `usedforsecurity=False` at `migkit/canon.py:1070`, `migkit/tally.py:28`, `migkit/users.py:31`, `migkit/engines/postgres.py:1154`, `migkit/engines/sqlite.py:847`, `migkit/engines/mongodb.py:1301`, `migkit/engines/kafka.py:1323`; `hashlib.sha1` at `migkit/engines/parquet.py:314` and `migkit/engines/dynamodb.py:141`. Under a FIPS-enforcing OpenSSL these raise.
- age (X25519 + ChaCha20-Poly1305) and ed25519 SSH signatures are not FIPS-approved primitives; Fernet (AES-128-CBC + HMAC-SHA256) is.

### A2. Scorecard: 31 tools and migkit, 10 factors

Cells: `++` best-in-class for that factor, `+` present or partial, `-` none or not documented, `?` not verified in the source reports. The factor numbers are the ten above. Facts per tool are from the two 2026-09-27 security reports; migkit's from A1.

| Tool | 1 TLS | 2 Secrets | 3 At rest | 4 Footprint | 5 Audit | 6 Masking | 7 Network | 8 RBAC | 9 Supply | 10 FIPS |
|---|---|---|---|---|---|---|---|---|---|---|
| pgcopydb | + libpq passthrough | + env URI, `password=****` | - work dir plain | + slot, origin; superuser for roles | + JSON log, unsigned hashes | - | - | - | - no sigs/SBOM/policy | - |
| pgloader | - no verify-full | - argv/PGPASSWORD, no redaction | - reject.dat plain | + arbitrary SQL hooks | + log, dry-run | + CAST/USING, no masking | - | - | - | - |
| mydumper | + VERIFY_IDENTITY, certs, no CRL | + ask-password, defaults file | + openssl recipe only | + FTWRL modes, broad grants | + metadata checksums unsigned | ++ masking functions | - | - | - checksums only | - |
| MySQL Shell | ++ modes, CRL, default REQUIRED | ++ stdin, login-path, keychain | + zstd, no encryption | + LOCK INSTANCE, chunks pinned | + checksums.json unsigned | + where/partitions | ++ built-in SSH, known_hosts | - | + SECURITY.md, GPG | - |
| gh-ost | + off by default, mTLS, no CRL | + ask-pass, conf | + no spill | - `_gho/_ghc/_del`, rbr switch stays | + hooks | - | - unauthenticated socket | + cut-over flag, hook gate | ++ SHA256SUMS + attestations | - |
| Percona Toolkit | + DSN `s`, verify ND | + ask-pass, defaults file | + no spill | - triggers, `percona.checksums`, STATEMENT binlog | + pt_osc_history | - | - version-check on | + `--execute`, dry-run | - | - |
| mongosync | + URI options, OIDC | + config file | + no spill | - 5 roles, `__mdb_internal_mongosync` | + verifier on | - namespaces only | - HTTP 27182 | - unprotected start | - | - |
| mongodump/restore | ++ CA, PEM, CRL | + config yaml, PEM pw redacted | - gzip only | + dump read-only; `--drop admin` | + dryRun | - rename only | - | - | ++ GPG, CycloneDX, SARIF | - |
| redis-shake | - no verification | - plaintext TOML | - data dir plain | - wipes destination option | + log, status port | + Lua, unsandboxed | - | - | - | - |
| MirrorMaker 2 | + SSL/SASL stores | ++ config providers | + brokers | + internal topics, ACL downgrade | - | + regex, policy class | - | + broker ACLs | + asc/sha512, Connect CVEs | - |
| DSBulk | ++ mTLS, hostname on | + config file, prompt | - `.bad` plain | ? | - bound values logged | - | - | + proxy-auth (DSE) | - | - |
| CDM | + mTLS, old cipher default | - default creds | - | - `cdm_run_*`, autocorrect writes | + run tracking | + filters, no masking | - | - | - | - |
| clickhouse-backup | ++ mTLS, API TLS | + yaml + env | ++ SSE/CMK, no client-side | + on-host FREEZE/ATTACH | + log_sql_queries | - | + SFTP known_hosts | ++ 127.0.0.1 + basic auth | + sha256, SECURITY.md | ++ fips builds |
| elasticdump | + CA, client certs | + env or URL | + S3 SSE-KMS | ? | - | ++ JS transform, anonymise | + SOCKS5 | - | - | - |
| OpenSearch MA | ? | + k8s secrets, SigV4 | ? | - traffic captured to Kafka | + workflow log | + transformers | ? | ++ approve step | + SECURITY.md | - |
| Vitess | ++ verify_identity, gRPC mTLS + CRL | ++ file/SIGHUP or Vault | - | - `_vt` tables, drops source tables | + `_vt.vreplication_log` | + SQL filter rules | - | ++ table ACLs, VTAdmin RBAC | + audits, no release sigs | - |
| TiDB DM / Lightning | ++ mTLS, cert CN allowlist | + AES-encrypted or plaintext | - relay/dump/sorted-kv plain | - `dm_meta`, SUPER for physical | - error tables hold rows | + filters | - OpenAPI unauthenticated | + CN allowlist | + TiUP signed mirror | - |
| MOLT | ++ secure by default, loud opt-out | + URL via env, storage redacted | - | + exceptions table, pub+slot; Verify read-only | ++ JSON logs, telemetry opt-out | + transforms, userscript | - | + endpoint auth | + Dependabot | - |
| Voyager | ++ 6 modes, CRL both ends | + env vars; argv allowed | - | + own user, grant script, REPLICA IDENTITY FULL | - diagnostics on by default | - | - | - | - | - |
| AWS DMS | + default none, no mTLS | ++ Secrets Manager, IAM auth | ++ KMS | - `awsdms_ddl_audit`, grant to public, superuser | + CloudTrail, 10-day logs | + digits/hash, unsalted | ++ VPC, PrivateLink | + IAM fine-grained | ? | + FIPS API endpoints |
| Google DMS / Datastream | + SERVER_CLIENT mTLS, no CRL | + Secret Manager (Datastream) | ++ CMEK | + pglogical + PUBLIC grants (DMS); pub+slot (Datastream) | + Cloud Audit Logs | - | ++ reverse/forward SSH, PSC | + IAM | ? | ? |
| Azure DMS | + TLS1.2 default, can be off | - Key Vault False | + no customer content | + JIT permissions removed | - resource logs False | - | ++ VNet, private endpoints | + Azure RBAC | ? | ? |
| Alibaba DTS | + certs, modes ND | - per-task creds | ? | - 8 `dts_*` tables, superuser | ? | ? ETL | + Express Connect, VPN, gateway | ? RAM | ? | ? |
| Tencent DTS | + SSL option | + CAM password-free | ? | - `__tencentdb__` persists, REPLICA IDENTITY FULL forced | ? | - | + DC, VPN, CCN, allowlist | + CAM policies | ? | ? |
| GoldenGate | ++ TLS1.3, mTLS, OAuth | ++ credential store, Kerberos, OKV | ++ per-file AES keys under KMS master, spill too | + heartbeat/checkpoint tables; `OGG_*` roles | ++ audit on by default | + mapping/transform | ++ target-initiated paths, reverse proxy | ++ 4 roles | ? | ++ FIPS 140-2 L1 |
| Qlik Replicate | ++ 6 modes, client cert, CRL | ++ AES-256 master key, vault plugin | ++ customer-data parts of logs encrypted | - `attrep_*` trigger/tables, superuser for CDC | ? | + expression builder | - | ++ 4 roles via AD | ? | ? |
| Fivetran | ++ TLS required, pin-and-revoke | ++ 4 secret managers at sync time | ++ CMK for creds + temp data | + REPLICATION + SELECT, pub+slot | ++ AUDIT_TRAIL | ++ SHA-256 with per-destination salt | ++ SSH, reverse SSH, PrivateLink, proxy agent | ++ custom roles, SSO/SCIM/2FA | + security@ | - |
| Striim | - no SSL props documented | ++ 6 vault types, AES-256 props | ? | + REPLICATION, slot, DDL table | ? | ? | ? | ? | ? | ? |
| Debezium | ++ 6 modes, sslcert/sslkey | ++ ConfigProvider file/env allowlist | ? | + publication `all_tables` default; non-superuser | - | ++ hash with salt, mask, truncate | - | - | - no SECURITY.md | - |
| Airbyte | ++ 6 modes; Cloud forbids disable | + plaintext by default, managers optional | + purged; Cloud AES-256 | + REPLICATION, slot, publication | + audit logs (Pro+) | + hash unsalted, RSA | ++ SSH, allowlist, PrivateLink | ++ RBAC + SSO | - | - |
| Estuary | ++ verify-full, internal mTLS | + auto-encrypted, OIDC | + own bucket, enc ND | ++ read-only mode, no helper table | - | ++ per-task salt at capture | ++ SSH, PrivateLink, BYOC | + prefix grants, SSO | - | - |
| **migkit** | **+** all libpq modes + CRL, MySQL CA/identity/mTLS, Kafka SASL_SSL; **default `prefer`, `ssl: true` = CERT_NONE**; no Mongo/Redis/MSSQL/Cassandra TLS options | **++** 6 secret backends + IAM tokens; children via env/0600 files/stdin; redacted `commands.log`; scrubbed diagnostics; mongosync telemetry off | **++/+** age to recipients for every value file; Fernet restore points; 0700 local copy removed; **wrapped dumps plain** | **++** source read-only; only pub/slot/repl user; `migkit_origin` two-way only; **publication FOR ALL TABLES; no grant script/preflight** | **++** SHA-256 hash-chained changelog, verified by `history`; **no signed checkpoint/anchor** | **+** salted SHA-256, display only; **not HMAC, not at capture, no transform hook** | **++** ssh tunnels + jump + command tunnels + multi-leg + relay; UI 127.0.0.1; **no reverse tunnel, TOFU host keys** | **++** SSH-signed N-of-M approvals with expiry and nonce; operator/viewer; token+CSRF+host allowlist; **token in URL query** | **++** SECURITY.md, SBOM, SHA256SUMS, Sigstore provenance + SBOM attestation, wrapped-program matrix; **no Dependabot/pip-audit/PyPI attestations/lock** | **-** md5 without `usedforsecurity=False` x7; age/ed25519 non-FIPS |

**migkit against the field, per factor**

| Factor | Verdict | Who leads, and on what |
|---|---|---|
| 1 TLS | **Behind** on the default; equal to the best on PostgreSQL options; behind on six engines | MOLT (secure by default, `--allow-tls-mode-disable`), Fivetran (required + pinned), Airbyte Cloud (no `disable`); Qlik/Voyager/mongodump for CRL on every engine |
| 2 Secrets | **Ahead** of every OSS tool; equal to Fivetran/Striim | Nothing in the 31 combines six reference backends, IAM tokens, child-program isolation and log redaction. MySQL Shell has the prompt/stdin migkit lacks |
| 3 At rest | **Ahead** of every OSS tool; **behind** GoldenGate and the CMK services | GoldenGate: per-file data key wrapped by a KMS master, covering spill, cache and checkpoints; Fivetran/DMS/Google: customer-managed keys. migkit's gap is the wrapped programs' dump directory |
| 4 Footprint | **Ahead** on footprint (no helper table on the source, none on the target outside two-way); **behind** on the operator-facing grant script and preflight | Estuary (read-only mode), Voyager (grant script), GoldenGate (`OGG_*` roles), DM (precheck). Debezium and migkit share the `FOR ALL TABLES` default |
| 5 Audit | **Ahead** of all 31 | None of the 31 documents tamper evidence; migkit's hash chain is the only one. Fivetran and GoldenGate lead on breadth (every dashboard/API action), not integrity |
| 6 Masking | **Behind** | Estuary (per-task salt, applied at capture before any disk write), Fivetran (per-destination salt), Debezium (salted hash), mydumper/MOLT/elasticdump (transform hooks) |
| 7 Network | **Ahead** of every OSS tool; **behind** Fivetran, Google DMS, GoldenGate, Estuary | Reverse SSH / target-initiated paths / PrivateLink / outbound-only proxy agent; Fivetran's pin-and-revoke of SSH host keys |
| 8 RBAC/approvals | **Ahead** on approvals (cryptographic N-of-M is unique among the 31); **behind** Fivetran/Airbyte/Qlik on identity | Fivetran (custom roles, SSO, SCIM, 2FA), Qlik (AD groups). OpenSearch MA and gh-ost have gates without signatures |
| 9 Supply chain | **Equal** to the best OSS (gh-ost, mongo-tools); **behind** MOLT on dependency automation | gh-ost (attestations), mongo-tools (GPG + CycloneDX), MOLT (Dependabot). PyPI Trusted Publishing with PEP 740 would put migkit ahead of all |
| 10 FIPS | **Behind** GoldenGate, clickhouse-backup, DMS; equal (undocumented) to the other 28 | GoldenGate (OpenSSL FIPS provider, 140-2 L1), clickhouse-backup (`-fips` builds) |

### A3. Gap table: mechanism, library, effort

| # | Gap (migkit today) | Leader's mechanism | Fix for migkit | Effort |
|---|---|---|---|---|
| S1 | TLS default is `prefer` / unverified (`config.py:80-82,109-118`; `postgres.py:111-115`) | MOLT: secure by default, loud named opt-out | Default `sslmode=verify-full` (`PGSSLMODE`) and MySQL `ssl_verify_cert/identity=True` when the host is not loopback; a system CA bundle via `certifi`/`ssl.create_default_context()`; an explicit `tls: insecure` per endpoint that `doctor` prints in red every run and `assess` fails on for a production hop. Keep `prefer` only for `127.0.0.1`/unix sockets | S |
| S2 | No TLS parameters for MongoDB, Redis, SQL Server, Cassandra; ClickHouse `secure` only; MySQL no CRL | Voyager/mongodump: CA, cert, key, CRL per side | pymongo `tls`, `tlsCAFile`, `tlsCertificateKeyFile`, `tlsCRLFile` from the same option names; redis-py `ssl`, `ssl_ca_certs`, `ssl_certfile`, `ssl_keyfile`, `ssl_cert_reqs`; pymssql `tds_version`+`encrypt` (or `pyodbc` `Encrypt=yes;TrustServerCertificate=no`); cassandra-driver `ssl_context`; clickhouse-connect `verify`, `ca_cert`, `client_cert`; MySQL CRL via an `ssl.SSLContext` with `load_verify_locations(cafile=crl)` and `VERIFY_CRL_CHECK_LEAF`. One `tls:` block resolved per engine in `config.py` so `doctor` can report all of them | M |
| S3 | Wrapped programs' dump directories plain (`evidence.py:16-18`) | GoldenGate: every file under a wrapped data key; mydumper's `--exec-per-thread` recipe | mydumper: `--exec-per-thread "age -r <recipient> -o %s.age"` (mydumper already supports the hook), myloader reverses with `--exec-per-thread "age -d -i $MIGKIT_IDENTITY"`; PostgreSQL: switch the dump path from `-Fd` to `-Fc` streamed through `pyrage`/`cryptography` Cobblestone (chunked AES-GCM, `docs/backlog.md:4290-4293`) into a single sealed file and `pg_restore` from a decrypting pipe; mongodump `--archive` to stdout through the same seal. Directory stays 0700 either way | M |
| S4 | Recipients are people's keys only; no key service | GoldenGate: master key in OKV/OCI KMS/KMS plugin; Fivetran CMK | Envelope: a random age identity per hop wrapped by `aws-kms:`/`gcp-kms:`/`azure-kv:` (boto3 `kms.encrypt`, `google-cloud-kms`, `azure-keyvault-keys`) and stored in the report dir; rotation re-wraps one header | M |
| S5 | Publication `FOR ALL TABLES` (`postgres.py:7521-7523`) | Estuary/Fivetran/Airbyte: a publication for the tables in scope | `create publication ... for table <hop scope>` from the hop's `tables`/`exclude`; `for all tables` only where the hop names everything and the account is superuser; `doctor` names the publication and its table count | S |
| S6 | No least-privilege grant script, no preflight | Voyager `yb-voyager-pg-grant-migration-permissions.sql`, DM precheck, GoldenGate roles | `migkit grants <hop>` prints per-engine SQL for source (SELECT + REPLICATION or `rds_replication`; MySQL REPLICATION SLAVE/CLIENT + SELECT + SHOW VIEW) and target (owner of the hop's schemas, `SET ON PARAMETER session_replication_role`); `doctor` checks with `has_table_privilege`, `pg_has_role`, `SHOW GRANTS`, MongoDB `usersInfo`, and fails the plan naming the missing grant | S |
| S7 | Hash chain without a signature or anchor (`audit.py`) | None of the 31 has tamper evidence; the backlog's own R5 design | Every N entries or on `history`, sign the last hash with the operator's SSH key (`ssh-keygen -Y sign -n migkit-audit`, same machinery as `approvals.py:62-75`) into `changelog.sig`; optional anchor: PUT the checkpoint to an S3 Object Lock bucket (boto3 `put_object` with `ObjectLockMode=COMPLIANCE`) or append to a Rekor-style transparency log via `sigstore` (`sigstore-python`) | S |
| S8 | Salted SHA-256 display mask; nothing at capture; no hook (`masking.py:85-91`) | Estuary: per-task salt applied before any disk write; Fivetran: joinable salted hash; mydumper: function per column | `hmac.new(key, value, hashlib.sha256)` with a 32-byte `secrets.token_bytes` key sealed by `evidence.py` (equal values still equal, dictionary attack closed); a `mask:` block on the hop applied in the copier and the tail before the row is rendered (the column list already exists, `masking.py:43-56`); a per-column Python callable hook loaded from the hop (`transform: {col: module:function}`) with the mydumper vocabulary (`random_string`, `constant`, `apply`) built in | M |
| S9 | No reverse tunnel, TOFU host keys with no record | Fivetran: reverse SSH; fingerprint approved once, stored, revocable | `tunnel: {ssh: ..., reverse: true}` builds `ssh -N -R` from the bastion back to migkit for a source that must dial out; `host_keys: pinned` writes the accepted fingerprint into the hop's `known_hosts` under the report dir and `doctor` shows it; default `host_keys: yes` when `known_hosts` is given | S |
| S10 | Dashboard token in the URL query (`ui.py:548`) | clickhouse-backup: basic auth on a loopback API | Print the token separately and take it from a POST `/login` form (or the URL fragment, which never leaves the browser); set `SameSite=Strict` on the cookie | S |
| S11 | No Dependabot, no `pip-audit`, no lock, actions by tag | MOLT: Dependabot; gh-ost: attestations | `.github/dependabot.yml` (pip + github-actions, weekly); `pip-audit --strict` job in `ci.yml`; `uv lock`/`pip-compile` a `requirements.lock` used by the release build; pin actions to commit SHAs | S |
| S12 | GitHub-only release; no PyPI attestations | PyPI Trusted Publishing (PEP 740) | `pypa/gh-action-pypi-publish@release/v1` with `attestations: true` under an `id-token: write` job after `build`; the SBOM and SHA256SUMS stay on the GitHub release | S |
| S13 | md5 without `usedforsecurity=False` (7 sites), sha1 (2 sites) | The OSS report's practice 6 | `hashlib.md5(data, usedforsecurity=False)` at `canon.py:1070`, `tally.py:28`, `users.py:31`, `engines/postgres.py:1154`, `engines/sqlite.py:847`, `engines/mongodb.py:1301`, `engines/kafka.py:1323`; same for the two sha1 sites. The in-server digests must stay md5 to match `md5()` on the server, so this is the flag, not a hash change | S |
| S14 | No FIPS mode | GoldenGate: OpenSSL FIPS provider | Document: run under a FIPS OpenSSL (`OPENSSL_CONF` fips provider or RHEL/UBI FIPS image); `at_rest.cipher: aes-gcm` selecting Cobblestone (`cryptography`, FIPS-approved AES-GCM) instead of age; approvals accept `ecdsa-sha2-nistp256` signers. A CI job on a FIPS image running the unit suite | M |
| S15 | Plaintext `password:` accepted; no prompt/stdin | MySQL Shell `--passwords-from-stdin`; mongodump prompt | `assess` warns on a literal password in `hops.yaml`; `password: prompt` and `password: stdin` as two more reference forms in `_secret` (`getpass.getpass`, `sys.stdin.readline`) | S |
| S16 | `docs/threat-model.md:89-93` stale about at-rest | - | Rewrite the "Not covered yet" section around `evidence.py` and the S3 gap | S |

---

## Part B. Throughput

### B1. What migkit has measured (the baseline every claim below is held to)

| What | Number | Where |
|---|---|---|
| Bulk, PostgreSQL 16, two shared cores | 172k rows/s at 1M rows, 173k at 10M (~48 MB/s), `pg_dump -Fd -j2 \| pg_restore -j2` path; migkit RSS 30 MB | `docs/scale.md:32-40,86-89` |
| pgcopydb vs the dump path | 555 MB plain: 4.1 s vs 11.7 s (2.9x); 2,093 MB LOB: 18.2 s vs 93.6 s (5.2x); the dump path burns 83 of 94 s gzipping random bytes | `docs/scale.md:45-64` |
| Chooser after the fix | same 555 MB move end to end through `migkit move --go`: 11.7 s -> 5.4 s | `docs/scale.md:193-198` |
| Verify | 5.84 s per million rows + 3.96 s fixed; drilldown of a planted diff at 10M rows: 128 s, 814 MB | `docs/scale.md:98-110` |
| Change tail, same engine | 1,030 rows/s (MySQL) and 1,150 rows/s (PostgreSQL) for 20 s, ending 0.13 s / 0.72 s behind; the writer was the limit | `docs/scale.md:165-171` |
| Applier, MySQL -> PostgreSQL, 320k queued | 21.8 s -> 7.7 s (read-ahead + growing batches) -> 5.5 s (no-op mapping skipped) -> 4.9 s (lanes); at 10 ms: 59.6 s one lane, 17.1 s four, 10.0 s / 6.0 s as runs | `docs/backlog.md:3597-3604`; `migkit/engines/hetero.py:3196-3205` |
| COPY staging vs multi-row | 5,000 upserts 24 ms vs 51 ms; 20,000 91 vs 166; MySQL staging measured slower and not adopted (36 vs 55 ms) | `migkit/engines/postgres.py:1143-1148`; `docs/backlog.md:3604-3612` |
| Keys off where every parent is in scope, 10 ms away | 6,000 parent/child rows: 154 s held together -> 1.4 s | `docs/backlog.md:3612-3620` |
| Decoder vs applier | binlog reader 300k changes in 2.58 s (116k/s); whole tail 65k/s: the applier is the limit | `docs/backlog.md:3620-3624` |
| Index window | PostgreSQL 300k rows, 3 secondary indexes: 1.241 s -> 0.652 s (1.9x); MySQL 200k: 1.150 s -> 0.814 s (1.41x) | `migkit/movers.py:822-826,1556-1560` |
| LOAD DATA LOCAL, pinned | 36% faster on MySQL, taken | `docs/backlog.md:4518-4522` |
| Ranges in processes | 1M rows MySQL -> PostgreSQL: 10.6 s in one process, 8.1 in two, 7.0 in four; threads gave nothing; 8 ranges 7.7 s vs 16 ranges 11.2 s when a process was started per range | `migkit/engines/hetero.py:1207-1215`; `migkit/ranges.py:97-101` |
| Adaptive workers (R1) | controller settled at the best fixed number within noise (8.7-11.5 s either way), nothing set | `docs/backlog.md:3543-3552` |
| Read-back vs digest per range | 400k rows: 1.9 s / 2.4 s local, 2.5 / 3.2 at 10 ms, 4.4 / 4.2 at 20 MB/s per connection; the copier times both once and keeps the cheaper | `migkit/engines/postgres.py:7270-7290` |
| Relay beside the source | 200k rows over 5 MB/s + 10 ms: 7.1 s as COPY text, 4.3 s read beside the source, zstd inside ssh | `migkit/engines/postgres.py:7140-7147` |
| Multi-leg ssh | 50 ms path, 400k rows: 28.3 s one leg, 27.2 s four; the copier's own round trips are the limit, not the window | `docs/backlog.md:4331-4339` |
| Harness | `bench/run.py`: disposable pair (postgres:16 / mysql:8.4), six shapes (keyed, keyless, wide, lob, skewed, partitioned), times every mover path + the direct programs + `--cdc-rate` lag; writes hardware and versions with the numbers | `bench/run.py:1-40,274-330` |

### B2. Technique scorecard

Verdict: **has** (used, measured), **partial**, **inherited** (only when a wrapped program does it), **no**. "Expected" is an estimate from the published figure scaled to what migkit measured; it is claimed only after B3's measurement.

| # | Technique | migkit today (file:line) | Leader and published gain | Expected for migkit where missing | Verdict |
|---|---|---|---|---|---|
| 1 | Binary COPY on both ends | No. The copier renders COPY **text** (`migkit/engines/postgres.py:72-78 _copy_text`, `855-870`, staging `1133`); pgcopydb path is text too (pgcopydb default). R19.2 lists binary as to-do (`docs/backlog.md:4498-4501`) | PeerDB (binary COPY PG->PG, part of its 150 MB/s); DuckDB scanner (`FORMAT BINARY`); docs: "somewhat faster"; asyncpg's binary protocol 5x vs psycopg3 on fetch | 10-30% less host CPU per row on numeric/timestamp-heavy tables; the real win is a server-to-server pipe of raw binary bytes with no Python parse (the per-row cost is Python's, `hetero.py:1210`). Read-back verification stays text (`_copy_out_tally`) | no |
| 2 | COPY FREEZE | No on migkit's copier and dump path; **inherited** when pgcopydb copies an unsplit table (TRUNCATE + COPY FREEZE in one transaction, pgcopydb concurrency doc) | pgcopydb; Cybertec: 500M rows 627 s -> 304 s (with `wal_level=minimal`); first read after plain COPY 360 s vs 110 s, VACUUM 364 s avoided | 1.3-2x on the load of a table copied whole, plus the first-read/VACUUM cost removed; not applicable to a table split into ranges (each range is its own transaction), so only below `LEAST`/split thresholds or per partition | partial (inherited) |
| 3 | Unlogged staging, then SET LOGGED | No (`postgres.py:6071` only inventories unlogged tables as not-carried). Applier staging is a temporary table, which is unlogged by nature (`postgres.py:855-857,1150-1156`) | Crunchy: COPY 2.6 s logged vs 0.6 s unlogged, but SET LOGGED 16.6 s (rewrites and WALs the table) | Net negative for a table that must end logged; nothing to gain beyond the temp-table staging already there | equal by design |
| 4 | Deferred index/constraint build, parallel workers | Has: `_IndexWindow` drops secondary indexes and rebuilds N at once on N sessions (`movers.py:818-835,783-800 _rebuild`); MySQL half `_MyIndexWindow` (`1553-1570`); threshold 100k rows (`base.py:3093,3116-3125`); runbook `pg_restore --section=post-data -j` (`postgres.py:5589-5612`). Not on the pgcopydb `copy table-data` path (indexes stay, `movers.py:2737-2745`); no `maintenance_work_mem`/`max_parallel_maintenance_workers` set per rebuild session (only advised, `postgres.py:3376`) | pgcopydb `--index-jobs` + PK `USING INDEX`; PlanetScale 2-3x; mydumper up to 40%; Cybertec parallel B-tree 17m12s -> 6m48s (2.5x) with workers + 4 GB `maintenance_work_mem`; MySQL's own benchmark: deferring is **slower** on MySQL | On the rebuild phase: up to 2.5x by `SET maintenance_work_mem` and `max_parallel_maintenance_workers` per rebuild session; on the pgcopydb path, `clone`-style index deferral is the larger win migkit currently forgoes | has (PG), partial (pgcopydb path, session settings) |
| 5 | LOAD DATA LOCAL with a restricted file handler | Has: `_put_rows` uses `load data local infile` from 500 rows (`mysql.py:5220-5250`), the connection answers only the one name it prepared (`6240-6270`, `feed`), refuses warnings (`_LoadRefused`) and falls back to inserts; server `local_infile` probed (`5272-5280`) | MySQL Shell (same guard; "20x vs INSERT" per MySQL docs) | Measured 36% and taken | has |
| 6 | Parallel chunking by PK quantile / ctid | Has: quantile edges by `row_number()` (`ranges.py:331-340`), rows-per-range from workers and `LEAST=50_000` (`322-328`), one process per slot kept across ranges (`97-118`), ctid page spans on PG >= 14 for keyless tables (`postgres.py:714-745`), cross-engine ranges in processes (`hetero.py:1207-1235`), workers sized and paced (`sizing.py:95-340`) | PeerDB CTID + shared snapshot 16x vs Airbyte, 8 threads 150 MB/s; Artie CTID "10-20x"; DMS ranges 2-4x; pgcopydb split-tables-larger-than | Already there; the adaptive pace is ahead of every static knob in the field | has, ahead |
| 7 | zstd on the wire via a relay | Has, PostgreSQL source only: `\copy` on the bastion piped through `zstd -3 -T0` inside ssh, `zstandard` stream reader here (`postgres.py:7140-7178`); measured 7.1 s -> 4.3 s at 5 MB/s. Needs an ssh tunnel with `psql` + `zstd` present (`7158`). Not for MySQL/Mongo readers, not on the write leg | HVR "10x or higher" compression; GoldenGate "at least 4:1"; MySQL Shell zstd 317 MB/s vs gzip 162 | On a bandwidth-bound link, wall time drops by the compression ratio (3-5x on ordinary rows) wherever the reader is beside the source; MySQL needs `mysql --batch` or `mydumper -o -` + zstd on the bastion | partial |
| 8 | Multi-leg TCP | Has: `legs_for(rtt, workers)` and a round-robin splitter (`tunnel.py:70-140,288-315`); measured no gain for the copier (28.3 vs 27.2 s at 50 ms) because its round trips dominate; pays for one long stream (a dump) | GoldenGate multiple DISTPATHS; Qlik several streams; ES.net BDP: 32 -> 64 MB buffer nearly 2x at 75 ms | Nothing more for the copier; route the dump paths (`pg_dump`, mydumper) through the legs, where one stream is window-bound at ~20 MB/s per 100 ms | has |
| 9 | Pipelining / prepared multi-row | Partial: `execute_values` below 1,000 rows and COPY staging from 1,000 (`postgres.py:1140,1143-1148`); MySQL `executemany` multi-row (`mysql.py:5249`) and LOAD from 500; lanes 4x at 10 ms. **No psycopg3 pipeline mode** (`psycopg` is installed, `pyproject.toml`, but `.pipeline()` appears nowhere) | psycopg3: 100 statements at 300 ms 30 s -> 0.3 s; Dalibo 2x on localhost; Debezium JDBC batching 79x; Columnar ADBC 455 s -> 7 s | For the runs too small for staging (mixed tables, few rows each) at 10+ ms: statements per batch collapse to one round trip, i.e. a 500-statement batch at 10 ms from ~5 s to ~0.1 s. Same on MySQL through the pinned LOAD + `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` (R19.10) | partial |
| 10 | Session-level binlog / redo skipping | Partial: `foreign_key_checks=0` (`mysql.py:612,4401,4487,5372`; `movers.py:1945`), `session_replication_role=replica` (`postgres.py:437,749,844`; `movers.py:991`); myloader's binlog-off default is **reversed** when the target has replicas (`movers.py:2136-2143`, correctness over speed). No `unique_checks=0`, no `sql_log_bin=0` on migkit's own apply sessions, no `synchronous_commit=off`, no `ALTER INSTANCE DISABLE INNODB REDO_LOG`, no `wal_level=minimal` | MySQL Shell `skipBinlog` + redo off (load > 200 MB/s); MySQL docs: `unique_checks`/`foreign_key_checks` "save a lot of disk I/O"; PG docs: `synchronous_commit=off` "significant boost for small transactions" | `sql_log_bin=0` per session on a target with no replicas and no PITR need: 10-30% of the load; `synchronous_commit=off` on the tail's apply sessions where the disk fsync is the limit: 1.5-3x on small batches (loss window 3 x `wal_writer_delay`, acceptable because batches are exact and replayed, R3); redo-log off is instance-wide and unrecoverable on crash: refuse unless the target is empty and disposable, as migkit's decision layer can tell | partial |
| 11 | Sorted ingest (PK order) | Has for MySQL: reads are `order by <key>` and resume by key (`mysql.py:228-240`), so rows land in clustered-index order; ranges ascend by key (`ranges.py`) | MySQL docs (primary-key order); Lightning sorted-kv | Already there where it matters (InnoDB); PostgreSQL heaps do not reward it | has |
| 12 | Arrow columnar transfer | No for database pairs: `pyarrow` only in `engines/parquet.py` and `engines/duckdb.py`; rows cross as Python tuples through canon rendering | ConnectorX 13x vs pandas on PG; dlt + Arrow 30x vs SQLAlchemy JSON; ADBC 5-10x; dlt + ConnectorX 10m51s vs Sling 14m16s on 9.7 GB | On the cross-engine copier, whose ceiling is Python per row (`hetero.py:1207-1215`): 2-5x on the read+render side by `connectorx.read_sql(partition_on=key, partition_num=workers, return_type="arrow")` and an Arrow-native writer (ADBC `adbc_ingest`, or Arrow -> COPY text via `pyarrow.csv`). Verification is unaffected: digests are computed in-server, not from the batch | no |
| 13 | Server-side cursors | Has: named cursor with `itersize` (`postgres.py:780-784`), `SSCursor.fetchmany` (`mysql.py:251-254,5359-5371,5426-5430`), generic `fetchmany` (`dbapi.py:136`), reads capped by rows and bytes (`hetero.py:1400-1410`) | Everyone (PgJDBC `setFetchSize`) | - | has |
| 14 | CTID-parallel snapshot with one exported snapshot | Partial: `pg_export_snapshot()` is used by the consistent **verify** pass so its lanes read one instant (`postgres.py:6478-6490,6558-6570`); the **copier's** ranges/spans each run in their own transaction (`postgres.py:7180-7250`) and cross-range consistency comes from the change tail's interleaving invariants (`tests/test_a_copy_and_its_changes_interleave_safely.py`); the pgcopydb path inherits pgcopydb's exported snapshot | pgcopydb, PeerDB (all threads `SET TRANSACTION SNAPSHOT`), DuckDB scanner | No speed gain; a correctness gain for `--mode full` without a tail against a live source (a table's ranges consistent with each other). Cost: a long REPEATABLE READ transaction holds vacuum back on the source; bounded by the existing `snapshot_limit` (`postgres.py:6568-6570`) | partial |
| 15 | Physical copy + fast-forwarded slot | No: no `pg_basebackup`, `CLONE INSTANCE`, XtraBackup or storage-snapshot rung (only an advisory mention, `advisors.py:124`); slot creation exists (`pgslot.py`), `gtid_executed` is read (`mysql.py:5877`). R19.1 is the design (`docs/backlog.md:4482-4498`) | TiDB Lightning physical 100-500 GiB/h vs logical 10-50 (5-10x); XtraBackup fastest overall (Percona); MySQL Shell dump 3 GB/s is still logical | 3-10x on the bulk of a same-major-version pair with `REPLICATION`/`BACKUP_ADMIN`; indexes come with the pages, no rebuild; block checksums replace row verification for the bulk. Refused on managed sources without filesystem access unless a snapshot copy is offered | no |
| 16 | DBLog watermark: skip changes to uncopied ranges | No in migkit's own tail (R19.8, `docs/backlog.md:4535-4544`); the copier already records `ranges_done`/`spans_done` (`ranges.py:243-258,299-308`) the tail could consult. **Inherited** on the Debezium path: blocking snapshot / GTID watermarks (`movers.py:3366-3380`) | Netflix DBLog; Debezium incremental snapshot; Vitess PR #7708 filters catch-up rows outside the copied PK range (no number published) | Catch-up apply work during the copy falls by the share of changes that hit uncopied ranges: ~50% on a uniformly written source, more when hot keys are late in key order. Correct because a range is copied as read and every change after that read is applied | no |
| 17 | Batch apply + lanes | Has: batches 1,000 -> 16,000 while behind (`hetero.py:2915-2917`), read-ahead (`3196-3215`), `_net_rows` collapse to one write per key (`base.py:3652-3660`), lanes by key/FK/unique groups from 2,000 changes (`base.py:3606-3650`), COPY staging (`postgres.py:1143-1160`), keys off where every parent is in scope, lane failure falls back to one ordered transaction (`base.py:3587-3604`) | DMS batch apply ~30x; GoldenGate Coordinated Replicat ~5x classic; Alibaba hot-row merge 1,200 -> class cap; Debezium JDBC batch 79x | Measured 21.8 s -> 4.9 s locally, 59.6 -> 6.0 s at 10 ms. Left: SQL Server staging (R2.4), MySQL staging measured and rejected | has, equal/ahead |
| 18 | Compiled decoder sidecar | No; measured not yet the limit (116k/s decode vs 65k/s apply). Design R2.6/R19.9: go-mysql / pglogrepl handing Arrow batches | Debezium (Java), PeerDB (Go, pglogrepl), Artie (Go) | Only once apply passes ~116k changes/s; then 2-5x on decode. Defer | no (deferred by measurement) |
| 19 | Writer beside the target | No (R17d "still to: the writer beside the target"); reader beside the source exists for PG (`postgres.py:7140`) | GoldenGate/HVR/Qlik: agents on both sides; DMS pays the RTT from one instance (Artie: 29.7 s vs 2,029 s latency vs DMS) | At 30-60 ms to the target, the statement path's per-batch RTT is paid once: the 10 ms case measured 6.0 s would fall to the local 4.9 s and hold at 100 ms. Exactness unchanged: the batch's mark and number travel with it | no |
| 20 | Verify as it lands | Has: each range read back or digested on both servers right after its COPY while pages are hot; the cheaper way chosen by timing (`postgres.py:7255-7290`); digests in-server, client memory flat (`docs/scale.md:90-97`) | Nobody: Veridata, DVT, `pgcopydb compare` verify cold, after | - | has, ahead |
| 21 | Rows not copied (digest-driven re-sync) | Has: spans and ranges checkpointed with their tally; a restart copies only what is not done, and a keyless table whose target count disagrees restarts loudly (`ranges.py:260-298,299-308`) | Nobody among the bulk tools re-copies only differing ranges; all re-do the load | - | has, ahead |
| 22 | Parallelism sized by the tool | Has: `sizing.estimate` from cgroup CPUs, memory, each server's free connections/CPUs/sessions, RTT; `Pace` hill-climbs on committed bytes/s with BBR-style probes and a 30% cut on strain (`sizing.py:1-48,95-340`) | MongoDB 7 throughput probing, GoldenGate min/max apply parallelism; everyone else static | - | has, ahead |

### B3. How to measure each, in local docker

Common harness: `bench/run.py --engine postgres|mysql --rows N [--cdc-rate R --cdc-seconds S]` starts a disposable pair on free ports, seeds the six shapes inside the server, and times each mover path, `migkit check`, the tail's lag, and the direct programs as baseline (`bench/run.py:1-40,274-330`). For links with latency or a bandwidth cap, put a `ghcr.io/shopify/toxiproxy` container between migkit and one server, as `tests/test_a_link_cut_mid_move_is_survived.py` and `tests/test_a_source_that_fails_over_mid_move.py` already do (`latency` toxic for RTT, `bandwidth` toxic for MB/s). Record `/usr/bin/time -v` user CPU beside wall, and the container's `docker stats` network bytes, as `docs/scale.md` does. Run each pair three times; keep the median.

1. **Binary COPY**: pair postgres:16; shapes `keyed` and `wide`, 2M rows. Time (a) today's copier, (b) `COPY (SELECT ...) TO STDOUT (FORMAT binary)` piped straight into `COPY ... FROM STDIN (FORMAT binary)` on the target with no Python in the middle, (c) the same with psycopg3 `copy.write_row` binary. Record wall and user CPU; the numeric and timestamptz columns are where text parsing costs.
2. **COPY FREEZE**: same pair, `keyed` 5M rows into an empty table. Time `BEGIN; TRUNCATE; COPY ... FREEZE; COMMIT` vs `COPY`; then time the first `SELECT count(*)` and a `VACUUM` on each. Report load, first read, vacuum, and `pg_stat_wal` bytes.
3. **Unlogged**: same pair; `ALTER TABLE SET UNLOGGED; COPY; ALTER TABLE SET LOGGED` total vs plain COPY; expect the negative result the Crunchy post shows, which closes the item.
4. **Deferred/parallel index build**: `wide` shape with five secondary indexes, 2M rows. Time the `_IndexWindow` rebuild with workers 1, 2, 4 and with `SET maintenance_work_mem='1GB'; SET max_parallel_maintenance_workers=4` on each rebuild session; then the pgcopydb path with and without the window. MySQL: the same shape through `_MyIndexWindow` to confirm the 1.41x holds at 2M rows and does not invert as MySQL's own benchmark warns.
5. **LOAD DATA LOCAL**: mysql:8.4 pair, `keyed` 1M rows; time the copier with `_PLAIN.on` (inserts) vs pinned load (`mysql.py:5238-5240`); confirm `select @@global.local_infile` on and that a server request for any other filename is refused (the `feed` name check).
6. **Chunking**: postgres pair, `skewed` shape (nine rows in ten under one tenant) 5M rows; time equal-key-span vs quantile ranges (`bounds_sql`) with workers 4; then a keyless table through ctid spans vs one scan. Report per-range durations to show balance.
7. **zstd relay**: postgres source behind an `ssh` container (`lscr.io/linuxserver/openssh-server` with `postgresql-client` and `zstd` installed) with toxiproxy `bandwidth` 5 MB/s and `latency` 10 ms between migkit and the ssh box; `keyed` 1M rows; time plain tunnel vs relay; record bytes through the toxic.
8. **Multi-leg**: same ssh box, toxiproxy `latency` 50-100 ms; time `pg_dump -Fc | pg_restore` through one leg vs `legs_for` legs; confirm the copier shows no change (as measured) and the dump does.
9. **Pipelining**: postgres pair, toxiproxy `latency` 10 ms on the target; `--cdc-rate 500` against 20 small tables so every batch is mixed runs under 1,000 rows; time apply with `execute_values` vs psycopg3 `conn.pipeline()` sending the same statements; record lag at writer stop.
10. **Session logging skips**: mysql pair, `keyed` 1M rows, target with `log_bin=ON` and no replica; time the load with and without `SET sql_log_bin=0` on the loader session, reading `Binlog_cache_use` and bytes of binlog written. Postgres: tail at `--cdc-rate 1000` with and without `SET synchronous_commit=off` on the apply sessions; `pg_stat_wal.wal_sync` and lag. Redo-log off: measure once on a disposable target, then leave it out of the decision layer unless the target is empty.
11. **Sorted ingest**: mysql pair; load `keyed` in key order vs `ORDER BY random()`; time and `Innodb_buffer_pool_pages_flushed`.
12. **Arrow**: postgres -> mysql through the hetero copier, `keyed` 1M rows; prototype `connectorx.read_sql(..., partition_on="id", partition_num=4, return_type="arrow")` feeding the pinned LOAD via `pyarrow.csv.write_csv`; compare wall, user CPU, RSS; run `migkit check` after to hold zero diff.
13. **Server-side cursors**: already in place; confirm RSS flat at 10M rows (`docs/scale.md:90-97`).
14. **Shared snapshot on the copier**: postgres pair, `--mode full` without a tail while `bench/run.py --cdc-rate 200` writes; compare a cross-range consistency probe (a `rules:` sum over the table on both sides at the end) with and without `SET TRANSACTION SNAPSHOT` in every range; time the difference and `pg_stat_activity` xmin age on the source.
15. **Physical copy**: postgres:16 pair, 5 GB `keyed`: `pg_basebackup -h src -D - -Ft -Xs --compress=server-zstd:3 | tar -x` into the target's data dir, then a slot made **before** the backup and the tail started at the checkpoint LSN from `pg_controldata`; compare wall with the pgcopydb path and run `migkit check` for zero diff. MySQL 8.4: `INSTALL PLUGIN clone; CLONE INSTANCE FROM ...` (needs `BACKUP_ADMIN`/`CLONE_ADMIN`) vs mydumper; the tail from `gtid_executed`.
16. **Watermark skipping**: postgres pair, `--cdc-rate 500` during a 5M-row full copy with the tail on; count changes applied vs changes whose key fell in a range not yet copied (from `ranges_done`); compare catch-up time and end lag with the skip on and off; `migkit check` after.
17. **Batch apply + lanes**: already measured; re-run `--cdc-rate` at 2,000 and 5,000 with toxiproxy 10 ms to fix the current ceiling before 9 and 19 are built.
18. **Decoder sidecar**: ramp `--cdc-rate` until lag grows; `py-spy top` on the tail to see whether reader or applier holds the CPU; build only if the reader does.
19. **Writer beside the target**: toxiproxy 50 ms between migkit and the target; run the apply from a second migkit container on the target's docker network (the batch shipped as one sealed file) vs from the host; compare lag at writer stop.
20-22. Already measured in `docs/scale.md` and `postgres.py:7270-7290`; re-run at 10M rows when 1, 2 and 15 land so the verify-as-it-lands cost is stated beside the faster bulk.

### B4. The five rates the earlier report could not find (fresh searches, 2026-09-28)

**Oracle GoldenGate.** The OCI whitepaper *Performance Considerations for OCI GoldenGate* (March 2025, v1.1) is the only official measurement: a single Replicat over one trail of 73,634,600 OLTP records (3.3 GB) ran at **~5 to ~40 MB/s** depending on configuration, on a link offering ~100 MB/s; a public-internet path was within 3% of a dedicated 1 Gbps interconnect; Coordinated Replicat with 20 threads and `GROUPTRANSOPS 1000` was **nearly 5x** Classic Replicat and used up to 8 OCPUs. Derived: 18-144 GB/h per Replicat. Oracle's product page claims Parallel Replicat "up to 1 million+ operations per second" with no test described. The tuning blog's levers: `MAP_PARALLELISM`, foreign keys off during bulk, and low RTT to the target ("thousands of SQL statements per second" make small round-trip delays cascade). The PDF itself returned 403 to the fetch tool; the figures are from the search index's extract of it and should be re-read from the PDF before being quoted further.

**Qlik Replicate.** No vendor benchmark. Community and support figures: a 600M-row Oracle table full-loaded in **about 15 hours (~11k rows/s)** after tuning; **17,000 rows/s** baseline on another Oracle source falling to 1,000 with a `source_lookup`; **~2,000 rec/s** on a 91.5M-row wide HANA table; a pathological MS-CDC case at 1 row per fetch. Levers named: commit rate during full load (default 10,000, tests at 30k/100k/300k), `bulkArraySize` (+20%), max tables at once (default 5), parallel load by segmentation column, native drivers over ODBC. Qlik's console reports rec/s and kbyte/s per task, which is how to measure it.

**Vitess VReplication.** No project benchmark. Kir Shatrov's write-up: **65 GB (5M rows of ~13 KB) copied in 90 minutes, ~12 MB/s** for one stream with `vstream_packet_size` 500,000 (double the default), tables within a stream copied serially; the binlog side scaled to 400 streams keeping up with **330 MB/s** of binlogs. Issue #8056 showed smaller `LIMIT` batches without new gRPC calls keep throughput stable and beat gh-ost by 2 minutes on the same table. Since then: `--vreplication-parallel-insert-workers` (default 1; inserts in parallel, commits in order) and deferred secondary keys ("2-3x" per PlanetScale, using InnoDB parallel index build on 8.0.31+). Measure with `VReplicationCopyRowCount`.

**mongosync.** Still no published rate. MongoDB's own guidance is the mechanism: two round trips to the destination per source operation, majority reads and writes, one mongosync per shard on separate hosts, 90 chunks per destination shard, fewer concurrent writes below 64 GB / 4 vCPU, indexes built up front by default (set `buildIndexes: never` and build after when indexes are uncorrelated with `_id`), and `/progress` `estimatedCopiedBytes` to compute the rate yourself. Field reports: PhysicsWallah moved a cluster Atlas reported as 2.2 TB (about 5 TB uncompressed) with mongosync and the migration verifier, no rate given (page 403 to the fetch tool); a 500 GB two-datacentre sync on 1.7.0 kept `lagTimeSeconds` growing to 16,493 in the change-event phase. The docker measurement for migkit is unchanged: `mongo:7` pair, time `migkit move` against `mongosync` from `estimatedCopiedBytes`.

**`pg_dump -j` / `pg_restore -j`.** Measured cases now on record: a 50 GB (2 GB compressed) database on a 4-core VM restored in **30m04s plain, 25m40s -j2, 22m06s -j4, 16m50s -j8, 16m07s -j12** (1.87x; `-Fc` dump 6m28s vs 7m10s plain+gzip); a production copy that went from 4 h dump + 5 h restore to about 2 h + 2.5 h with `-j4` (~1.8x); Kevin Grittner's 40 runs with `-j2` clustering at 77-84 min, "not even twice as fast"; a 25 GB database on 24 cores where `--jobs` 8/12/20 gained about a minute (I/O-bound, one big table). Against single-table parallel COPY loaders: Citus's script moved a 1.4 TB table in **7h45m vs more than a day** (>3x, 8 threads, COPY per range); PeerDB's 1.5 TB in 7 h vs 1.5 days (5x). migkit's own numbers sit beside these: `pg_dump -Fd -j2 | pg_restore -j2` 11.7 s vs pgcopydb 4.1 s on 555 MB and 93.6 s vs 18.2 s on 2 GB of LOBs (`docs/scale.md:45-60`). The lesson matches pgcopydb's: `-j` parallelises across tables and indexes, never within a table, and the gzip in `-Fd` is host CPU spent on incompressible bytes.

### B5. Order of work the two scorecards give

Security first, because every item is S or M and none changes a measured number: S1 (verify-full default) and S13 (`usedforsecurity=False`) the same day; S5, S6, S7, S9, S10, S11, S12, S15, S16 in the same week; S2, S3, S8, S4, S14 after.

Throughput, by expected gain over effort, each claimed only after its B3 measurement: 9 (pipeline mode, M), 10's `sql_log_bin`/`synchronous_commit` session settings (S), 4's rebuild session settings (S), 16 (watermark skipping, M), 14 (shared snapshot on the copier, S, correctness), 1 (binary pipe, M), 7 on MySQL (M), 15 (physical rung, L, the largest), 19 (writer beside the target, L), 12 (Arrow, L), 18 only when measurement says the reader is the limit.

---

## Sources

Repository (working tree, 2026-09-28): `migkit/config.py`, `migkit/evidence.py`, `migkit/audit.py`, `migkit/approvals.py`, `migkit/tunnel.py`, `migkit/ui.py`, `migkit/movers.py`, `migkit/users.py`, `migkit/wording.py`, `migkit/masking.py`, `migkit/diagnostics.py`, `migkit/state.py`, `migkit/ranges.py`, `migkit/sizing.py`, `migkit/twoway.py`, `migkit/engines/{base,postgres,mysql,hetero,kafka,mongodb,redis,mssql,clickhouse,opensearch,cassandra,duckdb,parquet,dbapi}.py`, `tools/check_no_secrets.py`, `pyproject.toml`, `SECURITY.md`, `.github/workflows/{ci,release,wrapped-programs}.yml`, `docs/threat-model.md`, `docs/backlog.md` (R1, R2, R5, R14, R17, R19), `docs/scale.md`, `docs/what-a-migration-actually-costs.md`, `bench/run.py`, `bench/seed.py`.

Earlier reports (tool facts and their URLs): `docs/research/security-oss-tools-2026-09-27.md`, `docs/research/security-managed-tools-2026-09-27.md`, `docs/research/throughput-published-2026-09-27.md`, `docs/research/throughput-cdc-techniques-2026-09-27.md`.

Fresh, this pass:
- Oracle, *Performance Considerations for OCI GoldenGate* (Mar 2025): https://www.oracle.com/a/ocom/docs/oci-goldengate-performance-tuning.pdf (403 to fetch; figures via search extract)
- Oracle, GoldenGate networking guidelines: https://blogs.oracle.com/dataintegration/oracle-goldengate-networking-guidelines-mtu-bandwidth-benchmarking-bdp-tuning-and-sdu-configuration
- Oracle, Parallel Replicat performance tuning: https://blogs.oracle.com/dataintegration/oracle-goldengate-parallel-replicat-performance-tuning
- Oracle, tuning the performance of GoldenGate (19.1): https://docs.oracle.com/en/middleware/goldengate/core/19.1/admin/tuning-performance-oracle-goldengate.html
- DBASolved, performance with GoldenGate: https://www.dbasolved.com/2023/01/performance-with-oracle-goldengate/
- Qlik support, 600M-row table full load: https://community.qlik.com/t5/Official-Support-Articles/Qlik-Replicate-and-how-to-Replicate-very-large-table-having/ta-p/1891265
- Qlik community, performance tuning thread: https://community.qlik.com/t5/Qlik-Replicate-Discussions/What-are-performance-tuning-for-replicating-data-Any-detailed/td-p/1694581
- Qlik community, slow full load: https://community.qlik.com/t5/Qlik-Replicate/Slow-full-load/td-p/2023496
- Qlik community, spiking throughput: https://community.qlik.com/t5/Qlik-Replicate/Spiking-Throughput-during-full-load/td-p/2070417
- Qlik community, lookup and filter cost: https://community.qlik.com/t5/Qlik-Replicate/Qlik-Replicate-Lookup-and-filter-sourcetables/td-p/2137306
- Qlik help, full load tuning: https://help.qlik.com/en-US/replicate/May2022/Content/Global_Common/Content/SharedEMReplicate/Customize%20Tasks/tasks_fullLoadTunetstab.htm
- Qlik help, monitoring full-load throughput: https://help.qlik.com/en-US/replicate/May2022/Content/Global_Common/Content/SharedEMReplicate/Monitor%20and%20Control%20Tasks/monitor_full_load_throughput.htm
- Kir Shatrov, scaling VReplication: https://kirshatrov.com/posts/scaling-vreplication
- Vitess issue #8056, analysing VReplication behaviour: https://github.com/vitessio/vitess/issues/8056
- Vitess PR #7708, filter catch-up rows outside copied PK range: https://github.com/vitessio/vitess/pull/7708
- Vitess RFC #4604, VReplication SplitClone: https://github.com/vitessio/vitess/issues/4604
- Vitess docs, life of a stream: https://vitess.io/docs/25.0/reference/vreplication/internal/life-of-a-stream/
- Vitess docs, vttablet flags: https://vitess.io/docs/24.0/reference/vreplication/flags/
- Vitess docs, metrics: https://vitess.io/docs/25.0/reference/vreplication/metrics/
- Vitess docs, MoveTables: https://vitess.io/docs/archive/14.0/reference/vreplication/movetables/
- MongoDB, mongosync behaviour: https://www.mongodb.com/docs/mongosync/current/reference/mongosync-behavior/
- MongoDB, mongosync FAQ: https://www.mongodb.com/docs/mongosync/current/faq/
- MongoDB, sync sharded clusters: https://www.mongodb.com/docs/cluster-to-cluster-sync/current/multiple-mongosyncs/
- MongoDB community, lagTimeSeconds on a 500 GB sync: https://www.mongodb.com/community/forums/t/mongosync-keeps-lagtimeseconds-big/251609
- MongoDB community, mongosync stuck during collection copy: https://www.mongodb.com/community/forums/t/mongosync-stuck-during-collection-copy/293398
- PhysicsWallah Engineering, migrating TBs of live data: https://medium.com/physicswallah-engineering/lessons-from-migrating-tbs-of-live-data-without-breaking-production-or-losing-sleep-ca1bb8bf36b5 (403 to fetch)
- mckerlie, speeding up Postgres restores: (mckerlie.com, "speeding up Postgres restores")
- Shreehari Vaasistha, parallel export/import: https://medium.com/@shreehari9481/boost-postgresql-export-and-import-with-parallelism-30fb20e665dd
- pgsql-admin, multiple streams do not improve times: https://www.postgresql.org/message-id/em5e30e63a-661c-44ad-8996-601c945efc8b%40oyster-creek
- Kevin Grittner, parallel pg_restore scheduling review: https://www.postgresql.org/message-id/4A71922E02000025000290FD%40gw.wicourts.gov
- Citus, faster data migrations in Postgres: https://www.citusdata.com/blog/2021/02/20/faster-data-migrations-in-postgres/
- PeerDB, pg_dump/pg_restore 5x: https://blog.peerdb.io/how-can-we-make-pgdump-and-pgrestore-5-times-faster
- PostgreSQL docs, pg_restore: https://www.postgresql.org/docs/current/app-pgrestore.html
- Sixteen Pillars, why pg_restore is slow: https://sixteenpillars.com/why-did-my-pg-restore-take-all-night-parallelism-and-deferred-indexes/
- iSeatz, speeding up Postgres data dumps: https://www.iseatz.com/blog/speeding-up-postgres-data-dumps
