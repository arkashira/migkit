# Security of the managed and commercial migration services, factor by factor (research as of 2026-09-27)

The facts below come from 60+ official doc pages. Where I couldn't reach a page, I say so rather than fill the gap.

**Caveats:**
- The session's WebSearch budget ran out partway through, so the rest was done with WebFetch on known documentation URLs.
- Some vendor docs couldn't be fetched: pages load via JavaScript, return 404, or are too large for the fetch tool. These are marked **NV** (not verified in this pass).
- **ND** means not documented on the pages read.
- The fetch tool summarizes pages. Quoted object names and SQL were checked directly; everything else is from those summaries.

---

### AWS DMS
1. **TLS:** modes are `none`, `require`, `verify-ca` and `verify-full`, and the default is `none`. Support varies by engine: MySQL has no `require`, Oracle has only `verify-ca`, SQL Server has no `verify-ca`. `require` trusts self-signed certificates. You import CA certificates as PEM or Oracle wallet (.sso). There is no TLS 1.3 for MySQL endpoints. Client certificates/mTLS and CRL/OCSP: ND. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Security.SSL.html
2. **Secrets:**
   - Credentials are either plaintext `UserName`/`Password` on the endpoint, or `SecretsManagerSecretId` plus `SecretsManagerAccessRoleArn`. Rotation uses a Secrets Manager Lambda, and the task may need a restart to pick up the new password. RDS-managed master secrets can't be used. https://docs.aws.amazon.com/dms/latest/userguide/security_iam_secretsmanager.html
   - RDS IAM database authentication works from DMS 3.6.1 (`AuthenticationMethod: iam`). There is no CDC for PostgreSQL with IAM auth. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Endpoints.Creating.IAMRDS.html
3. **At rest:**
   - A symmetric KMS key (`aws/dms` or your own) encrypts replication-instance storage and endpoint connection info. The key can't be changed after creation. S3 and Redshift targets take their own KMS keys. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Security.html#CHAP_Security.EncryptionKey
   - "Time Travel" logs go to S3, encrypted with your keys. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Monitoring.html
4. **Source writes (PostgreSQL):** https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html
   - A logical replication slot (test_decoding or pglogical), recreated on restart.
   - For DDL capture: table `awsdms_ddl_audit`, function `awsdms_intercept_ddl()`, and an event trigger on `ddl_command_end`. You can skip these with `CaptureDdls=false`.
   - The docs tell you to run `grant all on public.awsdms_ddl_audit to public`.
   - WAL heartbeat objects go in `HeartbeatSchema`, default `public`.
   - Privileges: superuser for full load + CDC on self-managed databases; `rds_superuser` + `rds_replication` on RDS.
5. **Audit:** CloudTrail records all DMS API calls. Context logging writes SQL "without data". Task logs are deleted after 10 days. Tamper evidence: ND on the DMS pages (it's a CloudTrail-side feature). https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Monitoring.html#logging-using-cloudtrail
6. **Masking:** added in DMS 3.5.4 as `data-masking-digits-mask`, `data-masking-digits-randomize` and `data-masking-hash-mask`. The hash is SHA-256 with no salt or key parameter documented. `DataMaskingErrorPolicy` controls failures. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Masking.html
7. **Network:**
   - The replication instance always runs in a VPC. Options include VPC peering, VPN/Direct Connect and NAT.
   - The DMS API is reachable over PrivateLink (`com.amazonaws.<region>.dms`).
   - Since 3.4.7, VPC endpoints are required for S3, Kinesis, Secrets Manager and similar services from private subnets (`secretsManagerEndpointOverride`).
   - SSH tunnel: ND.
   - https://docs.aws.amazon.com/dms/latest/userguide/CHAP_VPC_Endpoints.html , https://docs.aws.amazon.com/dms/latest/userguide/infrastructure-security.html
8. **RBAC:** IAM, plus fine-grained access by resource name and tag. Two-person approval: ND. https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Security.FineGrainedAccess.html
9. **Supply chain:** managed service; ND.
10. **FIPS:** `dms-fips` API endpoints exist in us-east-1/2, us-west-1/2 and Canada. They cover API calls only, not database connections. DMS is in FedRAMP scope. https://docs.aws.amazon.com/general/latest/gr/dms.html , https://docs.aws.amazon.com/dms/latest/userguide/dms-compliance.html

### Google Database Migration Service + Datastream
1. **TLS:**
   - DMS `--ssl-type` is `NONE`, `REQUIRED`, `SERVER_ONLY` or `SERVER_CLIENT` (mTLS with client cert and key). Connections to the destination are always encrypted. https://docs.cloud.google.com/sdk/gcloud/reference/database-migration/connection-profiles/create/postgresql
   - Datastream offers None, Server-only and Server-client for MySQL, PostgreSQL, Oracle and SQL Server. https://docs.cloud.google.com/datastream/docs/create-connection-profiles
   - CRL/OCSP: ND.
2. **Secrets:**
   - Datastream supports Secret Manager (`secretManagerStoredPassword`). The secret can be in another project, and the service account needs `roles/secretmanager.secretAccessor`. https://docs.cloud.google.com/datastream/docs/use-secret-manager
   - DMS and Secret Manager: ND on its security page. https://docs.cloud.google.com/database-migration/docs/security-and-encryption
3. **At rest:**
   - DMS supports CMEK on migration jobs. Disabling the key sets the job to FAILED; job metadata isn't covered. https://cloud.google.com/database-migration/docs/oracle-to-postgresql/cmek-for-migration-jobs
   - Datastream CMEK covers source rows including backfill, but not metadata. https://docs.cloud.google.com/datastream/docs/use-cmek
4. **Source writes:**
   - DMS PostgreSQL: the `pglogical` extension in every database, one slot per database, and grants including `GRANT USAGE on SCHEMA pglogical to PUBLIC`, plus `REPLICATION` or `rds_replication`. https://docs.cloud.google.com/database-migration/docs/postgres/configure-source-database
   - Datastream: you create a publication and a `pgoutput` slot, with `ALTER ROLE … WITH REPLICATION` and SELECT/USAGE grants. No helper tables. https://docs.cloud.google.com/datastream/docs/configure-self-managed-psql
5. **Audit:**
   - DMS logs to Cloud Audit Logs as `datamigration.googleapis.com`; some methods aren't logged. https://cloud.google.com/database-migration/docs/audit-logging
   - Datastream logs Admin Activity and Data Access. https://docs.cloud.google.com/datastream/docs/audit-logging
   - Tamper evidence: ND.
6. **Masking:** ND.
7. **Network:**
   - DMS: IP allowlist, reverse SSH tunnel through a VM, VPC peering, Private Service Connect interfaces. https://docs.cloud.google.com/database-migration/docs/mysql/configure-connectivity
   - Datastream: IP allowlist, forward SSH (password or key), and PSC interfaces or peering. The docs warn against `ACCEPT_AUTOMATIC`, and unused private-connectivity configs are deleted after 6 months. https://docs.cloud.google.com/datastream/docs/psc-interfaces
8. **RBAC:** IAM, with permission types ADMIN_READ/WRITE and DATA_READ/WRITE (see the DMS audit page). Approvals: ND.
9. **Supply chain:** managed; ND.
10. **FIPS:** NV (Google's FIPS page came back truncated).

### Azure Database Migration Service
Scope: Azure DMS now covers SQL Server only. PostgreSQL migrations go through the "migration service in Azure Database for PostgreSQL", which runs **pgcopydb** underneath. https://learn.microsoft.com/en-us/azure/dms/dms-overview

Most of the facts below come from the security baseline: https://learn.microsoft.com/en-us/security/benchmark/azure/baselines/azure-database-migration-service-security-baseline (it notes it's based on benchmark v1.0 and may be outdated).
1. **TLS:** TLS 1.2+ from the source by default, but you can turn it off if the source doesn't support it. DMS to target is always encrypted. mTLS/CRL: ND.
2. **Secrets:** Key Vault integration **False**; managed identities for the data plane **False**; Entra ID data-plane auth True; service principals True.
3. **At rest:** "Stores customer content at rest: False". The Log Replay Service path stages backups in your own Blob storage.
4. **Source writes:**
   - SQL Server Managed Instance link builds a distributed availability group and grants just-in-time permissions that are removed afterwards. https://learn.microsoft.com/en-us/sql/sql-server/azure-arc/migrate-to-azure-sql-managed-instance
   - pgcopydb creates a slot and a sentinel table on the source and a replication origin on the target, and has a `stream cleanup` command. https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_stream.html
5. **Audit:** Azure resource logs **False** per the baseline; Activity Log only.
6. **Masking:** DLP False; masking ND.
7. **Network:** VNet integration True (default); NSG support; "Disable public network access" False; private endpoints supported (overview page).
8. **RBAC:** Azure RBAC True; Customer Lockbox False; Conditional Access False.
9. **Supply chain:** ND.
10. **FIPS:** ND.

### Alibaba Cloud DTS
1. **TLS:** PostgreSQL has optional SSL with CA cert, client cert and private key. MySQL offers "Non-encrypted" or "SSL-encrypted". Verify modes and CRL: ND. https://www.alibabacloud.com/help/en/dts/user-guide/migrate-data-from-a-self-managed-postgresql-database-to-an-apsaradb-rds-for-postgresql-instance
2. **Secrets:** credentials are entered per task. The docs say to "delete these accounts after the migration is complete". KMS/vault: ND. https://www.alibabacloud.com/help/en/dts/user-guide/migrate-data-from-a-self-managed-mysql-database-to-an-apsaradb-rds-for-mysql-instance
3. **At rest:** ND.
4. **Source writes (PostgreSQL):**
   - Tables `public.dts_pg_class`, `dts_pg_attribute`, `dts_pg_type`, `dts_pg_enum`, `dts_postgres_heartbeat`, `dts_ddl_command`, `dts_args_session` and `aliyun_dts_instance`.
   - A replication slot prefixed `dts_sync_`.
   - **Superuser is mandatory** for incremental migration.
   - MySQL needs only REPLICATION SLAVE/CLIENT, SHOW VIEW and SELECT, and no source objects are mentioned.
5. **Audit:** NV (ActionTrail pages wouldn't load).
6. **Masking:** NV. An ETL feature exists (https://www.alibabacloud.com/help/en/dts/user-guide/etl-task-management/).
7. **Network:** whitelist DTS CIDR blocks (including `pg_hba.conf`), Express Connect, VPN, database gateway, CEN.
8. **RBAC:** RAM. The page exists (https://www.alibabacloud.com/help/en/dts/use-ram-for-access-control/) but its content wouldn't load: NV.
9. **Supply chain:** ND. 10. **FIPS:** ND.

### Tencent Cloud DTS
1. **TLS:** an SSL option for public network, Direct Connect and VPN access, which needs SSL enabled on the database first. Modes: ND. https://www.tencentcloud.com/document/product/571/42645
2. **Secrets:** account and password per task. "CAM authentication provides password-free access" for TencentDB.
3. **At rest:** ND.
4. **Source writes:**
   - MySQL: a `__tencentdb__` system database that **persists after migration** (about 0.01–0.1% of storage). Grants include RELOAD, LOCK TABLES, REPLICATION CLIENT/SLAVE, SHOW DATABASES, SHOW VIEW, PROCESS, `ALL PRIVILEGES ON __tencentdb__.*` and `SELECT ON *.*`. https://www.tencentcloud.com/document/product/571/42645
   - PostgreSQL: a `__tencentdb__`-prefixed schema. **It runs `ALTER TABLE … REPLICA IDENTITY FULL` on source tables** and creates a publication and slot, which are deleted afterwards. https://www.tencentcloud.com/document/product/571/78744
5. **Audit:** NV.
6. **Masking:** ND.
7. **Network:** public network, CVM, Direct Connect, VPN, CCN, and a DTS IP allowlist (https://www.tencentcloud.com/document/product/571/60054).
8. **RBAC:** CAM preset policies `QcloudDTSFullAccess` and `QcloudDTSReadOnlyAccess`. Sub-users have no DTS access by default. https://www.tencentcloud.com/document/product/571/61582
9. **Supply chain:** ND. 10. **FIPS:** ND.

### Oracle GoldenGate (Microservices, 26ai docs)
All page names below live under https://docs.oracle.com/en/database/goldengate/core/26/coredoc/
1. **TLS:** TLS 1.2 and 1.3 (1.3 recommended). mTLS over secure WebSocket for distribution paths, including target-initiated ones. Client-certificate and OAuth (external IdP) authentication. mTLS doesn't work behind the reverse proxy. CRL/OCSP: ND. `secure-data-transit.html`
2. **Secrets:** a credential store holding `USERIDALIAS` entries. Kerberos is supported. Entra ID database auth is listed in the table of contents (`db-conn-with-ms-entraid.html`). OCI Vault: NV. `secure-authn-database.html`
3. **At rest:** `secure-data-rest.html`
   - Trail files are encrypted with AES-128/192/256.
   - Each trail file gets its own data key, wrapped by a master key (ANSI X9.102). The wrapped key sits in the trail header.
   - The master key lives in a local wallet, Oracle Key Vault, OCI KMS ("master key never leaves"), or a third-party KMS via a plugin service (Linux only).
   - Spilled/staged data, cache files and Bounded Recovery data files are also encrypted.
4. **Source writes:**
   - `ADD HEARTBEATTABLE` creates `GG_HEARTBEAT`, `GG_HEARTBEAT_SEED`, `GG_HEARTBEAT_HISTORY` and update/purge jobs (on PostgreSQL, `gg_hb_job_run()`). Replicat needs a checkpoint table. `configure-ogg-adding-extract-and-replicat.html`
   - Privileges: least-privilege roles `OGG_CAPTURE`, `OGG_APPLY` and `OGG_APPLY_PROCREP` in 26ai; `DBMS_GOLDENGATE_AUTH.GRANT_ADMIN_PRIVILEGE` on 21c and earlier. `prepare-database-user-and-privileges-oracle.html`
5. **Audit:** "Auditing is enabled by default". It records REST API calls and system events, configured via `ogg-audit.xml`, with syslog to `/var/log/secure` and `restapi.log`. Tamper evidence: ND. `secure-accountability.html`
6. **Masking:** column mapping and transformation only (`administer-mapping-and-manipulating-data.html`). Dedicated masking: ND.
7. **Network:** target-initiated (receiver-pull) distribution paths and an nginx reverse proxy (both in the table of contents).
8. **RBAC:** User, Operator, Administrator and Security roles. No approvals. `secure-az.html`
9. **Supply chain:** ND.
10. **FIPS:** "FIPS-140-2 Level 1 compliant" using the OpenSSL FIPS Provider. `secure-miscellaneous.html`

### Qlik Replicate
Page names below live under https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/
1. **TLS:**
   - PostgreSQL `sslmode` supports all six modes (disable through verify-full), plus client cert, key, CA and a **CRL path**. `PostgreSQL/set_up_postgresql_db_as_source.htm`
   - TLS 1.2+ on the server listener (port 3552); HTTPS console and HSTS. `Security/tls_support.htm`
2. **Secrets:**
   - Stored secrets use AES-256 under the master key `mk.dat`, with a salt and a nonce. You change it with `repctl setmasterkey`, and the key can be scoped to file, user or machine. `Security/Protect_Passwords.htm`
   - An external-credentials plugin (C addon, `get_secret`, referenced as `lookup::key`, max 4 KB) fetches secrets from a vault. `Security/external_credentials.htm`
3. **At rest:** at Trace/Verbose log levels, **the parts of log files that may contain customer data are encrypted** (key in `log.key`; decrypt with `repctl dumplog`). The user-permissions file can also be encrypted. Staging-file encryption: ND. `Security/log_encryption.htm`
4. **Source writes (PostgreSQL):** event trigger `attrep_intercept_ddl`, function `public.attrep_intercept_ddl()`, table `public.attrep_ddl_audit`, and heartbeat table `<schema>.attrep_wal_heartbeat`. There's a documented removal procedure. SELECT is enough for full load; CDC needs **superuser**, with a documented non-superuser option. `PostgreSQL/remove_artifacts_from_postgresql_source_db.htm`
5. **Audit:** NV.
6. **Masking:** the expression builder exists, but hash functions are NV.
7. **Network:** ND.
8. **RBAC:** Admin, Designer, Operator, Viewer, mapped to AD groups (`AttunityReplicateAdmins`, etc.). Approvals: ND. https://help.qlik.com/en-US/replicate/May2025/Content/Global_Common/Content/SharedEMReplicate/Server%20Settings/user_permissions.htm
9. **Supply chain:** ND.
10. **FIPS:** the docs refer to a separate FIPS section, which I couldn't read: NV.

### Fivetran (HVR / Local Data Processing: NV, docs not reachable)
1. **TLS:**
   - Database connections are SSL by default; the PostgreSQL guide says TLS is "required" for direct connections.
   - Self-signed and private-CA certificates are shown for you to approve on first connect. SSH host-key fingerprints are approved the same way. Both are stored and can be revoked. CRL: ND.
   - https://fivetran.com/docs/getting-started/fivetran-dashboard/account-settings/validated-certificates-keys
2. **Secrets:** external secret managers (AWS Secrets Manager, Azure Key Vault, Google Secret Manager, HashiCorp Vault) are read "at sync time" (Business Critical plan). https://fivetran.com/docs/core-concepts/features/external-secret-managers
3. **At rest:** customer-managed keys (AWS, Azure, GCP) protect the master key for "credentials and temporary data". Disabling the key stops syncs. https://fivetran.com/docs/getting-started/fivetran-dashboard/account-settings/cmk
4. **Source writes (PostgreSQL):** `REPLICATION` or `rds_replication` plus SELECT; publication `fivetran_pub`; slot `fivetran_pgoutput_slot` (must use `pgoutput`). https://fivetran.com/docs/connectors/databases/postgresql/setup-guide
5. **Audit:** the Platform Connector's `AUDIT_TRAIL` table records dashboard user actions, API calls and role changes. https://fivetran.com/docs/logs/fivetran-platform
6. **Masking:** column blocking, and SHA-256 column hashing with a **unique salt per destination** (values stay joinable). https://fivetran.com/docs/using-fivetran/features/data-blocking-column-hashing
7. **Network:** direct with IP safelist, SSH, reverse SSH, AWS PrivateLink / Azure Private Link / GCP PSC, VPN, an outbound-only proxy agent, and Hybrid Deployment. https://fivetran.com/docs/connectors/databases/connection-options
8. **RBAC:** account, destination and connection roles; custom roles on Enterprise; SAML SSO, SCIM, 2FA for admins; no approval workflow. https://fivetran.com/docs/getting-started/fivetran-dashboard/account-settings/role-based-access-control
9. **Supply chain:** vulnerability reports go to security@fivetran.com. Signing/SBOM: ND. https://fivetran.com/docs/security
10. **FIPS:** ND.

### Striim
1. **TLS:** the PostgreSQL Reader reference lists no SSL properties: ND. https://www.striim.com/docs/en/postgresql-programmer-s-reference.html
2. **Secrets:** https://www.striim.com/docs/en/using-vaults.html , https://www.striim.com/docs/en/using-source-and-target-adapters-in-applications.html#encrypted-passwords
   - Vaults: Striim's own (AES-256), AWS Secrets Manager, Azure Key Vault, Google Secret Manager, HashiCorp Vault KV v2 and CyberArk, referenced as `[[vault.key]]`.
   - `ALTER VAULT` requires restarting the app.
   - Password properties are stored AES-256 encrypted (`passwordEncryptor.sh`).
3. **At rest:** NV.
4. **Source writes (PostgreSQL):** a role with `REPLICATION` or `rds_replication`, slot `striim_slot` using `wal2json`, and `pg_ddl_setup.sql`, which creates a DDL tracking table in the source. https://www.striim.com/docs/en/configuring-postgresql-to-use-postgresql-reader.html
5–10. NV. Masking functions, roles, audit and FIPS pages returned 404 or 403.

### Debezium / Kafka Connect
1. **TLS:** `database.sslmode` supports disable, allow, prefer, require, verify-ca and verify-full. Also `database.sslcert`, `sslkey`, `sslpassword`, `sslrootcert` and `sslfactory`, so client-certificate mTLS is possible. CRL: ND. https://raw.githubusercontent.com/debezium/debezium/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java
2. **Secrets:**
   - Kafka's `ConfigProvider` mechanism (KIP-297, Kafka 2.0): `FileConfigProvider` with `${provider:path:key}` syntax. Configs are stored unresolved in the Connect config topic. https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations
   - `EnvVarConfigProvider` (`${env:VAR}`) with an `allowlist.pattern` setting. https://cwiki.apache.org/confluence/display/KAFKA/KIP-887%3A+Add+ConfigProvider+to+make+use+of+environment+variables
   - DirectoryConfigProvider and Vault providers: not fetched.
3. **At rest:** ND (depends on Kafka).
4. **Source writes:**
   - Default slot and publication are both named `debezium`.
   - **`publication.autocreate.mode` defaults to `all_tables`** (other values: filtered, disabled, no_tables).
   - Optional `heartbeat.action.query` and signal table.
   - The docs recommend a dedicated replication user rather than superuser. https://debezium.io/documentation/reference/stable/connectors/postgresql.html
5. **Audit:** ND.
6. **Masking:** `column.mask.hash.<alg>.with.salt.<salt>`, `column.mask.with.<N>.chars` and `column.truncate.to.<N>.chars` are listed in the connector doc's table of contents. Their exact definitions are NV (the page is too large for the fetch tool).
7–8. ND. 9. **Supply chain:** the GitHub repo has no SECURITY.md (https://github.com/debezium/debezium/security/policy). 10. ND.

### Airbyte
1. **TLS:** PostgreSQL `sslmode` supports all six modes; Cloud doesn't allow `disable`. https://docs.airbyte.com/integrations/sources/postgres
2. **Secrets:** **by default secrets are stored in the configured database "in plain-text without encryption"**. External options: AWS Secrets Manager (optional KMS), GCP Secret Manager, Azure Key Vault, HashiCorp Vault. Switching stores breaks existing connectors. https://docs.airbyte.com/platform/deploying-airbyte/integrations/secrets
3. **At rest:** data is "purged" after transfer; Cloud metadata is AES-256. https://docs.airbyte.com/platform/operating-airbyte/security
4. **Source writes:** `REPLICATION`, slot `airbyte_slot` (pgoutput), publication `airbyte_publication`.
5. **Audit:** Pro/Enterprise Flex audit logs of who changed workspaces, connections, connectors, users and permissions, kept for 365 days.
6. **Masking:** "Mappings": hashing (MD5, SHA-256, SHA-512 in the UI; MD2, SHA-1, SHA-384 via API), **no salt documented**; RSA encryption; field rename; row filter. Plus tier and above. https://docs.airbyte.com/platform/using-airbyte/mappings
7. **Network:** SSH tunnel (key or password), IP allowlist, PrivateLink.
8. **RBAC:** role-based access control on Cloud/Enterprise, plus SSO.
9. **Supply chain:** security@ disclosure email. Signing/SBOM: ND.
10. **FIPS:** ND (SOC 2 Type II, ISO 27001).

### Estuary Flow
1. **TLS:** `sslmode` can be overridden (e.g. `verify-full`). Internal traffic uses TLS/mTLS. https://docs.estuary.dev/security/security-features/
2. **Secrets:** connector secrets are "automatically encrypted" from the UI or `flowctl`; the mechanism is ND. Cloud IAM auth uses OIDC `AssumeRoleWithWebIdentity` (AWS, plus Azure and GCP guides). https://docs.estuary.dev/guides/iam-auth/aws/
3. **At rest:** collection data goes to your own bucket via storage mappings (recommended for production); encryption: ND. https://docs.estuary.dev/concepts/storage-mappings/
4. **Source writes:** https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
   - User `flow_capture` with `REPLICATION` and `pg_read_all_data`.
   - `public.flow_watermarks`, written during backfills.
   - Publication `flow_publication`, slot `flow_slot`.
   - A **read-only mode needs no watermarks table** and works on PG16+ standbys.
5. **Audit:** ND.
6. **Masking:** a `redact` schema annotation with `block` or `sha256`, using a **per-task salt** (auto-generated or set via `redactSalt`). It is applied at capture, before anything is written to disk or error messages. https://docs.estuary.dev/features/redaction/
7. **Network:** SSH `networkTunnel`, IP allowlist, PrivateLink, private deployments and BYOC. https://docs.estuary.dev/private-byoc/privatelink/
8. **RBAC:** grants on name prefixes with read, write or admin capability, plus SSO. https://docs.estuary.dev/reference/authentication/
9. **Supply chain:** ND.
10. **FIPS:** ND (SOC 2 Type II, HIPAA, GDPR, CCPA). https://docs.estuary.dev/security/compliance/

---

### Best-in-class features a small open-source CLI could copy
1. **Envelope encryption of spill/staging files (GoldenGate):** a separate AES-256 key per file, wrapped by a master key held in a pluggable key store (local keyfile, AWS KMS, Vault Transit), with the wrapped key in the file header. Rotating the master key only means re-wrapping headers. Cover spill and checkpoint files, not just output.
2. **Store secret references, never values:** Kafka's `${provider:path:key}` providers (file, directory, env with `allowlist.pattern`), Striim's `[[vault.key]]`, Qlik's `lookup::key`, Fivetran reading secrets at sync time. Keep configs unresolved on disk and never put secrets in argv.
3. **Salted/keyed hashing for masking, applied before any disk write:** Estuary's per-task salt at capture and Fivetran's per-destination salt. DMS (SHA-256) and Airbyte hash with no documented salt, which leaves low-entropy values open to dictionary attacks. Use HMAC-SHA256 with a per-job key.
4. **Log hygiene:** Qlik encrypts only the customer-data parts of verbose logs; DMS logs SQL "without data". Default to redacted logs.
5. **Strict TLS by default:** `verify-full` by default (DMS and Debezium's options show the range), client certificates (Debezium `sslcert`/`sslkey`, Google `SERVER_CLIENT`), a CRL path (Qlik), and approve-on-first-use pinning of certificates and SSH host keys with revocation (Fivetran).
6. **Small, reversible source footprint:**
   - Publish an artifact list plus a cleanup command (Qlik's removal page, pgcopydb `stream cleanup`).
   - Default to filtered publications, unlike Debezium's `all_tables`.
   - Offer a read-only mode with no helper table (Estuary).
   - Avoid DMS's `grant all … to public`, Tencent's leftover `__tencentdb__` database, and Tencent's forced `REPLICA IDENTITY FULL`.
7. **Least privilege without superuser:** as GoldenGate's `OGG_CAPTURE`/`OGG_APPLY` roles and Datastream, Fivetran, Airbyte and Estuary (REPLICATION + SELECT) show. DMS, Qlik and Alibaba require superuser for CDC.
8. **Short-lived credentials:** RDS IAM tokens (DMS 3.6.1+; note there's no PostgreSQL CDC with IAM auth there) and Estuary's OIDC role assumption.
9. **Tamper-evident local audit log:** GoldenGate audits by default and Fivetran has `AUDIT_TRAIL`, but none of the 12 documents tamper evidence of its own. A hash-chained, append-only operator log would stand out.
10. **FIPS mode through the OpenSSL FIPS provider (GoldenGate's approach):** document it as a runtime option for the CLI.
11. **Master key scoped to user or machine (Qlik):** tie the local key store to the OS keychain.
12. **Outbound-only connectivity:** reverse SSH, Fivetran's proxy agent, and GoldenGate's target-initiated paths avoid opening inbound ports on the source side.

**Coverage gaps:** Google FIPS, Alibaba RAM/ActionTrail, Qlik audit/FIPS/hash functions, Fivetran HVR/LDP, Striim items 3 and 5–10, and Debezium's exact masking definitions weren't verified. Raising `CLAUDE_CODE_MAX_WEB_SEARCHES_PER_SESSION` would let a follow-up pass fill them in.
