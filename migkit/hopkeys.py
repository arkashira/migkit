"""Every key a hop may say, in one place, and the configuration checked
against it before anything runs (backlog R21).

Measured on the code the day this was written (the scan in the test
below, run on it): 85 option keys - 50 of a hop's, 35 of a side's - read
by name in 40 files, each as a `.get("name")` string where it is used,
and three functions more reading them by a key held in a variable; and
nothing that knew the whole list - so `workrs: 2` was read by nothing
and said nothing, and a filter on a table the hop also excluded moved
nothing while the operator believed it filtered.
`tests/test_every_hop_key_is_declared.py` reads the code and fails on a
key it reads that this registry does not declare, so the list cannot
fall behind the code.

What the registry answers, and who asks:
* `check` - the configuration before anything runs (`config.load_hops`):
  unknown keys with the closest known one, wrong types, contradictions
  (a filter on an excluded table, a goal that writes nothing beside an
  option that writes), passwords in plain text. An error stops every
  command; a warning is said by `doctor`.
* `said_by_doctor` - the keys that do nothing on this hop, and the ones
  that set what migkit would otherwise decide (`workers` is a ceiling
  its sizing never passes; `big_rows` is read by nothing at all).
* `GOALS` - the one word a hop may give for what it wants, the options
  each word stands for, and what each does on the source and the target
  and guarantees; `MODES` and `CUTOVER` the same for the modes of `move`
  and `sync` and for `cutover:` on or off (`explain_goal`,
  `explain_mode`): `init`, `assess`, the dry run and the docs say them
  from here.
* `smallest` - a hop as the fewest lines that load into the same
  decisions (`decisions`), which `migkit init` writes.
"""
import difflib
from dataclasses import dataclass

#: the engine every endpoint key and option may be read by
ANY = ()


@dataclass(frozen=True)
class Key:
    """One key: what it is, the types it is written as, the engines that
    read it (`ANY`: every one), the keys under it where they are checked
    (None: a free mapping), and what it is to migkit's own decisions:
    `ceiling` (the most migkit may choose), `size` (a number migkit
    would otherwise use its own of), `nothing` (read by nothing) or `way`
    (turns off one of the ways migkit may choose to do something)."""
    what: str
    kind: str = "any"
    engines: tuple = ANY
    subs: tuple = None
    knob: str = ""
    #: the key's own name is a program's, and is not offered back as a
    #: "did you mean" (programs are not named in what migkit says)
    named_for_a_program: bool = False


STREAMS = ("kafka", "kinesis", "pubsub")

#: the keys of a hop itself
HOP_KEYS = {
    "goal": Key("what the hop is for - copy, cutover, verify, two-way,"
                " keep-in-sync, rehearse, upgrade, subset, consolidate,"
                " split or archive", "str"),
    "engine": Key("the engine both sides run, or hetero - read from the"
                  " addresses when they are addresses", "str"),
    "source": Key("where the data is: an address or the long form",
                  "str|dict"),
    "target": Key("where it goes: an address or the long form",
                  "str|dict"),
    "databases": Key("the source databases to move, all of them when left"
                     " out", "list"),
    "exclude": Key("databases, schemas or tables left alone - dotted"
                   " names and patterns, or databases each with all, its"
                   " tables or its schemas", "list|dict"),
    "include": Key("the only databases, schemas or tables taken - moved,"
                   " followed, checked and repaired - the rest left alone"
                   " as if excluded; exclude wins where both name one."
                   " Written as exclude is", "list|dict"),
    "service": Key("the playbook `migkit advise` shows", "str"),
    "big_rows": Key("read by nothing", "int", knob="nothing"),
    "slice": Key("the rows one compared range of a large table holds",
                 "int", knob="size"),
    "workers": Key("the most workers migkit's own sizing may reach",
                   "int", knob="ceiling"),
    "options": Key("the hop's options", "dict"),
    "db_map": Key("source database name -> target database name", "dict"),
    "mapping": Key("renames, column rules, row filters, the changes"
                   " carried per table (ops: insert, update, delete), and"
                   " the percentage a table's aggregates may drift by when"
                   " read a second way", "dict",
                   subs=("tables", "columns", "where", "tolerance",
                         "ops")),
}

#: the keys under a hop's `options`
HOP_OPTIONS = {
    "accept_changes": Key("table.column patterns whose values may arrive"
                          " as the target keeps them", "list",
                          ("hetero",)),
    "access": Key("who sees and does what on this hop in the web view and"
                  " the API - viewer, operator, approver, admin - added to"
                  " the file's own `access`", "dict",
                  subs=("viewer", "operator", "approver", "admin")),
    "app_roles": Key("the application's roles protect_target holds",
                     "list"),
    "approvals": Key("the steps that wait for an approval", "dict",
                     subs=("steps", "signers", "count", "expires")),
    "at_rest": Key("who may read the files holding values", "dict",
                   subs=("recipients",)),
    "atlas": Key("the structural schema reading, on or off", "bool",
                 ("postgres", "mysql"), knob="way",
                 named_for_a_program=True),
    "archive": Key("goal archive: remove - the archived rows removed from"
                   " the source once proved (with the source's answer,"
                   " source_changes: {archived_rows: yes}), restore - the"
                   " removed rows put back from the archive, batch - rows"
                   " a batch", "dict", subs=("remove", "restore", "batch")),
    "charset": Key("the character set, and the collation, the target's"
                   " text is converted to - a name, or {to, collation,"
                   " repair: double-encoded | none}; converted by MySQL's"
                   " own conversion after the copy, held to elsewhere",
                   "str|dict", subs=("to", "collation", "repair")),
    "consolidate": Key("goal consolidate: column - the column each"
                       " source's rows are held under, value or values -"
                       " this source's, keys - refuse, tenant, offset or"
                       " remap, offsets - each value's under offset, with"
                       " - the other hops writing the same target",
                       "dict", subs=("column", "value", "values", "keys",
                                     "offsets", "with")),
    "split": Key("goal split: with - the other parts of the same source,"
                 " shared - tables every part takes whole, rest - refuse"
                 " or stays (the rows no part takes stay on the source)",
                 "dict", subs=("with", "shared", "rest")),
    "table_engine": Key("the storage engine the target's tables are made"
                        " in - a name, or {to, accept: [group numbering,"
                        " remote rows], foreign_keys: {child.column:"
                        " parent.column}}", "str|dict",
                        ("mysql", "hetero"),
                        subs=("to", "accept", "foreign_keys")),
    "cdc_confidence_seconds": Key("seconds the follow of a change log"
                                  " stays behind the source's clock",
                                  "number", ("cassandra",)),
    "checksum": Key("text or jsonb row rendering", "str", ("postgres",)),
    "checksum_workers": Key("tables checksummed at once", "int",
                            ("postgres",), knob="size"),
    "cloud": Key("the cloud the servers run in - aws, gcp or azure -"
                 " where their addresses and the servers do not say; the"
                 " machine and the target's class are named in it", "str"),
    "code_dir": Key("where the operator's own versions of routines are"
                    " kept, one <name>.sql each", "str", ("hetero",)),
    "code_model": Key("the model a routine no converter proves is asked"
                      " of", "dict", ("hetero",),
                      subs=("provider", "url", "model")),
    "code_model_rounds": Key("rounds the model gets on one routine", "int",
                             ("hetero",), knob="size"),
    "compare_with": Key("another tool's own check of this move - a Tencent"
                        " or Alibaba DTS data check's report, AWS DMS's"
                        " table statistics - held table by table against"
                        " migkit's answers in the evidence", "str"),
    "create_streams": Key("create a missing stream", "bool", ("kinesis",)),
    "cutover": Key("the cutover as one timed path that freezes the source"
                   " and undoes itself (`migkit.cutover`)", "dict|bool",
                   subs=("budget", "start_below", "freeze", "pooler",
                         "flip", "flip_undo", "keys", "jobs",
                         "no_return", "restore_points")),
    "ddl": Key("what a DDL on the source does to a running tail: stop"
               " it, park only the table it touched, or apply what can be"
               " carried at its place in the log - one word, or a mapping"
               " of `default`, tables or patterns to a word each, and"
               " `skip`: the kinds left out (truncate, drop, create)",
               "str|dict"),
    "deadline": Key("when the move must be done by - a duration (6h,"
                    " 90m, 2d) or a moment (2026-10-01T02:00): the plan"
                    " names the cheapest machine for migkit that meets it",
                    "str|number"),
    "edition": Key("what a change of edition may leave behind on the"
                   " target - accept: [the features whose loss the hop"
                   " accepts, as assess names them: encryption at rest,"
                   " auditing, change data capture]", "dict",
                   ("mssql", "oracle", "mysql", "mongodb"),
                   subs=("accept",)),
    "deep": Key("compare every key's value, not a sample", "bool",
                ("redis",)),
    "exact_batches": Key("a one-way tail numbers its batches on the target,"
                         " so a stop resumes after the last committed", "bool",
                         knob="way"),
    "follow_until": Key("stopped or caught_up: how long a follow runs",
                        "str"),
    "exclude_schema": Key("schemas left out of the schema dump", "str",
                          ("postgres",)),
    "export_bucket": Key("s3://bucket/prefix the source's own export of a"
                         " table is written under, for a copy that reads"
                         " the table off it", "str", ("dynamodb",)),
    "fence_timeout": Key("seconds a fenced re-check waits", "int"),
    "fk_scan_seconds": Key("the budget of the orphaned-row scan", "int",
                           ("postgres",)),
    "float_tolerance": Key("how far two floats may differ", "number",
                           ("postgres",)),
    "format": Key("the change stream's message format", "str", STREAMS),
    "keep_proof": Key("the tail keeps every table's digest leaves up as"
                      " it applies, so a check reads only what changed",
                      "bool"),
    "key": Key("the key column(s) of a listed table", "str|list",
               ("generic",)),
    "limits": Key("what the move may take at most, kept to as hard"
                  " limits by its plan (`LIMITS`): source_load (the share"
                  " of the source's reads, 0.3 or 30%), money (in the"
                  " currency transfer_price_per_gb is in), bytes_billed"
                  " (a warehouse's), outage (the cutover's write outage,"
                  " seconds), replica_lag (seconds the source's replica"
                  " may fall behind), target_memory and"
                  " target_connections",
                  "dict", subs=("source_load", "money", "bytes_billed",
                                "outage", "replica_lag", "target_memory",
                                "target_connections")),
    "liquibase": Key("the second schema reading, on or off", "bool",
                     ("postgres",), knob="way", named_for_a_program=True),
    "lob_scan_seconds": Key("the budget of the large-value scan", "int",
                            ("mysql",)),
    "mask": Key("values reports show as salted hashes", "str|list|bool"),
    "mask_on_write": Key("table.column -> null or constant <text>: masked"
                         " on the way into the target", "dict", ("mysql",)),
    "max_message_bytes": Key("the largest message a stream takes", "int",
                             STREAMS),
    "mongodb_entitlement": Key("atlas or enterprise: the MongoDB licence"
                               " the hop has", "str", ("mongodb",)),
    "newer_wins": Key("the column that keeps the newer row", "str"),
    "noise_prefix": Key("object names a schema check leaves out", "str",
                        ("postgres",)),
    "notify": Key("who is told what the migration is doing - an address,"
                  " a list of them, or {to, events, lag, detail}: the"
                  " events told (verdict, drift, lag, cutover, approval,"
                  " failure, fallback, done), the seconds a change tail may"
                  " be behind, and whether an error's words are sent and"
                  " not only its kind", "str|list|dict",
                  subs=("to", "events", "lag", "detail")),
    "egress": Key("what may leave the machine for this hop - none, or"
                  " the kinds that may (notify, traces, assist, evidence),"
                  " or {allow, to}: the kinds and the hosts they may go"
                  " to", "str|list|dict|bool", subs=("allow", "to")),
    "evidence": Key("the evidence export: {sign: a private SSH key} it is"
                    " signed with", "dict", subs=("sign",)),
    "offset_header": Key("each message the copy carries keeps its source"
                         " partition and offset in a header, so a group's"
                         " position is placed on the target exactly", "bool",
                         ("kafka",)),
    "on_conflict": Key("source-wins or keep-target", "str"),
    "origin_table": Key("keep or drop: the table of migkit's batches on a"
                        " one-way target (migkit_origin), taken away when"
                        " the stream is torn down for good - at a --drop,"
                        " and at the cutover's point of no return - once no"
                        " tail of the hop runs", "str"),
    "partition_by": Key("the column a stream partitions by", "str",
                        STREAMS),
    "performance": Key("gate: a slower target fails the check", "str"),
    "physical": Key("the target may be seeded from a copy of the source's"
                    " pages, SQL Server's own backup restored, or a cloud's"
                    " own copy (`physical`, `backupseed`, `cloudseed`), and"
                    " how", "dict", ("postgres", "mysql", "mssql"),
                    subs=("data", "run", "stop", "start", "restart",
                          "source", "source_plugin", "cloud", "source_id",
                          "target_id", "region", "target_region",
                          "target_role_arn", "kms_key", "instance_class",
                          "way", "subnet_group", "security_groups",
                          "subscription", "resource_group", "location",
                          "project", "address", "upgrade", "backup",
                          "restore_from", "profile", "role", "product",
                          "zone", "vpc", "subnet", "spec", "storage")),
    "proof_sandbox": Key("the two scratch servers stored code, the"
                         " constraints, the collations and the sized types"
                         " are run and compared on, or docker for migkit"
                         " to start them", "str|dict", ("hetero",),
                         subs=("source", "target", "docker")),
    "production": Key("host patterns of production servers: a target"
                      " matching one is written only when the hop names"
                      " it (production_target)", "str|list"),
    "production_target": Key("the production server this hop writes to"
                             " on purpose, by a name it answers to",
                             "str|list"),
    "protect_target": Key("the application may not write the target"
                          " while migkit moves into it", "bool"),
    "pt_apply": Key("the online schema change path, on or off", "bool",
                    ("mysql",), knob="way", named_for_a_program=True),
    "region": Key("the region the machine and the target's class are"
                  " priced in, where the addresses do not say", "str"),
    "rehearses": Key("goal rehearse: the hop this one rehearses, whose plan"
                     " is forecast from what this one measures - its name,"
                     " or {hop, databases: {this hop's database: that"
                     " hop's}}", "str|dict", subs=("hop", "databases")),
    "reladiff": Key("the second data reading, on or off", "bool",
                    ("mysql",), knob="way", named_for_a_program=True),
    "repair_window": Key("seconds a writer is paused for a repair",
                         "int"),
    "replication_factor": Key("a created topic's replicas", "int",
                              ("kafka",)),
    "reverse": Key("at_cutover: a stream back starts at cutover", "str"),
    "rules": Key("questions both sides must answer alike", "dict"),
    "sample": Key("how many keys or messages are compared", "int",
                  ("redis", "kafka")),
    "schedule": Key("when the hop runs by itself", "dict",
                    subs=("cron", "timezone", "run", "args", "catchup",
                          "pause_after_failures", "max_duration",
                          "retry_window")),
    "schema_authority": Key("which schema reading decides", "str"),
    "server_id": Key("the replica id the binlog reader uses", "int",
                     ("mysql",)),
    "server_to_server": Key("the target may pull the rows from the source"
                            " itself, with the objects that needs made on"
                            " the servers for the copy and dropped after:"
                            " true, or {source: host:port} where the target"
                            " reaches the source at another address",
                            "bool|dict", ("postgres",), subs=("source",)),
    "settle": Key("seconds a re-check waits for a stream to settle",
                  "int"),
    "share_tables": Key("machines share one move's tables: true, each"
                        " table to one machine; ranges, a large table's"
                        " ranges as well, every write held to the number"
                        " of the claim that took it", "bool|str"),
    "ship": Key("the seed may go on media where the link is too narrow"
                " (`ship`): the carrier (disk, snowball, databox,"
                " transfer_appliance), where it is written (path, or"
                " endpoint with bucket or container, prefix, region,"
                " profile, ca_bundle, credential), its model or capacity,"
                " where the far end reads it (load_from, load_endpoint,"
                " load_region, load_profile, load_credential), how long it"
                " takes to reach the far end as the operator states it"
                " (shipping), and who must have signed its manifest"
                " (signers)", "dict",
                subs=("carrier", "path", "endpoint", "bucket", "container",
                      "prefix", "region", "profile", "ca_bundle",
                      "credential", "model", "capacity", "load_from",
                      "load_endpoint", "load_region", "load_profile",
                      "load_credential", "shipping", "signers")),
    "slot": Key("the slot a pipeline migkit did not set up reads, which"
                " the fence waits for", "str|list", ("postgres",)),
    "standby": Key("the same move run on two or more machines: one moves"
                   " it, the others take it over from the last position"
                   " committed once it stops renewing its lease, the"
                   " target refusing the one that lost it", "bool"),
    "source_changes": Key("the answer, given ahead, to a change on the"
                          " source a faster way needs: yes (migkit makes it"
                          " and takes it off again), no or dba - for every"
                          " change, or by its name (clone_plugin: yes)",
                          "str|dict|bool"),
    "shards": Key("a created stream's shards", "int", ("kinesis",)),
    "snapshot_limit": Key("how old a snapshot may get", "number",
                          ("postgres",)),
    "source_engine": Key("the source's engine of a hetero hop", "str",
                         ("hetero",)),
    "spool_limit": Key("bytes a parked table's changes may take on this"
                       " machine before the table is copied again instead",
                       "int", knob="size"),
    "state": Key("where run state is kept, and a directory every"
                 " machine of the hop sees (shared)", "dict",
                 subs=("backend", "bucket", "prefix", "region",
                       "endpoint_url", "mirror", "shared")),
    "subset": Key("goal subset: the rows a copy for testing starts from -"
                  " start: {table: a condition, a share like 10% or a"
                  " number of rows} - whether the rows that refer to them"
                  " come too (children), and the most rows it may hold"
                  " (most)", "dict", subs=("start", "children", "most")),
    "tables": Key("the tables of a generic hop", "list", ("generic",)),
    "target_engine": Key("the target's engine of a hetero hop", "str",
                         ("hetero",)),
    "target_has_replicas": Key("false: the target has no replicas and no"
                               " restore replays its binlog, so a load"
                               " may leave the binlog off", "bool",
                               ("mysql",)),
    "target_rows": Key("refuse, replace or keep: what a copy does with"
                       " the rows a table on the target already holds",
                       "str"),
    "tombstones": Key("carry deletes' empty messages", "bool", ("kafka",)),
    "topic": Key("the stream name template", "str", STREAMS),
    "topology": Key("one_way or two_way", "str"),
    "transfer_price_per_gb": Key("what a carried gigabyte costs",
                                 "number"),
    "two_readings": Key("a second, independent reading of every table,"
                        " row by row where it has a key (a table that"
                        " differs is read twice whatever this says):"
                        " true, or rows, sample and case_insensitive",
                        "bool|dict",
                        subs=("rows", "sample", "case_insensitive")),
    "two_way": Key("migkit's own tails both ways, a row both sides"
                   " changed settled column by column: the rule of a"
                   " column nothing names (on_conflict), each table's"
                   " rules by column (tables), and what settles a row one"
                   " side deleted", "dict|bool",
                   subs=("on_conflict", "column", "source_rank",
                         "target_rank", "delta", "tables",
                         "delete_vs_update", "delete_missing")),
    "types_from_data": Key("a column whose declaration leaves its domain"
                           " open (a bare decimal, a text of no length) is"
                           " built sized from the data it holds, each"
                           " narrowing recorded and proved", "bool",
                           ("hetero",)),
    "verify_batches": Key("compare a bulk copy with the source", "bool"),
}

#: the keys of a side written in the long form
ENDPOINT_KEYS = {
    "host": Key("the server's name or address", "str"),
    "port": Key("the server's port", "int"),
    "user": Key("the user migkit signs in as", "str"),
    "password": Key("a reference to the password - env:, file:, vault:,"
                    " keyring:, aws-sm:, gcp-sm:, azure-kv:", "str"),
    "options": Key("the endpoint's options, the same as writing them"
                   " beside host", "dict"),
}

PG_FAMILY = ("postgres", "redshift")
#: the engines whose TLS migkit names itself (`Endpoint.mongo_tls`,
#: `redis_tls`, `cassandra_tls`)
TLS_OWN = ("mongodb", "redis", "cassandra")
#: the keys of a side's own options (under it, or under its `options`)
ENDPOINT_OPTIONS = {
    "auth": Key("password, or aws_iam for a token the cloud signs", "str",
                ("postgres", "mysql", "dsql")),
    "changes": Key("where the source publishes its changes, where a"
                   " client cannot read its log: a Kafka topic (servers,"
                   " topic or topics, format, and how to sign in), a"
                   " Kinesis stream (stream, region, cdc_stream,"
                   " order_window), or a changefeed streamed to migkit"
                   " (via: changefeed)", "dict",
                   ("tidb", "cockroachdb", "dsql", "couchbase"),
                   subs=("via", "servers", "hosts", "topic", "topics",
                         "format", "security_protocol", "sasl_mechanism",
                         "user", "password", "ssl_cafile", "stream",
                         "region", "endpoint_url", "cdc_stream",
                         "order_window")),
    "change_feed": Key("latest: follow by the latest-version change feed,"
                       " which carries no delete - where the account has"
                       " no all-versions-and-deletes feed", "str",
                       ("cosmosdb",)),
    "priority": Key("low: reads yield to the application where the"
                    " account runs priority-based execution", "str",
                    ("cosmosdb",)),
    "network": Key("external: reach the cluster's nodes at their"
                   " alternate addresses", "str", ("couchbase",)),
    "mgmt_port": Key("the cluster manager's port, where not 8091 (18091"
                     " with TLS)", "int", ("couchbase",)),
    "aws_region": Key("the region an aws_iam token is signed for", "str",
                      ("postgres", "mysql")),
    "aws_role_arn": Key("the role an aws_iam token is signed as", "str",
                        ("postgres", "mysql")),
    "sslmode": Key("TLS: disable ... verify-full", "str",
                   PG_FAMILY + ("teradata",)),
    "sslrootcert": Key("the authority's certificate file", "str",
                       PG_FAMILY + ("teradata",)),
    "logmech": Key("how a Teradata side signs in, where not by its own"
                   " password (LDAP, KRB5)", "str", ("teradata",)),
    "client": Key("the SAP client the services are read in", "str",
                  ("sapodp",)),
    "language": Key("the SAP logon language", "str", ("sapodp",)),
    "service_path": Key("where the SAP system publishes its OData services,"
                        " where not /sap/opu/odata/sap", "str",
                        ("sapodp",)),
    "kind": Key("how a program source is spoken to - the kind of"
                " connector program it runs, its address's scheme", "str",
                ("saas",)),
    "program": Key("what a program source runs: a tap's command, a"
                   " connector's image or command, a dlt source's"
                   " module:function, a Salesforce org's sign-in host",
                   "str", ("saas",)),
    "config": Key("the file of a program's own settings, credentials"
                  " included", "str", ("saas",)),
    "streams": Key("the streams (or objects) of a program source to read,"
                   " all of them when left out", "str|list", ("saas",)),
    "install": Key("what installs a program source's program where it is"
                   " not on this machine: a pip requirement, installed into"
                   " an environment of its own", "str", ("saas",)),
    "container": Key("run a connector as the container its image is, or as"
                     " a program on this machine", "bool", ("saas",)),
    "api_version": Key("the Salesforce API version the objects are read"
                       " through", "str", ("saas",)),
    "pubsub_endpoint": Key("where Salesforce's change events are read from"
                           " (host:port)", "str", ("saas",)),
    "pubsub_tls": Key("TLS to the change events' endpoint: off only for a"
                      " local stand-in", "bool", ("saas",)),
    "sslcert": Key("a client certificate file", "str", PG_FAMILY),
    "sslkey": Key("a client key file", "str", PG_FAMILY),
    "sslcrl": Key("a revocation list file", "str", PG_FAMILY),
    "ssl": Key("TLS, nothing checked", "bool", ("mysql",)),
    "ssl_ca": Key("the authority's certificate file", "str", ("mysql",)),
    "ssl_cert": Key("a client certificate file", "str", ("mysql",)),
    "ssl_key": Key("a client key file", "str", ("mysql",)),
    "ssl_verify_identity": Key("check the name on the certificate",
                               "bool", ("mysql",)),
    "hosts": Key("more than one server", "str|list",
                 ("mongodb", "kafka")),
    "uri_options": Key("the driver's own options", "str", ("mongodb",)),
    "path": Key("the file or directory", "str",
                ("sqlite", "duckdb", "parquet", "copybook",
                 "delta")),
    "tables": Key("each table of a directory of files described by COBOL"
                  " copybooks: its file, copybook, record, recfm, lrecl,"
                  " codepage, key, segment, redefines, odo, bytes and"
                  " unreadable", "dict", ("copybook",)),
    "codepage": Key("the EBCDIC code page the files' text is in (cp037,"
                    " cp838, cp1047 ...)", "str", ("copybook",)),
    "recfm": Key("the files' record format: F, FB, V, VB or ZOSMF",
                 "str", ("copybook",)),
    "unreadable": Key("stop (the default) or null: a value that is not"
                      " one stops the read, or lands as NULL and is"
                      " listed", "str", ("copybook",)),
    "odo": Key("actual (the default) or max: whether a table OCCURS"
               " DEPENDING ON its count takes the room of its count or"
               " of its most", "str", ("copybook",)),
    "url": Key("the whole connection address (Snowflake's account URL a"
               " streaming channel signs in at, a Cosmos DB account's"
               " endpoint, a Couchbase connection string)", "str",
               ("generic", "opensearch", "parquet", "delta", "snowflake",
                "couchbase", "cosmosdb")),
    "secure": Key("TLS", "bool", ("clickhouse",)),
    "address_from_target": Key("host:port the target reaches the source"
                               " at, where it knows it by another name",
                               "str", ("clickhouse",)),
    "service": Key("the service to connect to", "str", ("oracle",)),
    "service_name": Key("the service to connect to", "str", ("oracle",)),
    "mine_service": Key("the container's root service, where the redo is"
                        " mined from there (a database before 21c)", "str",
                        ("oracle",)),
    "mine_user": Key("the user the redo is mined as, where not the side's"
                     " own", "str", ("oracle",)),
    "mine_password": Key("that user's password reference", "str",
                         ("oracle",)),
    "mine_standby": Key("a physical standby of the source the redo is"
                        " mined on instead (host:port/service)", "str",
                        ("oracle",)),
    "redo_reader": Key("where a redo-file reader run beside the redo"
                       " listens (host:port)", "str", ("oracle",)),
    "redo_reader_dsn": Key("how that reader reaches the database, where"
                           " not as migkit does", "str", ("oracle",)),
    "redo_reader_mb": Key("the memory that reader may take, in MB", "int",
                          ("oracle",), knob="size"),
    "link_dsn": Key("how the target reaches the source over a database"
                    " link, where not as migkit does", "str", ("oracle",)),
    "database": Key("the database to connect to (a z/OS subsystem's"
                    " location name, an IBM i system's relational"
                    " database)", "str",
                    ("db2", "db2i", "db2zos", "redshift", "snowflake",
                     "bigquery")),
    "account": Key("the account identifier", "str", ("snowflake",)),
    "warehouse": Key("the warehouse queries run in (Snowflake), where new"
                     " tables' files go (Iceberg)", "str",
                     ("snowflake", "iceberg")),
    "role": Key("the role queries run as", "str", ("snowflake",)),
    "authenticator": Key("how the side signs in, where not by password",
                         "str", ("snowflake",)),
    "private_key_file": Key("a key pair's private key file, which a"
                            " streaming channel signs in with", "str",
                            ("snowflake",)),
    "private_key_passphrase": Key("the private key's passphrase", "str",
                                  ("snowflake",)),
    "location": Key("the datasets' region", "str", ("bigquery",)),
    "credentials_file": Key("a service account's key file", "str",
                            ("bigquery", "pubsub")),
    "anonymous": Key("sign in as nobody, which only an emulator takes",
                     "bool", ("bigquery",)),
    "api_endpoint": Key("the API's address, a private one or an"
                        " emulator's", "str", ("bigquery",)),
    "write_endpoint": Key("the Storage Write API's host:port", "str",
                          ("bigquery",)),
    "stage": Key("an S3 prefix the cluster reads staged files from", "str",
                 ("redshift",)),
    "iam_role": Key("the role the cluster reads the stage as", "str",
                    ("redshift",)),
    "stage_region": Key("the stage bucket's region", "str", ("redshift",)),
    "stage_endpoint_url": Key("the stage's S3-compatible endpoint", "str",
                              ("redshift",)),
    "iam": Key("sign in by IAM rather than a password", "bool",
               ("redshift",)),
    "cluster_identifier": Key("the provisioned cluster an IAM sign-in is"
                              " for", "str", ("redshift",)),
    "serverless_work_group": Key("the Serverless workgroup an IAM sign-in"
                                 " is for", "str", ("redshift",)),
    "profile": Key("the AWS profile an IAM sign-in uses", "str",
                   ("redshift",)),
    "db_user": Key("the database user an IAM sign-in becomes", "str",
                   ("redshift",)),
    "catalog": Key("the Iceberg catalog: sqlite:///... or another SQL"
                   " database's URL, or a REST catalog's http(s) address",
                   "str", ("iceberg",)),
    "catalog_name": Key("the name the catalog keeps its tables under",
                        "str", ("iceberg",)),
    "catalog_properties": Key("anything else the catalog takes", "dict",
                              ("iceberg",)),
    "project": Key("the project", "str", ("pubsub",)),
    "region": Key("the region", "str", ("dynamodb", "kinesis", "parquet",
                                        "redshift", "delta", "dsql")),
    "endpoint_url": Key("an endpoint of its own (a local one, a"
                        " compatible store)", "str",
                        ("dynamodb", "kinesis", "parquet", "delta")),
    "table_prefix": Key("the table names' template", "str",
                        ("dynamodb",)),
    "index_prefix": Key("the index names' template", "str",
                        ("opensearch",)),
    "tls": Key("TLS, the certificate checked against the system's"
               " authorities (https on OpenSearch, Cosmos DB and SAP)", "bool",
               ("opensearch", "db2i", "couchbase", "cosmosdb", "sapodp")
               + TLS_OWN),
    "tls_ca_file": Key("the authorities' certificates file", "str",
                       ("db2i", "couchbase", "sapodp") + TLS_OWN),
    "tls_cert_file": Key("a client certificate file", "str", TLS_OWN),
    "tls_key_file": Key("a client key file", "str", ("redis", "cassandra")),
    "tls_crl_file": Key("a revocation list file", "str", ("mongodb",)),
    "tls_insecure": Key("TLS with nothing checked (an emulator's own"
                        " certificate)", "bool",
                        ("db2i", "couchbase", "cosmosdb", "sapodp")
                        + TLS_OWN),
    "encrypt": Key("require, request or off", "str|bool", ("mssql",)),
    "redis_product": Key("community, cloud or software: the Redis product"
                         " the target is, where its name cannot say",
                         "str", ("redis",)),
    "rdb_file": Key("a backup file of the source's keyspace, read in its"
                    " place so the server is left alone", "str",
                    ("redis",)),
    "aof_file": Key("the source's append-only file, read in its place",
                    "str", ("redis",)),
    "aof_timestamp": Key("the moment, in seconds since the epoch, the"
                         " append-only file is read up to", "int",
                         ("redis",)),
    "snapshot": Key("a snapshot of the source an index may be copied from:"
                    " its name, its repository on the source, and where its"
                    " files are", "dict", ("opensearch",),
                    subs=("name", "repository", "local_dir", "s3_uri",
                          "s3_region")),
    "verify": Key("check the certificate", "bool", ("opensearch",)),
    "odbc_driver": Key("the ODBC driver's file", "str", ("ase",)),
    "replication": Key("a created keyspace's replication", "str|dict",
                       ("cassandra",)),
    "security_protocol": Key("PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
                             "str", ("kafka",)),
    "sasl_mechanism": Key("how SASL signs in", "str", ("kafka",)),
    "ssl_cafile": Key("the authority's certificate file", "str",
                      ("kafka",)),
    "schema_registry": Key("the schema registry's address", "str",
                           ("kafka",)),
    "schema_registry_user": Key("the schema registry's user", "str",
                                ("kafka",)),
    "schema_registry_password": Key("the schema registry's password",
                                    "str", ("kafka",)),
    "managed": Key("the managed service this side is and how its provider's"
                   " API reaches it: service (its name in the managed-service"
                   " catalogue, where neither its address nor its server"
                   " says), id (its instance at the provider, where its"
                   " address does not find it), region, profile (the"
                   " provider CLI's profile its credentials come from), role"
                   " (one assumed), and what else the provider asks for",
                   "dict", subs=("service", "id", "region", "profile",
                                 "role", "project", "subscription",
                                 "resource_group", "compartment",
                                 "project_id", "zone", "cluster", "vpc",
                                 "subnet", "spec", "storage")),
    "tunnel": Key("reach the server through a tunnel", "str|dict"),
    "tunnel_to": Key("what carries the connection, for the report",
                     "str"),
}

#: each goal: what it is, the options it stands for (the hop's own win),
#: whether it writes at all, the commands that do it - and, for `init`,
#: `assess`, the dry run and the docs, what it does on each side and what
#: it guarantees (the one place these are written; `explain_goal` says
#: them). A value of `SUPPORTED` is set only where the engine can do it
#: (`freeze.supported`). No goal turns `cutover:` on (the owner,
#: 2026-09-30): the cutover migkit drives runs only where the hop sets it,
#: and `CUTOVER` says what that adds.
SUPPORTED = object()
#: the goal of a hop that names none
DEFAULT_GOAL = "copy"
GOALS = {
    "copy": {"what": "move the data once and prove every row arrived",
             "options": {}, "writes": True,
             "next": ["migkit move {hop} --mode full",
                      "migkit move {hop} --mode full --go",
                      "migkit check {hop}"],
             "source": "read, never written: each table read as of one"
                       " moment where the engine gives one; a row the"
                       " application changes while it is read is not"
                       " carried, and the check names it",
             "target": "the tables it lacks made, and filled with the"
                       " source's rows, indexes built after them; a table"
                       " that already holds rows is refused unless the hop"
                       " says what to do with them (target_rows: refuse,"
                       " replace or keep)",
             "guarantees": "every row compared after the copy (migkit"
                           " check): equal, or each row that differs"
                           " named"},
    "cutover": {"what": "move, follow the source's changes, prove both"
                        " equal, and switch with the least downtime - a"
                        " stream back ready from the moment of the switch",
                "options": {"reverse": "at_cutover",
                            "protect_target": SUPPORTED},
                "writes": True,
                "next": ["migkit sync {hop} --mode migrate",
                         "migkit sync {hop} --mode migrate --go",
                         "migkit watch {hop}"],
                "source": "read, and its log of changes followed from a"
                          " position taken before the copy - the stream's"
                          " own slot and publication on PostgreSQL, its"
                          " binary log read on MySQL, a change stream on"
                          " MongoDB - made by the stream and dropped when"
                          " it is torn down; nothing else is written to it"
                          " unless the hop turns cutover: on, and then only"
                          " during the cutover",
                "target": "filled as a copy fills it, then kept level with"
                          " the source change by change; kept from the"
                          " application's writes until the cutover where"
                          " the engine allows it (protect_target); after"
                          " it, a stream back from it to the old source"
                          " (reverse: at_cutover)",
                "guarantees": "no change lost between the copy and the"
                              " stream (the position is taken before the"
                              " copy); each cycle's changes compared (watch"
                              " --verify --delta); the stream torn down only"
                              " once nothing writes to the source and the"
                              " stream has passed where it stands"},
    "verify": {"what": "prove two databases hold the same; nothing is"
                       " written to either",
               "options": {}, "writes": False,
               "next": ["migkit check {hop}"],
               "source": "read only",
               "target": "read only",
               "guarantees": "equal, or every table and row that differs"
                             " named; nothing written to either side"},
    "two-way": {"what": "both sides written, each side's changes carried"
                        " to the other",
                "options": {"topology": "two_way", "two_way": True},
                "writes": True,
                "next": ["migkit move {hop} --mode full+cdc",
                         "migkit move {hop} --mode full+cdc --go"],
                "source": "written: the target's changes carried to it by"
                          " key, and its own told apart from what migkit"
                          " wrote there, so nothing goes round",
                "target": "written the same way: the source's changes"
                          " carried to it",
                "guarantees": "a change made on either side arrives on the"
                              " other once; where both sides changed one"
                              " row, what only one side changed is kept"
                              " and a column both changed is settled by"
                              " its rule, alike on both sides (two_way)"},
    "keep-in-sync": {"what": "a copy that follows the source for as long"
                             " as it runs, the application kept off it",
                     "options": {"protect_target": SUPPORTED},
                     "writes": True,
                     "next": ["migkit sync {hop} --mode migrate",
                              "migkit sync {hop} --mode migrate --go"
                              " --serve"],
                     "source": "read, and its log of changes followed for as"
                               " long as it runs; nothing else is written"
                               " to it",
                     "target": "a copy kept level with the source, the"
                               " application kept from writing to it"
                               " (protect_target)",
                     "guarantees": "each cycle's changes compared, the"
                                   " stream's lag said, a difference named"},
    "rehearse": {"what": "the whole path the real run will take - copy,"
                         " follow, prove, cut over - on a copy or a sample"
                         " of the source into a target of its own, every"
                         " phase timed, so the hop it rehearses"
                         " (rehearses:) is forecast from what it measured",
                 "options": {"protect_target": SUPPORTED},
                 "writes": True,
                 "next": ["migkit move {hop} --mode full+cdc --go",
                          "migkit check {hop}",
                          "migkit move {hop} --mode cdc --drop --go",
                          "migkit assess {hop}"],
                 "source": "a copy or a sample of the real run's source,"
                           " read and followed as a cutover follows it;"
                           " written only by a cutover the hop turns on,"
                           " as the real run's would be",
                 "target": "a target of its own, filled, followed and cut"
                           " over to as the real run's will be; its own"
                           " writes are timed there, rolled back",
                 "guarantees": "every phase measured and kept for the real"
                               " run's forecast; the two sides proved equal"
                               " before the real run is allowed to start"},
    "upgrade": {"what": "the same engine at a newer version, a higher"
                        " edition (SQL Server Web to Enterprise), or both,"
                        " with the least downtime: what the new version"
                        " or edition changes checked first, then the data"
                        " moved into it and followed, or a copy of its"
                        " pages taken to the target where the hop allows"
                        " it - upgraded there where the version moves -"
                        " and switched over",
                "options": {"protect_target": SUPPORTED},
                "writes": True,
                "next": ["migkit assess {hop}",
                         "migkit move {hop} --mode full+cdc",
                         "migkit move {hop} --mode full+cdc --go",
                         "migkit check {hop}",
                         "migkit move {hop} --mode cdc --drop --go"],
                "source": "read and followed as a cutover follows it; never"
                          " upgraded where it runs - its downtime is the"
                          " cutover's alone",
                "target": "a server of the new version or edition filled"
                          " and followed, or a copy of the source's pages"
                          " taken there - SQL Server's own backup restored,"
                          " MySQL's clone, PostgreSQL's pages - upgraded by"
                          " the new version's own upgrade where the version"
                          " moves, then followed from the copy's position",
                "guarantees": "what the new version removes or changes, and"
                              " what the new edition lacks of what the"
                              " source uses, said before anything is"
                              " written; the source's own statements"
                              " planned, timed and held to the same answers"
                              " on both versions"},
    "subset": {"what": "a copy for testing: the rows chosen and every row"
                       " they refer to, masked where the hop says, into a"
                       " staging or developer database, proved row by row",
               "options": {}, "writes": True,
               "next": ["migkit move {hop} --mode full",
                        "migkit move {hop} --mode full --go",
                        "migkit check {hop}"],
               "source": "read, never written: the rows chosen and those"
                         " they refer to",
               "target": "its tables in scope emptied and filled with the"
                         " subset, masked on the way where the hop says",
               "guarantees": "every reference in the copy resolves inside"
                             " it; every row as the source's, its masked"
                             " columns as the rule writes them, none"
                             " beyond the subset"},
    "consolidate": {"what": "many databases or tenants into one: each"
                            " source's rows under a column the hop names,"
                            " filled with its own value, keys kept apart"
                            " by the hop's rule, proved source by source",
                    "options": {}, "writes": True,
                    "next": ["migkit assess {hop}",
                             "migkit move {hop} --mode full",
                             "migkit move {hop} --mode full --go",
                             "migkit check {hop}"],
                    "source": "read, never written: each table through its"
                              " row filter",
                    "target": "one database the sources share: this"
                              " source's rows there replaced by its own,"
                              " under its value, keys shifted where the"
                              " rule shifts them; the other sources' rows"
                              " left as they are; the counters raised past"
                              " every source's keys",
                    "guarantees": "keys two sources hold found before"
                                  " anything is written and refused, or kept"
                                  " apart by the rule (tenant, offset,"
                                  " remap); every source's rows proved in"
                                  " the target under its value; no row"
                                  " under a value no source has"},
    "split": {"what": "one database into many - a service extracted, a"
                      " database split by region: each part's rows by its"
                      " rule, every row placed exactly once, proved",
              "options": {}, "writes": True,
              "next": ["migkit assess {hop}",
                       "migkit move {hop} --mode full",
                       "migkit move {hop} --mode full --go",
                       "migkit check {hop}"],
              "source": "read, never written: every row counted against"
                        " every part's rule before the move",
              "target": "this part's tables (the hop's scope) filled with"
                        " the rows its rule takes (mapping.where)",
              "guarantees": "rows no part takes and rows two parts take"
                            " found before the move and refused (rest:"
                            " stays, shared); each key in exactly one"
                            " part's target, the targets together the"
                            " source's; references across the split"
                            " counted and said"},
    "archive": {"what": "old rows to cheaper storage: the rows each rule"
                        " picks added to an archive and proved there, then"
                        " removed from the source only once proved and"
                        " only where the hop allows it",
                "options": {}, "writes": True,
                "next": ["migkit assess {hop}",
                         "migkit move {hop} --mode full",
                         "migkit move {hop} --mode full --go",
                         "migkit check {hop}"],
                "source": "read; written only with archive: {remove: true}"
                          " and the source's own answer by name"
                          " (source_changes: {archived_rows: yes}) behind"
                          " the hop's approvals: each archived row removed"
                          " once the archive holds it as the source has it"
                          " now, a row other rows still name never",
                "target": "the archive: the rows each rule picks added,"
                          " what it holds already kept, never overwritten",
                "guarantees": "every row the rule picks in the archive as"
                              " the source has it; every row removed from"
                              " the source in the archive with the digest"
                              " it had, in the ledger; put back from the"
                              " archive by archive: {restore: true}"},
}

#: each mode of `move` and `sync`: what it does on each side, as the dry
#: run says it before `--go`
MODES = {
    ("move", "full"): "one copy of every table the hop carries: the source"
                      " read, the target written",
    ("move", "cdc"): "the stream of changes started, or resumed from where"
                     " it stood: the source's log read, the target written"
                     " change by change",
    ("move", "full+cdc"): "the copy, then the stream from a position taken"
                          " before the copy began, so nothing changed"
                          " during it is lost",
    ("move", "cdc --drop"): "the cutover: the stream torn down, and what"
                            " else happens on each side is what cutover:"
                            " says (below); run again once a cutover of the"
                            " hop has flipped, its point of no return",
    ("sync", "reconcile"): "the last check's differences shown; --apply"
                           " writes the target to match the source",
    ("sync", "verify"): "read only: every table compared as of one moment",
    ("sync", "seed"): "the schema aligned, the copy, the differences"
                      " repaired and a consistent check, one after another",
    ("sync", "stream"): "the stream started, and each cycle's changes"
                        " compared",
    ("sync", "migrate"): "seed, then stream",
}

#: what `cutover:` in a hop's options adds, and what the operator does by
#: hand without it (`cutover.plan` says the steps from here). `{hop}` is
#: the hop's name, `{back}` what the stream back adds to the teardown.
CUTOVER = {
    "adds": [
        "a timed path: the write outage clocked from the pooler's pause or"
        " the freeze to the flip, against the hop's budget - past it, all"
        " of it is undone",
        "the freeze: migkit stops the source's writes itself, the way"
        " chosen from what the source allows - the whole server made"
        " read-only where it may, or the application's accounts kept from"
        " writing and their sessions ended - and reads it back; PostgreSQL,"
        " MySQL, MongoDB, Redis, SQL Server, Cassandra, Kafka and OpenSearch"
        " sources alike",
        "the drain: the source's position taken after the freeze, and the"
        " hop's own stream waited for until it passes it - or, where the"
        " stream keeps no position migkit can read, a mark written on the"
        " source after the freeze and waited for on the target; a"
        " streaming pipeline and a leg run from here drained by their own"
        " offsets and positions",
        "the fenced check: the rows changed since the last verified pass"
        " compared inside the outage, and the schema compared with the"
        " preflight's",
        "the counters: the target's sequences and auto-increments raised"
        " past every key the source handed out",
        "the stream back: from the new primary to the old source"
        " (reverse: at_cutover), made and proven before the freeze where"
        " its engine lets it wait idle, so the flip only starts it, and"
        " proven to reach the old source",
        "the undo: every step recorded before it acts and put back, the"
        " last first, on any failure, on the budget passing, or on the"
        " next run after one killed half way",
        "the outage measured: each run's outage and each step's time"
        " recorded, and the next cutover refused where the last one's was"
        " past its budget",
        "the point of no return: a step of its own behind an approval of"
        " its own - the forward stream ended for good, the old source"
        " given back its writes, the stream back dropped, what the cutover"
        " left taken away - after which going back is refused",
    ],
    "on source": "written during the cutover only: its jobs switched off,"
                 " its writes frozen, each step reversible and behind the"
                 " hop's approvals; after the flip it stays frozen until"
                 " `migkit rollback {hop} --db DB --apply`, or until the"
                 " point of no return gives it back",
    "by hand": [
        "switch off on the source the jobs it runs (pg_cron jobs, MySQL"
        " events), so they write in one place",
        "stop the application's writes to the source: stop it, or revoke"
        " its write privileges and end its sessions - ending them is what"
        " rolls back what they had not committed",
        "migkit move {hop} --mode cdc --drop --go - migkit proves nothing"
        " writes to the source and nothing is prepared there, waits for"
        " the stream to pass where the source stands, and only then tears"
        " it down{back}",
        "migkit check {hop} - the final comparison",
        "migkit sync {hop} --go --kind sequences --tag pre-cutover - the"
        " target's counters past the source's",
        "switch the jobs on on the target",
        "point the application at the target",
        "once the application is on the target for good, the point of no"
        " return: drop the stream back, if one runs, and give the old"
        " source back its writes",
    ],
    "off source": "nothing written at cutover: the operator stops its"
                  " writes and its jobs",
}


#: what a hop's `ship:` does to a full copy (`migkit.ship`): the seed on
#: media, in two halves the same command runs where each server is
SHIP = {
    "what": "a seed shipped on media: the copy cut in two halves, the rows"
            " carried between them on a carrier - a disk, or a provider's"
            " appliance - where the link is too narrow to copy them in"
            " time; taken where the hop's carrier would meet the deadline"
            " and the link would not, or where nothing measured says the"
            " link is faster",
    "source": "read into parts beside it, each table from the position the"
              " change stream starts from - taken before a row is read -"
              " each part in the format the target's own loader reads;"
              " nothing written to it but what the stream itself makes"
              " there (a slot)",
    "carrier": "every part digested as the check digests, compressed,"
               " encrypted to the hop's at_rest recipients and hashed as"
               " stored; a manifest of them all, encrypted and signed; the"
               " seal written last",
    "target": "every part's hash checked before it is read, its rows held"
              " to its digest, loaded by the target's own loader and"
              " proved there; each table then held as a whole",
    "guarantees": "no part loaded that is not the one exported; a part"
                  " that fails is read again from the source over the"
                  " link; the stream starts from the seed's position, and"
                  " where the source no longer keeps its log back to it,"
                  " only the rows that changed since are copied, found"
                  " leaf by leaf - the seed is not shipped again",
    "halves": "move --mode full or full+cdc runs the export where no sealed"
              " seed is found, and the load where one is: run it beside"
              " the source with the carrier attached, ship the carrier,"
              " and run the same command beside the target",
}


def ship_lines(hop):
    """What the seed on media does for this hop, where it names a
    carrier; [] where it does not."""
    got = (hop.options or {}).get("ship")
    if not isinstance(got, dict) or not got:
        return []
    return [f"ship: {SHIP['what']}",
            f"  on the source: {SHIP['source']}",
            f"  on the carrier: {SHIP['carrier']}",
            f"  on the target: {SHIP['target']}",
            f"  it guarantees: {SHIP['guarantees']}",
            f"  {SHIP['halves']}"]


#: what the machines of a hop do together where it says so (`standby`,
#: `share_tables: ranges`), for `init`, `assess`, the dry run and the docs
#: (`machines_lines`): the one place these are written
MACHINES = {
    "standby": {
        "what": "the same move run on two or more machines: one holds the"
                " hop and moves it, the others stand by and take it over"
                " from the last position committed once it stops renewing"
                " its lease (MIGKIT_STANDBY_SECONDS, 15 by default)",
        "source": "read by the machine that holds the hop only; a machine"
                  " standing by reads nothing until it takes over",
        "target": "written by the machine that holds the hop only, every"
                  " write held to the number its lease carries: the target"
                  " refuses a write under an older number, so a machine"
                  " that paused past its term cannot land a batch after the"
                  " next one began; the tables are copied by the table"
                  " copier every pair shares, whose writes carry the"
                  " number",
        "guarantees": "no batch applied twice and none lost across a"
                      " takeover - what a restart on one machine gives;"
                      " which machine wrote which batch kept, and proved"
                      " one writer at a time (watch, the evidence)",
        "needs": "a store every machine writes only over the version it"
                 " read and that keeps files: the run state's bucket"
                 " (state: {backend: s3}) or a directory every machine sees"
                 " (state: {shared: <path>}); and a target that refuses a"
                 " write under an older number, proved before the first"
                 " write (PostgreSQL, MySQL, MariaDB)",
    },
    "ranges": {
        "what": "every machine running the move copies at once: a table"
                " large enough is split once and its ranges taken by every"
                " machine's workers, each machine sized from its own share"
                " of the servers; the other tables are taken whole, one"
                " machine each",
        "source": "read by every machine, each range by the one that took"
                  " it",
        "target": "written by every machine, each range held to the number"
                  " of the claim that took it; a range taken again after"
                  " its machine stopped renewing is emptied and copied"
                  " again, and the older claim's late writes are refused",
        "guarantees": "every range copied and read back by the claim that"
                      " holds it, the table finished by one machine; which"
                      " machine copied which range kept with the evidence",
        "needs": "a store every machine writes only over the version it"
                 " read: the run state's bucket, a directory every machine"
                 " sees, or a table of migkit's own on the target",
    },
}


def machines_lines(hop):
    """What the hop's machines do together, where it says so: [] for a
    hop run on one machine."""
    opts = hop.options or {}
    keys = [k for k, on in (
        ("standby", bool(opts.get("standby"))),
        ("ranges", str(opts.get("share_tables") or "").lower() == "ranges"))
        if on]
    out = []
    for k in keys:
        m = MACHINES[k]
        out += [f"{'standby' if k == 'standby' else 'share_tables: ranges'}:"
                f" {m['what']}",
                f"  on the source: {m['source']}",
                f"  on the target: {m['target']}",
                f"  it guarantees: {m['guarantees']}",
                f"  it needs: {m['needs']}"]
    return out


#: what each of a hop's limits holds the move to (`limits:` and
#: `deadline:`), as the plan says it: every one is a hard limit - the
#: plan keeps to it where any way can, and says what binds where none can
#: - and a hop that sets none is planned for the soonest end
LIMITS = {
    "deadline": "the whole move - copy, index builds and check - planned"
                " to end within it; where no plan can, what binds is said"
                " and what would help",
    "source_load": "the share of the source's reads the move may take:"
                   " the source is planned as if it read only that fast,"
                   " the ways that read it less are preferred where that"
                   " ends sooner, and while it runs the pace holds the"
                   " move's reads at that share",
    "replica_lag": "the seconds the source's replica may fall behind"
                   " while it is read: the pace holds the move to it",
    "money": "what the move may spend on what it carries (at the hop's"
             " transfer_price_per_gb) and on what a warehouse bills: the"
             " ways that bill less are taken first where the budget would"
             " be passed",
    "bytes_billed": "the bytes a warehouse may bill the move for: read"
                    " as money at its price",
    "outage": "the cutover's write outage, seconds: the plan says whether"
              " its predicted outage keeps within it (with cutover: on,"
              " the cutover's own budget is this limit)",
    "target_memory": "the target's memory the move may take: as many"
                     " writers at once as it holds",
    "target_connections": "the target's connections the move may take:"
                          " as many writers at once as they carry",
}


def explain_limits(hop):
    """What the hop's limits hold its move to, a line each - nothing
    where it sets none."""
    opts = getattr(hop, "options", None) or {}
    said = [("deadline", opts.get("deadline"))] + list(
        (opts.get("limits") or {}).items())
    return [f"limit {k} {v}: {LIMITS[k]}" for k, v in said
            if k in LIMITS and v not in (None, "")]


def cutover_on(hop):
    """Whether the hop turns the migkit-driven cutover on: its own
    `cutover:` and nothing else - a goal never does."""
    got = (hop.options or {}).get("cutover")
    return got is not None and got is not False


def origin_dropped(hop):
    """Whether the hop asks for migkit's batch table to be taken off a
    one-way target when its stream is torn down for good
    (`origin_table: drop`); kept otherwise, as it always was."""
    got = str((hop.options or {}).get("origin_table", "keep") or "keep")
    if got.lower() not in ("keep", "drop"):
        raise SystemExit(f"origin_table: {got} - keep, or drop")
    return got.lower() == "drop"


def cutover_lines(hop):
    """What the cutover does on each side for this hop: with `cutover:`,
    what it adds; without it, that no goal turns it on, the steps by hand,
    and what turning it on would add."""
    name = getattr(hop, "name", "HOP")
    back = (" and starts the stream back"
            if (hop.options or {}).get("reverse") == "at_cutover" else "")
    adds = [f"    - {a}" for a in CUTOVER["adds"]]
    if cutover_on(hop):
        return (["cutover: on - migkit runs the cutover itself, as one"
                 " timed path:"] + adds
                + ["  on the source: "
                   + CUTOVER["on source"].format(hop=name)])
    return (["cutover: off - the goal does not turn it on; at the cutover"
             " migkit writes nothing to the source, and the operator does"
             " this by hand, in order:"]
            + [f"    {i}. {s.format(hop=name, back=back)}"
               for i, s in enumerate(CUTOVER["by hand"], 1)]
            + ["  turning it on (options: cutover: true, or its settings)"
               " adds:"] + adds)


def explain_goal(hop, goal=None):
    """The goal said plainly: what it is for, what it does on the source
    and on the target, what it guarantees - and, for a hop that is cut
    over, what `cutover:` on or off means for it."""
    goal = goal or getattr(hop, "goal", "") or DEFAULT_GOAL
    g = GOALS[goal]
    out = [f"goal {goal}: {g['what']}",
           f"  on the source: {g['source']}",
           f"  on the target: {g['target']}",
           f"  it guarantees: {g['guarantees']}"]
    if goal == "cutover" or cutover_on(hop):
        out += ["  " + line for line in cutover_lines(hop)]
    out += ["  " + line for line in ship_lines(hop)]
    out += ["  " + line for line in machines_lines(hop)]
    return out


def explain_mode(command, mode, drop=False):
    """What one mode of `move` or `sync` does on each side, in a line."""
    key = (command, f"{mode} --drop" if drop and mode == "cdc" else mode)
    got = MODES.get(key)
    return f"--mode {key[1]}: {got}" if got else ""

#: options that write, which a hop whose goal writes nothing contradicts
WRITING = ("reverse", "topology", "two_way", "protect_target", "cutover",
           "ship")
#: options that stream, which a one-off copy contradicts
STREAMING = ("reverse", "topology", "two_way")
#: the goals that move the data once
ONCE = ("copy", "subset", "consolidate", "split", "archive")
#: each goal's own setting, read by no other goal
GOAL_OPTION = {"subset": "subset", "consolidate": "consolidate",
               "split": "split", "archive": "archive"}


def goal_options(hop, goal):
    """The options `goal` stands for on this hop."""
    from . import freeze
    out = {}
    for k, v in GOALS[goal]["options"].items():
        if v is SUPPORTED:
            if not freeze.supported(hop):
                continue
            v = True
        out[k] = v
    return out


def apply_goal(hop):
    """The hop's options with its goal's under them - an option the hop
    says itself is kept."""
    if not hop.goal:
        return
    hop.options = {**goal_options(hop, hop.goal), **(hop.options or {})}


def refuse_writes(hop, what):
    """Stops a writing command on a hop whose goal writes nothing."""
    goal = getattr(hop, "goal", "") or ""
    if goal and not GOALS[goal]["writes"]:
        raise SystemExit(
            f"{hop.name}: goal {goal} - {GOALS[goal]['what']} - so {what}"
            " is not run. Change the hop's goal to copy or cutover to"
            " move data.")


def endpoint_keys_for(engine):
    """The option keys a side of this engine reads - an engine on another's
    wire (`engines.WIRE`) reads that one's connection keys too."""
    from .engines import ALIASES, WIRE
    e = ALIASES.get(engine, engine)
    return {k for k, key in ENDPOINT_OPTIONS.items()
            if not key.engines or e in key.engines
            or WIRE.get(e) in key.engines}


# ---- checking ------------------------------------------------------------

@dataclass(frozen=True)
class Problem:
    level: str      # "error" stops everything; "warn" is said by doctor
    where: str
    text: str

    def __str__(self):
        return f"{self.where}: {self.text}"


def _near(word, keys):
    offered = [k for k, v in keys.items() if not v.named_for_a_program]
    got = difflib.get_close_matches(str(word), offered, n=1, cutoff=0.7)
    return f" - did you mean {got[0]}?" if got else ""


KINDS = {"str": (str,), "int": (int,), "number": (int, float),
         "bool": (bool,), "list": (list, tuple), "dict": (dict,)}


def _fits(kind, value):
    if kind == "any" or value is None:
        return True
    for k in kind.split("|"):
        types = KINDS[k]
        if k in ("int", "number") and isinstance(value, bool):
            continue
        if isinstance(value, types):
            return True
        if k in ("int", "number") and isinstance(value, str):
            try:
                (int if k == "int" else float)(value)
                return True
            except ValueError:
                pass
    return False


def _kind_words(kind):
    words = {"str": "text", "int": "a whole number", "number": "a number",
             "bool": "true or false", "list": "a list", "dict": "a mapping"}
    return " or ".join(words[k] for k in kind.split("|"))


def _keys(out, where, got, registry, level="error"):
    """Unknown keys and wrong types of a mapping against a registry."""
    for k, v in got.items():
        key = registry.get(k)
        if key is None:
            out.append(Problem(level, f"{where}.{k}",
                               f"not a key migkit knows{_near(k, registry)}"))
            continue
        if not _fits(key.kind, v):
            out.append(Problem("error", f"{where}.{k}",
                               f"{v!r} - {_kind_words(key.kind)} is"
                               " expected here"))
            continue
        if key.subs is not None and isinstance(v, dict):
            # said, not refused: the keys under an option are read where
            # the option is used, which the registry's test cannot follow
            # as surely as it follows the options themselves
            subs = {s: Key("") for s in key.subs}
            for s in v:
                if s not in subs:
                    out.append(Problem("warn", f"{where}.{k}.{s}",
                                       "not a key migkit knows"
                                       + _near(s, subs)))


def sides(raw):
    """(source engine, target engine, hop engine) a hop's raw mapping
    names, or raises where they disagree - the one reading of it that
    `config` builds the hop from as well."""
    from . import addresses
    from .engines import ALIASES
    ends = {}
    for side in ("source", "target"):
        got = raw.get(side)
        ends[side] = (addresses._scheme(got, side)[1]
                      if addresses.is_address(got) else None)
    declared = raw.get("engine")
    opts = raw.get("options") if isinstance(raw.get("options"), dict) \
        else {}
    src, dst = ends["source"], ends["target"]
    if declared == "hetero" or (declared is None and src and dst
                                and src != dst):
        s = opts.get("source_engine") or src or "mysql"
        t = opts.get("target_engine") or dst or "postgres"
        for said, got, side in ((src, s, "source"), (dst, t, "target")):
            if said and ALIASES.get(got, got) != said:
                raise SystemExit(f"{side}_engine: {got}, and the {side}"
                                 f" address is a {said} one")
        return s, t, "hetero"
    engine = declared or src or dst or "postgres"
    canon = ALIASES.get(engine, engine)
    for said, side in ((src, "source"), (dst, "target")):
        if said and said != canon:
            raise SystemExit(f"engine: {engine}, and the {side} address is"
                             f" a {said} one - leave engine out and the"
                             " two addresses say what the pair is")
    return engine, engine, engine


def check(data):
    """Every problem of a configuration's mapping, errors first."""
    out = []
    if not isinstance(data, dict):
        return [Problem("error", "the file", "is not a mapping of keys")]
    for k in data:
        if k not in ("hops", "access"):
            out.append(Problem("warn", str(k), "read by nothing - only"
                               " `hops` and `access` are (a YAML anchor"
                               " kept here is fine)"))
    # who may do what in the web view, for every hop or a group of them
    # (`web.roles`)
    from .web import roles
    for where, text in roles.problems(data.get("access")):
        out.append(Problem("error", where, text))
    hops = data.get("hops") or {}
    if not isinstance(hops, dict):
        return [Problem("error", "hops", "is not a mapping of hops")]
    for name, raw in hops.items():
        out += check_hop(str(name), raw)
    out += _two_way_pairs(hops)
    return sorted(out, key=lambda p: p.level != "error")


def check_hop(name, raw):
    from . import addresses
    where = f"hops.{name}"
    out = []
    if not isinstance(raw, dict):
        return [Problem("error", where, "is not a mapping")]
    _keys(out, where, raw, HOP_KEYS)
    shape = {f"{where}.{k}" for k in ("engine", "source", "target",
                                        "options")}
    if any(p.level == "error" and p.where in shape for p in out):
        # the rest is read through these
        return out
    try:
        src_e, dst_e, engine = sides(raw)
    except SystemExit as e:
        return out + [Problem("error", where, str(e))]
    from .engines import ALIASES, NAMES
    said = raw.get("engine")
    if said is not None and said not in NAMES and said not in ALIASES:
        near = _near(said, {n: Key("") for n in list(NAMES) + list(ALIASES)})
        out.append(Problem("error", f"{where}.engine",
                           f"{said!r} is not an engine migkit has{near}"))
    goal = raw.get("goal")
    if goal is not None and goal not in GOALS:
        out.append(Problem("error", f"{where}.goal",
                           f"{goal!r} - one of {', '.join(GOALS)}"
                           + _near(goal, {g: Key("") for g in GOALS})))
    opts = raw.get("options") or {}
    _keys(out, f"{where}.options", opts, HOP_OPTIONS)
    if isinstance(opts, dict):
        out += _notify(f"{where}.options.notify", opts.get("notify"))
        out += _egress(f"{where}.options.egress", opts.get("egress"))
        out += _two_way(f"{where}.options.two_way", opts.get("two_way"))
        out += _ddl_problems(f"{where}.options.ddl", opts.get("ddl"))
        from .web import roles
        for w, text in roles.hop_problems(opts.get("access")):
            out.append(Problem("error", f"{where}.options.{w}", text))
    for side, e in (("source", src_e), ("target", dst_e)):
        got = raw.get(side)
        if got is None:
            continue
        if addresses.is_address(got):
            try:
                addresses.parse(got, side)
            except SystemExit as err:
                out.append(Problem("error", f"{where}.{side}", str(err)))
            continue
        if not isinstance(got, dict):
            out.append(Problem("error", f"{where}.{side}",
                               f"{got!r} is neither an address"
                               " (scheme://user@host/database) nor a"
                               " mapping of host, port and user"))
            continue
        out += _check_endpoint(f"{where}.{side}", e, got)
    out += _contradictions(where, raw, opts, goal, engine)
    return out


def _check_endpoint(where, engine, raw):
    from . import addresses
    out = []
    flat = {k: v for k, v in raw.items() if k != "options"}
    nested = raw.get("options")
    if nested is not None and not isinstance(nested, dict):
        return [Problem("error", f"{where}.options", "is not a mapping")]
    flat.update(nested or {})
    registry = {**ENDPOINT_KEYS, **ENDPOINT_OPTIONS}
    registry.pop("options")
    _keys(out, where, flat, registry)
    pw = flat.get("password")
    if isinstance(pw, str) and pw and not addresses.is_reference(pw) \
            and pw != "CHANGE_ME":
        out.append(Problem(
            "warn", f"{where}.password",
            "written in plain text - keep it in this machine's keychain"
            " (`migkit init` does) or write a reference: env:NAME,"
            " file:/path, vault:path#key, keyring:name"))
    for k in ("schema_registry_password",):
        v = flat.get(k)
        if isinstance(v, str) and v and not addresses.is_reference(v):
            out.append(Problem("warn", f"{where}.{k}",
                               "written in plain text - write a reference"
                               " (env:NAME, file:/path, ...)"))
    return out


def _notify(where, got):
    """A receiver written out in the file, which is its own password (a
    webhook's address is enough to post to it); an event nobody tells; a
    lag that is not a number of seconds."""
    from . import addresses
    from .notify import EVENTS
    out = []
    if got is None:
        return out
    to = got.get("to") if isinstance(got, dict) else got
    if isinstance(got, dict):
        for e in got.get("events") or []:
            if str(e) not in EVENTS:
                out.append(Problem("error", f"{where}.events",
                                   f"{e!r} - one of {', '.join(EVENTS)}"
                                   + _near(e, {x: Key("") for x in EVENTS})))
        lag = got.get("lag")
        if lag is not None and not _fits("number", lag):
            out.append(Problem("error", f"{where}.lag",
                               f"{lag!r} - the seconds a change tail may be"
                               " behind before it is said"))
        detail = got.get("detail")
        if detail is not None and not isinstance(detail, bool):
            out.append(Problem("error", f"{where}.detail",
                               f"{detail!r} - true sends an error's words,"
                               " false (the default) only its kind"))
    plain = 0
    for r in [to] if isinstance(to, str) else list(to or []):
        r = str(r)
        key = r.split(":", 1)[1] if r.startswith("pagerduty:") else r
        plain += not addresses.is_reference(key)
    if plain:
        out.append(Problem(
            "warn", where,
            f"{plain} receiver{'s are' if plain > 1 else ' is'} written out"
            " in the file, and an address is its own password - keep it in"
            " this machine's keychain or write a reference: env:NAME,"
            " keyring:name, file:/path, vault:path#key"))
    return out


def _egress(where, got):
    """A kind of send migkit does not have, or a detail that is not
    true or false."""
    from . import egress
    if got is None:
        return []
    allow = got.get("allow") if isinstance(got, dict) else got
    try:
        egress._listed(allow, where)
    except SystemExit as e:
        return [Problem("error", where, str(e).split(": ", 1)[-1])]
    return []


def _two_way(where, got):
    """A two-way hop's rules read whole (`twoway.rules`): a rule migkit
    does not know, a `newest` with no column, a group settled by a value
    it does not name, `source_priority` with no ranks - before anything
    runs rather than when the tail starts."""
    from . import twoway
    if not isinstance(got, dict):
        return []
    try:
        twoway.Rules(got, twoway._ranks_of(got))
    except SystemExit as e:
        return [Problem("error", where, str(e))]
    return []


def _ddl_problems(where, got):
    """The hop's `ddl` said wrong (`tailtables.policy`, `skipped`): a word
    that is not stop, park or apply - for every table, the default, or a
    table or pattern - and a kind `skip` cannot leave out. Said before
    anything runs rather than when the tail meets its first DDL."""
    from .tailtables import POLICIES, SKIPPABLE
    if got is None:
        return []
    out = []
    if not isinstance(got, dict):
        if str(got).strip().lower() not in POLICIES:
            out.append(Problem("error", where,
                               f"{got!r} - one of {', '.join(POLICIES)}"))
        return out
    for k, v in got.items():
        if k == "skip":
            words = [v] if isinstance(v, str) else v
            if not isinstance(words, (list, tuple)):
                out.append(Problem("error", f"{where}.skip",
                                   "a list of the kinds left out"))
                continue
            for w in words:
                if str(w).strip().lower() not in SKIPPABLE:
                    out.append(Problem(
                        "error", f"{where}.skip",
                        f"{w!r} - one of {', '.join(SKIPPABLE)}: an ALTER"
                        " or a rename left out would have every change"
                        " after it written where the target has no column"
                        " or table for it"))
            continue
        if not isinstance(v, str) or v.strip().lower() not in POLICIES:
            out.append(Problem("error", f"{where}.{k}",
                               f"{v!r} - one of {', '.join(POLICIES)}"))
    return out


def _scope_apart(ra, rb):
    """What one hop takes that its way back does not, in words; "" where
    the two take the same tables. The way back's `include` and `exclude`
    are read in the first's names (`Hop.reversed` reads them the other
    way), so a pair that names one table on each side as the database it
    lands in is the same scope."""
    from .config import Endpoint, Hop
    try:
        a = Hop(name="", engine="", source=Endpoint(), target=Endpoint(),
                include=_patterns_or_none(ra.get("include")),
                exclude=_patterns_or_none(ra.get("exclude")),
                db_map=dict(ra.get("db_map") or {}))
        back = a.reversed()
    except Exception:  # noqa: BLE001 - each hop's own check says it
        return ""
    for k in ("include", "exclude"):
        there = sorted(str(p) for p in getattr(back, k) or [])
        theirs = sorted(_patterns_or_none(rb.get(k)))
        if there != theirs:
            return (f"one takes {k} {there or 'nothing'} in the other's"
                    f" names and the other {theirs or 'nothing'} - a change"
                    " to a table only one of them takes is carried one way"
                    " and never back")
    return ""


def _two_way_pairs(hops):
    """Two hops of the file that are each other's way back, both two-way:
    every conflict is settled at both ends, so a rule that would settle
    one apart - `overwrite` both ways, ranks turned the same way on both -
    leaves the two sides different for good (`twoway.apart`)."""
    from . import twoway
    both = [(str(n), r) for n, r in hops.items() if isinstance(r, dict)
            and isinstance(r.get("options"), dict)
            and r["options"].get("two_way")]
    out = []
    for i, (a, ra) in enumerate(both):
        for b, rb in both[i + 1:]:
            if ra.get("source") is None or ra.get("source") != \
                    rb.get("target") or ra.get("target") != rb.get("source"):
                continue
            ga, gb = (r["options"]["two_way"] for r in (ra, rb))
            for said in twoway.apart(ga if isinstance(ga, dict) else {},
                                     gb if isinstance(gb, dict) else {}):
                out.append(Problem("error", f"hops.{a} and hops.{b}",
                                   f"each is the other's way back: {said}"))
            # and the same tables both ways
            said = _scope_apart(ra, rb)
            if said:
                out.append(Problem("error", f"hops.{a} and hops.{b}",
                                   f"each is the other's way back: {said}"))
    return out


def _ops_problems(where, mapping):
    """`mapping.ops` said wrong: not a mapping, a word that is not a
    change, or a table that carries nothing at all - which is a table
    left out, and said as one (`exclude`)."""
    from .config import parse_ops
    rules = mapping.get("ops") if isinstance(mapping, dict) else None
    if rules is None:
        return []
    if not isinstance(rules, dict):
        return [Problem("error", f"{where}.mapping.ops", "is not a mapping")]
    out = []
    for table, value in rules.items():
        try:
            got = parse_ops(value)
        except SystemExit as e:
            out.append(Problem("error", f"{where}.mapping.ops.{table}",
                               str(e)))
            continue
        if not got:
            out.append(Problem(
                "error", f"{where}.mapping.ops.{table}",
                "carries no change at all - leave the table out with"
                " exclude instead"))
    return out


def _patterns_or_none(value):
    """`exclude` as patterns, or none where it is said wrong - which its
    own check says."""
    from .config import scope_patterns
    try:
        return scope_patterns(value)
    except SystemExit:
        return []


def _include_problems(where, raw, probe):
    """`include` said wrong, or said against the rest of the hop: an
    entry that is not a name, one `exclude` takes back whole, a mapping
    rule for a table it does not take. Each is a table the operator
    believes is moved and checked while nothing looks at it."""
    from .config import name_matches, scope_patterns
    out = []
    if isinstance(raw.get("exclude"), dict):
        try:
            scope_patterns(raw["exclude"])
        except SystemExit as e:
            out.append(Problem("error", f"{where}.exclude", str(e)))
    got = raw.get("include")
    if got is None or not isinstance(got, (list, dict)):
        # its type is said with every other key's
        return out
    try:
        got = scope_patterns(got)
        excluded = scope_patterns(raw.get("exclude"))
    except SystemExit as e:
        return [Problem("error", f"{where}.include", str(e))]
    fold = probe.folds_case()
    for i, p in enumerate(got):
        if not isinstance(p, str) or not p.strip() or p.strip() != p \
                or ".." in p or p.startswith(".") or p.endswith("."):
            out.append(Problem("error", f"{where}.include[{i}]",
                               f"{p!r} is not a dotted name"))
            continue
        if any(name_matches(p, x, fold) for x in excluded):
            out.append(Problem(
                "error", f"{where}.include[{i}]",
                f"{p} is excluded as a whole as well, so it takes nothing"
                " - drop one of the two"))
    probe.include = [p for p in got if isinstance(p, str) and p.strip()]
    if not probe.include:
        return out
    whole = probe.whole_name()
    for part in ("where", "tables", "columns", "tolerance", "ops"):
        rules = (raw.get("mapping") or {}).get(part) or {}
        if not isinstance(rules, dict):
            continue
        for table in rules:
            if any(c in str(table) for c in "*?["):
                continue
            names = str(table).split(".")
            # only a rule that names its database says which table it is;
            # a shorter one may be any database's, one include takes
            if whole and len(names) == whole and \
                    not probe.included(*names):
                out.append(Problem(
                    "error", f"{where}.mapping.{part}.{table}",
                    f"{table} is not among what include takes, so this rule"
                    " touches nothing - add it to include or drop the rule"))
    return out


def _contradictions(where, raw, opts, goal, engine):
    from .config import Endpoint, Hop
    out = []
    probe = Hop(name="", engine=engine, source=Endpoint(),
                target=Endpoint(),
                exclude=_patterns_or_none(raw.get("exclude")),
                options={k: v for k, v in opts.items()
                         if k == "source_engine"})
    mapping = raw.get("mapping") or {}
    for part in ("where", "tables", "columns"):
        rules = mapping.get(part) or {}
        if not isinstance(rules, dict):
            out.append(Problem("error", f"{where}.mapping.{part}",
                               "is not a mapping"))
            continue
        for table in rules:
            if probe.excluded(*str(table).split(".")):
                out.append(Problem(
                    "error", f"{where}.mapping.{part}.{table}",
                    f"{table} is also excluded, so this rule touches"
                    " nothing - drop one of the two"))
    out += _include_problems(where, raw, probe)
    out += _ops_problems(where, mapping)
    if goal in GOALS and not GOALS[goal]["writes"]:
        for k in WRITING:
            if opts.get(k):
                out.append(Problem(
                    "error", f"{where}.options.{k}",
                    f"goal {goal} writes to neither side, and {k} writes"
                    " - drop one of the two"))
    if goal in ONCE:
        for k in STREAMING:
            if opts.get(k):
                out.append(Problem(
                    "error", f"{where}.options.{k}",
                    f"goal {goal} moves the data once, and {k} keeps a"
                    " stream running - make the goal cutover or two-way,"
                    " or drop the option"))
    out += _goal_settings(where, opts, goal, engine)
    out += _source_changes(where, opts, goal)
    out += _ship(where, opts)
    share = opts.get("share_tables")
    if isinstance(share, str) and share.lower() != "ranges":
        out.append(Problem("error", f"{where}.options.share_tables",
                           f"{share!r} - true, or ranges"))
    from .interlock import CHOICES
    rows = opts.get("target_rows")
    if rows is not None and str(rows).lower() not in CHOICES:
        out.append(Problem("error", f"{where}.options.target_rows",
                           f"{rows!r} - one of {', '.join(CHOICES)}"))
    if opts.get("production_target") and not opts.get("production"):
        out.append(Problem(
            "warn", f"{where}.options.production_target",
            "names a production server to write to, and no production"
            " list says which servers are production, so it opens"
            " nothing - add production: [host patterns]"))
    dbs = [str(d) for d in (raw.get("databases") or [])]
    for src in (raw.get("db_map") or {}):
        if dbs and str(src) not in dbs:
            out.append(Problem(
                "warn", f"{where}.db_map.{src}",
                f"{src} is not one of the hop's databases"
                f" ({', '.join(dbs)}), so this rename is used by nothing"))
    if raw.get("databases") is not None and not isinstance(
            raw.get("databases"), (list, tuple)):
        out.append(Problem("error", f"{where}.databases",
                           "is not a list"))
    return out


def _goal_settings(where, opts, goal, engine):
    """The settings a goal reads, held to the goal: the rows a subset
    starts from, the hop a rehearsal rehearses, and each said as doing
    nothing on a hop whose goal does not read it."""
    out = []
    name = where.split(".", 1)[-1]
    subset = opts.get("subset")
    if goal == "subset":
        start = subset.get("start") if isinstance(subset, dict) else None
        if not isinstance(start, dict) or not start:
            out.append(Problem(
                "error", f"{where}.options.subset",
                "goal subset copies the rows chosen and every row they"
                " refer to, and no rows are chosen - subset: {start:"
                " {table: a condition, a share like 10% or a number of"
                " rows}}"))
    for g, k in GOAL_OPTION.items():
        if g != goal and k != "subset" and opts.get(k) is not None:
            out.append(Problem("warn", f"{where}.options.{k}",
                               f"read only by a hop whose goal is {g}, so"
                               " it does nothing here"))
    if subset is not None and goal != "subset":
        out.append(Problem("warn", f"{where}.options.subset",
                           "read only by a hop whose goal is subset, so it"
                           " does nothing here"))
    out += _scenario_settings(where, opts, goal)
    said = opts.get("rehearses")
    rehearsed = (said.get("hop") if isinstance(said, dict) else said) or ""
    if goal == "rehearse":
        if not rehearsed:
            out.append(Problem(
                "warn", f"{where}.options.rehearses",
                "goal rehearse and no hop named as the one it rehearses:"
                " what it measures is taken by any hop of the same"
                " engines, and none is forecast from it by name"))
        elif str(rehearsed) == name:
            out.append(Problem("error", f"{where}.options.rehearses",
                               "a hop does not rehearse itself - name the"
                               " hop of the real run"))
    elif said is not None:
        out.append(Problem("warn", f"{where}.options.rehearses",
                           "read only by a hop whose goal is rehearse, so"
                           " it does nothing here"))
    if goal == "upgrade" and engine == "hetero":
        out.append(Problem(
            "error", f"{where}.goal",
            "goal upgrade is one engine at a newer version, and this hop"
            " joins two engines - a move between engines is goal cutover"))
    physical = opts.get("physical")
    if isinstance(physical, dict) and physical.get("upgrade") is not None \
            and goal != "upgrade":
        out.append(Problem("warn", f"{where}.options.physical.upgrade",
                           "read only by a hop whose goal is upgrade, so it"
                           " does nothing here"))
    return out


def _scenario_settings(where, opts, goal):
    """The settings of the scenarios a hop may say, each read as its own
    module reads it (`charsets`, `tableengine`, `consolidate`, `split`,
    `archive`) - one reading, so what loads is what runs - and what a
    goal needs of its own: a consolidation's column."""
    from types import SimpleNamespace

    from . import archive, charsets, consolidate, split, tableengine
    out = []
    probe = SimpleNamespace(goal=goal, options=opts)
    for key, read in (("charset", charsets.settings),
                      ("table_engine", tableengine.settings),
                      ("consolidate", consolidate.settings),
                      ("split", split.settings),
                      ("archive", archive.settings)):
        try:
            read(probe)
        except SystemExit as e:
            out.append(Problem("error", f"{where}.options.{key}", str(e)))
    if goal == "consolidate":
        got = opts.get("consolidate")
        if not isinstance(got, dict) or not got.get("column"):
            out.append(Problem(
                "error", f"{where}.options.consolidate",
                "goal consolidate holds each source's rows under a column,"
                " and none is named - consolidate: {column: tenant_id,"
                " value: <this source's>}"))
        elif str(got.get("keys") or "") == "offset" and not got.get(
                "offsets"):
            out.append(Problem(
                "error", f"{where}.options.consolidate.offsets",
                "keys: offset shifts each source's keys by its own offset,"
                " and none is given - offsets: {<value>: <number>}, or"
                " keys: remap for migkit to work them out"))
    got = opts.get("archive")
    if goal == "archive" and isinstance(got, dict) and got.get("remove"):
        answers = opts.get("source_changes")
        named = isinstance(answers, dict) and "archived_rows" in answers
        if not named:
            out.append(Problem(
                "warn", f"{where}.options.archive.remove",
                "the archived rows are removed only with the source's own"
                " answer by its name, and the hop gives none ahead - a"
                " terminal asks it under --go; without one they stay"
                " (source_changes: {archived_rows: yes})"))
    return out


def _ship(where, opts):
    """The seed on media's settings held before anything runs: a carrier
    migkit knows, reached the way that carrier is, a shipping time and a
    capacity it can read, and recipients to seal the parts to - a carrier
    travels."""
    from . import carriers, machine
    got = opts.get("ship")
    if got is None:
        return []
    at = f"{where}.options.ship"
    if not isinstance(got, dict):
        return [Problem("error", at, "is not a mapping")]
    out = []
    kind = str(got.get("carrier") or "").lower()
    if kind not in carriers.KINDS:
        out.append(Problem("error", f"{at}.carrier",
                           f"{got.get('carrier')!r} - one of"
                           f" {', '.join(carriers.KINDS)}"))
    elif not (got.get("path") or got.get("endpoint")):
        out.append(Problem("error", at,
                           f"{carriers.SAID[kind]} is reached through a"
                           " directory here (path:) or an endpoint"
                           " (endpoint:), and the hop gives neither"))
    elif kind == "databox" and got.get("path"):
        why = carriers._share_problem(str(got["path"]))
        if why:
            out.append(Problem("error", f"{at}.path", why))
    if got.get("shipping") is not None and \
            machine.duration(got["shipping"]) is None:
        out.append(Problem("error", f"{at}.shipping",
                           f"{got['shipping']!r} - a duration (7d, 36h)"))
    if got.get("capacity") is not None and \
            carriers.size_of(got["capacity"]) is None:
        out.append(Problem("error", f"{at}.capacity",
                           f"{got['capacity']!r} - a size (80TB, 120TiB)"))
    rest = opts.get("at_rest")
    if not (isinstance(rest, dict) and rest.get("recipients")):
        out.append(Problem("error", at,
                           "a carrier travels, and its parts are sealed to"
                           " the hop's at_rest recipients - this hop names"
                           " none (options at_rest: {recipients: [...]})"))
    if got.get("shipping") is None:
        out.append(Problem("warn", f"{at}.shipping",
                           "not given: how long the source keeps its log"
                           " for the stream is not held against the time"
                           " the carrier takes, and the plan cannot time"
                           " the seed"))
    return out


def _source_changes(where, opts, goal):
    """The answers a hop gives ahead to changes on the source: each yes,
    no or dba, each named change one migkit knows - and a yes on a hop
    whose goal writes nothing is a contradiction."""
    from .sourcechange import DEFINITIONS, answer_word
    out = []
    raw = opts.get("source_changes")
    given = []
    if isinstance(raw, dict):
        for name, v in raw.items():
            given.append((f"{where}.options.source_changes.{name}", v))
            if name not in DEFINITIONS:
                near = _near(name, {d: Key("") for d in DEFINITIONS})
                out.append(Problem("warn",
                                   f"{where}.options.source_changes.{name}",
                                   "not a change migkit knows" + near))
    elif raw is not None:
        given.append((f"{where}.options.source_changes", raw))
    phys = opts.get("physical")
    if isinstance(phys, dict) and "source_plugin" in phys:
        given.append((f"{where}.options.physical.source_plugin",
                      phys["source_plugin"]))
    for at, v in given:
        answer = answer_word(v)
        if answer is None:
            out.append(Problem("error", at, f"{v!r} - yes, no or dba"))
        elif answer == "yes" and goal in GOALS and not GOALS[goal]["writes"]:
            out.append(Problem("error", at,
                               f"goal {goal} writes to neither side, and a"
                               " yes lets migkit change the source - drop"
                               " one of the two"))
    return out


# ---- what doctor says of a hop -------------------------------------------

def said_by_doctor(hop, raw):
    """(key, sentence) for every key of this hop that does nothing here,
    or sets what migkit would otherwise decide."""
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    opts = hop.options or {}
    if engine == "hetero":
        on = {ALIASES.get(s, s) for s in (opts.get("source_engine", "mysql"),
              opts.get("target_engine", "postgres"))} | {"hetero"}
    else:
        on = {engine}
    out = []
    for k in HOP_KEYS:
        key = HOP_KEYS[k]
        if k not in raw or not key.knob:
            continue
        v = raw[k]
        if key.knob == "nothing":
            out.append((k, f"{k}: {v} - read by nothing in migkit; remove"
                           " it"))
        elif key.knob == "ceiling":
            out.append((k, f"{k}: {v} - a ceiling: migkit works the number"
                           " out from both servers and this machine as it"
                           f" runs, and never goes past {v}. Left out, it"
                           " has no ceiling but the servers'"))
        elif key.knob == "size":
            out.append((k, f"{k}: {v} - {key.what}, which migkit otherwise"
                           " decides; leave it out unless its own"
                           " choice failed you"))
    for k, v in (raw.get("options") or {}).items():
        key = HOP_OPTIONS.get(k)
        if key is None:
            continue
        if key.engines and not (on & set(key.engines)):
            out.append((f"options.{k}",
                        f"options.{k} - read only by {' and '.join(key.engines)}"
                        f" hops, so it does nothing on this"
                        f" {hop.engine} hop"))
        elif key.knob == "way" and v is False:
            out.append((f"options.{k}",
                        f"options.{k}: false - turns off one of the ways"
                        f" migkit may choose ({key.what}); left out,"
                        " migkit chooses"))
        elif key.knob == "size":
            out.append((f"options.{k}",
                        f"options.{k}: {v} - {key.what}, which migkit"
                        " otherwise decides"))
    for side in ("source", "target"):
        ep = getattr(hop, side)
        named = opts.get(f"{side}_engine") or (
            "mysql" if side == "source" else "postgres")
        e = ALIASES.get(named, named) if engine == "hetero" else engine
        for k in (ep.options or {}):
            key = ENDPOINT_OPTIONS.get(k)
            if key is not None and key.engines and e not in key.engines:
                out.append((f"{side}.{k}",
                            f"{side}.{k} - read only by"
                            f" {' and '.join(key.engines)} sides, so it"
                            f" does nothing on this {e} one"))
    return out


# ---- the smallest configuration ------------------------------------------

def decisions(hop):
    """What a hop decides, in a form two configurations can be compared
    by: the smallest configuration must load into the same as the long
    one. `service` "" and "native" are one playbook; `big_rows` is read
    by nothing; a hetero hop's side engines are said even where the long
    form leaves them to their defaults; an identity rename is none."""
    opts = dict(hop.options or {})
    if hop.engine == "hetero":
        opts.setdefault("source_engine", "mysql")
        opts.setdefault("target_engine", "postgres")

    def side(ep):
        return {"host": ep.host, "port": ep.port, "user": ep.user,
                "password": ep.password, "options": dict(ep.options or {}),
                "class": type(ep).__name__.replace("Keychain", "")}
    return {"engine": hop.engine, "source": side(hop.source),
            "target": side(hop.target),
            "databases": [str(d) for d in hop.databases or []],
            "exclude": list(hop.exclude or []),
            "include": list(getattr(hop, "include", None) or []),
            "service": hop.service or "native", "slice": hop.slice,
            "workers": hop.workers if hop.workers_set else None,
            "options": opts,
            "db_map": {k: v for k, v in (hop.db_map or {}).items()
                       if k != v},
            "mapping": hop.mapping or {}, "goal": hop.goal or ""}


def smallest(name, raw):
    """The fewest keys that load into the same decisions as `raw`: each
    side as an address where an address can say all of it (its engine,
    port, database and TLS in it, the hop's engine and database list
    then left out), and every key equal to what migkit would decide on
    its own dropped."""
    from . import addresses
    from .config import DEFAULT_PORTS, keychain_name
    raw = dict(raw)
    src_e, dst_e, engine = sides(raw)
    opts = dict(raw.get("options") or {})
    dbs = [str(d) for d in raw.get("databases") or []]
    db_map = {str(k): str(v) for k, v in (raw.get("db_map") or {}).items()
              if str(k) != str(v)}
    out = {}
    if raw.get("goal"):
        out["goal"] = raw["goal"]
    rendered = {}
    target_dbs = [db_map.get(d, d) for d in dbs]
    maps_known = all(k in dbs for k in db_map)
    for side, e, names in (("source", src_e, dbs),
                           ("target", dst_e, target_dbs)):
        got = raw.get(side)
        if addresses.is_address(got):
            got = addresses.parse(got, side)[1]
        if not isinstance(got, dict):
            rendered[side] = None
            continue
        flat = {k: v for k, v in got.items() if k != "options"}
        flat.update(got.get("options") or {})
        if flat.get("password") == f"keyring:{keychain_name(name, side)}":
            flat.pop("password")
        if engine != "hetero" and not flat.get("port"):
            # the long form's side took its engine's port where it named
            # none (a cross-engine hop's took none at all)
            flat["port"] = DEFAULT_PORTS.get(e, 0)
        from .engines import ALIASES
        canon = ALIASES.get(e, e)
        rendered[side] = addresses.render(
            canon, flat, names if maps_known else [],
            DEFAULT_PORTS.get(e, 0))
        if canon != e:
            rendered[side] = None
    whole = rendered["source"] and rendered["target"]
    for side in ("source", "target"):
        out[side] = rendered[side] if whole else raw.get(side)
    if not whole:
        if raw.get("engine"):
            out["engine"] = raw["engine"]
        if dbs:
            out["databases"] = dbs
        if db_map:
            out["db_map"] = db_map
    else:
        if not maps_known and db_map:
            out["db_map"] = db_map
        if engine == "hetero":
            opts.pop("source_engine", None)
            opts.pop("target_engine", None)
    if raw.get("service") not in (None, "", "native"):
        out["service"] = raw["service"]
    if "workers" in raw:
        out["workers"] = raw["workers"]
    if "slice" in raw and int(raw["slice"]) != 1_000_000:
        out["slice"] = raw["slice"]
    for k in ("exclude", "include", "mapping"):
        if raw.get(k):
            out[k] = raw[k]
    if opts:
        out["options"] = opts
    return out
