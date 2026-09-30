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
* `GOALS` - the one word a hop may give for what it wants, and the
  options each word stands for.
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
    "goal": Key("what the hop is for - copy, cutover, verify, two-way or"
                " keep-in-sync", "str"),
    "engine": Key("the engine both sides run, or hetero - read from the"
                  " addresses when they are addresses", "str"),
    "source": Key("where the data is: an address or the long form",
                  "str|dict"),
    "target": Key("where it goes: an address or the long form",
                  "str|dict"),
    "databases": Key("the source databases to move, all of them when left"
                     " out", "list"),
    "exclude": Key("databases, schemas or tables left alone", "list"),
    "service": Key("the playbook `migkit advise` shows", "str"),
    "big_rows": Key("read by nothing", "int", knob="nothing"),
    "slice": Key("the rows one compared range of a large table holds",
                 "int", knob="size"),
    "workers": Key("the most workers migkit's own sizing may reach",
                   "int", knob="ceiling"),
    "options": Key("the hop's options", "dict"),
    "db_map": Key("source database name -> target database name", "dict"),
    "mapping": Key("renames, column rules and row filters", "dict",
                   subs=("tables", "columns", "where")),
}

#: the keys under a hop's `options`
HOP_OPTIONS = {
    "accept_changes": Key("table.column patterns whose values may arrive"
                          " as the target keeps them", "list",
                          ("hetero",)),
    "access": Key("who may use the dashboard's pages", "dict",
                  subs=("operator", "viewer")),
    "app_roles": Key("the application's roles protect_target holds",
                     "list"),
    "approvals": Key("the steps that wait for an approval", "dict",
                     subs=("steps", "signers", "count", "expires")),
    "at_rest": Key("who may read the files holding values", "dict",
                   subs=("recipients",)),
    "atlas": Key("the structural schema reading, on or off", "bool",
                 ("postgres", "mysql"), knob="way",
                 named_for_a_program=True),
    "checksum": Key("text or jsonb row rendering", "str", ("postgres",)),
    "checksum_workers": Key("tables checksummed at once", "int",
                            ("postgres",), knob="size"),
    "code_dir": Key("where the operator's own versions of routines are"
                    " kept, one <name>.sql each", "str", ("hetero",)),
    "code_model": Key("the model a routine no converter proves is asked"
                      " of", "dict", ("hetero",),
                      subs=("provider", "url", "model")),
    "code_model_rounds": Key("rounds the model gets on one routine", "int",
                             ("hetero",), knob="size"),
    "create_streams": Key("create a missing stream", "bool", ("kinesis",)),
    "cutover": Key("the cutover as one timed path that freezes the source"
                   " and undoes itself (`migkit.cutover`)", "dict|bool",
                   subs=("budget", "start_below", "freeze", "pooler",
                         "flip", "flip_undo", "keys", "jobs")),
    "deep": Key("compare every key's value, not a sample", "bool",
                ("redis",)),
    "exclude_schema": Key("schemas left out of the schema dump", "str",
                          ("postgres",)),
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
    "notify": Key("who is told a verdict", "str|list"),
    "on_conflict": Key("source-wins or keep-target", "str"),
    "partition_by": Key("the column a stream partitions by", "str",
                        STREAMS),
    "performance": Key("gate: a slower target fails the check", "str"),
    "proof_sandbox": Key("the two scratch servers stored code is run and"
                         " compared on, or docker for migkit to start"
                         " them", "str|dict", ("hetero",),
                         subs=("source", "target", "docker")),
    "protect_target": Key("the application may not write the target"
                          " while migkit moves into it", "bool"),
    "pt_apply": Key("the online schema change path, on or off", "bool",
                    ("mysql",), knob="way", named_for_a_program=True),
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
    "settle": Key("seconds a re-check waits for a stream to settle",
                  "int"),
    "share_tables": Key("machines share one move's tables", "bool"),
    "slot": Key("the slot a pipeline migkit did not set up reads, which"
                " the fence waits for", "str|list", ("postgres",)),
    "shards": Key("a created stream's shards", "int", ("kinesis",)),
    "snapshot_limit": Key("how old a snapshot may get", "number",
                          ("postgres",)),
    "source_engine": Key("the source's engine of a hetero hop", "str",
                         ("hetero",)),
    "state": Key("where run state is kept", "dict",
                 subs=("backend", "bucket", "prefix", "region",
                       "endpoint_url", "mirror")),
    "tables": Key("the tables of a generic hop", "list", ("generic",)),
    "target_engine": Key("the target's engine of a hetero hop", "str",
                         ("hetero",)),
    "target_has_replicas": Key("false: the target has no replicas and no"
                               " restore replays its binlog, so a load"
                               " may leave the binlog off", "bool",
                               ("mysql",)),
    "tombstones": Key("carry deletes' empty messages", "bool", ("kafka",)),
    "topic": Key("the stream name template", "str", STREAMS),
    "topology": Key("one_way or two_way", "str"),
    "transfer_price_per_gb": Key("what a carried gigabyte costs",
                                 "number"),
    "two_readings": Key("a second, independent reading of every table"
                        " that differs", "bool"),
    "two_way": Key("migkit's own tails both ways", "dict|bool",
                   subs=("on_conflict", "column", "source_rank",
                         "target_rank", "delta")),
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
                ("postgres", "mysql")),
    "aws_region": Key("the region an aws_iam token is signed for", "str",
                      ("postgres", "mysql")),
    "aws_role_arn": Key("the role an aws_iam token is signed as", "str",
                        ("postgres", "mysql")),
    "sslmode": Key("TLS: disable ... verify-full", "str", PG_FAMILY),
    "sslrootcert": Key("the authority's certificate file", "str",
                       PG_FAMILY),
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
                ("sqlite", "duckdb", "parquet")),
    "url": Key("the whole connection address", "str",
               ("generic", "opensearch", "parquet")),
    "secure": Key("TLS", "bool", ("clickhouse",)),
    "service": Key("the service to connect to", "str", ("oracle",)),
    "service_name": Key("the service to connect to", "str", ("oracle",)),
    "database": Key("the database to connect to", "str",
                    ("db2", "redshift", "snowflake", "bigquery")),
    "account": Key("the account identifier", "str", ("snowflake",)),
    "warehouse": Key("the warehouse queries run in", "str",
                     ("snowflake",)),
    "role": Key("the role queries run as", "str", ("snowflake",)),
    "location": Key("the datasets' region", "str", ("bigquery",)),
    "credentials_file": Key("a service account's key file", "str",
                            ("bigquery", "pubsub")),
    "project": Key("the project", "str", ("pubsub",)),
    "region": Key("the region", "str", ("dynamodb", "kinesis", "parquet")),
    "endpoint_url": Key("an endpoint of its own (a local one, a"
                        " compatible store)", "str",
                        ("dynamodb", "kinesis", "parquet")),
    "table_prefix": Key("the table names' template", "str",
                        ("dynamodb",)),
    "index_prefix": Key("the index names' template", "str",
                        ("opensearch",)),
    "tls": Key("TLS, the certificate checked against the system's"
               " authorities (https on OpenSearch)", "bool",
               ("opensearch",) + TLS_OWN),
    "tls_ca_file": Key("the authorities' certificates file", "str",
                       TLS_OWN),
    "tls_cert_file": Key("a client certificate file", "str", TLS_OWN),
    "tls_key_file": Key("a client key file", "str", ("redis", "cassandra")),
    "tls_crl_file": Key("a revocation list file", "str", ("mongodb",)),
    "tls_insecure": Key("TLS with nothing checked", "bool", TLS_OWN),
    "encrypt": Key("require, request or off", "str|bool", ("mssql",)),
    "redis_product": Key("community, cloud or software: the Redis product"
                         " the target is, where its name cannot say",
                         "str", ("redis",)),
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
    "tunnel": Key("reach the server through a tunnel", "str|dict"),
    "tunnel_to": Key("what carries the connection, for the report",
                     "str"),
}

#: each goal: what it is, the options it stands for (the hop's own win),
#: whether it writes at all, and the commands that do it. A value of
#: `SUPPORTED` is set only where the engine can do it (`freeze.supported`)
SUPPORTED = object()
GOALS = {
    "copy": {"what": "move the data once and prove every row arrived",
             "options": {}, "writes": True,
             "next": ["migkit move {hop} --mode full",
                      "migkit move {hop} --mode full --go",
                      "migkit check {hop}"]},
    "cutover": {"what": "move, follow the source's changes, prove both"
                        " equal, and switch with the least downtime - a"
                        " stream back ready from the moment of the switch",
                "options": {"reverse": "at_cutover",
                            "protect_target": SUPPORTED},
                "writes": True,
                "next": ["migkit sync {hop} --mode migrate",
                         "migkit sync {hop} --mode migrate --go",
                         "migkit watch {hop}"]},
    "verify": {"what": "prove two databases hold the same; nothing is"
                       " written to either",
               "options": {}, "writes": False,
               "next": ["migkit check {hop}"]},
    "two-way": {"what": "both sides written, each side's changes carried"
                        " to the other",
                "options": {"topology": "two_way", "two_way": True},
                "writes": True,
                "next": ["migkit move {hop} --mode full+cdc",
                         "migkit move {hop} --mode full+cdc --go"]},
    "keep-in-sync": {"what": "a copy that follows the source for as long"
                             " as it runs, the application kept off it",
                     "options": {"protect_target": SUPPORTED},
                     "writes": True,
                     "next": ["migkit sync {hop} --mode migrate",
                              "migkit sync {hop} --mode migrate --go"
                              " --serve"]},
}

#: options that write, which a hop whose goal writes nothing contradicts
WRITING = ("reverse", "topology", "two_way", "protect_target", "cutover")
#: options that stream, which a one-off copy contradicts
STREAMING = ("reverse", "topology", "two_way")


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
    """The option keys a side of this engine reads."""
    from .engines import ALIASES
    e = ALIASES.get(engine, engine)
    return {k for k, key in ENDPOINT_OPTIONS.items()
            if not key.engines or e in key.engines}


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
        if k != "hops":
            out.append(Problem("warn", str(k), "read by nothing - only"
                               " `hops` is (a YAML anchor kept here is"
                               " fine)"))
    hops = data.get("hops") or {}
    if not isinstance(hops, dict):
        return [Problem("error", "hops", "is not a mapping of hops")]
    for name, raw in hops.items():
        out += check_hop(str(name), raw)
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


def _contradictions(where, raw, opts, goal, engine):
    from .config import Endpoint, Hop
    out = []
    probe = Hop(name="", engine=engine, source=Endpoint(),
                target=Endpoint(),
                exclude=[str(p) for p in (raw.get("exclude") or [])])
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
    if goal in GOALS and not GOALS[goal]["writes"]:
        for k in WRITING:
            if opts.get(k):
                out.append(Problem(
                    "error", f"{where}.options.{k}",
                    f"goal {goal} writes to neither side, and {k} writes"
                    " - drop one of the two"))
    if goal == "copy":
        for k in STREAMING:
            if opts.get(k):
                out.append(Problem(
                    "error", f"{where}.options.{k}",
                    f"goal copy moves the data once, and {k} keeps a"
                    " stream running - make the goal cutover or two-way,"
                    " or drop the option"))
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


# ---- what doctor says of a hop -------------------------------------------

def said_by_doctor(hop, raw):
    """(key, sentence) for every key of this hop that does nothing here,
    or sets what migkit would otherwise decide."""
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    opts = hop.options or {}
    if engine == "hetero":
        on = {ALIASES.get(opts.get("source_engine", "mysql")),
              ALIASES.get(opts.get("target_engine", "postgres")), "hetero"}
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
        e = (ALIASES.get(opts.get(f"{side}_engine") or (
            "mysql" if side == "source" else "postgres"))
            if engine == "hetero" else engine)
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
    for k in ("exclude", "mapping"):
        if raw.get(k):
            out[k] = raw[k]
    if opts:
        out["options"] = opts
    return out
