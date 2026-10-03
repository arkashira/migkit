"""A hop from another tool's task (backlog R21: switching costs nothing).

`migkit init` given a file where it asks for the source reads it as the
task it describes and writes the hop that does the same:

* an AWS DMS replication task - the JSON `describe-replication-tasks`
  prints, with the `Endpoints` of `describe-endpoints` beside it (or a
  task alone, whose endpoints are then asked for): the endpoints become
  the two sides, the selection rules the hop's scope, the filters its row
  filters, table and column renames its mapping, the migration type its
  goal;
* a change-capture connector's configuration (the JSON a Kafka Connect
  worker is given, or its properties file): the database it reads, its
  include and exclude lists, its snapshot mode;
* a load-command file (`LOAD DATABASE FROM ... INTO ...`): both
  addresses, the tables it includes and excludes, its renames;
* a Tencent Cloud DTS job - a migration job's detail, a sync job, or the
  objects a data check compares, as the service prints them - or an
  Alibaba Cloud DTS job's detail or request (its objects): the hop
  checks exactly what the job takes, down to its tables' row filters and
  columns, and says each of the job's settings that makes a correct move
  look different (`from_tencent_dts`);
* the same of the other clouds' services: a Huawei Cloud DRS job's
  detail with the objects it takes beside it (`from_huawei_drs`), a
  Google Cloud migration job with its connection profiles beside it
  (`from_google_dms`) or a Datastream stream (`from_datastream`), and an
  Azure migration - a classic project task or a database migration of
  the newer service (`from_azure_dms`).

What migkit decides for itself - types, batch sizes, workers, how a
table is loaded - is left behind and said so; so is anything the other
tool did that migkit has no word for (a column added by expression, a
prefix added to every name), rather than carried half-way. A password in
the file is never copied: the guide asks for it and keeps it in the
keychain.

A list of tables to include is the hop's own `include` where each entry
is a name or a shell pattern (`_carried`); one a pattern cannot say (a
regular expression, a search) is returned as `include` and made into the
hop's `exclude` against the source's own tables when the guide connects,
and each such pattern is said in the notes - where the guide cannot reach
the source, it is what the operator writes into the hop by hand.
"""
import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Imported:
    name: str
    raw: dict
    #: what was left behind, and why, in plain words
    notes: list = field(default_factory=list)
    #: (kind, pattern) of the tables the task took: `glob` (a shell
    #: pattern over `schema.table`), `regex` (the whole of it), `search`
    #: (anywhere in the table's own name)
    include: list = field(default_factory=list)
    #: regexes over `schema.table` of tables it left out that a shell
    #: pattern cannot say
    exclude_re: list = field(default_factory=list)


def looks_like_a_file(text):
    return bool(text) and "://" not in str(text) and Path(
        str(text)).expanduser().is_file()


def read(path):
    """The hop a file describes, whichever tool it came from."""
    p = Path(str(path)).expanduser()
    text = p.read_text()
    stem = _name(p.stem)
    head = text.lstrip()
    if head.startswith("{") or head.startswith("["):
        data = json.loads(text)
        if _is_tencent_dts(data):
            return from_tencent_dts(data, stem)
        if _is_alibaba_dts(data):
            return from_alibaba_dts(data, stem)
        if _is_huawei_drs(data):
            return from_huawei_drs(data, stem)
        if _is_google_dms(data):
            return from_google_dms(data, stem)
        if _is_datastream(data):
            return from_datastream(data, stem)
        if _is_azure_dms(data):
            return from_azure_dms(data, stem)
        if _is_dms(data):
            return from_dms(data, stem)
        if _is_connector(data):
            return from_connector(data, stem)
        raise SystemExit(f"{p}: JSON, but not a replication task (it"
                         " has no TableMappings), a DTS job (no"
                         " MigrateOption, Objects, ObjectItems or"
                         " DbObject), a DRS job (no base_info or"
                         " object_info), a migration job or stream (no"
                         " sourceDatabase or sourceConfig), an Azure"
                         " migration (no taskType or sourceDatabaseName)"
                         " or a connector's configuration (no"
                         " connector.class)")
    if re.search(r"(?is)^\s*load\s+\w+", _strip_comments(text)):
        return from_load_file(text, stem)
    props = _properties(text)
    if "connector.class" in props:
        return from_connector(props, stem)
    raise SystemExit(f"{p}: not a file migkit reads a hop from - a"
                     " replication task's JSON, a connector's"
                     " configuration, or a LOAD DATABASE command file")


def _name(text):
    got = re.sub(r"[^a-z0-9-]+", "-", str(text).lower()).strip("-")
    return got or "hop"


def _glob(pattern):
    """A `%` (SQL LIKE's any run) as a shell pattern's `*`."""
    return str(pattern).replace("%", "*")


def _side(engine, ep, dbs=()):
    """A side as an address where one can say it, the long form
    otherwise."""
    from . import addresses
    from .engines import PORTS
    got = addresses.render(engine, ep, list(dbs), PORTS.get(engine, 0))
    return got if got else dict(ep)


# ---- AWS DMS -------------------------------------------------------------

#: DMS's EngineName and the engine migkit reads it as
DMS_ENGINES = {"mysql": "mysql", "mariadb": "mysql", "aurora": "mysql",
               "postgres": "postgres", "aurora-postgresql": "postgres",
               "oracle": "oracle", "sqlserver": "mssql",
               "azuredb": "mssql", "azure-sql-managed-instance": "mssql",
               "mongodb": "mongodb", "docdb": "mongodb",
               "redshift": "redshift", "redshift-serverless": "redshift",
               "db2": "db2", "sybase": "ase", "dynamodb": "dynamodb",
               "kinesis": "kinesis", "kafka": "kafka",
               "elasticsearch": "opensearch", "opensearch": "opensearch"}
DMS_GOALS = {"full-load": "copy", "full-load-and-cdc": "cutover",
             "cdc": "keep-in-sync"}


#: DMS's name rules for every name at a level, as the hop's `names`
_DMS_NAMING = {"convert-lowercase": ("case", "lower"),
               "convert-uppercase": ("case", "upper"),
               "add-prefix": ("prefix", None), "add-suffix": ("suffix", None),
               "remove-prefix": ("strip_prefix", None),
               "remove-suffix": ("strip_suffix", None)}


def _is_dms(data):
    if isinstance(data, dict):
        return bool(data.get("ReplicationTasks") or data.get(
            "ReplicationTask") or "TableMappings" in data)
    return False


def from_dms(data, stem="hop"):
    tasks = data.get("ReplicationTasks") or (
        [data["ReplicationTask"]] if data.get("ReplicationTask") else [data])
    if len(tasks) != 1:
        raise SystemExit(f"the file holds {len(tasks)} replication tasks;"
                         " give it one (a hop is one task)")
    task = tasks[0]
    notes = []
    ends = {e.get("EndpointArn"): e for e in data.get("Endpoints") or []}
    rules = task.get("TableMappings") or {"rules": []}
    if isinstance(rules, str):
        rules = json.loads(rules)
    rules = rules.get("rules") or []
    src_ep = ends.get(task.get("SourceEndpointArn"))
    dst_ep = ends.get(task.get("TargetEndpointArn"))
    src_engine = _dms_engine(src_ep, notes)
    dst_engine = _dms_engine(dst_ep, notes)
    out = Imported(_name(task.get("ReplicationTaskIdentifier") or stem), {},
                   notes)
    goal = DMS_GOALS.get(str(task.get("MigrationType", "")).lower())
    if goal and goal != "copy":
        out.raw["goal"] = goal
    schemas = set()
    exclude, where, tables, columns, naming = [], {}, {}, {}, {}
    for r in sorted(rules, key=lambda r: int(r.get("rule-id") or 0)):
        kind, action = r.get("rule-type"), r.get("rule-action")
        loc = r.get("object-locator") or {}
        sch, tab = loc.get("schema-name"), loc.get("table-name")
        ident = f"{sch}.{tab}" if tab else str(sch)
        if kind == "selection":
            if action in ("include", "explicit"):
                if sch and "%" not in sch:
                    schemas.add(sch)
                if not (sch == "%" and tab == "%"):
                    out.include.append(("glob", _glob(ident)))
                if r.get("filters"):
                    pred = _dms_filter(r["filters"], src_engine)
                    if "%" in ident:
                        notes.append(f"the row filter of rule"
                                     f" {r.get('rule-name')} covers every"
                                     f" table matching {ident}; migkit's"
                                     " filters name one table each - not"
                                     " carried")
                    else:
                        where[ident] = pred
            elif action == "exclude":
                exclude.append(_glob(ident))
        elif kind == "transformation":
            target = r.get("rule-target")
            value = r.get("value")
            if target == "table" and action == "rename" and "%" not in \
                    ident:
                tables[ident] = f"{sch}.{value}" if sch and "%" not in \
                    sch else value
            elif target == "column" and action in ("rename",
                                                   "remove-column") \
                    and "%" not in ident:
                col = loc.get("column-name")
                got = columns.setdefault(ident, {})
                if action == "rename":
                    got.setdefault("rename", {})[col] = value
                else:
                    got.setdefault("drop", []).append(col)
            elif target == "schema" and action == "rename" and \
                    src_engine == "mysql" and sch and "%" not in sch:
                out.raw.setdefault("db_map", {})[sch] = value
            elif target in ("schema", "table") and action in _DMS_NAMING \
                    and (sch == "%" and (target == "schema"
                                         or tab in (None, "%"))):
                # one rule for every name at a level: the hop's name rule;
                # MySQL's schema is its database
                level = ("databases" if src_engine == "mysql" else
                         "schemas") if target == "schema" else "tables"
                key, arg = _DMS_NAMING[action]
                naming.setdefault(level, {})[key] = arg or value
            else:
                notes.append(f"rule {r.get('rule-name')}"
                             f" ({target} {action}) has no match in"
                             " migkit - not carried")
    settings = task.get("ReplicationTaskSettings") or {}
    if isinstance(settings, str):
        settings = json.loads(settings or "{}")
    prep = ((settings.get("FullLoadSettings") or {})
            .get("TargetTablePrepMode"))
    if prep and prep != "DO_NOTHING":
        notes.append(f"TargetTablePrepMode {prep}: migkit creates what"
                     " the target lacks and never drops or empties a"
                     " table on its own")
    if (settings.get("TargetMetadata") or {}).get("LimitedSizeLobMode"):
        notes.append("limited LOB mode cut values at a size; migkit"
                     " carries every value whole")
    dbs = []
    if src_engine == "mysql":
        dbs = sorted(schemas)
    elif src_ep and src_ep.get("DatabaseName"):
        dbs = [src_ep["DatabaseName"]]
    for side, ep, engine in (("source", src_ep, src_engine),
                             ("target", dst_ep, dst_engine)):
        if ep is None:
            notes.append(f"the file names no {side} endpoint - the guide"
                         " asks for its address")
            continue
        long = _dms_endpoint(ep, engine, notes)
        if side == "source":
            out.raw[side] = _side(engine, long, dbs)
        else:
            names = []
            if dst_ep.get("DatabaseName") and dbs and src_engine != "mysql":
                names = [dst_ep["DatabaseName"]]
            elif src_engine == "mysql" and dbs:
                names = [out.raw.get("db_map", {}).get(d, d) for d in dbs]
            out.raw[side] = _side(engine, long, names)
            if names and len(names) != len(dbs):
                out.raw[side] = long
    if src_engine and dst_engine and src_engine != dst_engine and any(
            isinstance(out.raw.get(s), dict) for s in ("source", "target")):
        out.raw["engine"] = "hetero"
        out.raw["options"] = {"source_engine": src_engine,
                              "target_engine": dst_engine}
    elif src_engine and any(isinstance(out.raw.get(s), dict)
                            for s in ("source", "target")):
        out.raw["engine"] = src_engine
    if any(isinstance(out.raw.get(s), dict) for s in ("source", "target")):
        if dbs:
            out.raw["databases"] = dbs
    else:
        out.raw.pop("db_map", None)
    if exclude:
        out.raw["exclude"] = exclude
    if out.include and all(k == "glob" for k, _ in out.include):
        # what the task took, said as the hop's own `include`: the same
        # scope for a table made on the source later, which a list of
        # what it left out, read once, would take
        out.raw["include"] = [p for _, p in out.include]
        out.include = []
    mapping = {k: v for k, v in (("tables", tables), ("columns", columns),
                                 ("where", where), ("names", naming)) if v}
    if mapping:
        out.raw["mapping"] = mapping
    return _carried(out)


def _dms_engine(ep, notes):
    if ep is None:
        return None
    name = str(ep.get("EngineName", "")).lower()
    got = DMS_ENGINES.get(name)
    if got is None:
        raise SystemExit(f"the endpoint {ep.get('EndpointIdentifier')} is a"
                         f" {name} one, which migkit has no engine for")
    return got


def _dms_endpoint(ep, engine, notes):
    own = next((v for k, v in ep.items() if k.endswith("Settings")
                and isinstance(v, dict) and v.get("ServerName")), {})
    out = {}
    host = ep.get("ServerName") or own.get("ServerName")
    port = ep.get("Port") or own.get("Port")
    user = ep.get("Username") or own.get("Username")
    if engine == "kafka":
        broker = (ep.get("KafkaSettings") or {}).get("Broker", "")
        hosts = [b for b in broker.split(",") if b]
        if len(hosts) > 1:
            out["hosts"] = hosts
        elif hosts:
            host, _, p = hosts[0].partition(":")
            port = int(p) if p else None
    if host:
        out["host"] = host
    if port:
        out["port"] = int(port)
    if user:
        out["user"] = user
    mode = str(ep.get("SslMode", "none")).lower()
    if engine in ("postgres", "redshift"):
        if mode != "none":
            out["sslmode"] = mode
    elif engine == "mysql" and mode == "require":
        out["ssl"] = True
    elif engine == "mysql" and mode.startswith("verify"):
        notes.append(f"the {ep.get('EndpointType', '').lower()} endpoint"
                     f" checks the server's certificate ({mode}) against"
                     " a certificate DMS keeps: give its file as ssl-ca in"
                     " the address")
    if engine == "oracle" and ep.get("DatabaseName"):
        out["service"] = ep["DatabaseName"]
    return out


def _quoting(engine):
    """(identifier, literal) quoting of the source's engine, its own
    (`_quote_ident`, `_quote_literal`): a row filter is written in the
    source's SQL and translated for the target where it speaks another
    (`hetero`). Measured with quoting of this module's own: a filter on
    PostgreSQL's column `user` became `user = 'bob'` - the session's user,
    so zero rows on both sides and "same" over nothing - `order` a syntax
    error, and `'C:\\new'` read by MySQL with a newline in it (audit
    2026-10-01, DU4)."""
    from .engines import _class_for, canonical
    cls = _class_for(canonical(engine) or str(engine))
    if cls is None:
        raise SystemExit(f"a row filter on a {engine} source: migkit has"
                         " no engine to write it in")
    one = object.__new__(cls)
    return one._quote_ident, one._quote_literal


def _dms_filter(filters, engine):
    """DMS's filters as one predicate: the conditions of a column any of
    them, the columns all - each name and value quoted as the source's
    engine quotes them."""
    ops = {"eq": "=", "noteq": "<>", "lte": "<=", "ste": "<=", "gte": ">="}
    _q, _lit = _quoting(engine)
    parts = []
    for f in filters:
        if f.get("filter-type", "source") != "source":
            continue
        col = _q(f["column-name"])
        conds = []
        for c in f.get("filter-conditions") or []:
            op = c.get("filter-operator")
            if op in ops:
                conds.append(f"{col} {ops[op]} {_lit(c.get('value'))}")
            elif op == "between":
                conds.append(f"{col} between {_lit(c['start-value'])} and"
                             f" {_lit(c['end-value'])}")
            elif op == "notbetween":
                conds.append(f"{col} not between {_lit(c['start-value'])}"
                             f" and {_lit(c['end-value'])}")
            elif op == "null":
                conds.append(f"{col} is null")
            elif op == "notnull":
                conds.append(f"{col} is not null")
            else:
                raise SystemExit(f"filter operator {op}: not one migkit"
                                 " reads")
        if conds:
            parts.append(conds[0] if len(conds) == 1
                         else "(" + " or ".join(conds) + ")")
    return " and ".join(parts)


# ---- Tencent Cloud DTS ---------------------------------------------------

#: DTS's DatabaseType and the engine migkit reads it as
TDTS_ENGINES = {"mysql": "mysql", "mariadb": "mysql", "percona": "mysql",
                "cynosdbmysql": "mysql", "tdsqlmysql": "mysql",
                "tdstore": "mysql", "postgresql": "postgres",
                "cynosdbpg": "postgres", "tdpg": "postgres",
                "mongodb": "mongodb", "redis": "redis", "keewidb": "redis",
                "tendis": "redis", "sqlserver": "mssql", "kafka": "kafka",
                "ckafka": "kafka", "clickhouse": "clickhouse",
                "oracle": "oracle", "tidb": "tidb",
                "tdsqlpercona": "mysql", "tdsqlc": "mysql",
                "cynosdbmysql_serverless": "mysql", "cdb": "mysql",
                "tdsqlpg": "postgres", "cdwpg": "postgres",
                "tdsqlcpg": "postgres", "tdmysql": "mysql",
                "mongo": "mongodb", "tdmongodb": "mongodb",
                "elasticsearch": "opensearch", "es": "opensearch",
                "db2": "db2", "sqlserver2": "mssql", "mssql": "mssql"}


def _unwrap(data):
    """A response as `tccli` saves it, `{"Response": {...}}`, or as an SDK
    gives it."""
    if isinstance(data, dict) and isinstance(data.get("Response"), dict):
        return data["Response"]
    return data


def _tdts_kind(d):
    """Which of the service's things a dict is: a migration job (the API
    of 2021 or of 2018), a sync job, a data check's objects or its task,
    a subscription - or None."""
    if not isinstance(d, dict):
        return None
    opt = d.get("MigrateOption")
    if isinstance(opt, dict) and isinstance(opt.get("DatabaseTable"), dict):
        return "migration"
    if "DatabaseInfo" in d or (isinstance(opt, dict)
                               and "MigrateObject" in opt):
        return "migration-2018"
    if isinstance(opt, dict):
        return "migration"
    if isinstance(d.get("Objects"), dict):
        return "sync"
    if isinstance(d.get("ObjectItems"), list):
        return "check"
    cfg = d.get("Config")
    if isinstance(cfg, dict) and ("ObjectItems" in cfg
                                  or "ObjectMode" in cfg):
        return "check-task"
    if "SubscribeObjects" in d or ("SubscribeId" in d and (
            "Endpoints" in d or "SubscribeMode" in d)):
        return "subscription"
    return None


def _tdts_listed(d):
    """The jobs (or checks) a list response holds, or None."""
    for key in ("JobList", "Items"):
        many = d.get(key) if isinstance(d, dict) else None
        if isinstance(many, list) and many and all(
                isinstance(j, dict) for j in many):
            return many
    return None


def _is_tencent_dts(data):
    d = _unwrap(data)
    if not isinstance(d, dict):
        return False
    many = _tdts_listed(d)
    if many is not None:
        return all(_tdts_kind(j) or str(j.get("JobId", "")).startswith(
            ("dts-", "sync-", "subs-")) for j in many)
    return _tdts_kind(d) is not None


def from_tencent_dts(data, stem="hop"):
    """A hop that checks what a Tencent Cloud DTS job takes, read from what
    the service says of it - whichever of its things the file holds: a
    migration job's detail (DescribeMigrationDetail, or the 2018 API's
    DescribeMigrateJobs with its DatabaseInfo), a sync job
    (DescribeSyncJobs), a data check's objects or its task
    (DescribeCompareTasks), a subscription (DescribeSubscribeDetail), or
    the request any of them was made with.

    The job moves the data, so the hop's goal is `verify`: it compares
    exactly what the job was told to take - its databases and their new
    names, the schemas, tables and views it picked and their new names,
    each table's row filter and its columns, the changes it carries - and
    nothing it left out, so a log table the job was told to leave behind
    is neither moved by migkit nor called missing. Every setting of the
    job that makes a correct move look different (deletes it does not
    carry, conflicts it ignores, a target left writable) is said beside
    it."""
    d = _unwrap(data)
    many = _tdts_listed(d)
    if many is not None:
        if len(many) != 1:
            names = ", ".join(str(j.get("JobName") or j.get("TaskName")
                                  or j.get("JobId")) for j in many[:6])
            raise SystemExit(f"the file holds {len(many)} jobs ({names});"
                             " give it one (a hop is one job)")
        d = _unwrap(many[0])
    kind = _tdts_kind(d)
    if kind is None:
        job = d.get("JobId") or "the job"
        raise SystemExit(f"{job}: a list of migration jobs says no objects;"
                         " save the job's own detail - tccli dts"
                         f" DescribeMigrationDetail --JobId {job}")
    notes = []
    out = Imported(_name(d.get("JobName") or d.get("TaskName")
                         or d.get("SubscribeName") or d.get("JobId")
                         or d.get("SubscribeId") or stem), {}, notes)
    out.raw["goal"] = "verify"
    notes.append("the job moves the data and this hop checks what it took"
                 " (goal: verify) - make the goal copy or cutover to have"
                 " migkit move it instead")
    src = _tdts_end(d, "Src", notes)
    dst = _tdts_end(d, "Dst", notes) if kind != "subscription" else None
    ops, ddl = {}, None
    if kind == "migration":
        opt = d["MigrateOption"]
        picked = opt.get("DatabaseTable") or {"ObjectMode": "all"}
        mode, items = picked.get("ObjectMode"), picked.get("Databases")
        _tdts_migrate_notes(opt, notes)
    elif kind == "migration-2018":
        opt = d.get("MigrateOption") or {}
        mode = "all" if str(opt.get("MigrateObject")) == "1" else "partial"
        items = _tdts_2018_items(d.get("DatabaseInfo"))
        _tdts_2018_notes(opt, notes)
    elif kind == "sync":
        picked = d["Objects"]
        mode, items = picked.get("Mode"), picked.get("Databases")
        _tdts_sync_notes(d.get("Options") or {}, picked, notes)
        ops = _tdts_ops(d.get("Options") or {}, picked,
                        flat=(src or {}).get("engine") in ("mysql",
                                                            "mongodb"))
        ddl = _tdts_ddl(d.get("Options") or {}, notes)
    elif kind == "subscription":
        mode, items = _tdts_subscribed(d)
        notes.append("a subscription carries the changes to a topic, not a"
                     " database: give as the target the database its"
                     " consumer writes, and the check compares the two"
                     " within what the subscription takes")
    else:
        cfg = d["Config"] if kind == "check-task" else d
        mode, items = cfg.get("ObjectMode"), cfg.get("ObjectItems")
        _tdts_check_notes(d, notes)
    got = _tdts_selection(mode, items or [], notes)
    if ops:
        got["ops"] = ops
    if kind == "migration":
        _tdts_own_check(d["MigrateOption"].get("Consistency") or {}, got,
                        notes)
    dbs, db_map = got["dbs"], got["db_map"]
    src_e, dst_e = src and src["engine"], dst and dst["engine"]
    for side, end, names in (("source", src, dbs),
                             ("target", dst, [db_map.get(x, x)
                                              for x in dbs])):
        if end is None or not end["long"].get("host"):
            continue
        out.raw[side] = _side(end["engine"], end["long"], names)
    if src_e and dst_e and src_e != dst_e:
        out.raw["engine"] = "hetero"
        out.raw["options"] = {"source_engine": src_e,
                              "target_engine": dst_e}
    elif (src_e or dst_e) and any(isinstance(out.raw.get(x), dict)
                                  for x in ("source", "target")):
        out.raw["engine"] = src_e or dst_e
    if dbs and not isinstance(out.raw.get("source"), str):
        out.raw["databases"] = dbs
    if db_map and not isinstance(out.raw.get("target"), str):
        out.raw["db_map"] = db_map
    if got["include"]:
        out.raw["include"] = got["include"]
    if got.get("objects"):
        out.raw["objects"] = got["objects"]
    mapping = {k: got[k] for k in ("tables", "columns", "where", "ops")
               if got.get(k)}
    if mapping:
        out.raw["mapping"] = mapping
    if ddl is not None:
        # what a tail does with each DDL, as the job carries them
        out.raw.setdefault("options", {})["ddl"] = ddl
    return out


def _tdts_2018_items(info):
    """The 2018 API's `DatabaseInfo` - `[{"Database": .., "Table": [..]}]`,
    a JSON string or the list - in the shape the 2021 API names objects."""
    if isinstance(info, str):
        info = json.loads(info or "[]")
    out = []
    for e in info or []:
        if not isinstance(e, dict):
            continue
        item = {"DbName": e.get("Database") or e.get("DB")
                or e.get("DbName"),
                "NewDbName": e.get("NewDatabase") or e.get("NewDbName"),
                "SchemaName": e.get("Schema") or e.get("SchemaName") or ""}
        tables = e.get("Table", e.get("Tables")) or []
        named = []
        for t in tables if isinstance(tables, list) else [tables]:
            if isinstance(t, str):
                named.append({"TableName": t})
            elif isinstance(t, dict):
                named.append({"TableName": t.get("Table")
                              or t.get("TableName"),
                              "NewTableName": t.get("NewTable")
                              or t.get("NewTableName")})
        if named:
            item["TableMode"], item["Tables"] = "partial", named
        out.append(item)
    return out


def _tdts_2018_notes(opt, notes):
    kind = str(opt.get("MigrateType") or "")
    if kind == "1":
        notes.append("the job moves the structure only: every table's rows"
                     " will read as missing")
    elif kind == "2":
        notes.append("the job moves the data once and no change after it:"
                     " a row changed on the source since reads as different")
    if str(opt.get("IsOverrideRoot")) in ("1", "True", "true"):
        notes.append("the job overwrote the target's root account with the"
                     " source's")


def _tdts_subscribed(d):
    """(mode, objects) of a subscription, in the shape the other jobs name
    their objects: a whole instance, a database, or some of its tables."""
    mode = str(d.get("SubscribeMode") or d.get("SubscribeObjectType")
               or "").lower()
    objs = d.get("SubscribeObjects") or d.get("Objects") or []
    if mode in ("all", "ddl", "0", "2") or not objs:
        return "all", []
    items = []
    for o in objs if isinstance(objs, list) else []:
        if not isinstance(o, dict):
            continue
        db = o.get("Database") or o.get("DatabaseName")
        tables = o.get("Tables") or o.get("TableNames") or []
        typ = str(o.get("ObjectType", o.get("ObjectsType", ""))).lower()
        if typ in ("table", "1") and tables:
            items.append({"DbName": db, "TableMode": "partial",
                          "Tables": [{"TableName": t} for t in tables]})
        else:
            items.append({"DbName": db})
    return "partial", items


def _tdts_check_notes(d, notes):
    """What a data check of the service's own is, said beside the hop that
    checks the same objects again."""
    method = d.get("Method") or (d.get("Options") or {}).get("Method")
    if method == "rowsCount":
        notes.append("the service's own check of these objects counts rows"
                     " only")
    elif method == "sampleDataCheck":
        rate = (d.get("Options") or {}).get("SampleRate")
        notes.append("the service's own check of these objects reads"
                     f" {rate if rate is not None else 'a sample'}"
                     f"{'%' if rate is not None else ''} of the rows")
    if d.get("Conclusion"):
        notes.append(f"the service's own check concluded {d['Conclusion']}"
                     " - give its report as the hop's compare_with to have"
                     " each table's answer held against migkit's")


#: DTS's words for the changes a job carries
_DTS_OPS = {"insert": "insert", "update": "update", "delete": "delete"}


def _dml(words):
    """The changes a list of DTS operation words carries, in migkit's
    words, or None where it carries all three (nothing to say)."""
    got = sorted({_DTS_OPS[str(w).lower()] for w in words or []
                  if str(w).lower() in _DTS_OPS})
    return None if len(got) == 3 else got


def _ddl_option(carried, notes, said):
    """The hop's `ddl` (`tailtables.policy`) from the DDL kinds a job
    carries of a table: `apply` where it carries every one, with the kinds
    it leaves out that a tail can leave out under `skip`
    (`tailtables.SKIPPABLE`). An ALTER or a rename it leaves out is said:
    a tail cannot leave those out, since every change after one is
    written to columns or a table the target would not have."""
    from .tailtables import SKIPPABLE
    skip = [k for k in SKIPPABLE if k not in carried]
    held = [k for k in ("alter", "rename") if k not in carried]
    if held:
        notes.append(f"{said} does not carry a table's {' or '.join(held)}:"
                     " migkit's tail cannot leave "
                     + ("it" if len(held) == 1 else "them") + " out - under"
                     " this hop's ddl the target follows where it can, and"
                     " the table parks where not")
    return {"default": "apply", "skip": skip} if skip else "apply"


def _tdts_ddl(options, notes):
    """The hop's `ddl` from a Tencent sync job's `OpTypes` and
    `DdlOptions`: `DDL` carries every one, `PartialDDL` the kinds its
    table options name (`DdlObject` Table, `DdlValue` Create, Drop,
    Alter, Rename, Truncate). None where the job says neither."""
    ops = [str(o) for o in options.get("OpTypes") or []]
    if "DDL" in ops:
        return "apply"
    if "PartialDDL" not in ops:
        if ops:
            notes.append("the job carries no DDL: a schema change on the"
                         " source is not on the target, and reads as a"
                         " schema difference")
        return None
    carried = set()
    for o in options.get("DdlOptions") or []:
        if isinstance(o, dict) and str(o.get("DdlObject") or "").lower() \
                == "table":
            carried |= {str(v).lower() for v in o.get("DdlValue") or []}
    return _ddl_option(carried, notes, "the job")


def _adts_ddl(entries, notes):
    """The hop's `ddl` from an Alibaba job's `ddl_op` on its objects (`ct`,
    `at`, `dt`, `rt`, `tt`; empty for every one, `none` for none). The
    hop's `skip` is the hop's: where objects say it apart, what any of
    them leaves out is left out, and that is said. None where no object
    says it."""
    if not entries:
        return None
    sets = {frozenset(c) for _, c in entries}
    if len(sets) > 1:
        notes.append("the job carries other DDL for some objects than for"
                     " others (" + ", ".join(i for i, _ in entries[:4])
                     + (" ..." if len(entries) > 4 else "") + "): the"
                     " hop leaves out what any of them leaves out")
    carried = set.intersection(*[set(c) for c in sets])
    return _ddl_option(carried, notes, "the job")


def _tdts_ops(options, objects, flat=True):
    """`mapping.ops` from what a sync job carries: the job's own DML list
    for every table, a database's or a table's whitelist for it - a table
    named in any of the database's schemas where the source has them (the
    whitelist names none)."""
    out = {}
    ops = options.get("OpTypes")
    if ops:
        got = _dml(ops)
        if got is not None:
            out["*"] = got
    for f in objects.get("DatabasesOpFilter") or []:
        if not isinstance(f, dict) or not f.get("DbName"):
            continue
        db = f["DbName"]
        got = (f.get("OpFilter") or {}).get("OpTypes")
        if got:
            got = _dml(got)
            if got is not None:
                out[f"{db}.*"] = got
        for t in f.get("Tables") or []:
            got = (t.get("OpFilter") or {}).get("OpTypes")
            if got and t.get("TableName"):
                got = _dml(got)
                if got is not None:
                    out[f"{db}.{t['TableName']}" if flat else
                        f"{db}.*.{t['TableName']}"] = got
    return out


def _tdts_end(d, side, notes):
    """One side of the job: its engine and the long form of its address,
    or None where the file does not say. The password is never read; the
    instance the job reaches by its own network is said where there is
    no address migkit could use."""
    word = "source" if side == "Src" else "target"
    info = d.get(f"{side}Info")
    kind = d.get(f"{side}DatabaseType") or (
        info.get("DatabaseType") if isinstance(info, dict) else None)
    if side == "Src" and info is None and isinstance(d.get("Endpoints"),
                                                     list):
        # a subscription: its source's nodes, its product the engine
        info = {"Info": d["Endpoints"], "AccessType": d.get("AccessType"),
                "DatabaseType": d.get("Product") or d.get("DatabaseType")}
        kind = kind or info["DatabaseType"]
    nodes = []
    if isinstance(info, dict) and isinstance(info.get("Info"), list):
        nodes = [n for n in info["Info"] if isinstance(n, dict)]
        access = info.get("AccessType")
    elif isinstance(info, dict):
        nodes = [info]
        access = d.get(f"{side}AccessType")
    else:
        many = d.get(f"{side}Infos")
        if isinstance(many, dict):
            nodes = [n for n in many.get("Info") or [] if isinstance(n, dict)]
            kind = kind or many.get("DatabaseType")
            access = many.get("AccessType")
        else:
            access = d.get(f"{side}AccessType")
    if not kind and not nodes:
        notes.append(f"the file names no {word} - the guide asks for its"
                     " address")
        return None
    engine = TDTS_ENGINES.get(str(kind or "").lower())
    if engine is None:
        raise SystemExit(f"the job's {word} is a {kind} one, which migkit"
                         " has no engine for")
    node = nodes[0] if nodes else {}
    if len(nodes) > 1:
        notes.append(f"the {word} is {len(nodes)} nodes; the address is the"
                     " first one's - give the one migkit should read")
    long = {}
    host = node.get("Host") or node.get("Ip") or node.get("HostName")
    if host:
        long["host"] = str(host)
    if node.get("Port"):
        long["port"] = int(node["Port"])
    if node.get("User"):
        long["user"] = str(node["User"])
    if str(node.get("EncryptConn") or "") == "Encrypted":
        if engine == "postgres":
            long["sslmode"] = "require"
        elif engine == "mysql":
            long["ssl"] = True
    elif str(node.get("EncryptConn") or "") == "UnEncrypted":
        notes.append(f"the job reaches the {word} without encrypting the"
                     " connection (EncryptConn UnEncrypted)")
    if not host:
        what = node.get("InstanceId") or node.get("CvmInstanceId") or ""
        notes.append(f"the {word} is {what or 'an instance'}, which the job"
                     " reaches through the cloud's own network"
                     f" ({access or 'not said'}) - the guide asks for an"
                     " address migkit can reach")
    return {"engine": engine, "long": long}


def _tdts_selection(mode, items, notes):
    """What the job takes, as the hop says it: its databases and their
    new names, `include` where it takes less than whole databases, table
    renames, row filters and columns - each named from its database
    down, so two databases' `orders` cannot be confused."""
    got = {"dbs": [], "db_map": {}, "include": [], "tables": {},
           "where": {}, "columns": {}}
    if str(mode or "").lower() == "all":
        notes.append("the job takes the whole instance; so does the hop")
        return got
    whole = True
    for it in items:
        db = it.get("DbName")
        if not db:
            notes.append("an object with no database name - not carried")
            continue
        if db not in got["dbs"]:
            got["dbs"].append(db)
        new = it.get("NewDbName")
        if new and new != db:
            if got["db_map"].get(db, new) != new:
                raise SystemExit(f"the job sends database {db} to both"
                                 f" {got['db_map'][db]} and {new}")
            got["db_map"][db] = new
        sch, new_sch = it.get("SchemaName") or "", it.get("NewSchemaName")
        tables = it.get("Tables") or []
        tmode = str(it.get("TableMode") or (
            "partial" if tables else "all")).lower()
        base = f"{db}.{sch}" if sch else db
        if new_sch and new_sch != sch and tmode != "partial":
            notes.append(f"schema {base} lands as {new_sch}: migkit renames"
                         " a table by its name - not carried")
        if tmode != "partial":
            got["include"].append(f"{base}.*")
            whole = whole and not sch
        else:
            whole = False
            for t in tables:
                _tdts_table(got, base, sch, new_sch, t, notes)
        _tdts_objects(got, base, it)
    if whole:
        # whole databases: the databases say it
        got["include"] = []
    # a kind every database takes whole is all of it: nothing to say
    got["objects"] = {k: list(dict.fromkeys(v)) for k, v in
                      got.get("objects", {}).items()
                      if v and not all(p.endswith(".*") for p in v)}
    return got


#: the job's per-database modes for the objects beside its tables, the
#: list each names them in, the key a name sits under, and the kind of
#: the hop's `objects`
_TDTS_OBJECTS = (("ViewMode", "Views", "ViewName", "views"),
                 ("FunctionMode", "Functions", None, "functions"),
                 ("ProcedureMode", "Procedures", None, "procedures"),
                 ("TriggerMode", "Triggers", None, "triggers"),
                 ("EventMode", "Events", None, "events"),
                 ("RoleMode", "Roles", "RoleName", "roles"))


def _tdts_objects(got, base, item):
    """The objects beside its tables a database of the job names one by
    one - views, functions and procedures, triggers, events, roles - as
    the hop's `objects` names them. Only a choice the job made by name is
    carried: a kind it does not take at all is not one migkit leaves out
    of the check, because the application needs it on the target all
    the same - its absence there is a finding, said beside the hop."""
    objects = got.setdefault("objects", {})
    for mode_key, list_key, name_key, kind in _TDTS_OBJECTS:
        # a database that says nothing of a kind takes all of it
        mode = str(item.get(mode_key) or "all").lower()
        mine = objects.setdefault(kind, [])
        if mode == "partial":
            for o in item.get(list_key) or []:
                name = o.get(name_key) if name_key and isinstance(o, dict) \
                    else o
                if name:
                    mine.append(f"{base}.{name}")
        elif mode == "all":
            mine.append(f"{base}.*")


def _tdts_table(got, base, sch, new_sch, t, notes):
    name = t.get("TableName")
    if not name:
        return
    ident = f"{base}.{name}"
    got["include"].append(ident)
    new = t.get("NewTableName")
    if (new and new != name) or (new_sch and new_sch != sch):
        got["tables"][ident] = ".".join(
            x for x in (new_sch or sch, new or name) if x)
    if t.get("FilterCondition"):
        got["where"][ident] = str(t["FilterCondition"])
        if t.get("FilterTimeZone") not in (None, "", "+00:00"):
            notes.append(f"the row filter of {ident} reads its times in"
                         f" {t['FilterTimeZone']}; migkit asks it of each"
                         " server in that server's own zone")
    if str(t.get("ColumnMode") or "").lower() == "partial":
        cols = [c for c in t.get("Columns") or [] if c.get("ColumnName")]
        rule = {"keep": [c["ColumnName"] for c in cols]}
        renamed = {c["ColumnName"]: c["NewColumnName"] for c in cols
                   if c.get("NewColumnName")
                   and c["NewColumnName"] != c["ColumnName"]}
        if renamed:
            rule["rename"] = renamed
        got["columns"][ident] = rule
    if t.get("TmpTables"):
        notes.append(f"the online schema change tables of {ident}"
                     f" ({', '.join(t['TmpTables'])}): migkit knows such"
                     " working tables by itself - not carried")


def _tdts_migrate_notes(opt, notes):
    kind = str(opt.get("MigrateType") or "")
    if kind == "structure":
        notes.append("the job moves the structure only: every table's rows"
                     " will read as missing")
    elif kind == "full":
        notes.append("the job moves the data once and no change after it:"
                     " a row changed on the source since reads as different")
    if opt.get("IsMigrateAccount") is False:
        notes.append("the job does not move accounts (IsMigrateAccount"
                     " false): the check shows the target's users and"
                     " grants as they are, missing ones among them - make"
                     " them before the cutover (migkit users create), or"
                     " say accounts: false where the target's accounts are"
                     " made apart from this move")
    if opt.get("IsDstReadOnly") is False:
        notes.append("the target stays writable while the job runs"
                     " (IsDstReadOnly false): a row written there directly"
                     " reads as a difference")
    adv = (opt.get("DatabaseTable") or {}).get("AdvancedObjects")
    left = sorted({"trigger", "function", "procedure", "event"}
                  - {str(a).lower() for a in adv or []})
    if left:
        notes.append(f"the job carries no {', '.join(x + 's' for x in left)}:"
                     " the check shows"
                     " them missing on the target until they are made there"
                     " (migkit sync --kind schema makes them)")
    if adv:
        notes.append(f"the job also takes {', '.join(adv)}; migkit compares"
                     " them with the schema")


def _tdts_sync_notes(options, objects, notes):
    ops = [str(o) for o in options.get("OpTypes") or []]
    if ops:
        for op, said in (("Delete", "a row deleted on the source stays on"
                                    " the target and reads as extra"),
                         ("Update", "a row updated on the source keeps its"
                                    " old values on the target and reads as"
                                    " different"),
                         ("Insert", "a row added on the source never"
                                    " arrives and reads as missing")):
            if op not in ops:
                notes.append(f"the job does not carry {op.lower()}s: {said}")
    init = options.get("InitType")
    if init == "None":
        notes.append("the job carries changes only, from when it began: a"
                     " row from before is on the target only if it was"
                     " there already")
    elif init == "Structure":
        notes.append("the job made the structure and carries changes only:"
                     " the rows from before it began read as missing")
    conflict = options.get("ConflictHandleType")
    if conflict == "Ignore":
        notes.append("the job keeps a row the target already had under the"
                     " same key (ConflictHandleType Ignore): it reads as"
                     " different")
    elif conflict == "ConditionCover":
        notes.append("the job overwrites a row the target already had only"
                     " under a condition (ConditionCover): the rows it"
                     " kept read as different")
    if options.get("DealOfExistSameTable") == "ExecuteAfterIgnore":
        notes.append("the job wrote into tables the target already had"
                     " (ExecuteAfterIgnore): a row the target had of its"
                     " own reads as extra")
    if options.get("AddAdditionalColumn"):
        notes.append("the job adds columns of its own to each target table")
    for f in objects.get("DatabasesOpFilter") or []:
        if not isinstance(f, dict):
            continue
        for t in f.get("Tables") or []:
            ops = (t.get("OpFilter") or {}).get("OpTypes") or []
            if ops:
                notes.append(f"{f.get('DbName')}.{t.get('TableName')}: the"
                             f" job carries only {', '.join(ops)}")
        ops = (f.get("OpFilter") or {}).get("OpTypes") or []
        if ops:
            notes.append(f"{f.get('DbName')}: the job carries only"
                         f" {', '.join(ops)}")


def _tdts_own_check(consistency, moved, notes):
    """What the job's own data check looks at, beside what it moves: a
    check that is off, that samples, that counts, or that covers less
    than the job moves is a claim of consistency about less than the
    whole."""
    if not consistency:
        return
    mode = consistency.get("Mode")
    if mode in ("noCheck", "notConfigured"):
        notes.append(f"the job's own data check is off ({mode})")
        return
    how = consistency.get("Options") or {}
    method = how.get("Method")
    if method == "rowsCount":
        notes.append("the job's own data check counts rows only")
    elif method == "sampleDataCheck":
        notes.append(f"the job's own data check reads"
                     f" {how.get('SampleRate', '?')}% of the rows")
    if consistency.get("ObjectMode") == "custom":
        objs = consistency.get("Objects") or {}
        own = _tdts_selection(objs.get("ObjectMode"),
                              objs.get("Databases") or [], [])
        said = own["include"] or own["dbs"]
        if said and (set(own["include"]) != set(moved["include"])
                     or set(own["dbs"]) != set(moved["dbs"])):
            notes.append("the job's own data check covers only"
                         f" {', '.join(said[:8])}"
                         + (" ..." if len(said) > 8 else ""))


# ---- Alibaba Cloud DTS ---------------------------------------------------

#: DTS's EngineName and the engine migkit reads it as
ADTS_ENGINES = {"mysql": "mysql", "mariadb": "mysql", "polardb": "mysql",
                "polardb_mysql": "mysql", "drds": "mysql",
                "postgresql": "postgres", "polardb_pg": "postgres",
                "polardb_o": "postgres", "greenplum": "postgres",
                "oracle": "oracle", "mssql": "mssql", "sqlserver": "mssql",
                "mongodb": "mongodb", "redis": "redis", "tair": "redis",
                "kafka": "kafka", "clickhouse": "clickhouse",
                "tidb": "tidb", "db2": "db2"}


#: where an Alibaba Cloud job keeps its objects, by the API that says them
_ADTS_OBJECTS = ("DbObject", "DbList", "MigrationObject",
                 "SynchronizationObjects")
#: the lists those APIs answer with
_ADTS_LISTS = ("DtsJobList", "MigrationJobs", "SynchronizationInstances")


def _adts_listed(data):
    for key in _ADTS_LISTS:
        got = data.get(key) if isinstance(data, dict) else None
        if isinstance(got, dict):
            # {"MigrationJob": [...]}, as the older APIs nest it
            got = next((v for v in got.values() if isinstance(v, list)),
                       None)
        if isinstance(got, list) and got:
            return got
    return None


def _is_alibaba_dts(data):
    if not isinstance(data, dict):
        return False
    many = _adts_listed(data)
    if many is not None:
        return all(isinstance(j, dict) and any(k in j for k in _ADTS_OBJECTS)
                   for j in many)
    return any(k in data for k in _ADTS_OBJECTS) and any(
        k in data for k in ("SourceEndpoint", "DtsJobId", "DtsJobName",
                            "SourceEndpointEngineName", "MigrationJobId",
                            "MigrationJobName", "SynchronizationJobId",
                            "SynchronizationJobName"))


def from_alibaba_dts(data, stem="hop"):
    """A hop that checks what an Alibaba Cloud DTS job takes, read from
    `DescribeDtsJobDetail` (its `DbObject`) or the request that made it
    (its `DbList`): the same hop `from_tencent_dts` makes of a Tencent
    job - goal `verify`, its databases (schemas, where the source has
    them) and their new names, its tables, their row filters and columns,
    and the job's settings that make a correct move look different."""
    jobs = _adts_listed(data)
    if jobs is not None:
        if len(jobs) != 1:
            names = ", ".join(str(_adts_job_name(j)) for j in jobs[:6])
            raise SystemExit(f"the file holds {len(jobs)} DTS jobs ({names});"
                             " give it one (a hop is one job)")
        data = jobs[0]
    notes = []
    out = Imported(_name(_adts_job_name(data) or stem), {}, notes)
    out.raw["goal"] = "verify"
    notes.append("the job moves the data and this hop checks what it took"
                 " (goal: verify) - make the goal copy or cutover to have"
                 " migkit move it instead")
    src = _adts_end(data, "Source", notes)
    dst = _adts_end(data, "Destination", notes)
    key = next((k for k in _ADTS_OBJECTS if k in data), "DbObject")
    objects = data.get(key) or {}
    if isinstance(objects, str):
        objects = json.loads(objects or "{}")
    src_e, dst_e = src and src["engine"], dst and dst["engine"]
    # where the source has schemas, the job's top level is its schemas
    # and the database is the endpoint's own
    one_db = src and src["db"] if src_e in ("postgres", "mssql",
                                            "oracle", "db2") else ""
    if isinstance(objects, list):
        got = _adts_list_selection(objects, notes)
    else:
        got = _adts_selection(objects, one_db, notes)
    if got.get("exclude"):
        out.raw["exclude"] = got["exclude"]
    _adts_mode_notes(data.get("MigrationMode") or {}, notes)
    _adts_accounts(data.get("Reserve"), notes)
    dbs, db_map = got["dbs"], got["db_map"]
    for side, end, names in (("source", src, dbs),
                             ("target", dst, [db_map.get(x, x)
                                              for x in dbs])):
        if end is None or not end["long"].get("host"):
            continue
        if side == "target" and one_db and dst["db"]:
            names = [dst["db"]]
        out.raw[side] = _side(end["engine"], end["long"], names)
    if src_e and dst_e and src_e != dst_e:
        out.raw["engine"] = "hetero"
        out.raw["options"] = {"source_engine": src_e,
                              "target_engine": dst_e}
    elif (src_e or dst_e) and any(isinstance(out.raw.get(x), dict)
                                  for x in ("source", "target")):
        out.raw["engine"] = src_e or dst_e
    if one_db and dst and dst["db"] and dst["db"] != one_db:
        db_map = {one_db: dst["db"]}
    if dbs and not isinstance(out.raw.get("source"), str):
        out.raw["databases"] = dbs
    if db_map and not isinstance(out.raw.get("target"), str):
        out.raw["db_map"] = db_map
    if got["include"]:
        out.raw["include"] = got["include"]
    mapping = {k: got[k] for k in ("tables", "columns", "where", "ops")
               if got.get(k)}
    if mapping:
        out.raw["mapping"] = mapping
    ddl = _adts_ddl(got.get("ddl"), notes)
    if ddl is not None:
        # what a tail does with each DDL, as the job carries them
        out.raw.setdefault("options", {})["ddl"] = ddl
    return out


def _adts_job_name(job):
    for k in ("DtsJobName", "MigrationJobName", "SynchronizationJobName",
              "DtsJobId", "MigrationJobId", "SynchronizationJobId"):
        if job.get(k):
            return job[k]
    return ""


def _adts_list_selection(objects, notes):
    """The older APIs' objects - `[{DBName, NewDBName, SchemaName,
    NewSchemaName, AllTable, TableIncludes: [{TableName, NewTableName,
    FilterCondition, ColumnIncludes, ColumnExcludes}], TableExcludes}]`
    (MigrationObject; SynchronizationObjects names the database
    DatabaseName) - as the hop says them, the tables a database's
    `TableExcludes` names left out by `exclude`."""
    got = {"dbs": [], "db_map": {}, "include": [], "tables": {},
           "where": {}, "columns": {}, "exclude": []}
    whole = True
    for o in objects:
        if not isinstance(o, dict):
            continue
        db = o.get("DBName") or o.get("DatabaseName")
        if not db:
            notes.append("an object with no database name - not carried")
            continue
        if db not in got["dbs"]:
            got["dbs"].append(db)
        new = o.get("NewDBName")
        if new and new != db:
            got["db_map"][db] = new
        sch, new_sch = o.get("SchemaName") or "", o.get("NewSchemaName")
        base = f"{db}.{sch}" if sch else db
        if new_sch and new_sch != sch and not o.get("TableIncludes"):
            notes.append(f"schema {base} lands as {new_sch}: migkit renames"
                         " a table by its name - not carried")
        incs = o.get("TableIncludes") or []
        if _flag(o.get("AllTable")) or not incs:
            got["include"].append(f"{base}.*")
            whole = whole and not sch
        else:
            whole = False
            for t in incs:
                if not isinstance(t, dict) or not t.get("TableName"):
                    continue
                name = t["TableName"]
                ident = f"{base}.{name}"
                got["include"].append(ident)
                target = t.get("NewTableName") or name
                if target != name or (new_sch and new_sch != sch):
                    got["tables"][ident] = ".".join(
                        x for x in (new_sch or sch, target) if x)
                if t.get("FilterCondition"):
                    got["where"][ident] = str(t["FilterCondition"])
                cols = [c for c in t.get("ColumnIncludes") or []
                        if isinstance(c, dict) and c.get("ColumnName")]
                gone = [c["ColumnName"] for c in t.get("ColumnExcludes") or []
                        if isinstance(c, dict) and c.get("ColumnName")]
                rule = {}
                if cols:
                    rule["keep"] = [c["ColumnName"] for c in cols]
                elif gone:
                    rule["drop"] = gone
                renamed = {c["ColumnName"]: c["NewColumnName"] for c in cols
                           if c.get("NewColumnName")
                           and c["NewColumnName"] != c["ColumnName"]}
                if renamed:
                    rule["rename"] = renamed
                if rule:
                    got["columns"][ident] = rule
        for t in o.get("TableExcludes") or []:
            if isinstance(t, dict) and t.get("TableName"):
                got["exclude"].append(f"{base}.{t['TableName']}")
    if whole:
        got["include"] = []
    return got


def _adts_end(data, side, notes):
    word = "source" if side == "Source" else "target"
    ep = data.get(f"{side}Endpoint")
    if not isinstance(ep, dict):
        ep = {k[len(f"{side}Endpoint"):]: v for k, v in data.items()
              if k.startswith(f"{side}Endpoint")}
    kind = ep.get("EngineName") or ep.get("Engine")
    if not kind:
        notes.append(f"the file names no {word} - the guide asks for its"
                     " address")
        return None
    engine = ADTS_ENGINES.get(str(kind).lower())
    if engine is None:
        raise SystemExit(f"the job's {word} is a {kind} one, which migkit"
                         " has no engine for")
    long = {}
    host = ep.get("Ip") or ep.get("IP") or ep.get("Host")
    if host:
        long["host"] = str(host)
    if ep.get("Port"):
        long["port"] = int(ep["Port"])
    user = ep.get("UserName") or ep.get("User")
    if user:
        long["user"] = str(user)
    if str(ep.get("SslSolutionEnum") or "").lower() not in ("", "disable"):
        if engine == "postgres":
            long["sslmode"] = "require"
        elif engine == "mysql":
            long["ssl"] = True
    if not host:
        what = ep.get("InstanceID") or ep.get("InstanceId") or ""
        notes.append(f"the {word} is {what or 'an instance'}, which the job"
                     " reaches through the cloud's own network"
                     f" ({ep.get('InstanceType') or 'not said'}) - the"
                     " guide asks for an address migkit can reach")
    return {"engine": engine, "long": long,
            "db": str(ep.get("DatabaseName") or "")}


def _flag(value):
    return str(value).lower() == "true"


def _adts_selection(objects, one_db, notes):
    """The job's objects as the hop says them: `{name: {name, all, Table:
    {name: {name, all, filter, column}}}}` - its top level databases, or a
    source with schemas' schemas inside the endpoint's one database."""
    got = {"dbs": [], "db_map": {}, "include": [], "tables": {},
           "where": {}, "columns": {}}
    whole = True
    if one_db:
        got["dbs"].append(one_db)
    for top, spec in objects.items():
        spec = spec if isinstance(spec, dict) else {}
        new = spec.get("name")
        if one_db:
            base = f"{one_db}.{top}"
            whole = False
            if new and new != top:
                notes.append(f"schema {top} lands as {new}: migkit renames a"
                             " table by its name - renamed table by table")
        else:
            base = top
            if top not in got["dbs"]:
                got["dbs"].append(top)
            if new and new != top:
                got["db_map"][top] = new
        tables = spec.get("Table") or spec.get("table") or {}
        if _flag(spec.get("all")) or not tables:
            got["include"].append(f"{base}.*")
            _adts_ops(f"{base}.*", spec, notes, got)
            continue
        whole = False
        for name, t in tables.items():
            t = t if isinstance(t, dict) else {}
            ident = f"{base}.{name}"
            got["include"].append(ident)
            target = t.get("name") or name
            if target != name or (one_db and new and new != top):
                got["tables"][ident] = ".".join(
                    x for x in ((new or top) if one_db else "", target) if x)
            if t.get("filter"):
                got["where"][ident] = str(t["filter"])
            cols = t.get("column") or t.get("Column") or {}
            if cols and not _flag(t.get("all")):
                picked = {c: v for c, v in cols.items()
                          if not isinstance(v, dict)
                          or v.get("state", "checked") == "checked"}
                rule = {"keep": sorted(picked)}
                renamed = {c: v["name"] for c, v in picked.items()
                           if isinstance(v, dict) and v.get("name")
                           and v["name"] != c}
                if renamed:
                    rule["rename"] = renamed
                got["columns"][ident] = rule
            _adts_ops(ident, t, notes, got)
    if whole:
        got["include"] = []
    return got


#: Alibaba Cloud DTS's `ddl_op` codes, as `canon.ddl` kinds
_ADTS_DDL = {"ct": "create", "at": "alter", "dt": "drop", "rt": "rename",
             "tt": "truncate"}


def _adts_ops(ident, spec, notes, got):
    ddl = spec.get("ddl_op")
    if ddl is not None:
        codes = {o.strip().lower() for o in str(ddl).split(",") if o.strip()}
        got.setdefault("ddl", []).append(
            (ident, set(_ADTS_DDL.values()) if not codes else
             {_ADTS_DDL[c] for c in codes if c in _ADTS_DDL}))
    ops = spec.get("dml_op")
    if ops in (None, ""):
        return
    have = {o.strip() for o in str(ops).split(",") if o.strip()}
    kept = sorted({"i": "insert", "u": "update", "d": "delete"}[o]
                  for o in have if o in ("i", "u", "d"))
    if len(kept) < 3:
        got.setdefault("ops", {})[ident] = kept
    for op, word, said in (("d", "deletes", "a row deleted on the source"
                                            " stays on the target and reads"
                                            " as extra"),
                           ("u", "updates", "a row updated on the source"
                                            " keeps its old values on the"
                                            " target"),
                           ("i", "inserts", "a row added on the source never"
                                            " arrives")):
        if op not in have:
            notes.append(f"{ident}: the job does not carry {word} - {said}")


def _adts_accounts(reserve, notes):
    """Whether the job moves accounts: not where its reserved settings
    (`Reserve`, a JSON text or a mapping) say `privilegeMigration` false -
    the setting the service's documentation names for it. Where they do
    not say, the job is taken as moving them. Said beside the hop, not
    written into it: the application needs its accounts on the target all
    the same, so their absence there is a finding the check keeps."""
    if isinstance(reserve, str):
        try:
            reserve = json.loads(reserve or "{}")
        except ValueError:
            return True
    said = (reserve or {}).get("privilegeMigration") \
        if isinstance(reserve, dict) else None
    if str(said).lower() != "false":
        return True
    notes.append("the job does not move accounts (privilegeMigration"
                 " false): the check shows the target's users and grants"
                 " as they are, missing ones among them - make them before"
                 " the cutover (migkit users create), or say accounts:"
                 " false where they are made apart from this move")
    return False


def _adts_mode_notes(mode, notes):
    if not mode:
        return
    if mode.get("DataInitialization") is False:
        notes.append("the job does not copy the rows that were there before"
                     " it began: they are on the target only if they were"
                     " there already")
    if mode.get("DataSynchronization") is False:
        notes.append("the job moves the data once and no change after it:"
                     " a row changed on the source since reads as different")


# ---- what another cloud's job takes, as one hop -------------------------

#: said beside every hop that checks what a service moved
_VERIFY = ("the job moves the data and this hop checks what it took"
           " (goal: verify) - make the goal copy or cutover to have"
           " migkit move it instead")
_ONCE = ("the job moves the data once and no change after it: a row"
         " changed on the source since reads as different")
_CHANGES_ONLY = ("the job carries changes only, from when it began: a row"
                 " from before is on the target only if it was there"
                 " already")
#: a change a job does not carry, and how the target looks for it
_NOT_CARRIED = (("delete", "a row deleted on the source stays on the target"
                           " and reads as extra"),
                ("update", "a row updated on the source keeps its old"
                           " values on the target and reads as different"),
                ("insert", "a row added on the source never arrives and"
                           " reads as missing"))


def _scope():
    """What a job takes, empty, for an importer to fill: its databases
    and their new names, `include` and `exclude`, table renames, row
    filters, columns, and the objects beside the tables it names."""
    return {"dbs": [], "db_map": {}, "include": [], "exclude": [],
            "tables": {}, "where": {}, "columns": {}, "objects": {}}


def _took_db(got, db, new=None):
    """A database the job takes, and the name it lands under."""
    db = str(db)
    if db not in got["dbs"]:
        got["dbs"].append(db)
    if new and str(new) != db:
        if got["db_map"].get(db, str(new)) != str(new):
            raise SystemExit(f"the job sends database {db} to both"
                             f" {got['db_map'][db]} and {new}")
        got["db_map"][db] = str(new)


def _leaf(text):
    """The last part of a resource's path - its own name."""
    return str(text or "").rstrip("/").rsplit("/", 1)[-1]


def _host_port(text):
    """`host:port` as its two parts; a bracketed address kept whole."""
    text = str(text or "").strip()
    m = re.match(r"^\[([^\]]+)\](?::(\d+))?$", text) or re.match(
        r"^([^:]+):(\d+)$", text)
    if m:
        return m.group(1), int(m.group(2)) if m.group(2) else None
    return text, None


def _dotted(name):
    """A dotted name's parts, each as its engine quotes it or bare:
    `[dbo].[orders]`, `"Sales"."Order"`, `` `shop`.`orders` `` and
    `public.orders` alike - a dot inside quotes is the name's own."""
    closing = {"[": "]", '"': '"', "`": "`"}
    out, cur, quote, i, s = [], "", None, 0, str(name)
    while i < len(s):
        c = s[i]
        if quote:
            if c == quote and s[i + 1:i + 2] == quote:
                cur += c
                i += 1
            elif c == quote:
                quote = None
            else:
                cur += c
        elif c in closing:
            quote = closing[c]
        elif c == ".":
            out.append(cur)
            cur = ""
        else:
            cur += c
        i += 1
    out.append(cur)
    return [p for p in out if p]


def _unreached(word, what, how, notes):
    notes.append(f"the {word} is {what or 'an instance'}, which the job"
                 " reaches through the cloud's own network"
                 f" ({how or 'not said'}) - the guide asks for an"
                 " address migkit can reach")


def _say_ops(kept, notes, who="the job"):
    for op, said in _NOT_CARRIED:
        if op not in kept:
            notes.append(f"{who} does not carry {op}s: {said}")


def _verify_sides(out, src, dst, got):
    """The rest of the hop that checks what a service's job took, the
    same for every service: each side as an address where the job names
    a server (one it reaches only through the cloud's own network is left
    for the guide to ask), the two engines, the databases and their new
    names, and what it takes - its scope, renames, row filters, columns,
    the changes it carries and the objects beside the tables it names
    one by one (a kind every database takes whole is all of it: nothing
    to say)."""
    dbs, db_map = got["dbs"], got["db_map"]
    src_e, dst_e = src and src["engine"], dst and dst["engine"]
    for side, end, names in (("source", src, dbs),
                             ("target", dst, [db_map.get(x, x)
                                              for x in dbs])):
        if end is None:
            continue
        if end.get("uri"):
            head, query = end["uri"]
            out.raw[side] = (head + ("/" + ",".join(names) if names else "")
                             + (f"?{query}" if query else ""))
        elif end["long"].get("host") or end["long"].get("hosts"):
            long = dict(end["long"])
            if long.get("host") and not long.get("port"):
                # a service that names no port reaches the engine's own
                from .config import DEFAULT_PORTS
                if DEFAULT_PORTS.get(end["engine"]):
                    long["port"] = DEFAULT_PORTS[end["engine"]]
            out.raw[side] = _side(end["engine"], long, names)
    if src_e and dst_e and src_e != dst_e:
        out.raw["engine"] = "hetero"
        out.raw["options"] = {"source_engine": src_e,
                              "target_engine": dst_e}
    elif (src_e or dst_e) and any(isinstance(out.raw.get(x), dict)
                                  for x in ("source", "target")):
        out.raw["engine"] = src_e or dst_e
    if dbs and not isinstance(out.raw.get("source"), str):
        out.raw["databases"] = dbs
    if db_map and not isinstance(out.raw.get("target"), str):
        out.raw["db_map"] = db_map
    for key in ("include", "exclude"):
        if got.get(key):
            out.raw[key] = list(dict.fromkeys(got[key]))
    objects = {k: list(dict.fromkeys(v))
               for k, v in (got.get("objects") or {}).items()
               if v and not all(p.endswith(".*") for p in v)}
    if objects:
        out.raw["objects"] = objects
    mapping = {k: got[k] for k in ("tables", "columns", "where", "ops",
                                   "names") if got.get(k)}
    if mapping:
        out.raw["mapping"] = mapping
    return out


# ---- Huawei Cloud DRS ----------------------------------------------------

#: DRS's db_type (and each side of its engine_type, `mysql-to-mysql`) and
#: the engine migkit reads it as; TaurusDB is MySQL's, DDS MongoDB's,
#: GeminiDB Redis Redis's
HDRS_ENGINES = {"mysql": "mysql", "taurus": "mysql",
                "postgresql": "postgres", "mongodb": "mongodb",
                "dds": "mongodb", "oracle": "oracle", "sqlserver": "mssql",
                "redis": "redis", "rediscluster": "redis",
                "gaussredis": "redis", "kafka": "kafka"}
#: the objects beside its tables a DRS job names among them, by `type`
_HDRS_OBJECTS = {"view": "views", "procedure": "procedures"}


def _hdrs_jobs(data):
    """The jobs a DRS answer holds - ShowJobDetail's `job`, the v5 batch
    APIs' `jobs`, the v3 batch detail's `results` - or None."""
    if not isinstance(data, dict):
        return None
    if isinstance(data.get("job"), dict):
        return [data["job"]]
    for key in ("jobs", "results"):
        many = data.get(key)
        if isinstance(many, list) and many and all(isinstance(j, dict)
                                                   for j in many):
            return many
    return None


def _hdrs_job_like(job):
    if isinstance(job.get("base_info"), dict):
        return True
    ep = job.get("source_endpoint")
    if isinstance(ep, list):
        ep = ep[0] if ep else None
    return isinstance(ep, dict) and "db_type" in ep and any(
        k in job for k in ("task_type", "db_use_type", "object_infos",
                           "db_object"))


def _hdrs_objects_like(data):
    """The objects of a job alone - the answer of its db-objects query."""
    return isinstance(data, dict) and isinstance(
        data.get("object_info"), dict) and (
            "object_scope" in data or "target_root_db" in data)


def _is_huawei_drs(data):
    if _hdrs_objects_like(data):
        return True
    jobs = _hdrs_jobs(data)
    return bool(jobs) and all(_hdrs_job_like(j) for j in jobs)


def _hdrs_job_name(job):
    return ((job.get("base_info") or {}).get("name") or job.get("name")
            or job.get("id") or "")


def from_huawei_drs(data, stem="hop"):
    """A hop that checks what a Huawei Cloud DRS job takes, read from what
    the service says of it: a job's detail (v5 ShowJobDetail, `{"job":
    ...}`) with the objects it takes beside it under `db_object` - the
    answer of GET /v5/{project_id}/jobs/{job_id}/db-objects, or the
    `db_object` the job was made with - the v5 batch APIs' `jobs`, the v3
    batch detail's `results` with their `object_infos` and the answer's
    row filters (`data_transformation`), or the objects alone.

    The same hop `from_tencent_dts` makes of a Tencent job: goal
    `verify`, its databases and their new names, its schemas and tables
    and theirs, each table's row filter and columns, the changes it
    carries (`dml_types`), and every setting of the job that makes a
    correct move look different - conflicts ignored, a dropped database
    or a structure change not carried, a target left writable, accounts
    not moved."""
    if _hdrs_objects_like(data):
        job, objects = {}, data
    else:
        jobs = _hdrs_jobs(data)
        if len(jobs) != 1:
            names = ", ".join(str(_hdrs_job_name(j)) for j in jobs[:6])
            raise SystemExit(f"the file holds {len(jobs)} DRS jobs ({names});"
                             " give it one (a hop is one job)")
        job = jobs[0]
        objects = job.get("db_object") if isinstance(
            job.get("db_object"), dict) else data.get("db_object")
    notes = []
    out = Imported(_name(_hdrs_job_name(job) or stem), {}, notes)
    out.raw["goal"] = "verify"
    notes.append(_VERIFY)
    pair = str((job.get("base_info") or {}).get("engine_type")
               or "").lower().split("-to-")
    pair = pair if len(pair) == 2 else [None, None]
    src = _hdrs_end(job, "source", pair[0], notes)
    dst = _hdrs_end(job, "target", pair[1], notes)
    if isinstance(objects, dict) and objects:
        got = _hdrs_selection(objects, notes)
    elif isinstance(job.get("object_infos"), list):
        objects, gone = _hdrs_v3_objects(job["object_infos"])
        got = _hdrs_selection(objects, notes)
        got["exclude"] += gone
        _hdrs_v3_filters(data.get("data_transformation"), got, notes)
    else:
        ident = job.get("id") or "{job_id}"
        raise SystemExit(f"{job.get('id') or 'the job'}: its detail says no"
                         " objects; save the objects it takes beside it,"
                         " under db_object - GET /v5/{project_id}/jobs/"
                         f"{ident}/db-objects")
    policy = job.get("policy_config") or {}
    kept = _ops_letters(policy.get("dml_types"))
    if kept is not None:
        got["ops"] = {"*": kept}
        _say_ops(kept, notes)
    _hdrs_notes(job, notes)
    return _verify_sides(out, src, dst, got)


def _ops_letters(value):
    """The changes a DRS `dml_types` names - `i,u,d`, the letters run
    together, or the words - in migkit's words, or None where it names
    all three or none (nothing to say)."""
    letters = {"i": "insert", "u": "update", "d": "delete"}
    got = set()
    for w in re.findall(r"[a-z]+", str(value or "").lower()):
        if w in _DTS_OPS:
            got.add(w)
        elif set(w) <= set(letters):
            got |= {letters[c] for c in w}
    return None if len(got) in (0, 3) else sorted(got)


def _hdrs_end(job, side, kind, notes):
    """One side of a DRS job: its engine and the long form of its address,
    or None where the file does not say. v5 names a side as a list of
    endpoints, each with its `endpoint` within; v3 as the endpoint
    itself. The password is never read; an instance the job reaches by
    the cloud's own network is said where there is no address."""
    eps = job.get(f"{side}_endpoint")
    if isinstance(eps, list):
        eps = [e for e in eps if isinstance(e, dict)]
        if len(eps) > 1:
            notes.append(f"the {side} is {len(eps)} endpoints; the address"
                         " is the first one's - give the one migkit should"
                         " read")
        ep = eps[0] if eps else None
    else:
        ep = eps if isinstance(eps, dict) else None
    kind = (ep or {}).get("db_type") or kind
    engine = HDRS_ENGINES.get(str(kind or "").lower())
    if kind and engine is None:
        raise SystemExit(f"the job's {side} is a {kind} one, which migkit"
                         " has no engine for")
    if ep is None:
        notes.append(f"the file names no {side} - the guide asks for its"
                     " address")
        return {"engine": engine, "long": {}, "db": ""} if engine else None
    node = ep["endpoint"] if isinstance(ep.get("endpoint"), dict) else ep
    if engine is None:
        raise SystemExit(f"the job's {side} names no engine migkit has")
    long = {}
    hosts = [h.strip() for h in str(node.get("ip") or "").split(",")
             if h.strip()]
    if len(hosts) > 1 and engine in ("mongodb", "kafka"):
        long["hosts"] = hosts
    elif hosts:
        host, port = _host_port(hosts[0])
        long["host"] = host
        port = port or node.get("db_port")
        if port:
            long["port"] = int(port)
        if len(hosts) > 1:
            notes.append(f"the {side} is {len(hosts)} servers; the address"
                         " is the first one's - give the one migkit should"
                         " read")
    if node.get("db_user"):
        long["user"] = str(node["db_user"])
    ssl = ep["ssl"] if isinstance(ep.get("ssl"), dict) else node
    if _flag(ssl.get("ssl_link")):
        if engine == "postgres":
            long["sslmode"] = "require"
        elif engine == "mysql":
            long["ssl"] = True
        elif engine == "mongodb":
            long["uri_options"] = "tls=true"
    if not hosts:
        what = (node.get("instance_id") or node.get("inst_id")
                or node.get("instance_name") or node.get("inst_name"))
        _unreached(side, what, ep.get("endpoint_type") or (
            job.get("base_info") or {}).get("net_type"), notes)
    if side == "target" and (ep.get("config") or {}).get(
            "is_target_readonly") is False:
        notes.append("the target stays writable while the job runs"
                     " (is_target_readonly false): a row written there"
                     " directly reads as a difference")
    return {"engine": engine, "long": long,
            "db": str(node.get("db_name") or "")}


def _hdrs_selection(objects, notes):
    """What a DRS job takes - `object_scope` all, database or table, and
    `object_info`: `{db: {name, all, schemas: {schema: {name, all,
    tables}}, tables: {table: {name, type, all, filtered,
    filter_conditions, columns}}}}`, each `name` the one it lands under -
    as the hop says it, each table named from its database down."""
    got = _scope()
    scope = str(objects.get("object_scope") or "").lower()
    info = objects.get("object_info")
    root = (objects.get("target_root_db") or {}).get("db_name")
    if root:
        notes.append(f"the job writes what it takes into the target"
                     f" database {root}, each source database a schema in"
                     " it: migkit compares a database with a database -"
                     " give the target the hop compares, and its db_map")
    if scope == "all" or not isinstance(info, dict) or not info:
        notes.append("the job takes the whole instance; so does the hop")
        return got
    whole = True
    for db, spec in info.items():
        spec = spec if isinstance(spec, dict) else {}
        _took_db(got, db, spec.get("name"))
        schemas = spec.get("schemas") if isinstance(spec.get("schemas"),
                                                    dict) else {}
        tables = spec.get("tables") if isinstance(spec.get("tables"),
                                                  dict) else {}
        whole_db = scope == "database" or _flag(spec.get("all")) or not (
            schemas or tables)
        if whole_db:
            got["include"].append(f"{db}.*")
        whole = whole and whole_db
        for sch, s in schemas.items():
            s = s if isinstance(s, dict) else {}
            base, new_sch = f"{db}.{sch}", s.get("name")
            listed = s.get("tables") if isinstance(s.get("tables"),
                                                   dict) else {}
            whole_sch = whole_db or _flag(s.get("all")) or not listed
            if whole_sch and not whole_db:
                got["include"].append(f"{base}.*")
            if whole_sch and new_sch and new_sch != sch:
                notes.append(f"schema {base} lands as {new_sch}: migkit"
                             " renames a table by its name - not carried")
            named = {}
            for name, t in listed.items():
                _hdrs_table(got, base, sch, new_sch, name, t, named, notes,
                            take=not whole_sch)
            _hdrs_named(got, base, {} if whole_sch else named)
        named = {}
        for name, t in tables.items():
            _hdrs_table(got, db, "", None, name, t, named, notes,
                        take=not whole_db)
        if whole_db or tables:
            _hdrs_named(got, db, {} if whole_db else named)
    if whole:
        # whole databases: the databases say it
        got["include"] = []
    return got


def _hdrs_named(got, base, named):
    """The views and procedures a job names one by one at `base`, as the
    hop's `objects` names them; a kind it names none of there is all of
    it (as `_tdts_objects`: a kind not carried is a finding, never left
    out of the check)."""
    for kind in _HDRS_OBJECTS.values():
        picked = named.get(kind)
        got["objects"].setdefault(kind, []).extend(
            [f"{base}.{n}" for n in picked] if picked else [f"{base}.*"])


def _hdrs_table(got, base, sch, new_sch, name, t, named, notes, take=True):
    """One object of a job's tables: a view or procedure named, or a table
    taken (`take`; in a database or schema taken whole, only what it says
    of the table) - its new name, row filter and columns."""
    t = t if isinstance(t, dict) else {}
    kind = _HDRS_OBJECTS.get(str(t.get("type") or "table").lower())
    if kind:
        named.setdefault(kind, []).append(name)
        return
    ident = f"{base}.{name}"
    if take:
        got["include"].append(ident)
    new = t.get("name") or name
    if new != name or (new_sch and new_sch != sch):
        got["tables"][ident] = ".".join(x for x in (new_sch or sch, new)
                                        if x)
    for key, word in (("db_alias_name", "database"),
                      ("schema_alias_name", "schema")):
        if t.get(key):
            notes.append(f"{ident} lands in the {word} {t[key]}: migkit"
                         " writes a table where its own database lands -"
                         " not carried")
    conds = t.get("filter_conditions") or []
    conds = [str(c) for c in ([conds] if isinstance(conds, str) else conds)
             if str(c).strip()]
    if conds and str(t.get("filtered", True)).lower() != "false":
        if len(conds) == 1:
            got["where"][ident] = conds[0]
        else:
            notes.append(f"the row filter of {ident} is {len(conds)}"
                         " conditions, and the service does not say how it"
                         " joins them - not carried; give mapping.where")
    if t.get("config_conditions"):
        notes.append(f"the row filter of {ident} reads another table (an"
                     " advanced setting of the job's) - not carried; give"
                     " mapping.where")
    drop, rename = [], {}
    cols = t.get("columns") if isinstance(t.get("columns"), dict) else {}
    for col, c in cols.items():
        c = c if isinstance(c, dict) else {}
        if _flag(c.get("additional")):
            notes.append(f"{ident}: the job adds a column of its own"
                         f" ({c.get('name') or col}) - it reads as extra on"
                         " the target")
        elif str(c.get("filtered")).lower() == "false":
            # the service's own words: false, the column is not carried
            drop.append(col)
        elif c.get("name") and c["name"] != col:
            rename[col] = c["name"]
    rule = {k: v for k, v in (("drop", drop), ("rename", rename)) if v}
    if rule:
        got["columns"][ident] = rule


def _hdrs_v3_objects(items):
    """The v3 API's `object_infos` - `[{id, parent_id, type, name,
    alias_name, select}]`, a table naming its database (or its schema)
    as its parent, `select` true, partial or false - in the shape the v5
    API says objects, and the tables a database taken whole leaves out."""
    items = [o for o in items if isinstance(o, dict)]
    by_id = {str(o.get("id") or o.get("name")): o for o in items}

    def picked(o):
        return str(o.get("select", "")).lower()

    def kind(o):
        return str(o.get("type") or "").lower()
    info, gone = {}, []
    for o in items:
        if kind(o) == "database" and picked(o) != "false":
            db = str(o.get("name") or o.get("id"))
            info[db] = {"name": o.get("alias_name") or db,
                        "select": picked(o), "schemas": {}, "tables": {}}
    for o in items:
        parent = by_id.get(str(o.get("parent_id"))) or {}
        if kind(o) == "schema" and picked(o) != "false":
            db = str(parent.get("name") or o.get("parent_id"))
            if db in info:
                info[db]["schemas"][str(o.get("name"))] = {
                    "name": o.get("alias_name") or o.get("name"),
                    "tables": {}}
    for o in items:
        if kind(o) not in ("table", "view") or not o.get("name"):
            continue
        parent = by_id.get(str(o.get("parent_id"))) or {}
        sch = ""
        if kind(parent) == "schema":
            sch = str(parent.get("name"))
            parent = by_id.get(str(parent.get("parent_id"))) or {}
        db = str(parent.get("name") or o.get("parent_id"))
        if db not in info or (sch and sch not in info[db]["schemas"]):
            # a database or schema the job does not take
            continue
        name = str(o["name"])
        if picked(o) == "false":
            gone.append(".".join(x for x in (db, sch, name) if x))
            continue
        spot = info[db]["schemas"][sch]["tables"] if sch else info[db][
            "tables"]
        spot[name] = {"type": kind(o), "name": o.get("alias_name") or name}
    for spec in info.values():
        said = spec.pop("select")
        spec["all"] = said == "true" or (said != "partial" and not (
            spec["schemas"] or spec["tables"]))
    return {"object_scope": "table", "object_info": info}, gone


def _hdrs_v3_filters(transformed, got, notes):
    """The v3 batch detail's row filters (`data_transformation`, beside
    the jobs it answers for): a condition on the table's own columns, or
    one that reads another table - said, not carried. A filter of a
    database the job does not take is another job's."""
    for f in (transformed or {}).get("filter_conditions") or []:
        if not isinstance(f, dict):
            continue
        info = f.get("transformation_info") or {}
        kind = str(info.get("transformation_type") or "")
        for o in f.get("data_transformation_object_infos") or []:
            if not isinstance(o, dict) or not o.get("table_name"):
                continue
            if got["dbs"] and str(o.get("db_name")) not in got["dbs"]:
                continue
            ident = ".".join(str(x) for x in (
                o.get("db_name"), o.get("schema_name"), o.get("table_name"))
                if x)
            if kind == "contentConditionalFilter" and info.get("value"):
                got["where"][ident] = str(info["value"])
            elif kind == "configConditionalFilter":
                notes.append(f"the row filter of {ident} reads another table"
                             " (an advanced setting of the job's) - not"
                             " carried; give mapping.where")


def _hdrs_notes(job, notes):
    """What a DRS job's settings make a correct move look like."""
    base = job.get("base_info") or {}
    policy = job.get("policy_config") or {}
    task = str(base.get("task_type") or job.get("task_type") or "").upper()
    if task == "FULL_TRANS":
        notes.append(_ONCE)
    elif task == "INCR_TRANS":
        notes.append(_CHANGES_ONLY)
    conflict = str(policy.get("conflict_policy")
                   or job.get("conflict_policy") or "").lower()
    if conflict == "ignore":
        notes.append("the job keeps a row the target already had under the"
                     " same key (conflict_policy ignore): it reads as"
                     " different")
    if str(policy.get("filter_ddl_policy") or job.get("filter_ddl_policy")
           or "").startswith("drop_database"):
        notes.append("the job does not carry a database dropped on the"
                     " source (filter_ddl_policy drop_database): it stays"
                     " on the target")
    if policy.get("ddl_trans") is False:
        notes.append("the job does not carry structure changes (ddl_trans"
                     " false): a table altered on the source since it began"
                     " reads as different in structure")
    if policy.get("index_trans") is False:
        notes.append("the job does not carry ordinary indexes (index_trans"
                     " false): the check of the schema shows them missing")
    if base.get("multi_write") is True:
        notes.append("the job is dual-active (multi_write): both sides take"
                     " writes, so a row written on either reads as a"
                     " difference until the job carries it across")
    users = job.get("user_migration")
    moved = users.get("is_migrate_user") if isinstance(users, dict) \
        else job.get("migrate_user")
    if moved is False:
        notes.append("the job does not move accounts: the target's users"
                     " and grants are not the source's")
    if job.get("is_target_readonly") is False:
        notes.append("the target stays writable while the job runs"
                     " (is_target_readonly false): a row written there"
                     " directly reads as a difference")


# ---- Google Cloud Database Migration Service and Datastream -------------

#: the engines Database Migration Service names (`DatabaseEngine`) and
#: the engine migkit reads each as
GDMS_ENGINES = {"MYSQL": "mysql", "POSTGRESQL": "postgres",
                "SQLSERVER": "mssql", "ORACLE": "oracle"}
#: a connection profile's engine, by the key its settings sit under
_GDMS_PROFILES = {"mysql": "mysql", "postgresql": "postgres",
                  "sqlserver": "mssql", "oracle": "oracle"}
#: a Cloud SQL instance's engine, by its databaseVersion's first word
_CLOUDSQL = {"MYSQL": "mysql", "POSTGRES": "postgres",
             "SQLSERVER": "mssql"}
#: a stream's source settings: the engine, the list its objects sit in
#: and the key of its name, its tables' (collections') list and the key
#: of theirs, their columns' list, and the backfill's list of what it
#: left out
_DS_SOURCES = {
    "mysqlSourceConfig": ("mysql", "mysqlDatabases", "database",
                          "mysqlTables", "table", "mysqlColumns",
                          "mysqlExcludedObjects"),
    "postgresqlSourceConfig": ("postgres", "postgresqlSchemas", "schema",
                               "postgresqlTables", "table",
                               "postgresqlColumns",
                               "postgresqlExcludedObjects"),
    "oracleSourceConfig": ("oracle", "oracleSchemas", "schema",
                           "oracleTables", "table", "oracleColumns",
                           "oracleExcludedObjects"),
    "sqlServerSourceConfig": ("mssql", "schemas", "schema", "tables",
                              "table", "columns",
                              "sqlServerExcludedObjects"),
    "mongodbSourceConfig": ("mongodb", "databases", "database",
                            "collections", "collection", None,
                            "mongodbExcludedObjects")}


def _gcp_kind(d):
    """Which of Google Cloud's things a resource is - a migration job, a
    stream, either service's connection profile, a profile of neither
    kind migkit reads - by its fields, its name
    (`projects/p/locations/l/migrationJobs/j`) where they say less, or
    None."""
    if not isinstance(d, dict):
        return None
    m = re.match(r"^projects/[^/]+/locations/[^/]+/(\w+)/[^/]+$",
                 str(d.get("name") or ""))
    path = m.group(1) if m else ""
    if path == "migrationJobs" or (
            str(d.get("type")) in ("ONE_TIME", "CONTINUOUS")
            and (isinstance(d.get("sourceDatabase"), dict)
                 or ("source" in d and "destination" in d))):
        return "job"
    if path == "streams" or (isinstance(d.get("sourceConfig"), dict)
                             and isinstance(d.get("destinationConfig"),
                                            dict)):
        return "stream"
    if any(isinstance(d.get(k), dict) for k in list(_GDMS_PROFILES)
           + ["cloudsql", "alloydb"]):
        return "dms-profile"
    if any(k.endswith("Profile") and isinstance(v, dict)
           for k, v in d.items()):
        return "stream-profile"
    return "profile" if path == "connectionProfiles" else None


def _gcp_items(data):
    """The resources a file holds: one, a JSON list of them, or the lists
    the APIs answer with (`migrationJobs`, `streams`,
    `connectionProfiles`) side by side."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    items = [data] if _gcp_kind(data) else []
    for key in ("migrationJobs", "streams", "connectionProfiles"):
        if isinstance(data.get(key), list):
            items += data[key]
    return items


def _gcp_holds(data, main, beside):
    kinds = [_gcp_kind(i) for i in _gcp_items(data)]
    return main in kinds and all(k in (main,) + beside for k in kinds)


def _is_google_dms(data):
    return _gcp_holds(data, "job", ("dms-profile", "profile"))


def _is_datastream(data):
    return _gcp_holds(data, "stream", ("stream-profile", "profile"))


def _gcp_one(data, kind, word):
    got = [i for i in _gcp_items(data) if _gcp_kind(i) == kind]
    if len(got) != 1:
        names = ", ".join(_leaf(i.get("name")) or str(i.get("displayName"))
                          for i in got[:6])
        raise SystemExit(f"the file holds {len(got)} {word}s ({names}); give"
                         f" it one (a hop is one {word})")
    return got[0]


def _gcp_profile(ref, data, kinds):
    """The connection profile a job names, from those beside it: by its
    whole resource name, or by its own."""
    if not ref:
        return None
    mine = [p for p in _gcp_items(data) if _gcp_kind(p) in kinds]
    return next((p for p in mine if str(p.get("name")) == str(ref)),
                None) or next((p for p in mine
                               if _leaf(p.get("name")) == _leaf(ref)), None)


def from_google_dms(data, stem="hop"):
    """A hop that checks what a Google Cloud Database Migration Service
    job takes, read from the job as the service says it (`gcloud
    database-migration migration-jobs describe --format=json`, or a list
    of them) and, beside it, the connection profiles it names - as
    `{"migrationJobs": [...], "connectionProfiles": [...]}` or a JSON
    list of the resources: its two sides from their profiles (one whose
    profile the file does not hold is left for the guide to ask), its
    databases, schemas and tables from `objectsConfig` (a SQL Server
    job's databases from its backups), goal `verify`, and what the job's
    type and the service's documented limits make a correct move look
    like."""
    job = _gcp_one(data, "job", "migration job")
    notes = []
    out = Imported(_name(job.get("displayName") or _leaf(job.get("name"))
                         or stem), {}, notes)
    out.raw["goal"] = "verify"
    notes.append(_VERIFY)
    src_kind = (job.get("sourceDatabase") or {}).get("engine")
    dst_kind = (job.get("destinationDatabase") or {}).get("engine")
    src = _gdms_end(job.get("source"), data, src_kind, "source", notes)
    if not dst_kind and not job.get("conversionWorkspace"):
        # one engine on both sides unless a conversion workspace says
        # otherwise
        dst_kind = src_kind
    dst = _gdms_end(job.get("destination"), data, dst_kind, "target", notes)
    got = _gdms_selection(job, src, notes)
    _gdms_notes(job, src and src["engine"], notes)
    return _verify_sides(out, src, dst, got)


def _gdms_end(ref, data, kind, word, notes):
    """One side of a migration job from the connection profile it names:
    its engine and the long form of its address (the password never
    read), or a note where the file does not hold the profile."""
    engine = GDMS_ENGINES.get(str(kind or "").upper())
    prof = _gcp_profile(ref, data, ("dms-profile", "profile"))
    own = next((k for k in _GDMS_PROFILES
                if isinstance((prof or {}).get(k), dict)), None)
    if prof is not None:
        cloud = ((prof.get("cloudsql") or {}).get("settings") or {}).get(
            "databaseVersion")
        engine = engine or (_GDMS_PROFILES[own] if own else None) or (
            "postgres" if isinstance(prof.get("alloydb"), dict) else
            _CLOUDSQL.get(str(cloud or "").split("_")[0]))
    if prof is None:
        notes.append(f"the file holds no connection profile {_leaf(ref)} -"
                     f" the guide asks for the {word}'s address" if ref
                     else f"the file names no {word} - the guide asks for"
                     " its address")
        return {"engine": engine, "long": {}, "db": ""} if engine else None
    if engine is None:
        raise SystemExit(f"the job's {word} ({_leaf(ref)}) names no engine"
                         " migkit has")
    long, db = {}, ""
    if own:
        body = prof[own]
        if body.get("host"):
            long["host"] = str(body["host"])
        if body.get("port"):
            long["port"] = int(body["port"])
        if body.get("username"):
            long["user"] = str(body["username"])
        if own == "oracle" and body.get("databaseService"):
            long["service"] = str(body["databaseService"])
        db = str(body.get("database") or "")
        _gcp_tls(engine, str((body.get("ssl") or {}).get("type") or ""),
                 word, long, notes)
        where = body.get("cloudSqlId") or body.get("alloydbClusterId")
    else:
        sql = prof.get("cloudsql") if isinstance(prof.get("cloudsql"),
                                                 dict) else {}
        ip = sql.get("privateIp") or sql.get("publicIp")
        if ip:
            long["host"] = str(ip)
        if sql.get("privateIp") and sql.get("publicIp"):
            notes.append(f"the {word} is read at its private address; its"
                         f" public one is {sql['publicIp']}")
        where = sql.get("cloudSqlId") or (prof.get("alloydb") or {}).get(
            "clusterId")
    if not long.get("host"):
        _unreached(word, where or _leaf(prof.get("name")),
                   "its connection profile names no address", notes)
    return {"engine": engine, "long": long, "db": db}


def _gcp_tls(engine, kind, word, long, notes):
    """A profile's TLS (`REQUIRED`: encrypted, nothing checked;
    `SERVER_ONLY`, `SERVER_CLIENT`: the server's certificate checked
    against one the profile holds) as the address says it."""
    kind = kind.upper()
    if kind in ("", "NONE", "SSL_TYPE_UNSPECIFIED"):
        return
    if engine == "postgres":
        long["sslmode"] = "require"
    elif engine == "mysql":
        long["ssl"] = True
    elif engine == "mssql":
        long["encrypt"] = "require"
    if kind != "REQUIRED":
        notes.append(f"the job checks the {word}'s certificate against one"
                     " its connection profile holds: give its file in the"
                     " address (sslrootcert, ssl-ca) to have migkit check"
                     " it too")


def _gdms_selection(job, src, notes):
    """What a migration job takes: the databases, schemas and tables its
    `objectsConfig` names (`SPECIFIED_OBJECTS`), a SQL Server job's
    databases from its backups, or the whole instance."""
    got = _scope()
    cfg = (job.get("objectsConfig") or {}).get("sourceObjectsConfig") or {}
    picked = [c.get("objectIdentifier") or {} for c in
              cfg.get("objectConfigs") or [] if isinstance(c, dict)]
    backups = [b["database"] for b in (
        job.get("sqlserverHomogeneousMigrationJobConfig") or {}).get(
            "databaseBackups") or [] if isinstance(b, dict)
        and b.get("database")]
    one_db = src and src["engine"] != "mysql" and src.get("db") or ""
    if cfg.get("objectsSelectionType") == "SPECIFIED_OBJECTS" and picked:
        whole = True
        for o in picked:
            kind = str(o.get("type") or "")
            db = o.get("database") or (one_db if kind != "DATABASE"
                                       else "")
            sch, tab = o.get("schema"), o.get("table")
            if kind == "DATABASE" and db:
                _took_db(got, db)
                got["include"].append(f"{db}.*")
            elif kind in ("SCHEMA", "TABLE") and (sch or tab):
                if db:
                    _took_db(got, db)
                got["include"].append(".".join(
                    str(x) for x in (db, sch, tab if kind == "TABLE"
                                     else "*") if x))
                whole = False
            else:
                notes.append(f"an object the job names ({kind or 'of no'}"
                             " type) with no name of its level - not"
                             " carried")
        if whole:
            got["include"] = []
    elif backups:
        for db in backups:
            _took_db(got, db)
    elif job.get("conversionWorkspace") or job.get("filter"):
        said = f", picked by the filter {job['filter']}" if job.get(
            "filter") else ""
        notes.append(f"the job takes what its conversion workspace"
                     f" holds{said}: the file does not say which tables -"
                     " give the hop's include")
    else:
        notes.append("the job takes the whole instance; so does the hop")
    return got


def _gdms_notes(job, engine, notes):
    """What a migration job's type and the service's documented limits
    make a correct move look like."""
    if job.get("type") == "ONE_TIME":
        notes.append(_ONCE)
    if job.get("purpose") == "FAILBACK":
        notes.append("a failback job (purpose FAILBACK): its source is the"
                     " target an earlier migration made")
    if job.get("conversionWorkspace"):
        return
    if engine == "postgres":
        notes.append("the service does not move users and roles: the"
                     " target's are not the source's")
        notes.append("the service moves a materialized view's definition,"
                     " not its rows: it reads as empty on the target until"
                     " refreshed")
        notes.append("the service does not carry unlogged or temporary"
                     " tables: they read as missing or empty on the target")
        notes.append("a sequence's state on the target may differ from the"
                     " source's: its next value reads as different")
        if job.get("type") == "CONTINUOUS":
            notes.append("a table with no primary key carries inserts only"
                         " while the job runs: a row updated or deleted on"
                         " the source since reads as different")
    elif engine == "mysql":
        notes.append("the service does not move the mysql system database:"
                     " the target's users and grants are not the source's")


def from_datastream(data, stem="hop"):
    """A hop that checks what a Google Cloud Datastream stream takes, read
    from the stream as the service says it (`gcloud datastream streams
    describe --format=json`) and, beside it, its source's connection
    profile: the databases (schemas, where the source has them), tables
    and columns its `includeObjects` take and its `excludeObjects` leave
    out (a table's columns left out, the table kept), goal `verify`, and
    what its backfill and its destination make a correct move look like.
    It writes into BigQuery or Cloud Storage, which a hop does not name
    by a database's address: the guide asks for the target."""
    stream = _gcp_one(data, "stream", "stream")
    notes = []
    out = Imported(_name(stream.get("displayName")
                         or _leaf(stream.get("name")) or stem), {}, notes)
    out.raw["goal"] = "verify"
    notes.append(_VERIFY.replace("the job", "the stream"))
    cfg = stream.get("sourceConfig") or {}
    key = next((k for k in _DS_SOURCES if isinstance(cfg.get(k), dict)),
               None)
    if key is None:
        other = next((k[:-len("SourceConfig")] for k in cfg
                      if k.endswith("SourceConfig")), "a source")
        raise SystemExit(f"the stream reads {other}, which migkit has no"
                         " engine for")
    shape = _DS_SOURCES[key]
    src = _ds_end(cfg.get("sourceConnectionProfile"), data, shape[0], notes)
    got = _ds_selection(cfg[key], shape, src["db"], notes)
    _ds_notes(stream, shape, src["db"], notes)
    return _verify_sides(out, src, None, got)


def _ds_end(ref, data, engine, notes):
    """A stream's source from its connection profile: the long form of
    its address (the password never read)."""
    prof = _gcp_profile(ref, data, ("stream-profile", "profile"))
    if prof is None:
        notes.append(f"the file holds no connection profile {_leaf(ref)} -"
                     " the guide asks for the source's address" if ref
                     else "the file names no source - the guide asks for"
                     " its address")
        return {"engine": engine, "long": {}, "db": ""}
    body = next((v for k, v in prof.items() if k.endswith("Profile")
                 and isinstance(v, dict)), {})
    long = {}
    if engine == "mongodb":
        hosts = [f"{a['hostname']}:{a['port']}" if a.get("port")
                 else str(a["hostname"])
                 for a in body.get("hostAddresses") or []
                 if isinstance(a, dict) and a.get("hostname")]
        if len(hosts) > 1:
            long["hosts"] = hosts
        elif hosts:
            long["host"], port = _host_port(hosts[0])
            if port:
                long["port"] = port
        if body.get("replicaSet"):
            long["uri_options"] = f"replicaSet={body['replicaSet']}"
        if "srvConnectionFormat" in body:
            notes.append("the source is named by a DNS seed list"
                         " (mongodb+srv): give its address as one")
    else:
        if body.get("hostname"):
            long["host"] = str(body["hostname"])
        if body.get("port"):
            long["port"] = int(body["port"])
        if engine == "oracle" and body.get("databaseService"):
            long["service"] = str(body["databaseService"])
    if body.get("username"):
        long["user"] = str(body["username"])
    if isinstance(body.get("sslConfig"), dict) and body["sslConfig"]:
        _gcp_tls(engine, "SERVER_ONLY", "source", long, notes)
    if not (long.get("host") or long.get("hosts")):
        _unreached("source", _leaf(prof.get("name")),
                   "its connection profile names no address", notes)
    elif isinstance(prof.get("forwardSshConnectivity"), dict):
        notes.append("the stream reaches the source through an SSH tunnel"
                     " (forwardSshConnectivity): migkit needs the same"
                     " way in")
    return {"engine": engine, "long": long,
            "db": str(body.get("database") or "")}


def _ds_walk(objects, shape, one_db):
    """[(base, [(table, [column])])] of a stream's include or exclude
    objects, each named from its database down - a source with schemas'
    schemas inside the profile's one database; a name left empty is every
    one (`*`)."""
    engine, tops, top_key, tables_key, table_key, cols_key, _ = shape
    flat = engine in ("mysql", "mongodb", "oracle")
    out = []
    for t in (objects or {}).get(tops) or []:
        if not isinstance(t, dict):
            continue
        top = str(t.get(top_key) or "") or "*"
        base = top if flat or not one_db else f"{one_db}.{top}"
        tables = []
        for x in t.get(tables_key) or []:
            if isinstance(x, dict) and x.get(table_key):
                cols = [str(c["column"]) for c in (
                    x.get(cols_key) or [] if cols_key else [])
                    if isinstance(c, dict) and c.get("column")]
                tables.append((str(x[table_key]), cols))
        out.append((base, tables))
    return out


def _ds_selection(cfg, shape, one_db, notes):
    """What a stream takes, as the hop says it: its databases (or the
    profile's one database), `include` where it takes less than whole
    databases, the columns it keeps, and `exclude` - a table its
    `excludeObjects` names with columns keeps the table and drops those
    columns."""
    flat = shape[0] in ("mysql", "mongodb", "oracle")
    got = _scope()
    if one_db and not flat:
        _took_db(got, one_db)
    taken = _ds_walk(cfg.get("includeObjects"), shape, one_db)
    if not taken:
        notes.append("the stream takes everything its source has; so does"
                     " the hop")
    whole = True
    for base, tables in taken:
        if flat and base != "*":
            _took_db(got, base)
        if not tables:
            got["include"].append(f"{base}.*")
            whole = whole and flat and base != "*"
            continue
        whole = False
        for name, cols in tables:
            ident = f"{base}.{name}"
            got["include"].append(ident)
            if cols:
                got["columns"][ident] = {"keep": cols}
    if whole:
        got["include"] = []
    for base, tables in _ds_walk(cfg.get("excludeObjects"), shape, one_db):
        if not tables:
            got["exclude"].append(f"{base}.*")
        for name, cols in tables:
            ident = f"{base}.{name}"
            if not cols:
                got["exclude"].append(ident)
                continue
            rule = got["columns"].setdefault(ident, {})
            if "keep" in rule:
                rule["keep"] = [c for c in rule["keep"] if c not in cols]
            else:
                rule.setdefault("drop", []).extend(cols)
    return got


def _ds_notes(stream, shape, one_db, notes):
    """What a stream's backfill and destination make a correct move look
    like."""
    if "backfillNone" in stream:
        notes.append(_CHANGES_ONLY.replace("the job", "the stream"))
    left = (stream.get("backfillAll") or {}).get(shape[6])
    names = [f"{b}.{t}" for b, ts in _ds_walk(left, shape, one_db)
             for t, _ in ts] + [f"{b}.*" for b, ts in _ds_walk(
                 left, shape, one_db) if not ts]
    if names:
        notes.append("the stream did not copy the rows that were there"
                     f" before it began of {', '.join(names[:8])}"
                     f"{' ...' if len(names) > 8 else ''}: they are on the"
                     " target only if they were there already")
    out = stream.get("destinationConfig") or {}
    lake = out.get("bigqueryDestinationConfig")
    if isinstance(lake, dict):
        notes.append("the stream writes into BigQuery: give as the target"
                     " the dataset the check compares, each source table a"
                     " table there")
        if "appendOnly" in lake:
            notes.append("the stream appends each change as a row of its"
                         " own (appendOnly): the tables there are a history"
                         " of the source's rows, not the rows themselves")
    elif isinstance(out.get("gcsDestinationConfig"), dict):
        notes.append("the stream writes files into Cloud Storage, which the"
                     " check does not read as a database: give as the"
                     " target the database the files are loaded into")
    else:
        notes.append("the file names no target - the guide asks for its"
                     " address")


# ---- Azure Database Migration Service ------------------------------------

#: a classic project task's taskType (as the API spells it, read without
#: case): the engines of its two sides, and whether it follows the source
#: until the cutover (None: its input says)
AZDMS_TASKS = {
    "migrate.sqlserver.sqldb": ("mssql", "mssql", False),
    "migrate.sqlserver.azuresqldb.sync": ("mssql", "mssql", True),
    "migrate.sqlserver.azuresqldbmi": ("mssql", "mssql", False),
    "migrate.sqlserver.azuresqldbmi.sync.lrs": ("mssql", "mssql", True),
    "migrate.mysql.azuredbformysql": ("mysql", "mysql", False),
    "migrate.mysql.azuredbformysql.sync": ("mysql", "mysql", True),
    "migrate.postgresql.azuredbforpostgresql.syncv2": ("postgres",
                                                       "postgres", True),
    "migrate.oracle.azuredbforpostgresql.sync": ("oracle", "postgres",
                                                 True),
    "migrate.mongodb": ("mongodb", "mongodb", None)}
#: a database migration's kind and the engine both its sides run
AZDMS_KINDS = {"SqlDb": "mssql", "SqlMi": "mssql", "SqlVm": "mssql",
               "MongoToCosmosDbMongo": "mongodb"}


def _az_listed(data):
    """The resources a list answers with (`value`, or a JSON list), or
    None."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("value"), list):
        return data["value"]
    return None


def _az_like(res):
    if not isinstance(res, dict):
        return False
    if str(res.get("type") or "").lower().startswith(
            "microsoft.datamigration/"):
        return True
    props = res["properties"] if isinstance(res.get("properties"),
                                            dict) else res
    if str(props.get("taskType") or "").lower().startswith("migrate.") \
            and isinstance(props.get("input"), dict):
        return True
    return props.get("kind") in AZDMS_KINDS and any(
        k in props for k in ("sourceDatabaseName", "sourceSqlConnection",
                             "collectionList", "sourceMongoConnection"))


def _is_azure_dms(data):
    many = _az_listed(data)
    if many is not None:
        return bool(many) and all(_az_like(r) for r in many)
    return _az_like(data)


def from_azure_dms(data, stem="hop"):
    """A hop that checks what an Azure Database Migration Service
    migration takes, read from the resource as the API answers it: a
    classic project task (`az dms project task show`; its
    `properties.input`: the two connections, `selectedDatabases` with
    their target names and the tables each takes - SQL Server, MySQL,
    PostgreSQL and Oracle sources - or a MongoDB task's databases and
    collections), or a database migration of the newer service (SQL
    Database, Managed Instance, SQL Server on a VM: `sourceDatabaseName`,
    the resource's own name the target database, `tableList`; MongoDB to
    Cosmos DB: `collectionList`). The same hop `from_tencent_dts` makes:
    goal `verify`, what it takes and the names it lands under, and what
    the migration's settings make a correct move look like."""
    many = _az_listed(data)
    if many is not None:
        if len(many) != 1:
            names = ", ".join(str(r.get("name")) for r in many[:6]
                              if isinstance(r, dict))
            raise SystemExit(f"the file holds {len(many)} migrations"
                             f" ({names}); give it one (a hop is one"
                             " migration)")
        data = many[0]
    props = data["properties"] if isinstance(data.get("properties"),
                                             dict) else data
    notes = []
    out = Imported(_name(data.get("name") or stem), {}, notes)
    if props.get("taskType"):
        src, dst, got = _az_task(props, notes)
    elif props.get("kind") in AZDMS_KINDS:
        src, dst, got = _az_migration(data, props, notes)
    else:
        raise SystemExit(f"{data.get('name') or 'the file'}: a"
                         f" {data.get('type') or 'resource'} of the"
                         " migration service, not a migration - save the"
                         " migration task, or the database migration")
    out.raw["goal"] = "verify"
    notes.insert(0, _VERIFY.replace("the job", "the migration"))
    return _verify_sides(out, src, dst, got)


def _az_task(props, notes):
    """(source, target, scope) of a classic project task."""
    kind = str(props["taskType"])
    spec = AZDMS_TASKS.get(kind.lower())
    if spec is None:
        raise SystemExit(f"{kind}: a task that does not migrate a database -"
                         " save the project's migration task (its taskType"
                         " begins Migrate.)")
    src_e, dst_e, online = spec
    task = props.get("input") or {}
    if src_e == "mongodb":
        src = _az_mongo(task.get("source"), "source", notes)
        dst = _az_mongo(task.get("target"), "target", notes)
        got = _az_collections(task.get("databases"), notes)
        online = str(task.get("replication") or "OneTime") == "Continuous"
    else:
        src = _az_end(task.get("sourceConnectionInfo"), src_e, "source",
                      notes)
        dst = _az_end(task.get("targetConnectionInfo"), dst_e, "target",
                      notes)
        got = _az_selected(task.get("selectedDatabases") or [], src_e,
                           kind.lower(), notes)
    if not online:
        notes.append(_ONCE.replace("the job", "the task"))
    if kind.lower().startswith("migrate.sqlserver.azuresqldbmi"):
        logins = [str(x) for x in task.get("selectedLogins") or []]
        notes.append(f"the task moves the logins {', '.join(logins[:8])}"
                     " and no other: the target's other logins are not the"
                     " source's" if logins else "the task moves no logins:"
                     " the target's logins are not the source's")
    if src_e == "postgres":
        notes.append("the task does not carry a sequence's state: its next"
                     " value on the target reads as different until it is"
                     " set again")
    return src, dst, got


def _az_end(info, engine, word, notes):
    """One side of an Azure migration from its connection info (the
    password never read): a SQL Server's `dataSource` -
    `tcp:host\\instance,port` - a server's name and port, an Oracle
    source's EZConnect `host:port/service`, or a managed instance named
    only by its resource id."""
    if not isinstance(info, dict):
        notes.append(f"the file names no {word} - the guide asks for its"
                     " address")
        return {"engine": engine, "long": {}, "db": ""}
    long = {}
    if info.get("type") == "MiSqlConnectionInfo" or (
            info.get("managedInstanceResourceId")
            and not info.get("dataSource")):
        _unreached(word, _leaf(info.get("managedInstanceResourceId")),
                   "a managed instance named by its resource id", notes)
    elif engine == "oracle":
        m = re.match(r"^(?://)?([^:/\s]+)(?::(\d+))?/([^\s:/]+)",
                     str(info.get("dataSource") or ""))
        if m:
            long["host"] = m.group(1)
            if m.group(2):
                long["port"] = int(m.group(2))
            long["service"] = m.group(3)
        else:
            notes.append(f"the {word} is named by"
                         f" {info.get('dataSource') or 'nothing'}, not a"
                         " host and service - the guide asks for them")
    else:
        where = re.sub(r"^[a-z]+:", "", str(info.get("dataSource")
                                            or info.get("serverName")
                                            or ""), flags=re.I)
        host, _, port = where.partition(",")
        host, _, instance = host.partition("\\")
        if instance:
            notes.append(f"the {word} is the named instance {instance} of"
                         f" {host}: give the port it listens on")
        if host.strip():
            long["host"] = host.strip()
        port = port.strip() or info.get("port")
        if port and str(port).isdigit():
            long["port"] = int(port)
    if info.get("userName"):
        long["user"] = str(info["userName"])
    if info.get("encryptConnection") is True:
        if engine == "postgres":
            long["sslmode"] = "require"
        elif engine == "mysql":
            long["ssl"] = True
        elif engine == "mssql":
            long["encrypt"] = "require"
    elif info.get("encryptConnection") is False:
        notes.append(f"the migration reaches the {word} without encrypting"
                     " the connection (encryptConnection false)")
    if info.get("trustServerCertificate") is True:
        notes.append(f"the migration takes the {word}'s certificate"
                     " unchecked (trustServerCertificate)")
    auth = str(info.get("authentication") or "")
    if auth and auth not in ("SqlAuthentication", "None"):
        notes.append(f"the migration signs in to the {word} by {auth}: the"
                     " guide asks how migkit signs in")
    return {"engine": engine, "long": long,
            "db": str(info.get("databaseName") or "")}


def _az_selected(selected, engine, kind, notes):
    """The databases a task names (`selectedDatabases`) and their target
    names, and in each the tables it takes: a `tableMap` of source table
    to target table (`schema.table` on SQL Server, `database.table` on
    MySQL), the tables a PostgreSQL task selects (`schema.table`), a SQL
    Server sync task's schema - none of these, the whole database. An
    Oracle task's database is its schema."""
    got = _scope()
    whole = True
    for d in selected:
        if not isinstance(d, dict):
            continue
        db = d.get("schemaName") if engine == "oracle" else d.get("name")
        if not db:
            notes.append("a database the task names with no name - not"
                         " carried")
            continue
        _took_db(got, db, d.get("restoreDatabaseName")
                 or d.get("targetDatabaseName"))
        pairs = list((d.get("tableMap") or {}).items()) if isinstance(
            d.get("tableMap"), dict) else []
        for t in d.get("selectedTables") or []:
            name = t.get("name") if isinstance(t, dict) else t
            if name:
                pairs.append((name, None))
        if not pairs:
            if engine == "mssql" and d.get("schemaName"):
                got["include"].append(f"{db}.{d['schemaName']}.*")
                whole = False
            else:
                got["include"].append(f"{db}.*")
            continue
        whole = False
        for src, dst in pairs:
            ident, new = _az_table(db, engine, src, dst)
            if ident:
                got["include"].append(ident)
            if new:
                got["tables"][ident] = new
        if str(d.get("caseManipulation") or "").lower() == "tolower":
            got["names"] = {"tables": {"case": "lower"}}
            notes.append(f"the task writes {db}'s names in lower case: the"
                         " hop's name rule lowers its tables'; a schema's"
                         " is not carried")
    if whole:
        got["include"] = []
    return got


def _az_table(db, engine, src, dst):
    """(the table fully named, its new name or None) of one entry of a
    task's tables - a MySQL or Oracle table named from its database
    (schema), the rest from the database through the schema."""
    s = _dotted(src)
    if not s:
        return None, None
    flat = engine in ("mysql", "oracle")
    sch = "" if flat or len(s) < 2 else s[-2]
    ident = ".".join(x for x in (db, sch, s[-1]) if x)
    if not dst:
        return ident, None
    t = _dotted(dst) or s
    if flat:
        return ident, t[-1] if t[-1] != s[-1] else None
    new_sch = t[-2] if len(t) >= 2 else sch
    if (new_sch, t[-1]) == (sch, s[-1]):
        return ident, None
    return ident, ".".join(x for x in (new_sch, t[-1]) if x)


def _az_collections(databases, notes):
    """A MongoDB task's databases and the collections of each it takes
    (`{db: {collections: {name: ...}}}`); a database naming none, all of
    it."""
    got = _scope()
    whole = True
    for db, spec in (databases or {}).items():
        _took_db(got, db)
        colls = spec.get("collections") if isinstance(spec, dict) else None
        if not colls:
            got["include"].append(f"{db}.*")
            continue
        whole = False
        got["include"] += [f"{db}.{c}" for c in colls]
    if not databases:
        notes.append("the task takes the whole instance; so does the hop")
    if whole:
        got["include"] = []
    return got


def _az_mongo(info, word, notes):
    """A MongoDB side: its connection string with any password left out,
    or its host and port; a storage container of files is no server."""
    if not isinstance(info, dict):
        notes.append(f"the file names no {word} - the guide asks for its"
                     " address")
        return {"engine": "mongodb", "long": {}, "db": ""}
    url = str(info.get("connectionString") or "")
    if url.startswith(("mongodb://", "mongodb+srv://")):
        if re.match(r"^mongodb(\+srv)?://[^:/@]*:[^/@]*@", url):
            url = re.sub(r"(//[^:/@]*):[^/@]*@", r"\1@", url, count=1)
            notes.append(f"the {word}'s connection string's password was"
                         " left out")
        base, _, query = url.partition("?")
        head = re.match(r"^(mongodb(?:\+srv)?://[^/]*)", base).group(1)
        user = info.get("userName")
        if user and "@" not in head:
            from urllib.parse import quote
            head = head.replace("://", f"://{quote(str(user), safe='')}@",
                                1)
        return {"engine": "mongodb", "long": {}, "uri": (head, query),
                "db": ""}
    long = {}
    if info.get("host"):
        long["host"] = str(info["host"])
        if info.get("port"):
            long["port"] = int(info["port"])
        if info.get("userName"):
            long["user"] = str(info["userName"])
        if info.get("useSsl") is True:
            long["uri_options"] = "tls=true"
    elif url:
        notes.append(f"the {word} is files in a storage container, not a"
                     " server: give the database they were taken from")
    else:
        notes.append(f"the file names no {word} address - the guide asks"
                     " for it")
    return {"engine": "mongodb", "long": long, "db": ""}


def _az_migration(res, props, notes):
    """(source, target, scope) of a database migration of the newer
    service: one SQL Server database, its target database the resource's
    own name, the tables of `tableList` (all of them where it names
    none); or MongoDB collections into Cosmos DB, each with the names it
    lands under."""
    kind = props["kind"]
    got = _scope()
    if AZDMS_KINDS[kind] == "mongodb":
        src = _az_mongo(props.get("sourceMongoConnection"), "source", notes)
        dst = _az_mongo(props.get("targetMongoConnection"), "target", notes)
        whole = True
        for c in props.get("collectionList") or []:
            if not isinstance(c, dict) or not c.get("sourceDatabase"):
                continue
            db, coll = c["sourceDatabase"], c.get("sourceCollection")
            _took_db(got, db, c.get("targetDatabase"))
            if not coll:
                got["include"].append(f"{db}.*")
                continue
            whole = False
            got["include"].append(f"{db}.{coll}")
            if c.get("targetCollection") and c["targetCollection"] != coll:
                got["tables"][f"{db}.{coll}"] = c["targetCollection"]
        if not got["dbs"]:
            notes.append("the migration names no collections - the guide"
                         " asks which")
        if whole:
            got["include"] = []
        return src, dst, got
    src = _az_end(props.get("sourceSqlConnection"), "mssql", "source",
                  notes)
    if isinstance(props.get("targetSqlConnection"), dict):
        dst = _az_end(props["targetSqlConnection"], "mssql", "target",
                      notes)
    else:
        dst = {"engine": "mssql", "long": {}, "db": ""}
        _unreached("target", _leaf(props.get("scope")), {
            "SqlDb": "an Azure SQL server", "SqlMi": "a managed instance",
            "SqlVm": "SQL Server on a virtual machine"}[kind]
            + " named by its resource id", notes)
    db = props.get("sourceDatabaseName")
    if db:
        named = res.get("name") if isinstance(res.get("properties"),
                                              dict) else None
        _took_db(got, db, named)
        for t in props.get("tableList") or []:
            parts = _dotted(t)
            if parts:
                got["include"].append(".".join([str(db)] + parts[-2:]))
    else:
        notes.append("the migration names no source database - the guide"
                     " asks for it")
    if (props.get("offlineConfiguration") or {}).get("offline") is True:
        notes.append(_ONCE.replace("the job", "the migration"))
    if kind in ("SqlMi", "SqlVm"):
        notes.append("the service does not move logins: the target's"
                     " logins are not the source's")
    if props.get("targetDatabaseCollation"):
        notes.append("the target database is made with the collation"
                     f" {props['targetDatabaseCollation']}: text sorts and"
                     " compares there in its way")
    return src, dst, got


# ---- a change-capture connector ------------------------------------------

#: the connector class's package and the engine it reads
CONNECTORS = {"postgresql": "postgres", "postgres": "postgres",
              "mysql": "mysql", "mariadb": "mysql", "mongodb": "mongodb",
              "sqlserver": "mssql", "oracle": "oracle", "db2": "db2",
              "jdbc": "jdbc"}
SNAPSHOT_GOALS = {"initial": "keep-in-sync", "when_needed": "keep-in-sync",
                  "always": "keep-in-sync", "never": "keep-in-sync",
                  "no_data": "keep-in-sync", "schema_only":
                  "keep-in-sync", "initial_only": "copy"}


def _is_connector(data):
    return isinstance(data, dict) and (
        "connector.class" in data or "connector.class" in (
            data.get("config") or {}))


def _properties(text):
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        m = re.match(r"([^=:\s]+)\s*[=:]?\s*(.*)$", line)
        # read as the connector's own runtime reads the file: a backslash
        # keeps the character after it, so `a\\.b` is the regex `a\.b`
        out[m.group(1)] = re.sub(r"\\(.)", r"\1", m.group(2))
    return out


def _regexes(value):
    return [p.strip() for p in str(value or "").split(",") if p.strip()]


def as_glob(regex):
    """A simple regex as the shell pattern it means, or None where it
    means something a shell pattern cannot say."""
    text = str(regex).replace("\\.", "\x00").replace(".*", "*")
    if re.search(r"[\[\]\(\)\{\}\|\^\$\+\?\\.]", text):
        return None
    return text.replace("\x00", ".")


def from_connector(data, stem="hop"):
    cfg = dict(data.get("config") or data) if isinstance(data, dict) else {}
    name = _name(data.get("name") or cfg.get("name") or stem) \
        if isinstance(data, dict) else stem
    cls = str(cfg.get("connector.class", ""))
    kind = next((CONNECTORS[p] for p in cls.lower().split(".")
                 if p in CONNECTORS), None)
    if kind is None:
        # the class is not echoed: it names the program behind it
        raise SystemExit("the file's connector.class reads no database"
                         " migkit has an engine for")
    notes = []
    out = Imported(name, {}, notes)
    if cfg.get("database.password") or cfg.get("connection.password"):
        notes.append("the file holds a password; migkit does not copy it"
                     " - the guide asks for it")
    if kind == "jdbc":
        url = str(cfg.get("connection.url", ""))
        m = re.match(r"jdbc:(\w+)://([^/?]+)/?([^?;]*)", url)
        if not m:
            raise SystemExit(f"connection.url {url}: not an address migkit"
                             " reads")
        engine = {"postgresql": "postgres", "mysql": "mysql",
                  "mariadb": "mysql", "sqlserver": "mssql"}.get(
                      m.group(1), m.group(1))
        host, _, port = m.group(2).partition(":")
        ep = {"host": host, **({"port": int(port)} if port else {}),
              **({"user": cfg["connection.username"]}
                 if cfg.get("connection.username") else {})}
        out.raw["target"] = _side(engine, ep, [m.group(3)] if m.group(3)
                                  else [])
        notes.append("a sink: the file says where changes go, not where"
                     " they come from - the guide asks for the source")
        return out
    ep, dbs = {}, []
    if kind == "mongodb":
        url = str(cfg.get("mongodb.connection.string") or "")
        if re.match(r"^mongodb(\+srv)?://[^:/@]*:[^/@]*@", url):
            url = re.sub(r"(//[^:/@]*):[^/@]*@", r"\1@", url, count=1)
            notes.append("the connection string's password was left out")
        base, _, query = url.partition("?")
        head = re.match(r"^(mongodb(?:\+srv)?://[^/]*)", base)
        dbs = _databases(cfg, out)
        if head:
            out.raw["source"] = (head.group(1)
                                 + ("/" + ",".join(dbs) if dbs else "")
                                 + (f"?{query}" if query else ""))
        for pat in _regexes(cfg.get("collection.include.list")):
            out.include.append(("regex", pat))
    else:
        if cfg.get("database.hostname"):
            ep["host"] = cfg["database.hostname"]
        if cfg.get("database.port"):
            ep["port"] = int(cfg["database.port"])
        if cfg.get("database.user"):
            ep["user"] = cfg["database.user"]
        if kind == "postgres" and cfg.get("database.sslmode"):
            ep["sslmode"] = cfg["database.sslmode"]
        if kind == "mysql" and str(cfg.get("database.ssl.mode", "")).lower() \
                == "required":
            ep["ssl"] = True
        if kind in ("postgres", "oracle", "db2") and cfg.get(
                "database.dbname"):
            dbs = [cfg["database.dbname"]]
        if kind == "mssql" and cfg.get("database.names"):
            dbs = _regexes(cfg["database.names"])
        if kind == "mysql":
            dbs = _databases(cfg, out)
        if kind == "oracle":
            ep["service"] = dbs.pop() if dbs else None
            ep = {k: v for k, v in ep.items() if v}
        out.raw["source"] = _side(kind, ep, dbs)
        for key in ("schema.include.list", "table.include.list"):
            for pat in _regexes(cfg.get(key)):
                out.include.append(("regex", pat if key.startswith(
                    "table") else pat + r"\..*"))
    exclude = []
    for key in ("table.exclude.list", "schema.exclude.list",
                "collection.exclude.list", "database.exclude.list"):
        for pat in _regexes(cfg.get(key)):
            whole = pat if key.startswith(("table", "collection")) \
                else pat + r"\..*" if key.startswith("schema") else pat
            got = as_glob(whole)
            if got:
                exclude.append(got)
            else:
                out.exclude_re.append(whole)
    if exclude:
        out.raw["exclude"] = exclude
    if cfg.get("column.exclude.list"):
        notes.append("column.exclude.list: migkit leaves a column out by"
                     " the table it is in (mapping.columns.<table>.drop) -"
                     " not carried")
    mode = str(cfg.get("snapshot.mode", "initial")).lower()
    goal = SNAPSHOT_GOALS.get(mode, "keep-in-sync")
    if mode in ("never", "no_data", "schema_only"):
        notes.append(f"snapshot.mode {mode} carried only changes; a hop"
                     " that follows a source copies it first, so the"
                     " target is whole")
    if goal != "copy":
        out.raw["goal"] = goal
    notes.append("the connector writes to a stream; the guide asks where"
                 " the data should go")
    return _carried(out)


def _databases(cfg, out):
    """The connector's `database.include.list`: its names, where each is
    one; else every database, the patterns carried as the hop's include -
    unless a table or collection list says which, each of its patterns
    naming the database too. A pattern was left out without a word, and
    the hop moved every database (audit 2026-10-01, K16)."""
    pats = _regexes(cfg.get("database.include.list"))
    globs = [as_glob(p) for p in pats]
    if all(g and "*" not in g for g in globs):
        return sorted(set(globs))
    if cfg.get("table.include.list") or cfg.get("collection.include.list"):
        out.notes.append("database.include.list holds patterns: every"
                         " database is read, and the table list - which"
                         " names each table's database - says which tables")
        return []
    for p in pats:
        out.include.append(("regex", p + r"\..*"))
    out.notes.append("database.include.list holds patterns: every database"
                     " is read, and the hop's include keeps the tables of"
                     " those they match")
    return []


def _carried(out):
    """The task's own scope carried into the hop: what it took as the
    hop's `include` where every entry is a name or a shell pattern - so
    the hop holds to it whether or not the guide reaches the source - and
    a note for every pattern that cannot be (a regular expression, a
    search), taken or left out, since only the guide can then apply it.
    A list of what the task took was applied only against the source's
    tables as the guide read them: written where the source could not be
    reached, the hop moved every table, and a wildcard pattern was left
    out without a word (audit 2026-10-01, K16)."""
    taken = [p if k == "glob" else as_glob(p) if k == "regex" else None
             for k, p in out.include]
    if out.include and all(taken):
        out.raw["include"] = list(dict.fromkeys(
            list(out.raw.get("include") or []) + taken))
        out.include = []
    for (kind, pat), got in zip(out.include, taken):
        if got is None:
            out.notes.append(
                f"the task takes the tables {_said(kind, pat)}, which a"
                " hop's include cannot say: the guide leaves out the"
                " source's other tables when it reaches the source - where"
                " it cannot, the hop takes every table until its include"
                " or exclude says which")
    for pat in out.exclude_re:
        out.notes.append(
            f"the task leaves out the tables matching the regular"
            f" expression {pat}, which a hop's exclude cannot say: the"
            " guide leaves them out when it reaches the source - where it"
            " cannot, the hop takes them until its exclude names them")
    return out


def _said(kind, pat):
    return (f"the regular expression {pat} matches" if kind == "regex"
            else f"whose names hold {pat}")


# ---- a load-command file -------------------------------------------------

def _strip_comments(text):
    return re.sub(r"(?m)--.*$", "", text)


#: `~/re/`, `~<re>`, `~[re]`, `~{re}`, `~(re)`, `~|re|`, `~"re"`, `~'re'`
_REGEX = re.compile(r"~(?:/([^/]*)/|<([^>]*)>|\[([^\]]*)\]|\{([^}]*)\}"
                    r"|\(([^)]*)\)|\|([^|]*)\||\"([^\"]*)\"|'([^']*)')")
_LITERAL = re.compile(r"'([^']*)'")


def _matching(clause):
    """[(kind, pattern)] of a `... NAMES MATCHING a, ~/b/` list."""
    out = []
    pos = 0
    while pos < len(clause):
        m = _REGEX.match(clause, pos)
        if m:
            # matched anywhere in the name, as that file's regexes are
            out.append(("search", next(g for g in m.groups()
                                       if g is not None)))
            pos = m.end()
            continue
        m = _LITERAL.match(clause, pos)
        if m:
            out.append(("glob", m.group(1)))
            pos = m.end()
            continue
        pos += 1
    return out


def from_load_file(text, stem="hop"):
    body = _strip_comments(text)
    kind = re.match(r"(?is)\s*load\s+(\w+)", body).group(1).lower()
    if kind != "database":
        raise SystemExit(f"LOAD {kind.upper()} loads files into a table; a"
                         " hop moves a database - only LOAD DATABASE is"
                         " read")
    notes = []
    frm = re.search(r"(?is)\bfrom\s+(\S+)", body)
    into = re.search(r"(?is)\binto\s+(\S+)", body)
    if not frm or not into:
        raise SystemExit("LOAD DATABASE without FROM and INTO addresses")
    out = Imported(_name(stem), {}, notes)
    for side, url in (("source", frm.group(1)), ("target", into.group(1))):
        url = url.strip("'\"")
        if re.match(r"^\w+://[^/@:]*:[^/@]*@", url):
            url = re.sub(r"^(\w+://[^/@:]*):[^/@]*@", r"\1@", url)
            notes.append(f"the {side} address's password was left out")
        url = re.sub(r"^pgsql://", "postgresql://", url)
        url = re.sub(r"^sqlite:///", "sqlite:///", url)
        out.raw[side] = url
    inc = re.search(r"(?is)\bincluding\s+only\s+table\s+names\s+matching\s+"
                    r"(.+?)(?=\b(?:excluding|alter|cast|with|set|before|"
                    r"after|decoding|materialize)\b|;|$)", body)
    if inc:
        out.include += _matching(inc.group(1))
    exc = re.search(r"(?is)\bexcluding\s+table\s+names\s+matching\s+(.+?)"
                    r"(?=\b(?:including|alter|cast|with|set|before|after|"
                    r"decoding|materialize)\b|;|$)", body)
    exclude = []
    if exc:
        for kind_, pat in _matching(exc.group(1)):
            if kind_ == "glob":
                exclude.append(pat)
            else:
                out.exclude_re.append(f".*(?:{pat}).*")
    if exclude:
        out.raw["exclude"] = exclude
    tables = {}
    for m in re.finditer(r"(?is)\balter\s+table\s+names\s+matching\s+(.+?)"
                         r"\s+rename\s+to\s+'([^']*)'", body):
        got = _matching(m.group(1))
        if len(got) == 1 and got[0][0] == "glob":
            tables[got[0][1]] = m.group(2)
        else:
            notes.append(f"a rename of every table matching"
                         f" {m.group(1).strip()} - migkit renames a table"
                         " by its name; not carried")
    if tables:
        out.raw["mapping"] = {"tables": tables}
    if re.search(r"(?is)\balter\s+schema\b", body):
        notes.append("ALTER SCHEMA ... RENAME: migkit writes each source"
                     " database into the target database the target"
                     " address names - not carried")
    for word, said in (("cast", "CAST rules: migkit decides each column's"
                                " type on the target"),
                       ("before load", "BEFORE LOAD: not run"),
                       ("after load", "AFTER LOAD: not run"),
                       (r"with\b", "WITH options: migkit decides how the"
                                   " rows are loaded"),
                       (r"set\b", "SET: migkit sets its own sessions")):
        if re.search(rf"(?is)\b{word}", body):
            notes.append(said)
    return _carried(out)


def left_out(imported, ids):
    """The source's tables (`schema.table`, or `database.table` where the
    database is the schema) the task did not take: those no include
    pattern takes, and those an exclude regex does."""
    from fnmatch import fnmatch
    out = []
    for ident in ids:
        ident = str(ident)
        leaf = ident.rsplit(".", 1)[-1]

        def takes(kind, pat):
            if kind == "glob":
                return fnmatch(ident, pat) or fnmatch(leaf, pat)
            if kind == "regex":
                return re.fullmatch(pat, ident) is not None
            return re.search(pat, leaf) is not None
        if imported.include and not any(takes(k, p)
                                        for k, p in imported.include):
            out.append(ident)
        elif any(re.fullmatch(r, ident) for r in imported.exclude_re):
            out.append(ident)
    return out
