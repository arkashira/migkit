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
  look different (`from_tencent_dts`).

What migkit decides for itself - types, batch sizes, workers, how a
table is loaded - is left behind and said so; so is anything the other
tool did that migkit has no word for (a column added by expression, a
prefix added to every name), rather than carried half-way. A password in
the file is never copied: the guide asks for it and keeps it in the
keychain.

A list of tables to include is the hop's own `include` where each entry
is a name or a shell pattern; one a pattern cannot say (a regular
expression, a search) is returned as `include` and made into the hop's
`exclude` against the source's own tables when the guide connects.
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
        if _is_dms(data):
            return from_dms(data, stem)
        if _is_connector(data):
            return from_connector(data, stem)
        raise SystemExit(f"{p}: JSON, but not a replication task (it"
                         " has no TableMappings), a DTS job (no"
                         " MigrateOption, Objects, ObjectItems or"
                         " DbObject) or a"
                         " connector's configuration (no connector.class)")
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
    from .config import DEFAULT_PORTS
    got = addresses.render(engine, ep, list(dbs),
                           DEFAULT_PORTS.get(engine, 0))
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
    return out


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


def _lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def _q(column, engine):
    """A column as a filter names it: bare where it can be, which both
    sides of a cross-engine hop read alike (the filter is asked of both);
    quoted in the source's way where it cannot."""
    if re.fullmatch(r"[a-z_][a-z0-9_]*", str(column)):
        return str(column)
    if engine == "mysql":
        return "`" + str(column).replace("`", "``") + "`"
    return '"' + str(column).replace('"', '""') + '"'


def _dms_filter(filters, engine):
    """DMS's filters as one predicate: the conditions of a column any of
    them, the columns all."""
    ops = {"eq": "=", "noteq": "<>", "lte": "<=", "ste": "<=", "gte": ">="}
    parts = []
    for f in filters:
        if f.get("filter-type", "source") != "source":
            continue
        col = _q(f["column-name"], engine)
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
    ops = {}
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


def _adts_ops(ident, spec, notes, got):
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
        dbs = sorted({g for g in (as_glob(p) for p in _regexes(
            cfg.get("database.include.list"))) if g and "*" not in g})
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
            dbs = sorted({g for g in (as_glob(p) for p in _regexes(
                cfg.get("database.include.list"))) if g and "*" not in g})
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
    return out


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
    return out


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
