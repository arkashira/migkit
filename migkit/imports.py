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
  addresses, the tables it includes and excludes, its renames.

What migkit decides for itself - types, batch sizes, workers, how a
table is loaded - is left behind and said so; so is anything the other
tool did that migkit has no word for (a column added by expression, a
prefix added to every name), rather than carried half-way. A password in
the file is never copied: the guide asks for it and keeps it in the
keychain.

A list of tables to include, which migkit has no key for (a hop says what
it leaves out), is returned as `include` and made into the hop's
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
        if _is_dms(data):
            return from_dms(data, stem)
        if _is_connector(data):
            return from_connector(data, stem)
        raise SystemExit(f"{p}: JSON, but neither a replication task (it"
                         " has no TableMappings) nor a connector's"
                         " configuration (it has no connector.class)")
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
    exclude, where, tables, columns = [], {}, {}, {}
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
    mapping = {k: v for k, v in (("tables", tables), ("columns", columns),
                                 ("where", where)) if v}
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
