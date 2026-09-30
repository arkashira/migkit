"""What the DBA must do before migkit can, written for them (backlog R21).

A move that stops on `permission denied for table orders` half an hour in
has asked the operator a question they could have been told at the start,
and a DBA handed "migkit needs replication" has to work out the rest. So
each side is asked, before anything runs, for what the hop's goal needs
of it - reading every table in scope, writing the target, following the
source's changes - and what is missing is written as the exact
statements that grant it and nothing more: a table's SELECT rather than
a superuser, `rds_replication` on RDS where a server run by its owner
takes `ALTER ROLE ... REPLICATION`, a setting in the words of the cloud
that runs the side (`migkit.clouds`: a parameter group, a database flag,
a server parameter, `ALTER SYSTEM` on a server run by its owner).

`migkit init` writes them to a file per side and says where;
`migkit doctor` asks again and says what is still missing. migkit never
runs them: changing what a server allows is the DBA's to do.

The requirements are read off the server as they stand - `has_table_
privilege`, the grants the server lists, `wal_level`, `binlog_format`,
change tracking - so a grant through a role counts, and a table the hop
excludes is not asked for.
"""
import re
from dataclasses import dataclass, field

#: goals that follow the source's changes, and that write to the source
STREAMS = ("cutover", "keep-in-sync", "two-way")


@dataclass
class Need:
    """One thing a side must allow. `ok` None: it could not be asked,
    which is said as unknown rather than as present."""
    side: str
    what: str
    ok: object
    now: str = ""
    fix: list = field(default_factory=list)
    #: settings a cloud changes outside the server, as that cloud says it
    outside: list = field(default_factory=list)
    restart: bool = False
    #: whether it is what following the source's changes needs
    follow: bool = False


def _following(found):
    for n in found:
        n.follow = True
    return found


def _side_engine(eng, side):
    """(engine object, side key) that asks one side's server."""
    key = "src" if side == "source" else "dst"
    if getattr(eng, "src_engine", None) is not None:
        return (eng.src_engine if key == "src" else eng.dst_engine), key
    return eng, key


def _canon(e):
    return getattr(e, "CANON_ENGINE", "") or ""


def needs(hop, eng, goal=None, wheres=None, streaming=None):
    """Every requirement of both sides for `goal` (the hop's own where not
    given; a hop with none is a copy). `wheres`: {side: clouds.Where}.
    `streaming` forces the change-following checks on or off - the guide
    asks them whatever the goal, to say whether the source could."""
    goal = goal or getattr(hop, "goal", "") or "copy"
    follow = goal in STREAMS if streaming is None else streaming
    wheres = wheres or {}
    out = []
    try:
        dbs = list(eng.databases())
    except Exception as e:  # noqa: BLE001 - said as a need nobody could ask
        return [Need("source", "list the source's databases", None,
                     _last(e))]
    same = hop.engine != "hetero"
    for side in ("source", "target"):
        e, key = _side_engine(eng, side)
        fn = ASK.get(_canon(e))
        if fn is None:
            continue
        where = wheres.get(side)
        # two ways: the target is followed as well, into the source
        both = goal == "two-way"
        try:
            out += fn(hop, e, key, side, dbs, where,
                      read=side == "source" or goal == "verify",
                      write=side == "target" and goal != "verify",
                      follow=follow and (side == "source" or both),
                      native=same and follow)
        except Exception as err:  # noqa: BLE001 - unknown, not clean
            out.append(Need(side, "ask the server what it allows", None,
                            _last(err)))
    return out


def _last(e):
    text = str(e).strip()
    return (text.splitlines() or [type(e).__name__])[-1][:160]


def missing(found):
    return [n for n in found if n.ok is False]


# ---- how a cloud spells a setting ----------------------------------------

#: a PostgreSQL setting under the name each service gives it
PG_NAMES = {("aws", "wal_level", "logical"): ("rds.logical_replication",
                                              "1"),
            ("gcp", "wal_level", "logical"): ("cloudsql.logical_decoding",
                                              "on")}


def _setting(engine, where, name, value, restart, target_label):
    """(statements run on the server, lines said about the cloud) that set
    `name` to `value` on this side."""
    cloud = getattr(where, "cloud", "") or ""
    service = getattr(where, "service", "") or ""
    if cloud == "gcp" and service == "alloydb" and name == "wal_level":
        name, value = "alloydb.logical_decoding", "on"
    else:
        name, value = PG_NAMES.get((cloud, name, value), (name, value)) \
            if engine == "postgres" else (name, value)
    after = " (the server restarts to take it)" if restart else ""
    if cloud == "aws":
        group = ("DB cluster parameter group" if service.startswith("aurora")
                 else "DB parameter group")
        api = ("modify-db-cluster-parameter-group"
               " --db-cluster-parameter-group-name <its cluster parameter"
               " group>" if service.startswith("aurora") else
               "modify-db-parameter-group --db-parameter-group-name <its"
               " parameter group>")
        return [], [f"in {target_label}'s {group}: {name} = {value}{after}",
                    f"aws rds {api} --parameters"
                    f" \"ParameterName={name},ParameterValue={value},"
                    f"ApplyMethod={'pending-reboot' if restart else 'immediate'}\""]
    if cloud == "gcp":
        return [], [f"database flag {name}={value}{after}",
                    f"gcloud sql instances patch <instance>"
                    f" --database-flags={name}={value}"
                    "  (the list replaces every flag the instance has:"
                    " name the ones it keeps as well)"]
    if cloud == "azure":
        kind = "postgres" if engine == "postgres" else "mysql"
        return [], [f"server parameter {name} = {value}{after}",
                    f"az {kind} flexible-server parameter set"
                    " --resource-group <group> --server-name <server>"
                    f" --name {name} --value {value}"]
    if cloud == "tencent":
        return [], [f"TencentDB console, the instance's parameter settings:"
                    f" {name} = {value}{after}"]
    if cloud == "alibaba":
        return [], [f"the instance's parameters in the console, or"
                    f" `aliyun rds ModifyParameter --DBInstanceId <id>"
                    f" --Parameters '{{\"{name}\":\"{value}\"}}'"
                    f"{' --Forcerestart true' if restart else ''}`"]
    if engine == "postgres":
        return ([f"alter system set {name} = '{value}';"]
                + ([] if restart else ["select pg_reload_conf();"]),
                [f"restart the server for {name} to take effect"]
                if restart else [])
    if engine == "mysql":
        if restart:
            return [], [f"in the server's configuration file: {name} ="
                        f" {value}, then restart it"]
        return [f"set persist {name} = '{value}';"], []
    return [], [f"{name} = {value}{after}"]


# ---- PostgreSQL ----------------------------------------------------------

def _pgi(name):
    """An identifier as PostgreSQL reads it back unchanged."""
    return name if re.fullmatch(r"[a-z_][a-z0-9_]*", str(name)) else \
        '"' + str(name).replace('"', '""') + '"'


def _pg_rows(e, key, db, sql):
    return [r.split("\x1f") for r in e._psql(key, db, sql).splitlines()
            if r]


def _pg(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    me, sup, repl, createdb, version = _pg_rows(
        e, key, "postgres",
        "select current_user||chr(31)||rolsuper::int||chr(31)"
        "||rolreplication::int||chr(31)||rolcreatedb::int||chr(31)"
        "||current_setting('server_version_num') from pg_roles where"
        " rolname = current_user")[0]
    who = _pgi(me)
    super_ = sup == "1"
    label = f"the {side}"
    if read:
        for db in dbs:
            gaps = _pg_rows(
                e, key, db,
                "select n.nspname||chr(31)||c.relname||chr(31)||c.relkind"
                "||chr(31)||has_schema_privilege(n.oid, 'USAGE')::int"
                " from pg_class c join pg_namespace n on n.oid ="
                " c.relnamespace where c.relkind in ('r', 'p', 'S')"
                " and n.nspname not in ('pg_catalog', 'information_schema')"
                " and n.nspname not like 'pg\\_%' and not case c.relkind"
                " when 'S' then has_sequence_privilege(c.oid, 'SELECT')"
                " else has_table_privilege(c.oid, 'SELECT') end"
                " order by 1")
            gaps = [g for g in gaps if not hop.excluded(db, g[0], g[1])]
            schemas = sorted({g[0] for g in gaps if g[3] == "0"})
            shown = hop.target_db(db) if key == "dst" else db
            fix = [f"-- in database {shown}:"] if gaps else []
            fix += [f"grant usage on schema {_pgi(s)} to {who};"
                    for s in schemas]
            fix += [f"grant select on {'sequence ' if g[2] == 'S' else ''}"
                    f"{_pgi(g[0])}.{_pgi(g[1])} to {who};" for g in gaps]
            out.append(Need(side, f"read every table of {shown}", not gaps,
                            f"{len(gaps)} tables and sequences it may not"
                            " read" if gaps else "every one", fix))
    if key == "dst" and write:
        for db in dbs:
            name = hop.target_db(db)
            got = _pg_rows(
                e, key, "postgres",
                "select coalesce((select has_database_privilege(datname,"
                " 'CREATE')::int||chr(31)||(pg_get_userbyid(datdba) ="
                f" current_user)::int from pg_database where datname ="
                f" {_lit(name)}), 'missing')")
            if got[0][0] == "missing":
                out.append(Need(side, f"a database {name} to write into",
                                createdb == "1" or super_,
                                "it is not there" + (
                                    "; migkit's user may create it"
                                    if createdb == "1" or super_ else ""),
                                [f"create database {_pgi(name)} owner"
                                 f" {who};"]))
                continue
            can, owns = got[0]
            ok = super_ or owns == "1" or can == "1"
            out.append(Need(side, f"create what the source has in {name}",
                            ok, "the owner" if owns == "1" else
                            ("may create in it" if ok else
                             "may not create schemas in it"),
                            [f"grant create on database {_pgi(name)} to"
                             f" {who};"]))
            if ok and owns != "1" and not super_:
                pub = _pg_rows(e, key, db,
                               "select has_schema_privilege('public',"
                               " 'CREATE')::int")
                if pub and pub[0][0] == "0":
                    out.append(Need(
                        side, f"create tables in {name}.public", False,
                        "may not", [f"-- in database {name}:",
                                    f"grant usage, create on schema public"
                                    f" to {who};"]))
    if follow:
        out += _following(_pg_follow(hop, e, key, side, dbs, where, who,
                                     super_, repl == "1", native, label))
    if key == "dst" and write and native and _streams_natively(hop):
        ok = super_ or (int(version) >= 160000 and _pg_rows(
            e, key, "postgres", "select pg_has_role(current_user,"
            " 'pg_create_subscription', 'member')::int")[0][0] == "1") \
            or getattr(where, "cloud", "") == "aws" and _member(
                e, key, "rds_superuser")
        fix = ([f"grant pg_create_subscription to {who};"]
               if int(version) >= 160000 else
               ["-- before PostgreSQL 16 only a superuser creates a"
                " subscription: have one run the statement"
                f" `migkit move {hop.name} --mode cdc` shows"])
        out += _following([Need(side, "subscribe to the source's changes",
                                ok, "may" if ok else "may not", fix)])
    if key == "dst" and write and hop.engine != "hetero":
        out += _pg_extensions(hop, e, dbs, where, who)
    return out


def _streams_natively(hop):
    """Whether this hop follows its source with the servers' own
    subscription (a PostgreSQL pair), not migkit's own tail."""
    from .engines import ALIASES
    return ALIASES.get(hop.engine, hop.engine) == "postgres"


def _member(e, key, role):
    try:
        return _pg_rows(e, key, "postgres",
                        f"select pg_has_role(current_user, {_lit(role)},"
                        " 'member')::int")[0][0] == "1"
    except Exception:  # noqa: BLE001 - no such role here
        return False


def _lit(text):
    return "'" + str(text).replace("'", "''") + "'"


def _pg_follow(hop, e, key, side, dbs, where, who, super_, repl, native,
               label):
    out = []
    level = e._psql(key, "postgres", "show wal_level").strip()
    stmts, said = _setting("postgres", where, "wal_level", "logical", True,
                           label)
    out.append(Need(side, "a log that carries rows (wal_level logical)",
                    level == "logical", f"wal_level is {level}", stmts,
                    said, restart=True))
    aws = getattr(where, "cloud", "") == "aws"
    ok = super_ or repl or (aws and _member(e, key, "rds_replication"))
    out.append(Need(side, "read the change log (replication)", ok,
                    "may" if ok else "may not",
                    [f"grant rds_replication to {who};"] if aws else
                    [f"alter role {who} with replication;"]))
    free = _pg_rows(
        e, key, "postgres",
        "select current_setting('max_replication_slots')::int - (select"
        " count(*) from pg_replication_slots)||chr(31)||"
        "current_setting('max_wal_senders')::int - (select count(*) from"
        " pg_stat_replication)")[0]
    for (name, left, what) in (
            ("max_replication_slots", int(free[0]), "a free replication"
                                                    " slot"),
            ("max_wal_senders", int(free[1]), "a free WAL sender")):
        # ten: room for this hop's and a few more, where the server's own
        # default is ten already
        stmts, said = _setting("postgres", where, name, "10", True, label)
        out.append(Need(side, what, left > 0, f"{max(left, 0)} free ({name})",
                        stmts, said, restart=True))
    if native and _streams_natively(hop) and key == "src":
        name = e._repl_name() if hasattr(e, "_repl_name") else "migkit"
        for d in dbs:
            has = _pg_rows(e, key, d,
                           f"select count(*) from pg_publication where"
                           f" pubname = {_lit(name)}")[0][0] != "0"
            ok = has or super_ or (aws and _member(e, key,
                                                   "rds_superuser"))
            out.append(Need(side, f"publish every table's changes in {d}",
                            ok, "the publication is there" if has else
                            ("may create it" if ok else
                             "only a superuser publishes every table"),
                            [f"-- in database {d}:",
                             f"create publication {name} for all"
                             " tables;"]))
    return out


def _pg_extensions(hop, e, dbs, where, who):
    """Extensions the source uses that the target lacks."""
    out = []
    for db in dbs:
        src = {r[0] for r in _pg_rows(e, "src", db, "select extname from"
                                                    " pg_extension")}
        dst = {r[0]: r[1] for r in _pg_rows(
            e, "dst", db, "select name||chr(31)||coalesce(installed_version,"
                          " '') from pg_available_extensions")}
        lack = sorted(x for x in src if not dst.get(x))
        if not lack:
            continue
        absent = [x for x in lack if x not in dst]
        said = []
        if getattr(where, "cloud", "") == "azure":
            said.append("server parameter azure.extensions must list "
                        + ", ".join(lack))
        if absent:
            said.append("not installed on the target server at all: "
                        + ", ".join(absent) + " - the server needs the"
                        " package before it can be created")
        out.append(Need("target", f"the extensions {db} uses", False,
                        "missing: " + ", ".join(lack),
                        [f"-- in database {hop.target_db(db)}:"]
                        + [f"create extension if not exists {_pgi(x)};"
                           for x in lack], said))
    return out


# ---- MySQL ---------------------------------------------------------------

GRANT = re.compile(r"^GRANT (.+?) ON (\S+) TO ", re.I)


def _my_privileges(e, key):
    """{object: {privilege}} the signed-in user holds, through its active
    roles as well."""
    rows = [r[0] for r in e._q(key, "show grants")]
    try:
        role = e._q(key, "select current_role()")[0][0]
    except Exception:  # noqa: BLE001 - before 8.0
        role = "NONE"
    if role and role != "NONE":
        try:
            rows += [r[0] for r in e._q(
                key, f"show grants for current_user() using {role}")]
        except Exception:  # noqa: BLE001 - said by what is missing
            pass
    got = {}
    for line in rows:
        m = GRANT.match(str(line))
        if not m:
            continue
        privs = {p.strip().upper() for p in re.sub(r"\([^)]*\)", "",
                                                    m.group(1)).split(",")}
        obj = m.group(2).replace("`", "")
        got.setdefault(obj, set()).update(privs)
    return got


def _has(privs, db, want):
    for obj in ("*.*", f"{db}.*"):
        held = privs.get(obj, set())
        if "ALL PRIVILEGES" in held or want in held:
            return True
    return False


def _myi(name):
    return "`" + str(name).replace("`", "``") + "`"


#: what a MySQL target needs on the database migkit writes
MY_WRITE = ("SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
            "DROP", "INDEX", "REFERENCES", "CREATE VIEW", "SHOW VIEW",
            "TRIGGER", "CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE")


def _my(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    user, host = e._q(key, "select substring_index(current_user(), '@', 1),"
                           " substring_index(current_user(), '@', -1)")[0]
    who = f"'{user}'@'{host}'"
    privs = _my_privileges(e, key)
    label = f"the {side}"
    if read:
        for db in dbs:
            name = hop.target_db(db) if key == "dst" else db
            ok = _has(privs, name, "SELECT")
            out.append(Need(side, f"read every table of {name}", ok,
                            "may" if ok else "may not",
                            [f"grant select, show view on {_myi(name)}.* to"
                             f" {who};"]))
    if key == "dst" and write:
        for db in dbs:
            name = hop.target_db(db)
            lack = [p for p in MY_WRITE if not _has(privs, name, p)]
            exists = bool(e._q(key, "select 1 from information_schema"
                                    ".schemata where schema_name = %s",
                               (name,)))
            fix = ([] if exists else
                   [f"create database if not exists {_myi(name)};"])
            fix.append(f"grant {', '.join(lack or MY_WRITE).lower()} on"
                       f" {_myi(name)}.* to {who};")
            ok = not lack and (exists or _has(privs, name, "CREATE"))
            out.append(Need(side, f"create and write what the source has"
                            f" in {name}", ok,
                            "may" if ok else ("it is not there; " if not
                                              exists else "")
                            + (f"lacks {', '.join(lack).lower()}" if lack
                               else ""), fix))
    start = len(out)
    if follow:
        var = dict(e._q(key, "show global variables where variable_name in"
                             " ('log_bin', 'binlog_format',"
                             " 'binlog_row_image',"
                             " 'binlog_expire_logs_seconds')"))
        cloud = getattr(where, "cloud", "") or ""
        if str(var.get("log_bin", "")).upper() != "ON":
            said = {"aws": ["binary logging is on while automated backups"
                            " are kept: set the backup retention above 0"],
                    "gcp": ["gcloud sql instances patch <instance>"
                            " --enable-bin-log"]}.get(cloud)
            stmts, other = _setting("mysql", where, "log_bin", "ON", True,
                                    label)
            out.append(Need(side, "a binary log", False,
                            "log_bin is OFF", stmts, said or other,
                            restart=True))
        else:
            out.append(Need(side, "a binary log", True, "log_bin is ON"))
        for name, want in (("binlog_format", "ROW"),
                           ("binlog_row_image", "FULL")):
            have = str(var.get(name, "")).upper()
            stmts, said = _setting("mysql", where, name, want,
                                   cloud == "aws" and "aurora" in
                                   getattr(where, "service", ""), label)
            out.append(Need(side, f"rows in the log ({name} {want})",
                            have == want, f"{name} is {have or '?'}",
                            stmts, said))
        keep = int(var.get("binlog_expire_logs_seconds") or 0)
        if cloud == "aws":
            # RDS keeps its own setting, empty by default - and then it
            # removes each log as soon as it may
            try:
                hours = next((r[1] for r in e._q(
                    key, "call mysql.rds_show_configuration")
                    if str(r[0]) == "binlog retention hours"), None)
                ok = hours is not None and int(hours) >= 24
                now = (f"kept {hours} hours" if hours is not None else
                       "not kept: removed as soon as it may be")
            except Exception as err:  # noqa: BLE001 - not allowed to ask
                ok, now = None, _last(err)
            out.append(Need(side, "a log kept a day or more", ok, now,
                            ["call mysql.rds_set_configuration('binlog"
                             " retention hours', 24);"]))
        else:
            stmts, said = _setting("mysql", where,
                                   "binlog_expire_logs_seconds", "86400",
                                   False, label)
            out.append(Need(side, "a log kept a day or more", keep == 0 or
                            keep >= 86400,
                            f"kept {keep}s" if keep else "kept for ever",
                            stmts, said))
        ok = _has(privs, "*", "REPLICATION SLAVE") and _has(
            privs, "*", "REPLICATION CLIENT")
        out.append(Need(side, "read the binary log (replication)", ok,
                        "may" if ok else "may not",
                        [f"grant replication slave, replication client on"
                         f" *.* to {who};"]))
    _following(out[start:])
    return out


# ---- MongoDB -------------------------------------------------------------

def _mongo(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    client = e._client(key)
    status = client.admin.command("connectionStatus", showPrivileges=True)
    auth = status.get("authInfo") or {}
    users = auth.get("authenticatedUsers") or []
    user = users[0]["user"] if users else ""
    held = {}
    for p in auth.get("authenticatedUserPrivileges") or []:
        res = p.get("resource") or {}
        held.setdefault((res.get("db"), res.get("collection")),
                        set()).update(p.get("actions") or [])

    def can(db, action):
        if not users:
            # nobody signed in and the server answered: it asks for no one
            return True
        for res in ((db, ""), ("", "")):
            if action in held.get(res, set()):
                return True
        return False
    grant = (f'db.getSiblingDB("admin").grantRolesToUser({_js(user)},'
             " [{roles}]);")
    if read:
        for db in dbs:
            name = hop.target_db(db) if key == "dst" else db
            ok = can(name, "find")
            out.append(Need(side, f"read every collection of {name}", ok,
                            "may" if ok else "may not",
                            [grant.format(roles=f'{{role: "read", db:'
                                                f' {_js(name)}}}')]))
    if key == "dst" and write:
        for db in dbs:
            name = hop.target_db(db)
            ok = all(can(name, a) for a in ("insert", "update", "remove",
                                             "createIndex"))
            out.append(Need(side, f"write {name}", ok,
                            "may" if ok else "may not",
                            [grant.format(roles=f'{{role: "readWrite", db:'
                                                f' {_js(name)}}}')]))
    start = len(out)
    if follow:
        hello = client.admin.command("hello")
        rs = bool(hello.get("setName")) or hello.get("msg") == "isdbgrid"
        out.append(Need(side, "a change stream (a replica set)", rs,
                        f"replica set {hello.get('setName')}" if rs else
                        "a single server",
                        [] if rs else ["rs.initiate();"],
                        [] if rs else ["start the server as a replica set"
                                       " member (replSetName in its"
                                       " configuration) and restart it"]))
        docdb = getattr(where, "service", "") == "documentdb"
        for db in dbs:
            ok = can(db, "changeStream")
            fix = [grant.format(roles=f'{{role: "read", db: {_js(db)}}}')]
            if docdb:
                fix.insert(0, 'db.adminCommand({modifyChangeStreams: 1,'
                              f' database: {_js(db)}, collection: "",'
                              " enable: true});")
            out.append(Need(side, f"follow {db}'s changes", ok,
                            "may" if ok else "may not", fix))
    _following(out[start:])
    return out


def _js(text):
    import json
    return json.dumps(str(text))


# ---- SQL Server ----------------------------------------------------------

def _msi(name):
    return "[" + str(name).replace("]", "]]") + "]"


def _mssql(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    for db in dbs:
        name = hop.target_db(db) if key == "dst" else db
        me = e._rows(key, db, "select user_name()")[0][0]
        who = _msi(me)
        if read:
            gaps = [(s, t) for s, t, ok in e._rows(
                key, db, "select s.name, t.name, has_perms_by_name("
                         "quotename(s.name) + '.' + quotename(t.name),"
                         " 'OBJECT', 'SELECT') from sys.tables t join"
                         " sys.schemas s on s.schema_id = t.schema_id")
                if not ok and not hop.excluded(db, s, t)]
            schemas = sorted({s for s, _ in gaps})
            out.append(Need(side, f"read every table of {name}", not gaps,
                            f"{len(gaps)} tables it may not read" if gaps
                            else "every one",
                            [f"use {_msi(name)};"] + [
                                f"grant select on schema::{_msi(s)} to"
                                f" {who};" for s in schemas]))
        if key == "dst" and write:
            ok = bool(e._rows(key, db, "select has_perms_by_name(db_name(),"
                                       " 'DATABASE', 'CREATE TABLE')")[0][0])
            out.append(Need(side, f"create and write tables in {name}", ok,
                            "may" if ok else "may not",
                            [f"use {_msi(name)};",
                             f"alter role db_ddladmin add member {who};",
                             f"alter role db_datawriter add member {who};",
                             f"alter role db_datareader add member {who};"]))
        start = len(out)
        if follow and key == "src":
            on = bool(e._rows(key, db, "select count(*) from"
                                       " sys.change_tracking_databases"
                                       " where database_id = db_id()")[0][0])
            out.append(Need(side, f"change tracking on {db}", on,
                            "on" if on else "off",
                            [f"alter database {_msi(db)} set change_tracking"
                             " = on (change_retention = 2 days,"
                             " auto_cleanup = on);"]))
            rows = e._rows(key, db, "select s.name, t.name, case when"
                                    " c.object_id is null then 0 else 1 end,"
                                    " has_perms_by_name(quotename(s.name) +"
                                    " '.' + quotename(t.name), 'OBJECT',"
                                    " 'VIEW CHANGE TRACKING') from sys.tables"
                                    " t join sys.schemas s on s.schema_id ="
                                    " t.schema_id left join"
                                    " sys.change_tracking_tables c on"
                                    " c.object_id = t.object_id")
            rows = [r for r in rows if not hop.excluded(db, r[0], r[1])]
            off = [(s, t) for s, t, tracked, _ in rows if not tracked]
            blind = sorted({s for s, _, _, can in rows if not can})
            out.append(Need(side, f"every table of {db} tracked", not off,
                            f"{len(off)} not tracked" if off else "all",
                            [f"use {_msi(db)};"] + [
                                f"alter table {_msi(s)}.{_msi(t)} enable"
                                " change_tracking;" for s, t in off]))
            out.append(Need(side, f"read {db}'s change tracking", not blind,
                            "may" if not blind else "may not",
                            [f"use {_msi(db)};"] + [
                                f"grant view change tracking on schema::"
                                f"{_msi(s)} to {who};" for s in blind]))
        _following(out[start:])
    return out


def _cl(command):
    """A CL command as the SQL statement that runs it on IBM i."""
    return "call qsys2.qcmdexc('" + command.replace("'", "''") + "');"


def _db2i(hop, e, key, side, dbs, where, read, write, follow, native):
    """What following an IBM i source needs: every table journaled, with
    before images, to a journal that keeps whole entries - each missing
    one as the CL command that sets it, run through `QSYS2.QCMDEXC`."""
    out = []
    for db in dbs:
        start = len(out)
        if follow and key == "src":
            tables = e._journaled(key, db)
            loose = sorted(t for t, i in tables.items() if not i["journal"])
            after = sorted(t for t, i in tables.items()
                           if i["journal"] and i["images"] != "*BOTH")
            out.append(Need(
                side, f"every table of {db} journaled", not loose,
                f"{len(loose)} not journaled" if loose else "all",
                [_cl(f"STRJRNPF FILE({tables[t]['system'][1]}/"
                     f"{tables[t]['system'][0]}) JRN({tables[t]['system'][1]}"
                     "/QSQJRN) IMAGES(*BOTH)") for t in loose]))
            out.append(Need(
                side, f"before images journaled for {db}", not after,
                f"{len(after)} with after images only" if after else "all",
                [_cl(f"CHGJRNOBJ OBJ(({tables[t]['system'][1]}/"
                     f"{tables[t]['system'][0]} *FILE)) ATR(*IMAGES)"
                     " IMAGES(*BOTH)") for t in after]))
            for journal in sorted({i["journal"] for i in tables.values()
                                   if i["journal"]}):
                attached = [r for r in e._receivers(key, db, journal)
                            if str(r[4]).strip() == "ATTACHED"]
                whole = not attached or str(attached[0][5] or "NO") \
                    .strip() == "NO"
                out.append(Need(side, f"the journal {journal} keeps whole"
                                      " entries", whole,
                                "whole" if whole else "minimized",
                                [_cl(f"CHGJRN JRN({journal})"
                                     " MINENTDTA(*NONE)")]))
        _following(out[start:])
    return out


# ---- Oracle ----------------------------------------------------------------

def _ori(name):
    return '"' + str(name).replace('"', '""') + '"'


def _ora_privs(e, key, db):
    """(the account's name, the system privileges and roles it holds)."""
    from .engines import oracle_logminer
    me = e._rows(key, db, "select user from dual")[0][0]
    return me, oracle_logminer.granted(e, key, db)


#: what migkit's LogMiner reader asks the database for: mining, the
#: transactions still open, the redo's own views, a table as of an SCN
ORA_FOLLOW = ("LOGMINING", "SELECT ANY TRANSACTION", "SELECT_CATALOG_ROLE",
              "FLASHBACK ANY TABLE")


#: what the reader reads of SYS on RDS, granted by `rdsadmin`
ORA_RDS_OBJECTS = ("DBMS_LOGMNR", "DBMS_FLASHBACK", "V_$LOGMNR_CONTENTS",
                   "V_$TRANSACTION", "V_$DATABASE", "V_$ARCHIVED_LOG",
                   "V_$LOG", "V_$LOGFILE")


#: the dictionary the redo-file reader reads the redo's tables by, as its
#: own grant list names it (select and flashback on each)
ORA_DICTIONARY = ("CCOL$", "CDEF$", "COL$", "DEFERRED_STG$", "ECOL$", "LOB$",
                  "LOBCOMPPART$", "LOBFRAG$", "OBJ$", "TAB$", "TABCOMPART$",
                  "TABPART$", "TABSUBPART$", "TS$", "USER$")
#: and the views it finds the redo through
ORA_REDO_VIEWS = ("V_$ARCHIVED_LOG", "V_$DATABASE", "V_$DATABASE_INCARNATION",
                  "V_$LOG", "V_$LOGFILE", "V_$PARAMETER", "V_$PDBS",
                  "V_$STANDBY_LOG", "V_$TRANSPORTABLE_PLATFORM")


def _ora_redo_files(e, key, db, who):
    """What the redo-file reader, run beside the redo, reads of the
    dictionary: asked of the server one table at a time."""
    short = []
    for t in ORA_DICTIONARY:
        try:
            e._rows(key, db, f"select count(*) from sys.{_ori(t)} where"
                             " 1 = 0")
        except Exception:  # noqa: BLE001 - not granted
            short.append(t)
    fix = [f"grant select, flashback on sys.{_ori(t)} to {who};"
           for t in short]
    if short:
        fix += [f"grant select on sys.{v} to {who};" for v in ORA_REDO_VIEWS]
        fix += [f"grant select, flashback on xdb.xdb$ttset to {who};",
                "-- and select, flashback on each xdb.x$nm, x$qn and x$pt"
                " table of xdb.xdb$ttset's suffixes"]
    return Need("source", "the redo-file reader reads the dictionary",
                not short, f"{len(short)} tables it may not read" if short
                else "may", fix)


def _oracle(hop, e, key, side, dbs, where, read, write, follow, native):
    from .engines import oracle_logminer
    out = []
    aws = getattr(where, "cloud", "") == "aws"
    for db in dbs:
        owner = e._owner(key, db)
        me, have = _ora_privs(e, key, db)
        who = _ori(me)
        mine = me == owner
        if read:
            if mine or "SELECT ANY TABLE" in have:
                gaps = []
            else:
                seen = {t for (t,) in e._rows(
                    key, db, "select table_name from all_tables where"
                             " owner = :1", (owner,))}
                try:
                    every = {t for (t,) in e._rows(
                        key, db, "select table_name from dba_tables where"
                                 " owner = :1", (owner,))}
                except Exception:  # noqa: BLE001 - asked another way
                    every = seen
                gaps = sorted(t for t in every - seen
                              if not hop.excluded(db, e._said(t)))
            out.append(Need(side, f"read every table of {owner}", not gaps,
                            f"{len(gaps)} tables it may not read" if gaps
                            else "every one",
                            [f"grant select on {_ori(owner)}.{_ori(t)} to"
                             f" {who};" for t in gaps]))
        if key == "dst" and write:
            ok = mine or {"CREATE ANY TABLE", "INSERT ANY TABLE",
                          "UPDATE ANY TABLE", "DELETE ANY TABLE",
                          "DROP ANY TABLE"} <= have
            out.append(Need(side, f"create and write tables in {owner}", ok,
                            "may" if ok else "may not",
                            [f"grant create any table, alter any table,"
                             " drop any table, insert any table, update any"
                             " table, delete any table, select any table,"
                             f" create any sequence, alter any sequence to"
                             f" {who};"]))
        start = len(out)
        if follow and key == "src":
            mode = oracle_logminer._mode(e, key, db)
            archived = mode["log_mode"] == "ARCHIVELOG"
            out.append(Need(side, "the redo archived", archived,
                            mode["log_mode"],
                            [] if aws else
                            ["shutdown immediate;", "startup mount;",
                             "alter database archivelog;",
                             "alter database open;"],
                            ["the automated backups' retention above 0"
                             " keeps the redo archived, and"
                             " `exec rdsadmin.rdsadmin_util.set_configuration"
                             "('archivelog retention hours', 24);` keeps it"
                             " for a day"] if aws else [],
                            restart=not aws))
            minimal = mode["min"] in ("YES", "IMPLICIT")
            out.append(Need(side, "minimal supplemental logging", minimal,
                            str(mode["min"]),
                            ["exec rdsadmin.rdsadmin_util."
                             "alter_supplemental_logging(p_action =>"
                             " 'ADD');"] if aws else
                            ["alter database add supplemental log data;"]))
            keys = oracle_logminer.logged_keys(e, key, db)
            bare = sorted(t for t, k in keys.items() if k is None)
            out.append(Need(side, f"every table of {owner} logs its key",
                            not bare, f"{len(bare)} do not" if bare
                            else "every one",
                            [f"alter table {_ori(owner)}.{e._q(t)} add"
                             " supplemental log data (primary key) columns;"
                             for t in bare]))
            short = [p for p in ORA_FOLLOW if p not in have]
            fix = ([f"grant {p.lower()} to {who};" for p in short
                    if p != "SELECT_CATALOG_ROLE"]
                   + ([f"grant select_catalog_role to {who};"]
                      if "SELECT_CATALOG_ROLE" in short else [])
                   + [f"grant execute on sys.dbms_logmnr to {who};",
                      f"grant execute on sys.dbms_flashback to {who};"])
            if aws:
                # on RDS the SYS objects are granted through its own
                # procedure, the master user holding no grant option
                fix = [f"grant {p.lower()} to {who};" for p in short] + [
                    f"exec rdsadmin.rdsadmin_util.grant_sys_object('{v}',"
                    f" '{me}');" for v in ORA_RDS_OBJECTS]
            out.append(Need(side, "read the redo back", not short,
                            "may" if not short else
                            f"lacks {', '.join(short)}", fix))
            from .engines import oracle_redo
            if oracle_redo.chosen(e, key, db) == oracle_redo.RUNG:
                out.append(_ora_redo_files(e, key, db, who))
            if mode["force"] != "YES":
                loose = [t for (t,) in e._rows(
                    key, db, "select table_name from all_tables where owner"
                             " = :1 and logging = 'NO' and temporary = 'N'",
                    (owner,)) if not hop.excluded(db, e._said(t))]
                out.append(Need(side, f"no table of {owner} skips the redo",
                                not loose, f"{len(loose)} NOLOGGING" if loose
                                else "none", [f"alter table {_ori(owner)}."
                                              f"{_ori(t)} logging;"
                                              for t in loose]))
        _following(out[start:])
    return out


#: the engines whose servers can be asked, by canonical name
ASK = {"postgres": _pg, "mysql": _my, "mongodb": _mongo, "mssql": _mssql,
       "db2i": _db2i, "oracle": _oracle}

#: how each writes a comment, and its script's file suffix
SCRIPT = {"postgres": ("--", "sql"), "mysql": ("--", "sql"),
          "mongodb": ("//", "js"), "mssql": ("--", "sql"),
          "db2i": ("--", "sql"), "oracle": ("--", "sql")}


def script(hop, eng, side, found, where=None):
    """The text of one side's script: what is missing, each with the
    statements that grant it and what the cloud changes outside the
    server. None where nothing is."""
    lack = [n for n in missing(found) if n.side == side]
    if not lack:
        return None
    e, _ = _side_engine(eng, side)
    mark, _ = SCRIPT[_canon(e)]
    ep = hop.source if side == "source" else hop.target
    head = [f"{mark} what migkit needs of the {side} of hop {hop.name}"
            f" ({ep.user or '?'}@{ep.host or ep.options.get('hosts', '')})"
            f" that it does not have yet",
            f"{mark} run as a user who may grant it; migkit runs none of"
            " this. `migkit doctor` asks again.",
            f"{mark} {getattr(where, 'label', '') or ''}".rstrip()]
    body = []
    for n in lack:
        body.append("")
        body.append(f"{mark} {n.what}: {n.now}"
                    + (" - takes a restart" if n.restart else ""))
        body += [f"{mark}   {line}" for line in n.outside]
        body += n.fix
    return "\n".join([h for h in head if h.strip() != mark] + body) + "\n"


def write(hop, eng, found, wheres=None):
    """{side: path} of the scripts written for what is missing; a side
    missing nothing has its old script taken away."""
    wheres = wheres or {}
    out = {}
    for side in ("source", "target"):
        e, _ = _side_engine(eng, side)
        if _canon(e) not in SCRIPT:
            continue
        path = hop.report_dir() / f"for-the-dba-{side}.{SCRIPT[_canon(e)][1]}"
        text = script(hop, eng, side, found, wheres.get(side))
        if text is None:
            path.unlink(missing_ok=True)
            continue
        path.write_text(text)
        out[side] = path
    return out
