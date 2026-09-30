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
runs them: changing what a server allows is the DBA's to do - but for a
change a faster way needs that the operator answered yes to, which
migkit makes and takes off again itself (`sourcechange`).

The requirements are read off the server as they stand - `has_table_
privilege`, the grants the server lists, `wal_level`, `binlog_format`,
change tracking - so a grant through a role counts, and a table the hop
excludes is not asked for.

Every change to the source's settings, logs and replication rights is
written from `sourcechange.DEFINITIONS`, the one list the offers of a
faster way are written from too, with what not having it means beside
it; a change a faster way needs that was handed to the DBA - the answer
dba, or one only a DBA can make - joins the script (`for_the_dba`). The
read and write grants of the tables themselves stay here: every way needs
them, and none is faster for them.
"""
import re
from dataclasses import dataclass, field

from . import sourcechange as sc

#: goals that follow the source's changes, and that write to the source
STREAMS = ("cutover", "keep-in-sync", "two-way", "rehearse", "upgrade")


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
    #: what not having it means (`sourcechange.consequence`)
    without: str = ""


def _following(found):
    for n in found:
        n.follow = True
    return found


def _change(side, what, ok, now, name, ctx, fix_before=(), restart=None):
    """A `Need` for one of `sourcechange.DEFINITIONS`: its statements, what
    the side's cloud changes outside the server, whether it takes a
    restart and what not having it means, all from the one list."""
    return Need(side, what, ok, now,
                list(fix_before) + sc.statements(name, ctx),
                sc.outside(name, ctx),
                sc.restarts(name, ctx) if restart is None else restart,
                without=sc.consequence(name))


def _side_engine(eng, side):
    """(engine object, side key) that asks one side's server."""
    key = "src" if side == "source" else "dst"
    if getattr(eng, "src_engine", None) is not None:
        return (eng.src_engine if key == "src" else eng.dst_engine), key
    return eng, key


def _canon(e):
    """The engine a side's needs are asked by: its own, where it speaks
    another's API (`Engine.PAIR_ENGINE`)."""
    return getattr(e, "PAIR_ENGINE", "") or getattr(e, "CANON_ENGINE",
                                                    "") or ""


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
    # what a faster way needs of the source that was handed to the DBA
    return out + sc.for_the_dba(hop)


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
    ctx = sc.Ctx(who=who, where=where, label=label)
    level = e._psql(key, "postgres", "show wal_level").strip()
    out.append(_change(side, "a log that carries rows (wal_level logical)",
                       level == "logical", f"wal_level is {level}",
                       "wal_level", ctx))
    aws = getattr(where, "cloud", "") == "aws"
    ok = super_ or repl or (aws and _member(e, key, "rds_replication"))
    out.append(_change(side, "read the change log (replication)", ok,
                       "may" if ok else "may not", "replication_role", ctx))
    free = _pg_rows(
        e, key, "postgres",
        "select current_setting('max_replication_slots')::int - (select"
        " count(*) from pg_replication_slots)||chr(31)||"
        "current_setting('max_wal_senders')::int - (select count(*) from"
        " pg_stat_replication)")[0]
    # ten: room for this hop's and a few more, where the server's own
    # default is ten already (`sourcechange.DEFINITIONS`)
    for (change, name, left, what) in (
            ("replication_slots", "max_replication_slots", int(free[0]),
             "a free replication slot"),
            ("wal_senders", "max_wal_senders", int(free[1]),
             "a free WAL sender")):
        out.append(_change(side, what, left > 0,
                           f"{max(left, 0)} free ({name})", change, ctx))
    if native and _streams_natively(hop) and key == "src":
        name = e._repl_name() if hasattr(e, "_repl_name") else "migkit"
        for d in dbs:
            has = _pg_rows(e, key, d,
                           f"select count(*) from pg_publication where"
                           f" pubname = {_lit(name)}")[0][0] != "0"
            ok = has or super_ or (aws and _member(e, key,
                                                   "rds_superuser"))
            out.append(_change(side, f"publish every table's changes in"
                                     f" {d}",
                               ok, "the publication is there" if has else
                               ("may create it" if ok else
                                "only a superuser publishes every table"),
                               "publication",
                               sc.Ctx(who=who, where=where, label=label,
                                      db=d, name=name)))
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
        ctx = sc.Ctx(who=who, where=where, label=label)
        on = str(var.get("log_bin", "")).upper() == "ON"
        out.append(_change(side, "a binary log", on,
                           "log_bin is ON" if on else "log_bin is OFF",
                           "log_bin", ctx))
        for change, name, want in (("binlog_format", "binlog_format", "ROW"),
                                   ("binlog_row_image", "binlog_row_image",
                                    "FULL")):
            have = str(var.get(name, "")).upper()
            out.append(_change(side, f"rows in the log ({name} {want})",
                               have == want, f"{name} is {have or '?'}",
                               change, ctx))
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
        else:
            ok = keep == 0 or keep >= 86400
            now = f"kept {keep}s" if keep else "kept for ever"
        out.append(_change(side, "a log kept a day or more", ok, now,
                           "binlog_retention", ctx))
        ok = _has(privs, "*", "REPLICATION SLAVE") and _has(
            privs, "*", "REPLICATION CLIENT")
        out.append(_change(side, "read the binary log (replication)", ok,
                           "may" if ok else "may not", "replication_grants",
                           ctx))
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
        ctx = sc.Ctx(where=where, label=f"the {side}")
        out.append(_change(side, "a change stream (a replica set)", rs,
                           f"replica set {hello.get('setName')}" if rs else
                           "a single server", "replica_set", ctx))
        docdb = getattr(where, "service", "") == "documentdb"
        for db in dbs:
            ok = can(db, "changeStream")
            got = _change(side, f"follow {db}'s changes", ok,
                          "may" if ok else "may not", "change_streams",
                          sc.Ctx(where=where, label=f"the {side}", db=db,
                                 docdb=docdb))
            got.fix.append(grant.format(roles=f'{{role: "read", db:'
                                              f' {_js(db)}}}'))
            out.append(got)
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
            out.append(_change(side, f"change tracking on {db}", on,
                               "on" if on else "off", "change_tracking",
                               sc.Ctx(who=who, where=where, db=db)))
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
            out.append(_change(side, f"every table of {db} tracked", not off,
                               f"{len(off)} not tracked" if off else "all",
                               "table_tracking",
                               sc.Ctx(who=who, where=where, db=db,
                                      tables=off)))
            out.append(_change(side, f"read {db}'s change tracking",
                               not blind, "may" if not blind else "may not",
                               "tracking_grant",
                               sc.Ctx(who=who, where=where, db=db,
                                      tables=blind)))
        _following(out[start:])
    return out


def _redshift(hop, e, key, side, dbs, where, read, write, follow, native):
    """A cluster's privileges, asked in the PostgreSQL functions Redshift
    keeps: reading every table of the hop's schemas, and on the target
    making tables and writing them - with, where the target stages its
    batches in S3, the role the cluster reads the bucket as. A warehouse
    keeps no change log, so a follow asks nothing of it."""
    from .engines.dbapi import MARK
    out = []
    me = e._rows(key, dbs[0] if dbs else "", "select current_user")[0][0]
    who = _pgi(me)
    for db in dbs:
        schema = e._d(key, db)
        if read:
            gaps = [t for (t,) in e._rows(
                key, db, "select tablename from pg_tables where schemaname ="
                         f" {MARK} and not has_table_privilege(current_user,"
                         " quote_ident(schemaname) || '.' ||"
                         " quote_ident(tablename), 'SELECT')", (schema,))
                    if not hop.excluded(db, t)]
            out.append(Need(side, f"read every table of {schema}", not gaps,
                            f"{len(gaps)} tables it may not read" if gaps
                            else "every one",
                            [f"grant usage on schema {_pgi(schema)} to"
                             f" {who};",
                             f"grant select on all tables in schema"
                             f" {_pgi(schema)} to {who};"]))
        if key == "dst" and write:
            ok = bool(e._rows(
                key, db, f"select has_schema_privilege(current_user, {MARK},"
                         " 'CREATE') and has_schema_privilege(current_user,"
                         f" {MARK}, 'USAGE')", (schema, schema))[0][0])
            out.append(Need(side, f"create and write tables in {schema}", ok,
                            "may" if ok else "may not",
                            [f"grant usage, create on schema {_pgi(schema)}"
                             f" to {who};",
                             "grant select, insert, update, delete on all"
                             f" tables in schema {_pgi(schema)} to {who};"]))
    stage = e._stage(key) if key == "dst" and write else None
    if stage:
        bucket, prefix, role = stage
        out.append(Need(side, "a COPY reads the stage", None,
                        "asked of the cluster only by a COPY",
                        [], outside=[
                            f"associate {role} with the cluster, and let it"
                            f" read s3://{bucket}/{prefix}"]))
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
            out.append(_change(
                side, f"every table of {db} journaled", not loose,
                f"{len(loose)} not journaled" if loose else "all",
                "table_journaling", sc.Ctx(tables=[
                    tuple(tables[t]["system"][:2]) for t in loose])))
            out.append(_change(
                side, f"before images journaled for {db}", not after,
                f"{len(after)} with after images only" if after else "all",
                "before_images", sc.Ctx(tables=[
                    tuple(tables[t]["system"][:2]) for t in after])))
            for journal in sorted({i["journal"] for i in tables.values()
                                   if i["journal"]}):
                attached = [r for r in e._receivers(key, db, journal)
                            if str(r[4]).strip() == "ATTACHED"]
                whole = not attached or str(attached[0][5] or "NO") \
                    .strip() == "NO"
                out.append(_change(side, f"the journal {journal} keeps whole"
                                         " entries", whole,
                                   "whole" if whole else "minimized",
                                   "whole_entries", sc.Ctx(name=journal)))
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
    return _change("source", "the redo-file reader reads the dictionary",
                   not short, f"{len(short)} tables it may not read" if short
                   else "may", "redo_file_grants",
                   sc.Ctx(who=who, tables=short))


def _oracle(hop, e, key, side, dbs, where, read, write, follow, native):
    from .engines import oracle_logminer
    out = []
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
            ctx = sc.Ctx(who=who, me=me, where=where, db=db, owner=owner)
            archived = mode["log_mode"] == "ARCHIVELOG"
            out.append(_change(side, "the redo archived", archived,
                               mode["log_mode"], "archivelog", ctx))
            minimal = mode["min"] in ("YES", "IMPLICIT")
            out.append(_change(side, "minimal supplemental logging", minimal,
                               str(mode["min"]), "supplemental_logging",
                               ctx))
            keys = oracle_logminer.logged_keys(e, key, db)
            bare = sorted(t for t, k in keys.items() if k is None)
            out.append(_change(side, f"every table of {owner} logs its key",
                               not bare, f"{len(bare)} do not" if bare
                               else "every one", "table_key_logging",
                               sc.Ctx(who=who, where=where, owner=owner,
                                      tables=[e._q(t) for t in bare])))
            short = [p for p in ORA_FOLLOW if p not in have]
            out.append(_change(side, "read the redo back", not short,
                               "may" if not short else
                               f"lacks {', '.join(short)}",
                               "logmining_grants",
                               sc.Ctx(who=who, me=me, where=where,
                                      short=short)))
            from .engines import oracle_redo
            if oracle_redo.chosen(e, key, db) == oracle_redo.RUNG:
                out.append(_ora_redo_files(e, key, db, who))
            if mode["force"] != "YES":
                loose = [t for (t,) in e._rows(
                    key, db, "select table_name from all_tables where owner"
                             " = :1 and logging = 'NO' and temporary = 'N'",
                    (owner,)) if not hop.excluded(db, e._said(t))]
                out.append(_change(side, f"no table of {owner} skips the"
                                         " redo",
                                   not loose, f"{len(loose)} NOLOGGING"
                                   if loose else "none", "table_logging",
                                   sc.Ctx(who=who, where=where, owner=owner,
                                          tables=loose)))
        _following(out[start:])
    return out


# ---- the sources that publish their changes elsewhere (ER4, ER5) --------
#
# What each needs to be followed is not a grant on the server alone: a
# changefeed, a connector or a stream the operator makes, which migkit then
# reads. Asked by reading where the hop says the changes are; what makes
# them is said as the command that does, outside the server.

def _feed(e, key, side, dbs, what, make):
    """Whether the hop's change feed can be read: [Need]."""
    out = []
    for db in dbs:
        try:
            lanes = e._follower(db).lanes.lanes()
            out.append(Need(side, what, bool(lanes),
                            f"{len(lanes)} partitions or shards to read"
                            if lanes else "nothing to read", [], make))
        except (Exception, SystemExit) as err:  # noqa: BLE001 - the answer
            out.append(Need(side, what, False, _last(err), [], make))
    return out


def _tidb(hop, e, key, side, dbs, where, read, write, follow, native):
    out = _my(hop, e, key, side, dbs, where, read, write, False, False)
    if not follow:
        return out
    spec = e._changes_spec(key) or {}
    topic = spec.get("topic") or "<topic>"
    servers = spec.get("servers") or "<kafka>:9092"
    make = [f"cdc cli changefeed create --server=http://<ticdc>:8300"
            f" --changefeed-id=migkit-{hop.name} --sink-uri=\"kafka://"
            f"{servers}/{topic}?protocol=canal-json&enable-tidb-extension"
            "=true\""]
    found = _feed(e, key, side, dbs, "a TiCDC changefeed to the topic the"
                  " hop reads", make)
    got = dict(e._q(key, "show global variables like 'tidb_gc_life_time'"))
    keep = str(got.get("tidb_gc_life_time") or "")
    hours = _duration_hours(keep)
    found.append(Need(side, "rows kept an hour back, to read each change as"
                      " it stood at its watermark", hours is not None
                      and hours >= 1, f"tidb_gc_life_time is {keep or '?'}",
                      ["set global tidb_gc_life_time = '1h';"]))
    return out + _following(found)


def _duration_hours(text):
    """Hours of a Go duration (`10m0s`, `1h`, `24h0m0s`), or None."""
    parts = re.findall(r"(\d+(?:\.\d+)?)(h|m|s)", str(text))
    if not parts:
        return None
    scale = {"h": 1, "m": 1 / 60, "s": 1 / 3600}
    return sum(float(n) * scale[u] for n, u in parts)


def _pgwire_rows(e, key, db, sql, args=None):
    return e._rows(key, db, sql, args)


def _pgwire(hop, e, key, side, dbs, read, write):
    """Reading every table and writing the target, on an engine over
    PostgreSQL's wire, by the privilege functions both keep."""
    from .engines.dbapi import MARK
    out = []
    me = _pgwire_rows(e, key, dbs[0] if dbs else "postgres",
                      "select current_user")[0][0]
    who = _pgi(me)
    if read:
        for db in dbs:
            shown = hop.target_db(db) if key == "dst" else db
            gaps = []
            for t in e.neutral_tables(key, db):
                sch, tbl = e._split(t)
                ok = _pgwire_rows(e, key, db, "select has_table_privilege("
                                              f"{MARK}, 'SELECT')",
                                  (f"{_pgi(sch)}.{_pgi(tbl)}",))[0][0]
                if not ok:
                    gaps.append(f"{_pgi(sch)}.{_pgi(tbl)}")
            out.append(Need(side, f"read every table of {shown}", not gaps,
                            f"{len(gaps)} tables it may not read" if gaps
                            else "every one",
                            [f"grant select on {g} to {who};"
                             for g in gaps]))
    if key == "dst" and write:
        for db in dbs:
            name = hop.target_db(db)
            ok = _pgwire_rows(e, key, db, "select has_database_privilege("
                                          f"{MARK}, 'CREATE')", (name,))[0][0]
            out.append(Need(side, f"create what the source has in {name}",
                            bool(ok), "may" if ok else "may not",
                            [f"grant create on database {_pgi(name)} to"
                             f" {who};"]))
    return out


def _crdb(hop, e, key, side, dbs, where, read, write, follow, native):
    out = _pgwire(hop, e, key, side, dbs, read, write)
    if not follow:
        return out
    found = []
    db = dbs[0] if dbs else "defaultdb"
    on = e.rangefeeds(key, db)
    found.append(Need(side, "rangefeeds, which a changefeed reads through",
                      on, "kv.rangefeed.enabled is"
                      f" {'on' if on else 'off' if on is False else '?'}",
                      ["set cluster setting kv.rangefeed.enabled = true;"]))
    me = _pgwire_rows(e, key, db, "select current_user")[0][0]
    admin = _pgwire_rows(e, key, db, "select pg_has_role(current_user,"
                                     " 'admin', 'member')")[0][0]
    lack = []
    if not admin:
        for d in dbs:
            for t in e.neutral_tables(key, d):
                got = _pgwire_rows(e, key, d, "select privilege_type from"
                                              f" [show grants on table {t}"
                                              f" for {_pgi(me)}]")
                if not {str(p).upper() for (p,) in got} & {"CHANGEFEED",
                                                            "ALL"}:
                    lack.append(f"{d}.{t}")
    found.append(Need(side, "make a changefeed on every table in scope",
                      not lack, f"not on {len(lack)} tables" if lack
                      else "may",
                      [f"grant changefeed on table {t} to {_pgi(me)};"
                       for t in lack]))
    if e._changes_spec(key):
        found += _feed(e, key, side, dbs, "the changefeed's Kafka topic",
                       ["create changefeed for <tables> into"
                        " 'kafka://<servers>' with updated, resolved;"])
    return out + _following(found)


def _dsql(hop, e, key, side, dbs, where, read, write, follow, native):
    out = _pgwire(hop, e, key, side, dbs, read, write)
    if not follow:
        return out
    spec = e._changes_spec(key) or {}
    ep = hop.source if key == "src" else hop.target
    make = ["aws dsql create-stream --cluster-identifier"
            f" {str(ep.host).split('.')[0]} --target-definition"
            " kinesis=<stream ARN>,role=<role ARN>",
            "the reader's role: kinesis:GetRecords, GetShardIterator,"
            " ListShards, DescribeStreamSummary on the stream, and"
            " cloudwatch:GetMetricStatistics for the stream's lag"]
    found = _feed(e, key, side, dbs, "DSQL's CDC stream to the Kinesis"
                  " stream the hop reads", make)
    if spec.get("cdc_stream"):
        try:
            import boto3
            got = boto3.client("dsql", region_name=e._region(ep)).get_stream(
                clusterIdentifier=str(ep.host).split(".")[0],
                streamIdentifier=str(spec["cdc_stream"]))
            status = str(got.get("status") or "")
            found.append(Need(side, "the CDC stream active", status ==
                              "ACTIVE", f"it is {status or '?'}", [],
                              ["an IMPAIRED stream says why in GetStream's"
                               " statusReason; a FAILED one is made again"]))
        except Exception as err:  # noqa: BLE001 - unknown, not clean
            found.append(Need(side, "the CDC stream active", None,
                              _last(err)))
    return out + _following(found)


def _couchbase(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    try:
        me = e._rest(key, "/whoami")
    except Exception as err:  # noqa: BLE001 - said as unknown
        return [Need(side, "ask the cluster who migkit signs in as", None,
                     _last(err))]
    roles = {(r.get("role"), r.get("bucket_name") or "*")
             for r in me.get("roles", [])}
    user = me.get("id") or "<user>"

    def has(bucket, *names):
        return any((n, b) in roles for n in names + ("admin",)
                   for b in (bucket, "*"))
    for db in dbs:
        name = hop.target_db(db) if key == "dst" else db
        if read:
            ok = has(name, "data_reader", "bucket_full_access",
                     "bucket_admin", "ro_admin")
            out.append(Need(side, f"read every document of {name}", ok,
                            "may" if ok else "may not", [],
                            [f"couchbase-cli user-manage --set --rbac-username"
                             f" {user} --roles data_reader[{name}]"
                             " --auth-domain local"]))
        if key == "dst" and write:
            ok = has(name, "bucket_full_access", "bucket_admin") or (
                has(name, "data_writer") and has(name, "scope_admin",
                                                 "bucket_admin"))
            out.append(Need(side, f"write documents and make collections"
                            f" in {name}", ok, "may" if ok else "may not",
                            [], [f"couchbase-cli user-manage --set"
                                 f" --rbac-username {user} --roles"
                                 f" data_writer[{name}],scope_admin[{name}]"
                                 " --auth-domain local"]))
        if key == "src" and follow:
            ok = has(name, "data_monitoring", "bucket_admin", "ro_admin",
                     "cluster_admin")
            out += _following([Need(
                side, f"read {name}'s sequence numbers, the follow's"
                      " fence", ok, "may" if ok else "may not", [],
                [f"couchbase-cli user-manage --set --rbac-username {user}"
                 f" --roles data_monitoring[{name}] --auth-domain local"])])
    if follow and key == "src":
        out += _following(_feed(e, key, side, dbs, "the Kafka connector's"
                                " topic the hop reads",
                                ["the connector with its default source"
                                 " handler, publishing to the hop's topic"]))
    return out


def _cosmos(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    for db in dbs:
        try:
            tables = e.neutral_tables(key, db)
            out.append(Need(side, f"read {len(tables)} containers of"
                            f" {e._d(key, db)}", True, "may"))
        except Exception as err:  # noqa: BLE001 - the answer
            out.append(Need(side, f"read the containers of {e._d(key, db)}",
                            False, _last(err), [],
                            ["az cosmosdb sql role assignment create"
                             " --role-definition-name 'Cosmos DB Built-in"
                             " Data Reader' --principal-id <migkit's"
                             " principal> --scope /"]))
            continue
        if key == "src" and follow and tables:
            try:
                e._mode(key, db, e._container(key, db, tables[0]))
                out += _following([Need(side, "every delete in the change"
                                        " feed (all versions and deletes)",
                                        True, "the account serves it")])
            except SystemExit as err:
                out += _following([Need(
                    side, "every delete in the change feed (all versions"
                          " and deletes)", False, _last(err), [],
                    ["az cosmosdb update --backup-policy-type Continuous",
                     "then turn on the all-versions-and-deletes change"
                     " feed on the account's Features page"])])
    return out


# ---- the cloud-native services (P0-CLOUDS part 2) --------------------------

def _granted(side, what, have, want, fix):
    """A Need per permission `want` holds of the ones a cloud's IAM says
    the caller has (`have`, None where it could not be asked)."""
    lack = None if have is None else [p for p in want if p not in have]
    return Need(side, what, None if lack is None else not lack,
                "could not be asked" if lack is None else
                (f"lacks {', '.join(lack)}" if lack else "may"), [],
                fix if lack or lack is None else [])


def _spanner(hop, e, key, side, dbs, where, read, write, follow, native):
    """The database's IAM, asked of the service (`TestIamPermissions`),
    and a change stream the follow reads."""
    out = []
    ep = hop.source if key == "src" else hop.target
    for db in dbs:
        name = e._d(key, db)
        want = ((["spanner.databases.select", "spanner.databases.read"]
                 if read else [])
                + (["spanner.databases.write", "spanner.databases.updateDdl"]
                   if key == "dst" and write else [])
                + (["spanner.databases.select"] if follow else []))
        try:
            api = e._client(key).database_admin_api
            got = set(api.test_iam_permissions(request={
                "resource": e._database(key, db).name,
                "permissions": sorted(set(want))}).permissions)
        except Exception:  # noqa: BLE001 - said as unknown
            got = None
        out.append(_granted(
            side, f"read{' and write' if key == 'dst' and write else ''}"
                  f" {name}", got, sorted(set(want)),
            [f"gcloud spanner databases add-iam-policy-binding {name}"
             f" --instance={ep.options.get('instance')} --member=<migkit's"
             " principal> --role=roles/spanner.databaseUser"]))
        if follow and key == "src":
            try:
                stream = e._stream(db)
                out += _following([Need(side, "a change stream over every"
                                        " table in scope", True, stream)])
            except SystemExit as err:
                out += _following([_change(
                    side, "a change stream over every table in scope", False,
                    _last(err), "spanner_change_stream",
                    sc.Ctx(name=f"migkit_{hop.name}".replace("-", "_"),
                           db=name))])
    return out


def _bigtable(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    for db in dbs:
        want = ((["bigtable.tables.readRows", "bigtable.tables.sampleRowKeys"]
                 if read else [])
                + (["bigtable.tables.mutateRows", "bigtable.tables.create"]
                   if key == "dst" and write else [])
                + (["bigtable.tables.readChangeStream"] if follow else []))
        try:
            got = set(e._instance(key, db).test_iam_permissions(
                sorted(set(want))))
        except Exception:  # noqa: BLE001 - said as unknown
            got = None
        out.append(_granted(side, f"what the move needs of {e._d(key, db)}",
                            got, sorted(set(want)),
                            [f"gcloud bigtable instances add-iam-policy-binding"
                             f" {e._d(key, db)} --member=<migkit's principal>"
                             " --role=roles/bigtable.user"]))
        if follow and key == "src":
            off = [t for t in e.neutral_tables(key, db)
                   if e._retention_s(db, t) is None]
            out += _following([_change(
                side, "a change stream on every table in scope", not off,
                f"off on {len(off)} tables" if off else "on",
                "bigtable_change_stream", sc.Ctx(db=db, tables=off))])
    return out


def _documents(hop, e, key, side, dbs, where, read, write, follow, native):
    """Firestore and Datastore: what reading them answers, and what
    writing needs - which only writing would prove, so it is said as
    unknown."""
    out = []
    for db in dbs:
        try:
            got = e.neutral_tables(key, db)
            out.append(Need(side, f"read {e._d(key, db)}", True,
                            f"{len(got)} collections"))
        except Exception as err:  # noqa: BLE001 - the answer
            out.append(Need(side, f"read {e._d(key, db)}", False, _last(err),
                            [], ["grant migkit's principal"
                                 " roles/datastore.viewer on the project"]))
        if key == "dst" and write:
            out.append(Need(side, f"write {e._d(key, db)}", None,
                            "only a write would say", [],
                            ["grant migkit's principal roles/datastore.user"
                             " on the project"]))
    return out


def _gauss(hop, e, key, side, dbs, where, read, write, follow, native):
    out = _pgwire(hop, e, key, side, dbs, read, write)
    if not follow:
        return out
    db = dbs[0] if dbs else "postgres"
    me = _pgwire_rows(e, key, db, "select current_user")[0][0]
    ctx = sc.Ctx(who=_pgi(me), me=me, where=where)
    level = str(_pgwire_rows(e, key, db, "show wal_level")[0][0])
    repl = bool(_pgwire_rows(e, key, db, "select rolreplication from"
                                         " pg_roles where rolname ="
                                         " current_user")[0][0])
    return out + _following([
        _change(side, "a log that carries rows", level == "logical",
                f"wal_level is {level}", "gauss_wal_level", ctx),
        _change(side, "the right to read the change log", repl,
                "may" if repl else "may not", "gauss_replication", ctx)])


def _on_mysql_wire(hop, e, key, side, dbs, where, read, write, follow,
                   native):
    """OceanBase, PolarDB-X and TDSQL: MySQL's grants, as the MySQL engine
    asks them, and a binlog to follow where the follow reads one."""
    out = _my(hop, e, key, side, dbs, where, read, write, False, False)
    if not follow or key != "src":
        return out
    db = dbs[0] if dbs else ""
    try:
        e.change_point(key, db)
        out += _following([Need(side, "a binlog the follow reads", True,
                                "there")])
    except SystemExit as err:
        out += _following([Need(side, "a binlog the follow reads", False,
                                _last(err))])
    return out


def _keyspaces(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    for db in dbs:
        try:
            tables = e.neutral_tables(key, db)
            out.append(Need(side, f"read {e._ks(key, db)}", True,
                            f"{len(tables)} tables"))
        except Exception as err:  # noqa: BLE001 - the answer
            out.append(Need(side, f"read {e._ks(key, db)}", False,
                            _last(err), [],
                            ["attach AmazonKeyspacesReadOnlyAccess to"
                             " migkit's role"]))
            continue
        if follow and key == "src":
            off = [t for t in tables if e._stream_arn(db, t) is None]
            out += _following([_change(
                side, "a CDC stream on every table in scope", not off,
                f"off on {len(off)} tables" if off else "on", "keyspaces_cdc",
                sc.Ctx(db=db, tables=off))])
    return out


def _neptune(hop, e, key, side, dbs, where, read, write, follow, native):
    out = []
    if not follow or key != "src":
        return out
    try:
        e._follower(dbs[0] if dbs else "graph").lanes.latest()
        out += _following([Need(side, "Neptune Streams on", True, "on")])
    except (Exception, SystemExit) as err:  # noqa: BLE001 - the answer
        out += _following([_change(side, "Neptune Streams on", False,
                                   _last(err), "neptune_streams", sc.Ctx())])
    return out


#: the engines whose servers can be asked, by canonical name
ASK = {"postgres": _pg, "mysql": _my, "mongodb": _mongo, "mssql": _mssql,
       "redshift": _redshift, "db2i": _db2i, "oracle": _oracle,
       "tidb": _tidb, "cockroachdb": _crdb, "dsql": _dsql,
       "couchbase": _couchbase, "cosmosdb": _cosmos,
       "spanner": _spanner, "bigtable": _bigtable,
       "firestore": _documents, "datastore": _documents, "gaussdb": _gauss,
       "oceanbase": _on_mysql_wire, "polardbx": _on_mysql_wire,
       "dcdb": _on_mysql_wire, "keyspaces": _keyspaces,
       "neptune": _neptune, "geminidb-mongo": _mongo}

#: how each writes a comment, and its script's file suffix
SCRIPT = {"postgres": ("--", "sql"), "mysql": ("--", "sql"),
          "mongodb": ("//", "js"), "mssql": ("--", "sql"),
          "redshift": ("--", "sql"), "db2i": ("--", "sql"),
          "oracle": ("--", "sql"), "tidb": ("--", "sql"),
          "cockroachdb": ("--", "sql"), "dsql": ("--", "sql"),
          "couchbase": ("#", "sh"), "cosmosdb": ("#", "sh"),
          "spanner": ("--", "sql"), "bigtable": ("#", "sh"),
          "firestore": ("#", "sh"), "datastore": ("#", "sh"),
          "gaussdb": ("--", "sql"), "oceanbase": ("--", "sql"),
          "polardbx": ("--", "sql"), "dcdb": ("--", "sql"),
          "keyspaces": ("--", "cql"), "neptune": ("#", "sh"),
          "geminidb-mongo": ("//", "js")}


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
        if n.without:
            body.append(f"{mark}   {n.without}")
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
