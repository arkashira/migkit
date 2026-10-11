"""Scheduled jobs inside the databases, on exactly one side at a time.

A job the database runs by itself - a pg_cron job, a MySQL event - writes
rows nobody's application wrote. On both sides at once it writes them
twice: on the target before the flip (a copied `cron.job` is a table, and
pg_cron runs on a logical subscriber, which is an ordinary primary), and
on the old source after it, into the stream back. So at cutover each job
the source ran is switched off there before the source is frozen (a
frozen MySQL server refuses the statement that would do it afterwards)
and switched on on the target at the flip; each switch is recorded first
and undone from the record.

MySQL marks an event it received by replication `REPLICA_SIDE_DISABLED`
(`SLAVESIDE_DISABLED` before 8.4); a copied schema carries whatever the
source had. The event keeps its definer when it is switched: `ALTER
EVENT` run by another account makes that account the definer.

A job is the hop's only where it writes what the hop takes: one whose
every table is one the hop leaves out belongs to whatever writes those
tables, and is left as it is on both sides (`left_alone`).
"""
import re


def _side(hop, eng, side):
    from .freeze import side_engine
    return side_engine(hop, eng, side)


def inventory(hop, eng, db, side):
    """[(name, on)] of the hop's jobs in this database on one side: `on`
    True where it runs, False where it does not. None where the engine
    keeps none, or they cannot be read."""
    got = _read(hop, eng, db, side)
    return None if got is None else [(name, on) for name, on, ours in got
                                     if ours]


def left_alone(hop, eng, db, side):
    """The running jobs of this database on one side that write only
    tables the hop leaves out: neither switched off nor on, by the cutover
    or its undo. [] where none, or they cannot be read - and, unread,
    where the hop leaves nothing out by name."""
    from .config import scoped
    if not scoped(hop):
        return []
    return [name for name, on, ours in _read(hop, eng, db, side) or []
            if on and not ours]


def _read(hop, eng, db, side):
    """[(name, on, the hop's)] of the jobs in this database on one side,
    or None."""
    from .config import scoped
    kind, eng = _side(hop, eng, side)
    try:
        if kind == "postgres":
            got = _pg_jobs(eng, db, side)
        elif kind == "mysql":
            got = [(name, status == "ENABLED", body) for name, status, _, body
                   in _my_events(eng, db, side)]
        elif kind == "mssql":
            got = _ms_jobs(eng, db, side)
        else:
            return None
    except Exception:  # noqa: BLE001 - not readable: said as unknown
        return None
    # a hop that leaves nothing out by name takes every job, as it always
    # did, and reads nothing more to say so; where the tables and routines
    # cannot be read, every job is the hop's as before
    names = None
    if got and scoped(hop):
        try:
            names = _names(kind, eng, db, side)
        except Exception:  # noqa: BLE001 - unread: nothing left out
            names = None
    return [(name, on, names is None or not _outside(eng, db, body, names))
            for name, on, body in got]


#: words that run what a job's text does not name - a routine, a statement
#: built as text: what such a job writes cannot be read from its text
OPAQUE = frozenset({"call", "execute", "prepare", "perform"})


def _outside(eng, db, body, names):
    """Whether a job's text writes only tables the hop leaves out: every
    table of its database it names is one, and it names one at least. A
    job that names none, runs a routine of the database or a statement
    built as text may write anything, and stays the hop's."""
    tables, routines = names
    words = {w.lower() for w in re.findall(r"[A-Za-z_][\w$]*",
                                           str(body or ""))}
    if words & (OPAQUE | routines):
        return False
    named = [t for leaf, ids in tables.items() if leaf in words for t in ids]
    return bool(named) and not any(eng.in_scope(db, t) for t in named)


def _names(kind, eng, db, side):
    """({leaf name, lower case: [table as the engine lists it]}, {routine
    name, lower case}) of one database on one side: what a job's text is
    read against."""
    tables = {}
    for t in (_ms_tables(eng, db, side) if kind == "mssql"
              else eng._all_tables(side, db)):
        tables.setdefault(str(t).rpartition(".")[2].strip('"').lower(),
                          []).append(t)
    if kind == "mssql":
        routines = {str(r[0]).lower() for r in eng._rows(
            side, db, "select o.name from sys.objects o where o.type in"
                      " ('P', 'FN', 'IF', 'TF') and o.is_ms_shipped = 0")}
    elif kind == "postgres":
        got = eng._psql(side, db, "select distinct lower(p.proname) from"
                                  " pg_proc p join pg_namespace n on n.oid ="
                                  " p.pronamespace where n.nspname not in"
                                  " ('pg_catalog', 'information_schema')")
        routines = set(got.split())
    else:
        routines = {str(r[0]).lower() for r in eng._q(
            side, "select routine_name from information_schema.routines"
                  " where routine_schema = %s", (eng._d(side, db),))}
    return tables, routines


def _ms_jobs(eng, db, side):
    """[(name, on, its text)] of the SQL Server Agent jobs with a step that
    runs in this database. A job belongs to the server, not to a database:
    it is the database's for each step it runs there, and its text is
    those steps'."""
    out = {}
    for name, on, command in eng._rows(
            side, db, "select j.name, j.enabled, cast(s.command as"
                      " nvarchar(max)) from msdb.dbo.sysjobs j join"
                      " msdb.dbo.sysjobsteps s on s.job_id = j.job_id where"
                      " s.database_name = db_name() order by j.name,"
                      " s.step_id"):
        one = out.setdefault(str(name), [bool(on), []])
        one[1].append(str(command or ""))
    return [(name, on, "\n".join(steps)) for name, (on, steps) in
            sorted(out.items())]


def _ms_tables(eng, db, side):
    """Every table of the database as SQL Server lists them, the hop's or
    not: what a job's text is read against."""
    return [str(r[0]) for r in eng._rows(
        side, db, "select s.name + '.' + t.name from sys.tables t join"
                  " sys.schemas s on s.schema_id = t.schema_id where"
                  " t.is_ms_shipped = 0 order by 1")]


def _ms_enable(name, on):
    """The statement that switches a SQL Server Agent job on or off,
    its name a literal."""
    return ("exec msdb.dbo.sp_update_job @job_name = N'"
            + str(name).replace("'", "''") + f"', @enabled = {int(bool(on))}")


def _pg_cron_db(eng, db, side):
    """The database pg_cron keeps its jobs in, where it is installed."""
    got = eng._psql(side, db, "select coalesce((select setting from"
                              " pg_settings where name ="
                              " 'cron.database_name'), '')").strip()
    return got or None


def _pg_jobs(eng, db, side):
    """[(name, on, its command)] of the pg_cron jobs run in this
    database."""
    home = _pg_cron_db(eng, db, side)
    if not home:
        return []
    dbname = eng._d(side, db)
    got = eng._psql(side, home,
                    "select jobid||chr(31)||coalesce(jobname, jobid::text)"
                    "||chr(31)||active||chr(31)||regexp_replace(command,"
                    " '\\s+', ' ', 'g') from cron.job where database ="
                    f" '{dbname}' order by 1") if _has_cron(eng, side, home) \
        else ""
    out = []
    for line in got.splitlines():
        jid, name, active, command = (line.split("\x1f") + [""])[:4]
        out.append((name, active == "t", command))
    return out


def _has_cron(eng, side, home):
    return eng._psql(side, home, "select count(*) from pg_extension where"
                                 " extname = 'pg_cron'").strip() == "1"


def _my_events(eng, db, side):
    """[(name, status, definer, body)] of the events of this database."""
    return [(str(n), str(s), str(d), str(b or "")) for n, s, d, b in eng._q(
        side, "select event_name, status, definer, event_definition from"
              " information_schema.events where event_schema = %s"
              " order by 1", (eng._d(side, db),))]


def _my_off(eng, side):
    """How this server says "not here": `DISABLE ON REPLICA` from 8.0.22,
    `DISABLE ON SLAVE` before and on MariaDB - asked of the server."""
    got = str(eng._q(side, "select version()")[0][0])
    if "mariadb" in got.lower():
        return "disable on slave"
    try:
        v = tuple(int(x) for x in got.split("-")[0].split(".")[:3])
    except ValueError:
        v = (8, 0, 0)
    return "disable on replica" if v >= (8, 0, 22) else "disable on slave"


def _my_definer(definer):
    user, _, host = str(definer).rpartition("@")
    return f"'{user}'@'{host}'"


def _path(hop):
    return hop.report_dir() / "cutover-jobs.json"


def recorded(hop):
    """The switches recorded so far, oldest first. A record that cannot be
    read whole stops, naming it: read as nothing, a rollback would switch
    nothing back (`durable`)."""
    from . import durable
    return durable.read_state(_path(hop), "journal") or []


def _save(hop, done):
    """Recorded before the switch it records, and on the disk (`durable`)."""
    from . import durable
    durable.write_json(_path(hop), done, indent=1)


def switch(hop, eng, db, side, names, on, say):
    """Switch these jobs on (or off) on one side, recording each before
    it is switched. Returns how many were switched."""
    kind, eng = _side(hop, eng, side)
    done = recorded(hop)
    n = 0
    if kind == "mysql":
        tdb = eng._d(side, db)
        have = {name: (status, definer) for name, status, definer, _ in
                _my_events(eng, db, side)}
        for name in names:
            if name not in have:
                continue
            status, definer = have[name]
            if (status == "ENABLED") == on:
                continue
            done.append({"db": db, "side": side, "name": name,
                         "was": status, "definer": definer})
            _save(hop, done)
            eng._q(side, f"alter definer = {_my_definer(definer)} event"
                         f" `{tdb}`.`{name}` "
                         + ("enable" if on else _my_off(eng, side)))
            n += 1
    elif kind == "mssql":
        have = {name: enabled for name, enabled, _ in _ms_jobs(eng, db, side)}
        for name in names:
            if name not in have or have[name] == on:
                continue
            done.append({"db": db, "side": side, "name": name,
                         "was": have[name]})
            _save(hop, done)
            eng._cmd(side, db, _ms_enable(name, on))
            n += 1
    elif kind == "postgres":
        home = _pg_cron_db(eng, db, side)
        if not home or not _has_cron(eng, side, home):
            return 0
        have = {}
        for line in eng._psql(side, home,
                              "select jobid||chr(31)||coalesce(jobname,"
                              " jobid::text)||chr(31)||active from cron.job"
                              f" where database = '{eng._d(side, db)}'"
                              ).splitlines():
            jid, name, active = line.split("\x1f")
            have[name] = (jid, active == "t")
        for name in names:
            if name not in have or have[name][1] == on:
                continue
            jid, was = have[name]
            done.append({"db": db, "side": side, "name": name,
                         "was": was, "jobid": jid})
            _save(hop, done)
            eng._psql(side, home, f"select cron.alter_job({int(jid)},"
                                  f" active := {'true' if on else 'false'})")
            n += 1
    if n:
        say(f"{eng._d(side, db)}: {n} jobs switched "
            + ("on" if on else "off") + (" on the target" if side == "dst"
                                         else " on the source"))
    return n


def undo(hop, eng, say, side=None):
    """Put every recorded switch back, the last first; only one side's
    where `side` is given."""
    done = recorded(hop)
    keep = []
    for entry in reversed(done):
        if side and entry["side"] != side:
            keep.insert(0, entry)
            continue
        s, db, name = entry["side"], entry["db"], entry["name"]
        kind, e = _side(hop, eng, s)
        if kind == "mysql":
            was = entry["was"]
            how = ("enable" if was == "ENABLED" else
                   "disable" if was == "DISABLED" else _my_off(e, s))
            e._q(s, f"alter definer = {_my_definer(entry['definer'])}"
                    f" event `{e._d(s, db)}`.`{name}` {how}")
        elif kind == "mssql":
            e._cmd(s, db, _ms_enable(name, bool(entry["was"])))
        else:
            home = _pg_cron_db(e, db, s)
            e._psql(s, home, f"select cron.alter_job({int(entry['jobid'])},"
                             " active :="
                             f" {'true' if entry['was'] else 'false'})")
        say(f"{e._d(s, db)}: job {name} put back as it was")
    if keep:
        _save(hop, keep)
    else:
        _path(hop).unlink(missing_ok=True)


def forget(hop):
    """The switches stand: the cutover finished."""
    _path(hop).unlink(missing_ok=True)
