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
"""
import json


def _side(hop, eng, side):
    from .freeze import side_engine
    return side_engine(hop, eng, side)


def inventory(hop, eng, db, side):
    """[(name, on)] of the jobs in this database on one side: `on` True
    where it runs, False where it does not. None where the engine keeps
    none, or they cannot be read."""
    kind, eng = _side(hop, eng, side)
    try:
        if kind == "postgres":
            return _pg_jobs(eng, db, side)
        if kind == "mysql":
            return [(name, status == "ENABLED") for name, status, _ in
                    _my_events(eng, db, side)]
    except Exception:  # noqa: BLE001 - not readable: said as unknown
        return None
    return None


def _pg_cron_db(eng, db, side):
    """The database pg_cron keeps its jobs in, where it is installed."""
    got = eng._psql(side, db, "select coalesce((select setting from"
                              " pg_settings where name ="
                              " 'cron.database_name'), '')").strip()
    return got or None


def _pg_jobs(eng, db, side):
    home = _pg_cron_db(eng, db, side)
    if not home:
        return []
    dbname = eng._d(side, db)
    got = eng._psql(side, home,
                    "select jobid||chr(31)||coalesce(jobname, jobid::text)"
                    "||chr(31)||active from cron.job where database ="
                    f" '{dbname}' order by 1") if _has_cron(eng, side, home) \
        else ""
    out = []
    for line in got.splitlines():
        jid, name, active = line.split("\x1f")
        out.append((name, active == "t"))
    return out


def _has_cron(eng, side, home):
    return eng._psql(side, home, "select count(*) from pg_extension where"
                                 " extname = 'pg_cron'").strip() == "1"


def _my_events(eng, db, side):
    return [(str(n), str(s), str(d)) for n, s, d in eng._q(
        side, "select event_name, status, definer from"
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
    """The switches recorded so far, oldest first."""
    try:
        return json.loads(_path(hop).read_text())
    except (OSError, ValueError):
        return []


def _save(hop, done):
    tmp = _path(hop).with_suffix(".tmp")
    tmp.write_text(json.dumps(done, indent=1))
    tmp.replace(_path(hop))


def switch(hop, eng, db, side, names, on, say):
    """Switch these jobs on (or off) on one side, recording each before
    it is switched. Returns how many were switched."""
    kind, eng = _side(hop, eng, side)
    done = recorded(hop)
    n = 0
    if kind == "mysql":
        tdb = eng._d(side, db)
        have = {name: (status, definer) for name, status, definer in
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
