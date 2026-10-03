"""SQL Server's own backup, restored on the target: a database's pages
carried whole, and its changes followed from the version the restored
copy holds (backlog P0-SCENARIOS - the owner's "SQL Server Web edition to
Datacenter (Enterprise)").

The source's server writes a copy-only backup of each database where the
hop says (`physical: {backup: <a directory>}`) - copy-only, so the
source's own chain of backups is as it was; the hop naming the directory
is what asks for the file, and its server records the backup in its own
history as it records any. The target's server proves it page by page
first (`RESTORE VERIFYONLY ... WITH CHECKSUM`, the pages' own checksums
written by `BACKUP ... WITH CHECKSUM`), then restores it under the
database's name on the target (`db_map` renames it), its files where the
target keeps its own. A restore opens a database an older version made
and upgrades it as it does, so the same rung is the way up a version too
(`upgrade`): the pages arrive with their indexes built and nothing is
decoded. The file stays in the directory for the operator to remove.

What it needs, each read on both servers before anything is written
(`needs`): the hop allows it and names the directory; neither side a
database of Azure SQL Database, which backs up to no file, nor a managed
instance, which backs up only to a URL; the target at the source's
version or a later one that still reads it (a backup is read by no
older version, and by a newer one from 2008 on - 2005 on for 2016 and
earlier); the target's edition has every feature the database holds
(`editions.refusals` - a restored database with one it has not does not
open) and holds its files (Express's cap on a database); the hop carries
every table of it, under any name; no database of that name on the
target; the right to back it up on the source and to make a database on
the target; and, to go on after it, change tracking on the database.
Proved before it is trusted: both servers see the directory
(`sys.dm_os_file_exists`, from 2017; before it the restore's own check
says it).

Going on after it: the restored copy holds the source's change tracking
as it was at the backup's end, so its `CHANGE_TRACKING_CURRENT_VERSION()`
is where the tail starts - each change after it read on the source and
applied by key, which converges on a change the copy already holds.
"""
import re
import time

from . import physical
from .config import narrowed_said
from .physical import Need, _hop_allows, _hop_says, _said_count

#: what an operator reads for this way
BACKUP = "the database's own backup, restored on the target"

#: the oldest version a target restores a backup of, by the target's
#: major version (Microsoft Learn, "Compatibility support" of each
#: version's RESTORE page)
RESTORES_FROM = ((17, 11), (14, 10), (0, 9))


def oldest_read(dst_major):
    """The oldest major version whose backup a target of `dst_major`
    restores."""
    for at, oldest in RESTORES_FROM:
        if dst_major >= at:
            return oldest
    return RESTORES_FROM[-1][1]


def _esc(text):
    return str(text).replace("'", "''")


def join(directory, name):
    """`name` in `directory`, as a server of that directory's kind writes
    it: a Windows path by backslashes, any other by slashes."""
    d = str(directory).rstrip("/\\")
    sep = "\\" if ("\\" in d or re.match(r"^[A-Za-z]:", d)) else "/"
    return f"{d}{sep}{name}"


def _safe(text):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(text))


class MsFacts(physical.Facts):
    """What the backup's needs read of both SQL Server servers."""

    def _one(self, side, db, sql):
        return self.eng._rows(side, db, sql)

    def _is_ours(self, rec):
        return rec.get("rung") == RUNG_NAME and is_the_seed(self.eng, rec)

    def _about(self, side):
        from .lacks import numbers
        ver, n = self._one(side, "master", "select cast(serverproperty("
                                           "'ProductVersion') as"
                                           " nvarchar(128)),"
                                           " cast(serverproperty("
                                           "'EngineEdition') as int)")[0]
        return {"version": numbers(ver, 3), "text": str(ver),
                "engine_edition": int(n or 0)}

    def _src_about(self):
        return self._about("src")

    def _dst_about(self):
        return self._about("dst")

    def _editions(self):
        from . import editions
        return editions.pair(self.eng)

    def _followed(self):
        return list(self.eng.databases())

    def _refused(self):
        """{database: the features it holds that the target's edition
        does not have, in words}."""
        from . import editions
        src, dst = self["editions"]
        if src is None or not src.known or not dst.known:
            raise RuntimeError("the editions could not be read")
        return {d: editions.refusals(self.eng, d, src, dst)
                for d in self["followed"]}

    def _allocated(self):
        """{database: the bytes of its data files}, as a restore makes
        them on the target."""
        return {d: int(self._one("src", d, "select sum(cast(size as bigint))"
                                           " * 8192 from sys.database_files"
                                           " where type = 0")[0][0] or 0)
                for d in self["followed"]}

    def _dst_databases(self):
        return {str(r[0]).lower() for r in self._one(
            "dst", "master", "select name from sys.databases")}

    def _backup_rights(self):
        return {d: bool(self._one("src", d, "select has_perms_by_name("
                                            "db_name(), 'DATABASE', 'BACKUP"
                                            " DATABASE')")[0][0])
                for d in self["followed"]}

    def _create_right(self):
        return bool(self._one("dst", "master", "select has_perms_by_name("
                                               "null, null, 'CREATE ANY"
                                               " DATABASE')")[0][0])

    def _tracking(self):
        return {d: bool(self._one("src", d, "select count(*) from"
                                            " sys.change_tracking_databases"
                                            " where database_id = db_id()")
                        [0][0]) for d in self["followed"]}


def _following(facts):
    return facts.mode == "full+cdc"


def _no_file(about):
    return {5: "a database of Azure SQL Database, which backs up to no"
               " file", 8: "a managed instance, which backs up only to a"
                           " URL"}.get(about["engine_edition"])


def _newer_or_same(f):
    a, b = f["src_about"]["version"], f["dst_about"]["version"]
    return bool(a and b) and b[0] >= a[0] and a[0] >= oldest_read(b[0])


def _why_version(f):
    a, b = f["src_about"], f["dst_about"]
    if b["version"] and a["version"] and b["version"][0] < a["version"][0]:
        return (f"the target runs {b['text']}, older than the source's"
                f" {a['text']}: a backup is restored only by the version"
                " that made it or a newer one")
    return (f"the target runs {b['text']}, which restores no backup older"
            f" than version {oldest_read((b['version'] or (0,))[0])} -"
            f" the source's is {a['text']}")


def _edition_takes(f):
    return not any(f["refused"].values())


def _why_edition(f):
    src, dst = f["editions"]
    said = "; ".join(f"{d} holds {', '.join(r)}"
                     for d, r in f["refused"].items() if r)
    return (f"{dst.label()} does not have what the source's databases hold"
            f" - {said}: a restored database with it does not open there,"
            " and the logical copy makes the tables without it")


def _cap(f):
    from . import editions
    return editions.caps(f["editions"][1]).get("database")


def _fits(f):
    cap = _cap(f)
    return cap is None or all(n <= cap for n in f["allocated"].values())


def _why_fits(f):
    from .wording import human_bytes
    cap = _cap(f)
    big = [f"{d} ({human_bytes(n)})" for d, n in f["allocated"].items()
           if n > cap]
    return (f"{f['editions'][1].label()} holds {human_bytes(cap)} of data"
            f" files a database, and a restore makes the source's as they"
            f" are: {', '.join(big)}")


#: what a hop may narrow and still be seeded from a backup: the
#: databases' names on the target (a restore names each), and the changes
#: the tail carries after it (`config.narrowed_by`)
_SEED_SERVES = ("databases_renamed", "changes_carried")


def _covers(f):
    return not narrowed_said(f.hop, _SEED_SERVES)


def _free_names(f):
    have = f["dst_databases"]
    return f["our_seed"] is True or not any(
        f.hop.target_db(d).lower() in have for d in f["followed"])


def _needs():
    return (
        _hop_allows(BACKUP),
        _hop_says(("backup",), "where the source's server writes the"
                               " backup - a directory the target's server"
                               " reads too"),
        Need("a source that backs up to a file", ("src_about",),
             lambda f: not _no_file(f["src_about"]),
             lambda f: f"the source is {_no_file(f['src_about'])}"),
        Need("a target that restores from a file", ("dst_about",),
             lambda f: not _no_file(f["dst_about"]),
             lambda f: f"the target is {_no_file(f['dst_about'])}"),
        Need("the target at the source's version or a later one that reads"
             " its backups", ("src_about", "dst_about"), _newer_or_same,
             _why_version),
        Need("every table of the database carried, under any name", (),
             _covers,
             lambda f: (f"the hop {narrowed_said(f.hop, _SEED_SERVES)},"
                        " and a backup carries every table of the database"
                        " as it is")),
        Need("no database of that name on the target",
             ("dst_databases",), _free_names,
             lambda f: ("the target already holds "
                        + _said_count(sorted(
                            f.hop.target_db(d) for d in f["followed"]
                            if f.hop.target_db(d).lower()
                            in f["dst_databases"]))
                        + ": a restore makes the database, and replaces"
                          " none")),
        Need("the target's edition has what the databases hold",
             ("editions", "refused"), _edition_takes, _why_edition),
        Need("the target's edition holds the databases' files",
             ("editions", "allocated"), _fits, _why_fits),
        Need("the right to back up each database on the source",
             ("backup_rights",), lambda f: all(f["backup_rights"].values()),
             lambda f: (f"{f.hop.source.user} may not back up "
                        + ", ".join(d for d, ok in f["backup_rights"].items()
                                    if not ok)
                        + "; a DBA grants it: alter role db_backupoperator"
                          " add member <its user>, in each")),
        Need("the right to make a database on the target", ("create_right",),
             lambda f: f["create_right"],
             lambda f: (f"{f.hop.target.user} may not make a database on"
                        " the target; a DBA grants it: alter server role"
                        f" dbcreator add member [{f.hop.target.user}];")),
        Need("change tracking on each database, to go on from the copy",
             ("tracking",), lambda f: all(f["tracking"].values()),
             lambda f: ("change tracking is off for "
                        + ", ".join(d for d, on in f["tracking"].items()
                                    if not on)
                        + ", and the changes after the copy are read"
                          " from it"), when=_following),
    )


def _prove(facts):
    """Both servers see the directory, where they can say it (2017 on)."""
    opts = facts["options"]
    for side, where in (("src", opts.get("backup")),
                        ("dst", opts.get("restore_from")
                         or opts.get("backup"))):
        try:
            got = facts.eng._rows(
                side, "master", "select file_is_a_directory from"
                                " sys.dm_os_file_exists(N'"
                                + _esc(str(where).rstrip("/\\")) + "')")
        except Exception:  # noqa: BLE001 - before 2017: the restore says
            continue
        if not got or not got[0][0]:
            who = "source's" if side == "src" else "target's"
            return f"the {who} server sees no directory {where}"
    return None


RUNG_NAME = "physical-backup"


def rungs():
    return (physical.Rung(
        RUNG_NAME, BACKUP, gives=physical.GIVES | {"pages proved"},
        needs=_needs(), prove=_prove, footprint=1,
        because="the database's pages arrive with their indexes built and"
                " nothing decoded, proved by their own checksums",
        engine="mssql"),)


def facts_for(hop, eng, mode):
    return MsFacts(hop, eng, mode)


# --- the copy -----------------------------------------------------------------------

def _run(eng, side, sql):
    """One statement on a side, outside any transaction - a backup and a
    restore are refused inside one - with its result rows, if any."""
    conn = eng._connect(side, "master")
    try:
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute(sql)
        rows = []
        while True:
            try:
                rows += [list(r) for r in cur.fetchall()]
            except Exception:  # noqa: BLE001 - a statement with no rows
                pass
            if not cur.nextset():
                break
        return rows
    finally:
        conn.close()


def _compresses(edition):
    """Whether the source's edition writes a compressed backup: Standard
    and the Enterprise class do, Web and Express do not."""
    from .editions import ENTERPRISE_CLASS
    return edition is not None and (edition.name in ENTERPRISE_CLASS
                                     or edition.name == "standard")


def _moves(eng, name, files):
    """MOVE clauses putting each file of the backup where the target keeps
    its own, named for the target's database."""
    data, log = _run(eng, "dst", "select cast(serverproperty("
                                 "'InstanceDefaultDataPath') as"
                                 " nvarchar(4000)), cast(serverproperty("
                                 "'InstanceDefaultLogPath') as"
                                 " nvarchar(4000))")[0]
    out = []
    for f in files:
        logical, phys, kind = str(f[0]), str(f[1]), str(f[2]).upper()
        ext = re.search(r"(\.[A-Za-z0-9]+)$", phys.replace("\\", "/"))
        leaf = _safe(f"{name}_{logical}") + (ext.group(1) if ext and
                                             kind != "S" else "")
        where = join(log if kind == "L" else data, leaf)
        out.append(f"move N'{_esc(logical)}' to N'{_esc(where)}'")
    return out


def restore_one(hop, eng, d, opts, src_edition, log):
    """One database backed up on the source and restored on the target:
    {"file", "version", "created"} - the change tracking version the copy
    holds (None where the database tracks none) and when the target made
    it, which says the copy is this one on a later run."""
    name = hop.target_db(d)
    stamp = time.strftime("%Y%m%d%H%M%S")
    leaf = f"migkit-{_safe(hop.name)}-{_safe(d)}-{stamp}.bak"
    src_path = join(opts["backup"], leaf)
    dst_path = join(opts.get("restore_from") or opts["backup"], leaf)
    q = eng._q
    extra = ", compression" if _compresses(src_edition) else ""
    began = time.time()
    _run(eng, "src", f"backup database {q(d)} to disk = N'{_esc(src_path)}'"
                     f" with copy_only, checksum, init, format{extra}")
    log(f"{d}: backed up on the source, copy-only, in"
        f" {time.time() - began:,.0f}s ({src_path})")
    _run(eng, "dst", f"restore verifyonly from disk = N'{_esc(dst_path)}'"
                     " with checksum")
    files = _run(eng, "dst", f"restore filelistonly from disk ="
                             f" N'{_esc(dst_path)}'")
    began = time.time()
    _run(eng, "dst", f"restore database {q(name)} from disk ="
                     f" N'{_esc(dst_path)}' with checksum, recovery, "
                     + ", ".join(_moves(eng, name, files)))
    log(f"{d}: its pages proved by their checksums and restored as {name}"
        f" in {time.time() - began:,.0f}s")
    got = eng._rows("dst", d, "select change_tracking_current_version(),"
                              " (select cast(create_date as nvarchar(30))"
                              " from sys.databases where database_id ="
                              " db_id())")[0]
    version = int(got[0]) if got[0] is not None else None
    return {"file": dst_path, "version": version, "created": str(got[1]),
            "name": name}


def is_the_seed(eng, rec):
    """Whether each database the seed `rec` restored is the target's still:
    the target made it when the seed says."""
    try:
        for d, one in (rec.get("databases") or {}).items():
            got = eng._rows("dst", d, "select cast(create_date as"
                                      " nvarchar(30)) from sys.databases"
                                      " where database_id = db_id()")
            if not got or str(got[0][0]) != str(one.get("created")):
                return False
    except Exception:  # noqa: BLE001 - not reachable: not this seed
        return False
    return bool(rec.get("databases"))


def carry(hop, eng, rung, db, mode, go, log):
    """Each database of the hop backed up on the source and restored on the
    target, then its changes gone on from the version the copy holds."""
    from . import editions, planner
    opts = physical.settings(hop)
    dbs = [db] if db else list(eng.databases())
    rec = physical.seeded(hop)
    if rec and rec.get("rung") == rung.name and is_the_seed(eng, rec):
        log(f"the target holds the databases restored on {rec['at']};"
            " going on from them")
    else:
        log(f"plan: each database backed up on the source, copy-only, to"
            f" {opts['backup']}, proved and restored on the target, where a"
            " newer version upgrades it as it opens it; the changes after"
            " it are read from the change tracking version the copy holds;"
            " the file stays there for the operator to remove")
        if not go:
            log("dry-run: nothing backed up")
            return
        src_edition = editions.pair(eng)[0]
        began = time.time()
        made = {}
        for d in dbs:
            made[d] = restore_one(hop, eng, d, opts, src_edition, log)
        took = time.time() - began
        rec = {"rung": rung.name, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "seconds": round(took, 1), "databases": made}
        physical._save_seed(hop, rec)
        planner.record_rate(hop, rung.name, physical._rows(eng, dbs), took)
    for d in dbs:
        at = ((rec.get("databases") or {}).get(d) or {}).get("version")
        if at is None and mode == "full+cdc":
            raise SystemExit(f"{d} was restored with no change tracking"
                             " version, so nothing says where its changes"
                             " go on from")
        physical._record_positions(hop, [d], at, at)
