"""A drain proved by a mark written on the source, where the stream keeps
no position migkit can hold it to.

The cutover's drain takes the source's position after the freeze and
waits for the hop's own stream to pass it. Some streams give no position
migkit can read: a MySQL replica without GTIDs (a file and an offset of
the source's own binary log mean nothing once the replica's source
changes), the streaming pipeline (its offsets live in its own broker),
a stream another tool runs. There a cutover of a hop that turns
`cutover:` on falls back to a mark: a row migkit writes on the source
after the freeze, in a table of its own, and waits for on the target.
An applier that keeps the source's commit order has applied everything
committed before the mark once the mark is there; one that does not - a
pipeline that applies each table's changes on their own - is waited for
by its own offsets too (`cutover.Leg.settle`), and the fenced check that
follows compares the rows either way.

The mark is written only inside the cutover's permission: a hop with
`cutover:`, a freeze of migkit's own (never `freeze: operator`, where
migkit writes nothing), and a freeze that migkit's own account writes
through (`freeze.OURS`). The table is made before the freeze - so the
stream has taken it in, and a first mark proved it arrives, before any
outage - and is taken away again: on the undo, and once the old source
is written again (the point of no return, or going back).

    PostgreSQL   public.migkit_cutover_mark (token text key, at timestamptz)
    MySQL        <db>.migkit_cutover_mark   (token varchar(64) key, at
                 datetime(6)) - made on the source only: a replica takes the
                 table from the source's log
    MongoDB      the collection migkit_cutover_mark, {_id: token, at}

It is migkit's own table (`config.MIGKIT_OWN`), so no check compares it
and no copy carries it as the application's.

The other drain is the operator's: `to_position` waits for a running tail
to apply a named position (or `now`), holds it there, and reports the seam
- where the tail really stopped, whether it went past, and whether the
target checks the same there. It is one-way and goes through the
interlock's guard (`interlock.before_one_way`).
"""
import json
import re
import time

from .freeze import side_engine

TABLE = "migkit_cutover_mark"
#: the engines a mark is written on and read back from
MARKED = ("postgres", "mysql", "mongodb")


def able(hop, eng):
    """"" where a mark can be written on the source and read back on the
    target, or why not."""
    src = side_engine(hop, eng, "src")[0]
    dst = side_engine(hop, eng, "dst")[0]
    for kind, side in ((src, "source"), (dst, "target")):
        if kind not in MARKED:
            return f"no mark of migkit's is written on or read back from a" \
                   f" {kind} {side}"
    return ""


def make(hop, eng, db):
    """The table on the source, where it is not there."""
    kind, src = side_engine(hop, eng, "src")
    if kind == "postgres":
        src._psql("src", db, f"create table if not exists public.{TABLE}"
                             " (token text primary key, at timestamptz not"
                             " null default clock_timestamp())")
    elif kind == "mysql":
        _my_write(src, f"create table if not exists {_my_table(src, db)}"
                       " (token varchar(64) primary key, at datetime(6) not"
                       " null)")
    elif kind == "mongodb":
        # a collection is made by its first document
        pass


def put(hop, eng, db, token):
    """The mark `token` written on the source, committed."""
    kind, src = side_engine(hop, eng, "src")
    if kind == "postgres":
        src._psql("src", db, f"insert into public.{TABLE} (token) values"
                             f" ('{_safe(token)}')")
    elif kind == "mysql":
        _my_write(src, f"insert into {_my_table(src, db)} (token, at) values"
                       f" ('{_safe(token)}', now(6))")
    elif kind == "mongodb":
        import datetime
        src._client("src")[db][TABLE].insert_one(
            {"_id": token, "at": datetime.datetime.now(datetime.timezone.utc)},
            )
    return token


def seen(hop, eng, db, token):
    """Whether the target holds the mark `token`: False while its table is
    not there yet."""
    kind, dst = side_engine(hop, eng, "dst")
    tdb = hop.target_db(db)
    try:
        if kind == "postgres":
            got = dst._psql("dst", db, f"select count(*) from public.{TABLE}"
                                       f" where token = '{_safe(token)}'")
            return got.strip() == "1"
        if kind == "mysql":
            got = dst._q("dst", f"select count(*) from `{_tick(tdb)}`"
                                f".`{TABLE}` where token = %s", (token,))
            return bool(got and int(got[0][0]))
        if kind == "mongodb":
            return dst._client("dst")[tdb][TABLE].find_one(
                {"_id": token}) is not None
    except Exception as e:  # noqa: BLE001 - a table not there yet is "no"
        if _missing(e):
            return False
        raise
    return False


def wait(hop, eng, db, token, timeout, seen_by=None):
    """True once the target holds the mark, False when `timeout` seconds
    pass first. `seen_by(db, token)` reads it where the stream keeps it
    instead (a pipeline's own topic)."""
    look = seen_by or (lambda d, t: seen(hop, eng, d, t))
    end = time.monotonic() + timeout
    while True:
        if look(db, token):
            return True
        if time.monotonic() >= end:
            return False
        time.sleep(0.1)


def drop(hop, eng, db):
    """The table taken away from both sides, where it is there: the
    target's first, since a source's drop that a replica carries finds it
    gone and asks only `if exists`."""
    for side in ("dst", "src"):
        kind, e = side_engine(hop, eng, side)
        name = hop.target_db(db) if side == "dst" else db
        if kind == "postgres":
            e._psql(side, db, f"drop table if exists public.{TABLE}")
        elif kind == "mysql":
            conn = e._conn(side)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"drop table if exists `{_tick(name)}`"
                                f".`{TABLE}`")
                conn.commit()
            finally:
                conn.close()
        elif kind == "mongodb":
            e._client(side)[name][TABLE].drop()


def _safe(token):
    """A mark's token is migkit's own: letters, digits and a dash."""
    token = str(token)
    if not all(c.isalnum() or c == "-" for c in token):
        raise ValueError(f"a mark's token is letters, digits and dashes,"
                         f" not {token!r}")
    return token


def _tick(name):
    return str(name).replace("`", "``")


def _my_table(src, db):
    return f"`{_tick(src._d('src', db))}`.`{TABLE}`"


def _my_write(src, sql):
    """A statement on the source, committed, and written to its log so
    the stream carries it."""
    conn = src._conn("src")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _missing(e):
    said = str(e).lower()
    return ("does not exist" in said or "doesn't exist" in said
            or "1146" in said or "42p01" in said)


# --- a drain the operator stops at a position ---------------------------------

#: what the operator writes to drain to where the source stands this moment
NOW = "now"
_LSN = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")
_GTID = re.compile(r"^[0-9A-Fa-f-]{36}:\d+(-\d+)?(:\d+(-\d+)?)*"
                   r"(,\s*[0-9A-Fa-f-]{36}:\d+(-\d+)?(:\d+(-\d+)?)*)*$")


def named(hop, fence, db, text):
    """The position the operator asked the drain to stop at: `now` is
    where the source stands at this moment (as the stream's own fence
    reads it); anything else is a position written the way the engine
    writes it - a WAL location, a set of transaction ids, an offset the
    engine names. Raises SystemExit saying what is wrong with it and what
    to write; None never comes back."""
    text = str(text or "").strip()
    if not text:
        raise SystemExit(f"{db}: drain to what? Name a position, or write"
                         f" `{NOW}` for where the source stands this moment")
    kind = side_engine(hop, fence, "src")[0]
    if text.lower() == NOW:
        at = fence.src_lsn(db)
        if at is None:
            raise SystemExit(
                f"{db}: the source gives no position this stream can be"
                " held to, so there is nothing to stop at. A cutover of"
                " this hop proves its drain by a mark instead")
        return at
    own = getattr(fence, "parse_position", None)
    if own is not None:
        return own(db, text)
    if kind == "postgres" and not _LSN.match(text):
        raise SystemExit(f"{text!r} is not a WAL location - write it as"
                         " 0/16B3748 (the form pg_current_wal_lsn shows)")
    if kind == "mysql" and not _GTID.match(text):
        raise SystemExit(f"{text!r} is not a set of transaction ids - write"
                         " it as uuid:1-500 (several, comma-separated)")
    return text


def order(kind, a, b):
    """-1, 0 or 1 as position `a` is before, at or after `b`, where the
    engine's positions have one total order migkit can read (a PostgreSQL
    WAL location); None where they have not - a set of transaction ids
    is partial, an offset is the engine's own."""
    if kind != "postgres" or not (_LSN.match(str(a)) and _LSN.match(str(b))):
        return None

    def n(x):
        hi, lo = str(x).split("/")
        return (int(hi, 16) << 32) + int(lo, 16)
    return (n(a) > n(b)) - (n(a) < n(b))


def to_position(hop, eng, db, where, at, timeout, go, log, fence=None,
                verify=None):
    """Drain the change tail of `db` to the position `at` and hold it
    there: the apply is waited for until the target has the position,
    paused, and the seam read back from what the tail saved.

    One-way, so it is planned first and taken with `go`
    (`interlock.before_one_way`); without `go` the position and the
    tail's state are said and nothing is touched. `verify(db)` returns
    (True or False, what it saw) - the check of the target at the seam -
    and a drain whose check does not pass says so and is not called done.
    Returns {"position", "reached", "held", "applied", "past", "verified",
    "said"}; raises SystemExit where the drain cannot be taken at all."""
    from . import interlock, tailctl
    fence = fence or eng
    pos = named(hop, fence, db, at)
    kind = side_engine(hop, fence, "src")[0]
    if not tailctl.alive(where):
        raise SystemExit(f"{db}: no change tail runs for this database, so"
                         " nothing applies the changes up to a position."
                         f" Start it (`migkit move {hop.name} --mode cdc"
                         " --go`) and drain again")
    interlock.before_one_way(hop, eng, db, "drain", go, log)
    if not go:
        log(f"{db}: would drain the tail to {pos} and hold it there")
        return {"position": pos, "reached": None, "held": None,
                "applied": None, "past": None, "verified": None,
                "said": "planned"}
    began = time.monotonic()
    got = fence.fence_wait(db, pos, timeout=timeout)
    took = time.monotonic() - began
    if not got:
        left = ""
        report = getattr(fence, "drain_report", None)
        if report is not None:
            try:
                left = f". Left: {report(db, pos)}"
            except Exception:  # noqa: BLE001 - the stop is said without it
                left = ""
        raise SystemExit(
            f"{db}: the tail did not reach {pos} within {timeout:g}s"
            + (" - no stream of this hop's reads the source" if got is None
               else f" ({took:.1f}s waited)") + left
            + ". Nothing was stopped; the tail goes on. Give it longer"
              " (a larger wait), or name an earlier position")
    held = tailctl.pause(where, max(5.0, timeout))
    saved = tailctl.saved(tailctl.token_path(hop, db))
    applied = None if saved is None else (
        saved if isinstance(saved, str) else json.dumps(saved, default=str,
                                                        sort_keys=True))
    past = order(kind, applied, pos) if isinstance(applied, str) else None
    verified = None
    seen = ""
    if verify is not None:
        verified, seen = verify(db)
    said = (f"{db}: drained to {pos} in {took:.1f}s; "
            + ("the tail is held" if held else "the tail did not hold")
            + (f" at {applied}" if applied else "")
            + (" (the batch in flight went past the position)"
               if past and past > 0 else "")
            + ("" if verified is None else
               "; the target checked at the seam: "
               + ("same" if verified else f"NOT the same - {seen}")))
    log(said)
    from . import audit
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "drained", "db": db, "position": pos,
                  "applied": applied, "held": bool(held),
                  "verified": verified})
    return {"position": pos, "reached": True, "held": bool(held),
            "applied": applied, "past": past, "verified": verified,
            "said": said}
