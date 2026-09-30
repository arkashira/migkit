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
"""
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
