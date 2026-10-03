"""PostgreSQL into PostgreSQL with no row passing through migkit (backlog
R20 items 1, 4 and 5).

The table copier read each range as COPY text through the source's client,
tallied every row here (split on newlines, an md5 a row) and wrote the
text into the target's client. The rows crossed this process twice and
were hashed once each on the way. Here a range is read by the source's
own client in COPY's binary encoding, under one snapshot every worker of
the table shares, and written into the target's COPY as the bytes arrive,
64 KB at a time - never a row at a time, never parsed. Each server then
digests the range itself, in SQL, and the range is recorded only once the
two numbers agree and the target has the commit on its disk.

Why the shared snapshot and not a transaction a range: the source's
digest has to read exactly what the copy read. Read under the snapshot
the copy imported, it does, however much the source is written to
meanwhile - so a range that digests different was changed by the target,
never by the source moving on; and the ranges of a table read side by
side are consistent with each other, which a range a transaction was not.
It is also the mark the change tail holds a change to
(`ranges.already_read`): the snapshot's own `pg_current_snapshot()`, taken
in the transaction that exported it, is exactly what every read under it
sees.

Loading sessions set only session settings, never a server's: `COPY ...
FREEZE` where the table is emptied in the same transaction, and
`synchronous_commit = off` where the choice measured it faster. The
second is safe because a range is recorded only once the target reports
its log on disk past the range's commit (`TableCopy.durable`): a target
that crashes loses at most what it had not flushed, which was never
recorded, and the next run copies it again.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .postgres import _ident, _literal

#: every digest session, on both sides alike: a value's text depends on
#: these, and two servers configured apart would render one value two ways
#: - on top of what every session of migkit's is pinned to
#: (`postgres.SESSION`: UTC, ISO dates, the interval style, every digit)
DIGEST_OPTIONS = "-c lc_monetary=C"

#: a loading or snapshot-holding session is never ended by a server's
#: default timeout while it works or waits for the next range (the
#: statement's own is off in every session, `postgres.SESSION`)
LOAD_OPTIONS = "-c idle_in_transaction_session_timeout=0"

#: what a source's refusal of a snapshot a read asked for reads like
_SNAPSHOT_GONE = ("invalid snapshot identifier", "could not import the"
                  " requested snapshot")


#: the work a range's copy is timed as, beside the engine's other ways
WORK = "range copy"


class SnapshotLost(RuntimeError):
    """The snapshot a read asked for had ended before it was imported."""


def connect(eng, side, db, options=""):
    """A psycopg connection to one side of the hop, in autocommit: every
    transaction here is begun and ended where it is written - made the one
    way every PostgreSQL connection of migkit's is (`postgres.connect`:
    the side's TLS, keepalives, UTC and the rest of `SESSION`), `options`
    after that."""
    from .postgres import connect as made
    return made(eng._ep(side), eng._d(side, db), "psycopg", options,
                autocommit=True)


class _Export:
    """One exported snapshot and the transaction keeping it alive."""

    def __init__(self, eng, db, current):
        self.conn = connect(eng, "src", db, LOAD_OPTIONS)
        try:
            self.conn.execute("begin transaction isolation level repeatable"
                              " read read only")
            self.id, snap = self.conn.execute(
                f"select pg_export_snapshot(), {current}()::text").fetchone()
        except Exception:
            self.conn.close()
            raise
        on = eng._marked_on("src", db)
        self.mark = f"{on}|{snap}" if on else None
        self.at = time.monotonic()
        self.users = 0
        self.stale = False

    def alive(self):
        from psycopg.pq import TransactionStatus
        return (not self.conn.closed and self.conn.info.transaction_status
                == TransactionStatus.INTRANS)

    def close(self):
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 - it is going anyway
            pass


class Snapshot:
    """The source as of one instant for every range of one table: exported
    by a transaction held open here and imported by each read. Held no
    longer than the hop's `snapshot_limit` seconds where it sets one - a
    snapshot held open keeps the source's vacuum from what it would free -
    after which the ranges not yet begun read a new one, each range's
    own mark saying which (`ranges.started`). An export in use is kept
    until the last read under it has imported it and been proved."""

    def __init__(self, eng, db, limit=0):
        self.eng, self.db = eng, db
        self.limit = float(limit or 0)
        self._lock = threading.Lock()
        self._held = []
        self._current = None

    def _fn(self):
        if self._current is None:
            v = self.eng._server_version("src", self.db) or 0
            self._current = ("pg_current_snapshot" if v >= 130000
                             else "txid_current_snapshot")
        return self._current

    def lease(self):
        """The export a range reads under, counted as in use until
        `release`."""
        with self._lock:
            now = self._held[-1] if self._held else None
            if now is not None and (
                    now.stale or not now.alive()
                    or (self.limit and time.monotonic() - now.at
                        > self.limit)):
                now.stale = True
                now = None
            if now is None:
                now = _Export(self.eng, self.db, self._fn())
                self._held.append(now)
            now.users += 1
            self._tidy()
            return now

    def release(self, export):
        with self._lock:
            export.users -= 1
            self._tidy()

    def lost(self, export):
        """An export a read could not import: not handed out again."""
        with self._lock:
            export.stale = True
            self._tidy()

    def _tidy(self):
        keep = []
        for i, e in enumerate(self._held):
            current = i == len(self._held) - 1 and not e.stale
            if e.users <= 0 and not current:
                e.close()
            else:
                keep.append(e)
        self._held = keep

    def close(self):
        with self._lock:
            for e in self._held:
                e.close()
            self._held = []


class TableCopy:
    """One table's pass-through: its snapshot, the target connection each
    worker keeps from range to range, and the checks - both servers'
    digests of a range read at the same moment, on connections of their
    own - run beside the next range's load rather than before it."""

    def __init__(self, eng, db, table, qt, cols, options, workers):
        self.eng, self.db, self.table, self.qt = eng, db, table, qt
        self.cols = cols
        self.options = options
        self.snapshot = Snapshot(eng, db,
                                 (eng.hop.options or {}).get("snapshot_limit"))
        self.collist = ", ".join(_ident(c) for c in cols)
        self._local = threading.local()
        self._opened, self._lock = [], threading.Lock()
        n = max(1, int(workers or 1))
        self._checks = ThreadPoolExecutor(n, thread_name_prefix="migkit-check")
        self._sources = ThreadPoolExecutor(n, thread_name_prefix="migkit-src")
        self._pending = []
        self._failed = None

    # --- connections ------------------------------------------------------

    def _conn(self, name, side, options):
        conn = getattr(self._local, name, None)
        if conn is None or conn.closed:
            conn = connect(self.eng, side, self.db, options)
            setattr(self._local, name, conn)
            with self._lock:
                self._opened.append(conn)
        return conn

    def _target(self):
        return self._conn("load", "dst",
                          " ".join(x for x in (LOAD_OPTIONS, self.options)
                                   if x))

    def _digester(self, side):
        return self._conn(f"digest_{side}", side, DIGEST_OPTIONS)

    # --- one range ------------------------------------------------------------

    def copy(self, export, where, pre_sql, what, freeze=False,
             unawaited=False):
        """The rows `where` selects under `export`, in place of what
        `pre_sql` clears on the target, in one transaction there. Returns
        (rows, the target's log position past the commit, or None where the
        commit waited for its disk)."""
        from ..stall import Guard
        eng = self.eng
        select = eng._copy_select(self.qt, self.cols, where)
        proc, stream, relayed = eng._source_copy(self.db, [
            "begin transaction isolation level repeatable read read only",
            f"set transaction snapshot {_literal(export.id)}",
            f"\\copy ({select}) to stdout with (format binary)",
            "commit"])
        conn = self._target()

        def end():
            proc.kill()
            conn.cancel_safe()
        guard = Guard(what, end)
        rows, said = 0, ""
        try:
            with conn.transaction():
                if unawaited:
                    conn.execute("set local synchronous_commit = off")
                if pre_sql:
                    conn.execute(pre_sql)
                with conn.cursor() as cur:
                    with cur.copy(f"copy {self.qt} ({self.collist}) from"
                                  " stdin (format binary"
                                  + (", freeze" if freeze else "") + ")") \
                            as cp:
                        while True:
                            chunk = stream.read(1 << 16)
                            guard.moved()
                            if not chunk:
                                break
                            cp.write(chunk)
                            guard.moved()
                    rows = cur.rowcount
                # the read's own end, before anything commits: a read
                # that stopped short sent a stream the target may have
                # taken as whole
                said = self._ended(proc, relayed)
                if said:
                    raise RuntimeError(said)
        except Exception as e:
            if not said:
                if proc.poll() is None:
                    # the load failed first; the read has nowhere to go
                    proc.kill()
                    proc.wait()
                else:
                    # the read failed first, and the load on what it sent
                    said = self._ended(proc, relayed)
            if any(s in said for s in _SNAPSHOT_GONE):
                raise SnapshotLost(said) from e
            raise RuntimeError(f"{what}: {said or e}"[:600]) from e
        finally:
            guard.stop()
        lsn = None
        if unawaited:
            lsn = conn.execute("select pg_current_wal_insert_lsn()::text"
                               ).fetchone()[0]
        return rows, lsn

    @staticmethod
    def _ended(proc, relayed):
        """What went wrong with the source's read, or "" where it ended
        well."""
        try:
            proc.wait(timeout=60)
        except Exception:  # noqa: BLE001 - killed below
            proc.kill()
            proc.wait()
        err = proc.stderr.read() if proc.stderr else b""
        text = err.decode(errors="replace")
        if relayed and "migkit-read:0" not in text:
            return ("the read beside the source did not finish: "
                    + text[-300:])
        if not relayed and proc.returncode:
            return text[-300:] or f"the read ended with {proc.returncode}"
        return ""

    def digest(self, side, export, where):
        """(rows, fold) of the range on one side - the source's read under
        the range's own snapshot, the target's as it stands."""
        sql = self.eng._range_digest_sql(self.db, self.table, where)
        conn = self._digester(side)
        # a table of wide rows digested by fewer parallel workers, each of
        # which holds the row it hashes (`_digest_workers`); the server's
        # own default of two at most otherwise
        workers = self.eng._digest_workers(self.db, self.table, 2)
        if side == "dst":
            conn.execute(f"set max_parallel_workers_per_gather = {workers}")
            return conn.execute(sql).fetchone()[0]
        conn.execute("begin transaction isolation level repeatable read"
                     " read only")
        try:
            try:
                conn.execute(f"set transaction snapshot {_literal(export.id)}")
            except Exception as e:
                if any(s in str(e) for s in _SNAPSHOT_GONE):
                    raise SnapshotLost(str(e)) from e
                raise
            conn.execute(f"set local max_parallel_workers_per_gather ="
                         f" {workers}")
            return conn.execute(sql).fetchone()[0]
        finally:
            conn.execute("rollback")

    def durable(self, lsn):
        """Returns once the target's log is on its disk past `lsn`: the
        server's own writer flushes an unawaited commit within three of its
        `wal_writer_delay`s, which the target is asked for. Waited for here,
        beside the next range's load, not before it. A transaction of this
        session's own committed awaiting the disk would not hurry it: the
        server waits for the disk only where a transaction wrote something
        before its commit, and one that only took an xid wrote nothing."""
        conn = self._digester("dst")
        ask = ("select pg_current_wal_flush_lsn() >= %s::pg_lsn,"
               " (select setting::int from pg_settings"
               "  where name = 'wal_writer_delay')")
        flushed, delay = conn.execute(ask, (lsn,)).fetchone()
        end = time.monotonic() + 3 * int(delay or 200) / 1000 + 5
        while not flushed:
            if time.monotonic() > end:
                raise RuntimeError(f"the target's log did not reach {lsn} on"
                                   " its disk in three of its writer's"
                                   " rounds")
            time.sleep(0.05)
            flushed = conn.execute(ask, (lsn,)).fetchone()[0]

    def same(self, export, where):
        """Whether both servers digest the range alike, read at the same
        moment on connections of their own."""
        theirs = self._sources.submit(self.digest, "src", export, where)
        mine = self.digest("dst", export, where)
        return theirs.result() == mine

    def check(self, run):
        """`run()` beside the next range's load; a failure stops the table
        at the next range (`raise_failed`) and is raised by `wait`."""
        def guarded():
            try:
                return run()
            except BaseException as e:  # noqa: BLE001 - raised by wait
                with self._lock:
                    if self._failed is None:
                        self._failed = e
                raise
        with self._lock:
            self._pending.append(self._checks.submit(guarded))

    def raise_failed(self):
        if self._failed is not None:
            raise self._failed

    def wait(self):
        """Every check handed over finished; the first failure raised."""
        while True:
            with self._lock:
                todo = [f for f in self._pending if not f.done()]
            if not todo:
                break
            for f in todo:
                try:
                    f.result()
                except BaseException:  # noqa: BLE001 - kept in _failed
                    pass
        self.raise_failed()

    def close(self):
        for pool in (self._checks, self._sources):
            pool.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            opened, self._opened = self._opened, []
        for conn in opened:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass
        self.snapshot.close()


class Way:
    """How one unit of a table goes, as the table's trials chose it: the
    rung, and for the pass-through the snapshot export its read is under.
    `begin(mark)` is the caller's, called with the mark the read is held
    to before the read starts (`ranges.started`)."""

    def __init__(self, ways, choice, begin, export=None):
        self.ways, self.choice, self.begin = ways, choice, begin
        self.name = choice.path
        self.export = export

    @property
    def passes(self):
        return self.export is not None

    @property
    def ctx(self):
        return self.ways.ctx

    def took(self, units, seconds):
        self.ways.trials.took(self.name, units, seconds)

    def instead(self, why):
        """The pass-through gone wrong on this unit: given up on for the
        rest of the table, and the way to copy the unit again."""
        self.ways.give_up_passing(why)
        return self.ways.pick(self.begin)


class Ways:
    """The ways one table's units are copied, each unit climbing the
    rungs on this table's own measurements (`decide.Trials`), and what
    the pass-through keeps for the table while it is copied."""

    def __init__(self, eng, db, table, rungs, facts, ctx, log):
        self.eng, self.db, self.table, self.ctx, self.log = (
            eng, db, table, ctx, log)
        # the engine's own chooser, on the costs it keeps for every way
        # it times (`_trials`, `measured_ways`)
        self.trials = eng._trials(eng._work_key(WORK, table), rungs, facts)
        self.passing = {r.name for r in rungs if r.name != "pipe"}
        self._said = set()
        self._lock = threading.Lock()

    def pick(self, begin):
        """The `Way` for the next unit, its mark given to `begin`."""
        while True:
            got = self.trials.pick()
            if got.rung is None:
                raise RuntimeError(f"{self.table}: no way to copy it here:"
                                   f" {got.reason}")
            self._say(got)
            if got.path not in self.passing:
                begin(self.eng.snapshot_mark("src", self.db))
                return Way(self, got, begin)
            try:
                export = self.ctx.snapshot.lease()
            except Exception as e:  # noqa: BLE001 - said, and a rung down
                self.give_up_passing(
                    "the source would not export a snapshot for the ranges"
                    f" to share: {str(e).strip().splitlines()[-1][:120]}")
                continue
            begin(export.mark)
            return Way(self, got, begin, export)

    def _say(self, got):
        """The first way a table takes, with why; and once every way that
        holds has been timed on the table's own ranges, what each cost -
        in the run's decision record, the console only where asked
        (`decide.record`)."""
        from .. import decide
        with self._lock:
            first = not self._said
            self._said.add(got.path)
            timed = {r.name: self.trials.costs.get(r.name)
                     for r in got.ranked}
            settled = (len(timed) > 1 and "settled" not in self._said
                       and None not in timed.values())
            if settled:
                self._said.add("settled")
        if first:
            decide.record(self.eng.hop, got, say=self.log)
        if settled:
            costs = sorted(timed.items(), key=lambda kv: kv[1])
            decide.record(
                self.eng.hop, {"costs": dict(costs)}, say=self.log,
                work=self.table, way=got.said,
                reason="each way timed on its own ranges - "
                       + ", ".join(f"{n} {c * 1e6:,.1f} us a row"
                                   for n, c in costs)
                       + "; each range takes the cheapest from here on")

    def give_up_passing(self, why):
        for name in self.passing:
            self.trials.give_up(name, why)

    def wait(self):
        self.ctx.wait()

    def close(self):
        self.ctx.close()
