"""A running tail's tables, each in a state of its own (backlog L1, L4,
L5, L6; research: leapfrog live tail 2026-09-28, section 1.3).

One stream, one saved position, and a state per table beside it, in
the tail's own store (`Store`), written in one transaction a batch with
everything else the batch leaves:

    live      its changes applied as they are read
    copying   added to the tail: copied under its own marks beside the
              stream, its changes held until the range they fall in has
              landed, then held to that range's mark - left out where the
              copy read them, applied where it did not
    parked    a DDL the target does not follow yet: its changes set aside
              on this machine's disk, so the source's log is not held for
              it, and applied once the target is level - or the table
              copied again
    removed   out of the hop's scope: its changes left out and counted,
              where it left and what its target held then kept for a
              table added back

Nothing else stops. DMS, Qlik, TiDB DM and Debezium stop the task to
change its tables; Vitess cannot add one to a running workflow; DTS will
not change a task more than ten minutes behind. Here a table added to the
hop's scope - an `exclude` taken out of the hop file while the tail runs,
a table the tail left coming back, or one made on the source where the
hop's `ddl` says so - is copied while every other table goes on, and
joins at an exact seam: each range of its copy keeps the source's mark
taken before it was read (`ranges.started`), and a change is left out
only where that mark shows it committed (`Engine.mark_covers`) -
visibility, not a log position, which PostgreSQL does not keep in commit
order. A table that left and comes back with its target untouched, its
shape the same and the source's log still holding where it left, gets
only its changes since then, read again from there (`_add_ways`).

A change held for a range or set aside for a parked table is written to
that table's spool (`Spool`) in the batch's transaction, before the target
commits the batch and the position moves past it, so a stop loses none;
a batch read again is not spooled twice. A batch works on a copy of the
states (`begin`), sets them down with the rest of the batch (`prepare`),
and only once its position is saved are they the tail's (`committed`) -
a batch read again after a lost connection, or after a stop, finds the
states it was first read under (`Store.align`). Everything the stream
reads is accounted per table every batch - applied, left out and why, or
set aside - by the tail (`hetero._Accounts`).

What a DDL does is the hop's `ddl`:

    stop    (the default, as before) a DDL the target does not follow
            stops the whole tail before the batch; a table emptied or
            renamed on the source stops it too - the one was carried by
            nothing before, the other left the target's table holding the
            old rows under the new table's changes
    park    the table the DDL touched is parked, and only it; a table
            emptied on the source is copied again
    apply   what can be carried at its place in the log is: a column
            added that takes NULL or has a constant default is added on
            the target, a table emptied is emptied there at its place
            among the rows; the rest parks

Under park and apply a table dropped or renamed on the source leaves the
tail, and one a rename or a create brings in joins it with its copy;
migkit never drops a target table. `ddl` may say it per table, and leave
some kinds out (`policy`, `skipped`).

Each table is known by its whole name as the source names it (`key`):
by its last part alone, two schemas' `orders` shared one state and one
target table.
"""
import json
import os
import pickle
import re
import struct
import threading
import time
from collections import Counter

#: where an earlier version kept the tables' states, and their spools
STATE = "tail-tables.json"
SPOOLS = "tail-spool"
#: the checkpoint the tail's own copies keep, in the table copier's shape
COPY = "tail-copy.json"
POLICIES = ("stop", "park", "apply")
#: what a parked table's spool may hold, in bytes, before the table is
#: copied again once its target is level: its changes are then read from
#: the source as it is, not from this machine's disk
SPOOL_MOST = 1 << 30

#: why a change read is not applied in its batch, in the words the
#: accounts keep (`hetero._Accounts`)
HELD = "held until its range of the copy lands"
SPOOLED = "set aside while its table is parked"
OUT_OF_SCOPE = "not in the hop's scope"
COPIED = "already in what the copy read"
#: a change of a kind the hop's `mapping.ops` does not carry for its table
NOT_CARRIED = "not carried by the hop"
WHY = (HELD, SPOOLED, OUT_OF_SCOPE)

#: the DDL kinds `ddl: {skip: [...]}` may leave out: each leaves the
#: target's table as it is and the tail going on. An ALTER or a rename
#: left out would have every change after it written to columns or a
#: table the target does not have
SKIPPABLE = ("truncate", "drop", "create")
#: the keys of a `ddl` mapping that name no table
_DDL_WORDS = ("default", "skip")


def _word(got, where="ddl"):
    got = str(got or "stop").strip().lower()
    if got not in POLICIES:
        raise SystemExit(f"{where}: {got} is not one of {', '.join(POLICIES)}"
                         " - stop the tail, park the table the DDL"
                         " touched, or apply what can be carried")
    return got


def policy(hop, table=None):
    """What a DDL does to `table`'s tail - stop, park or apply: the hop's
    `ddl` as one word for every table, or a mapping of them -

        ddl:
          default: park             # every table no key names
          "billing.*": apply        # names or patterns, matched as
          logistics.orders: stop    #   mapping.ops is: the most
                                    #   particular key wins
          skip: [drop, truncate]    # kinds left out of every table

    as Tencent's and Alibaba's DTS let a job say which DDL it carries.
    `table` is its name's parts from the database down, or a dotted
    name; None asks for the default."""
    got = (hop.options or {}).get("ddl")
    if not isinstance(got, dict):
        return _word(got)
    rules = {k: v for k, v in got.items() if k not in _DDL_WORDS}
    if table is not None and rules:
        parts = table.split(".") if isinstance(table, str) else table
        best = hop.most_particular(rules, *parts)
        if best is not None:
            return _word(rules[best], f"ddl.{best}")
    return _word(got.get("default"), "ddl.default")


def skipped(hop):
    """The DDL kinds the hop's `ddl` leaves out (`skip`): each is left out
    of the tail and counted, and the target's table left as it is - a
    `reload` (a partition's rows gone with no row event for them) with a
    truncate."""
    got = (hop.options or {}).get("ddl")
    if not isinstance(got, dict) or not got.get("skip"):
        return frozenset()
    words = got["skip"]
    words = [words] if isinstance(words, str) else list(words)
    out = set()
    for w in words:
        w = str(w).strip().lower()
        if w not in SKIPPABLE:
            raise SystemExit(f"ddl.skip: {w} is not one of"
                             f" {', '.join(SKIPPABLE)} - an ALTER or a"
                             " rename left out would have every change"
                             " after it written where the target has no"
                             " column or table for it")
        out.add(w)
    if "truncate" in out:
        out.add("reload")
    return frozenset(out)


def spool_most(hop):
    try:
        return int((hop.options or {}).get("spool_limit") or SPOOL_MOST)
    except (TypeError, ValueError):
        raise SystemExit("spool_limit: the bytes a parked table's changes"
                         " may take on this machine, a whole number")


def _token_text(token):
    return json.dumps(token, sort_keys=True, default=str)


def _attr(obj, name):
    """The one attribute a value of a change log is pickled through
    (`ZoneInfo._unpickle`): `getattr` itself would reach anything from any
    class allowed."""
    from zoneinfo import ZoneInfo
    if obj is ZoneInfo and name == "_unpickle":
        return ZoneInfo._unpickle
    raise pickle.UnpicklingError(f"a change set aside asks for {name}, which"
                                 " is not how a value is made")


class _Values(pickle.Unpickler):
    """Changes set aside read back, their values their own types again -
    and nothing else: only the classes a change log's values come as, by
    name, are made, so a spool somebody wrote into cannot make this
    process run anything (audit SEC21). A whole module is never allowed:
    a module's functions can run a command."""

    #: {module: names} a value may be made of
    ALLOWED = {
        "builtins": {"set", "frozenset", "bytearray", "complex", "range",
                     "slice", "object", "tuple", "list", "dict", "int",
                     "float", "str", "bytes", "bool"},
        "copyreg": {"_reconstructor", "__newobj__", "__newobj_ex__"},
        "collections": {"OrderedDict"},
        "datetime": {"datetime", "date", "time", "timedelta", "timezone",
                     "tzinfo"},
        "decimal": {"Decimal"}, "fractions": {"Fraction"},
        "uuid": {"UUID", "SafeUUID"},
        "ipaddress": {"IPv4Address", "IPv6Address", "IPv4Network",
                      "IPv6Network", "IPv4Interface", "IPv6Interface"},
        "zoneinfo": {"ZoneInfo"}, "zoneinfo._zoneinfo": {"ZoneInfo"},
        "array": {"array", "_array_reconstructor"},
        "migkit.canon": {"Added", "Kept", "_Absent"},
        # NumPy's scalars from a columnar file, as NumPy pickles them
        "numpy": {"dtype", "ndarray"},
        "numpy.core.multiarray": {"_reconstruct", "scalar"},
        "numpy._core.multiarray": {"_reconstruct", "scalar"},
    }
    #: modules whose classes are values a reader hands over - BSON's, the
    #: PostgreSQL drivers' ranges - each a class, never a function
    CLASSES = ("bson", "psycopg2._range", "psycopg2.extras",
               "psycopg.types.range")

    def find_class(self, module, name):
        if name in self.ALLOWED.get(module, ()):
            return super().find_class(module, name)
        if (module, name) == ("builtins", "getattr"):
            return _attr
        if module == "bson" or module.startswith("bson.") or \
                module in self.CLASSES:
            got = super().find_class(module, name)
            if isinstance(got, type) and (module.startswith("bson")
                                          or name.endswith("Range")):
                return got
        raise pickle.UnpicklingError(
            f"a change set aside holds {module}.{name}, which is not a"
            " kind of value a change log hands over")


def _pack(changes, recipients):
    """A frame's body: its changes pickled - their values keep their own
    types, which JSON would not - zstd over them, sealed to the hop's
    `at_rest` recipients where it names them (`evidence`).

    Read back as `_unpack` reads it before it is kept: a value of a kind
    `_Values` does not make was set aside, the position moved past it,
    and the frame then refused when it was released - the change lost.
    Refused here instead, inside the batch's transaction, so nothing of
    the batch is kept and its position stays."""
    import io

    import zstandard
    raw = pickle.dumps(list(changes), protocol=pickle.HIGHEST_PROTOCOL)
    try:
        _Values(io.BytesIO(raw)).load()
    except pickle.UnpicklingError as e:
        raise SystemExit(
            f"{e}, so it could not be read back once set aside. Nothing of"
            " this batch was kept and its position stays where it was;"
            " the table's changes need a reader that hands its values"
            " over as plain kinds") from None
    data = zstandard.ZstdCompressor(level=3).compress(raw)
    if recipients:
        from . import evidence
        data = evidence._seal(data, recipients)
    return data


def _unpack(data):
    import io

    import zstandard

    from . import evidence
    data = bytes(data)
    if data.startswith(evidence.AGE):
        data = evidence._open(data)
    return _Values(io.BytesIO(
        zstandard.ZstdDecompressor().decompress(data))).load()


#: the tail's own state, beside its position
STORE = "tail-state.sqlite"
#: its layout (`pragma user_version`), and what each layout adds
STORE_VERSION = 1
_LAYOUT = {1: (
    "create table if not exists meta (k text primary key, v text)",
    "create table if not exists tables (leaf text primary key, v text)",
    "create table if not exists accounts (tbl text primary key, v text)",
    "create table if not exists frames (id integer primary key"
    " autoincrement, leaf text not null, at text, n integer, body blob)",
    "create index if not exists frames_leaf on frames (leaf, id)",
    "create table if not exists undo (seq integer primary key"
    " autoincrement, tbl text, k text, v text, body blob)")}


class Store:
    """The tail's own state beside its position, in one SQLite file
    (audit 2026-10-01, ST1, ST18, ST19): each table's state, what the tail
    read of each table and did with it (`hetero._Accounts`), the changes
    it set aside, one row each batch they were read in (`frames`), and the
    source's shape it last applied under.

    It was five files - the position, the tables' states, the accounts,
    a spool a table, the shape - each rewritten in place, none synced but
    the spools, with no barrier between them: after a power loss a
    newer position beside older states turned a table parked in the batch
    after it live again, and what was set aside for it was never applied.
    Measured in-process per batch at 2,000 tables (Apple SSD): the states
    and the accounts rewritten whole as they were, 6.02 ms and 350 KB a
    batch, none of it synced; the batch's transaction here - a table's
    state, five tables' accounts, the generation - 0.118 ms, synced; the
    position's envelope written whole and synced 0.454 ms, against 0.129
    ms rewritten in place.

    **One transaction a batch, before the target commits it** (`batch`):
    the batch's states, the frames it sets aside and the ones it releases,
    its accounts and a generation one past the position's, with what each
    replaced kept (`undo`). The position is written once the target has
    committed (`tailctl.moved`), and its generation is then the store's.
    A store one generation ahead of the position holds a batch the
    position never passed: where the target's mark says it committed that
    batch the tail goes on after it and the store stands
    (`hetero._committed_ahead`); anywhere else the batch is taken back out
    (`align`) and read again. A write between batches - a table added to
    the scope, a copy that finished - is its own transaction at the
    generation there is. Only the rows that changed are written: a table
    whose state is as stored is not rewritten, an account only where the
    batch read its table, a frame only where it is set aside or released
    (and a frame half released is rewritten, the rest of the spool not)."""

    _OPEN = {}
    _OPEN_LOCK = threading.Lock()

    @classmethod
    def of(cls, where):
        """The store of the tail whose report directory is `where`, one
        per process per file."""
        path = os.path.join(str(where), STORE)
        with cls._OPEN_LOCK:
            got = cls._OPEN.get(path)
            if got is None or not os.path.exists(path) \
                    or got.ino != os.stat(path).st_ino:
                got = cls._OPEN[path] = cls(path)
            return got

    @classmethod
    def forget(cls, where):
        """Close this process's store of `where`, for a file put in its
        place (`ha`, `state.Mirror`) to be opened afresh."""
        path = os.path.join(str(where), STORE)
        with cls._OPEN_LOCK:
            got = cls._OPEN.pop(path, None)
        if got is not None:
            got.conn.close()

    def __init__(self, path):
        import sqlite3

        from . import durable
        self.path = str(path)
        durable.private_dir(os.path.dirname(self.path) or ".")
        if not os.path.exists(self.path):
            try:
                os.close(os.open(self.path,
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
            except FileExistsError:
                pass
        self.ino = os.stat(self.path).st_ino
        self.lock = threading.RLock()
        self._undoing = None
        try:
            self.conn = sqlite3.connect(self.path, check_same_thread=False,
                                        isolation_level=None)
            c = self.conn
            c.execute("pragma journal_mode=wal")
            c.execute("pragma synchronous=full")
            have = c.execute("pragma user_version").fetchone()[0]
        except sqlite3.DatabaseError as e:
            raise SystemExit(
                f"{self.path} cannot be read ({e}). It holds the change"
                " tail's tables' states and what it set aside, and going on"
                " without it would be a guess, so migkit stops here;"
                " removing it and the tail's position beside it starts the"
                " tail over with --mode full+cdc") from None
        if have > STORE_VERSION:
            raise SystemExit(
                f"{self.path} was written by a newer migkit (layout {have});"
                f" this one reads layout {STORE_VERSION}. Run the newer"
                " migkit")
        for v in range(have + 1, STORE_VERSION + 1):
            for sql in _LAYOUT[v]:
                c.execute(sql)
            c.execute(f"pragma user_version = {v}")

    # --- reading --------------------------------------------------------------

    def get(self, k, default=None):
        with self.lock:
            row = self.conn.execute("select v from meta where k = ?",
                                    (k,)).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else default

    def tables(self):
        with self.lock:
            return {k: json.loads(v) for k, v in
                    self.conn.execute("select leaf, v from tables")}

    def accounts(self):
        with self.lock:
            return {k: json.loads(v) for k, v in
                    self.conn.execute("select tbl, v from accounts")}

    def frames(self, leaf):
        """[(id, where the batch was read from, how many, body)], oldest
        first."""
        with self.lock:
            return list(self.conn.execute(
                "select id, at, n, body from frames where leaf = ? order"
                " by id", (leaf,)))

    def spooled(self, leaf=None):
        """Bytes set aside, for `leaf`, or {leaf: bytes} for every one."""
        with self.lock:
            if leaf is not None:
                return int(self.conn.execute(
                    "select coalesce(sum(length(body)), 0) from frames"
                    " where leaf = ?", (leaf,)).fetchone()[0])
            return {k: int(n) for k, n in self.conn.execute(
                "select leaf, sum(length(body)) from frames group by"
                " leaf")}

    def gen(self):
        return int(self.get("gen") or 0)

    # --- writing ---------------------------------------------------------------

    _KEYS = {"meta": "k", "tables": "leaf", "accounts": "tbl"}

    def _remember(self, tbl, k):
        """What `tbl`'s row `k` held before the batch first touched it."""
        if self._undoing is None or (tbl, str(k)) in self._undoing:
            return
        self._undoing.add((tbl, str(k)))
        if tbl == "frames":
            row = self.conn.execute(
                "select leaf, at, n, body from frames where id = ?",
                (int(k),)).fetchone()
            self.conn.execute(
                "insert into undo (tbl, k, v, body) values (?, ?, ?, ?)",
                (tbl, str(k), None if row is None else json.dumps(
                    [row[0], row[1], row[2]]),
                 None if row is None else row[3]))
            return
        row = self.conn.execute(
            f"select v from {tbl} where {self._KEYS[tbl]} = ?",
            (k,)).fetchone()
        self.conn.execute("insert into undo (tbl, k, v) values (?, ?, ?)",
                          (tbl, str(k), None if row is None else row[0]))

    def put(self, tbl, k, value):
        self._remember(tbl, k)
        self.conn.execute(
            f"insert into {tbl} values (?, ?) on conflict"
            f" ({self._KEYS[tbl]}) do update set v = excluded.v",
            (k, json.dumps(value, default=str)))

    def drop(self, tbl, k):
        self._remember(tbl, k)
        self.conn.execute(f"delete from {tbl} where {self._KEYS[tbl]} = ?",
                          (k,))

    def add_frame(self, leaf, at, changes, recipients):
        got = self.conn.execute(
            "insert into frames (leaf, at, n, body) values (?, ?, ?, ?)",
            (leaf, at, len(changes), _pack(changes, recipients))).lastrowid
        if self._undoing is not None:
            # a frame the batch made: taken back out by its id
            self._undoing.add(("frames", str(got)))
            self.conn.execute("insert into undo (tbl, k, v) values"
                              " ('frames', ?, null)", (str(got),))
        return got

    def set_frame(self, fid, changes, recipients):
        """Frame `fid` holding `changes` now - gone where none are left."""
        self._remember("frames", fid)
        if not changes:
            self.conn.execute("delete from frames where id = ?", (int(fid),))
            return
        self.conn.execute("update frames set n = ?, body = ? where id = ?",
                          (len(changes), _pack(changes, recipients),
                           int(fid)))

    def move_frames(self, old, new):
        """What was set aside under one key, under another (`TableSet.
        _keyed`)."""
        self.conn.execute("update frames set leaf = ? where leaf = ?",
                          (new, old))

    def drop_frames(self, leaf):
        for (fid,) in list(self.conn.execute(
                "select id from frames where leaf = ?", (leaf,))):
            self._remember("frames", fid)
        self.conn.execute("delete from frames where leaf = ?", (leaf,))

    def batch(self, gen=None):
        """A transaction, `with store.batch(gen) as s:` - where `gen` is a
        batch's generation, what the batch replaces is kept to be taken
        back out (`undo`) and the generation written with it; None writes
        between batches, at the generation there is."""
        return _Transaction(self, gen)

    def undo(self):
        """The last batch taken back out: every row it wrote as it was
        before, its generation with them. True where there was one."""
        with self.lock:
            rows = list(self.conn.execute(
                "select tbl, k, v, body from undo order by seq desc"))
            if not rows:
                return False
            self.conn.execute("begin immediate")
            try:
                for tbl, k, v, body in rows:
                    if tbl == "frames":
                        self.conn.execute("delete from frames where id = ?",
                                          (int(k),))
                        if v is not None:
                            leaf, at, n = json.loads(v)
                            self.conn.execute(
                                "insert into frames (id, leaf, at, n, body)"
                                " values (?, ?, ?, ?, ?)",
                                (int(k), leaf, at, n, body))
                    elif v is None:
                        self.conn.execute(
                            f"delete from {tbl} where {self._KEYS[tbl]}"
                            " = ?", (k,))
                    else:
                        self.conn.execute(
                            f"insert into {tbl} values (?, ?) on conflict"
                            f" ({self._KEYS[tbl]}) do update set v ="
                            " excluded.v", (k, v))
                self.conn.execute("delete from undo")
                self.conn.execute("commit")
            except BaseException:
                self.conn.execute("rollback")
                raise
            return True

    def settled(self):
        """The last batch made the tail's: its position is saved, and it is
        never taken back out."""
        with self.lock:
            self.conn.execute("delete from undo")

    def reset(self, stream, gen=0):
        """Everything of an earlier stream gone: a position taken afresh
        begins with nothing set aside and no table's state."""
        with self.batch() as s:
            for tbl in ("meta", "tables", "accounts", "frames", "undo"):
                s.conn.execute(f"delete from {tbl}")
            s.put("meta", "stream", stream)
            s.put("meta", "gen", int(gen))
        # an earlier version's files of an earlier stream go with it
        import shutil
        where = os.path.dirname(self.path)
        for name in self.LEGACY:
            try:
                os.unlink(os.path.join(where, name))
            except FileNotFoundError:
                pass
        shutil.rmtree(os.path.join(where, SPOOLS), ignore_errors=True)

    def align(self, env, log, safe_behind=None):
        """The store held to the tail's position `env` before anything is
        read (the class's words): in step; a batch the position never
        passed taken back out; or - a store of another stream - started
        afresh. A store behind the position (it came from elsewhere,
        older than the position put beside it) stands only where
        `safe_behind()` says nothing it would have held can be missing;
        else it stops, naming both files."""
        stream = env.get("id") or "v0"
        have = self.get("stream")
        gen = int(env.get("gen") or 0)
        if have is None:
            with self.batch() as s:
                s.put("meta", "stream", stream)
                s.put("meta", "gen", gen)
            return "new"
        if have != stream:
            log("the tail's own state belonged to an earlier position; it"
                " starts afresh with this one")
            self.reset(stream, gen)
            return "afresh"
        mine = self.gen()
        if mine == gen:
            return "in step"
        if mine == gen + 1 and self.undo():
            log("the tail had set down its last batch before the target"
                " committed it or before its position was saved; taken back"
                " out, the batch is read again")
            return "undone"
        if mine < gen and safe_behind is not None and safe_behind():
            log(f"the tail's own state was {gen - mine} batches behind its"
                " position, with every table live and nothing set aside;"
                " it goes on from the position")
            with self.batch() as s:
                s.put("meta", "gen", gen)
            return "behind"
        raise SystemExit(
            f"{self.path} stands at batch {mine} and the tail's position"
            f" beside it at {gen}: the two were not saved together, and"
            " which tables were set aside, and what for, cannot be known."
            " Nothing was applied. Put back the two files as they were"
            " saved together, or remove both and move again with --mode"
            " full+cdc")

    # --- what an earlier version kept in files of its own ---------------------

    LEGACY = ("tail-tables.json", "tail-accounts.json", "tail-shape.json")

    def import_legacy(self, where, recipients=()):
        """The tables' states, the accounts, the shape and a spool a table
        an earlier version kept in files of their own, taken into the
        store once and the files removed. A file that cannot be read whole
        stops, naming it (`durable`)."""
        from pathlib import Path

        from . import durable
        where = Path(where)
        found = [n for n in self.LEGACY if (where / n).exists()]
        spools = sorted((where / SPOOLS).glob("*.spool")) \
            if (where / SPOOLS).is_dir() else []
        if not found and not spools:
            return False
        tables = durable.read_state(where / self.LEGACY[0], "state") or {}
        accounts = durable.read_state(where / self.LEGACY[1], "state") or {}
        shape = durable.read_state(where / self.LEGACY[2], "cache")
        states = tables.get("tables") or {}
        with self.batch() as s:
            for leaf, st in states.items():
                s.put("tables", leaf, st)
            if tables.get("scope") is not None:
                s.put("meta", "scope", tables["scope"])
            for t, row in accounts.items():
                s.put("accounts", t, row)
            if shape is not None:
                s.put("meta", "shape", shape)
            for path in spools:
                leaf = next((k for k in states
                             if _spool_name(k) == path.name), path.stem)
                for at, changes in _legacy_frames(path):
                    s.add_frame(leaf, at, changes, recipients)
        for n in found:
            (where / n).unlink(missing_ok=True)
        for path in spools:
            path.unlink(missing_ok=True)
        return True

    # --- as text, for a store of the machines' that keeps no files ------------

    def dump(self):
        """The store as JSON text - where the machines of a hop keep their
        records in the target (`shared.Target`), which holds text only.
        None where it holds changes set aside: those it cannot carry."""
        with self.lock:
            if self.conn.execute("select count(*) from frames").fetchone()[0]:
                return None
            return json.dumps({"meta": dict(self.conn.execute(
                                   "select k, v from meta")),
                               "tables": dict(self.conn.execute(
                                   "select leaf, v from tables")),
                               "accounts": dict(self.conn.execute(
                                   "select tbl, v from accounts"))})

    @classmethod
    def load(cls, where, text):
        """A store put in place from its `dump`."""
        got = json.loads(text)
        path = os.path.join(str(where), STORE)
        cls.forget(where)
        for extra in ("", "-wal", "-shm", "-journal"):
            try:
                os.unlink(path + extra)
            except FileNotFoundError:
                pass
        st = cls.of(where)
        with st.batch():
            for tbl, rows in (("meta", got.get("meta")),
                              ("tables", got.get("tables")),
                              ("accounts", got.get("accounts"))):
                for k, v in (rows or {}).items():
                    st.conn.execute(f"insert into {tbl} values (?, ?)",
                                    (k, v))
        return st


class _Transaction:
    def __init__(self, store, gen):
        self.store, self.gen = store, gen

    def __enter__(self):
        st = self.store
        st.lock.acquire()
        try:
            st.conn.execute("begin immediate")
            if self.gen is not None:
                st.conn.execute("delete from undo")
                st._undoing = set()
        except BaseException:
            st.lock.release()
            raise
        return st

    def __exit__(self, kind, exc, tb):
        st = self.store
        try:
            if kind is None:
                if self.gen is not None:
                    st.put("meta", "gen", int(self.gen))
                st.conn.execute("commit")
            else:
                st.conn.execute("rollback")
        finally:
            st._undoing = None
            st.lock.release()
        return False


def _spool_name(leaf):
    """The file an earlier version kept a table's set-aside changes in."""
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_"
                   for ch in leaf)
    return f"{safe}.spool"


def _legacy_frames(path):
    """[(position text, [changes])] of an earlier version's spool file, a
    torn last frame left out."""
    import io

    import zstandard

    from . import evidence
    raw = path.read_bytes()
    out, at = [], 0
    while at + 8 <= len(raw):
        n = struct.unpack(">Q", raw[at:at + 8])[0]
        if at + 8 + n > len(raw):
            break
        data = raw[at + 8:at + 8 + n]
        try:
            if data.startswith(evidence.AGE):
                data = evidence._open(data)
            got = _Values(io.BytesIO(
                zstandard.ZstdDecompressor().decompress(data))).load()
        except pickle.UnpicklingError:
            raise
        except Exception:  # noqa: BLE001 - torn, the last one
            break
        out.append((got[0], list(got[1])))
        at += 8 + n
    return out


class Spool:
    """The changes set aside for one table, in the order read: a frame each
    batch that set any aside, `(where the batch was read from, [changes])`
    (`Store`). Written in the batch's transaction, before the target
    commits the batch and the position moves past them, so a stop loses
    none; a batch taken back out takes its frames with it."""

    def __init__(self, store, leaf, recipients=()):
        self.store, self.leaf = store, leaf
        self.recipients = list(recipients or ())

    def frames(self):
        """[(frame id, position text, [changes])]."""
        return [(fid, at, _unpack(body))
                for fid, at, _, body in self.store.frames(self.leaf)]

    def changes(self):
        return [c for _, _, got in self.frames() for c in got]

    def size(self):
        return self.store.spooled(self.leaf)

    def remove(self):
        """Every frame gone, now - between batches."""
        with self.store.batch() as s:
            s.drop_frames(self.leaf)


class _HopFile:
    """The hop's `exclude`, `include` and `mapping` as its file says them
    now, read again only when the file has changed - so a table taken out
    of the scope, or put in it, joins or leaves the running tail at the
    next batch, and one whose row filter, columns, name or changes carried
    the mapping changes is taken again (`TableSet._remapped`). A hop the
    file does not name (made in code) is left as it is; what the
    environment or a command added to either list stays."""

    def __init__(self, hop):
        from .config import CONF
        self.hop, self.path = hop, CONF
        self.stamp = self._stamp()
        self.said = self._read()
        #: the mapping the hop had before the file changed it, until taken
        self.remapped = None

    def _stamp(self):
        try:
            s = os.stat(self.path)
            return (s.st_mtime_ns, s.st_size)
        except OSError:
            return None

    def _read(self):
        from .config import read_yaml
        try:
            with open(self.path) as f:
                # a key written twice is refused, as at load (K2)
                raw = read_yaml(f.read())
            got = (raw.get("hops") or {}).get(self.hop.name)
        except Exception:  # noqa: BLE001 - not this hop's file, then
            return None
        if not isinstance(got, dict):
            return None
        from .config import scope_patterns
        try:
            out = {k: scope_patterns(got.get(k))
                   for k in ("exclude", "include")}
        except SystemExit:
            # said wrong mid-run: the scope stays as it was until the
            # file says it right
            return None
        mapping = got.get("mapping")
        out["mapping"] = mapping if isinstance(mapping, dict) else {}
        return out

    def refresh(self):
        """Whether the hop's scope changed, its `exclude` and `include` set
        to what the file says now - and its `mapping`, the one it had kept
        for `take_remapped`."""
        stamp = self._stamp()
        if stamp == self.stamp or self.said is None:
            return False
        self.stamp = stamp
        now = self._read()
        if now is None or now == self.said:
            return False
        moved = any(now[k] != (self.said.get(k) or [])
                    for k in ("exclude", "include"))
        for k in ("exclude", "include"):
            was = self.said.get(k) or []
            others = [p for p in (getattr(self.hop, k, None) or [])
                      if p not in was]
            setattr(self.hop, k, list(now[k]) + [p for p in others
                                                 if p not in now[k]])
        if now["mapping"] != self.said.get("mapping"):
            if self.remapped is None:
                self.remapped = dict(self.hop.mapping or {})
            self.hop.mapping = dict(now["mapping"])
        self.said = now
        return moved

    def take_remapped(self):
        """The mapping the hop had before the file changed it, once; None
        where it has not changed."""
        got, self.remapped = self.remapped, None
        return got


def _add_ways():
    """How a table added to a running tail gets its rows (`decide`): its
    changes since it left, read again from where it left, where that is
    exact; else a copy under its own marks."""
    from .decide import Need, Rung
    return (
        Rung("from-removal", "its changes since it left the tail, read"
             " again from where it left",
             needs=(Need("where it left the tail", lambda f: f["at"],
                         "{rung} needs the table to have left this tail"
                         " before"),
                    Need("a source that reads from a position again",
                         lambda f: f["rereads"],
                         "the source's log is read once, from where the"
                         " tail is, and not again from an older position"),
                    Need("its target digested when it left",
                         lambda f: f["digest"],
                         "what its target held when it left was not read"),
                    Need("the same shape as when it left",
                         lambda f: f["same_shape"],
                         "its shape changed since it left"),
                    Need("the source's log still holding where it left",
                         lambda f: not f["lost"],
                         lambda f: "the source's log no longer holds where"
                                   f" it left: {f['lost']}")),
             because="only what changed since it left is read, not the"
                     " table"),
        Rung("copy", "a copy under its own marks, beside the stream",
             because="its rows are read as they are, range by range, and"
                     " its changes held to each range's mark",
             footprint=1),
    )


#: a column default that is the same value whenever it is taken: a
#: number, a quoted string, NULL, true or false, with a cast after it
_CONSTANT = re.compile(r"^\(*\s*(?:[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?|'(?:[^']"
                       r"|'')*'|null|true|false|b'[01]*')\s*\)*"
                       r"(?:\s*::\s*[a-z_ ][a-z0-9_ ()\[\],.]*)?\s*$",
                       re.I)


def constant_default(text):
    return text is None or bool(_CONSTANT.match(str(text).strip()))


def _state_text(st):
    return json.dumps(st, sort_keys=True, default=str)


def states(where):
    """{table: its state} a tail keeps in `where`, read only."""
    return Store.of(where).tables()


def accounts(where):
    """{table: what the tail read of it and did with it} kept in `where`
    (`hetero._Accounts`), read only."""
    return Store.of(where).accounts()


class TableSet:
    """The tables of one running tail, and what each is waiting for."""

    def __init__(self, pair, db, token_path, log, env=None):
        self.pair, self.db, self.log = pair, db, log
        self.hop = pair.hop
        self.src = pair.src_engine
        self.where = token_path.parent
        #: the hop's `ddl` for a table no rule names (`policy`)
        self.policy = policy(self.hop)
        skipped(self.hop)
        self.most = spool_most(self.hop)
        try:
            from . import evidence
            self.recipients = evidence.recipients_of(self.hop)
        except Exception:  # noqa: BLE001 - kept plain
            self.recipients = []
        self.store = Store.of(self.where)
        if self.store.get("stream") is None:
            self.store.import_legacy(self.where, self.recipients)
        if env is None:
            from . import tailctl
            env = tailctl.position(token_path)
        if env is not None:
            self.store.align(env, log, self._nothing_held)
        stored = self.store.tables()
        # each table's state as the store holds it, and the state it was
        # written from: only what changed is written (audit ST19)
        self._stored = {t: _state_text(st) for t, st in stored.items()}
        self.tables = self._keyed(stored)
        self._stored_obj = {}
        self.fresh = not self.tables
        #: the scope the tail last ran under (`save`), held to the hop's at
        #: the first batch: a scope changed while it was stopped is a
        #: scope changed
        self.was = self.store.get("scope") if not self.fresh else None
        self._scope_text = _state_text(self.store.get("scope"))
        self.lock = threading.RLock()
        self.file = _HopFile(self.hop)
        self.jobs, self.slots, self.ck = {}, None, None
        self.scope_at = self.level_at = 0.0
        # what a job beside the stream has to tell the next batch
        self.events = []
        # what the batch in hand will do once it is saved (`committed`)
        self.pending = {}
        # the states a batch set down (`prepare`) and not yet the tail's
        self._prepared = None
        # {leaf: what a copying table's held changes were last decided
        # under}: decided again only once that moves (`release`)
        self._released_at = {}
        self._now_mark = None
        self.entries = {}
        # copies that stopped, by table, for the wait before the next try
        self.failures = Counter()
        #: `quiet(name)`, where the tail gives one: the target's triggers
        #: held off for a table that joins, as for those it began with -
        #: and the tables it was asked for
        self.quiet, self.quieted = None, set()
        # the hop's mapping changed under the running tail, for the tail
        # to read its targets again (`take_remapped`)
        self.remapped = False

    # --- names --------------------------------------------------------------

    def _parts(self, name):
        """A table's name as its parts, a quoted part unquoted
        (`util._parts`)."""
        from .util import _canonical, _parts
        eng = _canonical(getattr(self.src, "CANON_ENGINE", "") or "")
        return [p for p in _parts(eng, str(name)) if p]

    def key(self, name):
        """A table's key among the tail's: its whole name as the source
        names it, each part unquoted - the log's `public."Orders"` and the
        catalogue's `public.Orders` one table."""
        return ".".join(self._parts(name))

    def _keyed(self, tables):
        """The states a tail saved, each under its `key` - one saved by its
        last part alone moved to its whole name, what it set aside with
        it, in the store at once."""
        out, moved = {}, {}
        for k, st in tables.items():
            new = self.key(st.get("name") or k)
            if new != k:
                moved[k] = new
            out[new] = st
        if moved:
            with self.store.batch() as s:
                for old, new in moved.items():
                    s.drop("tables", old)
                    s.put("tables", new, out[new])
                    s.move_frames(old, new)
            for old, new in moved.items():
                self._stored.pop(old, None)
                self._stored[new] = _state_text(out[new])
        return out

    def _out(self, name):
        """Whether the hop leaves this table out."""
        return self.hop.excluded(self.db, *self._parts(name))

    def _policy(self, name):
        return policy(self.hop, [self.db, *self._parts(name)])

    def _nothing_held(self):
        """Whether a store behind its position can be missing nothing:
        every table live, nothing set aside, and a hop whose `ddl` stops
        the tail at a DDL - so no batch it missed could have parked a
        table or set a change aside (`Store.align`)."""
        return (self.policy == "stop"
                and all(st.get("state") == "live"
                        for st in self.store.tables().values())
                and not self.store.spooled())

    # --- files --------------------------------------------------------------

    def spool(self, leaf):
        return Spool(self.store, leaf, self.recipients)

    def copies(self):
        """The tail's own copy checkpoint (`cli._Checkpoint` over
        `tail-copy.json`)."""
        if self.ck is None:
            from .cli import _Checkpoint
            self.ck = _Checkpoint(self.where / COPY)
        return self.ck

    def _entry(self, leaf):
        # the name the table copier keeps a table's checkpoint under
        return self.pair.move_key(self.db, "", self.pair._leaf(leaf))

    def _scope_said(self):
        """What the tail runs under, kept beside its tables: the hop's
        scope fingerprint (`Hop.scope_fingerprint`), and its lists and
        mapping, to tell a table they changed for."""
        return {"fingerprint": self.hop.scope_fingerprint(),
                "include": sorted(str(p) for p in self.hop.include or []),
                "exclude": sorted(str(p) for p in self.hop.exclude or []),
                "mapping": self.hop.mapping or {}}

    def save(self):
        """The tables' states that changed between batches, written now, at
        the position there is - each compared, since one may have been
        changed where it stands."""
        with self.lock:
            states = dict(self.tables)
        self._write(states, None, every=True)

    def _write(self, states, gen, accounts=None, pending=None, every=False):
        """One transaction: the states of `states` that differ from the
        store's, what the batch in hand sets aside and releases, and its
        `accounts` - a batch's where `gen` is given (`Store.batch`), with
        `pending`, the batch's number and where it ends, for the target's
        mark to be held to (`hetero._committed_ahead`). In a batch only a
        state the batch replaced is written out to be compared (`begin`
        hands it the tail's own, each replaced, never changed where it
        stands): 2,000 tables were 2,000 states written out a batch."""
        # what a batch sets aside and releases goes with the batch only
        work = {} if every else (self.pending or {})
        texts = {t: _state_text(st) for t, st in states.items()
                 if every or st is not self._stored_obj.get(t)}
        changed = {t: v for t, v in texts.items()
                   if self._stored.get(t) != v}
        gone = [t for t in self._stored if t not in states]
        adds = work.get("add") or {}
        frames = work.get("frames") or {}
        drops = work.get("drop") or ()
        # the scope it runs under, beside its tables (`_since_stopped`)
        scope = _state_text(self._scope_said())
        scope = scope if scope != self._scope_text else None
        if gen is None and not (changed or gone or adds or frames or drops
                                or accounts or scope):
            self._stored_obj = dict(states)
            return
        with self.store.batch(gen) as s:
            for t, v in changed.items():
                s.put("tables", t, json.loads(v))
            for t in gone:
                s.drop("tables", t)
            for leaf in drops:
                s.drop_frames(leaf)
            for fid, stay in frames.items():
                s.set_frame(fid, stay, self.recipients)
            for leaf, got in adds.items():
                for at, changes in got:
                    s.add_frame(leaf, at, changes, self.recipients)
            for t, row in (accounts or {}).items():
                s.put("accounts", t, row)
            if gen is not None:
                s.put("meta", "pending", pending)
            if scope is not None:
                s.put("meta", "scope", json.loads(scope))
        if scope is not None:
            self._scope_text = scope
        self._stored.update(changed)
        for t in gone:
            self._stored.pop(t, None)
        self._stored_obj = dict(states)

    def resumed(self, token):
        """The tail started at `token`: a copy or a catch-up that was
        running is started again, from its checkpoint. What a batch set
        aside that its position never passed is gone already
        (`Store.align`)."""
        for leaf, st in list(self.tables.items()):
            if st["state"] == "copying":
                self._start(leaf, st, fresh=False)
            if st["state"] == "removed" and "digest" not in st \
                    and st.get("why") != "dropped on the source":
                self._submit(leaf, self._digest_removed)

    def forget(self, token=None):
        """A batch whose position was not saved: what it set down is taken
        back out (`Store.undo`), and what it would have done is not
        done."""
        if self._prepared is not None:
            self.store.undo()
            self._stored = {t: _state_text(st) for t, st
                            in self.store.tables().items()}
            self._stored_obj = dict(self.tables)
            self._prepared = None
        self.pending = {}

    # --- scope, between batches -------------------------------------------

    def _scope(self, shape, changed=False):
        """{key: the name the log gives it} of the tables in the hop's
        scope now: `shape`'s (`drift.shape`, read under the hop's scope),
        read again where the scope has just changed."""
        from . import drift
        if shape is not None and changed:
            shape = drift.shape(self.src, "src", self.db)
        if shape is not None:
            names = [str(t) for t in shape]
        else:
            names = [str(t) for t in self.src.neutral_tables("src", self.db)
                     if not self._out(t)]
            working = drift.transient_among(names)
            names = [t for t in names if t not in working]
        return {self.key(t): t for t in names}

    def _since_stopped(self):
        """(whether the hop's scope changed while the tail was stopped,
        the mapping it ran under where that changed, else None) - read
        once, at the first batch after a start: a table it now takes was
        marked live from the current file, with none of its rows."""
        was, self.was = self.was, None
        if not isinstance(was, dict) or not self.hop.scope_changed(
                was.get("fingerprint")):
            return False, None
        said = self._scope_said()
        moved = any(sorted(was.get(k) or []) != said[k]
                    for k in ("include", "exclude"))
        mapping = was.get("mapping") or {}
        remapped = mapping if mapping != said["mapping"] else None
        if moved or remapped is not None:
            self.log("the hop's scope changed while the tail was stopped:"
                     " the tables it takes or leaves since are taken or"
                     " left as a change of it while running would")
        return moved, remapped

    def take_remapped(self):
        """Whether the hop's mapping changed since last asked: the tail's
        own reading of names and columns is then read again."""
        got, self.remapped = self.remapped, False
        return got

    def boundary(self, token, shape, force=False):
        """Between batches, before the next is read: the hop's scope held
        to the tables' states - a table added is started on, one gone is
        removed, one whose mapping changed is taken again, one replaced
        under its name is copied or stops the tail. Returns whether the
        hop's scope changed, for a reader that read ahead under the old
        one to be started again."""
        self._events()
        changed = self.file.refresh()
        remapped = self.file.take_remapped()
        if not self.fresh and self.was is not None:
            moved, before = self._since_stopped()
            changed = changed or moved
            if remapped is None:
                remapped = before
        if not (changed or force or self.fresh or shape is not None
                or remapped is not None
                or time.monotonic() - self.scope_at >= 60):
            return False
        self.scope_at = time.monotonic()
        try:
            scope = self._scope(shape, changed)
            oids = self._oids()
        except Exception as e:  # noqa: BLE001 - asked again next batch
            self.log(f"the source's tables could not be listed"
                     f" ({type(e).__name__}); the tail's scope is as it was")
            return False
        with self.lock:
            if self.fresh:
                for leaf, name in scope.items():
                    self.tables[leaf] = {"state": "live", "name": name}
                    if oids.get(leaf):
                        self.tables[leaf]["oid"] = oids[leaf]
                self.fresh = False
                self.save()
                return False
            known = {t for t, st in self.tables.items()
                     if st["state"] != "removed"}
            # a table the hop's `ddl` left out as it was made stays out
            # until the hop's scope is changed
            added = [t for t in scope if t not in known and (
                changed or not (self.tables.get(t) or {}).get("left"))]
            gone = [t for t in sorted(known) if t not in scope]
        for leaf in gone:
            self._remove(self.tables, leaf, token,
                         "taken out of the hop's scope" if changed
                         else "no longer on the source")
        unseen = getattr(self.src, "CREATES_UNSEEN", False)
        for leaf in added:
            st = self.tables.get(leaf) or {}
            back = st.get("state") == "removed" and not st.get("left")
            made = not (changed or back)
            if made and "create" in skipped(self.hop):
                self._leave(self.tables, leaf, scope[leaf], token,
                            "made on the source, and the hop's ddl leaves"
                            " creates out")
            elif not made or self._policy(scope[leaf]) != "stop" or unseen:
                self._add(self.tables, leaf, scope[leaf], token,
                          "put back in the hop's scope" if back
                          else "added to the hop's scope" if changed
                          else "made on the source, where the log says"
                               " nothing of a table's making and a table"
                               " made with its rows (CREATE TABLE AS) has"
                               " them in no change" if unseen
                          else "made on the source")
            else:
                # as the tail always did: a table made on the source is
                # applied from its first change, and the check of its
                # shape stops the tail where the target lacks it
                with self.lock:
                    self.tables[leaf] = {"state": "live",
                                         "name": scope[leaf]}
            if oids.get(leaf):
                with self.lock:
                    self.tables[leaf]["oid"] = oids[leaf]
        again = self._replaced(oids, token, set(added))
        if remapped is not None:
            again += self._remapped(remapped, token)
        if gone or added or again:
            self._settle()
            self.save()
        return bool(changed and (gone or added))

    def _oids(self):
        """{key: the table's own id} where the source keeps one a name can
        be given to another table (`table_oids`), else {}."""
        read = getattr(self.src, "table_oids", None)
        if read is None:
            return {}
        return {self.key(t): oid for t, oid in read("src", self.db).items()}

    def _replaced(self, oids, token, added):
        """The tables whose name now names another table on the source - a
        rename, or two that swap one in under it - where its log says
        nothing of a rename and its columns may be the same
        (`table_oids`): under stop the tail stops, under park and apply
        the table is copied again. [the keys copied again]"""
        out = []
        with self.lock:
            held = [(k, dict(st)) for k, st in self.tables.items()
                    if st["state"] != "removed" and k not in added]
        for k, st in held:
            now = oids.get(k)
            if not now:
                continue
            if not st.get("oid"):
                with self.lock:
                    self.tables[k]["oid"] = now
                continue
            if now == st["oid"]:
                continue
            name = st.get("name") or k
            if self._policy(name) == "stop":
                raise SystemExit(
                    f"{self.db}: {name} was replaced on the source - its"
                    " name names another table now (renamed, or two"
                    " renames swapped one in under it) - at"
                    f" {str(token)[:60]} or after, and nothing after that"
                    " position was applied. The hop's `ddl` is stop: set"
                    " `ddl: apply` or `ddl: park` to have it copied again,"
                    " and run the tail again; it resumes from there")
            self._add(self.tables, k, name, token,
                      "replaced on the source under its name (a rename or"
                      " a swap), copied again")
            with self.lock:
                self.tables[k]["oid"] = now
            out.append(k)
        return out

    def _remapped(self, before, token):
        """The tables the hop carries whose row filter, columns, name on
        the target or changes carried read otherwise under the mapping
        now than under `before`, taken again by a copy - their target
        holds rows the mapping they ran under put there. Said whether any
        are or not. [the keys taken again]"""
        import dataclasses
        was = dataclasses.replace(self.hop, mapping=dict(before or {}))
        self.remapped = True
        out = []
        with self.lock:
            held = [(k, dict(st)) for k, st in self.tables.items()
                    if st["state"] in ("live", "parked", "copying")]
        for k, st in held:
            parts = [self.db, *self._parts(st.get("name") or k)]
            if all(getattr(was, f)(*parts) == getattr(self.hop, f)(*parts)
                   for f in ("row_filter", "column_rules", "carries")) and \
                    was.target_table_in(*parts) == \
                    self.hop.target_table_in(*parts):
                continue
            self._add(self.tables, k, st.get("name") or k, token,
                      "its row filter, columns, name on the target or"
                      " changes carried changed in the hop's mapping,"
                      " taken again")
            out.append(k)
        self.log("the hop's mapping changed"
                 + (f": {', '.join(out[:8])}" + (" ..." if len(out) > 8
                                                 else "")
                    + " taken again by a copy" if out
                    else " - no table the tail carries reads otherwise"))
        return out

    def _leave(self, states, leaf, name, token, why):
        """A table the hop's `ddl` leaves out as it is made: out of the
        tail, its changes counted as out of the hop's scope, and its target
        - which it has none on - never digested."""
        states[leaf] = {"state": "removed", "name": name, "at": token,
                        "why": why, "was": None, "left": True, "said": True}
        self.log(f"{leaf}: {why} - its changes are left out and counted")

    def _remove(self, states, leaf, token, why):
        st = states.get(leaf) or {}
        if st.get("state") == "removed":
            return
        states[leaf] = {"state": "removed", "name": st.get("name") or leaf,
                        "at": token, "why": why, "was": st.get("state")}

    def _add(self, states, leaf, name, token, why):
        """`leaf` added at `token`, by the way the climb chooses
        (`_add_ways`); started once the states it is in are the tail's
        (`_settle`)."""
        from . import decide
        prev = dict(states.get(leaf) or {})
        if prev.get("state") != "removed":
            prev = {}
        facts = decide.Facts(
            {"at": prev.get("at"), "digest": bool(prev.get("digest"))},
            rereads=lambda: bool(getattr(self.src, "REREADS_FROM_A_POSITION",
                                         False)),
            same_shape=lambda: self._same_shape(prev, name),
            lost=lambda: self._lost(prev.get("at")))
        got = decide.climb(f"{self.db}.{leaf}", _add_ways(), facts)
        way, reason = got.path, got.reason
        st = {"state": "copying", "name": name, "since": token,
              "why": why, "start": True}
        if way == "from-removal":
            try:
                upto = self.src.log_position("src", self.db)
            except Exception:  # noqa: BLE001 - copied instead
                upto = None
            if upto is None:
                way, reason = "copy", ("the source could not say where its"
                                       " log is now")
            else:
                st.update(at=prev["at"], digest=prev["digest"],
                          columns=prev.get("columns"), upto=upto)
        st["way"] = way
        states[leaf] = st
        said = dict((r.name, r.said) for r in _add_ways())[way]
        self.log(f"{leaf}: added to the tail ({why}) - {said}: {reason}")

    def _columns_of(self, name):
        """The source's columns of `name` as it declares them now - read the
        same way when a table leaves and when it comes back, whatever the
        hop's scope says of it between."""
        return [[str(n), str(t)] for n, t in sorted(
            self.src.neutral_columns("src", self.db, name))]

    def _same_shape(self, prev, name):
        if prev.get("shape") is None:
            return False
        try:
            return self._columns_of(name) == prev["shape"]
        except Exception:  # noqa: BLE001 - not known to be the same
            return False

    def _lost(self, at):
        if at is None:
            return "no position"
        try:
            return self.src.position_lost("src", self.db, at)
        except Exception as e:  # noqa: BLE001 - not known to be held
            return type(e).__name__

    def _settle(self):
        """What the states now in force ask for, started: a copy or a
        catch-up for a table added, a digest of the target for a table
        that left, a spool dropped for a table that no longer sets one
        aside."""
        with self.lock:
            todo = [(t, dict(st)) for t, st in self.tables.items()
                    if (st["state"] == "copying" and st.get("start"))
                    or (st["state"] == "removed" and "said" not in st)
                    or (self.quiet is not None and t not in self.quieted
                        and st["state"] in ("live", "copying"))]
        for leaf, st in todo:
            if st["state"] in ("live", "copying") and not self._quiet(leaf,
                                                                     st):
                continue
            if st["state"] == "copying" and st.get("start"):
                with self.lock:
                    self.tables[leaf] = {k: v for k, v in
                                         self.tables[leaf].items()
                                         if k != "start"}
                self._stop_job(leaf)
                self.spool(leaf).remove()
                self._start(leaf, st, fresh=True)
            elif st["state"] == "removed" and "said" not in st:
                self._stop_job(leaf)
                with self.lock:
                    self.tables[leaf] = {**self.tables[leaf], "said": True}
                self.spool(leaf).remove()
                if st.get("why") != "dropped on the source":
                    self._submit(leaf, self._digest_removed)
                self.log(f"{leaf}: out of the tail from"
                         f" {str(st['at'])[:60]} - {st['why']}; its"
                         " changes are left out and counted, and its target"
                         " is left as it is")
        return bool(todo)

    def _quiet(self, leaf, st):
        """The target's triggers held off for a table the tail takes, once
        (`quiet`): a table that joined after the tail began had its
        target's triggers fire for every row its copy and its changes
        wrote. Where they cannot be held off the table parks, and its copy
        is tried again later. Whether it goes on."""
        if self.quiet is None or leaf in self.quieted:
            return True
        self.quieted.add(leaf)
        try:
            self.quiet(st.get("name") or leaf)
            return True
        except SystemExit as e:
            said = (str(e).strip().splitlines() or ["refused"])[0]
            with self.lock:
                self.quieted.discard(leaf)
                self.tables[leaf].pop("start", None)
                self._park(self.tables, leaf, None,
                           f"its target's triggers would fire for what is"
                           f" written: {said[:200]}", "copy")
            return False

    # --- the jobs beside the stream -----------------------------------------

    def _submit(self, leaf, fn):
        """`fn(leaf, stop)` beside the stream, as many at once as half the
        hop's workers. A thread the process does not wait for at its end:
        a tail stopped mid-copy ends at once, and the copy goes on from
        its checkpoint when the tail starts again."""
        if self.slots is None:
            self.slots = threading.BoundedSemaphore(
                max(1, int(getattr(self.hop, "workers", 2) or 2) // 2))
        stop = threading.Event()

        def job():
            with self.slots:
                if not stop.is_set():
                    self._run_job(leaf, fn, stop)
        t = threading.Thread(target=job, daemon=True,
                             name=f"migkit-tail-{leaf}")
        self.jobs[leaf] = (t, stop)
        t.start()

    def _run_job(self, leaf, fn, stop):
        try:
            fn(leaf, stop)
        except BaseException as e:  # noqa: BLE001 - the table parks
            if not stop.is_set():
                said = (str(e).strip().splitlines()
                        or [type(e).__name__])[0]
                with self.lock:
                    self.events.append(("park", leaf,
                                        f"its copy stopped: {said[:200]}"))

    def _stop_job(self, leaf):
        got = self.jobs.pop(leaf, None)
        if got is not None:
            got[1].set()

    def _events(self):
        """What the jobs said since the last batch, made the tail's."""
        with self.lock:
            events, self.events = self.events, []
        for kind, leaf, why in events:
            if kind == "park":
                self._park(self.tables, leaf, None, why, "copy")
                self.save()
            elif kind == "digested":
                with self.lock:
                    st = self.tables.get(leaf) or {}
                    if st.get("state") == "removed" \
                            and st.get("at") == why["at"]:
                        st.update(digest=why["digest"], shape=why["shape"],
                                  columns=why["columns"])
                self.save()
            elif kind in ("caught up", "copied instead"):
                with self.lock:
                    st = self.tables.get(leaf) or {}
                    if st.get("state") == "copying":
                        if kind == "caught up":
                            st["done"] = True
                        else:
                            st["way"] = "copy"
                            for k in ("at", "upto", "digest", "columns"):
                                st.pop(k, None)
                self.save()

    def _start(self, leaf, st, fresh):
        """The job a copying table's state asks for, handed the state it
        was started under."""
        st = dict(st)
        if st.get("way") == "from-removal":
            self._submit(leaf, lambda t, stop: self._catch_up(t, stop, st))
            return
        if fresh:
            ck = self.copies()
            with ck._lock:
                ck.pop(self._entry(leaf), None)
                ck.save()
        self._submit(leaf, lambda t, stop: self._copy(t, stop, st["name"]))

    def _worker(self):
        """A pair of its own for a job: the stream's engines hold what
        their reads and writes keep between batches, and a copy beside
        them must not share it."""
        return type(self.pair)(self.pair.hop)

    def _copy(self, leaf, stop, name):
        """The table copied by the pair's own copier into the tail's
        checkpoint, under a mark taken before it reads: each range's own
        where it is split (`ranges.started`), the table's where it is not
        (`mark_before`) - and its target emptied first, as a copy begun
        afresh empties it. A value the target would not take as it is
        stops it before anything is written, as it stops a move."""
        import contextlib
        pair = self._worker()
        ck = self.copies()
        key = self._entry(leaf)
        with ck._lock:
            fresh = not ck.get(key)
        if fresh:
            pair.refuse_unfit(self.db, [name])
            # what the stream left out of it before it joined is in the
            # log already: read only once a read can see all of it
            pair.src_engine.visible_now("src", self.db)
        with ck._lock:
            st = ck.setdefault(key, {})
            if not st.get("ranges") and "mark_before" not in st:
                st["mark_before"] = pair.src_engine.snapshot_mark(
                    "src", self.db)
                st["table"] = name
                ck.save()
        sch, _, tbl = name.rpartition(".")
        with contextlib.ExitStack() as later:
            # its rows written as a load writes them, the target's
            # triggers held off: this pair's engines are its own, and the
            # stream's window holds off nothing they write
            later.enter_context(pair.load_window(
                self.db, lambda m: self.log(f"{leaf}: {m}"), {name}))
            pair._neutral_move(self.db, sch, tbl, 10 ** 9, ck,
                               lambda m: self.log(f"{leaf}: {m}"), later)
        if stop.is_set():
            return
        try:
            pair.finish_created(self.db, lambda m: self.log(f"{leaf}: {m}"))
        except Exception as e:  # noqa: BLE001 - the rows are in
            self.log(f"{leaf}: its indexes were not built:"
                     f" {(str(e).splitlines() or [''])[-1][:100]}")
        with ck._lock:
            ck[key]["done"] = True
            ck.save()

    def _catch_up(self, leaf, stop, st):
        """The changes of `leaf` since it left, read again by a reader of
        its own from where it left to where the source's log was when it
        came back, and applied; its changes the stream reads meanwhile are
        held, and applied after. Its target must hold what it held when it
        left, and its shape stay as it was on the way: where either does
        not, it is copied instead."""
        from . import canon
        pair = self._worker()
        dst_t = self._target(pair, st["name"])
        if self._digest(pair.dst_engine, "dst", dst_t, st.get("columns")) \
                != st.get("digest"):
            self.log(f"{leaf}: its target changed while it was out of the"
                     " tail - copying it instead")
            return self._fall_back(leaf, stop, st["name"])
        token, upto = st["at"], st["upto"]
        reader = self._reader()
        try:
            while not stop.is_set():
                reached = reader.position_reached(token, upto)
                if reached is None:
                    self.log(f"{leaf}: the source cannot say how far a read"
                             " again has got - copying it instead")
                    return self._fall_back(leaf, stop, st["name"])
                if reached:
                    break
                changes, after = reader.neutral_changes(
                    "src", self.db, token, limit=16000)
                mine = [c for c in changes if self.key(c["table"]) == leaf]
                if any(canon.is_ddl(c) for c in mine):
                    self.log(f"{leaf}: its shape changed while it was out"
                             " of the tail - copying it instead")
                    return self._fall_back(leaf, stop, st["name"])
                if mine:
                    pair.carry(self.db, mine)
                if after == token and not changes:
                    time.sleep(0.5)
                token = after
        finally:
            reader.release_changes()
        if not stop.is_set():
            with self.lock:
                self.events.append(("caught up", leaf, ""))

    def _reader(self):
        """The source's engine for a read of its own beside the stream's:
        under a replica id of its own where the source knows its readers
        by one (a second binlog reader under the stream's id is cut off by
        the server, and the stream's with it)."""
        import dataclasses
        opts = dict(self.hop.options or {})
        sid = int(opts.get("server_id", 4379))
        opts["server_id"] = 2 ** 31 + (sid + 1) % 2 ** 31
        return type(self.src)(dataclasses.replace(self.hop, options=opts))

    def _fall_back(self, leaf, stop, name):
        """A catch-up that cannot be exact, copied instead: the stream holds
        the table's changes to the copy's marks from the next batch on
        (`_events`), and until then holds them all."""
        with self.lock:
            self.events.append(("copied instead", leaf, ""))
        ck = self.copies()
        with ck._lock:
            ck.pop(self._entry(leaf), None)
            ck.save()
        self._copy(leaf, stop, name)

    def _target(self, pair, name):
        return pair._tail_target(pair._tail_targets(self.db), name, self.db)

    def _digest(self, eng, side, table, cols):
        if not cols:
            return None
        got = eng.neutral_digest(side, self.db, table,
                                 [tuple(c) for c in cols])
        return [int(got[0]), str(got[1])]

    def _digest_removed(self, leaf, stop):
        """What the target of a table that left the tail holds, and the
        source's shape of it then, kept for it to come back by."""
        with self.lock:
            st = dict(self.tables.get(leaf) or {})
        if st.get("state") != "removed":
            return
        pair = self._worker()
        name = st["name"]
        dst_t = self._target(pair, name)
        try:
            shape = self._columns_of(name)
            _, dc, _ = pair._comparable_columns(
                self.db, pair.src_engine, name, pair.dst_engine, dst_t)
        except Exception:  # noqa: BLE001 - its source is gone
            shape, dc = None, []
        digest = self._digest(pair.dst_engine, "dst", dst_t, dc)
        if not stop.is_set():
            with self.lock:
                self.events.append(("digested", leaf, {
                    "at": st["at"], "digest": digest, "shape": shape,
                    "columns": [list(c) for c in dc]}))

    # --- a batch -------------------------------------------------------------

    def begin(self):
        """(the states the batch works on, a copy) - and the copies'
        checkpoint entries as they stand, read once: the changes released
        and the changes routed in one batch are held to the same, so a
        range landing in between cannot let a newer change past an older
        one held for it."""
        import copy
        self._events()
        self._now_mark = None
        # the tail's own states, each replaced where the batch changes it
        # and never changed where it stands (`ddl`, `route`, `release`,
        # `park`) - a batch read again finds them as they were
        with self.lock:
            states = dict(self.tables)
        # the entries of the tables being copied only: every table the
        # tail ever copied went through JSON each batch (audit ST19)
        keys = {self._entry(t) for t, st in states.items()
                if st.get("state") == "copying"}
        self.entries = {}
        if keys:
            ck = self.copies()
            with ck._lock:
                self.entries = copy.deepcopy(
                    {k: ck[k] for k in keys
                     if isinstance(ck.get(k), dict)})
        self.pending = {"add": {}, "frames": {}, "drop": [], "said": [],
                        "seen": {}, "live": []}
        return states

    def _now(self):
        if self._now_mark is None:
            self._now_mark = (self.src.snapshot_mark("src", self.db),)
        return self._now_mark[0]

    def _verdict(self, leaf, st, change):
        """What a change of a copying table is now: `out` (the copy read
        it), `apply`, or `held`."""
        from . import ranges
        from .engines.hetero import _CopiedRanges
        if st.get("way") == "from-removal":
            return "apply" if st.get("done") else "held"
        entry = self.entries.get(self._entry(leaf)) or {}
        done = bool(entry.get("done"))

        def covers(mark):
            return mark is not None and self.src.mark_covers(
                "src", self.db, mark, change) is True
        if entry.get("ranges"):
            values = _CopiedRanges._values(entry, change)
            places = ranges.place(entry, values) if values else []
            if not places:
                return "apply" if done else "held"
            if done:
                return "out" if all(w == "read" and covers(m)
                                    for w, m in places) else "apply"
            if any(w in ("reading", "outside") for w, _ in places):
                return "held"
            if all(w == "read" for w, _ in places):
                return "out" if all(covers(m) for _, m in places) \
                    else "apply"
            # a range not begun reads after now: what the source shows
            # committed now is in what it will read
            if all(covers(m) for w, m in places if w == "read") \
                    and covers(self._now()):
                return "out"
            return "held"
        if not done:
            return "held"
        return "out" if covers(entry.get("mark_before")) else "apply"

    def _landed(self, leaf, st):
        if st.get("way") == "from-removal":
            return bool(st.get("done"))
        return bool((self.entries.get(self._entry(leaf)) or {}).get("done"))

    def _decided_by(self, leaf, st):
        """What a copying table's held changes are decided by: its copy's
        ranges planned, begun and landed, whether it is done - a change
        held under one of these stays held until one of them moves (a
        range begins or lands), so its frames are not read again every
        batch while nothing it waits on has (audit ST18: 200,000 held
        changes read, unpickled and written back whole, 259 ms + 126 ms a
        batch)."""
        entry = self.entries.get(self._entry(leaf)) or {}
        return (st.get("way"), bool(st.get("done")), bool(entry.get("done")),
                len(entry.get("ranges") or ()),
                len(entry.get("ranges_done") or ()),
                len(entry.get("ranges_seen") or ()),
                entry.get("mark_before") is not None)

    def release(self, states):
        """(the changes set aside earlier that may be applied now, in
        their order, {table: how many}, how many were found in what a copy
        read): a copying table's held changes held to its ranges as they
        stand, a level parked table's all. A table with nothing left held
        and its copy done is live from this batch on. A frame whose
        changes all stay is not written; one half released is written with
        the rest; one released whole is deleted - the spool is not
        rewritten."""
        out, gone = [], 0
        for leaf, st in sorted(states.items()):
            if st["state"] == "copying":
                by = self._decided_by(leaf, st)
                if self._released_at.get(leaf) == by:
                    continue
                self.pending["seen"][leaf] = by
                held = 0
                for fid, _, got in self.spool(leaf).frames():
                    stay, went = [], []
                    for c in got:
                        v = self._verdict(leaf, st, c)
                        if v == "held":
                            stay.append(c)
                        elif v == "apply":
                            went.append(c)
                        else:
                            gone += 1
                    if len(stay) != len(got):
                        self.pending["frames"][fid] = stay
                    held += len(stay)
                    out += went
                if not held and self._landed(leaf, st):
                    states[leaf] = {"state": "live", "name": st["name"]}
                    self.pending["live"].append(leaf)
                    self.pending["said"].append(
                        f"{leaf}: caught up - its changes are applied as"
                        " they are read again")
            elif st["state"] == "parked" and st.get("level"):
                out += self.spool(leaf).changes()
                self.pending["drop"].append(leaf)
                states[leaf] = {"state": "live", "name": st["name"]}
                self.pending["said"].append(
                    f"{leaf}: its target is level - what was set aside is"
                    " applied, and its changes as they are read again")
        return out, Counter(c["table"] for c in out), gone

    def route(self, states, changes, token):
        """(the changes to apply, {why: Counter}) of one run of the batch's
        changes: a live table's go on, a copying table's are held to its
        copy, a parked table's set aside, a removed table's left out.
        What is set aside is filed under `token`, where the batch was read
        from."""
        out = []
        left = {w: Counter() for w in (COPIED,) + WHY}
        put = {}
        for c in changes:
            leaf = self.key(c["table"])
            st = states.get(leaf)
            if st is None and not self.fresh and (
                    self._policy(c["table"]) != "stop"
                    or getattr(self.src, "CREATES_UNSEEN", False)):
                # a table the tail has not met - made on the source since
                # the scope was read - joins with its copy, its changes
                # held to it from the first; or stays out, where the hop's
                # `ddl` leaves creates out
                if "create" in skipped(self.hop):
                    self._leave(states, leaf, c["table"], token,
                                "made on the source, and the hop's ddl"
                                " leaves creates out")
                else:
                    self._add(states, leaf, c["table"], token,
                              "made on the source")
                st = states[leaf]
            state = st["state"] if st else "live"
            if state == "live":
                out.append(c)
            elif state == "removed":
                left[OUT_OF_SCOPE][c["table"]] += 1
            elif state == "parked":
                put.setdefault(leaf, []).append(c)
                left[SPOOLED][c["table"]] += 1
            else:
                v = self._verdict(leaf, st, c)
                if v == "apply":
                    out.append(c)
                elif v == "out":
                    left[COPIED][c["table"]] += 1
                else:
                    put.setdefault(leaf, []).append(c)
                    left[HELD][c["table"]] += 1
        adds = self.pending.setdefault("add", {})
        for leaf, got in put.items():
            # written with the batch (`prepare`), before the target
            # commits it
            adds.setdefault(leaf, []).append((_token_text(token), got))
        return out, left

    def _names_among(self, states, change):
        """The tables the tail knows on the source - each state's name and
        key, and the tables the change names - against which a working
        table of an online schema change is told from an application's own
        (`drift.transient`): `_orders_new` is one beside `orders`, and the
        application's without it. Read by the name alone, the tail took an
        application's table so named for a working table and skipped its
        rename."""
        out = set()
        for leaf, st in states.items():
            out.add(str(leaf))
            if st.get("name"):
                out.add(str(st["name"]))
        out.update(str(x) for x in (change.get("table"), change.get("to"))
                   if x)
        return frozenset(out)

    def ddl(self, states, change, token, two_way=False):
        """What a DDL at its place in the batch does, by the hop's `ddl`:
        None, or ("truncate", leaf) for the target to be emptied there.
        The states the rest of the batch is routed by are changed here;
        they become the tail's when the batch is saved.

        The policy is the table's own (`policy`), and for a table renamed
        into the hop's scope the name it comes in under. A kind the hop's
        `ddl` leaves out (`skipped`), and a table emptied whose deletes
        the hop does not carry (`mapping.ops`, an archive kept on the
        target), are left out and counted (`pending["ddl_left"]`) - never
        applied, never a stop. The swap an online schema change ends with
        (gh-ost's and pt-online-schema-change's renames through their
        working tables, `drift.transient`) is the table's new content, not
        a table renamed out of the hop's scope and one renamed into it:
        under stop its shape is held as any DDL's is (`hetero._shape_now`),
        under park and apply it is copied again."""
        from . import canon, drift
        leaf = self.key(change["table"])
        kind = change["kind"]
        to = change.get("to")
        st = states.get(leaf) or {"state": "live", "name": change["table"]}
        among = self._names_among(states, change)
        if kind == "rename" and drift.transient(change["table"], among):
            # the working table swapped in under the table's name: the
            # table's own rename out of the way says what is done
            return None
        if st["state"] == "removed" and kind not in ("create", "rename"):
            return None
        why = self._left_out(kind, change["table"])
        if why:
            self.pending.setdefault("ddl_left", Counter())[
                (change["table"], why)] += 1
            return None
        theirs = change["table"] if (leaf in states or not to or not self._out(
            change["table"])) else to
        pol = self._policy(theirs)
        if kind == "rename" and to and drift.transient(to, among):
            if pol != "stop":
                self._add(states, leaf, st.get("name") or leaf, token,
                          f"swapped for its online schema change's copy"
                          f" ({to}) on the source, copied again")
            return None
        stops = getattr(self.src, "DDL_STOPS", ("truncate", "rename"))
        came = change.get(canon.CAME_FROM)
        if pol == "stop":
            # rows changed with no row event for them stop the tail
            # wherever an emptied table does, and a table renamed in from
            # another database, with its rows, wherever a rename does
            if kind in stops or (kind == "reload" and "truncate" in stops) \
                    or (came and "rename" in stops):
                raise SystemExit(
                    f"{self.db}: {change['table']} was "
                    + {"truncate": "emptied", "drop": "dropped",
                       "rename": f"renamed to {change.get('to')}",
                       "create": f"renamed in from {came}" if came
                       else "made", "alter": "altered",
                       "reload": "given or robbed of rows with no row"
                                 " event for them (a partition's)"}[kind]
                    + f" on the source at {str(token)[:60]} or after, and"
                    " nothing after that position was applied. The hop's"
                    " `ddl` is stop: set `ddl: apply` to carry what can be"
                    " carried at its place, or `ddl: park` to set only that"
                    " table aside, and run the tail again; it resumes from"
                    " there")
            return None
        if kind == "reload":
            # never emptied in place: the rows the rest of the table holds
            # are the source's still
            self._add(states, leaf, st.get("name") or leaf, token,
                      "rows changed on the source with no row event for"
                      " them (a partition's), copied again")
            return None
        if kind == "truncate":
            if st["state"] == "copying" or pol == "park" or two_way:
                self._add(states, leaf, st.get("name") or leaf, token,
                          "emptied on the source, copied again")
                return None
            if st["state"] == "parked":
                self._add(states, leaf, st.get("name") or leaf, token,
                          "emptied on the source while parked, copied"
                          " again")
                return None
            return ("truncate", change["table"])
        if kind == "drop":
            self._remove(states, leaf, token, "dropped on the source")
            return None
        if kind == "rename":
            # a table the hop never took is not one the tail lets go of
            if leaf in states or not self._out(change["table"]):
                self._remove(states, leaf, token,
                             f"renamed to {to} on the source")
            if to and not self._out(to):
                self._add(states, self.key(to), str(to), token,
                          f"renamed from {change['table']} on the source")
            return None
        if kind == "create":
            if st["state"] in ("live", "removed") and not st.get("left") \
                    and (leaf not in states or st["state"] == "removed"):
                self._add(states, leaf, change["table"], token,
                          "made on the source")
            return None
        return None

    def _left_out(self, kind, table):
        """Why a DDL of `table` is left out of the tail, or "": a kind the
        hop's `ddl` skips, or the table emptied - wholly, or a partition's
        rows with no row event for them - where the hop does not carry its
        deletes, which keeps what the source takes away."""
        if kind in skipped(self.hop):
            return f"{kind} left out by the hop's ddl"
        if kind in ("truncate", "reload") and "delete" not in \
                self.hop.carries(self.db, *self._parts(table)):
            return f"{kind} {NOT_CARRIED} (it does not carry deletes)"
        return ""

    def left_ddl(self):
        """{(table, why): DDLs} the batch in hand left out (`_left_out`),
        for the tail's accounts."""
        return dict(self.pending.get("ddl_left") or {})

    def park(self, states, leaf, token, why):
        self._park(states, leaf, token, why, "shape")

    def _park(self, states, leaf, token, why, kind):
        st = states.setdefault(leaf, {"state": "live", "name": leaf})
        if st.get("state") in ("removed", "parked"):
            return
        states[leaf] = {"state": "parked", "name": st.get("name") or leaf,
                        "why": why, "kind": kind, "since": token,
                        "parked_at": time.time()}
        if kind == "copy":
            self.failures[leaf] += 1
        self.log(f"{leaf}: parked - {why}. Every other table goes on;"
                 " its changes are set aside on this machine until its"
                 " target is level")
        try:
            from . import notify
            notify.table_parked(self.hop, self.db, leaf, why, self.log)
        except Exception:  # noqa: BLE001 - said above already
            pass

    def prepare(self, states, gen, accounts=None, pending=None):
        """The batch set down in one transaction before the target commits
        it (`Store`): its states, what it sets aside and releases, its
        `accounts`, and `gen` - one past the position's - with what each
        replaced, to be taken back out where the position never passes
        it. `gen` None: a batch that does not move the position, written
        at the one there is."""
        self._write(states, gen, accounts, pending)
        self._prepared = states

    def committed(self, states=None):
        """The batch's position is saved: its states are the tail's, what
        they ask for is started, and the store keeps it for good."""
        if states is None:
            states = self._prepared
        self._prepared = None
        self.store.settled()
        pending, self.pending = self.pending or {}, {}
        for said in pending.get("said") or ():
            self.log(said)
        self._released_at.update(pending.get("seen") or {})
        with self.lock:
            self.tables = states
        if pending.get("live"):
            # a table live again needs its copy's entry no more
            ck = self.copies()
            with ck._lock:
                for leaf in pending["live"]:
                    ck.pop(self._entry(leaf), None)
                    self._released_at.pop(leaf, None)
                ck.save()
        if self._settle():
            self.save()

    def commit(self, states):
        """A batch set down and made the tail's at once: one that does not
        move the position (`prepare` with no generation)."""
        self.prepare(states, None)
        self.committed(states)

    # --- parked tables coming level, between batches ---------------------

    def check_level(self, level_of):
        """At most every five seconds: a parked table whose target has
        come level (`level_of(name)` True) is released at the next batch;
        one parked because its copy stopped is copied again once level;
        one whose spool passed `spool_limit` has it dropped and is copied
        again once level - read from the source as it is then, rather
        than the tail stopping."""
        if time.monotonic() - self.level_at < 5:
            return
        self.level_at = time.monotonic()
        with self.lock:
            parked = {t: dict(st) for t, st in self.tables.items()
                      if st["state"] == "parked"}
        changed = False
        for leaf, st in sorted(parked.items()):
            if not st.get("recopy") and self.spool(leaf).size() > self.most:
                self.spool(leaf).remove()
                st["recopy"] = True
                with self.lock:
                    self.tables[leaf]["recopy"] = True
                changed = True
                self.log(f"{leaf}: what was set aside for it passed"
                         f" {self.most:,} bytes and was dropped; the table"
                         " is copied again once its target is level")
            try:
                level = level_of(st["name"])
            except Exception:  # noqa: BLE001 - asked again later
                level = False
            if not level or st.get("level"):
                continue
            if st.get("kind") == "copy" and time.time() - float(
                    st.get("parked_at") or 0) < 60 * 2 ** min(
                    self.failures[leaf], 6):
                # a copy that stopped for a reason of its own is tried
                # again after a wait that doubles, not every five seconds
                continue
            changed = True
            if st.get("kind") == "copy" or st.get("recopy"):
                with self.lock:
                    self._add(self.tables, leaf, st["name"],
                              st.get("since"),
                              f"copied again once level: {st['why']}")
            else:
                with self.lock:
                    self.tables[leaf]["level"] = True
        if changed:
            self._settle()
            self.save()

    # --- what the tail says ---------------------------------------------

    def not_live(self):
        """The names of the tables whose changes are not applied as they
        are read now: their target is not held to the source's shape
        (`hetero._shape_now`), nor their kept proof to their changes
        (`kept`)."""
        with self.lock:
            return {st.get("name") or t for t, st in self.tables.items()
                    if st["state"] != "live"}

    def spilled(self):
        """{table: bytes} set aside now, for `/metrics`."""
        with self.lock:
            names = {t for t, st in self.tables.items()
                     if st["state"] in ("parked", "copying")}
        got = self.store.spooled()
        return {t: got.get(t, 0) for t in sorted(names)}

    def taken(self):
        """The keys of the tables the tail carries or is taking in."""
        with self.lock:
            return frozenset(t for t, st in self.tables.items()
                             if st["state"] != "removed")

    def counts(self):
        """{state: tables in it}."""
        with self.lock:
            return dict(Counter(st["state"] for st in self.tables.values()))

    def close(self):
        """The jobs told to stop; none waited for (`_submit`)."""
        for leaf in list(self.jobs):
            self._stop_job(leaf)
