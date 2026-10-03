"""The mover highly available: a standby that takes a hop's move over
from the last position committed, and the proof that one machine wrote
at a time (backlog W1).

A change stream that runs for months must not stop with one machine. A
hop that says `standby: true` is run the same way on two or more machines
(`migkit move <hop> --mode full+cdc --go` on each): one holds the hop's
write lease and moves it, the others stand by, reading the lease every
third of its term; when the holder stops renewing it, one of them takes
the move over where it stood. `MIGKIT_STANDBY_SECONDS` is the term (15 by
default: renewed every 5 seconds, a machine that stops is taken over
within 20 seconds of its last renewal - a pause of the holder's shorter
than the term, a collector's or a network's, does not hand the hop
over).

What the taker does before anything else, in this order:

1. raises the hop's number on every target database (`fencing`): a batch
   the last holder had in flight commits before this or is refused after
   it, and nothing it sends later is taken;
2. reads what the last holder put in the machines' store (`shared`) and
   seals it with its own number, so a late write of the last holder's is
   refused there as well - the tail's position and batch number, its
   shape, its tables' states, the changes it set aside, the kept proof,
   the ways it chose, the copy's position - and writes them where this
   machine's move reads them, removing what this machine kept from
   before;
3. runs the move as a restart on this machine would: the copy goes on
   from its checkpoint (kept in the store as it is written, `cli`), the
   tail from its position.

Why that is exact. The holder puts those files in the store before every
transaction it commits on the target (`Holding.before`, called from
`fencing.hold`) and every two seconds besides, each time only over what
the store holds under a number no higher than its own. So the store is
never more than one batch behind the target - the batch whose commit the
push came just before - which is the one case every restart already
resolves: a batch the target committed after the position was saved is
known by the target's own mark of the last batch it committed
(`twoway.committed_ahead`), and the taker goes on after it, not from
before it. A batch applied in lanes side by side is not numbered and is
read again from the saved position and applied by key, as every restart
does; the kept proof one batch behind its position is read again, not
trusted (`kept.Keeper`). No batch twice and none lost: the promise a
restart on one machine gives, from another machine.

Who wrote what: every holding is a record of its own - its number, its
machine, the position it began from and the last one it saved - kept in
the store's ledger once the next one takes over (`epochs`); the target's
mark of its last batch names the number it was committed under
(`fencing.marked`); and `one_writer` holds the three to each other.
"""
import contextlib
import json
import os
import socket
import threading
import time
from pathlib import Path

from . import fencing, shared
from . import lease as _lease

#: files of a database's report directory, besides the tail's position
#: files (`cli.RUN_FILES`), that another machine needs to go on with its
#: move - text
MORE_TEXT = ("tail-accounts.json", "move-began.json", "dump-position.json")
#: where the files are kept in the store, per database
BUNDLE = "tail-bundle.json"
#: the holdings before the one that holds now, per database
LEDGER = "movers-ledger.json"
#: every machine of the hop with its role and when it was last heard
MOVERS = "movers.json"
#: seconds between the pushes a holder makes when nothing is committed
PUSH_EVERY = 2.0


def term():
    """The standby's term, in seconds (`MIGKIT_STANDBY_SECONDS`, 15 by
    default, 3 at the least)."""
    from . import envkeys
    return float(envkeys.number("MIGKIT_STANDBY_SECONDS"))


def standby(hop):
    return bool((hop.options or {}).get("standby"))


def mover():
    """This process, as the machines' records name it."""
    return f"{socket.gethostname()}:{os.getpid()}"


def hop_row(hop):
    """The target's row that holds the hop's number."""
    return f"{hop.name}#fence"


def _text_files(where):
    from .cli import RUN_FILES
    from .checkpoint import journal_of
    names = list(RUN_FILES) + list(MORE_TEXT)
    # the ways a restart stands on, kept beside each position
    names += sorted(p.name for p in where.glob("*-ways.json"))
    # a copy's checkpoint goes with its journal (`checkpoint.read_move`)
    names += [journal_of(where / n).name for n in names
              if n.endswith(".json")]
    return list(dict.fromkeys(names))


def _blob_files(where):
    from . import kept, tailtables
    # the tail's own state (`tailtables.Store`), and what an earlier
    # version kept in files of their own
    out = [tailtables.STORE, kept.FILE]
    spools = where / tailtables.SPOOLS
    if spools.is_dir():
        out += sorted(f"{tailtables.SPOOLS}/{p.name}"
                      for p in spools.glob("*.spool"))
    return out


def _stamp(path):
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def _blob_bytes(path):
    """A file's bytes as of one moment: a SQLite file - the tail's own
    state, the kept proof - through SQLite's own backup, which a writer
    beside it cannot tear."""
    if path.name.endswith(".sqlite"):
        import sqlite3
        src = sqlite3.connect(str(path))
        mem = sqlite3.connect(":memory:")
        try:
            src.backup(mem)
            return mem.serialize()
        finally:
            src.close()
            mem.close()
    return path.read_bytes()


def _write(path, data):
    """A run file put in place whole, and on the disk (`durable`): what
    this machine goes on from once it has taken the move over."""
    from . import durable
    durable.write_atomic(path, data)


def bundle(where, store):
    """{"files": {name: text}, "blobs": {name: digest}, "gen": the
    position's generation} of what another machine needs to go on with
    `where`'s move: the tail's position, its own state, the kept proof,
    the copy's position - the text files first, then the ones that are
    not text, each as of one moment, so a state saved after the position
    is read is at most one batch ahead of it, which the one that goes on
    takes back out (`tailtables.Store.align`). A store that keeps no
    files (`shared.Target`) takes the tail's state as text, where it holds
    nothing set aside."""
    from . import tailctl, tailtables
    files = {}
    for name in _text_files(where):
        try:
            files[name] = (where / name).read_text()
        except OSError:
            continue
    blobs = {}
    for name in _blob_files(where):
        p = where / name
        if not p.exists():
            continue
        if not store.blobs:
            text = tailtables.Store.of(where).dump() \
                if name == tailtables.STORE else None
            if text is None:
                raise SystemExit(
                    f"the tail keeps {name}, which the machines' store"
                    " cannot hold; keep the hop's run state in a bucket or"
                    " a directory every machine sees")
            files[STATE_TEXT] = text
            continue
        blobs[name] = store.put_blob(_blob_bytes(p))
    at = _position(files.get(tailctl.TOKEN))
    return {"files": files, "blobs": blobs,
            "gen": int((at or {}).get("gen") or 0)}


#: the tail's own state as text, in a store that keeps no files
STATE_TEXT = "tail-state.json"


def place(where, files, blobs, store):
    """What `bundle` kept, put where this machine's move reads it: each
    file written whole (`durable`), and this machine's own from before
    that the bundle does not hold removed - it is not the last holder's."""
    from . import tailtables
    tailtables.Store.forget(where)
    for name in dict.fromkeys(_text_files(where) + list(files)):
        if name == STATE_TEXT:
            continue
        if name in files:
            _write(where / name, files[name])
        else:
            (where / name).unlink(missing_ok=True)
    for name in _blob_files(where) + [n for n in blobs
                                      if n not in _blob_files(where)]:
        if name in blobs:
            _write(where / name, store.get_blob(blobs[name]))
        elif not (name == tailtables.STORE and STATE_TEXT in files):
            (where / name).unlink(missing_ok=True)
        if name.endswith(".sqlite"):
            for extra in ("-wal", "-shm", "-journal"):
                (where / (name + extra)).unlink(missing_ok=True)
    if STATE_TEXT in files:
        tailtables.Store.load(where, files[STATE_TEXT])


def _position(text):
    """{"batch", "token", "gen"} a tail's position holds, or None."""
    try:
        got = json.loads(text) if text else None
    except ValueError:
        return None
    if not isinstance(got, dict) or "token" not in got:
        return None
    return {"batch": got.get("batch"), "token": got.get("token"),
            "gen": got.get("gen")}


def _change(store, path, fn, sealed=True, tries=200):
    """fn(record) -> the record to write, or `_lease.KEEP`: written only
    over the version read, read again and decided again where another
    wrote first. Returns what was written, or the record kept."""
    for i in range(tries):
        have, version = store.read_record(path, sealed=sealed)
        new = fn(have)
        if new is _lease.KEEP:
            return have
        try:
            store.write_record(path, new, version, sealed=sealed)
            return new
        except shared.Taken:
            time.sleep(min(0.02 * (i + 1), 0.5))
    raise SystemExit("the machines' store kept changing under migkit while"
                     f" it wrote {Path(path).name}; too many machines write"
                     " one record at once")


# ---- the hop's holding ------------------------------------------------------

_HELD = {}
_HELD_LOCK = threading.Lock()
_PROVED = set()


def settled(where):
    """Whether the tail's last save is whole: its own state set down for a
    batch (`tailtables.Store`) no further than the position - the
    generations the two carry - and the kept proof at the position. A
    push between the two carries them one batch apart, which the one that
    goes on takes back out, where it is one batch; this keeps it so."""
    from . import kept, tailctl, tailtables
    try:
        env = tailctl.position(where / tailctl.TOKEN)
    except SystemExit:
        return False
    if env is None:
        return True
    if (where / tailtables.STORE).exists():
        st = tailtables.Store.of(where)
        if st.get("stream") == (env.get("id") or "v0") and \
                st.gen() != int(env.get("gen") or 0):
            return False
    if (where / kept.FILE).exists():
        try:
            k = kept.Store(where / kept.FILE)
            got = k.get("token")
            k.conn.close()
        except Exception:  # noqa: BLE001 - read at the next push
            return False
        if got is not None and got != kept._tok(env.get("token")):
            return False
    return True


def held(hop):
    """The holding this process has of the hop, or None."""
    with _HELD_LOCK:
        return _HELD.get(hop.name)


class Holding:
    """This process's hold on a hop with a standby: the fenced lease, the
    number armed on every write, the files pushed. `unlink()` lets go of
    one level of it; the last lets go of the lease (`cli._lock`)."""

    def __init__(self, hop, lease, store, say):
        self.hop, self.lease, self.store, self.say = hop, lease, store, say
        self.depth = 1
        self.token = None
        self.dbs = []
        self.stamps = {}
        self.epochs = {}
        self.took_over = None
        self.previous = {}
        self._push_lock = threading.Lock()
        self._stop = threading.Event()
        self._threads = []
        self.lost = None

    # the callers of `cli._lock` hold `took_over` and let go by `unlink`
    def nested(self):
        self.depth += 1
        return self

    def unlink(self):
        self.depth -= 1
        if self.depth > 0:
            return
        with _HELD_LOCK:
            if _HELD.get(self.hop.name) is self:
                _HELD.pop(self.hop.name)
        self._stop.set()
        for t in self._threads:
            t.join(timeout=10)
        try:
            if self.lost is None and self.lease.lost is None:
                for db in self.dbs:
                    self.push(db, force=True)
        except fencing.Fenced as e:
            self.lost = str(e)
        except Exception as e:  # noqa: BLE001 - said; let go all the same
            self.say(f"the last position could not be put where the"
                     f" standby reads it ({_first(e)}); it goes on from"
                     " the one before, which the target's mark completes")
        finally:
            fencing.disarm(self.hop, self.token)
            self.lease.release()
            try:
                beat(self.hop, self.store, "stopped")
            except Exception:  # noqa: BLE001 - health is said, not needed
                pass

    release = unlink

    # --- taking over ---------------------------------------------------------

    def take(self):
        """The hop's number raised on every target database, what the last
        holder saved sealed and put in place here, and every write of this
        process held to the number (the module's words, 1 and 2)."""
        from .engines import get_engine
        hop = self.hop
        eng = get_engine(hop)
        dst = getattr(eng, "dst_engine", None) or eng
        self.dbs = list(hop.databases or eng.databases() or [])
        for db in self.dbs:
            if (hop.name, db) in _PROVED:
                continue
            why = fencing.prove(dst, "dst", db)
            if why:
                raise SystemExit(
                    f"{hop.name}: standby needs a target that refuses a"
                    " write from a machine that lost the hop, and"
                    f" {db}'s does not prove it can - {why}. Nothing was"
                    " written; run one machine, without standby")
            _PROVED.add((hop.name, db))
        for _ in range(10):
            token = fencing.Token(hop_row(hop), self.lease.fence,
                                  self.lease.me, self.store.ident(),
                                  lease=self.lease)
            above = None
            for db in self.dbs:
                n, seen = fencing.raise_to(dst, "dst", db, token, self.say)
                if n == token.number and seen.get("by") == token.by:
                    continue
                if seen.get("store") and seen["store"] != token.store:
                    raise SystemExit(
                        f"{hop.name}: {seen.get('by')} holds its number on"
                        f" the target through another store"
                        f" ({seen['store']}; this machine's is"
                        f" {token.store}) - every machine of a hop must"
                        " name the same one. Nothing was written")
                above = max(above or 0, n)
            if above is None:
                break
            # the target holds a number the lease has not: its record was
            # lost, or a hop file kept it elsewhere once
            self.lease.bump(above)
        else:
            raise SystemExit(f"{hop.name}: another machine kept raising the"
                             " target's number past this one's; stop the"
                             " other runs")
        token.before = self.before
        self.token = fencing.arm(hop, token)
        for db in self.dbs:
            self.restore(db)
        for fn in (self._pushing, self._beating):
            t = threading.Thread(target=fn, daemon=True)
            t.start()
            self._threads.append(t)

    def restore(self, db):
        """What the last holder saved of `db`, sealed with this holding's
        number and put where this machine's move reads it."""
        where = self.hop.report_dir(db)
        path = where / BUNDLE
        mine = self.token.number
        got = {}

        def seal(have):
            if have and int(have.get("fence") or 0) > mine:
                raise fencing.Fenced(
                    f"{self.hop.name}: a newer holder ({have.get('by')})"
                    " sealed the saved position first; this machine stands"
                    " by again")
            got["have"] = have
            if have is None:
                return self._record(db, None)
            # the holdings before this one ride in the record until the
            # ledger has them: a machine stopped between the two loses none
            self.previous[db] = [e for e in (have.get("previous") or [])
                                 + [have.get("epoch")] if e]
            return {**have, "fence": mine, "by": self.lease.me,
                    "host": socket.gethostname(), "pid": os.getpid(),
                    "at": time.time(), "previous": self.previous[db],
                    "epoch": self._epoch(db, _position(
                        (have.get("files") or {}).get("tail-token.json")))}
        _change(self.store, path, seal)
        have = got["have"]
        if have is None:
            # nothing was saved there: a first run, and what this machine
            # holds stands - put there now, under this number
            self.push(db, force=True)
            return
        for e in self.previous.get(db) or []:
            if int(e.get("fence") or 0) < mine:
                self._ledger(db, e)
        # in the ledger now: the pushes after this carry this holding alone
        self.previous[db] = []
        files = have.get("files") or {}
        place(where, files, have.get("blobs") or {}, self.store)
        self.stamps[db] = self._stamps(where)
        taken = have.get("epoch") or {}
        self.took_over = taken or self.took_over
        at = _position(files.get("tail-token.json"))
        who = taken.get("host") or "the last holder"
        self.say(f"  {db}: going on from what {who} saved"
                 + (f" (batch {at['batch']})" if at and at.get("batch")
                    is not None else "")
                 + f", under number {mine}")

    def _epoch(self, db, first):
        got = self.epochs.get(db)
        if got is None:
            got = self.epochs[db] = {
                "fence": self.token.number if self.token else
                self.lease.fence, "by": self.lease.me,
                "host": socket.gethostname(), "pid": os.getpid(),
                "began": time.time(), "first": first, "last": first,
                "last_at": time.time()}
        return dict(got)

    def _ledger(self, db, epoch):
        path = self.hop.report_dir(db) / LEDGER

        def add(have):
            have = dict(have or {"epochs": []})
            if any(e.get("fence") == epoch.get("fence")
                   for e in have["epochs"]):
                return _lease.KEEP
            have["epochs"] = have["epochs"] + [epoch]
            return have
        _change(self.store, path, add)

    # --- keeping it where the standby reads it -------------------------------

    def _stamps(self, where):
        return tuple((n, _stamp(where / n))
                     for n in _text_files(where) + _blob_files(where))

    def _record(self, db, blobs_before):
        where = self.hop.report_dir(db)
        got = bundle(where, self.store)
        files, blobs = got["files"], got["blobs"]
        at = _position(files.get("tail-token.json"))
        epoch = self._epoch(db, at)
        epoch["last"], epoch["last_at"] = at, time.time()
        self.epochs[db] = epoch
        return {"fence": self.token.number, "by": self.lease.me,
                "host": socket.gethostname(), "pid": os.getpid(),
                "at": time.time(), "files": files, "blobs": blobs,
                "previous": self.previous.get(db) or [],
                "epoch": dict(epoch)}

    @staticmethod
    def _settled(where):
        return settled(where)

    def push(self, db, force=False, settled=False):
        """`db`'s files put in the store where they changed since the last
        push, only over a record under a number no higher than this
        holding's: `Fenced` where a newer holder sealed it. `settled`
        pushes only a whole save (`_settled`) - for the pushes between
        the tail's own, from another thread."""
        where = self.hop.report_dir(db)
        with self._push_lock:
            now = self._stamps(where)
            if not force and self.stamps.get(db) == now:
                return False
            if settled and not self._settled(where):
                return False
            record = self._record(db, None)
            mine = self.token.number

            def put(have):
                if have and int(have.get("fence") or 0) > mine:
                    raise fencing.Fenced(
                        f"{self.hop.name}: {have.get('by')} holds it now,"
                        f" under number {have.get('fence')}; this machine"
                        " writes nothing more")
                return record
            _change(self.store, where / BUNDLE, put)
            self.stamps[db] = now
            return True

    def before(self, db):
        """Ahead of every transaction this process commits on the target
        (`fencing.hold`): the position saved before it put where the
        standby reads it, so the store is never more than that one batch
        behind the target."""
        if self.lost is not None:
            raise fencing.Fenced(f"{self.hop.name}: this machine no longer"
                                 " holds the hop; nothing more is written")
        try:
            self.push(db)
        except fencing.Fenced as e:
            self._lose(e)
            raise

    def _lose(self, why):
        """Lost: every tail of this process asked to end at its next
        batch, and nothing more written."""
        from . import tailctl
        if self.lost is None:
            self.lost = str(why)
            for db in self.dbs:
                tailctl.stop(self.hop.report_dir(db))

    def _pushing(self):
        while not self._stop.wait(PUSH_EVERY):
            if self.lease.lost is not None:
                self._lose(f"{self.hop.name} was taken over while this"
                           " machine was not heard from")
                return
            for db in self.dbs:
                try:
                    self.push(db, settled=True)
                except fencing.Fenced as e:
                    self._lose(e)
                    return
                except Exception:  # noqa: BLE001 - the next turn tries again
                    continue

    def _beating(self):
        while True:
            try:
                beat(self.hop, self.store, "leading",
                     fence=self.token.number if self.token else None,
                     dbs=self.dbs)
            except Exception:  # noqa: BLE001 - health is said, not needed
                pass
            if self._stop.wait(term() / 3):
                return


def lock(hop, say):
    """The hop's write lease on a hop with a standby, for the process that
    moves it: waited for while another machine holds it - standing by -
    taken over once it lapses, and taken (`Holding.take`). Re-entrant: an
    operation inside one that holds it is handed the same holding."""
    got = held(hop)
    if got is not None:
        return got.nested()
    store = shared.store_for(hop, say)
    ls = _lease.Lease(hop.report_dir() / "lease.json", "move", remote=store,
                      fenced=True, term=term())
    said = None
    while True:
        try:
            ls.acquire()
            break
        except _lease.Held:
            try:
                have = _lease.holder(ls.path, store) or {}
            except Exception:  # noqa: BLE001 - who, is said when it answers
                have = {}
            who = (have.get("host"), have.get("pid"))
            if who != said:
                say(f"standing by: {have.get('host') or 'another machine'}"
                    + (f" process {have.get('pid')}" if have.get("pid")
                       else "")
                    + f" holds {hop.name}; this machine takes it over"
                      f" within {term():.0f}s of its last renewal")
                said = who
            try:
                beat(hop, store, "standing by")
            except Exception:  # noqa: BLE001 - health is said, not needed
                pass
            time.sleep(term() / 3)
        except Exception as e:  # noqa: BLE001 - a store out of reach
            # a machine standing by keeps standing by while the store is
            # out of reach: it takes nothing it cannot read
            if said != "unreachable":
                say(f"the machines' store did not answer ({_first(e)});"
                    " asking again")
                said = "unreachable"
            time.sleep(term() / 3)
    got = Holding(hop, ls, store, say)
    if ls.took_over:
        was = ls.took_over
        say(f"took {hop.name} over from {was.get('host')} process"
            f" {was.get('pid')}, whose lease had lapsed (number"
            f" {was.get('fence')}); this machine's is {ls.fence}")
    try:
        got.take()
    except BaseException:
        fencing.disarm(hop, got.token)
        ls.release()
        raise
    with _HELD_LOCK:
        _HELD[hop.name] = got
    if ls.took_over:
        from . import audit
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "took-over", "from": ls.took_over.get("host"),
                      "pid": ls.took_over.get("pid"),
                      "fence": got.token.number})
    return got


@contextlib.contextmanager
def leading(hop, say):
    got = lock(hop, say)
    try:
        yield got
    finally:
        got.unlink()


def run_led(hop, run, say):
    """`run()` on the machine that holds the hop, this one standing by
    until it does; where it loses the hop while it runs - taken over after
    a pause longer than its term - it stands by again."""
    while True:
        try:
            got = lock(hop, say)
        except (_lease.Lost, fencing.Fenced) as e:
            say(f"{_first(e)}; standing by again")
            time.sleep(term() / 3)
            continue
        except Exception as e:  # noqa: BLE001 - a server or store out of reach
            # taking over could not finish - a target or the store did not
            # answer - and nothing was written: stand by and try again
            say(f"could not take {hop.name} over ({_first(e)}); standing"
                " by again")
            time.sleep(term() / 3)
            continue
        try:
            out = run()
        except (_lease.Lost, fencing.Fenced) as e:
            got.lost = got.lost or str(e)
            say(f"this machine no longer holds {hop.name}: {_first(e)};"
                " standing by again")
            continue
        finally:
            got.unlink()
        if got.lost is not None or got.lease.lost is not None:
            say(f"this machine no longer holds {hop.name}; standing by"
                " again")
            continue
        return out


def _first(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[0][:300]


class _Pushed:
    """The holding's pushes, in the shape the run files' mirror has
    (`state.Mirror`), for the places a move keeps its run files where
    another machine reads them (`cli._shared`)."""

    def __init__(self, holding, db):
        self.holding, self.db = holding, db

    def fetch(self):
        # put in place when the hop was taken (`Holding.restore`)
        return self

    def push(self, force=False):
        self.holding.push(self.db, force=force)

    def start(self):
        return self

    def stop(self):
        try:
            self.holding.push(self.db, force=True)
        except fencing.Fenced as e:
            # taken over: what stopped the tail is said by the tail
            self.holding.lost = self.holding.lost or str(e)


def copied(hop, db):
    """Whether this process took `db`'s move over from a holder that had
    finished its copy: what it put where the standby reads it holds the
    copy's position (`cli._record_copy`)."""
    got = held(hop)
    return bool(got is not None and got.took_over and db and (
        hop.report_dir(db) / "copy-position.json").exists())


def mirror(hop, db):
    """Where a move of a hop this process holds keeps `db`'s run files for
    the standby: the holding's own pushes; None where it holds none."""
    got = held(hop)
    return _Pushed(got, db) if got is not None else None


#: what a way of copying must give where a standby may take the hop over
HELD = ("every write held to the number of the machine that holds the"
        " hop")


def _copiers(own, own_said):
    from .decide import Rung
    return (Rung(own, own_said),
            Rung("builtin", "the table copier every pair shares, each"
                            " write held to the number of the machine that"
                            " holds the hop", gives=frozenset({HELD})))


def copier(hop, own="own", own_said="the engine's own copier", log=None):
    """The way a hop's tables are copied where it has a standby: only one
    whose every write the target holds to the holder's number - a machine
    that lost the hop must not land a late write over the copy the next
    one made. `own` where the hop has no standby, or it gives that."""
    if not standby(hop):
        return own
    from . import decide
    got = decide.climb(f"copying {hop.name}'s tables with a standby",
                       _copiers(own, own_said), need=(HELD,))
    if got.rung is None:
        raise SystemExit(f"{hop.name}: {got.reason}")
    if got.path != own and log:
        log(f"the tables are copied by {got.said}: {got.reason}")
    return got.path


# ---- health -----------------------------------------------------------------

def beat(hop, store, role, **extra):
    """This machine's line in the hop's record of its machines: its role
    and when it was last heard. Lines not heard from for ten terms go."""
    me = mover()
    now = time.time()

    def put(have):
        have = dict(have or {})
        movers = {k: v for k, v in (have.get("movers") or {}).items()
                  if now - float(v.get("beat") or 0) < 10 * term()}
        got = movers.get(me) or {"since": now}
        movers[me] = {**got, "host": socket.gethostname(), "pid": os.getpid(),
                      "role": role, "beat": now, **extra}
        have["movers"] = movers
        return have
    _change(store, hop.report_dir() / MOVERS, put, sealed=False)


def machines(hop, store=None):
    """{mover: {"host", "pid", "role", "beat", ...}} of the machines heard
    from within three terms."""
    store = store or shared.run_state(hop)
    if store is None:
        return {}
    have, _ = store.read_record(hop.report_dir() / MOVERS)
    now = time.time()
    return {k: v for k, v in ((have or {}).get("movers") or {}).items()
            if now - float(v.get("beat") or 0) < 3 * term()}


def health(hop, db, store=None, eng=None):
    """What `watch` says of the machines moving the hop: who holds it and
    under which number, who stands by, and whether one machine wrote at a
    time. [] for a hop whose machines do not work together."""
    if not shared.together(hop):
        return []
    try:
        store = store or shared.store_for(hop)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said
        return [f"machines: the store they agree through cannot be read"
                f" ({_first(e)})"]
    out = []
    now = time.time()
    them = machines(hop, store)
    if them:
        out.append("machines: " + "; ".join(
            f"{v.get('host')} process {v.get('pid')} {v.get('role')}"
            + (f" (number {v['fence']})" if v.get("fence") else "")
            + f", heard {now - float(v.get('beat') or now):.0f}s ago"
            for _, v in sorted(them.items(),
                               key=lambda kv: kv[1].get("role") != "leading")))
    else:
        out.append("machines: none heard from within three terms")
    if standby(hop):
        try:
            got = one_writer(hop, db, store, eng)
            out.append(f"{got['status']}: {got['detail']}")
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            out.append(f"one writer at a time: not read ({_first(e)})")
    from . import claims
    out += claims.health(hop, db, store)
    return out


def plan_lines(hop, eng, db):
    """What the dry run says of the machines, where they work together:
    who is heard from and in which role, where each shared table's ranges
    stand and who copies them, and how many workers this machine would
    run as one of them - sized from its share of the servers
    (`claims.per_machine`)."""
    if not shared.together(hop):
        return []
    from . import claims, sizing
    from . import model as resource_model
    out = health(hop, db, eng=eng)
    if not claims.shared_ranges(hop):
        return out
    try:
        them = machines(hop)
    except (Exception, SystemExit):  # noqa: BLE001 - alone, then
        them = {}
    n = len(them) + (0 if mover() in them else 1)
    try:
        m = resource_model.load(hop.report_dir(db))
        sides = {s: sizing.capacity(eng, s, db) for s in ("src", "dst")}
        alone = claims.size(eng, db, None, 1, m, sides)
        shared_n = claims.size(eng, db, None, n, m, sides)
    except Exception as e:  # noqa: BLE001 - said
        return out + [f"this machine's workers: not worked out"
                      f" ({_first(e)})"]
    out.append(f"this machine: {shared_n} ranges at once as one of {n}"
               f" machine{'s' if n != 1 else ''} sharing the servers"
               f" ({alone} alone) - each server's rate divided among the"
               " machines, this machine's own CPUs and memory whole")
    return out


# ---- who wrote what ---------------------------------------------------------

def epochs(hop, db, store=None):
    """Every holding of `db`'s move there was, oldest first: {"fence",
    "by", "host", "pid", "began", "first", "last", "last_at"} - `first`
    the position it went on from, `last` the last it saved - the one
    holding now last."""
    store = store or shared.run_state(hop)
    if store is None:
        return []
    ledger, _ = store.read_record(hop.report_dir(db) / LEDGER, sealed=True)
    out = [e for e in (ledger or {}).get("epochs") or []
           if isinstance(e, dict)]
    now, _ = store.read_record(hop.report_dir(db) / BUNDLE, sealed=True)
    for e in ((now or {}).get("previous") or []) + [(now or {}).get("epoch")]:
        if isinstance(e, dict) and not any(
                x.get("fence") == e.get("fence") for x in out):
            out.append(e)
    return sorted(out, key=lambda e: int(e.get("fence") or 0))


def _batch(pos):
    try:
        return int(pos["batch"]) if pos and pos.get("batch") is not None \
            else None
    except (TypeError, ValueError):
        return None


def judge(held_epochs, target=None, mark=None):
    """The proof that one machine wrote at a time, from what was kept:
    (`two`, [what says so]) - `two` False where it holds.

    * numbers only rise: a number held twice is two holdings under one;
    * each holding goes on from the position the one before it saved
      last: an earlier batch is batches applied twice, a later one is
      batches never applied;
    * the target holds the newest number (`target`, what it holds): a
      lower one means the newest holding's writes were never held to it;
    * the target's mark of its last batch (`mark`, its "batch" and
      "fence") was committed under the holding whose batches those are.
    """
    said = []
    seen = set()
    for e in held_epochs:
        n = int(e.get("fence") or 0)
        if n in seen:
            said.append(f"number {n} was held twice")
        seen.add(n)
    for a, b in zip(held_epochs, held_epochs[1:]):
        last, first = _batch(a.get("last")), _batch(b.get("first"))
        if last is None or first is None:
            if (a.get("last") or {}).get("token") != (
                    b.get("first") or {}).get("token") and a.get("last") \
                    and b.get("first"):
                said.append(
                    f"number {b.get('fence')} went on from"
                    f" {str((b.get('first') or {}).get('token'))[:60]},"
                    f" not from where number {a.get('fence')} saved last"
                    f" ({str((a.get('last') or {}).get('token'))[:60]})")
            continue
        if first < last:
            said.append(f"number {b.get('fence')} went on from batch"
                        f" {first}, before batch {last} number"
                        f" {a.get('fence')} had saved: batches"
                        f" {first + 1} to {last} applied twice")
        elif first > last:
            said.append(f"number {b.get('fence')} went on from batch"
                        f" {first}, after batch {last} number"
                        f" {a.get('fence')} saved last: batches"
                        f" {last + 1} to {first} were never saved by it")
    newest = int(held_epochs[-1].get("fence") or 0) if held_epochs else None
    if target is not None and newest is not None and target < newest:
        said.append(f"the target holds number {target}, below the newest"
                    f" holding's {newest}: that holding's writes were not"
                    " held to it")
    if mark and mark.get("fence") is not None and held_epochs:
        b = _batch(mark)
        owner, before = None, None
        for e in held_epochs:
            first = _batch(e.get("first"))
            if first is None or b is None or b > first:
                owner, before = e, owner
        allowed = {int(owner.get("fence") or 0)} if owner else set()
        if owner is not None and before is not None \
                and _batch(owner.get("first")) is not None \
                and b == _batch(owner.get("first")) + 1:
            # the batch the last holding committed after the position it
            # saved last: the next one found it on the target and went on
            # after it (`twoway.committed_ahead`)
            allowed.add(int(before.get("fence") or 0))
        if owner is not None and b is not None \
                and int(mark["fence"]) not in allowed:
            said.append(f"the target's last batch ({b}) was committed under"
                        f" number {mark['fence']}, and those batches are"
                        f" number {owner.get('fence')}'s")
    return bool(said), said


def one_writer(hop, db, store=None, eng=None):
    """{"status", "detail", "epochs"}: whether one machine wrote `db` at a
    time, from the ledger, the target's number and its last batch's mark
    (`judge`) - "two writers" with what says so, where it does not."""
    store = store or shared.store_for(hop)
    held_epochs = epochs(hop, db, store)
    target, mark = None, None
    try:
        if eng is None:
            from .engines import get_engine
            eng = get_engine(hop)
        dst = getattr(eng, "dst_engine", None) or eng
        target, _ = fencing.read(dst, "dst", db, hop_row(hop))
        seen = getattr(dst, "origin_seen", None)
        if seen is not None:
            # the hop's own row, which numbered batches move
            # (`twoway.thread_origin`), not this thread's
            dst._numbered = True
            mark = seen("dst", db)
    except Exception:  # noqa: BLE001 - judged on what was kept
        pass
    two, said = judge(held_epochs, target, mark)
    if two:
        return {"status": "two writers", "detail": "; ".join(said),
                "epochs": held_epochs}
    n = len(held_epochs)
    return {"status": "one writer at a time",
            "detail": (f"{n} holding{'s' if n != 1 else ''}, numbers "
                       + ", ".join(str(e.get("fence")) for e in held_epochs)
                       + (f"; the target holds {target}" if target is not None
                          else "")
                       + ("; each went on from where the one before saved"
                          " last" if n > 1 else "")) if n else
            "no holding recorded yet",
            "epochs": held_epochs}


def evidence(hop):
    """Rows of who wrote what, for the evidence export: each holding of a
    database's move with the positions it went from and to, each range
    of a shared table with the machine and number it was copied under,
    and each database's proof of one writer."""
    if not shared.together(hop):
        return []
    try:
        store = shared.store_for(hop)
    except (Exception, SystemExit):  # noqa: BLE001 - no store, nothing kept
        return []
    from . import claims
    out = []
    root = Path(hop.report_dir())
    dbs = list(hop.databases or []) or sorted(
        p.name for p in root.iterdir() if p.is_dir() and (
            (p / "tail-token.json").exists() or (p / "move.json").exists()))
    for db in dbs:
        try:
            held_epochs = epochs(hop, db, store)
        except Exception:  # noqa: BLE001 - nothing kept for it
            held_epochs = []
        for e in held_epochs:
            first, last = e.get("first") or {}, e.get("last") or {}
            what = (f"batches {first.get('batch')} to {last.get('batch')}"
                    if first.get("batch") is not None else "the change"
                    " stream")
            out.append({"db": db, "what": what,
                        "by": f"{e.get('host')} process {e.get('pid')}",
                        "number": e.get("fence"),
                        "from": str(first.get("token"))[:80],
                        "to": str(last.get("token"))[:80],
                        "began": e.get("began"), "last": e.get("last_at")})
        if held_epochs and standby(hop):
            try:
                got = one_writer(hop, db, store)
                out.append({"db": db, "what": got["status"],
                            "by": "", "number": None,
                            "from": "", "to": got["detail"][:300]})
            except (Exception, SystemExit):  # noqa: BLE001 - not proved
                pass
        out += claims.evidence(hop, db, store)
    return out
