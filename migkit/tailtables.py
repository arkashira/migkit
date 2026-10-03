"""A running tail's tables, each in a state of its own (backlog L1, L4,
L5, L6; research: leapfrog live tail 2026-09-28, section 1.3).

One stream, one saved position, and a state per table beside it
(`tail-tables.json`, written in the step that writes the position):

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
that table's spool (`Spool`) before the position moves past it, so a stop
loses none; a batch read again is not spooled twice. A batch works on a
copy of the states (`begin`), and only the batch that is applied and
saved makes its states the tail's (`commit`) - a batch read again after a
lost connection finds the states it was first read under. Everything the
stream reads is accounted per table every batch - applied, left out and
why, or set aside - by the tail (`hetero._Accounts`).

What a DDL does is the hop's `ddl`:

    stop    (the default, as before) a DDL the target does not follow
            stops the whole tail before the batch; a table emptied on the
            source stops it too - it was carried by nothing before
    park    the table the DDL touched is parked, and only it; a table
            emptied on the source is copied again
    apply   what can be carried at its place in the log is: a column
            added that takes NULL or has a constant default is added on
            the target, a table emptied is emptied there at its place
            among the rows; the rest parks

Under park and apply a table dropped or renamed on the source leaves the
tail, and one a rename or a create brings in joins it with its copy;
migkit never drops a target table.
"""
import json
import os
import re
import struct
import threading
import time
from collections import Counter

STATE = "tail-tables.json"
#: the checkpoint the tail's own copies keep, in the table copier's shape
COPY = "tail-copy.json"
SPOOLS = "tail-spool"
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
WHY = (HELD, SPOOLED, OUT_OF_SCOPE)


def policy(hop):
    got = str((hop.options or {}).get("ddl") or "stop").strip().lower()
    if got not in POLICIES:
        raise SystemExit(f"ddl: {got} is not one of {', '.join(POLICIES)}"
                         " - stop the tail, park the table the DDL"
                         " touched, or apply what can be carried")
    return got


def spool_most(hop):
    try:
        return int((hop.options or {}).get("spool_limit") or SPOOL_MOST)
    except (TypeError, ValueError):
        raise SystemExit("spool_limit: the bytes a parked table's changes"
                         " may take on this machine, a whole number")


def _token_text(token):
    return json.dumps(token, sort_keys=True, default=str)


class Spool:
    """The changes set aside for one table, in the order read: frames of
    `(the position the batch was read from, [changes])`, each zstd over
    the records as the reader made them (pickled: their values keep their
    own types, which JSON would not), sealed to the hop's `at_rest`
    recipients where it names them (`evidence`), and synced to disk before
    the tail's position moves past them. A frame torn by a stop mid-write
    is the last, of a batch whose position was never saved: it is read
    again, and its frames dropped (`drop_batch`)."""

    def __init__(self, path, recipients=()):
        self.path = path
        self.recipients = list(recipients or ())

    def _seal(self, data):
        import zstandard
        data = zstandard.ZstdCompressor(level=3).compress(data)
        if self.recipients:
            from . import evidence
            data = evidence._seal(data, self.recipients)
        return data

    @staticmethod
    def _open(data):
        import zstandard

        from . import evidence
        if data.startswith(evidence.AGE):
            data = evidence._open(data)
        return zstandard.ZstdDecompressor().decompress(data)

    def frames(self):
        """[(position text, [changes])], a torn last frame left out."""
        import pickle
        try:
            raw = self.path.read_bytes()
        except OSError:
            return []
        out, at = [], 0
        while at + 8 <= len(raw):
            n = struct.unpack(">Q", raw[at:at + 8])[0]
            if at + 8 + n > len(raw):
                break
            try:
                out.append(tuple(pickle.loads(self._open(
                    raw[at + 8:at + 8 + n]))))
            except Exception:  # noqa: BLE001 - torn, the last one
                break
            at += 8 + n
        return out

    def changes(self):
        return [c for _, got in self.frames() for c in got]

    def add(self, token, changes):
        """`changes` read in the batch after `token`, appended and
        synced."""
        import pickle
        if not changes:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        blob = self._seal(pickle.dumps((_token_text(token), list(changes))))
        with open(self.path, "ab") as f:
            f.write(struct.pack(">Q", len(blob)) + blob)
            f.flush()
            os.fsync(f.fileno())

    def rewrite(self, frames):
        """The spool as `frames`, put in place whole."""
        import pickle
        frames = [(at, got) for at, got in frames if got]
        if not frames:
            self.path.unlink(missing_ok=True)
            return
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            for at, got in frames:
                blob = self._seal(pickle.dumps((at, got)))
                f.write(struct.pack(">Q", len(blob)) + blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def drop_batch(self, token):
        """The frames of the batch read after `token`, gone: a batch whose
        position was not saved is read again, and set aside again."""
        at = _token_text(token)
        frames = self.frames()
        kept = [f for f in frames if f[0] != at]
        if len(kept) != len(frames):
            self.rewrite(kept)

    def size(self):
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    def remove(self):
        self.path.unlink(missing_ok=True)


class _HopFile:
    """The hop's `exclude` and `include` as its file says them now, read
    again only when the file has changed - so a table taken out of the
    scope, or put in it, joins or leaves the running tail at the next
    batch. A hop the file does not name (made in code) is left as it is;
    what the environment or a command added to either list stays."""

    def __init__(self, hop):
        from .config import CONF
        self.hop, self.path = hop, CONF
        self.stamp = self._stamp()
        self.said = self._read()

    def _stamp(self):
        try:
            s = os.stat(self.path)
            return (s.st_mtime_ns, s.st_size)
        except OSError:
            return None

    def _read(self):
        import yaml
        try:
            with open(self.path) as f:
                raw = yaml.safe_load(f.read()) or {}
            got = (raw.get("hops") or {}).get(self.hop.name)
        except Exception:  # noqa: BLE001 - not this hop's file, then
            return None
        if not isinstance(got, dict):
            return None
        from .config import scope_patterns
        try:
            return {k: scope_patterns(got.get(k))
                    for k in ("exclude", "include")}
        except SystemExit:
            # said wrong mid-run: the scope stays as it was until the
            # file says it right
            return None

    def refresh(self):
        """Whether the hop's scope changed, its `exclude` and `include`
        set to what the file says now."""
        stamp = self._stamp()
        if stamp == self.stamp or self.said is None:
            return False
        self.stamp = stamp
        now = self._read()
        if now is None or now == self.said:
            return False
        for k in ("exclude", "include"):
            was = self.said.get(k) or []
            others = [p for p in (getattr(self.hop, k, None) or [])
                      if p not in was]
            setattr(self.hop, k, list(now[k]) + [p for p in others
                                                 if p not in now[k]])
        self.said = now
        return True


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


class TableSet:
    """The tables of one running tail, and what each is waiting for."""

    def __init__(self, pair, db, token_path, log):
        self.pair, self.db, self.log = pair, db, log
        self.hop = pair.hop
        self.src = pair.src_engine
        self.where = token_path.parent
        self.path = self.where / STATE
        self.policy = policy(self.hop)
        self.most = spool_most(self.hop)
        try:
            doc = json.loads(self.path.read_text())
        except (OSError, ValueError):
            doc = {}
        self.tables = doc.get("tables") or {}
        self.fresh = not self.tables
        self.lock = threading.RLock()
        self.file = _HopFile(self.hop)
        self.jobs, self.slots, self.ck = {}, None, None
        try:
            from . import evidence
            self.recipients = evidence.recipients_of(self.hop)
        except Exception:  # noqa: BLE001 - kept plain
            self.recipients = []
        self.scope_at = self.level_at = 0.0
        # what a job beside the stream has to tell the next batch
        self.events = []
        # what the batch in hand will do once it is saved (`commit`)
        self.pending = {}
        self._now_mark = None
        self.entries = {}
        # copies that stopped, by table, for the wait before the next try
        self.failures = Counter()

    # --- files --------------------------------------------------------------

    def spool(self, leaf):
        safe = "".join(ch if ch.isalnum() or ch in "._-" else "_"
                       for ch in leaf)
        return Spool(self.where / SPOOLS / f"{safe}.spool", self.recipients)

    def copies(self):
        """The tail's own copy checkpoint (`cli._Checkpoint` over
        `tail-copy.json`)."""
        if self.ck is None:
            from .cli import _Checkpoint
            self.ck = _Checkpoint(self.where / COPY)
        return self.ck

    def _entry(self, leaf):
        return self.pair.move_key(self.db, "", leaf)

    def save(self):
        """Written whole, before the position it goes with."""
        with self.lock:
            text = json.dumps({"tables": self.tables}, indent=1,
                              sort_keys=True, default=str)
        self.where.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(text)
        os.replace(tmp, self.path)

    def resumed(self, token):
        """The tail started at `token`: what the batch after it set aside
        before a stop is dropped - that batch is read again - and a copy
        or a catch-up that was running is started again, from its
        checkpoint."""
        for leaf, st in list(self.tables.items()):
            if st["state"] in ("parked", "copying"):
                self.spool(leaf).drop_batch(token)
            if st["state"] == "copying":
                self._start(leaf, st, fresh=False)
            if st["state"] == "removed" and "digest" not in st \
                    and st.get("why") != "dropped on the source":
                self._submit(leaf, self._digest_removed)

    def forget(self, token):
        """A batch that was not saved: what it set aside is dropped, and
        what it would have done is not done."""
        for leaf in list(self.tables):
            self.spool(leaf).drop_batch(token)
        self.pending = {}

    # --- scope, between batches -------------------------------------------

    def _scope(self, shape, changed=False):
        """{leaf: the name the log gives it} of the tables in the hop's
        scope now: `shape`'s (`drift.shape`, read under the hop's scope),
        read again where the scope has just changed."""
        from . import drift
        if shape is not None and changed:
            shape = drift.shape(self.src, "src", self.db)
        if shape is not None:
            names = [str(t) for t in shape]
        else:
            names = [str(t) for t in self.src.neutral_tables("src", self.db)
                     if not self.hop.excluded(self.db, *str(t).split("."))]
            working = drift.transient_among(names)
            names = [t for t in names if t not in working]
        return {self.pair._leaf(t): t for t in names}

    def boundary(self, token, shape, force=False):
        """Between batches, before the next is read: the hop's scope held
        to the tables' states - a table added is started on, one gone is
        removed. Returns whether the hop's scope changed, for a reader
        that read ahead under the old one to be started again."""
        self._events()
        changed = self.file.refresh()
        if not (changed or force or self.fresh or shape is not None
                or time.monotonic() - self.scope_at >= 60):
            return False
        self.scope_at = time.monotonic()
        try:
            scope = self._scope(shape, changed)
        except Exception as e:  # noqa: BLE001 - asked again next batch
            self.log(f"the source's tables could not be listed"
                     f" ({type(e).__name__}); the tail's scope is as it was")
            return False
        with self.lock:
            if self.fresh:
                for leaf, name in scope.items():
                    self.tables[leaf] = {"state": "live", "name": name}
                self.fresh = False
                self.save()
                return False
            known = {t for t, st in self.tables.items()
                     if st["state"] != "removed"}
            added = [t for t in scope if t not in known]
            gone = [t for t in sorted(known) if t not in scope]
        for leaf in gone:
            self._remove(self.tables, leaf, token,
                         "taken out of the hop's scope" if changed
                         else "no longer on the source")
        for leaf in added:
            back = (self.tables.get(leaf) or {}).get("state") == "removed"
            if changed or back or self.policy != "stop":
                self._add(self.tables, leaf, scope[leaf], token,
                          "put back in the hop's scope" if back
                          else "added to the hop's scope" if changed
                          else "made on the source")
            else:
                # as the tail always did: a table made on the source is
                # applied from its first change, and the check of its
                # shape stops the tail where the target lacks it
                with self.lock:
                    self.tables[leaf] = {"state": "live",
                                         "name": scope[leaf]}
        if gone or added:
            self._settle()
            self.save()
        return bool(changed and (gone or added))

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
            todo = [(t, dict(st)) for t, st in self.tables.items()]
        for leaf, st in todo:
            if st["state"] == "copying" and st.get("start"):
                with self.lock:
                    self.tables[leaf].pop("start", None)
                self._stop_job(leaf)
                self.spool(leaf).remove()
                self._start(leaf, st, fresh=True)
            elif st["state"] == "removed" and "said" not in st:
                self._stop_job(leaf)
                with self.lock:
                    self.tables[leaf]["said"] = True
                self.spool(leaf).remove()
                if st.get("why") != "dropped on the source":
                    self._submit(leaf, self._digest_removed)
                self.log(f"{leaf}: out of the tail from"
                         f" {str(st['at'])[:60]} - {st['why']}; its"
                         " changes are left out and counted, and its target"
                         " is left as it is")

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
                mine = [c for c in changes
                        if pair._leaf(c["table"]) == leaf]
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
        self._events()
        self._now_mark = None
        ck = self.copies()
        with ck._lock:
            self.entries = json.loads(json.dumps(
                {k: v for k, v in ck.items() if isinstance(v, dict)},
                default=str))
        self.pending = {"spools": {}, "said": []}
        with self.lock:
            return {t: dict(st) for t, st in self.tables.items()}

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

    def release(self, states):
        """(the changes set aside earlier that may be applied now, in
        their order, {table: how many}, how many were found in what a copy
        read): a copying table's held changes held to its ranges as they
        stand, a level parked table's all. A table with nothing left held
        and its copy done is live from this batch on."""
        out, gone = [], 0
        for leaf, st in sorted(states.items()):
            if st["state"] == "copying":
                frames = self.spool(leaf).frames()
                keep, went = [], []
                for at, got in frames:
                    stay = []
                    for c in got:
                        v = self._verdict(leaf, st, c)
                        if v == "held":
                            stay.append(c)
                        elif v == "apply":
                            went.append(c)
                        else:
                            gone += 1
                    keep.append((at, stay))
                if sum(len(g) for _, g in keep) != sum(len(g) for _, g
                                                       in frames):
                    self.pending["spools"][leaf] = keep
                out += went
                if not any(g for _, g in keep) and self._landed(leaf, st):
                    states[leaf] = {"state": "live", "name": st["name"]}
                    self.pending["said"].append(
                        f"{leaf}: caught up - its changes are applied as"
                        " they are read again")
            elif st["state"] == "parked" and st.get("level"):
                out += self.spool(leaf).changes()
                self.pending["spools"][leaf] = []
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
            leaf = self.pair._leaf(c["table"])
            st = states.get(leaf)
            if st is None and not self.fresh and self.policy != "stop":
                # a table the tail has not met - made on the source since
                # the scope was read - joins with its copy, its changes
                # held to it from the first
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
        for leaf, got in put.items():
            again = (self.pending.get("spools") or {}).get(leaf)
            if again is not None:
                # its spool is rewritten as the batch is saved (`commit`):
                # what this batch sets aside goes in with it
                again.append((_token_text(token), got))
            else:
                self.spool(leaf).add(token, got)
        return out, left

    def ddl(self, states, change, token, two_way=False):
        """What a DDL at its place in the batch does, by the hop's `ddl`:
        None, or ("truncate", leaf) for the target to be emptied there.
        The states the rest of the batch is routed by are changed here;
        they become the tail's when the batch is saved."""
        leaf = self.pair._leaf(change["table"])
        kind = change["kind"]
        st = states.get(leaf) or {"state": "live", "name": change["table"]}
        if st["state"] == "removed" and kind not in ("create", "rename"):
            return None
        stops = getattr(self.src, "DDL_STOPS", ("truncate",))
        if self.policy == "stop":
            # rows changed with no row event for them stop the tail
            # wherever an emptied table does
            if kind in stops or (kind == "reload" and "truncate" in stops):
                raise SystemExit(
                    f"{self.db}: {change['table']} was "
                    + {"truncate": "emptied", "drop": "dropped",
                       "rename": f"renamed to {change.get('to')}",
                       "create": "made", "alter": "altered",
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
            if st["state"] == "copying" or self.policy == "park" \
                    or two_way:
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
            self._remove(states, leaf, token,
                         f"renamed to {change.get('to')} on the source")
            to = change.get("to")
            if to and not self.hop.excluded(self.db, *str(to).split(".")):
                self._add(states, self.pair._leaf(to), str(to), token,
                          f"renamed from {change['table']} on the source")
            return None
        if kind == "create":
            if st["state"] in ("live", "removed") and (
                    leaf not in states or st["state"] == "removed"):
                self._add(states, leaf, change["table"], token,
                          "made on the source")
            return None
        return None

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

    def commit(self, states):
        """The batch applied, before its position is saved: what it
        released goes from the spools, its states become the tail's and
        are written, and what they ask for is started."""
        for leaf, frames in (self.pending.get("spools") or {}).items():
            self.spool(leaf).rewrite(frames)
        for said in self.pending.get("said") or ():
            self.log(said)
        self.pending = {}
        with self.lock:
            self.tables = states
        self._settle()
        self.save()

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
            names = [t for t, st in self.tables.items()
                     if st["state"] in ("parked", "copying")]
        return {t: self.spool(t).size() for t in names}

    def counts(self):
        """{state: tables in it}."""
        with self.lock:
            return dict(Counter(st["state"] for st in self.tables.values()))

    def close(self):
        """The jobs told to stop; none waited for (`_submit`)."""
        for leaf in list(self.jobs):
            self._stop_job(leaf)
