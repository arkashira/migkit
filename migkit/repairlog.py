"""A statement-level record of a repair, beside its undo (problems C3,
backlog C3.repair).

A row repair used to leave an undo file of the rows it overwrote, and
nothing that said which of its writes had landed when it stopped: a
repair killed half way was "some of it, perhaps". Every row repair now
writes a record, one line a statement, before it writes anything:

    {"record": 1, "table": ..., "form": ..., "columns": ..., "key": ...}
    {"n": 1, "op": "write", "key": [...], "before": [...] | null,
     "after": [...]}
    {"n": 2, "op": "delete", "key": [...], "before": [...], "after": null}
    {"applied": 1}
    {"applied": 2}

`before` is the row on the target before the repair (null where it had
none), `after` the row the repair puts there (null for a delete). An
`applied` line is written once the statements up to it have landed, so
after a stop the record says what had been applied, and which statement
was in flight.

**Reversible, and never over a newer row.** `reverse` puts each row back
as `before` - only where the target still holds what the repair wrote
(`after`). A row that already reads as `before` was never written, or is
back already; a row that is neither was changed since, and is left alone
and named. So a statement in flight when the repair stopped is reversed
if it landed and left if it did not, with no guess about which.
`replay` is the same the other way: `after` put where the row still reads
as `before`.

**Forms**, each put back by what the engine reads and writes rows with
(`io_for`): `canon`, a row as canonical text by its columns' classes,
through the neutral contract every table engine speaks (`neutral_rows_by_
key`, `neutral_write`, `_apply_delete`); `document`, a MongoDB document
as Extended JSON; `dump`, a Redis key as its serialised value and expiry;
`snapshot`, a whole table copied again (`repairways`), one statement
whose `before` is the table's digest and the file its rows were kept in
before it was replaced, and whose `after` is the digest it read once
the copy landed (`landed`) - put back only where the table still reads
as that. The record is written through the hop's report directory,
under `undo/`, so a hop that keeps its evidence encrypted (`at_rest`)
keeps this too. Rows put back are written as the repair wrote them:
inside the target engine's load window (`Engine.load_window`), so a
rollback follows the one rule for the target's triggers and foreign keys
the load and the repair follow.
"""
import json
import re
import time

#: a record file's name, under the database's `undo/`
SUFFIX = ".repair.jsonl"
#: this process's repairs: the records one `sync --apply` wrote carry it,
#: so a rollback with no saved state knows which were the last run's
RUN = f"{time.strftime('%Y%m%d-%H%M%S')}-{time.time_ns() % 10 ** 6:06d}"


def _name(table):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(table))[:120]


def _synced(handle):
    """What was written to `handle` on the disk: an undo journal is
    written ahead of what it undoes, and one a power loss took with it
    undoes nothing (audit ST12)."""
    import os
    handle.flush()
    os.fsync(handle.fileno())


class Record:
    """One table's repair, written as it goes - on the disk before what it
    records lands (`ready`, `applied`, `landed`)."""

    def __init__(self, undo_dir, table, form, columns=(), key=()):
        from . import durable
        durable.private_dir(undo_dir)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        self.path = undo_dir / f"{stamp}-{time.time_ns() % 10 ** 9:09d}-" \
                               f"{_name(table)}{SUFFIX}"
        self.n = 0
        self._handle = self.path.open("a")
        durable.sync_dir(undo_dir)
        self._line({"record": 1, "table": str(table), "form": form,
                    "columns": [list(c) if isinstance(c, (list, tuple))
                                else c for c in columns],
                    "key": list(key), "at": time.strftime("%F %T"),
                    "run": RUN})

    def _line(self, entry):
        self._handle.write(json.dumps(entry, default=str) + "\n")

    def planned(self, op, key, before, after):
        """A statement the repair is about to apply; its number."""
        self.n += 1
        self._line({"n": self.n, "op": op, "key": list(key),
                    "before": before, "after": after})
        return self.n

    def ready(self):
        """Every statement planned, on the disk before the first lands."""
        _synced(self._handle)

    def applied(self, upto):
        """The statements up to `upto` have landed."""
        if upto:
            self._line({"applied": int(upto)})
            _synced(self._handle)

    def landed(self, n, after):
        """What statement `n` left on the target, read once it landed,
        where only then can it be known: a table copied again whole reads
        as the source did while it was copied, not as it did when the
        statement was planned."""
        self._line({"landed": int(n), "after": after})
        _synced(self._handle)

    def close(self):
        self._handle.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def read(path):
    """(header, [statement], statements applied up to) of one record.

    A last line without its newline is a write a crash cut - a statement
    planned, or an `applied` line, that never finished and that nothing
    acted on: left out, and the record reads as it stood before it. It
    used to stop every rollback of the record (audit ST12). A line that
    is not a record anywhere else stops, naming the file (`durable`)."""
    from . import durable
    head, out, upto = {}, [], 0
    lines = path.read_text().split("\n")
    # the part after the last newline: "" where the file ends whole, else
    # the torn line
    lines = lines[:-1]
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            raise durable._torn(path, "journal",
                                f"line {n} is not a record") from None
        if "record" in entry:
            head = entry
        elif "applied" in entry:
            upto = max(upto, int(entry["applied"]))
        elif "landed" in entry:
            at = int(entry["landed"]) - 1
            if 0 <= at < len(out):
                out[at]["after"] = entry["after"]
        else:
            out.append(entry)
    return head, out, upto


def records(undo_dir, since=None):
    """The record files under `undo_dir`, newest first; only those written
    at or after `since` (a `YYYYmmdd-HHMMSS` stamp, or a state's
    timestamp in any form holding its digits) where given."""
    if not undo_dir.exists():
        return []
    got = sorted(undo_dir.glob(f"*{SUFFIX}"), reverse=True)
    if since:
        digits = re.sub(r"\D", "", str(since))[:14]
        got = [p for p in got
               if re.sub(r"\D", "", p.name[:15])[:14] >= digits]
    return got


def to_reverse(undo_dir, since=None):
    """What a rollback reverses, newest first: the records written since
    the saved state it goes back to, or - with no state - the last run's."""
    got = records(undo_dir, since)
    if since or not got:
        return got
    last = read(got[0])[0].get("run")
    return [p for p in got if read(p)[0].get("run") == last]


def stopped(undo_dir):
    """The records whose repair stopped before every statement landed:
    [(path, applied, planned)]."""
    out = []
    for p in records(undo_dir):
        _, stmts, upto = read(p)
        if stmts and upto < len(stmts):
            out.append((p, upto, len(stmts)))
    return out


def said(path):
    """One line about a record: its table, statements and how far it got."""
    head, stmts, upto = read(path)
    ops = {}
    for s in stmts:
        ops[s["op"]] = ops.get(s["op"], 0) + 1
    what = ", ".join(f"{n} {op}s" for op, n in sorted(ops.items()))
    state = ("all applied" if stmts and upto >= len(stmts) else
             f"stopped after {upto} of {len(stmts)}" if stmts else "empty")
    return f"{head.get('table', '?')}: {what or 'nothing'} ({state}) - {path}"


# --- putting rows back ------------------------------------------------------

def value(cls, text):
    """A value back from its canonical text (`canon.render_value`), as a
    driver takes it for a column of that class: bytes from their hex, the
    numbers as numbers, true or false; every other class as the text,
    which is the form each engine's own column reads (an ISO duration, a
    timestamp in UTC, a JSON document)."""
    from decimal import Decimal

    from . import canon
    if text is None:
        return None
    if cls == "bytes":
        return bytes.fromhex(text)
    if cls == "integer":
        return int(text)
    if cls == "float":
        return float(text)
    if cls in ("decimal", canon.NUMBER):
        return Decimal(text)
    if cls == "boolean":
        return text == "1"
    return text


class CanonIO:
    """A table's rows as canonical text, read and written through an
    engine's neutral contract on one side."""

    def __init__(self, engine, side, db, table, columns, key):
        self.eng, self.side, self.db, self.table = engine, side, db, table
        self.columns = [tuple(c) for c in columns]
        self.key = list(key)

    def _values(self, texts):
        return [value(c, t) for (_, c), t in zip(self.columns, texts)]

    def _key_values(self, key_texts):
        cls = dict(self.columns)
        return tuple(value(cls[k], t) for k, t in zip(self.key, key_texts))

    def rows(self, keys):
        """{key text tuple: [column texts]} as the side holds them now."""
        from . import canon
        if not keys:
            return {}
        got = self.eng.neutral_rows_by_key(
            self.side, self.db, self.table, self.columns, self.key,
            [self._key_values(k) for k in keys])
        return {tuple(k): [canon.render_value(c, v)
                           for (_, c), v in zip(self.columns, row)]
                for k, row in got.items()}

    def window(self):
        """Where rows are put back: the target engine's load window."""
        return self.eng.load_window(self.db, None, {self.table})

    def put(self, key, row):
        self.eng.neutral_write(self.side, self.db, self.table, self.columns,
                               [self._values(row)])

    def delete(self, key_texts):
        from . import canon
        cls = dict(self.columns)
        into = {k: c for k, c in cls.items() if canon.has_own_form(c)}
        values = self._key_values(key_texts)
        self.eng._apply_delete(self.side, self.db, self.table, {
            k: canon.write_value(self.eng.CANON_ENGINE, into[k], v)
            if k in into else v for k, v in zip(self.key, values)})


def planned(undo_dir, target, after, writes, changed=(), deletes=()):
    """A table's repair written down whole before any of it lands: a
    `Record` of every key it writes - a write where the source still has
    the row (`after`, {key: texts}), a delete where it is a changed row the
    source no longer has - and every key it deletes, each with the row the
    target (`target`, a `CanonIO`) holds now. Keys are canonical text."""
    writes, deletes = [tuple(k) for k in writes], [tuple(k) for k in deletes]
    changed = {tuple(k) for k in changed}
    before = target.rows(writes + deletes)
    record = Record(undo_dir, target.table, "canon", target.columns,
                    target.key)
    for k in writes:
        if k in after:
            record.planned("write", k, before.get(k), after[k])
        elif k in changed:
            record.planned("delete", k, before.get(k), None)
    for k in deletes:
        record.planned("delete", k, before.get(k), None)
    record.ready()
    return record


def canon_keys(columns, key, rows):
    """Key values as an engine's own text of them (a key file's lines),
    as canonical text."""
    from . import canon
    cls = dict(columns)
    return [tuple(canon.render_value(cls[k], canon.from_text(
        cls[k], None if v is None else str(v))) for k, v in zip(key, row))
        for row in rows]


def doc_text(doc):
    """A MongoDB document or `_id` as canonical Extended JSON: every value
    with its BSON type, so an int32 and an int64 put back are what they
    were."""
    from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
    return dumps(doc, json_options=CANONICAL_JSON_OPTIONS)


class DocumentIO:
    """A collection's documents by `_id`, as canonical Extended JSON."""

    def __init__(self, collection, table):
        self.coll, self.table = collection, table

    def rows(self, keys):
        from bson.json_util import loads
        ids = [loads(k[0]) for k in keys]
        out = {}
        for i in range(0, len(ids), 1000):
            for doc in self.coll.find({"_id": {"$in": ids[i:i + 1000]}}):
                out[(doc_text(doc["_id"]),)] = doc_text(doc)
        return out

    def put(self, key, row):
        from bson.json_util import loads
        doc = loads(row)
        self.coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    def delete(self, key):
        from bson.json_util import loads
        self.coll.delete_one({"_id": loads(key[0])})


class DumpIO:
    """A Redis database's keys as their serialised value and expiry
    (`{"dump": base64, "pttl": ms}`), put back with RESTORE ... REPLACE."""

    def __init__(self, client, table):
        self.client, self.table = client, table

    def rows(self, keys):
        import base64
        out = {}
        for k in keys:
            payload = self.client.dump(k[0].encode())
            if payload is None:
                continue
            ttl = self.client.pttl(k[0].encode())
            out[tuple(k)] = {"dump": base64.b64encode(payload).decode(),
                             "pttl": ttl if ttl and ttl > 0 else 0}
        return out

    @staticmethod
    def same(have, want):
        """One value, whatever is left of its expiry: the time to live
        counts down between two readings of the same key."""
        if have is None or want is None:
            return have is want
        return have["dump"] == want["dump"]

    def put(self, key, row):
        import base64
        self.client.restore(key[0].encode(), int(row.get("pttl") or 0),
                            base64.b64decode(row["dump"]), replace=True)

    def delete(self, key):
        self.client.delete(key[0].encode())


def canon_rows(columns, rows):
    """Rows of values as the record keeps them: canonical text a column."""
    from . import canon
    return [[canon.render_value(c, v) for (_, c), v in zip(columns, r)]
            for r in rows]


#: rows a snapshot of a table is read and put back in at once
SNAPSHOT_BATCH = 5000


def snapshot_name(path):
    """The file a table's snapshot is kept in, beside `path` (a record's
    own name, so the two go together)."""
    return path.name[:-len(SUFFIX)] + ".snapshot.jsonl"


def snapshot(engine, db, table, columns, key, path):
    """Every row the target holds in `table` written to `path` as
    canonical text, one JSON list a line, read in key order where the
    table has a key and in one pass where it has none. Returns the rows
    kept."""
    kept = 0
    with path.open("a") as out:
        for rows in _all_rows(engine, "dst", db, table, columns, key):
            for row in canon_rows(columns, rows):
                out.write(json.dumps(row) + "\n")
            kept += len(rows)
        # on the disk before the table it holds is replaced
        _synced(out)
    return kept


def _all_rows(engine, side, db, table, columns, key):
    """A side's whole table in batches: by key where it resumes by key,
    through the engine's batched read otherwise."""
    if key and getattr(engine, "RESUMES_BY_KEY", True):
        after = None
        while True:
            rows, after = engine.neutral_read(side, db, table, columns,
                                              after, SNAPSHOT_BATCH)
            if rows:
                yield rows
            if not rows or after is None:
                return
    yield from engine.neutral_batches(side, db, table, columns,
                                      SNAPSHOT_BATCH)


def table_digest(engine, side, db, table, columns):
    """A whole table's (count, digest) as JSON keeps it."""
    n, d = engine.neutral_digest(side, db, table, columns)
    return [int(n), str(d)]


class TableIO:
    """A whole table on the target as one statement (`snapshot` form): its
    digest is what it holds, and what it held before it was copied again
    is put back from the snapshot kept beside the record - emptied, then
    written from the snapshot's rows."""

    def __init__(self, engine, db, table, columns, key, undo_dir):
        self.eng, self.db, self.table = engine, db, table
        self.columns = [tuple(c) for c in columns]
        self.key, self.undo_dir = list(key), undo_dir

    def window(self):
        return self.eng.load_window(self.db, None, {self.table})

    def rows(self, keys):
        return {tuple(k): {"digest": table_digest(
            self.eng, "dst", self.db, self.table, self.columns)}
            for k in keys}

    @staticmethod
    def same(have, want):
        if have is None or want is None:
            return have is want
        return list(have["digest"]) == list(want["digest"])

    def put(self, key, row):
        if not row.get("snapshot"):
            raise SystemExit(
                f"{self.table}: a table copied again whole is not replayed"
                " from its record - run the repair again, which copies it"
                " from the source as it is now")
        path = self.undo_dir / row["snapshot"]
        if not path.exists():
            raise SystemExit(f"{self.table}: the rows it held before it was"
                             f" copied again were kept in {path}, which is"
                             " gone - nothing was put back")
        self.eng.neutral_empty("dst", self.db, self.table)
        batch = []
        with path.open("r") as f:
            for line in f:
                if not line.strip():
                    continue
                batch.append([value(c, t) for (_, c), t in
                              zip(self.columns, json.loads(line))])
                if len(batch) >= SNAPSHOT_BATCH:
                    self.eng.neutral_write("dst", self.db, self.table,
                                           self.columns, batch)
                    batch = []
        if batch:
            self.eng.neutral_write("dst", self.db, self.table, self.columns,
                                   batch)
        got = table_digest(self.eng, "dst", self.db, self.table,
                           self.columns)
        if list(got) != list(row["digest"]):
            was = row["digest"]
            raise SystemExit(
                f"{self.table}: put back from its snapshot, it reads as"
                f" {got[0]:,} rows (digest {got[1]}) and held {was[0]:,}"
                f" (digest {was[1]}) before it was copied again - the"
                " snapshot did not bring it back exactly")

    def delete(self, key):
        raise SystemExit(f"{self.table}: a table copied again whole is put"
                         " back from its snapshot, never removed")


def _turn(path, io, forward, log=None):
    """`reverse` (forward False) or `replay` (forward True) of one record
    through `io`: (put back, already so, left because changed since)."""
    import contextlib
    head, stmts, _ = read(path)
    if not stmts:
        return 0, 0, []
    order = stmts if forward else list(reversed(stmts))
    window = getattr(io, "window", None)
    with window() if window else contextlib.nullcontext():
        done, same, left = _turned(order, io, forward)
    if log:
        how = "reversed" if not forward else "replayed"
        log(f"{head.get('table', '?')}: {done} statements {how}, {same}"
            " already so"
            + (f", {len(left)} left alone - changed since the repair:"
               f" {', '.join('/'.join(map(str, k)) for k in left[:4])}"
               if left else ""))
    return done, same, left


def _turned(order, io, forward):
    now = io.rows([tuple(s["key"]) for s in order])
    done, same, left = 0, 0, []
    for s in order:
        want, was = ((s["after"], s["before"]) if forward
                     else (s["before"], s["after"]))
        have = now.get(tuple(s["key"]))
        alike = getattr(io, "same", None) or (lambda x, y: x == y)
        if alike(have, want):
            same += 1
            continue
        if not alike(have, was):
            left.append(s["key"])
            continue
        if want is None:
            io.delete(s["key"])
        else:
            io.put(s["key"], want)
        now[tuple(s["key"])] = want
        done += 1
    return done, same, left


def reverse(path, io, log=None):
    """Every row of the record put back as it was before the repair, where
    the target still holds what the repair wrote."""
    return _turn(path, io, False, log)


def replay(path, io, log=None):
    """Every row of the record put as the repair left it, where the target
    still holds what it held before the repair."""
    return _turn(path, io, True, log)


def io_for(eng, db, path):
    """The way to read and write the record's rows on the target, from the
    engine (`Engine.repair_io`)."""
    head, _, _ = read(path)
    return eng.repair_io(db, head)
