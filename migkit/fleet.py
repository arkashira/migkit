"""One table's ranges across machines, as the machine that takes a hop
over finds them, and the proof once the table is done that every range
was copied once and none was lost (backlog W1, LT.P1).

The ranges are handed out by `claims` - one record every machine writes
only over the version it read, kept where the hop's machines agree
(`shared`: the bucket, a shared directory or the target's own
`migkit_origin`, chosen from the hop file and proved) - and every claim
carries a number of its own, one higher than any before it, which the
target holds the range's writes to (`fencing`). The hop itself is taken
over by its lease's number (`lease.Lease.fence`, `ha`). What this module
adds is what the taker does with the ranges of the one it took over from:

    departed    every claim of a mover whose lease was taken over put
                back at once - not after its own term - since that mover
                holds nothing any more; the next claim of the range has a
                higher number, its first write raises the range's row on
                the target and empties its span, and anything the old
                mover still sends under its number is refused there
    reconcile   the table's record read with what the departed mover's
                ledger left open (`attempts.Ledger`, read only): each
                range done (kept - done is accepted only from the claim
                that held it, after the range was read back), taken back,
                held by a machine still in its term, or left to copy. The
                machines' record stays the truth: where the ledger says a
                range closed and the record does not hold it done, the
                record stands, the range is copied again and the conflict
                is said and audited
    kept        a mover's ledger, the part a taker needs - its range units
                and the operations it left uncertain - put in the machines'
                store with the hop's saved position (`ha`), so the machine
                that takes the hop over reads the ledger of the one that
                stopped, wherever that ran, not only its own (`Departed`)
    audit       the record of a table said done, read for what would
                mean a range copied twice or lost: spans that leave a gap
                or overlap, a range with no state or a state with no
                range, a range done under a number lower than a later
                claim of it, two ranges done under one number, a table
                finished with a range not done
"""
import dataclasses
import re
import time

from . import claims

#: the files a mover keeps its ledger in, beside the hop's reports
LEDGER_FILES = "attempts*.json"


def mover_of(took_over):
    """The name the machines' records give the mover whose lease record
    is `took_over` (`ha.mover`): None where the record names nobody."""
    if not isinstance(took_over, dict) or not took_over.get("host"):
        return None
    return f"{took_over['host']}:{took_over.get('pid')}"


def unit_name(table, after):
    """The ledger's name for a range of `table` a mover copies, opened
    with `level="range", parent=table`."""
    return f"{table} r:{int(after)}"


def parse_unit(name):
    """(table, after) of a ledger unit `unit_name` made, else None."""
    got = re.match(r"(.+) r:(-?\d+)$", str(name))
    return (got.group(1), int(got.group(2))) if got else None


def opened(ledger, table):
    """{after or name: the unit's record} of the ranges of `table` the
    ledger has open - begun and never closed - or closed failing (owed
    again), and the operations it keeps uncertain that name the table.
    Read only."""
    out = {}
    for name, rec in dict(ledger.records).items():
        if rec.get("level") != "range" or rec.get("parent") != table \
                or rec.get("state") not in ("open", "failed"):
            continue
        got = parse_unit(name)
        out[got[1] if got and got[0] == table else name] = dict(rec)
    for key, rec in ledger.uncertain().items():
        if rec.get("table") == table:
            after = rec.get("after")
            out.setdefault(int(after) if after is not None else key,
                           dict(rec))
    return out


def ledger_files(where):
    """The ledger files a mover keeps in the report directory `where`."""
    from pathlib import Path
    where = Path(where)
    return sorted(where.glob(LEDGER_FILES)) if where.is_dir() else []


def kept(where):
    """{"at", "ledgers": {file: {"units": the range units, "operations":
    the uncertain ones}}, "unread": [files]} of the ledgers in `where`:
    what a taker of the hop needs of them, small enough to ride in the
    record of the hop's saved position. A ledger that cannot be read
    (torn, with no snapshot) is named in `unread`, not left out."""
    from . import attempts
    from .durable import Torn
    out, unread = {}, []
    for path in ledger_files(where):
        try:
            led = attempts.Ledger(path)
        except (Torn, OSError, ValueError):
            unread.append(path.name)
            continue
        out[path.name] = {
            "units": {n: dict(r) for n, r in dict(led.records).items()
                      if r.get("level") == "range"},
            "operations": led.uncertain()}
    return {"at": time.time(), "ledgers": out, "unread": unread}


class Departed:
    """A mover's ledger as the machines' store kept it (`kept`), read the
    way `opened` and `conflicts` read a ledger: `records`, `uncertain()`
    and `range_units(table)`. Read only; the unit a later opening made
    wins where two files name one."""

    def __init__(self, units=None, operations=None, unread=(), at=None):
        self.records = dict(units or {})
        self.operations = dict(operations or {})
        self.unread = tuple(unread)
        self.at = at

    @classmethod
    def of(cls, *saved):
        """The ledgers of every `kept` given (None and a record that holds
        none are skipped), joined."""
        out = cls()
        for got in saved:
            if not isinstance(got, dict):
                continue
            out.unread += tuple(got.get("unread") or ())
            out.at = max(filter(None, (out.at, got.get("at"))), default=None)
            for led in (got.get("ledgers") or {}).values():
                out._take(led.get("units"), led.get("operations"))
        return out

    def _take(self, units, operations):
        for name, rec in dict(units or {}).items():
            old = self.records.get(name)
            if old is None or float(rec.get("started") or 0) >= \
                    float(old.get("started") or 0):
                self.records[name] = dict(rec)
        for key, rec in dict(operations or {}).items():
            self.operations.setdefault(key, dict(rec))

    def joined(self, ledger):
        """This view and `ledger` (an `attempts.Ledger`, or None), the one
        a taker reads: what the stopped mover kept and what this machine's
        own ledger holds."""
        out = Departed(self.records, self.operations, self.unread, self.at)
        if ledger is not None:
            out._take(getattr(ledger, "records", None),
                      ledger.uncertain() if hasattr(ledger, "uncertain")
                      else None)
        return out

    def empty(self):
        return not self.records and not self.operations

    def uncertain(self):
        return {k: dict(v) for k, v in self.operations.items()}

    def range_units(self, table):
        out = {}
        for name, rec in dict(self.records).items():
            got = parse_unit(name)
            if got and got[0] == table and rec.get("level") == "range":
                out[got[1]] = dict(rec)
        return out


def departed(rec, gone, now):
    """The record with every claim of the movers in `gone` put back,
    claimable at once, each kept in `lapsed` with why."""
    import copy
    rec = copy.deepcopy(rec)
    gone = {g for g in gone if g}
    for a, _ in rec["ranges"]:
        st = rec["state"][str(a)]
        if st["state"] != claims.CLAIMED or st.get("by") not in gone:
            continue
        rec["lapsed"].append({"after": a, "by": st.get("by"),
                              "host": st.get("host"),
                              "number": st.get("number"), "at": now,
                              "why": "its mover's lease was taken over"})
        st.update(state=claims.TODO)
        for k in ("expires", "mark"):
            st.pop(k, None)
    return rec


@dataclasses.dataclass
class Account:
    """What a taker found of one table's ranges."""
    done: list
    taken_back: list
    held: list
    todo: list
    #: ranges (or units) the departed mover's ledger had open
    uncertain: dict
    #: where the ledger and the machines' record disagree on a range
    conflicts: list = dataclasses.field(default_factory=list)

    def lines(self, table):
        out = [f"{table}: {len(self.done)} ranges done and kept,"
               f" {len(self.taken_back)} taken back from the mover that"
               f" stopped, {len(self.held)} held by machines still in"
               f" their term, {len(self.todo)} left to copy"]
        settled = sorted(k for k in self.uncertain if k in self.done)
        if settled:
            out.append(f"{table}: {len(settled)} ranges the stopped mover"
                       " left open are done in the machines' record, read"
                       " back by the claim that held them: kept")
        loose = sorted(str(k) for k in self.uncertain
                       if not isinstance(k, int))
        if loose:
            out.append(f"{table}: work the stopped mover left open names no"
                       f" range ({', '.join(loose)}); every range not done"
                       " is copied again from an emptied span")
        out += [f"{table}: {c}" for c in self.conflicts]
        return out


def conflicts(rec, ledger, table):
    """What the machines' record `rec` of `table` and the ledger of the
    mover that stopped say differently of a range: the ledger closed it
    done and the record does not hold it done, or the ledger names a range
    the record has none of. The record stands in both - the range is copied
    again from an emptied span - and each is said so it is audited."""
    units = getattr(ledger, "range_units", None)
    if not callable(units) or not rec or rec.get("ranges") is None:
        return []
    out = []
    state = rec.get("state") or {}
    for after, unit in sorted(units(table).items()):
        if unit.get("state") != "done":
            continue
        have = state.get(str(after))
        if have is None:
            out.append(f"the stopped mover's ledger closed the range after"
                       f" {after} done, which the machines' record has no"
                       " range for: the record stands")
        elif have["state"] != claims.DONE:
            out.append(f"the stopped mover's ledger closed the range after"
                       f" {after} done, but the machines' record holds it"
                       f" {have['state']}: the record stands, the range is"
                       " copied again from an emptied span")
    return out


def reconcile(rec, uncertain, now, lapsed_from=None, ledger=None,
              table=None):
    """The `Account` of the table's record at `now`; `uncertain` is what
    `opened` read of the departed mover's ledger, `lapsed_from` the
    claims `departed` just put back (their afters), `ledger` the departed
    mover's ledger the record is also held against (`conflicts`)."""
    done, back, held, todo = [], [], [], []
    taken = set(lapsed_from or ())
    for a, _ in rec["ranges"]:
        st = rec["state"][str(a)]
        if st["state"] == claims.DONE:
            done.append(a)
        elif st["state"] == claims.CLAIMED and \
                float(st.get("expires") or 0) > now:
            held.append(a)
        elif a in taken or a in uncertain or st["state"] == claims.CLAIMED:
            back.append(a)
        else:
            todo.append(a)
    return Account(done, back, held, todo, dict(uncertain),
                   conflicts(rec, ledger, table) if ledger is not None
                   else [])


def take_over(table, took_over, ledger=None, name=None):
    """The ranges of `table` (`claims.Table`) a mover whose lease record
    was `took_over` held, put back in one conditional write, and the
    `Account` of the table after it; `name` is the table as the ledger's
    units name it (the record's key where not given)."""
    gone = mover_of(took_over)
    now = time.time()
    before = table.read() or {}
    if before.get("ranges") is None:
        return None
    if gone:
        rec, _ = table._do(lambda r: departed(r, {gone}, now))
    else:
        rec = before
    back = [a for a, _ in before["ranges"]
            if before["state"][str(a)]["state"] == claims.CLAIMED
            and before["state"][str(a)].get("by") == gone]
    name = name or before.get("key") or ""
    return reconcile(rec, opened(ledger, name) if ledger else {},
                     now, back, ledger, name)


def audit(rec):
    """What in a table's record means a range was copied twice or lost;
    [] where nothing does."""
    if not rec or rec.get("ranges") is None:
        return ["the table has no record of its ranges"]
    out = []
    spans = sorted((int(a), int(u)) for a, u in rec["ranges"])
    for (a, u), (b, _) in zip(spans, spans[1:]):
        if b > u:
            out.append(f"no range covers ({u}, {b}]: those rows were lost")
        elif b < u:
            out.append(f"ranges after {a} and {b} overlap: rows in"
                       f" ({b}, {u}] were copied twice")
    afters = {str(a) for a, _ in spans}
    states = set(rec.get("state") or {})
    for a in sorted(afters - states, key=int):
        out.append(f"the range after {a} has no state: it was never copied")
    for a in sorted(states - afters, key=int):
        out.append(f"a state after {a} names no range")
    later = {}
    for lap in rec.get("lapsed") or ():
        a = str(lap.get("after"))
        later[a] = max(later.get(a, 0), int(lap.get("number") or 0))
    numbers = {}
    for a in sorted(afters & states, key=int):
        st = rec["state"][a]
        if st["state"] != claims.DONE:
            if rec.get("finished"):
                out.append(f"the table was finished with the range after"
                           f" {a} {st['state']}")
            continue
        n = int(st.get("number") or 0)
        if not n or not st.get("by"):
            out.append(f"the range after {a} is done under no claim")
        if later.get(a, 0) >= n:
            out.append(f"the range after {a} is done under number {n},"
                       f" not above the claim {later[a]} taken from"
                       " another: two claims may both have written it")
        if n in numbers:
            out.append(f"the ranges after {numbers[n]} and {a} are done"
                       f" under one number ({n})")
        numbers[n] = a
        if n > int(rec.get("numbers") or 0):
            out.append(f"the range after {a} is done under number {n},"
                       " above any the table handed out")
    return out


def _tables(hop, store, dbs=None):
    """[(db, key)] of every table whose ranges the hop's machines share."""
    out = []
    for db in dbs or hop.databases or [""]:
        have, _ = store.read_record(hop.report_dir(db) / claims.INDEX)
        out += [(db, k) for k in (have or {}).get("keys") or []]
    return out


def take_hop_over(hop, took_over, store, say, ledger=None, dbs=None):
    """Every shared table of `hop` taken over from the mover whose lease
    record was `took_over` (`take_over`), the account of each said: the
    one place a standby mover puts the departed mover's ranges back, at
    the moment it takes the hop. {(db, key): Account}; a table whose
    record cannot be read is said and left to its claims' own terms."""
    from . import ha
    out = {}
    for db, key in _tables(hop, store, dbs):
        table = claims.Table(store, claims.path(hop, db, key), ha.mover(),
                             ha.term())
        try:
            got = take_over(table, took_over, ledger, key)
        except Exception as e:  # noqa: BLE001 - said; the claim's term stands
            say(f"{key}: the stopped mover's ranges were not put back"
                f" ({type(e).__name__}); they are claimable once their"
                " term lapses")
            continue
        if got is None:
            continue
        out[(db, key)] = got
        for line in got.lines(key):
            say(line)
    return out


def audit_hop(hop, store, dbs=None):
    """{(db, key): [what in the table's record means a range copied twice
    or lost]} for every shared table of the hop that has a finding; {}
    where each is whole. A record that cannot be read is a finding."""
    out = {}
    for db, key in _tables(hop, store, dbs):
        try:
            rec, _ = store.read_record(claims.path(hop, db, key),
                                       sealed=True)
        except Exception as e:  # noqa: BLE001 - an unread record proves nothing
            out[(db, key)] = [f"the record could not be read"
                              f" ({type(e).__name__}): the table is not"
                              " proved copied once"]
            continue
        found = audit(rec)
        if found:
            out[(db, key)] = found
    return out


def audit_lines(found):
    """The findings of `audit_hop` as report lines, one per finding."""
    return [f"{key}: {why}" for (_, key), whys in sorted(found.items())
            for why in whys]
