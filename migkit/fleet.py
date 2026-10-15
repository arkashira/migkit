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
                held by a machine still in its term, or left to copy
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


def opened(ledger, table):
    """{after or name: the unit's record} of the ranges of `table` the
    ledger has open - begun and never closed - and the operations it
    keeps uncertain that name the table. Read only."""
    pattern = re.compile(re.escape(f"{table} r:") + r"(-?\d+)$")
    out = {}
    for name, rec in dict(ledger.records).items():
        if rec.get("level") != "range" or rec.get("parent") != table \
                or rec.get("state") != "open":
            continue
        got = pattern.match(name)
        out[int(got.group(1)) if got else name] = dict(rec)
    for key, rec in ledger.uncertain().items():
        if rec.get("table") == table:
            after = rec.get("after")
            out.setdefault(int(after) if after is not None else key,
                           dict(rec))
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
        return out


def reconcile(rec, uncertain, now, lapsed_from=None):
    """The `Account` of the table's record at `now`; `uncertain` is what
    `opened` read of the departed mover's ledger, `lapsed_from` the
    claims `departed` just put back (their afters)."""
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
    return Account(done, back, held, todo, dict(uncertain))


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
                     now, back)


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
