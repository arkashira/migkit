"""Two-way conflict resolution, held to what makes a pair of databases
end alike (backlog P0-DEPTH 2): each conflict is settled twice, once at
each end - A's change against B's row on B, B's against A's on A - and
both ends have to reach the same row.

Hypothesis chooses the rows, what each side does to them, the rules
(per hop, per table, per column and group, the rules of rows), and how
the changes are batched and interleaved:

* settled from both ends, one conflict comes to one outcome - both ends
  stop, both park, or both write the same row - for every rule, the
  rules that hold a row included (`settle`, pure)
* two sides that each changed rows since they last agreed, the changes
  carried each way through `twoway.resolve` and applied, end with the
  same rows, whatever the rules and however the batches fall
* for the rules that carry their own order - a group settled by a
  timestamp every write moves forward, counters, a maximum over values
  that only rise - any interleaving of writes and deliveries on both
  sides converges, to the newest version, every addition kept, the
  highest value
* a batch applied again leaves the rows as it left them the first time
* a side's several changes to a row in one batch settle as the one
  change they add up to

The pair is set up as the configuration check wants it (`twoway.apart`
finds nothing), which is part of what is held.
"""
import copy
import tempfile
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from migkit import twoway
from tests.twoway_sides import Side, mirrored, pair, sync

TEXT = st.sampled_from(["p", "q", "r", None])
INT = st.integers(0, 6)
TAGS = st.lists(st.integers(0, 4), max_size=3, unique=True)
ROW = st.fixed_dictionaries({"v": TEXT, "w": TEXT, "ts": INT,
                             "n": st.integers(-3, 3) | st.none(),
                             "hi": INT, "tags": TAGS})
#: rules that settle values; the ones that hold a row are added where the
#: test says so
SETTLING = ["overwrite", "ignore", "source_priority", "newest", "max",
            "min"]
HOLDING = ["error", "park"]
ROWS = ["update", "delete", "overwrite", "ignore", "source_priority"]
ONCE = settings(max_examples=150, deadline=None,
                suppress_health_check=[HealthCheck.too_slow])


@st.composite
def rules(draw, holding=False, counters=True):
    """A hop's `two_way`: its own rule, maybe a table's, the table's
    columns and groups, the rule of rows, the ranks."""
    pick = SETTLING + (HOLDING if holding else [])
    got = {"column": "ts", "on_conflict": draw(st.sampled_from(pick))}
    table = {}
    if draw(st.booleans()):
        table["default"] = draw(st.sampled_from(pick))
    columns = {}
    grouped = draw(st.booleans())
    compared = st.sampled_from(pick) | st.just({"newest": "ts"})
    if grouped:
        columns["v, w"] = draw(st.sampled_from(
            [r for r in pick if r not in ("max", "min")])
            | st.just({"newest": "ts"}) | st.just({"max": "ts"}))
    else:
        for c in ("v", "w"):
            if draw(st.booleans()):
                columns[c] = draw(compared)
    if counters and draw(st.booleans()):
        columns["n"] = "delta"
    if draw(st.booleans()):
        columns["hi"] = draw(st.sampled_from(["max", "min"] + pick))
    if draw(st.booleans()):
        columns["tags"] = "union"
    table["columns"] = columns
    if draw(st.booleans()):
        table["delete_vs_update"] = draw(st.sampled_from(
            ROWS + (HOLDING if holding else [])))
    if holding and draw(st.booleans()):
        table["delete_missing"] = draw(st.sampled_from(["ignore"]
                                                       + HOLDING))
    got["tables"] = {"t": table}
    if draw(st.booleans()) or "source_priority" in repr(got):
        got["source_rank"], got["target_rank"] = draw(st.sampled_from(
            [(2, 1), (1, 2)]))
    return got


#: what one side does to one row: nothing, an update of some of its
#: columns, a delete, or (where it has none) an insert
ACTS = st.one_of(
    st.just(("none", None)),
    st.tuples(st.just("update"), st.fixed_dictionaries(
        {}, optional={"v": TEXT, "w": TEXT, "ts": INT,
                      "n": st.integers(-3, 3), "hi": INT, "tags": TAGS})),
    st.just(("delete", None)),
    st.tuples(st.just("insert"), ROW))


def _act(side, key, act):
    kind, values = act
    if kind == "update" and key in side.rows:
        return [side.update(key, **values)]
    if kind == "delete" and key in side.rows:
        return [side.delete(key)]
    if kind == "insert" and key not in side.rows:
        return [side.insert(key, **values)]
    return []


def _world(base):
    return Side({k: r for k, r in base.items() if r is not None})


def _batches(changes, cuts):
    """The changes in batches, cut only between rows: a side's changes to
    one row reach the other together."""
    out, run, last = [], [], None
    for i, c in enumerate(changes):
        key = c["key"]["id"]
        if run and key != last and cuts[i % len(cuts)]:
            out.append(run)
            run = []
        run.append(c)
        last = key
    return out + ([run] if run else [])


@ONCE
@given(two_way=rules(holding=True), base=ROW | st.none(),
       a=ROW | st.none(), b=ROW | st.none())
def test_one_conflict_settled_from_both_ends_comes_to_one_outcome(
        two_way, base, a, b):
    """Pure: the outcome of `settle` at one end and at the other. A row
    one side never changed is no conflict at the other end, so only rows
    both changed are held to it."""
    back = mirrored(two_way)
    assert twoway.apart(two_way, back) == []
    if a == base or b == base:
        return
    row = {"id": 1}
    base, a, b = ({**row, **x} if x is not None else None
                  for x in (base, a, b))
    ends = []
    for got, remote, local in ((two_way, a, b), (back, b, a)):
        tr = twoway.Rules(got, twoway._ranks_of(got)).table("t")
        _, classes, hints = twoway._facts(Side(), "d", "t", tr)
        ends.append(twoway.settle(
            tr, classes, twoway._ranks_of(got), base is None, base, remote,
            local, {"id"}, hints))
    one, other = ends
    # one conflict, one outcome: both ends stop, both park, or neither
    # holds the row (which rows they then write is the next test's)
    assert ("stop" in (one.outcome, other.outcome)) == \
        (one.outcome == other.outcome == "stop"), (one, other)
    assert ("park" in (one.outcome, other.outcome)) == \
        (one.outcome == other.outcome == "park"), (one, other)
    # and where a column was settled, both ends name the same rule for it
    assert sorted((tuple(d["columns"]), d["tie"])
                  for d in one.decided or []) == \
        sorted((tuple(d["columns"]), d["tie"])
               for d in other.decided or []), (one, other)


@ONCE
@given(two_way=rules(), rows=st.lists(st.tuples(ROW | st.none(), ACTS, ACTS),
                                      min_size=1, max_size=4),
       cuts=st.lists(st.booleans(), min_size=1, max_size=6),
       a_first=st.booleans())
def test_two_sides_changed_apart_end_alike_through_the_tails(
        two_way, rows, cuts, a_first):
    """Each side changes its rows once since the two last agreed; the
    changes go each way through `twoway.resolve`, in batches cut anywhere
    between rows, one direction first or the other."""
    back = mirrored(two_way)
    assert twoway.apart(two_way, back) == []
    base = {i + 1: r for i, (r, _, _) in enumerate(rows)}
    a, b = _world(base), _world(base)
    ca, cb = [], []
    for i, (_, act_a, act_b) in enumerate(rows):
        ca += _act(a, i + 1, act_a)
        cb += _act(b, i + 1, act_b)
    with tempfile.TemporaryDirectory() as where:
        ab = pair("ab", two_way, b, Path(where) / "ab")
        ba = pair("ba", back, a, Path(where) / "ba")
        ways = [(ab, ca), (ba, cb)]
        for p, changes in (ways if a_first else ways[::-1]):
            for batch in _batches(changes, cuts):
                sync(p, batch)
    assert a.rows == b.rows, (a.rows, b.rows, ca, cb)


#: the rules that carry their own order: a group whose timestamp every
#: write moves forward, a counter, a maximum over values that only rise
ORDERED = {"on_conflict": "error", "tables": {"t": {"columns": {
    "v, w, ts": {"newest": "ts"}, "n": "delta", "hi": "max",
    "tags": "union"}}}}

STEPS = st.lists(st.one_of(
    st.tuples(st.just("write"), st.sampled_from("AB"), st.sampled_from(
        [1, 2]), st.booleans(), st.booleans(), st.integers(-3, 3),
        st.integers(0, 3)),
    st.tuples(st.just("send"), st.sampled_from("AB"), st.integers(1, 4))),
    max_size=30)


@ONCE
@given(steps=STEPS)
def test_any_interleaving_converges_under_the_ordered_rules(steps):
    assert twoway.apart(ORDERED, mirrored(ORDERED)) == []
    base = {k: {"v": "v0", "w": "w0", "ts": 0, "n": 0, "hi": 0,
                "tags": []} for k in (1, 2)}
    sides = {"A": _world(base), "B": _world(base)}
    queues = {"A": [], "B": []}
    clock, added, highest, last = 0, {1: 0, 2: 0}, {1: 0, 2: 0}, {}
    with tempfile.TemporaryDirectory() as where:
        pairs = {"A": pair("ab", ORDERED, sides["B"], Path(where) / "ab"),
                 "B": pair("ba", ORDERED, sides["A"], Path(where) / "ba")}

        def send(x, k):
            batch, queues[x][:k] = queues[x][:k], []
            if batch:
                sync(pairs[x], batch)
        for step in steps:
            if step[0] == "send":
                send(step[1], step[2])
                continue
            _, x, key, v, w, d, r = step
            clock += 1
            row = sides[x].rows[key]
            values = {"ts": clock, "n": row["n"] + d, "hi": row["hi"] + r}
            if v:
                values["v"] = f"{x}{clock}"
            if w:
                values["w"] = f"{x}{clock}w"
            queues[x].append(sides[x].update(key, **values))
            added[key] += d
            highest[key] = max(highest[key], values["hi"])
            last[key] = {c: sides[x].rows[key][c] for c in ("v", "w", "ts")}
        send("A", len(queues["A"]))
        send("B", len(queues["B"]))
    a, b = sides["A"].rows, sides["B"].rows
    assert a == b, (a, b, steps)
    for key in (1, 2):
        # every addition kept, the highest value, the newest version
        assert a[key]["n"] == added[key], (a[key], added[key])
        assert a[key]["hi"] == highest[key]
        if key in last:
            assert {c: a[key][c] for c in ("v", "w", "ts")} == last[key]


@ONCE
@given(two_way=rules(counters=False),
       rows=st.lists(st.tuples(ROW | st.none(), ACTS, ACTS), min_size=1,
                     max_size=3))
def test_a_batch_read_again_changes_nothing_more(two_way, rows):
    """What a tail does after a stop before its position was saved, on a
    hop whose batches are not numbered (no counters)."""
    base = {i + 1: r for i, (r, _, _) in enumerate(rows)}
    a, b = _world(base), _world(base)
    ca = []
    for i, (_, act_a, act_b) in enumerate(rows):
        ca += _act(a, i + 1, act_a)
        _act(b, i + 1, act_b)
    with tempfile.TemporaryDirectory() as where:
        ab = pair("ab", two_way, b, Path(where) / "ab")
        sync(ab, ca)
        once = copy.deepcopy(b.rows)
        sync(ab, ca)
    assert b.rows == once, (once, b.rows, ca)


@ONCE
@given(two_way=rules(), base=ROW, mine=ROW | st.none(),
       theirs=st.lists(st.fixed_dictionaries(
           {}, optional={"v": TEXT, "w": TEXT, "ts": INT,
                         "n": st.integers(-3, 3), "hi": INT, "tags": TAGS}),
           min_size=1, max_size=3))
def test_a_rows_changes_in_one_batch_settle_as_the_change_they_add_to(
        two_way, base, mine, theirs):
    source = _world({1: base})
    changes = [source.update(1, **values) for values in theirs]
    whole = dict(changes[0], values=changes[-1]["values"])
    ends = []
    with tempfile.TemporaryDirectory() as where:
        for name, batch in (("many", changes), ("one", [whole])):
            target = _world({1: base})
            if mine is None:
                target.delete(1)
            else:
                target.update(1, **mine)
            sync(pair(name, two_way, target, Path(where) / name), batch)
            ends.append(target.rows)
    assert ends[0] == ends[1], (ends, changes)
