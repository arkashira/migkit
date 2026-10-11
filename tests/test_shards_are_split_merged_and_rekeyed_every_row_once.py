"""Resharding (backlog W7): N sources onto M targets by one shard function,
every row placed exactly once, followed until one cutover - and the proof
able to say "different" for every way a placement goes wrong.

The settings, the group, the function, the plan of a batch of changes
and the proof's judgement run without a server; property tests drive the
batch plan over random histories - keys moving between targets with and
without the image of the row before, keys moving between sources, a
batch applied twice - against an in-memory model of the targets. The
rest moves one PostgreSQL 16 database onto three by a hash of a column
that is not the key, follows changes that move rows between the three,
proves it, and cuts it over.
"""
import subprocess
import time
from decimal import Decimal

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings as hsettings
from hypothesis import strategies as st

from migkit import reshard
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-reshard-src", 16530), ("migkit-test-reshard-dst",
                                               16531)


def _hop(name, opts=None, host="10.0.0.1", target="10.0.0.9", db_map=None,
         goal="reshard", engine="postgres"):
    return Hop(name=name, engine=engine,
               source=Endpoint(host=host, port=5432),
               target=Endpoint(host=target, port=5432), goal=goal,
               databases=["shop"], db_map=dict(db_map or {}),
               options={"reshard": dict(opts or {})})


# --- the hop's words ----------------------------------------------------------------

def test_the_hop_says_how_rows_are_placed_and_is_held_to_it():
    got = reshard.settings(_hop("a", {"by": "range", "key": {"orders":
                                                             "customer_id"},
                                      "ranges": {"a": 100, "b": None}}))
    assert got["by"] == "range" and got["key"] == {"orders": ["customer_id"]}
    assert got["ranges"] == {"a": 100, "b": None}
    for wrong, said in (
            ({"by": "modulo"}, "by: modulo - one of"),
            ({"by": "range"}, "by: range places rows by ranges, and none"),
            ({"by": "hash", "values": {"a": [1]}},
             "values is read only by by: list"),
            ({"by": "list", "values": {"a": ["eu"], "b": ["eu"]}},
             "values: eu is listed for both a and b"),
            ({"by": "range", "ranges": {"a": None, "b": None}},
             "only the last range is open")):
        with pytest.raises(SystemExit) as e:
            reshard.settings(_hop("a", wrong))
        assert said in str(e.value), (wrong, str(e.value))
    # a hop of another goal has none
    assert reshard.settings(_hop("a", goal="copy")) is None


def test_the_group_is_every_hop_named_in_one_order_whichever_runs():
    a = _hop("a", {"with": ["b"]}, db_map={"shop": "shop_a"})
    b = _hop("b", {"with": ["c"]}, host="10.0.0.2",
             db_map={"shop": "shop_b"})
    c = _hop("c", {}, host="10.0.0.2", target="10.0.0.10")
    have = {"a": a, "b": b, "c": c}
    # the hops each names, and the hops those name; c names nobody
    assert [[h.name for h in reshard.group(runs, "shop", have).hops]
            for runs in (a, b, c)] == [["a", "b", "c"], ["b", "c"], ["c"]]
    g = reshard.group(a, "shop", have)
    # two sources (two servers), three targets (two databases on one
    # server, and another server), each named by its first hop by name
    assert [s.name for s in g.sources] == ["a", "b"]
    assert [t.name for t in g.targets] == ["a", "b", "c"]
    assert [g.index(n) for n in ("a", "b", "c")] == [0, 1, 2]
    assert g.lead is a


def test_a_group_that_does_not_hold_together_is_refused_with_every_reason():
    a = _hop("a", {"with": ["b", "c", "d", "e"], "by": "hash"})
    b = _hop("b", {"with": ["a"], "by": "range",
                   "ranges": {"a": 1, "b": None}}, host="10.0.0.2")
    c = _hop("c", goal="copy")
    e = _hop("e", {"by": "hash"}, host="10.0.0.9", target="10.0.0.9")
    e.db_map = {}
    with pytest.raises(SystemExit) as got:
        reshard.group(a, "shop", {"a": a, "b": b, "c": c, "e": e})
    said = str(got.value)
    assert "with: c's goal is not reshard" in said, said
    assert "with: d is not a hop the configuration has" in said, said
    assert "b places rows otherwise than a" in said, said
    # e reads 10.0.0.9's shop and some target writes 10.0.0.9's shop
    assert "writes the database e reads" in said, said
    one = _hop("x", {"by": "target", "with": ["y"]},
               db_map={"shop": "a"})
    two = _hop("y", {"by": "target", "with": ["x"]},
               db_map={"shop": "b"})
    with pytest.raises(SystemExit) as got:
        reshard.group(one, "shop", {"x": one, "y": two})
    assert "by: target places rows by the one target cluster's own" \
        in str(got.value)


# --- the function -------------------------------------------------------------------

CLS = {"id": "integer", "k": "integer", "v": "text", "region": "text"}


def _fn(way="hash", n=3, **kw):
    return reshard.Function(way, kw.pop("key", ["k"]), way, list(range(n)),
                            dict(CLS), ["id"], ["id", "k", "v"], **kw)


@given(st.lists(st.integers(-10**12, 10**12), min_size=1, max_size=200),
       st.integers(1, 7))
def test_a_hash_places_every_key_once_and_alike_from_a_row_or_a_change(keys,
                                                                      n):
    fn = _fn(n=n)
    for k in keys:
        a = fn.place(reshard._texts(fn, {"k": k}))
        # a change log hands a value over as text: placed alike
        b = fn.place(reshard._texts(fn, {"k": str(k)}))
        assert a == b and 0 <= a < n
        assert reshard.hash_place((str(k),), n) == a
    # the function is no one process's: the same text, the same target
    assert reshard.hash_place(("42",), 3) == reshard.hash_place(("42",), 3)
    # and every target takes a share of many keys
    many = {reshard.hash_place((str(k),), 3) for k in range(300)}
    assert many == {0, 1, 2}


def test_ranges_lists_and_lookups_place_a_value_or_say_it_goes_nowhere():
    rng = _fn("range", bounds=[((0, Decimal(10)), 0), ((0, Decimal(20)), 1),
                               (None, 2)])
    assert [rng.place((str(v),)) for v in (-5, 9, 10, 19, 20, 10**9)] == \
        [0, 0, 1, 1, 2, 2]
    closed = _fn("range", bounds=[((0, Decimal(10)), 0)])
    assert closed.place(("10",)) is None and closed.place(("9",)) == 0
    assert rng.place(None) is None
    listed = _fn("list", key=["region"], table={"eu": 0, "uk": 0, "us": 1})
    assert [listed.place((r,)) for r in ("eu", "uk", "us", "apac")] == \
        [0, 0, 1, None]
    listed.default = 2
    assert listed.place(("apac",)) == 2 and listed.place((None,)) == 2
    assert _fn("whole").place(None) == reshard.ALL
    # a split part's rule is SQL: placed only by asking the source
    assert _fn("rule").place(("1",)) is None


# --- the plan of a batch, against a model of the targets ---------------------------

def _apply(plan, targets, order=None):
    """Phase one on every target, then phase two - the follow's order -
    with what each target holds after each phase held to no key on two
    targets."""
    for j in range(len(targets)):
        for c in plan.out[j]:
            targets[j].pop(c["key"]["id"], None)
    _once(targets)
    for j in (order or range(len(targets))):
        for c in plan.into[j]:
            if c["op"] == "delete":
                targets[j].pop(c["key"]["id"], None)
            else:
                targets[j][c["key"]["id"]] = dict(c["values"])
    _once(targets)


def _once(targets):
    seen = {}
    for j, t in enumerate(targets):
        for k in t:
            assert k not in seen, f"key {k} on targets {seen[k]} and {j}"
            seen[k] = j


def _placed(fn, rows, n):
    want = [dict() for _ in range(n)]
    for k, row in rows.items():
        j = fn.place(reshard._texts(fn, row))
        want[j][k] = dict(row)
    return want


@st.composite
def _history(draw, sources=1):
    """Each source's log - inserts, updates moving the shard key or not,
    updates of the key itself, deletes; a row moved between sources as a
    delete in one log and an insert in another - and each source's final
    rows."""
    ids = list(range(1, 16))
    rows = [dict() for _ in range(sources)]
    logs = [[] for _ in range(sources)]
    images = draw(st.booleans())
    for _ in range(draw(st.integers(1, 60))):
        s = draw(st.integers(0, sources - 1))
        held = rows[s]
        anywhere = {k for r in rows for k in r}
        op = draw(st.sampled_from(("insert", "update", "update", "rekey",
                                   "delete", "move")))
        if op == "insert" or not held:
            free = [k for k in ids if k not in anywhere]
            if not free:
                continue
            k = draw(st.sampled_from(free))
            row = {"id": k, "k": draw(st.integers(0, 40)),
                   "v": draw(st.text(max_size=3))}
            held[k] = row
            logs[s].append({"op": "insert", "table": "t",
                            "key": {"id": k}, "values": dict(row)})
            continue
        k = draw(st.sampled_from(sorted(held)))
        old = held[k]
        before = {"before": dict(old)} if images else {}
        if op == "update":
            row = {"id": k, "k": draw(st.integers(0, 40)),
                   "v": draw(st.text(max_size=3))}
            held[k] = row
            logs[s].append({"op": "update", "table": "t", "key": {"id": k},
                            "values": dict(row), **before})
        elif op == "rekey":
            free = [x for x in ids if x not in anywhere]
            if not free:
                continue
            new = draw(st.sampled_from(free))
            row = {**old, "id": new}
            del held[k]
            held[new] = row
            logs[s].append({"op": "update", "table": "t", "key": {"id": k},
                            "values": dict(row), **before})
        elif op == "delete" or sources == 1:
            del held[k]
            logs[s].append({"op": "delete", "table": "t", "key": {"id": k},
                            **before})
        else:
            # the row moves to another source: out of one log, into another
            to = draw(st.sampled_from([x for x in range(sources) if x != s]))
            del held[k]
            rows[to][k] = old
            logs[s].append({"op": "delete", "table": "t", "key": {"id": k},
                            **before})
            logs[to].append({"op": "insert", "table": "t", "key": {"id": k},
                             "values": dict(old)})
    return logs, rows


def _batches(draw, log):
    out, at = [], 0
    while at < len(log):
        n = draw(st.integers(1, 6))
        out.append(log[at:at + n])
        at += n
    return out


@hsettings(max_examples=300, deadline=None,
           suppress_health_check=[HealthCheck.too_slow])
@given(st.data(), st.integers(2, 4), st.sampled_from(("hash", "range")))
def test_one_source_followed_every_row_on_one_target_after_every_batch(
        data, n, way):
    """After every batch, and after a batch applied twice (a stop between
    the target's commit and the kept position), the targets hold exactly
    what the function places of the source's rows as they stand - no key
    on two targets after either phase, a row whose key moved taken off
    the target it left."""
    fn = _fn(way, n=n, bounds=[((0, Decimal(10)), 0), ((0, Decimal(25)), 1),
                               (None, n - 1)])
    (log,), (final,) = data.draw(_history(1))
    targets = [dict() for _ in range(n)]
    now = {}
    for batch in _batches(data.draw, log):
        plan = reshard.plan_batch(batch, {"t": fn}, n)
        order = data.draw(st.permutations(list(range(n))))
        for _ in range(1 + data.draw(st.integers(0, 1))):
            _apply(plan, targets, order)
        for c in batch:
            now.pop(c["key"]["id"], None)
            if c["op"] != "delete":
                now[c["values"]["id"]] = dict(c["values"])
        assert targets == _placed(fn, now, n)
    assert now == final
    assert targets == _placed(fn, final, n)


@hsettings(max_examples=300, deadline=None,
           suppress_health_check=[HealthCheck.too_slow])
@given(st.data(), st.integers(2, 4))
def test_keys_that_move_between_sources_end_once_on_their_target(data, n):
    """Two sources' logs applied in any interleaving their own orders allow:
    a key that left one source for another arrives as a delete in one log
    and an insert in the other, and the source holding it now
    (`_held_elsewhere`) is the one whose row the targets keep - once the
    follow has drained both logs, every row of either source is on the one
    target its key places it on, and no other."""
    fn = _fn(n=n)
    logs, finals = data.draw(_history(2))
    union = {k: r for rows in finals for k, r in rows.items()}
    assert len(union) == sum(len(r) for r in finals)
    queues = [_batches(data.draw, log) for log in logs]
    targets = [dict() for _ in range(n)]
    while any(queues):
        s = data.draw(st.sampled_from([i for i, q in enumerate(queues) if q]))
        batch = queues[s].pop(0)
        first = reshard.plan_batch(batch, {"t": fn}, n)
        held = {}
        for t, key in first.asked:
            for o, rows in enumerate(finals):
                if o != s and key["id"] in rows:
                    held[reshard._ident(t, key)] = dict(rows[key["id"]])
        plan = reshard.plan_batch(batch, {"t": fn}, n, held) if held \
            else first
        for j in range(n):
            for c in plan.out[j]:
                targets[j].pop(c["key"]["id"], None)
        for j in range(n):
            for c in plan.into[j]:
                if c["op"] == "delete":
                    targets[j].pop(c["key"]["id"], None)
                else:
                    targets[j][c["key"]["id"]] = dict(c["values"])
    assert targets == _placed(fn, union, n)


@hsettings(max_examples=200, deadline=None)
@given(st.data(), st.integers(2, 4))
def test_a_split_parts_rules_asked_of_the_source_place_every_row(data, n):
    """A split's parts place a row by their rules, which only the source can
    answer (`_rule_places`): asked of the rows as they stand when the
    follow has drained, each row is on the part whose rule takes it and
    no other, and a row no rule takes - the rest staying on the source -
    on none."""
    # each value of the column a part's rule reads taken by one part, or by
    # none: rules that overlap are refused before the move (`split`)
    owner = {k: data.draw(st.sampled_from(list(range(n)) + [None]))
             for k in range(41)}
    takes = {j: {k for k, o in owner.items() if o == j} for j in range(n)}
    fn = _fn("rule", n=n, rules={j: f"k in {sorted(t)}"
                                 for j, t in takes.items()}, stays=True)
    (log,), (final,) = data.draw(_history(1))
    targets = [dict() for _ in range(n)]
    for batch in _batches(data.draw, log):
        first = reshard.plan_batch(batch, {"t": fn}, n)
        placed = {}
        for t, key in first.rule:
            row = final.get(key["id"])
            placed[reshard._ident(t, key)] = (
                reshard.GONE if row is None else
                {j for j, ks in takes.items() if row["k"] in ks})
        _apply(reshard.plan_batch(batch, {"t": fn}, n, placed=placed),
               targets)
    for j, ks in takes.items():
        want = {k: r for k, r in final.items() if r["k"] in ks}
        # a row the source no longer holds is left to its own delete,
        # which the log carries: by its end none is left
        assert targets[j] == want, j


def test_a_row_that_is_not_whole_is_completed_from_the_source_now():
    fn = _fn(n=2)
    batch = [{"op": "update", "table": "t", "key": {"id": 1},
              "values": {"id": 1, "k": 7}}]
    first = reshard.plan_batch(batch, {"t": fn}, 2)
    assert first.partial == [("t", {"id": 1})]
    ident = reshard._ident("t", {"id": 1})
    plan = reshard.plan_batch(batch, {"t": fn}, 2,
                              filled={ident: {"id": 1, "k": 3, "v": "x"}})
    j = fn.place(("7",))
    [got] = plan.into[j]
    # what the change says wins; what it left out is the source's now
    assert got["values"] == {"id": 1, "k": 7, "v": "x"}
    gone = reshard.plan_batch(batch, {"t": fn}, 2, filled={ident: None})
    assert not any(gone.into.values()) and not any(gone.out.values())


def test_a_change_no_target_takes_stops_the_follow_before_it():
    fn = _fn("range", bounds=[((0, Decimal(10)), 0)])
    batch = [{"op": "insert", "table": "t", "key": {"id": 1},
              "values": {"id": 1, "k": 50, "v": ""}}]
    with pytest.raises(reshard.Unplaced) as e:
        reshard.plan_batch(batch, {"t": fn}, 3)
    assert "places its row on no target" in str(e.value)
    # a split part's rule that takes it nowhere, the rest not staying
    rule = _fn("rule", n=2, rules={0: "k < 5", 1: "k > 60"})
    ident = reshard._ident("t", {"id": 1})
    with pytest.raises(reshard.Unplaced) as e:
        reshard.plan_batch(batch, {"t": rule}, 2, placed={ident: set()})
    assert "in no part's rule" in str(e.value)
    rule.stays = True
    plan = reshard.plan_batch(batch, {"t": rule}, 2, placed={ident: set()})
    # the rest staying on the source: written on no part
    assert plan.into == {0: [], 1: []} and plan.out == {0: [], 1: []}


# --- a change of a table's shape, made once every source has made it ---------------

def test_a_batch_is_cut_at_the_first_change_of_a_group_tables_shape():
    fns = {"public.orders": _fn(), "public.countries": _fn("whole")}
    fns["public.countries"].home = 1

    def table_of(t):
        return t if t in fns else None
    row = {"op": "insert", "table": "public.orders", "key": {"id": 1},
           "values": {"id": 1, "k": 1, "v": ""}}
    other = {"op": "ddl", "table": "public.audit", "kind": "alter"}
    copy = {"op": "ddl", "table": "public.countries", "kind": "truncate"}
    mine = {"op": "ddl", "table": "public.orders", "kind": "alter"}
    before, ddl, after = reshard._split_at_ddl(
        [row, other, copy, row, mine, row], table_of, 0, fns)
    # a table outside the group, and source 0's copy of a table its
    # targets take whole from source 1, pass; the group's own does not
    assert before == [row, other, copy, row] and ddl is mine
    assert after == [row]
    assert reshard._split_at_ddl([row], table_of, 0, fns) == ([row], None,
                                                              [])
    _, ddl, _ = reshard._split_at_ddl([copy], table_of, 1, fns)
    assert ddl is copy
    # a log that keeps no DDL shows one by a column the table did not have
    wider = {**row, "values": {**row["values"], "note": "x"}}
    before, ddl, after = reshard._split_at_ddl([row, wider, row], table_of,
                                               0, fns)
    assert before == [row] and after == [wider, row]
    assert ddl == {"op": "ddl", "table": "public.orders", "kind": "alter"}


class _Shaped:
    def __init__(self, missing=(), takes=True):
        self.missing, self.takes, self.ran, self.emptied = missing, takes, \
            [], []

        class _E:
            def forget_shapes(self):
                pass

            def neutral_empty(e, side, db, table, where=None):
                self.emptied.append((table, where))
                return 3
        self.src_engine = self.dst_engine = _E()

    def _row_scope(self, db, t):
        return None, "region = 'eu'"

    def _lacking(self, db, before, after):
        return {t: ("orders", list(self.missing)) for t in after}

    def _additive(self, db, t, dst_t, missing):
        if not self.takes:
            return None, "a NOT NULL column with no default"
        return [f"alter table {dst_t} add column {m} text" for m in
                missing], ""

    def _run_ddl(self, db, sql):
        self.ran.append(sql)


def test_a_change_every_source_made_is_made_once_on_every_target(
        monkeypatch):
    lead = _hop("a")
    pairs = [_Shaped(["note"]), _Shaped(["note"]), _Shaped(["note"])]
    g = reshard.Group("reshard", lead, [lead],
                      [reshard.Side("a", lead, "shop")],
                      [reshard.Side(n, lead, "shop") for n in "abc"], {})
    for j, p in enumerate(pairs):
        g._pairs[(0, j)] = p

    class _Reader:
        def neutral_columns(self, side, db, t):
            return [("id", "integer"), ("note", "text")]
    monkeypatch.setattr(reshard.Group, "reader", lambda self, i: _Reader())
    monkeypatch.setattr(reshard, "_target_table", lambda pair, db, t: "orders")
    said = []
    # only the targets that hold the table are changed
    reshard._change_targets(g, "public.orders", {"alter"}, [0, 2],
                            said.append)
    assert [p.ran for p in pairs] == [
        ["alter table orders add column note text"], [],
        ["alter table orders add column note text"]]
    reshard._change_targets(g, "public.orders", {"truncate"}, [0, 1, 2],
                            said.append)
    # emptied within the hop's row filter, as a copy empties a table
    assert [p.emptied for p in pairs] == [[("orders", "region = 'eu'")]] * 3
    pairs[1].takes = False
    with pytest.raises(SystemExit) as e:
        reshard._change_targets(g, "public.orders", {"alter"}, [0, 1, 2],
                                said.append)
    assert "cannot take the change at its place in the log" in str(e.value)
    with pytest.raises(SystemExit) as e:
        reshard._change_targets(g, "public.orders", {"rename"}, [0],
                                said.append)
    assert "rename on every source" in str(e.value)


# --- the proof can say different --------------------------------------------------

def test_the_judgement_names_every_way_a_placement_is_wrong():
    want = {(1,): (0, "a"), (2,): (1, "b"), (3,): (1, "c"),
            (4,): (None, "d")}
    ok = [{(1,): "a"}, {(2,): "b", (3,): "c"}]
    per, twice, nowhere, lost = reshard.judge(want, ok)
    assert [p["missing"] + p["extra"] + p["changed"] for p in per] == [[],
                                                                       []]
    assert twice == [] and lost == [] and nowhere == [(4,)]
    # a row on the wrong target: missing where it belongs, extra where it is
    per, twice, _, lost = reshard.judge(want, [{(1,): "a", (2,): "b"},
                                               {(3,): "c"}])
    assert per[0]["extra"] == [(2,)] and per[1]["missing"] == [(2,)]
    assert twice == [] and lost == []
    # a row on two targets
    per, twice, _, _ = reshard.judge(want, [{(1,): "a", (2,): "b"},
                                            {(2,): "b", (3,): "c"}])
    assert twice == [(2,)] and per[0]["extra"] == [(2,)]
    # a row as the source does not have it, and a row on no target
    per, _, _, lost = reshard.judge(want, [{(1,): "x"}, {(3,): "c"}])
    assert per[0]["changed"] == [(1,)] and lost == [(2,)]
    # a key no source has
    per, _, _, _ = reshard.judge(want, [{(1,): "a", (9,): "z"},
                                        {(2,): "b", (3,): "c"}])
    assert per[0]["extra"] == [(9,)]
    # a table every target holds whole is not twice
    whole = {(1,): (reshard.ALL, "a")}
    per, twice, _, _ = reshard.judge(whole, [{(1,): "a"}, {(1,): "a"}])
    assert twice == [] and all(not p["missing"] for p in per)


# --- one PostgreSQL database onto three, followed and cut over --------------------

def _sql(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table countries (code text primary key, name text);
insert into countries values ('fr', 'France'), ('us', 'United States');
create table orders (id int primary key, customer_id int not null,
                     total numeric(10, 2));
insert into orders select g, g % 17, g * 1.5 from generate_series(1, 300) g;
"""
SHARDS = ("shop_a", "shop_b", "shop_c")


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                            "wal_level=logical"], check=True,
                           capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"], capture_output=True
                                  ).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
        _sql(SRC[0], "create database shop")
        _sql(SRC[0], SEED, "shop")
        for db in SHARDS:
            _sql(DST[0], f"create database {db}")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _conf(tmp_path, monkeypatch, cutover=False):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    hops = "hops:\n"
    for name, db in zip(("a", "b", "c"), SHARDS):
        hops += (f"  {name}:\n    engine: postgres\n    goal: reshard\n"
                 f"    source: {side.format(port=SRC[1])}\n"
                 f"    target: {side.format(port=DST[1])}\n"
                 "    databases: [shop]\n"
                 f"    db_map: {{shop: {db}}}\n"
                 "    options:\n"
                 "      follow_until: caught_up\n"
                 # the shards hold an earlier test's rows: replaced
                 "      target_rows: replace\n"
                 "      reshard: {with: [a, b, c], by: hash, key: {orders:"
                 " customer_id}, shared: [countries]}\n"
                 + ("      cutover: {budget: 120, freeze: operator}\n"
                    if cutover else ""))
    path.write_text(hops)
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


def _shard_rows():
    """{shard database: {order id: customer}} as the targets hold them."""
    out = {}
    for db in SHARDS:
        got = _sql(DST[0], "select id, customer_id from orders", db)
        out[db] = {int(a): int(b) for a, b in
                   (line.split("|") for line in got.splitlines() if line)}
    return out


def _held_once():
    """The targets' orders each in one shard - the one a hash of its
    customer places it in - and together the source's."""
    src = {int(a): int(b) for a, b in (
        line.split("|") for line in _sql(
            SRC[0], "select id, customer_id from orders", "shop"
        ).splitlines() if line)}
    shards = _shard_rows()
    seen = {}
    for j, db in enumerate(SHARDS):
        for oid, cust in shards[db].items():
            assert oid not in seen, f"order {oid} in {seen[oid]} and {db}"
            seen[oid] = db
            assert reshard.hash_place((str(cust),), 3) == j, (oid, cust, db)
    assert {o: c for rows in shards.values() for o, c in rows.items()} \
        == src
    return shards


@needs_docker
def test_one_database_onto_three_by_a_hash_followed_and_proved(
        pair, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch)
    got, out = _run("move", "b", "--mode", "full+cdc", "--db", "shop",
                    "--go")
    assert got.exit_code == 0, out
    assert "3 sources" not in out and "onto 3 targets" in out, out
    shards = _held_once()
    assert all(shards[db] for db in SHARDS), shards
    # every target holds the shared table whole
    for db in SHARDS:
        assert _sql(DST[0], "select count(*) from countries", db) == "2"
    # changes: a customer that moves a row to another shard, rows added,
    # rows removed - followed until caught up
    moved = [o for o, c in shards["shop_a"].items()][:5]
    for oid in moved:
        cust = next(c for c in range(17)
                    if reshard.hash_place((str(c),), 3) == 2)
        _sql(SRC[0], f"update orders set customer_id = {cust} where id ="
                     f" {oid}", "shop")
    _sql(SRC[0], "insert into orders select g, g % 13, 1 from"
                 " generate_series(301, 340) g", "shop")
    _sql(SRC[0], "delete from orders where id between 10 and 20", "shop")
    _sql(SRC[0], "update countries set name = 'USA' where code = 'us'",
         "shop")
    got, out = _run("move", "c", "--mode", "cdc", "--db", "shop", "--go")
    assert got.exit_code == 0, out
    shards = _held_once()
    assert all(oid in shards["shop_c"] for oid in moved), moved
    for db in SHARDS:
        assert _sql(DST[0], "select name from countries where code = 'us'",
                    db) == "USA"
    # a column added on the source: made on every shard once the source
    # has it, then the rows that carry it written
    _sql(SRC[0], "alter table orders add column note text;"
                 " update orders set note = 'n' || id where id <= 30",
         "shop")
    got, out = _run("move", "a", "--mode", "cdc", "--db", "shop", "--go")
    assert got.exit_code == 0, out
    assert out.count("changed on every source; on") == 3, out
    notes = {}
    for db in SHARDS:
        for line in _sql(DST[0], "select id, note from orders where note is"
                                 " not null", db).splitlines():
            oid, note = line.split("|")
            notes[int(oid)] = note
    assert notes == {o: f"n{o}" for o in range(1, 31) if not 10 <= o <= 20}
    got, out = _run("check", "a", "--db", "shop")
    assert got.exit_code == 0, out
    # the proof says different where a row sits on a shard the function
    # does not place it on
    _sql(DST[0], "insert into orders values (9999, 1, 0)",
         SHARDS[(reshard.hash_place(("1",), 3) + 1) % 3])
    _sql(SRC[0], "insert into orders values (9999, 1, 0)", "shop")
    got, out = _run("check", "a", "--db", "shop")
    assert got.exit_code != 0, out
    assert "the function places on another target" in out, out


@needs_docker
def test_a_row_no_target_takes_is_refused_before_anything_is_written(
        pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    path.write_text(
        "hops:\n"
        "  r:\n    engine: postgres\n    goal: reshard\n"
        f"    source: {side.format(port=SRC[1])}\n"
        f"    target: {side.format(port=DST[1])}\n"
        "    databases: [shop]\n    db_map: {shop: shop_a}\n"
        "    options:\n"
        "      reshard: {by: range, key: {orders: customer_id},"
        " ranges: {r: 5}, shared: [countries]}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    before = _sql(DST[0], "select count(*) from orders", "shop_a")
    got, out = _run("move", "r", "--mode", "full", "--db", "shop", "--go")
    assert got.exit_code != 0, out
    assert "rows no target takes" in out, out
    assert _sql(DST[0], "select count(*) from orders", "shop_a") == before


@needs_docker
def test_the_whole_group_is_cut_over_at_once(pair, tmp_path, monkeypatch):
    """The follow running, the cutover's own path over the group: the
    follow drained past every source's position taken after the operator
    stopped the writes, the proof across every shard, the flip - and the
    follow ended at the point of no return's first step."""
    import threading

    from migkit import tailctl
    _conf(tmp_path, monkeypatch, cutover=True)
    got, out = _run("move", "a", "--mode", "full+cdc", "--db", "shop",
                    "--go")
    assert got.exit_code == 0, out
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    path.write_text(path.read_text().replace("follow_until: caught_up",
                                             "follow_until: stopped"))
    hops = cfg.load_hops()
    g = reshard.group(hops["a"], "shop", hops)
    said = []
    follow = threading.Thread(target=reshard.follow,
                              args=(g, True, said.append), daemon=True)
    follow.start()
    for _ in range(60):
        if tailctl.alive(g.report_dir(), g.lead):
            break
        time.sleep(0.5)
    _sql(SRC[0], "update orders set total = total + 1 where id < 50",
         "shop")
    got, out = _run("move", "a", "--mode", "cdc", "--drop", "--db", "shop",
                    "--go")
    assert got.exit_code == 0, out
    assert "cut over" in out, out
    assert "the final check" in out and "found the two the same" in out, out
    _held_once()
    tailctl.stop(g.report_dir())
    tailctl.resume(g.report_dir())
    follow.join(timeout=60)
