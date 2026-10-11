"""TiDB's checks, repairs and changes go through the pair every engine is
carried by, and hold the hop's scope there (P0-PARITY.scope, verify),
without a server: the engine's own methods over a small in-memory server
that answers the neutral contract (`neutral_rows_by_key`, `neutral_write`,
`_apply_delete`) and the catalogue statements a check sends.

* the hop's row filter and its table renames are read by the count and the
  digest of a check, on each side under its own table's name;
* a repair plan holds the last check's drilldown to the tables the hop
  takes, and says what it leaves out;
* a repair writes what the source holds and removes what it does not, with
  every row it overwrites or removes kept first, and puts every one back
  exactly (the record `repairlog` keeps);
* the rows only what changed names are verified, from the position of the
  last clean pass, through the hop's scope, and a finding names its table
  and counts the rows apart;
* a difference found while the tail runs is held until the tail has read as
  far as the source's clock stood, and looked at again: what was still
  arriving is not called wrong, and what stays is;
* the tail keeps each table in a state of its own: a table the hop stops
  taking leaves at the position it was at, the others go on, and the states
  are kept where a restart finds them;
* the changes the tail carries are those the hop's `mapping.ops` carries.
"""
import json

import pytest

from migkit import canon, repairlog, tailctl, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines import mysql
from migkit.engines.base import LEFT_OUT
from migkit.engines.hetero import HeteroEngine
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.row-filter", "tidb"),
    ("scope.names", "tidb"),
    ("scope.repair", "tidb"),
    ("scope.ops", "tidb"),
    ("verify.repair", "tidb"),
    ("verify.repair-undo", "tidb"),
    ("verify.delta", "tidb"),
    ("verify.table", "tidb"),
    ("verify.confirm", "tidb"),
    ("tail.table-states", "tidb"),
]

EP = Endpoint(host="x", port=1, user="x", password="x")
COLS = [("id", "bigint"), ("label", "varchar(20)")]


@pytest.fixture
def server(monkeypatch):
    """Two small TiDB servers: `rows[side][table] = {id: (id, label)}`, the
    statements each is asked, and the changes TiCDC says were made."""
    class S:
        rows = {
            "src": {"orders": {1: (1, "one"), 2: (2, "TWO"),
                               3: (3, "three")},
                    "audit": {1: (1, "x")}},
            "dst": {"orders": {1: (1, "one"), 2: (2, "two"), 9: (9, "stray")},
                    "audit": {7: (7, "own")}}}
        asked = []
        feed = []
    s = S()
    s.rows = {side: {t: dict(r) for t, r in tables.items()}
              for side, tables in S.rows.items()}
    s.asked, s.feed = [], []

    def by_key(self, side, db, table, columns, key, keys, where=None):
        return {(str(k[0]),): list(s.rows[side][table][k[0]])
                for k in keys if k[0] in s.rows[side][table]}

    def write(self, side, db, table, columns, rows):
        for r in rows:
            s.rows[side][table][r[0]] = tuple(r)
        return len(rows)

    def delete(self, side, db, table, keyvals):
        s.rows[side][table].pop(keyvals["id"], None)

    def q(self, side, sql, args=None, fresh=False):
        s.asked.append((side, " ".join(sql.split()), tuple(args or ())))
        return []

    def digest(self, side, db, table, columns, where=None, **kw):
        rows = sorted(s.rows[side][table].values())
        return len(rows), str(hash(tuple(rows)) & 0xffffff)

    def read(self, side, db, table, columns, after=None, limit=1000,
             where=None):
        rows = [list(r) for k, r in sorted(s.rows[side][table].items())
                if after is None or k > int(after[0])][:limit]
        return rows, ((rows[-1][0],) if rows else None)

    def batches(self, side, db, table, columns, size=1000, where=None):
        yield [list(r) for _, r in sorted(s.rows[side][table].items())]

    def tso(self, side="src"):
        return 11
    cls = mysql.MySQLEngine
    monkeypatch.setattr(cls, "_all_tables",
                        lambda self, side, db: list(s.rows[side]))
    monkeypatch.setattr(cls, "neutral_columns",
                        lambda self, side, db, t: COLS)
    monkeypatch.setattr(cls, "neutral_key", lambda self, side, db, t: ["id"])
    monkeypatch.setattr(cls, "neutral_rows_by_key", by_key)
    monkeypatch.setattr(cls, "neutral_write", write)
    monkeypatch.setattr(cls, "_apply_delete", delete)
    monkeypatch.setattr(cls, "neutral_digest", digest)
    monkeypatch.setattr(cls, "neutral_read", read)
    monkeypatch.setattr(cls, "neutral_batches", batches)
    monkeypatch.setattr(cls, "_q", q)
    monkeypatch.setattr(TiDBEngine, "_tso", tso)
    monkeypatch.setattr(
        TiDBEngine, "copy_point", lambda self, side, db: {"fence": 1})
    monkeypatch.setattr(
        TiDBEngine, "neutral_changes",
        lambda self, side, db, token=None, limit=1000: (list(s.feed),
                                                        {"fence": 2}))
    return s


def _engine(tmp_path, **kw):
    hop = Hop(name="t", engine="tidb", source=EP, target=EP,
              databases=["d"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return TiDBEngine(hop)


# ---- the row filter and the renames, read by the check ----------------------

def test_the_count_and_the_digest_read_the_filter_and_each_sides_own_table(
        tmp_path, monkeypatch, server):
    """The filter narrows both sides, and the target's rows outside it are
    counted apart; the renamed table is read on the target by its own
    name."""
    seen = []

    def digest(self, side, db, table, columns, where=None, **kw):
        seen.append((side, table, where))
        return 3, "abc"
    monkeypatch.setattr(mysql.MySQLEngine, "neutral_digest", digest)
    server.rows["dst"] = {"orders_v2": {}}
    eng = _engine(tmp_path, exclude=["audit"],
                  mapping={"where": {"orders": "label <> 'x'"},
                           "tables": {"d.orders": "orders_v2"}})
    got = eng.check_data("d")
    assert [(r.scope, r.status) for r in got] == [("d.orders", "ok")], \
        [r.detail for r in got]
    assert ("src", "orders", "label <> 'x'") in seen
    assert ("dst", "orders_v2", "label <> 'x'") in seen
    # the target's rows the filter excludes are asked of it apart
    assert any(side == "dst" and table == "orders_v2" and "is not true" in
               str(where) for side, table, where in seen), seen
    # and no table the hop leaves out is read at all
    assert not [s for s in seen if s[1] == "audit"]
    counts = eng.check_counts("d")
    assert [r.status for r in counts] == ["ok"], [r.detail for r in counts]


# ---- what a check finds names its table -------------------------------------

def test_a_finding_names_its_table_as_the_source_names_it(tmp_path,
                                                          monkeypatch,
                                                          server):
    monkeypatch.setattr(HeteroEngine, "_drill",
                        lambda self, *a, **k: "; the rows were walked")
    eng = _engine(tmp_path, exclude=["audit"])
    got = eng.check_data("d")
    bad = [r for r in got if r.status == "diff"]
    assert bad, [(r.scope, r.status) for r in got]
    assert bad[0].scope == "d.orders" and bad[0].table == "orders"


# ---- a repair stays in the scope --------------------------------------------

def test_a_drilldown_of_a_table_the_hop_leaves_out_is_said_not_acted_on(
        tmp_path, server):
    (tmp_path / "data-orders.missing").write_text('["3"]\n')
    (tmp_path / "data-audit.extra").write_text('["7"]\n')
    plan = _engine(tmp_path, exclude=["audit"]).repair_plan("d", "rows")
    rows = [a for a in plan if a.kind == "rows"]
    assert [a.scope for a in rows] == ["d.orders"]
    assert all("audit" not in s for a in rows for s in a.statements)
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert said and "audit" in said[0].note
    assert not said[0].statements and not said[0].undo


# ---- a repair, and every row it touched put back ----------------------------

def _repair(tmp_path, server):
    for kind, keys in (("missing", ["3"]), ("changed", ["2"]),
                       ("extra", ["9"])):
        (tmp_path / f"data-orders.{kind}").write_text(
            "".join(json.dumps([k]) + "\n" for k in keys))
    eng = _engine(tmp_path, exclude=["audit"])
    plan = eng.repair_plan("d", "rows")
    for action in plan:
        eng.apply("d", action)
    return eng, plan


def test_a_repair_writes_what_the_source_holds_and_removes_the_rest(
        tmp_path, server):
    eng, plan = _repair(tmp_path, server)
    assert len(plan) == 1 and "1 missing, 1 changed, 1 extra" in plan[0].note
    assert server.rows["dst"]["orders"] == server.rows["src"]["orders"]
    # the target's own table the hop leaves out is not touched
    assert server.rows["dst"]["audit"] == {7: (7, "own")}


def test_every_row_a_repair_overwrote_or_removed_was_kept_first(tmp_path,
                                                                server):
    _repair(tmp_path, server)
    kept = [json.loads(line) for line in
            (tmp_path / "undo" / "orders.rows.jsonl").read_text()
            .splitlines()]
    assert {(k["key"][0], k["row"]["label"]) for k in kept} == {
        ("2", "two"), ("9", "stray")}


def test_a_repair_is_put_back_exactly_and_only_where_it_still_stands(
        tmp_path, server):
    eng, _ = _repair(tmp_path, server)
    before = {1: (1, "one"), 2: (2, "two"), 9: (9, "stray")}
    paths = repairlog.to_reverse(tmp_path / "undo")
    assert len(paths) == 1
    # a row written since the repair is not put back over
    server.rows["dst"]["orders"][2] = (2, "NEWER")
    io = repairlog.io_for(eng, "d", paths[0])
    done, same, left = repairlog.reverse(paths[0], io)
    assert left == [["2"]] and done == 2
    assert server.rows["dst"]["orders"] == {
        1: (1, "one"), 2: (2, "NEWER"), 9: (9, "stray")}
    # the same turn again changes nothing more
    done, same, left = repairlog.reverse(paths[0], io)
    assert done == 0 and left == [["2"]]
    server.rows["dst"]["orders"][2] = (2, "TWO")
    repairlog.reverse(paths[0], io)
    assert server.rows["dst"]["orders"] == before


# ---- what changed, and only what changed, is verified -----------------------

def test_only_the_rows_the_changes_name_are_asked_through_the_scope(
        tmp_path, server):
    eng = _engine(tmp_path, exclude=["audit"])
    first = eng.delta_verify("d")
    assert first[0].detail.startswith("baseline recorded")
    server.feed = [{"table": "orders", "key": {"id": 2}},
                   {"table": "orders", "key": {"id": 3}},
                   {"table": "audit", "key": {"id": 1}}]
    got = {r.scope: r for r in eng.delta_verify("d")}
    assert got["d"].status == "diff" and "NOT advanced" in got["d"].detail
    assert "2 changed rows across 1 tables" in got["d"].detail
    one = got["d.orders"]
    assert "missing=1 extra=0 changed=1" in one.detail
    # the finding names its table and counts the rows apart
    assert one.table == "orders"
    assert one.rows_apart == {"orders": {"missing": 1, "extra": 0,
                                         "changed": 1, "exact": True}}
    assert "d.audit" not in got
    # followed on the target, the next pass is clean and moves on
    server.rows["dst"]["orders"][3] = (3, "three")
    server.rows["dst"]["orders"][2] = (2, "TWO")
    got = {r.scope: r for r in eng.delta_verify("d")}
    assert got["d"].status == "ok" and "position advanced" in got["d"].detail
    assert got["d.orders"].status == "ok"


# ---- the changes the tail carries ---------------------------------------------

def _change(op, i, table="orders"):
    return canon.change(op, table, {"id": i},
                        None if op == "delete" else {"id": i, "label": "x"})


def test_the_tail_carries_only_the_changes_the_hop_carries(tmp_path,
                                                           server):
    eng = _engine(tmp_path, mapping={"ops": {"orders": "insert, update"}})
    batch = [_change("insert", 1), _change("delete", 2),
             _change("update", 3), _change("delete", 4, "audit")]
    out, _ = eng._as_pair()._to_target("d", batch, {})
    assert [(c["op"], c["key"]["id"], c["table"]) for c in out] == [
        ("insert", 1, "orders"), ("update", 3, "orders"),
        ("delete", 4, "audit")]


def test_a_hop_that_carries_everything_carries_it_all(tmp_path, server):
    batch = [_change("insert", 1), _change("delete", 2)]
    out, _ = _engine(tmp_path)._as_pair()._to_target("d", batch, {})
    assert [c["op"] for c in out] == ["insert", "delete"]


# ---- a difference still arriving is told from one that is wrong -------------

@pytest.fixture
def tail(monkeypatch, server):
    """A tail that is running and has saved the position it has read to:
    each time that position is read it has applied `arriving` to the
    target first, as a tail that caught up does."""
    state = {"arriving": [], "at": 10 ** 9}
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: True)

    def saved(path):
        for table, key, row in state["arriving"]:
            server.rows["dst"][table][key] = row
        state["arriving"] = []
        return {"fence": state["at"]}
    monkeypatch.setattr(tailctl, "saved", saved)
    server.rows["src"] = {"orders": {1: (1, "one"), 2: (2, "two"),
                                     3: (3, "three")}}
    server.rows["dst"] = {"orders": {1: (1, "one"), 2: (2, "two")}}
    return state


def test_a_difference_the_tail_was_still_applying_is_confirmed_not_wrong(
        tmp_path, server, tail):
    tail["arriving"] = [("orders", 3, (3, "three"))]
    got = _engine(tmp_path).check_data("d")
    assert [(r.scope, r.status) for r in got] == [("d.orders", "ok")], \
        [r.detail for r in got]
    assert "still arriving through migkit's change tail" in got[0].detail


def test_a_difference_that_stays_after_the_tail_caught_up_is_a_real_one(
        tmp_path, server, tail):
    got = _engine(tmp_path).check_data("d")
    assert [(r.scope, r.status) for r in got] == [("d.orders", "diff")], \
        [r.detail for r in got]
    assert "1 missing on the target" in got[0].detail


def test_with_no_tail_running_nothing_is_waited_for(tmp_path, server, tail,
                                                    monkeypatch):
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: False)
    tail["arriving"] = [("orders", 3, (3, "three"))]
    got = _engine(tmp_path).check_data("d")
    assert [r.status for r in got] == ["diff"]


# ---- each table in a state of its own ------------------------------------------

class _Pair:
    """What the tail's table set asks of the pair it serves."""
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, eng):
        self.hop, self.src_engine = hop, eng

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def test_the_tail_keeps_each_table_in_a_state_of_its_own(tmp_path, server,
                                                         monkeypatch):
    monkeypatch.setattr(TiDBEngine, "position_lost",
                        lambda self, side, db, at: None)
    server.rows["src"] = {"orders": {}, "audit": {}}
    eng = _engine(tmp_path, options={"ddl": "park"})
    tables = tailtables.TableSet(_Pair(eng.hop, eng), "d",
                                 tmp_path / "tail-token.json", lambda m: None)
    tables.file.said = None
    tables._settle = lambda: None
    tables.file.refresh = lambda: True
    tables.boundary({"fence": 400}, None)
    assert {k: v["state"] for k, v in tables.tables.items()} == {
        "orders": "live", "audit": "live"}
    # the hop stops taking the audit table while the tail runs
    eng.hop.exclude = ["audit"]
    tables.boundary({"fence": 450}, None)
    assert tables.tables["orders"]["state"] == "live"
    assert tables.tables["audit"]["state"] == "removed"
    assert tables.tables["audit"]["at"] == {"fence": 450}
    # the states are where a restart reads them
    kept = tailtables.states(tmp_path)
    assert {k: v["state"] for k, v in kept.items()} == {
        "orders": "live", "audit": "removed"}


# ---- a load sets nothing aside on TiDB ------------------------------------------
#
# Not named in `PROVES`: the cell reads "its indexes and triggers held off
# while a load writes", and TiDB has no triggers to hold off while its
# indexes stay in place - MySQL's measured gain from dropping them is no
# measurement of TiDB (backlog P0-PARITY.all).

def test_a_load_into_tidb_sets_no_trigger_or_index_aside(tmp_path, server):
    eng = _engine(tmp_path)
    with eng.load_window("d", None, {"orders"}):
        with eng.table_index_window("d", "orders", lambda m: None):
            pass
    assert server.asked == []


def test_a_large_table_goes_in_with_its_indexes_in_place(tmp_path, server):
    """The copier asks the target for an index window only past a table's
    size; a TiDB target gives none."""
    from contextlib import ExitStack

    from migkit.engines.base import Engine
    eng = _engine(tmp_path)
    with ExitStack() as later:
        Engine.set_aside_indexes(later, eng, "d", "orders", {}, 10_000_000,
                                 lambda m: None)
    assert server.asked == []
