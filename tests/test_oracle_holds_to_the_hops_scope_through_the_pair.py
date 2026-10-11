"""An Oracle hop is held to the hop's scope in every path, without a server
(P0-PARITY.scope): the schema's catalogue answered in-process, the
comparison, the copy list, the repair and the change tail run by the
pair every engine compares through.

Oracle's own part is what it answers the pair: the tables it lists
(`neutral_tables`, through `Engine.in_scope`), their columns, their digest.
Everything it is asked of afterwards - which of them are read, and how - is
the pair's, held to the hop's include and exclude, its row filter, its name
rules and the operations it carries.

The statements read here are not new: each is one the engine already ran
against Oracle Free in `test_oracle_is_a_whole_side.py`.
"""
import pytest

from migkit import canon, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.base import LEFT_OUT, Result
from migkit.engines.hetero import _Accounts
from migkit.engines.oracle import OracleEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "oracle"),
    ("scope.copy", "oracle"),
    ("scope.repair", "oracle"),
    ("scope.ops", "oracle"),
    ("scope.row-filter", "oracle"),
    ("scope.names", "oracle"),
    ("verify.table", "oracle"),
    ("verify.repair-undo", "oracle"),
    ("schema.foreign-keys", "oracle"),
]

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")
COLUMNS = [("id", "NUMBER(10,0)"), ("region", "VARCHAR2(10 CHAR)")]


class Server:
    """The catalogue and the digests the engine's two sides answer."""

    def __init__(self, src=("ORDERS", "AUDIT_LOG"), dst=("ORDERS", "AUDIT_LOG")):
        self.tables = {"src": list(src), "dst": list(dst)}
        self.digests = []
        self.extra = {}

    def rows(self, engine, side, db, sql, args=None):
        for match, answer in self.extra.items():
            if match in sql:
                return answer(side) if callable(answer) else answer
        if "from all_tables" in sql:
            return [(t,) for t in self.tables[side]]
        raise AssertionError(f"not answered: {sql}")


@pytest.fixture
def server(monkeypatch):
    got = Server()
    monkeypatch.setattr(OracleEngine, "_rows",
                        lambda self, side, db, sql, args=None:
                        got.rows(self, side, db, sql, args))
    monkeypatch.setattr(OracleEngine, "neutral_columns",
                        lambda self, side, db, table: list(COLUMNS))
    monkeypatch.setattr(OracleEngine, "neutral_key",
                        lambda self, side, db, table: ["id"])
    monkeypatch.setattr(OracleEngine, "canonical_type",
                        lambda self, side, db, declared: "decimal")
    monkeypatch.setattr(OracleEngine, "_mojibake_repair",
                        lambda self, db: None)

    def no_log(self, side, db):
        raise RuntimeError("no redo is read in this test")
    monkeypatch.setattr(OracleEngine, "log_position", no_log)

    def digest(self, side, db, table, columns, where=None, salt=None):
        got.digests.append((side, table, where))
        return (3, "abc")
    monkeypatch.setattr(OracleEngine, "same_engine_digest", digest)
    monkeypatch.setattr(
        OracleEngine, "neutral_digest",
        lambda self, side, db, table, columns, where=None:
        (got.digests.append(("outside", side, table, where)) or (0, "x")))
    return got


def _hop(tmp_path, **kw):
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _eng(tmp_path, **kw):
    return OracleEngine(_hop(tmp_path, **kw))


# ---- the checks and the copy list ----------------------------------------

def test_the_checks_read_only_the_tables_the_hop_takes(server, tmp_path):
    eng = _eng(tmp_path, exclude=["audit_log"])
    # the source has a table the target does not, which the hop leaves out
    server.tables["dst"] = ["ORDERS"]
    for got in (eng.check_counts("app"), eng.check_data("app"),
                eng.check_schema("app")):
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    # a table the hop leaves out is read on neither side, and the one only
    # the source has is not a table missing from the target
    assert {t for _, t, _ in server.digests} == {"orders"}
    assert not [r for r in eng.check_schema("app") if "audit_log" in r.scope]


def test_a_hop_that_takes_everything_reads_every_table_as_before(
        server, tmp_path):
    server.tables["dst"] = ["ORDERS"]
    got = _eng(tmp_path).check_schema("app")
    assert {r.scope for r in got if r.status == "diff"} == {
        "app.audit_log"}, [r.detail for r in got]


def test_the_copy_lists_only_the_tables_the_hop_takes(server, tmp_path):
    eng = _eng(tmp_path, include=["app.orders"])
    assert eng.neutral_tables("src", "app") == ["orders"]
    assert eng.list_move_tables("app") == [("", "orders")]
    every = _eng(tmp_path)
    assert sorted(every.list_move_tables("app")) == [("", "audit_log"),
                                                     ("", "orders")]


def test_a_finding_names_its_table_as_the_source_names_it(server, tmp_path):
    got = _eng(tmp_path).check_data("app")
    assert sorted(r.table for r in got if r.status == "ok") == [
        "audit_log", "orders"]
    assert all(isinstance(r, Result) for r in got)


# ---- the row filter and the name rules -----------------------------------

def test_the_copy_and_the_check_read_through_the_row_filter(server, tmp_path):
    eng = _eng(tmp_path, mapping={"where": {"orders": "region = 'TH'"}},
               exclude=["audit_log"])
    assert [r.status for r in eng.check_counts("app")] == ["ok"]
    seen = {(side, table): where for side, table, where in
            [d for d in server.digests if d[0] != "outside"]}
    assert seen == {("src", "orders"): "region = 'TH'",
                    ("dst", "orders"): "region = 'TH'"}
    # a target row outside the filter is counted, not hidden by the filter
    assert ("outside", "dst", "orders", "(region = 'TH') is not true") \
        in server.digests
    assert eng._as_pair()._row_scope("app", "orders") == (
        "region = 'TH'", "region = 'TH'")
    assert _eng(tmp_path)._as_pair()._row_scope("app", "orders") == (
        None, None)


def test_a_renamed_table_is_read_and_copied_under_its_new_name(
        server, tmp_path):
    server.tables["dst"] = ["ORDERS_V2"]
    eng = _eng(tmp_path, mapping={"tables": {"orders": "orders_v2"}},
               exclude=["audit_log"])
    assert [r.status for r in eng.check_counts("app")] == ["ok"]
    assert [r.status for r in eng.check_data("app")] == ["ok"]
    assert ("src", "orders", None) in server.digests
    assert ("dst", "orders_v2", None) in server.digests
    assert eng.list_move_tables("app") == [("", "orders")]
    # without the rule the same two sides are two wrong findings
    bare = _eng(tmp_path, exclude=["audit_log"])
    said = " ".join(r.detail for r in bare.check_schema("app"))
    assert "on the source and not on the target" in said
    assert "on the target and not on the source" in said


# ---- the repair ----------------------------------------------------------

def test_a_row_repair_leaves_alone_a_table_the_hop_does_not_take(
        server, tmp_path):
    (tmp_path / "data-orders.missing").write_text('["1"]\n')
    (tmp_path / "data-audit_log.extra").write_text('["7"]\n')
    eng = _eng(tmp_path, exclude=["audit_log"])
    plan = eng.repair_plan("app", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    left = [a for a in plan if a.kind == LEFT_OUT]
    assert left and "audit_log" in left[0].note
    assert not left[0].statements and not left[0].undo
    # a hop that takes everything plans every drilldown
    plan = _eng(tmp_path).repair_plan("app", "rows")
    assert sorted(a.table for a in plan) == ["audit_log", "orders"]


def _sequences(server, src_next, dst_next, top):
    """The sequences and the identity columns of both sides: ORDERS.ID and
    a sequence of a table the hop leaves out."""
    server.extra.update({
        "from all_tab_identity_cols i join": [
            ("ORDERS", "ID", "ISEQ$1", dst_next),
            ("AUDIT_LOG", "ID", "ISEQ$2", 5)],
        "from all_tab_identity_cols where": [
            ("ORDERS", "ID", "ISEQ$1"), ("AUDIT_LOG", "ID", "ISEQ$2")],
        "from all_sequences where": lambda side: [
            ("ISEQ$1", src_next if side == "src" else dst_next),
            ("ISEQ$2", 900 if side == "src" else 5)],
        "data_default": [],
        "select max(": [(top,)]})


def test_every_sequence_a_repair_moves_is_put_back_where_it_was(
        server, tmp_path):
    _sequences(server, src_next=100, dst_next=40, top=70)
    eng = _eng(tmp_path, exclude=["audit_log"])
    plan = [a for a in eng.repair_plan("app", "sequences")
            if a.kind == "sequences"]
    assert len(plan) == 1
    act = plan[0]
    # moved to the source's next value, then past the rows the column holds
    assert any("start with 100" in s for s in act.statements)
    assert any("start with limit value" in s for s in act.statements)
    # and each put back to the 40 it handed out next: none left to a
    # statement with nothing to undo it
    assert len(act.undo) == len(act.statements)
    assert all("start with 40" in u for u in act.undo), act.undo
    # the sequence of the table the hop leaves out is not touched
    assert not [s for s in act.statements + act.undo if "AUDIT_LOG" in s]


def test_a_column_raised_past_its_rows_alone_is_undone_too(server, tmp_path):
    _sequences(server, src_next=40, dst_next=40, top=70)
    eng = _eng(tmp_path, exclude=["audit_log"])
    act = [a for a in eng.repair_plan("app", "sequences")
           if a.kind == "sequences"][0]
    assert [("limit value" in s) for s in act.statements] == [True]
    assert act.undo == [eng._restart_sql("app", "orders.id", 40)]
    assert "start with 40" in act.undo[0]


# ---- foreign keys --------------------------------------------------------

def test_foreign_keys_are_read_whole_with_their_columns_in_order(
        server, tmp_path):
    server.extra["c.constraint_type = 'R'"] = [
        ("FK_LINE", "LINES", "ORDER_ID", "ORDERS", "ID"),
        ("FK_PAIR", "PAIRS", "A", "KEYS", "X"),
        ("FK_PAIR", "PAIRS", "B", "KEYS", "Y")]
    got = _eng(tmp_path).foreign_keys("src", "app")
    assert got == [("lines", ["order_id"], "orders", ["id"]),
                   ("pairs", ["a", "b"], "keys", ["x", "y"])]


# ---- the change tail -----------------------------------------------------

def _change(op, i, table="orders"):
    row = {"id": i, "v": "x"}
    return canon.change(op, table, {"id": i}, None if op == "delete" else row)


def test_a_change_the_hop_does_not_carry_is_left_out_and_counted(
        server, tmp_path):
    pair = _eng(tmp_path, mapping={"ops": {"orders": "insert, update"}}
                )._as_pair()
    acc = _Accounts(tailtables.Store.of(tmp_path))
    batch = [_change("insert", 1), _change("delete", 2),
             _change("update", 3), _change("delete", 4, "audit_log")]
    acc.read(batch)
    out, _ = pair._to_target("app", batch, {}, acc)
    assert [(c["op"], c["key"]["id"]) for c in out] == [
        ("insert", 1), ("update", 3), ("delete", 4)]
    acc.check("app", out, "0/1")
    assert acc.rows["orders"][tailtables.NOT_CARRIED] == 1
    # the pair an Oracle hop's tail runs is Oracle on both sides
    assert type(pair.src_engine) is OracleEngine \
        and type(pair.dst_engine) is OracleEngine
