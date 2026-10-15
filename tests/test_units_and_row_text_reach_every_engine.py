"""Units everywhere and the verify ladder on every engine (backlog
P0-ETA-ALL, P0-VERIFY-EXECUTION): a deep plan's range workers open their
own part units under their table, a pair's inner engines keep a ledger
file each, the row text way compares rows here and names the first key
held apart, and SQL Server's charset check reads in the one value plan.

Pure: the servers' answers are given in-process.
"""
import decimal
import json
from types import SimpleNamespace

import pytest

from migkit import attempts, decide, scanplan
from migkit.engines.base import Engine
from migkit.engines.mssql import MSSQLEngine

PROVES = [("verify.value.one-plan", ("mssql",))]


class UnitEngine:
    attempt_unit = Engine.attempt_unit
    unit_now = Engine.unit_now
    work_unit = Engine.work_unit

    def __init__(self, tmp_path, cuts):
        self.mon = attempts.Monitor(None, attempts.Ledger(), thread=False)
        self.hop = SimpleNamespace(report_dir=lambda db=None: tmp_path)
        self.scan_cuts = lambda db, t, most: cuts

    def attempts(self):
        return self.mon


def test_a_deep_plans_range_parts_roll_up_into_their_table(tmp_path,
                                                           monkeypatch):
    monkeypatch.setattr(scanplan, "plan_most", lambda eng, db: 2)
    eng = UnitEngine(tmp_path, ["k < 5", "k >= 5"])
    agg = scanplan.Aggregate("nullempty", "t", "c", ("sum(1)",))
    got = scanplan.for_engine(eng, "app", [agg], source=lambda s, t: t,
                              query=lambda side, sql: [(3, 3)])
    assert got.value("src", agg) == (6,)
    records = eng.mon.ledger.records
    assert records["app.t"]["state"] == "done"
    assert records["app.t"]["kind"] == "deep"
    parts = {n: r for n, r in records.items() if r["parent"] == "app.t"}
    # two ranges a side, each its own part of the table
    assert len(parts) == 4
    assert all(r["level"] == "range" and r["parts"] == 4
               and r["state"] == "done" for r in parts.values())


def test_a_failed_range_leaves_its_part_owed(tmp_path, monkeypatch):
    monkeypatch.setattr(scanplan, "plan_most", lambda eng, db: 2)
    eng = UnitEngine(tmp_path, ["k < 5", "k >= 5"])
    agg = scanplan.Aggregate("nullempty", "t", "c", ("sum(1)",))

    def query(side, sql):
        if side == "dst" and "k >= 5" in sql:
            raise RuntimeError("refused")
        return [(3, 3)]
    scanplan.for_engine(eng, "app", [agg], source=lambda s, t: t,
                        query=query)
    failed = [r for r in eng.mon.ledger.records.values()
              if r["parent"] == "app.t" and r["state"] == "failed"]
    assert failed and all(r["reason"] == "RuntimeError" for r in failed)


class Inner:
    attempts = Engine.attempts
    work_unit = Engine.work_unit
    attempt_unit = Engine.attempt_unit

    def __init__(self, name, hop):
        self.CANON_ENGINE = name
        self.hop = hop


def _hop(tmp_path, engine="hetero"):
    return SimpleNamespace(engine=engine, report_dir=lambda db=None: tmp_path,
                           options={"source_engine": "mysql",
                                    "target_engine": "postgres"})


def test_a_pairs_inner_engines_keep_a_ledger_file_each(tmp_path):
    hop = _hop(tmp_path)
    src, dst = Inner("mysql", hop), Inner("postgres", hop)
    assert attempts.pair_side(src) == "src"
    assert attempts.pair_side(dst) == "dst"
    assert attempts.ledger_path(src).name == "attempts.src-mysql.json"
    assert attempts.ledger_path(dst).name == "attempts.dst-postgres.json"
    with src.work_unit("move", "s.a"):
        pass
    with dst.work_unit("check", "s.a"):
        pass
    kept_src = json.loads((tmp_path / "attempts.src-mysql.json").read_text())
    kept_dst = json.loads((tmp_path / "attempts.dst-postgres.json")
                          .read_text())
    # the same unit name on both sides, neither overwriting the other
    assert kept_src["units"]["s.a"]["kind"] == "move"
    assert kept_dst["units"]["s.a"]["kind"] == "check"
    assert not (tmp_path / "attempts.json").exists()


def test_a_single_engine_hop_keeps_the_one_file(tmp_path):
    eng = Inner("postgres", _hop(tmp_path, engine="postgres"))
    assert attempts.pair_side(eng) == ""
    assert attempts.ledger_path(eng) == tmp_path / "attempts.json"


def test_a_reader_without_a_ledger_opens_no_unit():
    with attempts.work_unit(object(), "deep", "app.t"):
        pass


def _sides(src, dst):
    held = {"src": src, "dst": dst}

    def rows(side, pred):
        if not pred:
            return list(held[side])
        high = pred == "k >= 5"
        return [r for r in held[side] if (r[0] >= 5) == high]
    return rows


ROWS = [(i, f"v{i}", decimal.Decimal("1.50")) for i in range(10)]


def test_row_text_finds_rows_alike_whatever_a_server_renders():
    renders = [(i, v, decimal.Decimal("1.5")) for i, v, _ in ROWS]
    got = scanplan.row_text(_sides(ROWS, renders), ["k < 5", "k >= 5"],
                            most=2)
    assert got.alike and got.rows == {"src": 10, "dst": 10}
    assert len(got.digests) == 2
    assert got.digests[0]["src"] == got.digests[0]["dst"]
    assert ": OK rows=10" in scanplan.row_text_line("t", got)


def test_row_text_names_the_first_key_held_apart():
    changed = [r if r[0] != 7 else (7, "other", r[2]) for r in ROWS]
    got = scanplan.row_text(_sides(ROWS, changed), ["k < 5", "k >= 5"],
                            most=2)
    assert not got.alike
    assert got.first == ("k >= 5", ("7",), {"src": "differs",
                                            "dst": "differs"})
    line = scanplan.row_text_line("t", got)
    assert ": DIFF" in line and "first key=7" in line


def test_row_text_names_a_row_one_side_lacks():
    missing = [r for r in ROWS if r[0] != 9]
    got = scanplan.row_text(_sides(ROWS, missing), ["k < 5", "k >= 5"])
    assert got.rows == {"src": 10, "dst": 9}
    assert got.first[1] == ("9",)
    assert got.first[2] == {"src": "differs", "dst": "absent"}


def test_row_text_tells_null_from_the_empty_text():
    assert scanplan.canon_text(None) != scanplan.canon_text("")
    got = scanplan.row_text(_sides([(1, None)], [(1, "")]))
    assert not got.alike


def _mssql(read):
    eng = MSSQLEngine.__new__(MSSQLEngine)
    eng._charset_read = lambda db: read
    eng._columns_named = lambda db, kinds: []
    return eng


READ = {s: {("dbo.t", "name"): ("nvarchar", 0),
            ("dbo.t", "note"): ("varchar", 1252),
            ("dbo.t", "body"): ("ntext", 0)} for s in ("src", "dst")}


def test_sql_server_reads_charset_in_the_one_value_plan():
    eng = _mssql(READ)
    asked = []
    eng._value_scan = lambda db, aggs: asked.append(list(aggs)) or "plan"
    assert eng._deep_scans("app") == "plan"
    assert {a.check for a in asked[0]} == {"charset"}
    # an ntext column is not counted: charindex does not read it
    assert {a.name for a in asked[0]} == {"name", "note"}
    unicode_agg = next(a for a in asked[0] if a.name == "name")
    assert "nchar(65533)" in unicode_agg.exprs[0]
    code_page = next(a for a in asked[0] if a.name == "note")
    assert "'?'" in code_page.exprs[0]


class Scans:
    def __init__(self, aggs, src, dst):
        self.aggs, self.src, self.dst = aggs, src, dst

    def of(self, check):
        return [a for a in self.aggs if a.check == check]

    def value(self, side, agg):
        return ((self.src if side == "src" else self.dst)[agg.name],)

    def failure(self, agg):
        return None


def test_sql_server_charset_says_from_the_shared_plan_alone():
    eng = _mssql(READ)

    def no_second_read(db, aggs):
        raise AssertionError("the charset check read the table again")
    eng._value_scan = no_second_read
    aggs = eng._charset_aggregates(eng._charset_kinds(READ))
    ok = eng._deep_charset("app", Scans(aggs, {"name": 0, "note": 0},
                                        {"name": 0, "note": 0}))
    assert ok.status == "ok"
    bad = eng._deep_charset("app", Scans(aggs, {"name": 0, "note": 0},
                                         {"name": 3, "note": 0}))
    assert bad.status == "diff" and "dbo.t.name: 3 more rows" in bad.detail
    assert "U+FFFD" in bad.detail


def test_sql_server_row_parity_climbs_to_row_text():
    eng = MSSQLEngine.__new__(MSSQLEngine)
    eng._qualified = lambda side, db, t: t
    held = {"src": [['{"id":1}'], ['{"id":2}']],
            "dst": [['{"id":1}'], ['{"id":3}']]}

    def cmd(side, db, sql):
        if "hashbytes" in sql:
            raise RuntimeError("the row is too wide to hash")
        assert sql.endswith("order by 1")
        return held[side]
    eng._cmd = cmd
    a, b = eng._row_parity("app", "dbo.t", "j", "select hashbytes(1)", "")
    assert a[0] == b[0] == 2 and a != b
    held["dst"] = held["src"]
    a, b = eng._row_parity("app", "dbo.t", "j", "select hashbytes(1)", "")
    assert a == b


def test_sql_server_row_parity_says_when_no_way_is_left():
    eng = MSSQLEngine.__new__(MSSQLEngine)
    eng._qualified = lambda side, db, t: t

    def cmd(side, db, sql):
        raise RuntimeError("refused")
    eng._cmd = cmd
    with pytest.raises(decide.Exhausted):
        eng._row_parity("app", "dbo.t", "j", "select hashbytes(1)", "")
