"""The second reader asked everything it can answer, and its answers held
to what they can and cannot prove (backlog 0f).

No server here: what each reading asks for, how every answer becomes a
verdict, the narrowing that can never lose a row, and the explanation of
a small differing set. The readings against real servers are in
`test_the_second_reading_narrows_and_names_rows.py`.
"""
import pandas as pd
import pytest

from migkit import second_reader as sr
from migkit.config import Endpoint, Hop
from tests.test_the_report_does_not_name_its_tools import TOOLS

LEAKS = TOOLS + ("dvt", "data-validation", "data_validation")


def _clean(results):
    said = " ".join(f"{r.check} {r.scope} {r.detail} {r.fix_hint}"
                    for r in results).lower()
    return [t for t in LEAKS if t in said]


def _args(validations):
    return [v["args"] for v in validations]


COLS = [("id", "integer"), ("n", "integer"), ("d", "decimal"),
        ("f", "float"), ("s", "text"), ("ts", "timestamp")]


# --- what each reading asks ------------------------------------------------

def test_the_aggregates_ask_what_each_type_allows_and_nothing_else():
    first, more = sr.aggregates("column:public.t", ["public.t=public.t"],
                                COLS)
    assert first["args"][:8] == ["--count", "*", "--sum", "*", "--min", "*",
                                 "--max", "*"]
    typed = dict(zip(more["args"][::2], more["args"][1::2]))
    assert typed == {"--avg": "id,n,d", "--bit_xor": "id,n",
                     "--std": "id,n,d"}, typed
    # never of a float, whose sum in another order is another number
    assert "f" not in ",".join(typed.values()).split(",")
    # nothing the hop did not name
    for args in _args([first, more]):
        assert "--threshold" not in args and "--filter-status" not in args
        assert "--exclude-columns" not in args


def test_across_engines_the_same_typed_measures_measured_to_agree():
    got = sr.aggregates("column:app.t", ["app.t=public.t"], COLS,
                        same_engine=False)
    typed = dict(zip(got[1]["args"][::2], got[1]["args"][1::2]))
    assert typed == {"--avg": "id,n,d", "--bit_xor": "id,n",
                     "--std": "id,n,d"}, typed


def test_a_tolerance_only_where_the_hop_names_one():
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint(), mapping={"tolerance": {"public.t": 0.5}})
    assert hop.tolerance("app", "public", "t") == 0.5
    assert hop.tolerance("app", "public", "other") is None
    got = sr.aggregates("c", ["public.t"], COLS,
                        tolerance=hop.tolerance("app", "public", "t"))
    for args in _args(got):
        assert args[args.index("--threshold") + 1] == "0.5"
    assert "--threshold" not in sr.aggregates("c", ["public.t"], COLS)[0][
        "args"]
    # and a tolerance rule that matches no table is named like any other
    assert hop.unused_mapping(["public.x"]) == ["public.t"]


def test_the_hops_dropped_columns_are_left_out_of_every_measure():
    first, more = sr.aggregates("c", ["public.t"], COLS, drop=("n", "s"))
    assert first["args"][:9] == ["--count", "n,s", "--sum", "n,s", "--min",
                                 "n,s", "--max", "n,s", "--exclude-columns"]
    # measured: the reader refuses `*` beside `--exclude-columns`
    assert "*" not in first["args"]
    assert "n" not in ",".join(more["args"][1::2]).split(",")


def test_a_grouped_reading_asks_for_its_failures_and_what_groups_can_see():
    same = sr.aggregates("group:public.t", ["public.t"], COLS, grouped="g",
                         where=("x > 1", "x > 1"), only_failures=True)
    assert len(same) == 1, "no typed aggregates per group"
    args = same[0]["args"]
    for flag in ("--grouped-columns", "--filter-status", "--filters",
                 "--wildcard-include-string-len",
                 "--wildcard-include-timestamp"):
        assert flag in args, (flag, args)
    assert args[args.index("--filters") + 1] == "x > 1"
    across = sr.aggregates("group:app.t", ["app.t=public.t"], COLS,
                           same_engine=False, grouped="g",
                           where=("a = 1", "b = 1"))[0]["args"]
    # measured: a char column's length is padded on one side only
    assert "--wildcard-include-string-len" not in across
    assert across[across.index("--filters") + 1] == "a = 1:b = 1"


def test_the_row_reading_is_a_hash_within_an_engine_and_values_across():
    v = sr.rows("row:public.t", ["public.t=public.t"], ["id"], COLS)
    assert v["kind"] == "row"
    assert v["args"][:4] == ["--primary-keys", "id", "--hash", "*"]
    assert v["args"][-2:] == ["--filter-status", "fail"]
    across = sr.rows("row:app.t", ["app.t=public.t"], ["id"], COLS,
                     same_engine=False)
    fields = across["args"][across["args"].index("--comparison-fields") + 1]
    # measured: the hash called -1e-07 different across engines
    assert fields == "n,d,s,ts", fields
    assert "--hash" not in across["args"]
    assert sr.rows("r", ["t"], ["id"], [("id", "integer"), ("f", "float")],
                   same_engine=False) is None
    dropped = sr.rows("r", ["t"], ["id"], COLS, drop=("s",))
    assert dropped["args"][:5] == ["--primary-keys", "id", "--hash", "s",
                                   "--exclude-columns"]


def test_fixed_width_text_is_not_read_value_by_value_across_engines():
    """Measured: a `char(5)` holding `c1` reads `c1` from MySQL and `c1   `
    from PostgreSQL - a false difference on every row."""
    for declared in ("char(5)", "character(5)", "bpchar", "CHAR(1)"):
        assert sr.padded(declared), declared
    for declared in ("varchar(5)", "character varying(5)", "text", None):
        assert not sr.padded(declared), declared
    v = sr.rows("row:t", ["t"], ["id"], COLS + [("c", "text")],
                same_engine=False, fixed=["c"])
    fields = v["args"][v["args"].index("--comparison-fields") + 1]
    assert fields == "n,d,s,ts", fields
    ok = {"ok": True, "results": [],
          "ran": [{"label": "row:t", "table": "t"}]}
    said = sr.findings(ok, "app", {"validations": [v]},
                       same_engine=False)[0].detail
    assert sr.ACROSS in said and sr.HASHED not in said, said


def test_case_is_ignored_only_when_the_hop_asks():
    assert "--case-insensitive-match" not in sr.rows(
        "r", ["t"], ["id"], COLS)["args"]
    assert "--case-insensitive-match" in sr.rows(
        "r", ["t"], ["id"], COLS, case_insensitive=True)["args"]
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint())
    assert sr.asked(hop) == {}
    hop.options = {"two_readings": True}
    assert sr.asked(hop) == {"rows": True}
    hop.options = {"two_readings": {"sample": 200,
                                    "case_insensitive": True}}
    assert sr.asked(hop) == {"rows": True, "sample": 200,
                             "case_insensitive": True}
    hop.options = {"two_readings": {"rows": False}}
    assert sr.asked(hop) == {"rows": False, "sample": None,
                             "case_insensitive": False}
    # every form of it is one the configuration check takes
    from migkit import hopkeys
    key = hopkeys.HOP_OPTIONS["two_readings"]
    assert set(key.subs) == {"rows", "sample", "case_insensitive"}
    assert "tolerance" in hopkeys.HOP_KEYS["mapping"].subs


def test_a_sample_is_counted_rather_than_filtered():
    v = sr.rows("sample:t", ["t"], ["id"], COLS, sample=500)
    assert v["args"][v["args"].index("--random-row-batch-size") + 1] == "500"
    assert "--use-random-row" in v["args"]
    assert "--filter-status" not in v["args"]


def test_a_view_is_read_by_the_same_query_on_both_sides():
    v = sr.custom_query("view:v", "select * from a.v", "select * from b.v")
    assert v["kind"] == "custom-query column"
    assert v["args"][:4] == ["--source-query", "select * from a.v",
                             "--target-query", "select * from b.v"]


# --- what came back ---------------------------------------------------------

def _row(label, table, status, **kw):
    return dict({"migkit_label": label, "source_table_name": table,
                 "validation_name": kw.pop("name", "count"),
                 "validation_status": status, "source_agg_value": 3,
                 "target_agg_value": 3}, **kw)


def _job(*validations):
    return {"validations": list(validations)}


def test_a_sample_that_agrees_is_never_a_pass():
    job = _job(sr.rows("sample:public.t", ["public.t=public.t"], ["id"], COLS,
                       sample=50))
    answer = {"ok": True, "ran": [{"label": "sample:public.t",
                                   "table": "public.t"}],
              "results": [_row("sample:public.t", "public.t", "success",
                               primary_keys='{"id": 1}')] * 50}
    got = sr.findings(answer, "app", job)
    assert [r.status for r in got] == ["skip"], got
    assert "not a verdict" in got[0].detail and "50 rows" in got[0].detail
    assert not _clean(got)
    answer["results"] = answer["results"][:49] + [
        _row("sample:public.t", "public.t", "fail",
             primary_keys='{"id": 7}')]
    got = sr.findings(answer, "app", job)
    assert [r.status for r in got] == ["diff"], got
    assert "1 of 50" in got[0].detail and "7" in got[0].detail
    assert "early signal" in got[0].detail


def test_a_row_reading_that_found_nothing_to_report_is_ok_only_if_it_ran():
    job = _job(sr.rows("row:public.t", ["public.t=public.t"], ["id"], COLS))
    ran = {"ok": True, "results": [],
           "ran": [{"label": "row:public.t", "table": "public.t"}]}
    got = sr.findings(ran, "app", job)
    assert [(r.scope, r.status) for r in got] == [("app.public.t rows",
                                                   "ok")]
    # and it says what the hash does to values before it compares them
    assert sr.HASHED in got[0].detail
    for silent in ({"ok": True, "results": [], "ran": []},
                   {"ok": True, "results": [], "ran": [],
                    "broken": [{"label": "row:public.t",
                                "table": "public.t",
                                "error": "NoSuchTableError"}]}):
        got = sr.findings(silent, "app", job)
        assert [r.status for r in got] == ["error"], (silent, got)
        assert "not the same as both readings agreeing" in got[0].detail


def test_differing_rows_are_named_by_key():
    job = _job(sr.rows("row:public.t", ["public.t=public.t"], ["a", "b"],
                       COLS))
    answer = {"ok": True,
              "ran": [{"label": "row:public.t", "table": "public.t"}],
              "results": [_row("row:public.t", "public.t", "fail",
                               group_by_columns='{"a": 1, "b": "x"}'),
                          _row("row:public.t", "public.t", "fail",
                               group_by_columns='{"a": 2, "b": "y"}')]}
    got = sr.findings(answer, "app", job)
    assert [r.status for r in got] == ["diff"]
    assert "1/x, 2/y" in got[0].detail, got[0].detail
    assert sr.failing_keys(answer, "row:public.t") == [("1", "x"),
                                                        ("2", "y")]


def test_one_broken_table_does_not_pass_or_sink_the_others():
    job = _job(*sr.aggregates("column:public.a", ["public.a"], []),
               *sr.aggregates("column:public.b", ["public.b"], []))
    answer = {"ok": True,
              "ran": [{"label": "column:public.a", "table": "public.a"}],
              "broken": [{"label": "column:public.b", "table": None,
                          "error": "NoSuchTableError: b"}],
              "results": [_row("column:public.a", "public.a", "success")]}
    got = {r.scope: r.status for r in sr.findings(answer, "app", job)}
    assert got == {"app.public.a": "ok", "app.public.b": "error"}, got


def test_a_drift_inside_the_hops_tolerance_is_said():
    job = _job(*sr.aggregates("column:public.t", ["public.t"], [],
                              tolerance=1))
    answer = {"ok": True,
              "ran": [{"label": "column:public.t", "table": "public.t"}],
              "results": [_row("column:public.t", "public.t", "success",
                               name="sum__f", source_agg_value=100.0,
                               target_agg_value=100.5, difference=0.5,
                               pct_threshold=1.0)]}
    got = sr.findings(answer, "app", job)
    assert [r.status for r in got] == ["ok"]
    assert "within the hop's tolerance of 1.0%" in got[0].detail
    assert "source 100.0 target 100.5" in got[0].detail


def test_a_view_answer_is_its_own_verdict():
    job = _job(sr.custom_query("view:v_orders", "select 1", "select 1"))
    answer = {"ok": True, "ran": [{"label": "view:v_orders",
                                   "table": "view:v_orders"}],
              "results": [_row("view:v_orders", None, "fail",
                               name="sum__n", source_agg_value=1,
                               target_agg_value=2)]}
    got = sr.findings(answer, "app", job)
    assert [(r.scope, r.status) for r in got] == [("app view v_orders",
                                                   "diff")]


def test_two_readings_of_a_differing_tables_rows_never_pass_it():
    same = sr.compared("app", "t", [("1",), ("2",)], [("2",), ("1",)], True)
    assert same.status == "diff" and "the same 2 rows" in same.detail
    more = sr.compared("app", "t", [("1",)], [("1",), ("9",)], True)
    assert more.status == "diff" and "two answers" in more.detail
    assert "9" in more.detail and "migkit " in more.fix_hint
    blind = sr.compared("app", "t", [("1",), ("3",)], [("1",)], True)
    assert blind.status == "diff" and "cannot see" in blind.detail
    assert "3" in blind.detail
    not_read = sr.compared("app", "t", [("1",)], [], False)
    assert not_read.status == "error"
    counted = sr.compared("app", "t", [("1",)], [("x",), ("y",)], True,
                          by_value=False)
    assert counted.status == "diff" and "two answers" in counted.detail
    fewer = sr.compared("app", "t", [("1",), ("2",)], [("x",)], True,
                        by_value=False)
    assert fewer.status == "diff" and "cannot see" in fewer.detail
    alike = sr.compared("app", "t", [("1",)], [("x",)], True,
                        by_value=False)
    assert alike.status == "diff" and "as many as" in alike.detail
    every = (same, more, blind, not_read, counted, fewer, alike)
    for r in every:
        assert r.status != "ok", r
    assert not _clean(every)


# --- narrowing, which must never lose a row ---------------------------------

def test_the_group_column_is_the_one_that_rules_out_the_most():
    cands = [("id", "integer", 1_000_000), ("g", "integer", 40),
             ("region", "text", 900), ("flag", "boolean", 2),
             ("tiny", "integer", 2), ("wide", "integer", 50_000),
             ("never", "integer", None)]
    assert sr.group_column(cands, ["id"]) == ("region", "text", 900)
    # across engines text groups by two collations
    assert sr.group_column(cands, ["id"], same_engine=False) == (
        "g", "integer", 40)
    # a single-column key's groups are rows; a composite key's first part
    # is a fine group
    assert sr.group_column([("id", "integer", 100)], ["id"]) is None
    assert sr.group_column([("a", "integer", 100)], ["a", "b"]) == (
        "a", "integer", 100)
    assert sr.group_column([], ["id"]) is None
    # a column nearly as distinct as the rows groups them one or two a
    # group: on 3,000 rows, 900 values is no group, and 40 is
    assert sr.group_column(cands, ["id"], rows=3000) == ("g", "integer", 40)
    assert sr.group_column(cands, ["id"], rows=90_000) == (
        "region", "text", 900)


def test_group_values_come_back_as_sql_can_take_them():
    answer = {"results": [
        _row("g", "t", "fail", group_by_columns='{"g": "3"}'),
        _row("g", "t", "fail", group_by_columns='{"g": "7.0"}'),
        _row("g", "t", "fail", group_by_columns='{"g": null}'),
        _row("g", "t", "success", group_by_columns='{"g": "5"}')]}
    assert sr._group_values(answer, "g", "g", "integer") == (["3", "7"],
                                                              True)
    answer["results"].append(_row("g", "t", "fail",
                                  group_by_columns='{"g": "x"}'))
    assert sr._group_values(answer, "g", "g", "integer") is None


@pytest.fixture
def duck():
    import duckdb
    con = duckdb.connect()
    con.execute("create table t (id int, g int, s varchar)")
    con.execute("insert into t select i, case when i % 7 = 0 then null"
                " else i % 5 end, 'v' || i from range(1, 101) r(i)")
    return con


def _engines(tmp_path=None):
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint())
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop), MySQLEngine(hop)


@pytest.mark.parametrize("values,nulls", [(["1", "3"], False),
                                          (["2"], True), ([], True)])
def test_inside_and_outside_split_every_row_once(duck, values, nulls):
    """The two predicates partition the table, NULLs included - the
    outside is what the digest proves, so a row in neither would be a row
    nobody looked at."""
    pg, _ = _engines()
    n = sr.Narrowing("g", "integer", values, nulls, 5)
    total = duck.execute("select count(*) from t").fetchone()[0]
    inside = duck.execute(f"select count(*) from t where {n.inside(pg)}"
                          ).fetchone()[0]
    outside = duck.execute(f"select count(*) from t where {n.outside(pg)}"
                           ).fetchone()[0]
    both = duck.execute(f"select count(*) from t where {n.inside(pg)} and"
                        f" {n.outside(pg)}").fetchone()[0]
    assert inside + outside == total and both == 0
    assert inside > 0
    filtered = duck.execute(f"select count(*) from t where"
                            f" {n.inside(pg, 'id <= 50')}").fetchone()[0]
    assert 0 < filtered < inside


def test_text_group_values_are_written_in_each_sides_quoting():
    pg, my = _engines()
    n = sr.Narrowing("region", "text", ["it's", "a\\b"], False, 9)
    assert "'it''s'" in n.inside(pg) and "'a\\b'" in n.inside(pg)
    assert "`region`" in n.inside(my) and "'a\\\\b'" in n.inside(my)


def _walker(differ_in):
    """A fake engine: rows 1..100 in groups by id % 5; `differ_in` the
    ids that differ. `walk` answers only rows its predicate selects."""
    def rows_of(where):
        if where.startswith("("):
            return [i for i in range(1, 101) if i % 5 in (1, 3)]
        return [i for i in range(1, 101) if i % 5 not in (1, 3)]

    def walk(src_where, dst_where):
        assert src_where == dst_where
        return [], [], [i for i in rows_of(src_where) if i in differ_in]

    def same(src_where, dst_where):
        return not [i for i in rows_of(src_where) if i in differ_in]
    return walk, same


class _Quote:
    _quote_ident = staticmethod(lambda n: n)
    _quote_literal = staticmethod(lambda v: f"'{v}'")


def test_a_difference_the_groups_did_not_show_is_still_walked():
    n = sr.Narrowing("g", "integer", ["1", "3"], False, 5)
    # 11 is in group 1; 12 is in group 2, which the aggregates passed
    walk, same = _walker({11, 12})
    missing, extra, changed, said, only = sr.walk_narrowed(
        n, walk, same, (_Quote, _Quote))
    assert sorted(changed) == [11, 12] and only is False
    assert "walked as well" in said
    # where the rest is proved equal, only the groups are walked
    walk, same = _walker({11, 13})
    _, _, changed, said, only = sr.walk_narrowed(n, walk, same,
                                                 (_Quote, _Quote))
    assert sorted(changed) == [11, 13] and only is True
    assert "proved equal" in said and "2 of about 5 groups" in said
    # and groups that differ by their measures but hold no differing row
    # send the walk to the rest
    walk, same = _walker({12})
    _, _, changed, _, only = sr.walk_narrowed(n, walk, same,
                                              (_Quote, _Quote))
    assert changed == [12] and only is False


def test_nothing_changes_where_no_second_reading_can_be_had(monkeypatch,
                                                            tmp_path):
    from migkit import leaves
    pg, _ = _engines(tmp_path)
    monkeypatch.setattr(sr, "interpreter", lambda: None)
    # nor the bisecting reader: no second reading of any kind here
    monkeypatch.setattr(leaves, "bisection_reader", lambda e: False)
    called = []
    whole = (["m"], [], ["c"])
    assert pg._narrowed_walk("app", "public.t", "public.t", ["id"],
                             lambda *a: called.append(a),
                             lambda *a: called.append(a),
                             lambda: whole, 10_000) is whole
    # the engine's own walk, and nothing said, kept or measured
    assert called == [] and pg._narrowed_note("app", "public.t") == ""
    assert not (tmp_path / "throughput.json").exists()
    assert pg._read_rows_twice("app", "public.t", "public.t", ["id"],
                               ["integer"], [("1",)], 10) is None
    assert pg._second_reading("app", "public.t", "public.t", ["id"],
                              ["integer"], [("1",)], 10) is None
    assert "second" not in pg.planned_checks()
    pg.hop.options = {"two_readings": True}
    assert "second" not in pg.planned_checks()
    monkeypatch.setattr(sr, "interpreter", lambda: "/x/python")
    assert "second" in pg.planned_checks()
    pg.hop.options = {}
    assert "second" not in pg.planned_checks()


def test_a_table_too_large_to_read_twice_row_by_row_says_so(monkeypatch):
    pg, _ = _engines()
    monkeypatch.setattr(sr, "interpreter", lambda: "/x/python")
    monkeypatch.setattr(sr, "run", lambda *a, **k: pytest.fail("read"))
    monkeypatch.setattr(sr, "ROWS_MOST", 1000)
    got = pg._read_rows_twice("app", "public.t", "public.t", ["id"],
                              ["integer"], [("1",)], 5000)
    assert got.status == "skip" and "5,000 rows" in got.detail
    assert not _clean([got])


def _narrowed(pg, walk, same, whole, rows=10_000):
    return pg._narrowed_walk("app", "public.t", "public.t", ["id"], walk,
                             same, whole, rows)


def test_the_walk_is_chosen_by_what_each_way_cost(monkeypatch, tmp_path):
    """Never timed, the groups go first where they hold; once both are
    timed the cheaper wins - the owner's rule for every choice
    (`decide`)."""
    import time as _time
    pg, _ = _engines(tmp_path)
    monkeypatch.setattr(sr, "interpreter", lambda: "/x/python")
    monkeypatch.setattr(pg, "_group_for",
                        lambda *a: ("g", "integer", 5))
    narrowing = sr.Narrowing("g", "integer", ["1", "3"], False, 5)
    monkeypatch.setattr(sr, "narrow", lambda *a, **k: narrowing)
    groups_walk, same = _walker({11, 13})
    wholes = []

    def walk(*a):
        _time.sleep(0.01)
        return groups_walk(*a)

    def whole():
        wholes.append(1)
        _time.sleep(0.2)
        return [], [], [11, 13]
    got = _narrowed(pg, walk, same, whole)
    assert sorted(got[2]) == [11, 13] and wholes == []
    assert "2 of about 5 groups" in pg._narrowed_note("app", "public.t")
    # the walk never timed: tried once, to get its number
    got = _narrowed(pg, walk, same, whole)
    assert sorted(got[2]) == [11, 13] and wholes == [1]
    assert pg._narrowed_note("app", "public.t").startswith(
        "every row walked")
    # both timed, and the groups measured the cheaper
    got = _narrowed(pg, walk, same, whole)
    assert wholes == [1], "the dearer walk was taken again"


def test_groups_that_cannot_be_read_fall_to_the_walk(monkeypatch, tmp_path):
    pg, _ = _engines(tmp_path)
    monkeypatch.setattr(sr, "interpreter", lambda: "/x/python")
    monkeypatch.setattr(pg, "_group_for", lambda *a: ("g", "integer", 5))
    monkeypatch.setattr(sr, "narrow",
                        lambda *a, **k: "its groups could not be read")
    got = _narrowed(pg, *_walker({12}), lambda: ([], [], [12]))
    assert got == ([], [], [12])
    note = pg._narrowed_note("app", "public.t")
    assert "could not be read" in note, note
    # and no column to group by is the walk, said
    monkeypatch.setattr(pg, "_group_for", lambda *a: None)
    pg2, _ = _engines(tmp_path / "other")
    monkeypatch.setattr(pg2, "_group_for", lambda *a: None)
    got = _narrowed(pg2, *_walker({12}), lambda: ([], [], [12]))
    assert got == ([], [], [12])
    assert "statistics" in pg2._narrowed_note("app", "public.t")


def test_the_second_reading_of_rows_is_the_verify_passes_choice(
        monkeypatch, tmp_path):
    """The rung list is the verify passes' own (`leaves.PASSES`): the row
    reader where it is here, the bisecting reader where only it is, and
    a table read twice on the way to its rows is not read again."""
    from migkit import leaves
    from migkit.engines.base import Result
    pg, _ = _engines(tmp_path)
    asked = []
    monkeypatch.setattr(pg, "_read_rows_twice",
                        lambda *a, **k: asked.append("row hash") or
                        Result(sr.CHECK, "x", "diff", "held"))
    monkeypatch.setattr(leaves, "bisection_reading",
                        lambda *a, **k: asked.append("bisection") or
                        "; read a second way by bisection: 1")
    monkeypatch.setattr(sr, "interpreter", lambda: "/x/python")
    monkeypatch.setattr(leaves, "bisection_reader", lambda e: True)
    got = pg._second_reading("app", "public.t", "public.t", ["id"],
                             ["integer"], [("1",)], 10)
    assert got.detail == "held" and asked == ["row hash"]
    assert pg._second_reading("app", "public.t", "public.t", ["id"],
                              ["integer"], [("1",)], 10) is None
    assert asked == ["row hash"]
    monkeypatch.setattr(sr, "interpreter", lambda: None)
    got = pg._second_reading("app", "public.u", "public.u", ["id"],
                             ["integer"], [("1",)], 10)
    assert asked == ["row hash", "bisection"]
    assert got.status == "diff" and "by bisection" in got.detail
    assert got.scope == "app.public.u rows"
    monkeypatch.setattr(leaves, "bisection_reader", lambda e: False)
    assert pg._second_reading("app", "public.v", "public.v", ["id"],
                              ["integer"], [("1",)], 10) is None


def test_the_groups_are_a_localizer_the_verify_passes_can_climb_to():
    """A rung of `leaves.PASSES`, after the leaves in the list: tried where
    the leaves were timed and it was not, held to the totals like them."""
    from migkit import decide, leaves
    part = next(p for p in leaves.PASSES if p.name == "localize")
    names = [r.name for r in part.rungs]
    assert names.index("groups") > names.index("leaf reads")
    facts = {"localizer": None, "row hash reader": True,
             "group column": ("g", "integer", 40), "expected": None,
             "same engine": False}
    costs = decide.Costs({"leaf reads": 0.001})
    got = decide.climb("t", part.rungs, facts, costs=costs)
    assert got.path == "groups", got
    facts["group column"] = None
    got = decide.climb("t", part.rungs, facts, costs=costs)
    assert got.path == "leaf reads" and "statistics" in " ".join(
        why for _, why in got.passed), got
    facts.update({"group column": ("g", "integer", 40),
                  "groups failed": "the rows did not add up"})
    got = decide.climb("t", part.rungs, facts, costs=costs)
    assert got.path == "leaf reads", got


# --- the explanation of a small differing set -------------------------------

def test_the_explanation_names_rows_and_columns_in_migkits_words():
    a = pd.DataFrame({"ID": [1, 2, 3, 4], "v": ["x", "y", "z", "w"],
                      "n": [1, 2, 3, 4]})
    b = pd.DataFrame({"ID": [1, 2, 3, 5], "v": ["x", "Y", "z", "q"],
                      "n": [1, 2, 30, 5]})
    said = sr.explain(a, b, ["ID"])
    assert said[0] == "rows read: source 4, target 4, on both sides 3"
    assert "1 rows only on the source: id=4" in said
    assert "1 rows only on the target: id=5" in said
    assert "rows on both sides with a value that differs: 2" in said
    assert "v: 1 rows differ - id=2: source 'y' target 'Y'" in said
    # the value as it is held: the join would have made it 30.0
    assert "n: 1 rows differ - id=3: source 3 target 30" in said
    text = "\n".join(said).lower()
    assert not [t for t in LEAKS if t in text], text
    equal = sr.explain(a, a, ["ID"])
    assert equal[-1] == "rows on both sides with a value that differs: 0"


def test_the_explanation_masks_what_the_hop_masks(tmp_path):
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint(), options={"mask": ["email"]})
    hop.report_dir = lambda db=None: tmp_path
    from migkit import masking
    a = pd.DataFrame({"id": [1], "email": ["bo@example.com"]})
    b = pd.DataFrame({"id": [1], "email": ["al@example.com"]})
    said = "\n".join(sr.explain(a, b, ["id"], hop, "public.people"))
    assert "bo@example.com" not in said and "al@example.com" not in said
    assert said.count("masked:") >= 2, said
    # once, not a token of a token
    assert masking.token(hop, "bo@example.com") in said, said


# --- where the reader is found, and what a run leaves -----------------------

def test_the_reader_is_found_by_its_programs_first_line(tmp_path,
                                                        monkeypatch):
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("")
    program = tmp_path / "venv" / "bin" / sr.PROGRAM
    program.write_text(f"#!{python}\nimport sys\n")
    monkeypatch.delenv("MIGKIT_SECOND_READER_PYTHON", raising=False)
    monkeypatch.setattr(sr.Path, "home", lambda: tmp_path / "nohome")
    from migkit import util
    monkeypatch.setattr(util, "_BASE", tmp_path / "nocheckout")
    monkeypatch.setattr(util, "which", lambda n: str(program)
                        if n == sr.PROGRAM else None)
    assert sr.interpreter() == str(python)
    monkeypatch.setattr(util, "which", lambda n: None)
    assert sr.interpreter() is None
    # named, it is only that
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", str(tmp_path / "no"))
    monkeypatch.setattr(util, "which", lambda n: str(program))
    assert sr.interpreter() is None


def test_the_batch_is_kept_where_the_hop_keeps_its_evidence(tmp_path,
                                                            monkeypatch):
    class Done:
        stdout = '{"ok": true, "results": [], "config": "validations: []"}'
        stderr = ""
        returncode = 0
    from migkit import runner
    monkeypatch.setattr(runner, "run", lambda *a, **k: Done)
    got = sr.run({"validations": []}, python="/x/python", keep=tmp_path)
    assert got["ok"] is True
    assert (tmp_path / sr.BATCH).read_text() == "validations: []"
    # a name the hop's at-rest recipients encrypt
    from migkit import evidence
    import fnmatch
    assert any(fnmatch.fnmatch(sr.BATCH, p) for p in evidence.HOLDS_VALUES)
