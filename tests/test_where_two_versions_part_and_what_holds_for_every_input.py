"""The proof's depth on paper (backlog CV.P5, CV.P9, CV.P15, CV.P17,
CV.P19, CV.P20): where two versions of a routine first part, what a
solver says of a function of one expression for every input, the edges an
engine's own tests wrote down, the source's answers kept past the
cutover, the indexes a translated statement could use, and a target that
runs the source's own code.

No server here; `test_conversion_breadth_mysql_to_postgres.py` runs them
on servers.
"""
import json
import time
from types import SimpleNamespace

import pglast
import pytest

from migkit.config import Endpoint, Hop
from migkit.storedcode import builtins, convert, indexadvice, mysql_pg, \
    reference, rungs, scalar, trace
from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.translate import Refused

# ---- where two versions part (P5) -------------------------------------------

MY_BODY = """begin
  declare t int default 0;
  declare s varchar(10);
  set t = n * 2;
  set t = t + 1;
  set s = concat('a', t);
  return t;
end"""

PG_BODY = """
declare
  t integer := 0;
  s varchar(10);
begin
  t := n * 2;
  t := t + 2;
  s := 'a' || t;
  return t;
end
"""


def _my():
    return Routine(kind="function", name="bonus", params=[Param("n", "int")],
                   returns="int", body=MY_BODY,
                   definition="create function bonus(n int) returns int"
                              " deterministic " + MY_BODY)


def _pg():
    return Routine(kind="function", name="bonus",
                   params=[Param("n", "integer")], returns="integer",
                   body=PG_BODY, language="plpgsql",
                   definition="CREATE OR REPLACE FUNCTION public.bonus(n"
                              " integer)\n RETURNS integer\n LANGUAGE"
                              " plpgsql\nAS $function$" + PG_BODY +
                              "$function$\n")


def test_each_side_reports_its_state_after_every_statement():
    assert trace.variables(_my(), "mysql") == [
        ("n", "int"), ("t", "int"), ("s", "varchar(10)")]
    assert trace.variables(_pg(), "postgres") == [
        ("n", "integer"), ("t", "integer"), ("s", "varchar(10)")]
    names = [("n", "int"), ("t", "int"), ("s", "varchar(10)")]
    body, points = trace.reporting(_my(), "mysql", "migkit_trace_bonus",
                                   names)
    # the copy still reads as MySQL's own, and reports four times
    mysql_pg.read(body)
    assert body.count("insert into migkit_trace") == 4 and \
        [p for p, _ in points] == [1, 2, 3, 4]
    body, points = trace.reporting(_pg(), "postgres", "migkit_trace_bonus",
                                   names)
    from migkit.storedcode.reduce import renamed
    made = renamed(_pg(), "migkit_trace_bonus", body,
                   lambda n: '"' + n + '"')
    # PostgreSQL's own parser takes the copy
    pglast.parse_plpgsql(made)
    assert made.count("raise notice 'migkit-trace") == 4
    # a notice is no SQL statement: FOUND and ROW_COUNT stay the routine's
    assert "insert" not in body.lower()


def test_variables_meet_across_the_rules_renames():
    got = trace.matched([("@total", "int"), ("n", "int"), ("p_x", "int")],
                        [("v_total", "integer"), ("n", "integer"),
                         ("p_x", "integer")])
    assert got == {"@total": "v_total", "n": "n", "p_x": "p_x"}, got
    # two names of one side that meet one base are left out, not guessed
    assert trace.matched([("v_a", "int"), ("a", "int")],
                         [("a", "integer")]) == {}


def test_the_first_state_apart_is_found_and_alike_ones_are_not():
    names = {"n": "n", "t": "t", "s": "s"}
    classes = {"n": "integer", "t": "integer", "s": "text"}
    src = [(1, {"n": "3", "t": "6", "s": None}),
           (2, {"n": "3", "t": "7", "s": None}),
           (3, {"n": "3", "t": "7", "s": "a7"})]
    dst = [(1, {"n": "3", "t": "6", "s": None}),
           (2, {"n": "3", "t": "8", "s": None}),
           (3, {"n": "3", "t": "8", "s": "a8"})]
    got = trace.aligned(src, dst, names, classes)
    assert got == {"point": 2, "target point": 2,
                   "apart": [("t", 7, 8)]}, got
    # a target that runs more statements repeats a state; it is no parting
    dst = [(1, {"n": "3", "t": "6", "s": None}),
           (2, {"n": "3", "t": "6", "s": None}),
           (3, {"n": "3", "t": "7", "s": None}),
           (4, {"n": "3", "t": "7", "s": "a7"})]
    assert trace.aligned(src, dst, names, classes) is None
    # values compared by their class: 1.50 is 1.5, true is 1
    assert trace.aligned([(1, {"d": "1.50", "b": "1"})],
                         [(1, {"d": "1.5", "b": "true"})],
                         {"d": "d", "b": "b"},
                         {"d": "decimal", "b": "boolean"}) is None


def test_a_routine_a_report_would_change_is_left_alone():
    r = Routine(kind="function", name="f", body="begin update t set a = 1;"
                " return row_count(); end")
    assert trace.READS_WHAT_A_REPORT_CHANGES["mysql"].search(r.body)
    assert trace.READS_WHAT_A_REPORT_CHANGES["mssql"].search(
        "set @n = @@ROWCOUNT")
    assert trace.READS_WHAT_A_REPORT_CHANGES["oracle"].search(
        "if sql%notfound then")


# ---- every input the solver reaches (P20) ------------------------------------


class _E:
    def __init__(self, canon, dialect):
        self.CANON_ENGINE, self.SQL_DIALECT = canon, dialect


def _proof():
    return SimpleNamespace(pair=SimpleNamespace(
        src_engine=_E("mysql", "mysql"), dst_engine=_E("postgres",
                                                       "postgres")))


def test_the_solver_finds_the_input_two_bodies_answer_apart_on():
    r = Routine(kind="function", name="flag", params=[Param("x", "int")],
                returns="int", settings={"expression": "0"})
    t = Routine(kind="function", name="flag",
                params=[Param("x", "integer")], returns="integer",
                language="sql",
                body=" select case when x * 3 = 370371 then 1 else 0 end ")
    proof = _proof()
    got = scalar.counterexamples(proof, r, t)
    # only x = 123457 tells them apart: no pool, constant or draw reaches it
    assert got == [(123457,)], got
    assert not getattr(proof, "bounded", {})


def test_two_bodies_equal_for_every_input_are_said_so_with_the_bound():
    r = Routine(kind="function", name="pick", params=[
        Param("x", "int"), Param("y", "int")], returns="int",
        settings={"expression": "case when x > y then x - y else"
                                " coalesce(y, 0) end"})
    t = Routine(kind="function", name="pick", params=[
        Param("a", "integer"), Param("b", "integer")], returns="integer",
        language="plpgsql", body="begin return case when a > b then a - b"
                                 " else coalesce(b, 0) end; end")
    proof = _proof()
    assert scalar.counterexamples(proof, r, t) == []
    assert proof.bounded["pick"] == scalar.BOUND
    # one operator changed is found, and no bound is claimed
    t.body = t.body.replace("a > b", "a >= b")
    proof = _proof()
    assert scalar.counterexamples(proof, r, t), "x = y tells them apart"
    assert "pick" not in getattr(proof, "bounded", {})


def test_a_body_beyond_the_model_is_not_asked():
    r = Routine(kind="function", name="u", params=[Param("c", "varchar(9)")],
                returns="varchar(9)", settings={"expression": "upper(c)"})
    t = Routine(kind="function", name="u", params=[Param("c", "varchar")],
                language="sql", body="select upper(c)")
    proof = _proof()
    assert scalar.counterexamples(proof, r, t) == []
    assert not getattr(proof, "bounded", {})


# ---- the engines' own tests (P17) --------------------------------------------


def test_cases_are_read_from_each_suite_as_its_engine_writes_them(tmp_path):
    regress = ("-- a comment; with a semicolon\n"
               "SELECT substr('hello', 0, 2) AS a, lpad('x', 3, '');\n"
               "\\set VERBOSITY terse\n"
               "SELECT round(2.5), upper(name) FROM t;\n")
    mysql = ("--echo # LEFT\n"
             "SELECT LEFT('abc', -1), IFNULL(NULL, 2);\n"
             "--error ER_BAD\n"
             "delimiter //;\n"
             "SELECT 1//\n"
             "delimiter ;//\n"
             "SELECT LEFT(NULL, 1);\n")
    logic = ("statement ok\nCREATE TABLE t1(a INTEGER)\n\n"
             "onlyif sqlite\nquery I\nSELECT abs(-3)\n----\n3\n\n"
             "query T\nSELECT substr('abc', 2)\n----\nbc\n")
    (tmp_path / "strings.sql").write_text(regress)
    (tmp_path / "left.test").write_text(mysql)
    (tmp_path / "logic.test").write_text(logic)
    got = builtins.statements_of(tmp_path / "strings.sql", "postgres")
    assert [ln for ln, _, _ in got] == [2, 4], got
    cases = builtins.cases_in(got[0][1], "postgres", {"substr", "lpad"})
    # as the suite writes them, not as the translator reads them back
    assert cases == [("substr('hello', 0, 2)", {"substr"}),
                     ("lpad('x', 3, '')", {"lpad"})], cases
    # a statement reading a table holds no case
    assert builtins.cases_in(got[1][1], "postgres", {"round", "upper"}) == []
    assert builtins.cases_in("SELECT round(2.5), upper(name)", "postgres",
                             {"round", "upper"}) == [("round(2.5)",
                                                      {"round"})]
    got = builtins.statements_of(tmp_path / "left.test", "mysql")
    assert [(ln, s) for ln, s, _ in got] == [
        (2, "SELECT LEFT('abc', -1), IFNULL(NULL, 2)"), (5, "SELECT 1"),
        (7, "SELECT LEFT(NULL, 1)")], got
    assert builtins.cases_in(got[0][1], "mysql", {"left", "ifnull"}) == [
        ("LEFT('abc', -1)", {"left"}), ("IFNULL(NULL, 2)", {"ifnull"})]
    got = builtins.statements_of(tmp_path / "logic.test", "postgres")
    # a query meant for another engine is not this one's
    assert [s for _, s, _ in got] == ["SELECT substr('abc', 2)"], got
    hop = Hop(name="b", engine="hetero", source=Endpoint(),
              target=Endpoint(), options={"builtin_suites": str(tmp_path)})
    mined = builtins.mine(hop, "mysql", {"left"})
    assert mined == {"left": [("LEFT('abc', -1)", "left.test:2"),
                              ("LEFT(NULL, 1)", "left.test:7")]}, mined


PARTING = {"left": [{"case": "LEFT('abc', -1)", "from": "left.test:2",
                     "source answers": "", "target answers": "ab"}]}


def test_a_call_with_a_case_known_to_part_is_refused_with_its_source():
    from migkit.storedcode.translate import parse
    with pytest.raises(Refused, match=r"left.test:2"):
        builtins.refusal(parse("SELECT LEFT('abc', -1) AS x", "mysql"),
                         PARTING, "mysql")
    # the same function on other constants is the proof's to judge
    builtins.refusal(parse("SELECT LEFT('abc', 2)", "mysql"), PARTING,
                     "mysql")
    nums, strs = builtins.constants(
        {"left": [("LEFT('abc', -1)", "left.test:2")]},
        "begin return left(s, 3); end", "mysql")
    assert -1 in nums and "abc" in strs, (nums, strs)
    said = builtins.caveats(PARTING, "return left(s, 3)")
    assert said == ["calls left, which the target answers otherwise at"
                    " LEFT('abc', -1) (the engine's own test"
                    " left.test:2)"], said


def test_the_cases_kept_are_said_on_their_line(tmp_path):
    hop = Hop(name="b", engine="hetero", source=Endpoint(),
              target=Endpoint(), options={"builtin_suites": str(tmp_path)})
    hop.report_dir = lambda db=None: tmp_path
    got = {"left": {"cases": 2, "apart": PARTING["left"],
                    "not translated": 0},
           "ifnull": {"cases": 1, "apart": [], "not translated": 0}}
    builtins.keep(hop, "shop", "mysql->postgres", got)
    assert builtins.load(hop, "shop") == PARTING
    line = builtins.result("shop", got)
    assert line.status == "warn" and "left.test:2" in line.detail, line
    alike = builtins.result("shop", {"ifnull": got["ifnull"]})
    assert alike.status == "ok", alike


# ---- the source's answers past the cutover (P19) ----------------------------


def test_only_a_function_whose_answers_need_no_rows_is_kept():
    one = Routine(kind="function", name="twice", settings={
        "expression": "x * 2"})
    assert reference.pure(one, "postgres")
    for expr in ("(select count(*) from t)", "now()", "rand() * x",
                 "nextval('s')"):
        assert not reference.pure(Routine(
            kind="function", name="f", settings={"expression": expr}),
            "postgres"), expr
    assert not reference.pure(Routine(kind="procedure", name="p"), "mysql")


def test_the_reference_holds_from_the_cutover_for_its_days(tmp_path):
    hop = Hop(name="c", engine="hetero", source=Endpoint(),
              target=Endpoint(), options={"code_reference_days": True})
    hop.report_dir = lambda db=None: tmp_path
    assert reference.days(hop) == reference.DAYS
    assert reference.after_cutover(hop) is None
    now = time.time()
    (tmp_path / "cutover-runs.jsonl").write_text(
        json.dumps({"state": "flipped", "ended": now - 3600}) + "\n")
    assert abs(reference.after_cutover(hop) - (now - 3600)) < 1
    # gone back: the source is the source again
    with (tmp_path / "cutover-runs.jsonl").open("a") as f:
        f.write(json.dumps({"state": "rolled back", "ended": now}) + "\n")
    assert reference.after_cutover(hop) is None


def test_no_reference_is_kept_under_mask_all(tmp_path):
    hop = Hop(name="c", engine="hetero", source=Endpoint(),
              target=Endpoint(), options={"code_reference_days": 5,
                                          "mask": "all"})
    hop.report_dir = lambda db=None: tmp_path
    pair = SimpleNamespace(hop=hop, src_name="mysql")
    from migkit.storedcode.harness import Outcome
    assert reference.record(pair, "shop", {"twice": [((1,), Outcome(
        value="2"))]}, {"twice": ("integer", False)}) == 0
    assert not (tmp_path / reference.FILE).exists()
    hop.options.pop("mask")
    assert reference.record(pair, "shop", {"twice": [((1,), Outcome(
        value="2"))]}, {"twice": ("integer", False)}) == 1
    kept = json.loads((tmp_path / reference.FILE).read_text())
    assert kept["routines"]["twice"]["calls"] == [
        {"args": [1], "value": "2", "error": None}]


# ---- indexes for the translated workload (P15) ------------------------------


def test_the_candidates_are_what_a_statement_filters_joins_and_orders_by():
    got = indexadvice.candidates(
        "SELECT o.id FROM orders AS o JOIN items AS i ON i.order_id = o.id"
        " WHERE o.status = 'x' AND o.created > $1 ORDER BY o.created")
    assert ("orders", ("status",)) in got and \
        ("items", ("order_id",)) in got, got
    assert ("orders", ("id", "status", "created")) in got, got
    assert indexadvice.candidates("select count(*) from t where k = 7") == \
        [("t", ("k",))]
    # a pattern that starts with a wildcard is no index's
    assert indexadvice.candidates("select a from t where b like '%x'") == []


def test_a_partitioning_is_written_as_the_target_reads_it():
    from migkit.storedcode import partitions
    made, alike = partitions.advised(
        "sales", {"m": "RANGE", "e": "year(`made`)",
                  "p": {"p2024": "2025", "p2025": "2026",
                        "pmax": "MAXVALUE"}}, "sales", "mysql")
    assert alike and made == [
        'create table "sales_partitioned" (like "sales" including defaults)'
        ' partition by range ((EXTRACT(YEAR FROM CAST("made" AS DATE))))',
        'create table "sales_partitioned_p2024" partition of'
        ' "sales_partitioned" for values from (MINVALUE) to (2025)',
        'create table "sales_partitioned_p2025" partition of'
        ' "sales_partitioned" for values from (2025) to (2026)',
        'create table "sales_partitioned_pmax" partition of'
        ' "sales_partitioned" for values from (2026) to (MAXVALUE)'], made
    for statement in made:
        pglast.parser.parse_sql(statement)
    made, _ = partitions.advised(
        "regions", {"m": "LIST", "e": "`r`", "p": {"east": "1,2",
                                                   "rest": "DEFAULT"}},
        "regions", "mysql")
    assert made[1].endswith("for values in (1, 2)") and \
        made[2].endswith("default"), made
    # Oracle's high value of a date, as its catalogue keeps it
    made, _ = partitions.advised(
        "t", {"m": "RANGE", "e": "made", "p": {
            "p1": "TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS',"
                  " 'NLS_CALENDAR=GREGORIAN')", "pmax": "MAXVALUE"}}, "t",
        "oracle")
    assert made[1].endswith("to ('2024-01-01 00:00:00')"), made
    # a hash routes by each engine's own hash: advised, never held alike
    _, alike = partitions.advised("h", {"m": "HASH", "e": "`id`", "p": {
        "p0": "", "p1": ""}}, "h", "mysql")
    assert alike is False
    with pytest.raises(Refused, match="LINEAR HASH"):
        partitions.advised("h", {"m": "LINEAR HASH", "e": "`id`",
                                 "p": {"p0": ""}}, "h", "mysql")


def test_a_row_kept_in_another_partition_is_named():
    from migkit.storedcode import partitions
    source = {("1",): "s_p2024", ("2",): "s_p2025"}
    assert partitions.apart(source, dict(source)) == []
    got = partitions.apart(source, {("1",): "s_p2025", ("2",): "s_p2025"})
    assert got == [(("1",), "s_p2024", "s_p2025")], got
    assert partitions.apart(source, {("2",): "s_p2025"}) == [
        (("1",), "s_p2024", None)]


# ---- a target that runs the source's own code (P9) --------------------------


class _Target:
    """A target asked what it is."""

    def __init__(self, engine, version, ext=0, compat="", halo=0):
        self.CANON_ENGINE = engine
        self.version, self.ext, self.compat, self.halo = \
            version, ext, compat, halo

    def _q(self, side, sql, args=None):
        return [(self.version,)]

    def _psql(self, side, db, sql):
        if "version()" in sql:
            return self.version
        if "datcompatibility" in sql:
            return self.compat
        if "babelfishpg_tsql" in sql:
            return str(self.ext)
        return str(self.halo)


def test_the_target_says_which_engines_code_it_runs():
    assert convert.emulating(_Target("mysql", "11.4.2-MariaDB"), "dst",
                             "x") == {"mariadb"}
    assert convert.emulating(_Target("mysql", "8.4.0"), "dst", "x") == set()
    assert convert.emulating(_Target(
        "postgres", "PostgreSQL 16.4 (IvorySQL 4.0)"), "dst", "x") == \
        {"ivorysql"}
    assert convert.emulating(_Target(
        "postgres", "(openGauss 6.0.0 build abc)", compat="B"), "dst",
        "x") == {"opengauss-b"}
    assert convert.emulating(_Target("postgres", "PostgreSQL 16.4", ext=1),
                             "dst", "x") == {"babelfish"}
    assert convert.emulating(_Target("postgres", "PostgreSQL 16.4"), "dst",
                             "x") == set()


def test_the_sources_own_statement_is_made_in_the_emulations_settings():
    pair = SimpleNamespace(src_engine=_E("oracle", "oracle"))
    r = Routine(kind="function", name="f", definition="FUNCTION f(a NUMBER)"
                " RETURN NUMBER IS BEGIN RETURN a * 2; END;")
    got = convert.as_written(pair, {"emulating": {"mariadb"}}, r)
    assert got.statements == [
        "set session sql_mode = 'ORACLE'",
        "create or replace FUNCTION f(a NUMBER) RETURN NUMBER IS BEGIN"
        " RETURN a * 2; END;", "set session sql_mode = default"]
    with pytest.raises(Refused, match="no emulation"):
        convert.as_written(pair, {"emulating": {"babelfish"}}, r)
    with pytest.raises(Refused, match="package"):
        convert.as_written(pair, {"emulating": {"ivorysql"}}, Routine(
            kind="function", name="p.f", definition="x",
            settings={"package": "p"}))


def test_the_emulation_is_a_candidate_only_where_the_target_is_one():
    hop = Hop(name="e", engine="hetero", source=Endpoint(),
              target=Endpoint())
    pair = SimpleNamespace(src_engine=_E("oracle", "oracle"),
                           dst_engine=_E("mysql", "mysql"), hop=hop)
    cands = {c.name: c for c in rungs.candidates(pair, None, {})}
    facts = {"src": "oracle", "dst": "mysql", "kind": "function",
             "emulating": {"mariadb"}}
    assert cands["emulator"].lacks(facts) is None
    facts["emulating"] = set()
    assert "no emulation" in cands["emulator"].lacks(facts)
    # it carries what it says it carries, and nothing else
    assert cands["emulator"].lacks(dict(facts, kind="view",
                                        emulating={"mariadb"}))
