"""The pieces of R11 that decide and search, held without a server: the
order candidates are tried in, the pass rates kept per hop, a rule learned
from a fix, the inputs combined, the branch solved for, the statements a
converter wrote read out of its script, and the small changes the proof's
own score is made of.
"""
from decimal import Decimal

import pytest

from migkit.storedcode.routines import Param, Routine


def _candidates():
    from migkit.storedcode.rungs import Candidate, Need
    both = frozenset({"mysql->postgres:function"})
    return [
        Candidate("by-hand", "the operator's own file", both,
                  (Need("a file", lambda f: f.get("file")),)),
        Candidate("rules", "migkit's rules", both),
        Candidate("plsql-converter", "the PL/SQL converter", both),
        Candidate("dialect-converter", "the multi-dialect converter",
                  both),
        Candidate("model", "a model", both),
    ]


FACTS = {"src": "mysql", "dst": "postgres", "kind": "function",
         "file": None}


def test_a_candidate_never_tried_goes_before_one_that_was(tmp_path):
    from migkit.storedcode.rungs import Ledger, ranked
    ledger = Ledger(tmp_path / "rungs.json")
    order, fell = ranked(_candidates(), ledger, FACTS)
    assert [c.name for c in order] == ["rules", "plsql-converter",
                                       "dialect-converter", "model"]
    assert [(c.name, why) for c, why in fell] == [
        ("by-hand", "the operator's own file needs a file")]
    # measured: the one that proved more often goes first, the model last
    for _ in range(3):
        ledger.record("rules", FACTS, False)
        ledger.record("dialect-converter", FACTS, True)
    ledger.record("plsql-converter", FACTS, True)
    ledger.record("plsql-converter", FACTS, False)
    ledger.save()
    again = Ledger(tmp_path / "rungs.json")
    order, _ = ranked(_candidates(), again, dict(FACTS, file="x.sql"))
    assert [c.name for c in order] == ["by-hand", "dialect-converter",
                                       "plsql-converter", "rules", "model"]
    assert again.rate("rules", FACTS) == (0, 3)


def test_a_candidate_for_another_pair_is_said_not_tried():
    from migkit.storedcode.rungs import Ledger, ranked
    order, fell = ranked(_candidates(), Ledger(None),
                         dict(FACTS, src="oracle"))
    assert order == []
    assert all("does not convert a function from oracle to postgres" in why
               for _, why in fell if _.name != "by-hand"), fell


def test_a_fix_becomes_a_rule_only_as_a_substitution():
    from migkit.storedcode.learn import apply, substitution
    assert substitution("select OCT(n), oct(m)",
                        "select to_oct(n), to_oct(m)") == ("oct", "to_oct")
    # two functions changed, or anything else changed: not a rule
    assert substitution("select a(x), b(y)", "select c(x), d(y)") is None
    assert substitution("select oct(n) + 1", "select to_oct(n) + 2") is None
    assert apply("select OCT(n)", [{"from": "oct", "to": "to_oct"}]) == \
        "select to_oct(n)"


def test_inputs_combine_every_value_at_least_once_the_same_way_each_run():
    from migkit.storedcode.inputs import combine
    pools = [list(range(30)), list("abcdefghij"), [None, 1]]
    got = combine(pools, most=50)
    assert len(got) == 50
    assert got == combine(pools, most=50)
    for k, pool in enumerate(pools):
        assert {c[k] for c in got} == set(pool), k
    assert combine([[1, 2], ["a"]]) == [(1, "a"), (2, "a")]


def test_the_counterexample_bank_keeps_what_told_versions_apart(tmp_path):
    import datetime as dt
    from migkit.storedcode.inputs import Bank
    bank = Bank(tmp_path / "bank.json")
    bank.keep("function f", (Decimal("1.555"), dt.date(2024, 2, 29), None),
              constructs=["mysql.length-bytes"], shape="decimal,date,text")
    bank.save()
    again = Bank(tmp_path / "bank.json")
    assert again.inputs("function f") == [
        (Decimal("1.555"), dt.date(2024, 2, 29), None)]
    # another routine of the same shape and construct is tried with it too
    assert again.inputs("function g", ["mysql.length-bytes"],
                        "decimal,date,text") == [
        (Decimal("1.555"), dt.date(2024, 2, 29), None)]
    assert again.inputs("function g", ["mysql.length-bytes"], "text") == []


def test_a_branch_nothing_reached_is_solved_for():
    from migkit import canon
    from migkit.storedcode import concolic
    r = Routine("function", "f", [Param("p", "int"), Param("q",
                                                          "varchar(5)")],
                "int", body="begin\n if p > 100 and q = 'X' then\n return"
                            " 1;\n elseif p between 5 and 7 then\n return"
                            " 2;\n end if;\n return 0;\nend")
    got = concolic.inputs(r, {(2, "then"), (4, "then"), (2, "else (none)")},
                          lambda t: canon.type_class("mysql", t))
    assert len(got) == 3, got
    first, second, neither = got
    assert first[0] > 100 and first[1] == "X", got
    assert 5 <= second[0] <= 7 and not (second[0] > 100
                                        and second[1] == "X"), got
    assert not (neither[0] is not None and neither[0] > 100
                and neither[1] == "X"), got
    assert not (neither[0] is not None and 5 <= neither[0] <= 7), got


def test_the_statements_a_converter_wrote_are_read_out_of_its_script():
    from migkit.storedcode.programs import statements
    script = (
        "-- Generated\nSET client_encoding TO 'UTF8';\n\n\\set ON_ERROR_STOP"
        " ON\n\nCREATE OR REPLACE PROCEDURE p (IN a integer) AS $body$\n"
        "BEGIN\n  RAISE NOTICE 'a;b';\nEND;\n$body$\nLANGUAGE PLPGSQL\n;\n"
        "-- REVOKE ALL ON PROCEDURE p FROM PUBLIC;\n")
    got = statements(script)
    assert len(got) == 1, got
    assert got[0].startswith("create PROCEDURE p (IN a integer) AS $body$")
    assert "RAISE NOTICE 'a;b';" in got[0], got


def test_the_proofs_mutants_change_one_thing_and_never_the_signature():
    from migkit.storedcode import mutants, mysql_pg
    made = mysql_pg.convert(Routine(
        "function", "grade", [Param("s", "int")], "varchar(4)",
        body="begin if s >= 90 then return 'A'; end if; return 'C'; end"))
    got = mutants.mutants(made.statements)
    assert len(got) >= 3, got
    head = made.statements[0].split("$migkit$")[0]
    for m in got:
        assert m.text[0] != made.statements[0], m.what
        assert m.text[0].split("$migkit$")[0] == head, m.what
    assert {m.what for m in got} >= {
        "a comparison moved across its boundary", "a constant off by one"}


@pytest.mark.parametrize("value,loose,want", [
    (Decimal("2.50"), True, "2.5"), (Decimal("2.50"), False, "2.50"),
    (Decimal("100"), True, "100"), (7, True, "7")])
def test_a_scale_nobody_declared_is_not_compared(value, loose, want):
    from migkit.storedcode import harness, prove
    assert harness.render_any(prove._plain(value, loose)) == want


def test_a_counter_value_is_compared_by_how_many_not_which():
    """Two rows a trigger added, numbered by the table's counter in the
    order each engine happened to write them: the same rows, not a
    difference - and a row that is not the same still is one."""
    from migkit.storedcode.harness import GENERATED, row_changes
    before = [["1", "a"]]
    one = row_changes(before, before + [["2", "x"], ["3", "y"]], [0], 0)
    other = row_changes(before, before + [["2", "y"], ["3", "x"]], [0], 0)
    assert one == other == {"+": [[GENERATED, "x"], [GENERATED, "y"]]}
    changed = row_changes(before, before + [["2", "x"], ["3", "z"]], [0], 0)
    assert changed != one
    # without a counter the numbers are the rows' own, and compared
    assert row_changes(before, before + [["2", "x"], ["3", "y"]], [0]) != \
        row_changes(before, before + [["2", "y"], ["3", "x"]], [0])


def test_what_of_an_error_is_compared():
    from migkit.storedcode.harness import Outcome, differences, error_key
    # a class for the engine's own errors: MySQL's 23000 is PostgreSQL's
    # 23505 for a duplicate key
    assert error_key(("23000", "Duplicate entry")) == \
        error_key(("23505", "duplicate key value"))
    # the code and the words for one the routine raised itself
    assert error_key(("45000", "nothing to add")) != \
        error_key(("45000", "nothing to take"))
    assert differences(Outcome(error=("22012", "x")), Outcome()) == [
        "error: 22012 (x) against no error"]


def test_a_time_from_the_clock_is_compared_within_seconds():
    from migkit.storedcode.harness import Outcome, differences
    a = Outcome(value="2026-09-28 10:00:00.000000")
    b = Outcome(value="2026-09-28 10:00:03.000000")
    far = Outcome(value="2026-09-28 10:01:00.000000")
    assert differences(a, b, clock=True) == []
    assert differences(a, b) != []
    assert differences(a, far, clock=True) != []
