"""Every cross-engine verdict says which columns it compared exactly,
which by value and as what, and which not at all and why (problems D15).

"Checksums equal" is only as strong as the columns that went into the
checksum. The cross-engine path already named the columns it refused; what
it never said is how the ones it did compare were compared - a decimal by
its digits or by its value, a float to twenty places, two types brought to
one class - so a green verdict read the same whether a column was held byte
for byte or only as a value. Now the verdict ends with the three groups,
and a column in none of them is a failure of this test.
"""
import sqlite3

from migkit import canon, verdict
from migkit.config import Endpoint, Hop


def test_one_engine_one_type_is_compared_exactly():
    assert canon.how_compared("postgres", "bigint", "integer", "postgres",
                              "bigint", "integer") == (
        "exactly", "one engine, one type")


def test_two_types_are_compared_by_value_and_say_as_what():
    kind, why = canon.how_compared("postgres", "bigint", "integer", "mysql",
                                   "int", "integer")
    assert kind == "by value", kind
    assert "bigint and int compared as integers" == why, why


def test_a_rendering_that_lets_part_go_is_by_value_on_one_type_too():
    """A float to twenty places, JSON's normal form: one type on both sides
    and still not the whole of the value."""
    kind, why = canon.how_compared("mysql", "double", "float", "mysql",
                                   "double", "float")
    assert kind == "by value" and "twenty places" in why, (kind, why)
    kind, why = canon.how_compared("mysql", "json", canon.JSON_ONE, "mysql",
                                   "json", canon.JSON_ONE)
    assert kind == "by value" and "key order" in why, (kind, why)


def test_the_clause_names_all_three_groups_and_counts_them_whole():
    how = {f"c{i:02d}": ("exactly", "one engine, one type")
           for i in range(15)}
    how["n"] = ("by value", "numeric and decimal(10,2) compared as numbers")
    how["x"] = ("not at all", "only on the source")
    got = verdict.compared_clause(how)
    assert got.startswith("; of 17 columns, compared"), got
    assert "15 exactly (one engine, one type)" in got, got
    # named up to the cap, and the rest counted, never dropped
    assert "c11" in got and "c14" not in got and "and 3 more" in got, got
    assert "1 by value: n (numeric and decimal(10,2) compared as" in got, got
    assert "1 not at all: x (only on the source)" in got, got
    assert verdict.compared_clause({}) == ""


def _pair(tmp_path, mapping=None):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    con = sqlite3.connect(src)
    con.executescript(
        "create table t (id integer primary key, name text, n integer,"
        " ratio real, raw blob, secret text, dropped text);"
        " insert into t values (1, 'a', 5, 0.5, x'00ff', 's', 'd');")
    con.commit()
    con.close()
    con = sqlite3.connect(dst)
    con.executescript(
        "create table t (id integer primary key, name text, n text,"
        " ratio real, raw blob, extra text);"
        " insert into t values (1, 'a', '5', 0.5, x'00ff', null);")
    con.commit()
    con.close()
    hop = Hop(name="c", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"], mapping=mapping or {},
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_every_column_of_the_pair_is_in_one_group(tmp_path):
    eng = _pair(tmp_path, {"columns": {"t": {"drop": ["dropped"]}}})
    how = {}
    src, _, notes = eng._comparable_columns("main", eng.src_engine, "t",
                                            eng.dst_engine, "t", how)
    declared = {"id", "name", "n", "ratio", "raw", "secret", "dropped",
                "extra"}
    assert set(how) == declared, (set(how) ^ declared, how)
    assert how["name"][0] == "exactly", how
    assert how["n"][0] == "by value", how
    assert "compared as" in how["n"][1], how
    assert how["secret"] == ("not at all", "only on the source"), how
    assert how["extra"] == ("not at all", "only on the target"), how
    assert how["dropped"] == ("not at all",
                              "left out by the hop's column mapping"), how
    # compared or not, each once
    compared = {n for n, _ in src}
    assert compared == {n for n, (k, _) in how.items() if k != "not at all"}


def test_the_verdict_itself_carries_it(tmp_path):
    eng = _pair(tmp_path)
    got = [r for r in eng.check_data("main") if r.scope.endswith(".t")]
    assert len(got) == 1, [(r.scope, r.detail) for r in got]
    detail = got[0].detail
    assert "; of 8 columns, compared" in detail, detail
    assert "exactly (one engine, one type)" in detail, detail
    assert "by value: n (" in detail, detail
    assert "not at all:" in detail and "secret (only on the source)" \
        in detail, detail


def test_a_verdict_that_differs_carries_it_too(tmp_path):
    eng = _pair(tmp_path)
    con = sqlite3.connect(tmp_path / "d.db")
    con.execute("update t set name = 'changed'")
    con.commit()
    con.close()
    got = [r for r in eng.check_data("main") if r.scope.endswith(".t")][0]
    assert got.status == "diff", got.detail
    assert "; of 8 columns, compared" in got.detail, got.detail
