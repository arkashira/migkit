"""MySQL's deep check finds a column that differs between the sides, as a
finding of the category structure.columns (P0-PARITY.verify)."""
from tests.conftest import needs_docker
from tests.mysql_pair import engine, my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.columns", "mysql")]


@needs_docker
def test_a_column_that_differs_is_found(my_pair):
    for side, width in (("src", 10), ("dst", 20)):
        my(side, "drop database if exists coldrift; create database"
                 f" coldrift; create table coldrift.t (id int primary key,"
                 f" v varchar({width}), w int)")
    eng = engine("coldrift")
    found = [r for r in eng.check_deep("coldrift")
             if r.scope == "coldrift columns"]
    assert [r.status for r in found] == ["diff"], found
    assert found[0].category == "structure.columns"
    assert "t.v" in found[0].detail
    # identical, the same finding is ok
    my("dst", "alter table coldrift.t modify v varchar(10)")
    again = [r for r in eng.check_deep("coldrift")
             if r.scope == "coldrift columns"]
    assert [r.status for r in again] == ["ok"], again
