"""MySQL's data check names the table a finding is about and counts, row
by row, the rows apart (P0-PARITY.verify)."""
from tests.conftest import needs_docker
from tests.mysql_pair import engine, my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "mysql"), ("verify.table", "mysql")]


@needs_docker
def test_the_rows_apart_are_counted_and_the_table_named(my_pair, tmp_path):
    for side in ("src", "dst"):
        my(side, "drop database if exists apart; create database apart;"
                 " create table apart.t (id int primary key, v int)")
    my("src", "insert into apart.t values (1, 1), (2, 2), (3, 3), (4, 4)")
    # the target: 1 equal, 2 changed, 3 missing, 4 equal, 9 extra
    my("dst", "insert into apart.t values (1, 1), (2, 99), (4, 4), (9, 9)")
    eng = engine("apart")
    found = [r for r in eng.check_data("apart") if r.status == "diff"]
    assert len(found) == 1, found
    got = found[0]
    assert got.table == "t", got
    assert got.rows_apart == {"t": {"missing": 1, "extra": 1, "changed": 1,
                                    "exact": True}}, got.rows_apart
