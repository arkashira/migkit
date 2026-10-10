"""SQLite's data check names the table a finding is about and counts the
rows apart (P0-PARITY.verify)."""
from tests.test_sqlite_repair import PLAIN, ROWS, THREE, _build

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.table", "sqlite"), ("verify.rows-apart", "sqlite")]


def test_the_finding_names_its_table_and_counts_the_rows_apart(tmp_path):
    eng, _, _ = _build(tmp_path, PLAIN, ROWS, THREE)
    (found,) = eng.check_data("main")
    assert found.status == "diff"
    assert found.table == "t"
    assert found.rows_apart == {"t": {"missing": 1, "extra": 1, "changed": 1,
                                      "exact": True}}, found.rows_apart


def test_an_equal_table_still_names_its_table(tmp_path):
    eng, _, _ = _build(tmp_path, PLAIN, ROWS)
    (found,) = eng.check_data("main")
    assert found.status == "ok" and found.table == "t"
    assert found.rows_apart == {}
