"""A TiDB target whose index disagrees with its rows is found by the deep
check (P0-PARITY.verify, `verify.structure.indexes`), without a server:
the engine's own `check_deep` over what `ADMIN CHECK TABLE` answers for
each table.

A load that broke an index leaves rows a query by that index does not
find, and a row-by-row comparison by key reads past it - so each target
table is asked, and each that the server says disagrees is named, with the
server's own words.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.indexes", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _engine(broken=()):
    eng = TiDBEngine(Hop(name="i", engine="tidb", source=EP, target=EP,
                         databases=["d"]))
    asked = []
    eng.neutral_tables = lambda side, db: ["a", "b", "c"]

    def q(side, sql, *args, **kw):
        asked.append((side, sql))
        for table in broken:
            if f"`{table}`" in sql:
                raise RuntimeError(
                    f"[executor:8003]{table} err:ErrAdminCheckTable\n"
                    "handle 3, index:types.Datum{k:1} != record:<nil>")
        return []
    eng._q = q
    eng.asked = asked
    return eng


def test_a_target_whose_indexes_agree_with_its_rows_passes():
    eng = _engine()
    got = eng.check_deep("d")
    assert [(r.scope, r.status) for r in got] == [("d target indexes", "ok")]
    # every table is asked, on the target and nowhere else
    assert [s for s, _ in eng.asked] == ["dst"] * 3
    assert all(sql.startswith("admin check table") for _, sql in eng.asked)


def test_a_table_whose_index_disagrees_is_named_with_the_servers_words():
    got = _engine(broken=("b",)).check_deep("d")
    assert [r.status for r in got] == ["diff"], got
    assert got[0].scope == "d target indexes"
    assert "1 tables whose indexes do not agree with their rows" \
        in got[0].detail
    assert "b ([executor:8003]b err:ErrAdminCheckTable)" in got[0].detail
    # the others are not named
    assert "a (" not in got[0].detail and "c (" not in got[0].detail


def test_every_broken_table_is_found_not_only_the_first():
    got = _engine(broken=("a", "c")).check_deep("d")
    assert got[0].status == "diff"
    assert "2 tables whose indexes do not agree" in got[0].detail
    assert "a (" in got[0].detail and "c (" in got[0].detail
