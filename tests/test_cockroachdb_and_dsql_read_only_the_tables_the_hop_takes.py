"""CockroachDB and Aurora DSQL read only the tables the hop takes
(P0-PARITY.scope), without a server: each engine's own listings and checks
over a catalogue that holds a table the hop excludes (`public.audit`, the
target's own) and an online schema change's working table
(`public._orders_gho`, which goes when it swaps in).

Each is read through the one scope answer (`Engine.in_scope`), so none of
the statements a check sends names a table the hop leaves out.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import pgwire
from migkit.engines.cockroachdb import CockroachEngine
from migkit.engines.dsql import DsqlEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", ("cockroachdb", "dsql")),
          ("scope.copy", ("cockroachdb", "dsql"))]

EP = Endpoint(host="x", port=1, user="x", password="x")
LEFT_OUT = ("audit", "_orders_gho")
ENGINES = [("cockroachdb", CockroachEngine), ("dsql", DsqlEngine)]


@pytest.fixture
def asked(monkeypatch):
    """Every statement either engine sends, with its arguments, answered
    from a catalogue of three tables."""
    log = []

    def rows(self, side, db, sql, args=None):
        log.append((side, " ".join(sql.split()), tuple(args or ())))
        if "information_schema.tables" in sql:
            return [["public", "orders"], ["public", "audit"],
                    ["public", "_orders_gho"]]
        return []
    monkeypatch.setattr(pgwire.PgWireEngine, "_rows", rows)
    return log


def _engine(tmp_path, name, cls):
    hop = Hop(name="t", engine=name, source=EP, target=EP, databases=["d"],
              exclude=["public.audit"])
    hop.report_dir = lambda db=None: tmp_path
    return cls(hop)


def _names(log, table):
    return [s for s in log if table in s[1] or table in s[2]]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_the_copy_lists_only_the_tables_the_hop_takes(
        tmp_path, asked, name, cls):
    eng = _engine(tmp_path, name, cls)
    assert eng.list_move_tables("d") == [("public", "orders")]
    assert eng.neutral_tables("src", "d") == ["public.orders"]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_the_counts_the_data_and_the_schema_read_the_same_tables(
        tmp_path, asked, name, cls):
    eng = _engine(tmp_path, name, cls)
    for check in (eng.check_counts, eng.check_data, eng.check_schema):
        asked.clear()
        got = check("d")
        assert got, check.__name__
        scopes = {r.scope for r in got}
        assert not any(w in s for s in scopes for w in LEFT_OUT), scopes
        for table in LEFT_OUT:
            assert _names(asked, table) == [], (check.__name__, table)
    # what it does read is the table the hop takes
    asked.clear()
    eng.check_counts("d")
    assert {r.scope for r in eng.check_counts("d")} == {"d.orders"}
