"""A table is taken into the hop's running publication only where the
source's PostgreSQL does not lose its changes doing so (backlog L8):
before the minor releases of 2025-05-08 (17.5, 16.9, 15.13, 14.18,
13.21; 12 and older never) a table added to a publication while it was
written could have its changes filtered out by the server itself. There
the ladder of change ways (`movers.stream_ways`) takes migkit's own tail
instead, which reads every table from its slot and alters no
publication - and says why.
"""
from migkit import movers
from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine


def _eng(version, published, tables, there=True):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    eng = PostgresEngine(Hop(name="pub", engine="postgres", source=ep,
                             target=ep, databases=["d"]))
    eng._server_version = lambda side, db: version

    def rows(side, db, sql):
        if "from pg_publication where" in sql:
            return [(1,)] if there else []
        if "pg_publication_tables" in sql:
            return [(t,) for t in published]
        return [(t,) for t in tables]
    eng._plan_rows = rows
    return eng


def test_a_table_added_to_a_running_publication_below_the_fix_is_a_risk():
    got = _eng(160008, ["public.a"], ["public.a", "public.b"]) \
        .publication_add_risk("d")
    assert "16.8" in got and "16.9" in got and "public.b" in got, got
    # 12: never fixed
    assert _eng(120022, ["public.a"], ["public.a", "public.b"]) \
        .publication_add_risk("d")


def test_no_risk_at_or_past_the_fix_or_with_nothing_to_add():
    assert _eng(160009, ["public.a"], ["public.a", "public.b"]) \
        .publication_add_risk("d") == ""
    assert _eng(180000, ["public.a"], ["public.a", "public.b"]) \
        .publication_add_risk("d") == ""
    # every table already published
    assert _eng(160008, ["public.a", "public.b"],
                ["public.a", "public.b"]).publication_add_risk("d") == ""
    # a publication made with its tables from the start
    assert _eng(160008, [], ["public.a"], there=False) \
        .publication_add_risk("d") == ""


def test_the_ladder_takes_the_tail_where_the_publication_is_a_risk(
        monkeypatch):
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="pubways", engine="postgres", source=ep, target=ep)
    monkeypatch.setattr("migkit.decide.HopCosts.get",
                        lambda self, name, default=None: default)
    safe = movers.stream_way(hop, "postgres", True, True, True, risk=
                             lambda: "")
    assert safe.path == "subscription", safe
    risky = movers.stream_way(hop, "postgres", True, True, True, risk=
                              lambda: "before 16.9 PostgreSQL itself can"
                                      " lose the changes of a table added")
    assert risky.path == "tail", risky
    assert "16.9" in risky.reason, risky.reason
