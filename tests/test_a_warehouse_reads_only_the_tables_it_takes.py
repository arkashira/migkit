"""A warehouse hop lists, compares and copies only the tables the hop takes:
the tables its counts, data and schema checks read, and the ones its copy
lists, are asked of the engine's one scope answer (`Engine.in_scope`), so a
table the hop leaves out is never read, never counted and never made on the
target - and migkit's own tables (`_migkit_*`) are no table of the
application's (P0-PARITY.scope).

The listing is one catalogue read the engine always made; what is held here
is what is done with its answer. BigQuery's is answered as its
`INFORMATION_SCHEMA.TABLES` answers it, Redshift's and Snowflake's by their
engines' own listing of names, which the listing they share filters. No
warehouse is reached.
"""
import pytest

pytest.importorskip("google.cloud.bigquery")

from migkit.config import Endpoint, Hop  # noqa: E402
from tests import bigquery_fake as fake  # noqa: E402

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", ("bigquery", "redshift", "snowflake")),
          ("scope.copy", ("bigquery", "redshift", "snowflake"))]

LISTED = ["orders", "lines", "audit_log", "_migkit_ledger"]


def _eng(monkeypatch, name, **hop):
    if name == "bigquery":
        eng = fake.engine(monkeypatch, **hop)
        for side in ("src", "dst"):
            eng.clients[side].on("information_schema.tables",
                                 [[t] for t in LISTED])
        return eng
    from migkit.engines import _class_for, engine_named
    # the engine's own listing of the names its catalogue gives
    monkeypatch.setattr(_class_for(name), "_table_names",
                        lambda self, side, db: list(LISTED))
    return engine_named(name, Hop(
        name="w", engine=name, source=Endpoint(),
        target=Endpoint(options={"database": "analytics"}),
        databases=["sales"], **hop))


WAREHOUSES = ["bigquery", "redshift", "snowflake"]


@pytest.mark.parametrize("name", WAREHOUSES)
@pytest.mark.parametrize("hop, want", [
    ({}, ["orders", "lines", "audit_log"]),
    ({"exclude": ["audit_log"]}, ["orders", "lines"]),
    ({"include": ["orders"]}, ["orders"]),
    ({"include": ["orders", "lines"], "exclude": ["lines"]}, ["orders"]),
])
def test_the_listing_is_what_the_hop_takes(monkeypatch, name, hop, want):
    eng = _eng(monkeypatch, name, **hop)
    assert eng.neutral_tables("src", "sales") == want
    assert eng.neutral_tables("dst", "sales") == want


@pytest.mark.parametrize("name", WAREHOUSES)
def test_the_copy_lists_the_hops_tables_only(monkeypatch, name):
    eng = _eng(monkeypatch, name, exclude=["audit_log"])
    assert sorted(eng.list_move_tables("sales")) == [("", "lines"),
                                                      ("", "orders")]
    eng = _eng(monkeypatch, name, include=["lines"])
    assert eng.list_move_tables("sales") == [("", "lines")]


@pytest.mark.parametrize("name", WAREHOUSES)
def test_a_table_left_out_is_not_read_for_the_checks(monkeypatch, name):
    """The pair that compares a hop's two sides asks each side for its
    tables through the same answer: the table excluded is in neither."""
    eng = _eng(monkeypatch, name, exclude=["audit_log"])
    pair = eng._as_pair()
    assert pair.src_engine.neutral_tables("src", "sales") == [
        "orders", "lines"]
    assert pair.dst_engine.neutral_tables("dst", "sales") == [
        "orders", "lines"]
    # and the deep checks' own listings (views, routines, tables made on
    # the target) hold to it too
    assert not eng.in_scope("sales", "audit_log")
    assert eng.in_scope("sales", "orders")


@pytest.mark.parametrize("name", WAREHOUSES)
def test_migkits_own_tables_are_never_the_applications(monkeypatch, name):
    eng = _eng(monkeypatch, name)
    assert "_migkit_ledger" not in eng.neutral_tables("dst", "sales")
