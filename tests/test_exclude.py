"""hop.exclude must actually protect a table, not just be config decoration:
an excluded object is neither verified nor repaired, so a table the target owns
(rows written on the target, not carried from the source) is never deleted by a
reconcile. Pure-unit: exercises the matcher and the postgres table filter with
no live database, so it always runs."""
from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine


def _hop(engine, exclude):
    return Hop(name="h", engine=engine,
               source=Endpoint(host="s"), target=Endpoint(host="t"),
               databases=["oms_mkp_uat"], exclude=exclude)


def test_excluded_full_id():
    h = _hop("postgres", ["oms_mkp_uat.public.pick_dispatch_queue"])
    assert h.excluded("oms_mkp_uat", "public", "pick_dispatch_queue")
    assert not h.excluded("oms_mkp_uat", "public", "orders")


def test_excluded_suffix_forms():
    assert _hop("postgres", ["public.pick_dispatch_queue"]).excluded(
        "oms_mkp_uat", "public", "pick_dispatch_queue")
    tbl_only = _hop("postgres", ["pick_dispatch_queue"])
    assert tbl_only.excluded("oms_mkp_uat", "public", "pick_dispatch_queue")
    assert tbl_only.excluded("other_db", "public", "pick_dispatch_queue")


def test_excluded_wildcards():
    schema = _hop("postgres", ["oms_mkp_uat.public.*"])
    assert schema.excluded("oms_mkp_uat", "public", "anything")
    assert not schema.excluded("cart_uat", "public", "anything")


def test_excluded_empty_never_matches():
    assert not _hop("postgres", []).excluded("db", "public", "t")


def test_pg_keep_tbl_filters_excluded():
    eng = PostgresEngine(_hop("postgres",
                              ["oms_mkp_uat.public.pick_dispatch_queue",
                               "oms_mkp_uat.public.pick_dispatch_oos_queue"]))
    assert not eng._keep_tbl("oms_mkp_uat", "public.pick_dispatch_queue")
    assert not eng._keep_tbl("oms_mkp_uat", "public.pick_dispatch_oos_queue")
    assert eng._keep_tbl("oms_mkp_uat", "public.orders")


def test_mysql_mongo_matcher_two_parts():
    assert _hop("mysql", ["shop.audit_log"]).excluded("shop", "audit_log")
    assert not _hop("mysql", ["shop.audit_log"]).excluded("shop", "orders")
    assert _hop("mongodb", ["oss.local_cache"]).excluded("oss", "local_cache")
