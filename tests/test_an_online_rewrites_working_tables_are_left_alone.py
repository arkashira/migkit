"""A table an online schema change is working in is neither copied nor
reported, and an application's table that only looks like one is both.

`drift.transient` knew gh-ost's and pt-online-schema-change's names, and
only the shape comparison and the change tail asked it: the copy carried
the working tables to the target and `check` named them missing there.
pg_repack's (`repack.log_N`, `repack.table_N`, its trigger), Vitess's,
Spirit's, Facebook's OnlineSchemaChange's, LHM's and the server's own
`#sql-` copies are known too, and the move's table list, the counts, the
data and the object inventory leave them out.

A name an application could choose as well - `_archive_old` - counts only
beside the table it would be a copy of: a false "working table" would take
an application's rows out of the move and out of the check without a word.

The docker tests run one engine at a time (`-k postgres`, `-k mysql`).
"""
import pytest

from migkit import drift
from tests import mysql_pair
from tests.conftest import psql, verdict
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture


# -- one test per family of names -------------------------------------------

def test_gh_ost_beside_its_table():
    among = ["orders", "_orders_gho", "_orders_ghc", "_orders_del",
             "_orders_20260928120000_del"]
    assert drift.transient_among(among) == set(among) - {"orders"}


def test_pt_online_schema_change_beside_its_table():
    among = ["shop.orders", "shop._orders_new", "shop._orders_old"]
    assert drift.transient_among(among) == {"shop._orders_new",
                                            "shop._orders_old"}
    for trigger in ("pt_osc_shop_orders_ins", "pt_osc_shop_orders_upd",
                    "pt_osc_shop_orders_del"):
        assert drift.transient_trigger(trigger), trigger


def test_pg_repack_in_its_own_schema():
    for name in ("repack.log_16384", "repack.table_16384",
                 "repack.index_16390", "repack.log_16384_id_seq"):
        assert drift.transient(name), name
        assert drift.transient(name, ["public.orders"]), name
    # the same words anywhere else are an application's
    for name in ("public.log_16384", "repack.orders", "repack.log"):
        assert not drift.transient(name, ["public.orders"]), name
    for trigger in ("repack_trigger", "z_repack_trigger",
                    "public.orders.repack_trigger"):
        assert drift.transient_trigger(trigger), trigger
    assert not drift.transient_trigger("orders_repack_audit")


def test_vitess_shadow_and_retired_tables():
    for name in ("_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
                 "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
                 "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
                 "_6ace8bce_f732_11ea_87e9_f875a4d24e90_20200915120410"
                 "_vrepl"):
        assert drift.transient(name, []), name
    assert not drift.transient("_vt_settings", [])


def test_spirit_checkpoint_and_sentinel():
    among = ["orders", "_orders_new", "_orders_chkpnt", "_spirit_sentinel"]
    assert drift.transient_among(among) == set(among) - {"orders"}


def test_facebook_online_schema_change():
    among = ["orders", "__osc_new_orders", "__osc_chg_orders",
             "__osc_old_orders"]
    assert drift.transient_among(among) == set(among) - {"orders"}
    assert drift.transient_trigger("__osc_ins_orders")


def test_lhm_new_and_archived_copies():
    among = ["orders", "lhmn_orders", "lhma_2026_09_28_12_00_00_123_orders"]
    assert drift.transient_among(among) == set(among) - {"orders"}
    assert drift.transient_trigger("lhmt_upd_orders")


def test_the_servers_own_copy_during_an_alter():
    for name in ("#sql-ib1234-5678", "#sql-1a2b_3c", "shop.#sql2-1a2b-3c"):
        assert drift.transient(name, []), name


def test_a_look_alike_without_its_table_is_the_applications():
    """The false negative this guards: `_archive_old`, `lhmn_x` and
    `__osc_new_ledger` with no `archive`, `x` or `ledger` beside them are
    tables an application made, and they are moved and checked."""
    among = ["_archive_old", "lhmn_x", "__osc_new_ledger", "_notes_chkpnt",
             "orders"]
    assert drift.transient_among(among) == set()
    # read by name alone, as the tail still does, gh-ost's and
    # pt-online-schema-change's names are what they always were
    assert drift.transient("_archive_old")
    assert not drift.transient("lhmn_x")


def test_the_shape_leaves_out_only_working_tables():
    class _Eng:
        class hop:
            @staticmethod
            def excluded(db, *parts):
                return False

        @staticmethod
        def column_catalog(side, db):
            return {"public.orders": [("id", "int")],
                    "public._orders_new": [("id", "int")],
                    "public._archive_old": [("id", "int")],
                    "repack.log_16384": [("id", "bigint")]}
    got = drift.shape(_Eng, "src", "d")
    assert sorted(got) == ["public._archive_old", "public.orders"], got


# -- on the servers -----------------------------------------------------------

def _pg(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg_engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="chk", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=55432, user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=55433, user="postgres",
                        password="test"),
        databases=["postgres"]))


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.mark.docker
def test_postgres_a_repack_in_progress_is_not_moved_or_reported(
        pg_pair, reports):
    both = ("create table orders (id int primary key, v text);"
            " insert into orders select g, 'v' || g"
            " from generate_series(1, 5) g;"
            " create or replace function public.log_change() returns"
            " trigger language plpgsql as $$ begin return new; end $$;")
    _pg(pg_pair["dst"], both)
    # pg_repack's working objects as it leaves them while it runs, and an
    # application's table whose name only looks like a tool's
    _pg(pg_pair["src"], both
        + " create schema if not exists repack;"
          " create table repack.log_16384 (id bigserial primary key,"
          " pk int, row text);"
          " insert into repack.log_16384 (pk, row) values (1, 'x');"
          " create table repack.table_16384 as select * from orders;"
          " create trigger repack_trigger after insert on orders"
          " for each row execute function public.log_change();"
          " create table _archive_old (id int primary key);"
          " insert into _archive_old values (1);")
    try:
        eng = _pg_engine()
        moved = eng.list_move_tables("postgres")
        assert ("public", "_archive_old") in moved, moved
        assert not [t for t in moved if t[0] == "repack"], moved
        counts = verdict(eng.check_counts("postgres"), "postgres")
        assert "public._archive_old missing on target" in counts.detail
        assert "repack" not in counts.detail, counts.detail
        objects = verdict([eng.check_objects("postgres")],
                          "postgres objects")
        assert "_archive_old" in objects.detail, objects.detail
        assert "repack" not in objects.detail, objects.detail
    finally:
        _pg(pg_pair["src"], "drop schema if exists repack cascade")


@pytest.mark.docker
def test_mysql_a_rewrite_in_progress_is_not_moved_or_reported(
        my_pair, reports):
    both = ("drop database if exists osc; create database osc; use osc;"
            " create table orders (id int primary key, v varchar(10));"
            " insert into orders values (1, 'a'), (2, 'b');")
    my("dst", both)
    my("src", both
       + " create table _orders_new like orders;"
         " insert into _orders_new select * from orders;"
         " create table _orders_old like orders;"
         " create table _orders_gho like orders;"
         " create trigger pt_osc_osc_orders_ins after insert on orders"
         " for each row replace into _orders_new values (new.id, new.v);"
         " create table _archive_old (id int primary key);"
         " insert into _archive_old values (1);")
    eng = mysql_pair.engine("osc")
    moved = [t for _, t in eng.list_move_tables("osc")]
    assert sorted(moved) == ["_archive_old", "orders"], moved
    counts = verdict(eng.check_counts("osc"), "osc")
    assert "_archive_old" in counts.detail, counts.detail
    assert "_orders" not in counts.detail, counts.detail
    objects = verdict([eng.check_objects("osc")], "osc objects")
    assert "_archive_old" in objects.detail, objects.detail
    assert "_orders_" not in objects.detail, objects.detail
    assert "pt_osc" not in objects.detail, objects.detail
