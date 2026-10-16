"""A managed transfer service's own table on the target is not the
operator's data (field run 2026-10-16: a MySQL -> PostgreSQL run behind
Tencent DTS read `__dst__tencentdb__sync_<id>` on the target as a table of
the application's). One map (`leftovers.PROVIDER_TABLES`) feeds the scope
(`Hop.excluded` through `bookkeeping`), the listing (`whose`, `group`) and
the line said (`provider_table`)."""
from migkit import leftovers
from migkit.config import Endpoint, Hop

SYNC = "__dst__tencentdb__sync_6fve1pd0"


def _hop():
    return Hop(name="dts", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="u",
                               password="CHANGE_ME"),
               databases=["shop"])


def test_the_sync_back_table_is_left_out_of_the_scope():
    hop = _hop()
    assert hop.excluded("shop", "public", SYNC)
    assert hop.excluded("shop", SYNC)
    # the application's tables, even ones a word of the provider's begins
    assert not hop.excluded("shop", "public", "orders")
    assert not hop.excluded("shop", "public", "dst_sync_jobs")


def test_the_listing_names_the_provider():
    assert leftovers.whose(SYNC) == "Tencent DTS"
    found = leftovers.group([("table", f"public.{SYNC}"),
                             ("table", "public.orders")])
    assert found == {"Tencent DTS": [f"table public.{SYNC}"]}


def test_the_line_says_whose_it_is_and_what_is_left_out():
    lines = leftovers.provider_tables(["orders", SYNC,
                                       "awsdms_apply_exceptions"])
    assert lines == [
        f"{SYNC}: the provider's sync-back table (Tencent DTS), not your"
        " data: left out of scope, counts and the check",
        "awsdms_apply_exceptions: the provider's control table (AWS DMS),"
        " not your data: left out of scope, counts and the check"]
    assert leftovers.provider_table("orders") is None


def test_one_map_feeds_the_scope_and_the_line():
    for mover, pat, _ in leftovers.PROVIDER_TABLES:
        assert (mover, pat) in leftovers.NOT_DATA_TABLES
