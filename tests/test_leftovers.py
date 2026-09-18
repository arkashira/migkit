"""Recognising what a mover left in the source.

The risk here is the opposite of the usual one. Every other check errs towards
reporting too much; this one must err towards reporting too little, because
the source belongs to the application and a check that calls unfamiliar
schemas litter is a check nobody will read twice.
"""
from migkit import leftovers as lo


def test_known_movers_are_named_not_just_flagged():
    assert lo.whose("__tencentdb__") == "Tencent DTS"
    assert lo.whose("dts_publication") == "Tencent DTS"
    assert lo.whose("awsdms_validation_failures_v1") == "AWS DMS"
    assert lo.whose("debezium_signal") == "Debezium"


def test_our_own_litter_is_in_the_list():
    """A tool that reports everyone else's leftovers and not its own is not
    worth reading."""
    assert lo.whose("migkit_changelog") == "migkit"


def test_an_application_object_is_not_reported():
    for name in ("orders", "public", "customer_events", "reporting",
                 "dtsx_business_table", "analytics"):
        assert lo.whose(name) == "", name


def test_wrapped_artifacts_are_caught_at_both_ends():
    """gh-ost and pt-osc wrap the original name in a leading underscore AND a
    suffix, so both ends have to match."""
    assert lo.whose("_orders_gho") == "gh-ost"
    assert lo.whose("_orders_del") == "gh-ost"
    assert lo.whose("_orders_new") == "pt-online-schema-change"
    assert lo.whose("_orders_old") == "pt-online-schema-change"


def test_an_application_table_that_merely_ends_that_way_is_not_litter():
    """`orders_new` is somebody's table. Matching the suffix alone would call
    it litter and teach the reader to ignore this check."""
    for name in ("orders_new", "customer_del", "totals_old", "batch_gho",
                 "_", "_new", "_del"):
        assert lo.whose(name) == "", name


def test_unknown_objects_are_dropped_rather_than_guessed_at():
    by = lo.group([("schema", "__tencentdb__"), ("schema", "reporting"),
                   ("table", "orders")])
    assert list(by) == ["Tencent DTS"]
    assert lo.total(by) == 1


def test_nothing_found_is_nothing_said():
    by = lo.group([("schema", "public"), ("table", "orders")])
    assert by == {} and lo.total(by) == 0
    assert lo.describe(by) == "" and lo.urgent(by) == []


def test_the_report_names_the_mover_and_counts_per_mover():
    by = lo.group([("schema", "__tencentdb__"), ("slot", "dts_slot"),
                   ("table", "awsdms_apply_exceptions")])
    text = lo.describe(by)
    assert "Tencent DTS:" in text and "AWS DMS:" in text
    assert lo.total(by) == 3


def test_costly_leftovers_are_separated_from_untidy_ones():
    """A slot pinning WAL is an outage waiting on the source you already
    migrated away from. A stale schema is only untidy."""
    by = lo.group([("slot", "dts_slot"), ("schema", "__tencentdb__"),
                   ("event trigger", "dts_ddl_trigger")])
    u = dict(lo.urgent(by))
    assert "slot dts_slot" in u
    assert "event trigger dts_ddl_trigger" in u
    assert "schema __tencentdb__" not in u
    assert "run out of disk" in u["slot dts_slot"]


def test_duplicates_are_counted_once():
    by = lo.group([("schema", "__tencentdb__")] * 3)
    assert lo.total(by) == 1
