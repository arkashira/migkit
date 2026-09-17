"""The inventory of work a mover leaves behind.

The rules being defended here are the ones that are easy to break later and
expensive to notice: that a blocked probe never reads as zero, and that no
output ever looks like a duration estimate.
"""
import pytest

from migkit.handwork import KINDS, ORDER, Inventory, names


def test_every_kind_is_ranked():
    assert sorted(ORDER) == sorted(KINDS)


def test_names_admits_what_it_is_hiding():
    assert names(["b", "a"]) == "a, b"
    long = [f"t{i:02d}" for i in range(10)]
    out = names(long, show=3)
    assert out == "t00, t01, t02 and 7 more"


def test_a_counted_kind_reports_the_count_and_the_names():
    inv = Inventory()
    inv.add("no-row-key", "shop", "tables", ["public.a", "public.b"])
    row = inv.rows()[0]
    assert row["level"] == "warn"
    assert row["scope"] == "shop"
    assert "2" in row["detail"]
    assert "public.a, public.b" in row["detail"]


def test_a_blocked_probe_is_unknown_and_never_zero():
    """A managed provider that hides a catalogue must not read as a clean
    bill of health."""
    inv = Inventory()
    inv.unknown("server-side-code", "shop", "no permission on mysql.proc")
    row = inv.rows()[0]
    assert "UNKNOWN" in row["detail"]
    assert "Not zero" in row["detail"]
    assert row["level"] == "warn"
    assert inv.total() == 0 and inv.unknowns() == 1
    # and the summary says so rather than reporting a clean run
    assert "could not run" in inv.summary()[0]["item"]


def test_unknown_outranks_a_partial_count_in_the_same_scope():
    """Half a count presented as a total is the exact failure this guards."""
    inv = Inventory()
    inv.add("server-side-code", "shop", "triggers", ["t1"])
    inv.unknown("server-side-code", "shop", "cannot read routines")
    rows = [r for r in inv.rows() if r["scope"] == "shop"]
    assert len(rows) == 1
    assert "UNKNOWN" in rows[0]["detail"]
    assert "1 found by the probes that did run" in rows[0]["detail"]


def test_looked_and_found_none_is_distinct_from_never_looked():
    inv = Inventory()
    inv.add("not-carried", "shop", "unlogged tables", [])
    assert inv.rows() == [{"level": "pass", "scope": "shop",
                           "item": KINDS["not-carried"][1],
                           "detail": "none"}]


def test_nothing_probed_produces_nothing_at_all():
    inv = Inventory()
    assert inv.rows() == [] and inv.summary() == []


def test_worst_kind_comes_first_across_scopes():
    inv = Inventory()
    inv.add("server-side-code", "a", "triggers", ["t1"])
    inv.add("target-prereq", "b", "extensions", ["postgis"])
    kinds = [r["item"] for r in inv.rows()]
    assert kinds[0] == KINDS["target-prereq"][1]


def test_a_missing_prerequisite_fails_rather_than_warns():
    inv = Inventory()
    inv.add("target-prereq", "shop", "extensions", ["postgis"])
    assert inv.rows()[0]["level"] == "fail"
    assert inv.summary()[0]["level"] == "fail"


def test_the_report_never_reads_like_a_duration_estimate():
    """ora2pg's person-day numbers rest on a calibration migkit does not have.
    Printing one anyway would be a fabrication dressed as a measurement."""
    inv = Inventory()
    inv.add("server-side-code", "shop", "procedures", ["p1", "p2"])
    inv.add("no-row-key", "shop", "tables", ["t1"])
    text = " ".join(r["detail"] + r["item"]
                    for r in inv.rows() + inv.summary()).lower()
    for word in ("person-day", "man-day", "hour", "week", "effort score",
                 "estimated", "days"):
        assert word not in text, f"{word!r} leaked into the inventory"
    assert "does not estimate" in text


def test_an_unregistered_kind_is_refused_at_the_call_site():
    """So a new engine cannot invent private vocabulary that nothing else in
    the report understands."""
    inv = Inventory()
    with pytest.raises(KeyError):
        inv.add("made-up", "shop", "things", ["x"])
    with pytest.raises(KeyError):
        inv.unknown("made-up", "shop", "why")


def test_two_probes_of_one_kind_share_a_row_and_a_total():
    inv = Inventory()
    inv.add("not-carried", "shop", "unlogged tables", ["u1"])
    inv.add("not-carried", "shop", "materialized views", ["m1", "m2"])
    rows = [r for r in inv.rows() if r["scope"] == "shop"]
    assert len(rows) == 1
    assert rows[0]["detail"].startswith("3 - ")
    assert "materialized views: 2" in rows[0]["detail"]
    assert "unlogged tables: 1" in rows[0]["detail"]
    assert inv.total() == 3
