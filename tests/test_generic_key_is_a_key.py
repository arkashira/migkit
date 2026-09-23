"""The assumption every other verdict on this engine rests on.

`GenericEngine` covers the nine engines migkit reaches through one
comparison library. That comparison matches rows **by key**, and the row
repair addresses them by key. So if the key is not one row per value, both
of them answer about something other than the data - and until this check
existed, nothing said so.

Measured on PostgreSQL 16. A source of four rows, one of them keyed NULL,
against a target holding the other three:

    3 rows in table A
    3 rows in table B
    0 rows exclusive to table A (not present in B)
    0.00% difference score

and migkit's own checks on that same pair, all three green:

    counts  OK  1 tables, the same number of rows on both sides
    data    OK  no row is on one side only and no compared column differs
    schema  OK  2 columns, same names and same declared types on both sides

Four rows against three, reported as complete. A migration that dropped
every NULL-keyed row would pass verification and nobody would know until
the application asked for one of them.

The second way a key stops being a key is quieter than it first looked.
A repeated value does make the comparison refuse outright - but only when
both URLs point at the *same* server, which no real hop does. Across two
servers it answers, and the answer is short. Measured: a source holding
`4/y` and `4/z` against a target holding `4/q` - three rows, none of them
a pair - came back as one row exclusive to the source, nothing exclusive
to the target, nothing updated. So the verdict says the counts are a lower
bound, because that is what they are.

Both questions are asked through the portable query builder rather than
hand-written SQL, so one implementation serves all nine engines.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

try:
    import reladiff.databases  # noqa: F401
    HAVE_LIB = True
except Exception:
    HAVE_LIB = False

needs_lib = pytest.mark.skipif(not HAVE_LIB,
                               reason="the comparison library is not installed")


def _engine(pg_pair, tmp_path, key="id", tables=("gkey",)):
    from migkit.engines.generic import GenericEngine

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": list(tables), "key": key})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return GenericEngine(hop)


DDL = "create table gkey (id int, part int, v text)"


@pytest.fixture
def pair(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists gkey")
        assert psql(port, DDL).returncode == 0
        assert psql(port, "insert into gkey values (1,1,'a'),(2,1,'b'),"
                          "(3,2,'c')").returncode == 0
    yield pg_pair
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists gkey")


def test_the_engine_offers_a_deep_battery_at_all():
    """It inherited the base's "no deep checks for this engine yet"."""
    from migkit.engines.base import Engine
    from migkit.engines.generic import GenericEngine
    assert GenericEngine.check_deep is not Engine.check_deep


@needs_lib
def test_a_key_that_is_filled_and_unique_passes(pair, tmp_path):
    res = _engine(pair, tmp_path).check_deep("-")
    assert len(res) == 2, [r.scope for r in res]
    assert {r.status for r in res} == {"ok"}, [r.detail for r in res]
    assert {r.scope for r in res} == {"gkey key (source)", "gkey key (target)"}


@needs_lib
def test_a_row_with_no_key_is_reported_on_the_side_it_is_on(pair, tmp_path):
    """The silent one. Nothing else on this engine notices it."""
    assert psql(pair["src"], "insert into gkey values (null,1,'ghost')"
                ).returncode == 0
    res = _engine(pair, tmp_path).check_deep("-")
    src = [r for r in res if "source" in r.scope][0]
    dst = [r for r in res if "target" in r.scope][0]
    assert src.status == "diff", src.detail
    assert "1 rows on the source have no value for id" in src.detail
    assert "left out of the comparison" in src.detail, src.detail
    assert dst.status == "ok", dst.detail


@needs_lib
def test_that_row_really_is_invisible_to_everything_else(pair, tmp_path):
    """The measurement the whole file rests on, taken here rather than
    quoted: with the extra row on the source only, every other check on
    this engine still reports the pair as matching."""
    assert psql(pair["src"], "insert into gkey values (null,1,'ghost')"
                ).returncode == 0
    assert psql(pair["src"], "select count(*) from gkey").stdout.strip() == "4"
    assert psql(pair["dst"], "select count(*) from gkey").stdout.strip() == "3"
    eng = _engine(pair, tmp_path)
    for res in (eng.check_counts("-"), eng.check_data("-"),
                eng.check_schema("-")):
        for r in res:
            assert r.status == "ok", (r.scope, r.status, r.detail)
    # and the deep battery is the one that does not
    assert any(r.status == "diff" for r in eng.check_deep("-"))


@needs_lib
def test_a_repeated_key_is_reported_with_the_value(pair, tmp_path):
    assert psql(pair["src"], "insert into gkey values (2,9,'again')"
                ).returncode == 0
    src = [r for r in _engine(pair, tmp_path).check_deep("-")
           if "source" in r.scope][0]
    assert src.status == "diff", src.detail
    assert "names more than one row on the source" in src.detail, src.detail
    assert "(2)" in src.detail, src.detail
    assert "stops being repeatable" in src.detail, src.detail


@needs_lib
def test_a_repeated_key_makes_the_answer_stop_being_repeatable(pair,
                                                               tmp_path):
    """Why the verdict says the answer cannot be relied on.

    The first draft said the comparison refuses a repeated key. It does -
    but only when both URLs point at the *same* server, which no real hop
    does. Across two servers it sometimes answers and sometimes fails, on
    the same unchanged pair, and when it answers the answer is short.

    Staged so no row under the repeated key matches any row under it on the
    other side: the source holds `4/y` and `4/z`, the target holds `4/q`.
    The truth is two rows on the source alone and one on the target alone.
    Asserted as a property rather than a number, because the number is the
    thing that moves.
    """
    assert psql(pair["src"], "insert into gkey values (4,1,'y'),(4,2,'z')"
                ).returncode == 0
    assert psql(pair["dst"], "insert into gkey values (4,3,'q')"
                ).returncode == 0
    eng = _engine(pair, tmp_path)
    seen = []
    for _ in range(4):
        got = eng._stats(eng._reladiff("gkey", ["-c", "%"]))
        seen.append(got)
        if got:
            # never the honest 2-and-1; always fewer than really differ
            assert got["only_a"] + got["only_b"] + got["updated"] < 3, got
    assert any(not g for g in seen) or len({tuple(sorted(g.items()))
                                            for g in seen}) >= 1, seen
    # and the deep battery names it rather than leaving it to be noticed
    src = [r for r in eng.check_deep("-") if "source" in r.scope][0]
    assert src.status == "diff", src.detail
    assert "stops being repeatable" in src.detail, src.detail


@needs_lib
def test_a_two_column_key_is_checked_as_the_pair_it_is(pair, tmp_path):
    """`id` repeats and `part` repeats, but the two together do not - a
    per-column check would cry wolf here."""
    assert psql(pair["src"], "insert into gkey values (1,2,'ok')"
                ).returncode == 0
    res = _engine(pair, tmp_path, key=["id", "part"]).check_deep("-")
    assert {r.status for r in res} == {"ok"}, [r.detail for r in res]
    assert "id, part" in res[0].detail, res[0].detail


@needs_lib
def test_a_null_in_either_half_of_a_two_column_key_counts(pair, tmp_path):
    assert psql(pair["src"], "insert into gkey values (9,null,'half')"
                ).returncode == 0
    src = [r for r in _engine(pair, tmp_path, key=["id", "part"]
                              ).check_deep("-") if "source" in r.scope][0]
    assert src.status == "diff", src.detail
    assert "no value for id, part" in src.detail, src.detail


@needs_lib
def test_a_table_it_cannot_read_is_an_error_not_a_pass(pair, tmp_path):
    res = _engine(pair, tmp_path, tables=("no_such_table",)).check_deep("-")
    assert res and all(r.status == "error" for r in res), \
        [(r.scope, r.status) for r in res]
    assert all(r.status != "ok" for r in res)


@needs_lib
def test_the_two_questions_are_asked_of_one_connection_each(pair, tmp_path):
    """The builder resolves a column expression in place the first time it
    is compiled, so handing the same list to a second query raises
    `Already resolved!` - which is why they are rebuilt per query. Both
    questions running is the proof."""
    assert psql(pair["src"], "insert into gkey values (null,1,'ghost')"
                ).returncode == 0
    assert psql(pair["dst"], "insert into gkey values (3,9,'again')"
                ).returncode == 0
    res = _engine(pair, tmp_path).check_deep("-")
    src = [r for r in res if "source" in r.scope][0]
    dst = [r for r in res if "target" in r.scope][0]
    assert "no value for id" in src.detail, src.detail
    assert "names more than one row on the target" in dst.detail, dst.detail


def test_the_verdicts_name_no_tool(pair, tmp_path):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    psql(pair["src"], "insert into gkey values (null,1,'ghost')")
    for r in _engine(pair, tmp_path).check_deep("-"):
        text = f"{r.detail} {r.fix_hint}".lower()
        assert not [t for t in TOOLS if t in text], text
