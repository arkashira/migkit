"""The engine that wraps reladiff, run against two real PostgreSQL servers.

Two things were wrong, and both were found by running it rather than reading
it. The data check asked whether reladiff had printed `0 rows are different`.
Measured, reladiff 0.6.0 never prints that sentence - on two identical tables
it prints `0.00% difference score` - so `migkit check --check data` reported a
difference on every run, including on tables that matched.

The count check was worse in a quieter way. It compared reladiff's `N rows in
table A/B`, and those numbers are not a reading of the tables. The same command
against one unchanging pair of 50-row tables, three times in a row:

    50 rows in table A |  50 rows in table B | ... | 49 unchanged |   2.00%
     0 rows in table A |   1 rows in table B | ... | -1 unchanged | 200.00%
     0 rows in table A |   1 rows in table B | ... | -1 unchanged | 200.00%

`-1 rows unchanged` is its own proof. The exclusive and updated counts came out
identical every time and matched the rows that really differed, so the verdict
rests on those - and the row-count question is still answerable from them,
because everything the two sides share cancels out.

reladiff also exits 0 for everything: matching tables, differing tables, a
table that does not exist, a URL scheme it does not support. The absence of its
numbers is the only sign it did not compare anything.
"""
import subprocess

import pytest

from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

# migkit's own lookup, not `shutil.which`: the tool is installed beside the
# interpreter that runs it, and asking a different question here would skip
# every test below on a machine where the engine works perfectly well
RELADIFF = which("reladiff")
needs_reladiff = pytest.mark.skipif(
    not RELADIFF, reason="reladiff is not where migkit would look for it")


def _engine(pg_pair, tables=("t",), key="id"):
    from migkit.engines.generic import GenericEngine
    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": list(tables), "key": key})
    return GenericEngine(hop)


def _seed(pg_pair, target_extra=""):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table t (id bigint primary key, v text);"
                   " insert into t select g, 'v' from generate_series(1,50) g;")
    if target_extra:
        psql(pg_pair["dst"], target_extra)


@needs_reladiff
def test_two_tables_that_match_are_reported_as_matching(pg_pair):
    """The bug that made the engine useless: every run said `diff`."""
    _seed(pg_pair)
    eng = _engine(pg_pair)
    counts = eng.check_counts("-")
    data = eng.check_data("-")
    assert [r.status for r in counts] == ["ok"], [r.detail for r in counts]
    assert [r.status for r in data] == ["ok"], [r.detail for r in data]
    assert "no row is on one side only" in data[0].detail, data[0].detail


@needs_reladiff
def test_the_verdict_does_not_change_between_runs(pg_pair):
    """The totals reladiff prints do change between runs - that is the whole
    finding - so the same check is run three times and has to agree with
    itself."""
    _seed(pg_pair, "update t set v='CHANGED' where id in (3,7);"
                   " delete from t where id = 40;"
                   " insert into t values (99,'only here');")
    eng = _engine(pg_pair)
    seen = set()
    for _ in range(3):
        counts = eng.check_counts("-")[0]
        data = eng.check_data("-")[0]
        seen.add((counts.status, counts.detail, data.status, data.detail))
    assert len(seen) == 1, seen
    (count_status, _, data_status, data_detail), = seen
    # one row deleted and one inserted on the target: the counts really match
    assert count_status == "ok"
    assert data_status == "diff"
    assert "1 rows only on the source" in data_detail, data_detail
    assert "1 rows only on the target" in data_detail, data_detail
    assert "2 rows with different values" in data_detail, data_detail


@needs_reladiff
def test_a_real_count_difference_is_named_with_its_direction(pg_pair):
    _seed(pg_pair, "delete from t where id in (11,12,13);")
    got = _engine(pg_pair).check_counts("-")
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    assert "3 more rows on the source" in got[0].detail, got[0].detail

    # and the other way round
    psql(pg_pair["dst"], "insert into t values (101,'x'),(102,'x'),"
                         "(103,'x'),(104,'x'),(105,'x'),(106,'x');")
    got = _engine(pg_pair).check_counts("-")
    assert "3 more rows on the target" in got[0].detail, got[0].detail


@needs_reladiff
def test_a_table_that_is_not_there_is_an_error_not_a_difference(pg_pair):
    """reladiff exits 0 and prints an ERROR line on stderr, so the exit code
    cannot be what decides this."""
    _seed(pg_pair)
    eng = _engine(pg_pair, tables=["ghost"])
    counts = eng.check_counts("-")
    data = eng.check_data("-")
    assert [r.status for r in counts] == ["error"], [r.detail for r in counts]
    assert [r.status for r in data] == ["error"], [r.detail for r in data]
    assert "does not exist" in data[0].detail, data[0].detail
    assert "not a table whose counts match" in counts[0].detail


@needs_reladiff
def test_reladiff_really_does_exit_zero_on_a_failure(pg_pair):
    """Pinned because the checks above are built on it being true."""
    _seed(pg_pair)
    p = subprocess.run(
        [RELADIFF, f"postgresql://postgres:test@127.0.0.1:{pg_pair['src']}"
                   "/postgres", "ghost",
         f"postgresql://postgres:test@127.0.0.1:{pg_pair['dst']}/postgres",
         "ghost", "-k", "id", "--stats"],
        capture_output=True, text=True)
    assert p.returncode == 0, p.returncode
    assert "does not exist" in p.stderr, p.stderr[-300:]
    assert not p.stdout.strip(), p.stdout


@needs_reladiff
def test_the_numbers_the_verdict_avoids_are_the_ones_that_move(pg_pair):
    """The measurement in this file's header, run again. If reladiff is ever
    fixed this test fails, and the comment above it stops being true - which
    is the point of pinning it rather than describing it."""
    _seed(pg_pair, "delete from t where id = 40;"
                   " insert into t values (99,'only here');")
    eng = _engine(pg_pair)
    totals, differences = set(), set()
    for _ in range(3):
        got = eng._stats(eng._reladiff("t", []))
        assert got, "reladiff printed no stats at all"
        totals.add((got["rows_a"], got["rows_b"]))
        differences.add((got["only_a"], got["only_b"], got["updated"]))
    assert differences == {(1, 1, 0)}, differences
    assert len(totals) > 1 or (50, 50) in totals, totals


def test_the_engine_says_what_is_missing_before_it_runs_anything(tmp_path):
    """Configuration problems come back as a sentence, not a traceback."""
    from migkit.engines.generic import GenericEngine
    ep = Endpoint(host="x", port=0, user="", password="", options={})
    hop = Hop(name="g", engine="generic", source=ep, target=ep, options={})
    eng = GenericEngine(hop)
    with pytest.raises(SystemExit, match="src.url"):
        eng._url("src")
    hop.options["tables"] = []
    with pytest.raises(SystemExit, match="options.tables"):
        eng._tables()
