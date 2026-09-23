"""The engine that compared rows and never compared columns.

`GenericEngine` covers everything migkit reaches through one comparison
library - snowflake, bigquery, redshift, clickhouse, oracle, trino, presto,
duckdb, vertica. It declared `checks = ("counts", "data")`, so a hop on any
of them **never compared the two schemas at all**. A target built by hand
with `int` where the source has `bigint` matched on row counts, matched
row for row, and overflowed later: the class of failure that costs nothing
to find now and a cutover to find then.

What the catalogue answers is the same five columns for every one of those
engines - (name, declared type, datetime precision, numeric precision,
numeric scale) - so one comparison serves all nine. Measured on
PostgreSQL 16 against a deliberately mismatched pair:

    source id   ('id', 'bigint', None, 64, 0)
    target id   ('id', 'integer', None, 32, 0)              seen
    source amt  ('amt', 'numeric', None, 12, 2)
    target amt  ('amt', 'numeric', None, 12, 4)             seen
    source ts   ('ts', 'timestamp with time zone', 6, ...)
    target ts   ('ts', 'timestamp without time zone', 6, ...)  seen
    source v    ('v', 'character varying', None, None, None)
    target v    ('v', 'character varying', None, None, None)
                   varchar(50) against varchar(200)         NOT seen
    n_null      identical whether or not it is NOT NULL     NOT seen

Length and nullability are simply not in the query the library issues -
read its source: it selects those five and nothing else, per dialect. Asking
for more would mean writing that query nine times, eight of them against
engines that cannot be tried here.

So the check reports what it can see and **says what it did not look at,
every time, including when it passes**. A clean line that quietly means
"some of the schema" is the kind of reassurance this tool exists to refuse.

The normalised types are not usable for this and that is the reason the raw
rows are read: `bigint` and `integer` both come back as `Integer`, so the
target that will overflow looks identical to the source that will not.
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

SRC_DDL = ("create table gsch (id bigint primary key, v varchar(50),"
           " amt numeric(12,2), ts timestamptz, note text,"
           " n_null int not null)")
DST_DDL = ("create table gsch (id int primary key, v varchar(200),"
           " amt numeric(12,4), ts timestamp, note text, n_null int,"
           " extra text)")


def _engine(pg_pair, tmp_path, tables=("gsch",)):
    from migkit.engines.generic import GenericEngine

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": list(tables), "key": "id"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return GenericEngine(hop)


def test_the_check_is_offered_at_all():
    """It was not. `check --only schema` on a generic hop had nothing to
    run, and a full check silently skipped the schema."""
    from migkit.engines.generic import GenericEngine
    assert "schema" in GenericEngine.checks


# ---- the verdict, built from catalogue rows, no servers needed ----

def _row(name, kind, dt=None, prec=None, scale=None):
    return (name, kind, dt, prec, scale)


def _verdict(tmp_path, src, dst):
    from migkit.engines.generic import GenericEngine
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": "postgresql://x/y"}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": "postgresql://x/y"}),
              options={"tables": ["t"], "key": "id"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return GenericEngine(hop)._schema_result("t", src, dst)


def test_a_narrower_integer_on_the_target_is_named_as_a_loss(tmp_path):
    """The failure this whole file exists for: it passes counts, passes the
    row comparison, and overflows on a value the source already holds."""
    got = _verdict(tmp_path,
                   {"id": _row("id", "bigint", None, 64, 0)},
                   {"id": _row("id", "integer", None, 32, 0)})
    assert got.status == "diff", got.detail
    assert "bigint(64)" in got.detail and "integer(32)" in got.detail
    assert "narrower on the target" in got.detail, got.detail
    assert "will not fit" in got.detail, got.detail


def test_a_wider_integer_is_reported_without_crying_wolf(tmp_path):
    """Still a difference - the schemas are not the same - but the target
    holds every value the source does, and the line has to say which way
    the risk runs or it teaches people to ignore it."""
    got = _verdict(tmp_path,
                   {"id": _row("id", "integer", None, 32, 0)},
                   {"id": _row("id", "bigint", None, 64, 0)})
    assert got.status == "diff", got.detail
    assert "wider on the target" in got.detail, got.detail
    assert "will not fit" not in got.detail, got.detail


def test_losing_the_time_zone_is_called_what_it_is(tmp_path):
    got = _verdict(tmp_path,
                   {"ts": _row("ts", "timestamp with time zone", 6)},
                   {"ts": _row("ts", "timestamp without time zone", 6)})
    assert got.status == "diff", got.detail
    assert "drops the offset" in got.detail, got.detail


def test_gaining_a_time_zone_is_not_described_as_losing_one(tmp_path):
    got = _verdict(tmp_path,
                   {"ts": _row("ts", "timestamp without time zone", 6)},
                   {"ts": _row("ts", "timestamp with time zone", 6)})
    assert "drops the offset" not in got.detail, got.detail
    assert "keeps an offset" in got.detail, got.detail


def test_fewer_decimal_places_is_named_as_rounding(tmp_path):
    got = _verdict(tmp_path,
                   {"amt": _row("amt", "numeric", None, 12, 4)},
                   {"amt": _row("amt", "numeric", None, 12, 2)})
    assert got.status == "diff", got.detail
    assert "numeric(12,4)" in got.detail and "numeric(12,2)" in got.detail
    assert "round on the way in" in got.detail, got.detail


def test_a_column_the_target_does_not_have(tmp_path):
    got = _verdict(tmp_path,
                   {"id": _row("id", "integer", None, 32, 0),
                    "v": _row("v", "text")},
                   {"id": _row("id", "integer", None, 32, 0)})
    assert got.status == "diff", got.detail
    assert "target does not have" in got.detail and "v" in got.detail


def test_a_column_only_the_target_has(tmp_path):
    got = _verdict(tmp_path,
                   {"id": _row("id", "integer", None, 32, 0)},
                   {"id": _row("id", "integer", None, 32, 0),
                    "extra": _row("extra", "text")})
    assert got.status == "diff", got.detail
    assert "only the target has" in got.detail and "extra" in got.detail


def test_matching_columns_pass(tmp_path):
    same = {"id": _row("id", "bigint", None, 64, 0),
            "v": _row("v", "text")}
    got = _verdict(tmp_path, same, dict(same))
    assert got.status == "ok", got.detail
    assert "2 columns" in got.detail, got.detail


def test_every_verdict_says_what_it_did_not_look_at(tmp_path):
    """Including the clean one. Someone reading `schema OK` on a generic hop
    would otherwise believe lengths and nullability had been checked."""
    clean = _verdict(tmp_path, {"id": _row("id", "bigint", None, 64, 0)},
                     {"id": _row("id", "bigint", None, 64, 0)})
    dirty = _verdict(tmp_path, {"id": _row("id", "bigint", None, 64, 0)},
                     {"id": _row("id", "integer", None, 32, 0)})
    for got in (clean, dirty):
        assert "string lengths" in got.detail, got.detail
        assert "nullability" in got.detail, got.detail


def test_the_report_still_names_no_tool(tmp_path):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    got = _verdict(tmp_path, {"id": _row("id", "bigint", None, 64, 0)},
                   {"id": _row("id", "integer", None, 32, 0)})
    text = f"{got.detail} {got.fix_hint}".lower()
    assert not [t for t in TOOLS if t in text], text


# ---- against two real servers ----

@pytest.fixture
def mismatched(pg_pair):
    psql(pg_pair["src"], "drop table if exists gsch")
    psql(pg_pair["dst"], "drop table if exists gsch")
    assert psql(pg_pair["src"], SRC_DDL).returncode == 0
    assert psql(pg_pair["dst"], DST_DDL).returncode == 0
    yield pg_pair
    psql(pg_pair["src"], "drop table if exists gsch")
    psql(pg_pair["dst"], "drop table if exists gsch")


@needs_lib
def test_it_finds_all_of_that_on_two_real_servers(mismatched, tmp_path):
    res = _engine(mismatched, tmp_path).check_schema("-")
    assert len(res) == 1, res
    got = res[0]
    assert got.status == "diff", got.detail
    for expected in ("bigint(64)", "integer(32)", "narrower on the target",
                     "numeric(12,2)", "numeric(12,4)",
                     "drops the offset", "only the target has", "extra"):
        assert expected in got.detail, (expected, got.detail)


@needs_lib
def test_what_it_misses_is_really_missed_and_really_said(mismatched,
                                                         tmp_path):
    """The blind spots are asserted against the servers, not quoted from
    the library's docs. `v` is varchar(50) on the source and varchar(200)
    on the target, and `n_null` is NOT NULL on one side only - neither
    shows up, which is why every verdict says so."""
    got = _engine(mismatched, tmp_path).check_schema("-")[0]
    assert "v is" not in got.detail, got.detail
    assert "n_null" not in got.detail, got.detail
    assert "string lengths, nullability" in got.detail, got.detail
    # and the premise: the two really are declared differently
    assert psql(mismatched["src"], "select character_maximum_length from"
                " information_schema.columns where table_name='gsch'"
                " and column_name='v'").stdout.strip() == "50"
    assert psql(mismatched["dst"], "select character_maximum_length from"
                " information_schema.columns where table_name='gsch'"
                " and column_name='v'").stdout.strip() == "200"


@needs_lib
def test_two_matching_schemas_pass_on_real_servers(pg_pair, tmp_path):
    psql(pg_pair["src"], "drop table if exists gsch")
    psql(pg_pair["dst"], "drop table if exists gsch")
    for port in (pg_pair["src"], pg_pair["dst"]):
        assert psql(port, SRC_DDL).returncode == 0
    try:
        got = _engine(pg_pair, tmp_path).check_schema("-")[0]
        assert got.status == "ok", got.detail
        assert "6 columns" in got.detail, got.detail
    finally:
        for port in (pg_pair["src"], pg_pair["dst"]):
            psql(port, "drop table if exists gsch")


@needs_lib
def test_an_empty_table_is_compared_like_any_other(pg_pair, tmp_path):
    """Which is the normal state of a target before the data is moved -
    and the moment this check is most worth running."""
    psql(pg_pair["src"], "drop table if exists gsch")
    psql(pg_pair["dst"], "drop table if exists gsch")
    assert psql(pg_pair["src"], SRC_DDL).returncode == 0
    assert psql(pg_pair["dst"], DST_DDL).returncode == 0
    try:
        assert psql(pg_pair["dst"], "select count(*) from gsch"
                    ).stdout.strip() == "0"
        got = _engine(pg_pair, tmp_path).check_schema("-")[0]
        assert got.status == "diff", got.detail
        assert "narrower on the target" in got.detail, got.detail
    finally:
        for port in (pg_pair["src"], pg_pair["dst"]):
            psql(port, "drop table if exists gsch")


@needs_lib
def test_a_table_that_is_not_there_is_an_error_not_a_pass(pg_pair, tmp_path):
    """"I could not read it" must never read as "they match"."""
    got = _engine(pg_pair, tmp_path, tables=("no_such_table_at_all",)
                  ).check_schema("-")[0]
    assert got.status == "error", got.detail
    assert got.status != "ok"
    assert "does not exist" in got.detail, got.detail
