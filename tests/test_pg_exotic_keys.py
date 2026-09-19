"""Primary keys holding the characters the drilldown file is made of.

The row-level repair writes the differing keys to `data-<table>.missing`
and friends, and reads them back with psql's `\\copy`. That reader parses
COPY text: a tab separates columns and a backslash begins an escape. The
writer was putting the raw value in, so the two disagreed. Measured against
PostgreSQL 16 on a text primary key:

    key `has<TAB>tab`   ERROR: extra data after last expected column
                        COPY _pk, line 2: "has  tab"     nothing repaired
    key `back\\slash`    apply returned with no error, the row was not
                        restored, and the next check still said diff

The second is the one that matters. `\\s` is not an escape COPY knows, so it
read `backslash`, the join matched no row, and `migkit sync --apply`
reported a repair it had not performed. Neither key is exotic in the way
that word suggests: Windows paths, regexes and anything pasted out of a log
carry backslashes, and a tab arrives in a text key the first time somebody
loads a file with one in it.

The fix is to write what the reader parses. Everything below is the
behaviour that produces, against real servers.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

# a backslash, a tab, a newline and a carriage return: the four characters
# COPY text gives a meaning to, one per row
EXOTIC = (r"back\slash", "has\ttab", "line\nbreak", "carriage\rreturn")


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed(pg_pair, keys):
    # dollar quoting so the key goes in exactly as written, with no
    # escaping rules of its own between here and the table
    rows = ", ".join("($tag${}$tag$, 'v')".format(k) for k in keys)
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists t;"
                         " create table t (k text primary key, v text);"
                         f" insert into t values {rows};")
        assert got.returncode == 0, got.stderr


def _keys_on(port):
    got = psql(port, "select k from t order by k;")
    return got.stdout


def test_a_row_whose_key_holds_an_escape_is_really_restored(pg_pair,
                                                            tmp_path):
    """The measurement this file opens with. It is not enough that `apply`
    returns: the rows have to be there afterwards."""
    _seed(pg_pair, ("plain",) + EXOTIC)
    psql(pg_pair["dst"], "delete from t where k <> 'plain';")
    assert psql(pg_pair["dst"], "select count(*) from t;").stdout.strip() == "1"

    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("postgres")[0].status == "diff"
    written = (tmp_path / "data-public.t.missing").read_bytes()
    assert b"back\\\\slash" in written, written     # one backslash escaped
    assert b"has\\ttab" in written, written         # a tab, not a real one
    assert b"line\\nbreak" in written, written

    actions = eng.repair_plan("postgres", "rows")
    assert actions, "nothing to repair, so the repair path never ran"
    eng.apply("postgres", actions[0])

    assert psql(pg_pair["dst"], "select count(*) from t;").stdout.strip() == "5"
    assert _keys_on(pg_pair["dst"]) == _keys_on(pg_pair["src"])
    after = eng.check_data("postgres")
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]


def test_a_row_the_source_does_not_have_is_removed_by_its_escaped_key(
        pg_pair, tmp_path):
    """The delete half reads the same file, so it fails the same way."""
    _seed(pg_pair, ("plain",))
    for key in EXOTIC:
        got = psql(pg_pair["dst"], f"insert into t values ($tag${key}$tag$,"
                                   " 'only here');")
        assert got.returncode == 0, got.stderr

    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("postgres")[0].status == "diff"
    eng.apply("postgres", eng.repair_plan("postgres", "rows")[0])
    assert psql(pg_pair["dst"], "select count(*) from t;").stdout.strip() == "1"
    assert [r.status for r in eng.check_data("postgres")] == ["ok"]


def test_a_changed_row_with_such_a_key_is_put_right(pg_pair, tmp_path):
    _seed(pg_pair, ("plain",) + EXOTIC)
    psql(pg_pair["dst"], r"update t set v = 'wrong' where k = 'back\slash';")

    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("postgres")[0].status == "diff"
    eng.apply("postgres", eng.repair_plan("postgres", "rows")[0])
    got = psql(pg_pair["dst"], r"select v from t where k = 'back\slash';")
    assert got.stdout.strip() == "v", got.stdout
    assert [r.status for r in eng.check_data("postgres")] == ["ok"]


def test_two_composite_keys_that_would_have_collided_stay_apart(pg_pair,
                                                                 tmp_path):
    """Joining the columns with a tab is only unambiguous while no value
    contains one. `('x<TAB>y','z')` and `('x','y<TAB>z')` both render as
    `x<TAB>y<TAB>z`, and a repair addressing one of them would be working
    on whichever row that string happened to find.
    """
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists t;"
                         " create table t (a text, b text, v text,"
                         " primary key (a, b));"
                         " insert into t values"
                         " (E'x\\ty', 'z', 'first'),"
                         " ('x', E'y\\tz', 'second');")
        assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "update t set v='wrong' where v='first';")

    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("postgres")[0].status == "diff"
    lines = (tmp_path / "data-public.t.changed").read_text().splitlines()
    assert len(lines) == 1, lines
    # the two keys are distinguishable in the file: the tab inside a value
    # is written as an escape, the one between the columns is a real tab
    assert lines[0].count("\t") == 1, repr(lines[0])
    assert "\\t" in lines[0], repr(lines[0])

    eng.apply("postgres", eng.repair_plan("postgres", "rows")[0])
    assert psql(pg_pair["dst"], "select v from t order by v;"
                ).stdout.split() == ["first", "second"]
    assert [r.status for r in eng.check_data("postgres")] == ["ok"]


def test_the_encoding_survives_a_round_trip_without_a_server():
    """The reader has to undo exactly what the writer did, including
    PostgreSQL's own rule for an escape COPY does not recognise: the
    backslash goes, the character stays."""
    from migkit.engines.postgres import PostgresEngine as P
    for value in (r"back\slash", "has\ttab", "line\nbreak", "plain",
                  "\\", "\\\\", "ends with a backslash\\", "", "tab\tand\\"):
        encoded = (value.replace("\\", "\\\\").replace("\t", "\\t")
                        .replace("\n", "\\n").replace("\r", "\\r"))
        assert P._pk_unescape(encoded) == value, (value, encoded)
    assert P._pk_unescape(r"back\slash") == "backslash"
    assert P._pk_parts("a\\tb\tc") == ["a\tb", "c"]


def test_the_expression_escapes_before_it_joins(pg_pair):
    """A unit check on the SQL itself, run by the server rather than
    asserted about a string: the escaping has to happen per column, or the
    tab it writes would be taken for a column boundary."""
    from migkit.engines.postgres import PostgresEngine as P
    expr = P._pk_text_expr(["a", "b"])
    got = psql(pg_pair["src"],
               "with t(a, b) as (values (E'x\\ty', 'z'))"
               f" select {expr} from t;")
    assert got.returncode == 0, got.stderr
    assert got.stdout.strip() == "x\\ty\tz", repr(got.stdout)
