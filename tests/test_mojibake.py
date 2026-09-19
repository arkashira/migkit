"""Text that was already broken before anybody moved it.

An application sending UTF-8 through a latin1 connection stores the bytes as
latin1 characters: `é` becomes `Ã©`. Everything looks right until a
conversion or a client change, and by then the history is mixed - some rows
double-encoded, some fine - which is what makes the obvious repair dangerous.

The test is a round trip, not a list of suspicious substrings: re-encode the
characters into the bytes they would have been and see whether those bytes
are valid UTF-8 saying something else. Measured on a live column before any
of this was written, the rule separated them cleanly:

    caught      'cafÃ©'  'naÃ¯ve rÃ©sumÃ©'  'emâ\\x80\\x94dash'
                'emâ€"dash' (cp1252)       'Â£100'
    left alone  'café'  'Ångström'  'Müller'  'Ação'  'Ça va'  'Ægir'
                '£100'  '日本語'

`£100` and `Â£100` in one column is the whole problem in two rows. Measured:
the blanket repair applied to that column fails outright on PostgreSQL -
`invalid byte sequence for encoding "UTF8": 0xa3` - and the application-side
version of the same fix, the one that passes `errors='replace'`, turns it
into `�100` without a word.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

#: what a latin1 connection does to a UTF-8 string, written as SQL
BROKEN = "convert_from(convert_to({!r}, 'UTF8'), 'LATIN1')"


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="moji", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed(pg_pair, values):
    """`values` are SQL expressions, so a test can mix literals with the
    conversion that produces the real thing."""
    rows = ", ".join(f"({i}, {v})" for i, v in enumerate(values, start=1))
    got = psql(pg_pair["src"],
               "drop table if exists public.notes;"
               " create table public.notes (id int primary key, body text);"
               f" insert into public.notes values {rows};")
    assert got.returncode == 0, got.stderr


def _moji(engine):
    return engine._mojibake("postgres")


def test_a_column_holding_both_kinds_is_the_finding(pg_pair, tmp_path):
    """Not "this column has mojibake" - which columns cannot be fixed in one
    pass, because the fix destroys the rows that were never broken."""
    _seed(pg_pair, [BROKEN.format("café"), "'£100'", BROKEN.format("£100"),
                    "'Müller'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "public.notes.body" in got.detail, got.detail
    assert "2 double-encoded and 2 genuinely accented" in got.detail, got.detail
    assert "destroys the second" in got.detail, got.detail
    assert "row by row" in got.fix_hint, got.fix_hint


def test_what_the_text_really_says_is_in_the_report(pg_pair, tmp_path):
    """A name and a count are not enough to act on: the operator has to be
    able to agree that the value is broken before converting a column."""
    _seed(pg_pair, [BROKEN.format("café"), "'Müller'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "cafÃ©" in got.detail, got.detail
    assert "café" in got.detail, got.detail


def test_a_uniformly_broken_column_says_one_conversion_fits(pg_pair,
                                                             tmp_path):
    _seed(pg_pair, [BROKEN.format("café"), BROKEN.format("naïve résumé"),
                    "'plain ascii'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "double-encoded throughout" in got.detail, got.detail
    assert "one conversion over the column fits" in got.detail, got.detail


def test_genuinely_accented_text_is_not_a_finding(pg_pair, tmp_path):
    """The false-positive test, and the reason the check is a round trip
    rather than a search for `Ã`. Every one of these is correct text that a
    substring rule would have condemned."""
    _seed(pg_pair, ["'café'", "'Ångström'", "'Müller'", "'Ação'", "'Ça va'",
                    "'Ægir'", "'£100'", "'日本語'", "'naïve'", "'señor'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "ok", got.detail
    # and it did read them - an empty scan would also say ok
    assert "10 non-ASCII values sampled" in got.detail, got.detail


def test_the_cp1252_flavour_is_caught_too(pg_pair, tmp_path):
    """The `â€"` everybody recognises. cp1252 maps the C1 block to
    typographic characters, so the same UTF-8 bytes produce a different -
    and much more visible - wreck than latin1 does."""
    _seed(pg_pair, ["convert_from(convert_to('em—dash', 'UTF8'), 'WIN1252')",
                    "'Müller'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "â€" in got.detail, got.detail
    assert "em—dash" in got.detail, got.detail


def test_a_value_holding_a_tab_and_a_newline_is_still_one_value(pg_pair,
                                                                 tmp_path):
    """The rows come back as JSON for this reason. Read line by line and
    split on tabs, one broken value becomes three and the counts are
    fiction - a mistake this project has already made once, elsewhere."""
    _seed(pg_pair, [f"{BROKEN.format('café')} || chr(9) || 'x' || chr(10)"
                    f" || {BROKEN.format('résumé')}",
                    "'Müller'"])
    got = _moji(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "1 double-encoded and 1 genuinely accented" in got.detail, got.detail


def test_a_table_with_no_text_at_all_is_not_a_finding(pg_pair, tmp_path):
    got = psql(pg_pair["src"], "drop table if exists public.notes;"
                               " create table public.notes (id int, n int);"
                               " insert into public.notes values (1, 2);")
    assert got.returncode == 0, got.stderr
    res = _moji(_engine(pg_pair, tmp_path))
    assert res.status == "ok", res.detail
    assert "no text column holds a non-ASCII character" in res.detail


def test_the_full_deep_report_carries_it(pg_pair, tmp_path):
    _seed(pg_pair, [BROKEN.format("café"), "'Müller'"])
    got = [r for r in _engine(pg_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("mojibake")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail


def test_the_classification_needs_no_server():
    """The exact strings measured against a live column, pinned so the rule
    cannot drift into matching ordinary accented text."""
    from migkit.engines.base import Engine

    for good in ("café", "Ångström", "Müller", "Ação", "Ça va", "Ægir",
                 "£100", "日本語", "señor", "naïve", "plain ascii"):
        assert Engine._double_encoded(good) is None, good

    for bad, really in (("cafÃ©", "café"),
                        ("naÃ¯ve rÃ©sumÃ©", "naïve résumé"),
                        ("emâ\x80\x94dash", "em—dash"),
                        ("emâ€”dash", "em—dash"),
                        ("Â£100", "£100")):
        said = Engine._double_encoded(bad)
        assert said is not None, bad
        assert said[1] == really, (bad, said)


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="m", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    mixed = eng._mojibake_result(
        "x", [("t", "body", 3, 5, "cafÃ©", "café")], 8, "hint")
    assert mixed.status == "diff" and "destroys the second" in mixed.detail

    whole = eng._mojibake_result(
        "x", [("t", "body", 3, 0, "cafÃ©", "café")], 3, "hint")
    assert whole.status == "diff"
    assert "double-encoded throughout" in whole.detail

    # a column that is merely accented is not a finding
    fine = eng._mojibake_result(
        "x", [("t", "body", 0, 9, None, None)], 9, "hint")
    assert fine.status == "ok" and "9 non-ASCII values" in fine.detail

    # and a mixed column outranks a uniformly broken one in the same report
    both = eng._mojibake_result(
        "x", [("t", "clean_sweep", 3, 0, "cafÃ©", "café"),
              ("t", "half_and_half", 3, 5, "cafÃ©", "café")], 11, "hint")
    assert both.status == "diff"
    assert "half_and_half" in both.detail
    assert "clean_sweep" not in both.detail, both.detail


# ---- mysql, so this is not a postgres-only capability ------------------

MY = "migkit-test-moji-my"
MY_PORT = 13403


@pytest.fixture(scope="module")
def mysql_one():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                break
        time.sleep(1)
    for _ in range(60):
        if subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    yield MY_PORT
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def test_mysql_finds_the_same_mixed_column(mysql_one, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    sql = ("drop database if exists mo; create database mo"
           " character set utf8mb4;"
           " create table mo.notes (id int primary key, body text);"
           " insert into mo.notes values"
           # the same latin1 round trip, in MySQL's spelling
           " (1, convert(convert('café' using binary) using latin1)),"
           " (2, '£100'),"
           " (3, convert(convert('£100' using binary) using latin1)),"
           " (4, 'Müller');")
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "--default-character-set=utf8mb4", "-e", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr

    # the seed really is double-encoded before migkit is asked about it
    stored = subprocess.run(
        ["docker", "exec", MY, "mysql", "-uroot", "-ptest", "-N", "-B",
         "--default-character-set=utf8mb4", "-e",
         "select body from mo.notes where id = 1"],
        capture_output=True, text=True).stdout.strip()
    assert stored == "cafÃ©", repr(stored)

    hop = Hop(name="moji", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              databases=["mo"])
    hop.report_dir = lambda db=None: tmp_path
    res = MySQLEngine(hop)._mojibake("mo")
    assert res.status == "diff", res.detail
    assert "notes.body" in res.detail, res.detail
    assert "2 double-encoded and 2 genuinely accented" in res.detail, res.detail
