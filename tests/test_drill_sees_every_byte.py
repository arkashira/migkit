"""Two answers from one run, and the confident one was wrong.

`check --drill` exists to answer the question the digest cannot: *which
rows, and what in them*. On a pair whose only difference was a carriage
return inside one text value, the two halves of the same run said this:

    check --only data    DIFF  src=2|1033642321960617076|-6274568243343820993
                         pk-level file data-public.t.changed  ->  5
    check --drill        Number of rows with some compared columns unequal: 0
                         Number of rows with all compared columns equal: 2

An operator who reaches for `--drill` to understand a DIFF is told the
table is clean, and the natural reading of that is "the digest was a false
positive". The digest was right.

The mechanism, isolated rather than assumed. `fetch_sample_df` ran psql
with `text=True`, which decodes with universal newlines - the same
subprocess call, twice:

    capture_output=True                  b'"one\\r\\ntwo"\\n'
    capture_output=True, text=True        '"one\\ntwo"\\n'

So the CR was gone before pandas, on both sides, and the two sides became
equal. The MySQL engine reads its sample through a driver and never had
this; it is the CSV-through-psql path alone.

Carriage returns are not exotic. Any text authored on Windows, pasted from
one, or round-tripped through a CSV carries them, and they are exactly the
kind of difference a migration introduces.

Counting them was only half of it. datacompy's own sample prints the pairs
as they are, which for these rows is two identical-looking strings:

       id v (source)  v (target)
    0   2      hello      hello
    1   3         ab         ab
    2   6        red        blue

So the drilldown now adds what it can work out that datacompy cannot - the
values escaped, and the reason beside them:

      id=3  v
          source  'ab'
          target  'a\\u200bb'
          a zero-width character (U+200B)

`red` against `blue` is deliberately absent: a difference anybody can see
needs no explanation, and a section that repeated every differing row would
bury the ones that do.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15531, 15532
NAMES = {SRC: "migkit-test-inv-src", DST: "migkit-test-inv-dst"}

#: (key, what the source holds, what the target holds). Every pair differs
#: in bytes; every pair but the last looks identical printed.
PAIRS = [
    (1, "café", "café"),          # NFC vs NFD
    (2, "hello", "hello "),                  # trailing space
    (3, "ab", "a​b"),                   # zero-width space
    (4, "a b", "a b"),                  # space vs non-breaking space
    (5, "one\ntwo", "one\r\ntwo"),           # LF vs CRLF
    (6, "red", "blue"),                      # the control: visible
]


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", "-i", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


@pytest.fixture(scope="module")
def inv_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _seed(pair, rows):
    for port, idx in ((pair["src"], 1), (pair["dst"], 2)):
        sql = ["drop table if exists t;",
               "create table t (id int primary key, v text);"]
        for r in rows:
            sql.append("insert into t values (%d, '%s');"
                       % (r[0], r[idx].replace("'", "''")))
        got = q(port, "\n".join(sql))
        assert got.returncode == 0, got.stderr


def _engine(pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="inv", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _cli(pair, tmp_path, monkeypatch, args):
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  inv:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return CliRunner().invoke(cli.main, args).output


def _unequal(out):
    for line in out.splitlines():
        if "rows with some compared columns unequal" in line:
            return int(line.rsplit(":", 1)[1])
    raise AssertionError(f"no row summary in:\n{out}")


def test_the_carriage_return_really_is_in_the_database(inv_pair):
    """The control. Everything below is worthless if the CR never made it
    into the target in the first place."""
    _seed(inv_pair, PAIRS)
    lens = [q(p, "select octet_length(v) from t where id = 5").stdout.strip()
            for p in (inv_pair["src"], inv_pair["dst"])]
    assert lens == ["7", "8"], lens
    assert q(inv_pair["dst"], "select position(chr(13) in v) from t"
                              " where id = 5").stdout.strip() == "4"


def test_the_sample_reader_keeps_the_byte_the_decoder_used_to_eat(inv_pair,
                                                                    tmp_path):
    _seed(inv_pair, PAIRS)
    eng = _engine(inv_pair, tmp_path)
    got = {side: eng.fetch_sample_df(side, "postgres", "public.t", 100)
           for side in ("src", "dst")}
    values = {side: df[df["id"] == 5]["v"].iloc[0]
              for side, df in got.items()}
    assert values["src"] == "one\ntwo", values
    assert values["dst"] == "one\r\ntwo", values
    assert values["src"] != values["dst"]


def test_every_invisible_difference_is_counted(inv_pair, tmp_path,
                                                 monkeypatch):
    _seed(inv_pair, PAIRS)
    out = _cli(inv_pair, tmp_path, monkeypatch,
               ["check", "inv", "--db", "postgres", "--table", "public.t",
                "--drill"])
    assert _unequal(out) == len(PAIRS), out


def test_the_drilldown_and_the_pk_level_file_agree(inv_pair, tmp_path,
                                                     monkeypatch):
    """The finding that started this: one run, two answers. A table whose
    only difference is a carriage return is the smallest case that told
    them apart."""
    _seed(inv_pair, [(1, "same both sides", "same both sides"),
                     (5, "one\ntwo", "one\r\ntwo")])
    data = _cli(inv_pair, tmp_path, monkeypatch,
                ["check", "inv", "--only", "data"])
    assert "DIFF" in data, data
    changed = (tmp_path / "reports" / "inv" / "postgres"
               / "data-public.t.changed")
    assert changed.read_text().split() == ["5"], changed.read_text()

    drill = _cli(inv_pair, tmp_path, monkeypatch,
                 ["check", "inv", "--db", "postgres", "--table", "public.t",
                  "--drill"])
    assert _unequal(drill) == 1, drill


def test_a_pair_that_really_matches_still_reports_nothing(inv_pair, tmp_path,
                                                            monkeypatch):
    """The other half. A reader that stopped normalising must not have
    started inventing differences - a `--drill` that cries wolf on an equal
    table is worse than the bug it replaced."""
    same = [(i, s, s) for i, s, _ in PAIRS]
    _seed(inv_pair, same)
    out = _cli(inv_pair, tmp_path, monkeypatch,
               ["check", "inv", "--db", "postgres", "--table", "public.t",
                "--drill"])
    assert _unequal(out) == 0, out


def test_the_error_path_still_speaks_python(inv_pair, tmp_path):
    """psql's stderr is bytes now too, and a reader that raised the repr of
    a bytes object would make every failure harder to read than it was."""
    eng = _engine(inv_pair, tmp_path)
    with pytest.raises(RuntimeError) as e:
        eng.fetch_sample_df("src", "postgres", "public.no_such_table", 10)
    assert "b'" not in str(e.value), str(e.value)
    assert "does not exist" in str(e.value), str(e.value)


def test_mysql_reads_its_sample_through_a_driver(tmp_path):
    """Recorded rather than assumed: the same logical step on the other
    engine never went through a text decoder, so it never had this. If it
    is ever rewritten to shell out, this test is the reminder."""
    import inspect

    from migkit.engines.mysql import MySQLEngine
    src = inspect.getsource(MySQLEngine.fetch_sample_df)
    assert "text=True" not in src, src
    assert "self._q(" in src, src


def test_every_invisible_difference_is_named_not_just_counted(inv_pair,
                                                                tmp_path,
                                                                monkeypatch):
    """The count was never the hard part. Each pair gets its bytes and the
    reason they differ."""
    _seed(inv_pair, PAIRS)
    out = _cli(inv_pair, tmp_path, monkeypatch,
               ["check", "inv", "--db", "postgres", "--table", "public.t",
                "--drill"])
    section = out.split("Differences You Cannot See", 1)
    assert len(section) == 2, out
    seen = section[1]
    for escaped in ("'caf\\xe9'", "'cafe\\u0301'", "'hello '", "'a\\u200bb'",
                    "'a\\xa0b'", "'one\\r\\ntwo'"):
        assert escaped in seen, (escaped, seen)
    for reason in ("NFC vs NFD", "trailing whitespace", "U+200B", "U+00A0",
                   "carriage return"):
        assert reason in seen, (reason, seen)


def test_a_difference_anybody_can_see_stays_out(inv_pair, tmp_path,
                                                  monkeypatch):
    """`red` against `blue` needs no explanation. A section that repeated
    every differing row would bury the rows that do."""
    _seed(inv_pair, PAIRS)
    out = _cli(inv_pair, tmp_path, monkeypatch,
               ["check", "inv", "--db", "postgres", "--table", "public.t",
                "--drill"])
    seen = out.split("Differences You Cannot See", 1)[1]
    assert "blue" not in seen, seen
    assert "'red'" not in seen, seen
    # the sample datacompy prints still carries it, so nothing was hidden
    assert "blue" in out, out


def test_a_matching_pair_prints_no_section(inv_pair, tmp_path, monkeypatch):
    """The cry-wolf guard: a heading with nothing under it teaches people to
    scroll past the heading."""
    _seed(inv_pair, [(i, s, s) for i, s, _ in PAIRS])
    out = _cli(inv_pair, tmp_path, monkeypatch,
               ["check", "inv", "--db", "postgres", "--table", "public.t",
                "--drill"])
    assert "Differences You Cannot See" not in out, out


def test_both_engines_read_one_implementation(tmp_path):
    """It lives on the base contract, so MySQL gets it without a line of
    its own - asserted rather than assumed, because 'it should inherit' is
    how two copies start."""
    import pandas as pd

    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    src = pd.DataFrame({"id": [1], "v": [PAIRS[3][1]]})
    dst = pd.DataFrame({"id": [1], "v": [PAIRS[3][2]]})
    pg = PostgresEngine(hop)._invisible_section(src, dst, ["id"])
    my = MySQLEngine(hop)._invisible_section(src, dst, ["id"])
    assert "U+00A0" in pg, pg
    assert pg == my, (pg, my)


def test_the_reasons_need_no_server(tmp_path):
    from migkit.engines.base import Engine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Engine(hop)

    # written as escapes, not as literal characters: a heredoc or an
    # editor that normalises the file turns the NFD case into the NFC one
    # and the test then passes for the wrong reason
    named = {
        ("caf\u00e9", "cafe\u0301"): "NFC vs NFD",
        ("hello", "hello "): "leading or trailing whitespace",
        ("ab", "a\u200bb"): "U+200B",
        ("a b", "a\u00a0b"): "U+00A0",
        ("one\ntwo", "one\r\ntwo"): "carriage return on the target",
        ("a b", "a  b"): "runs of spaces",
    }
    for (a, b), want in named.items():
        why = eng._invisible_difference(a, b)
        assert len(why) == 1, (a, b, why)
        assert want in why[0], (a, b, why)

    # nothing to explain
    for a, b in (("red", "blue"), ("same", "same"), ("x", None), (None, 1)):
        assert eng._invisible_difference(a, b) == [], (a, b)

    # the carriage return is attributed to the side that carries it
    assert "on the source" in eng._invisible_difference("one\r\ntwo",
                                                        "one\ntwo")[0]

    # a lone carriage return between two letters is left out on purpose:
    # it returns the cursor rather than printing, so `a\rb` and `ab` really
    # do render differently and belong in the visible sample above
    assert eng._invisible_difference("a\rb", "ab") == []


def test_the_section_stops_and_says_it_stopped(tmp_path):
    """A list longer than the sample it explains is not an explanation."""
    import pandas as pd

    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    n = eng.INVISIBLE_CAP + 5
    src = pd.DataFrame({"id": list(range(n)), "v": ["ab"] * n})
    dst = pd.DataFrame({"id": list(range(n)), "v": ["a​b"] * n})
    got = eng._invisible_section(src, dst, ["id"])
    assert got.count("source  'ab'") == eng.INVISIBLE_CAP, got
    assert "... 5 more not shown" in got, got
