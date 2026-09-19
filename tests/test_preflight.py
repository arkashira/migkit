"""The answer that used to arrive only after the move.

migkit could already tell you that a target column was too narrow for the
rows about to be sent, and that a `timestamptz` was landing in a
`timestamp`. Both facts lived in `check --deep` - the command you run
*afterwards*. Measured on a pair with both problems:

    assess     13 pass, 5 warn, 2 fail     (neither mentioned)
    check --deep
      postgres target capacity: DIFF 1 columns hold values the target has
        no room for
      postgres temporal meaning: DIFF 1 columns change what they mean

The checks existed. The operator could only reach them once it was too late
to matter. `assess` now runs the predictive ones too:

    fail  before the move  postgres target capacity  1 columns hold values...
    fail  before the move  postgres temporal meaning  1 columns change what...
    16 pass, 5 warn, 4 fail

One implementation, two places to read it - not a second copy of the logic.

**What is deliberately left out** is as important: the checks that compare
what is *on* the target. Large objects, extension data, duplicate keys and
counts all report a difference against an empty target, which is the normal
state before a move. Including them would put a wall of red in front of
every migration and teach people to skim the section.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-pf-src", DST: "migkit-test-pf-dst"}


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-c", sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def pf_pair():
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


def _shape(pf_pair, target_note="varchar(50)", target_at="timestamp"):
    for port in NAMES:
        q(port, "drop table if exists people;")
    q(pf_pair["src"], "create table people (id int primary key,"
                      " note varchar(255), at timestamptz);"
                      " insert into people select g, repeat('x',120), now()"
                      " from generate_series(1,50) g;")
    q(pf_pair["dst"], "create table people (id int primary key,"
                      f" note {target_note}, at {target_at});")


def _engine(pf_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="pf", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pf_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pf_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _preflight(items):
    return [i for i in items if i["scope"] == "before the move"]


def test_it_names_what_will_stop_the_move(pf_pair, tmp_path):
    _shape(pf_pair)
    rows = _preflight(_engine(pf_pair, tmp_path).assess())
    failed = {i["item"]: i for i in rows if i["level"] == "fail"}
    assert "postgres target capacity" in failed, rows
    assert "postgres temporal meaning" in failed, rows
    assert "no room for" in failed["postgres target capacity"]["detail"]
    # the fix travels with the finding rather than living in another command
    assert "[fix]" in failed["postgres target capacity"]["detail"]


def test_a_clean_pair_adds_no_failures(pf_pair, tmp_path):
    """The cry-wolf guard. A pre-flight that fails on a healthy pair is a
    pre-flight people learn to skip."""
    _shape(pf_pair, target_note="varchar(255)", target_at="timestamptz")
    rows = _preflight(_engine(pf_pair, tmp_path).assess())
    assert rows, "the preflight section should still be present"
    assert [i for i in rows if i["level"] != "pass"] == [], rows


def test_the_retrospective_checks_stay_out(pf_pair, tmp_path):
    """Large objects, extension data and duplicate keys all compare what is
    *on* the target, which before a move is nothing. They would fail every
    pre-flight ever run."""
    from migkit.engines.postgres import PostgresEngine
    for name in ("_large_objects", "_extension_data", "_duplicate_keys"):
        assert hasattr(PostgresEngine, name), name
        assert name not in PostgresEngine.PREFLIGHT, name

    _shape(pf_pair)
    rows = _preflight(_engine(pf_pair, tmp_path).assess())
    said = " ".join(i["item"] for i in rows)
    for absent in ("large objects", "extension data", "duplicate keys"):
        assert absent not in said, (absent, said)


def test_a_check_that_cannot_run_is_a_warning_not_a_pass(pf_pair, tmp_path):
    """`unknown` is not `clean` - a pre-flight row that silently passed
    because the query blew up would be the worst kind of reassurance."""
    _shape(pf_pair)
    eng = _engine(pf_pair, tmp_path)

    def boom(db):
        raise RuntimeError("the server said no")

    eng._capacity_gaps = boom
    rows = _preflight(eng.assess())
    hit = [i for i in rows if "capacity" in i["item"]]
    assert len(hit) == 1, rows
    assert hit[0]["level"] == "warn", hit
    assert "could not run" in hit[0]["detail"], hit


def test_deep_still_says_the_same_thing(pf_pair, tmp_path):
    """One implementation read from two places. If these ever disagree, it
    means somebody made a second copy."""
    _shape(pf_pair)
    eng = _engine(pf_pair, tmp_path)
    deep = {r.scope: r for r in eng.check_deep("postgres")}
    rows = {i["item"]: i for i in _preflight(eng.assess())}
    for scope in ("postgres target capacity", "postgres temporal meaning"):
        assert deep[scope].status == "diff", deep[scope].detail
        assert rows[scope]["level"] == "fail", rows[scope]
        assert deep[scope].detail in rows[scope]["detail"], scope


def test_the_mapping_needs_no_server(tmp_path):
    from migkit.engines.base import Engine, Result
    hop = Hop(name="p", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Engine(hop)
    assert eng.PREFLIGHT == (), eng.PREFLIGHT
    assert eng._preflight_items() == []

    eng.PREFLIGHT = ("_fake",)
    eng.databases = lambda: ["x"]
    for status, level in (("diff", "fail"), ("warn", "warn"),
                          ("error", "warn"), ("ok", "pass"),
                          ("skip", "pass")):
        eng._fake = lambda db, s=status: Result("deep", "x thing", s, "why",
                                                "", "do this")
        got = eng._preflight_items()
        assert len(got) == 1, got
        assert got[0]["level"] == level, (status, got)
        assert got[0]["scope"] == "before the move"
        # the hint rides along only when there is something to fix
        assert ("do this" in got[0]["detail"]) is (status != "ok"), got
