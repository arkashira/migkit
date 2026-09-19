"""The sort order an index was built under, and what happens when it moves.

A text index is a list sorted by rules the C library owns, not the database.
glibc 2.28 rewrote those rules, every distribution crossed that line, and an
index built before the change is still sorted - sorted wrong. Lookups walk
past the row they wanted; a unique index stops catching duplicates, because
the duplicate lands somewhere the search never goes.

PostgreSQL records the version each object was built under, which is the only
reason this is findable. Faking that record reproduces the real thing exactly
- the server prints its own warning on every connection afterwards:

    WARNING:  database "postgres" has a collation version mismatch
    DETAIL:  The database was created using collation version 2.17, but the
             operating system provides version 2.41.

Two things were measured before this check was written, and both shaped it:

* **`ALTER DATABASE ... REFRESH COLLATION VERSION` silences the warning
  instantly and rebuilds nothing.** It is the first thing the hint text
  everyone copies tells you to run. So migkit's hint puts REINDEX first.
* **After a real library upgrade every collation the OS ships has drifted** -
  873 of them on this image. Reporting all of them would be a wall rather
  than a finding, so only collations a user column or index actually
  references are reported. Dropping the table that used one was measured to
  remove it from the result.

These tests run on their own pair of containers rather than the shared
fixture, because they vandalise `pg_collation` and `pg_database` on purpose
and a leaked fake would make every later connection in the session noisy.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15522, 15523
NAMES = {SRC: "migkit-test-coll-src", DST: "migkit-test-coll-dst"}


def _up(name, port):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                    "postgres:16"], check=True, capture_output=True)


@pytest.fixture(scope="module")
def coll_pair():
    for port, name in NAMES.items():
        _up(name, port)
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
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def q(port, sql):
    got = subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-At", "-q",
         "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(autouse=True)
def undo_the_vandalism(coll_pair):
    """Put both catalogs back, so one test's fake cannot decide another's
    verdict."""
    yield
    for port in NAMES:
        q(port, "drop table if exists public.t; drop table if exists"
                " public.u;")
        q(port, "update pg_collation set collversion ="
                " pg_collation_actual_version(oid) where collversion is"
                " distinct from pg_collation_actual_version(oid)")
        q(port, "alter database postgres refresh collation version")


def _engine(coll_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="coll", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=coll_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=coll_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _versions(engine):
    return engine._collation_versions("postgres")


def _use_collation(port, collation='"en_US.utf8"'):
    q(port, f"create table public.t (id int, v text collate {collation});")
    q(port, "create index t_v on public.t (v);")


def _fake(port, collname, version="2.17"):
    q(port, f"update pg_collation set collversion = '{version}' where"
            f" collname = '{collname}'")


def test_a_collation_that_changed_under_its_index_is_named(coll_pair,
                                                            tmp_path):
    _use_collation(coll_pair["dst"])
    _fake(coll_pair["dst"], "en_US.utf8")
    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "target en_US.utf8 built under 2.17, now" in got.detail, got.detail
    assert "stop catching duplicates" in got.detail, got.detail
    assert "REINDEX" in got.fix_hint, got.fix_hint
    # the trap: the hint everyone copies from the server silences the alarm
    assert got.fix_hint.index("REINDEX") < got.fix_hint.index("REFRESH"), \
        got.fix_hint


def test_a_drifted_collation_nothing_uses_is_not_reported(coll_pair,
                                                           tmp_path):
    """The difference between a finding and a wall. After a real glibc
    upgrade all 873 collations the image ships have drifted at once; only
    the ones something is sorted by can have broken anything."""
    _fake(coll_pair["dst"], "en_US.utf8")
    assert q(coll_pair["dst"], "select collversion from pg_collation where"
                               " collname = 'en_US.utf8'") == "2.17"
    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "ok", got.detail

    # and the moment a column uses it, the same drift is a finding
    _use_collation(coll_pair["dst"])
    after = _versions(_engine(coll_pair, tmp_path))
    assert after.status == "diff", after.detail
    assert "en_US.utf8" in after.detail


def test_an_index_alone_is_enough_to_count_as_in_use(coll_pair, tmp_path):
    """A collation can reach an index without any column declaring it."""
    q(coll_pair["dst"], "create table public.t (id int, v text);")
    q(coll_pair["dst"], 'create index t_v on public.t (v collate "en_US.utf8");')
    _fake(coll_pair["dst"], "en_US.utf8")
    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "en_US.utf8" in got.detail, got.detail


def test_the_database_default_is_checked_without_any_column_saying_so(
        coll_pair, tmp_path):
    """The one PostgreSQL warns about itself, and the one that matters most:
    every text column with no explicit COLLATE is sorted by it."""
    q(coll_pair["src"], "update pg_database set datcollversion = '2.17'"
                        " where datname = current_database()")
    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "source postgres built under 2.17, now" in got.detail, got.detail

    # the server agrees - this is the real state, not a shape migkit invented
    warned = subprocess.run(
        ["docker", "exec", NAMES[coll_pair["src"]], "psql", "-U", "postgres",
         "-At", "-c", "select 1"], capture_output=True, text=True)
    assert "collation version mismatch" in warned.stderr, warned.stderr


def test_a_collation_the_os_gives_no_version_for_outranks_drift(coll_pair,
                                                                 tmp_path):
    """Worse than drift: the rules are not merely different, they are gone.
    Rebuilding cannot help until the locale is installed, so this has its
    own verdict rather than being folded into the others."""
    q(coll_pair["dst"], 'create table public.u (id int, v text collate "C");')
    _fake(coll_pair["dst"], "C")
    assert q(coll_pair["dst"], "select coalesce(pg_collation_actual_version("
                               "oid), '') from pg_collation where collname ="
                               " 'C'") == ""

    # drift as well, so the ordering between the two verdicts is exercised
    _use_collation(coll_pair["src"])
    _fake(coll_pair["src"], "en_US.utf8")

    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "no longer provides" in got.detail, got.detail
    assert "target C built under 2.17" in got.detail, got.detail
    assert "install the missing locale" in got.fix_hint, got.fix_hint


def test_a_clean_pair_says_what_it_checked(coll_pair, tmp_path):
    _use_collation(coll_pair["src"])
    _use_collation(coll_pair["dst"])
    got = _versions(_engine(coll_pair, tmp_path))
    assert got.status == "ok", got.detail
    # en_US.utf8 in use on each side, plus each database's own default
    assert "4 versioned collations" in got.detail, got.detail


def test_the_full_deep_report_carries_it(coll_pair, tmp_path):
    _use_collation(coll_pair["dst"])
    _fake(coll_pair["dst"], "en_US.utf8")
    got = [r for r in _engine(coll_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("collation versions")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    drift = eng._collation_version_result(
        "x", [("target", "en_US.utf8", "2.17", "2.36")], [], 3, "hint")
    assert drift.status == "diff"
    assert "built under 2.17, now 2.36" in drift.detail

    gone = eng._collation_version_result(
        "x", [("source", "en_US.utf8", "2.17", "2.36")],
        [("target", "fr_FR.utf8", "2.28")], 3, "hint")
    assert gone.status == "diff"
    assert "no longer provides" in gone.detail, gone.detail
    # the harder failure wins the verdict rather than being averaged in
    assert "en_US" not in gone.detail, gone.detail

    fine = eng._collation_version_result("x", [], [], 3, "hint")
    assert fine.status == "ok" and "3 versioned collations" in fine.detail

    # nothing versioned is not the same as nothing wrong
    nothing = eng._collation_version_result("x", [], [], 0, "hint")
    assert nothing.status == "skip", nothing.detail


def test_mysql_has_no_version_to_drift(tmp_path):
    """Measured on MySQL 8.4 rather than assumed: every one of the 286 rows
    in information_schema.collations reports IS_COMPILED=Yes and the table
    carries no version column at all, so an OS upgrade cannot re-sort an
    index underneath it. Answering `ok` would be claiming a check that was
    never run; answering `skip` with the reason is the honest shape."""
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="c", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._collation_versions("x")
    assert got.status == "skip", got.detail
    assert "IS_COMPILED" in got.detail, got.detail
    assert "286" in got.detail, got.detail
