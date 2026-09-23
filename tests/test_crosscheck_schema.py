"""A second reading of the schema, used only in the direction it carries.

`pgcopydb compare data` earned its place as an auditor of migkit's own data
verdict because it answers the same question by a different route. The
schema half is not that. Measured on pgcopydb 0.18 by making one difference
at a time:

    target missing a column       differ  (names the column)
    target missing an index       differ
    target missing a table        differ
    varchar(50) -> varchar(200)   **successful** - missed

and migkit on the same four:

    identical                     OK
    varchar(50) -> varchar(200)   DIFF  (2 changed lines; liquibase:
                                         Changed Column(s))
    index missing                 DIFF  (objects: index 3/2 missing)

So `compare schema` is a **narrower** check, not an independent one. It
compares tables, columns and indexes by name; it does not compare column
types. migkit reports the widened column deliberately - `neutral_columns`
reads `format_type` for exactly that reason, because a target built wider
than its source loses a limit the application relied on without losing a
row to show for it.

That asymmetry decides how it is wrapped. Reporting "pgcopydb says same,
migkit says differs" as a clash would cry wolf on every widened column and
teach the operator to skip the line. The other direction is the finding:
**pgcopydb naming a difference that migkit passed over means migkit missed
something.** Only that one is reported as a difference.

Behind `MIGKIT_CROSSCHECK` with the data half, because it reads both
catalogues a second time.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = 15595, 15596
NS, ND = "migkit-test-cschema-src", "migkit-test-cschema-dst"

CLEAN = ("INFO   compare.c:593  [SOURCE] table: 2, index: 3\n"
         "INFO   compare.c:631  pgcopydb schema inspection is successful\n")
DIFFERS = (
    "INFO   compare.c:593  [SOURCE] table: 2, index: 3\n"
    "INFO   compare.c:695  Table public.a has 3 columns on source,"
    " 2 columns on target\n"
    "INFO   compare.c:736  Table public.a column \"n\" exists on source but"
    " not on target\n"
    "FATAL  compare.c:627  Schemas on source and target database differ\n")


def _engine(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="cs", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST, user="postgres",
                              password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


def test_a_clean_reading_is_read_as_clean(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    assert PostgresEngine._schema_verdict(CLEAN, 0) == (False, [])


def test_a_difference_is_read_with_what_it_named(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    differs, found = PostgresEngine._schema_verdict(DIFFERS, 12)
    assert differs is True
    assert any("column \"n\" exists on source" in f for f in found), found
    assert not any("Schemas on source and target" in f for f in found), found


def test_an_answer_it_did_not_give_is_not_an_agreement(tmp_path):
    """A run that died before comparing anything must not read as "same" -
    that is the false negative this whole file is a guard against."""
    from migkit.engines.postgres import PostgresEngine
    for junk in ("", "FATAL could not connect to source", "Running pgcopydb"):
        assert PostgresEngine._schema_verdict(junk, 1)[0] is None, junk


def test_pgcopydb_finding_what_migkit_passed_is_the_one_difference(tmp_path):
    got = _engine(tmp_path)._crosscheck_schema_result(
        "postgres", True, ["Failed to find table public.b in target"], True)
    assert got.status == "diff", got.detail
    assert "did not report" in got.detail, got.detail
    assert "public.b" in got.detail, got.detail


def test_migkit_finding_what_pgcopydb_does_not_look_for_is_not(tmp_path):
    """The case that would otherwise fire on every widened column."""
    got = _engine(tmp_path)._crosscheck_schema_result(
        "postgres", False, [], False)
    assert got.status == "ok", got.detail
    assert "does not compare column types" in got.detail, got.detail
    assert "Not a disagreement" in got.detail, got.detail


def test_agreeing_that_they_differ_is_not_a_finding(tmp_path):
    got = _engine(tmp_path)._crosscheck_schema_result("postgres", True,
                                                      ["x"], False)
    assert got.status == "ok", got.detail
    assert "agrees the schemas differ" in got.detail, got.detail


def test_agreeing_that_they_match_is_not_a_finding(tmp_path):
    got = _engine(tmp_path)._crosscheck_schema_result("postgres", False, [],
                                                      True)
    assert got.status == "ok", got.detail
    assert "both readings see the schemas as matching" in got.detail, got.detail


def test_being_unable_to_ask_is_a_skip_not_a_pass(tmp_path):
    got = _engine(tmp_path)._crosscheck_schema_result("postgres", None, [],
                                                      True)
    assert got.status == "skip", got.detail
    assert got.status != "ok"


def test_without_migkits_own_verdict_there_is_nothing_to_second_guess(
        tmp_path):
    got = _engine(tmp_path)._crosscheck_schema_result("postgres", True, ["x"],
                                                      None)
    assert got.status == "skip", got.detail
    assert "schema check has not run" in got.detail, got.detail


def test_the_verdict_is_read_from_the_file_the_schema_check_wrote(tmp_path):
    eng = _engine(tmp_path)
    assert eng._schema_from_evidence("postgres") is None
    (tmp_path / "schema-evidence.txt").write_text(
        "postgres: OK\npostgres objects: OK\n")
    assert eng._schema_from_evidence("postgres") is True
    (tmp_path / "schema-evidence.txt").write_text(
        "postgres: OK\npostgres objects: DIFF\n")
    assert eng._schema_from_evidence("postgres") is False, \
        "one DIFF among many is still a difference"
    (tmp_path / "schema-evidence.txt").write_text("\n \n")
    assert eng._schema_from_evidence("postgres") is None


def test_it_is_off_unless_asked_for(tmp_path, monkeypatch):
    monkeypatch.delenv("MIGKIT_CROSSCHECK", raising=False)
    assert _engine(tmp_path)._crosscheck_schema("postgres") is None
    for value in ("0", "", "no"):
        monkeypatch.setenv("MIGKIT_CROSSCHECK", value)
        assert _engine(tmp_path)._crosscheck_schema("postgres") is None


def _sql(name, s):
    p = subprocess.run(["docker", "exec", name, "psql", "-U", "postgres",
                        "-q", "-v", "ON_ERROR_STOP=1", "-c", s],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr


DDL = ("create table a (id int primary key, v varchar(50),"
       " n numeric(10,2)); create index a_v on a(v);"
       " create table b (id int primary key)")


@pytest.fixture(scope="module")
def pair():
    for n in (NS, ND):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    for n, p in ((NS, SRC), (ND, DST)):
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16"], check=True, capture_output=True)
    try:
        for n, p in ((NS, SRC), (ND, DST)):
            end = time.time() + 180
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(1)
            for _ in range(90):
                if subprocess.run(["docker", "exec", n, "pg_isready", "-U",
                                   "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
            else:
                pytest.fail(f"{n} never answered")
            _sql(n, DDL)
        yield
    finally:
        for n in (NS, ND):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


needs_pgcopydb = pytest.mark.skipif(
    subprocess.run(["which", "pgcopydb"], capture_output=True).returncode != 0,
    reason="pgcopydb binary not installed")


@needs_docker
@needs_pgcopydb
def test_the_narrowness_this_rests_on_is_real(pair, tmp_path, monkeypatch):
    """Asserted against pgcopydb rather than quoted from its docs: it does
    not see a widened column, and it does see a missing index. If a later
    version starts comparing types, this test says so and the wording
    above has to change."""
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setenv("MIGKIT_CROSSCHECK", "1")
    eng = _engine(tmp_path)

    _sql(ND, "alter table a alter column v type varchar(200)")
    try:
        eng.check_schema("postgres")
        assert eng._schema_from_evidence("postgres") is False, \
            "migkit sees the widened column"
        got = eng._crosscheck_schema("postgres")
        assert got.status == "ok", got.detail
        assert "does not compare column types" in got.detail, got.detail
    finally:
        _sql(ND, "alter table a alter column v type varchar(50)")

    _sql(ND, "drop index a_v")
    try:
        eng.check_schema("postgres")
        got = eng._crosscheck_schema("postgres")
        assert got.status == "ok", got.detail
        assert "agrees the schemas differ" in got.detail, got.detail
    finally:
        _sql(ND, "create index a_v on a(v)")


@needs_docker
@needs_pgcopydb
def test_two_matching_schemas_read_as_matching(pair, tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_CROSSCHECK", "1")
    eng = _engine(tmp_path)
    eng.check_schema("postgres")
    got = eng._crosscheck_schema("postgres")
    assert got.status == "ok", got.detail
    assert "both readings see the schemas as matching" in got.detail, got.detail


@needs_docker
@needs_pgcopydb
def test_the_reported_direction_is_reachable_on_a_real_pair(pair, tmp_path,
                                                            monkeypatch):
    """The branch that exists to catch migkit being wrong. Staged by
    handing the check a clean migkit verdict while the two databases really
    do differ - a check whose only firing path cannot be exercised is
    decoration.
    """
    monkeypatch.setenv("MIGKIT_CROSSCHECK", "1")
    eng = _engine(tmp_path)
    _sql(ND, "drop table b")
    try:
        (tmp_path / "schema-evidence.txt").write_text("postgres: OK\n")
        got = eng._crosscheck_schema("postgres")
        assert got.status == "diff", got.detail
        assert "public.b" in got.detail, got.detail
        assert "did not report" in got.detail, got.detail
    finally:
        _sql(ND, "create table b (id int primary key)")


@needs_docker
@needs_pgcopydb
def test_the_schema_check_leaves_the_evidence_behind(pair, tmp_path):
    """Without the file there is nothing to second-guess, so this is what
    makes the whole check reachable in a normal run."""
    eng = _engine(tmp_path)
    res = eng.check_schema("postgres")
    ev = tmp_path / "schema-evidence.txt"
    assert ev.exists()
    lines = [ln for ln in ev.read_text().splitlines() if ln.strip()]
    assert len(lines) == len(res), (lines, [r.scope for r in res])
    assert all(ln.rsplit(":", 1)[-1].strip() in ("OK", "DIFF", "SKIP", "ERROR")
               for ln in lines), lines
