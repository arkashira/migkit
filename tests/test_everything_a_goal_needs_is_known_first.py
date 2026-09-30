"""Right the first time, not a runbook to recover with (backlog R23, 6).

Everything a hop's goal needs is known before anything is written, in one
place (`ready`), each with its fix; a move does not start where something
known now for sure would stop it half way, and says all of them at once;
what a person must still do is said with the moment it is needed.

The pure parts hold the one place to its sources through stand-ins; the
last part runs PostgreSQL: a table the hop's user may not read, and a
target that already holds rows, each refused before a row is written.
"""
import json
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

SRC, DST = ("migkit-test-ready-src", 16336), ("migkit-test-ready-dst", 16337)


def _hop(goal="copy", options=None, name="h"):
    from migkit.config import Endpoint, Hop
    return Hop(name=name, engine="postgres", source=Endpoint(),
               target=Endpoint(), databases=["app"],
               options=dict(options or {}), goal=goal)


class _Eng:
    def databases(self):
        return ["app"]

    def _mapping_items(self):
        return []


def _only(monkeypatch, **parts):
    """`ready`'s parts replaced by what each returns here."""
    from migkit import ready
    for name in ("_servers", "_target", "_resources", "_goal_checks",
                 "_rehearsed_quick", "_mapping"):
        got = parts.get(name, [])
        monkeypatch.setattr(ready, name, lambda *a, got=got, **k: list(got))


def test_the_gate_says_every_sure_stop_at_once_with_its_fix(monkeypatch):
    from migkit import ready
    from migkit.ready import BEFORE, Item
    _only(monkeypatch,
          _servers=[Item("fail", BEFORE, "source: read every table of app",
                         "2 tables and sequences it may not read",
                         "grant select on public.secret to app",
                         by_hand=True, gates=True),
                    Item("warn", BEFORE, "target: create what the source"
                         " has in app", "may not create schemas in it",
                         "grant create on database app to app")],
          _resources=[Item("fail", BEFORE, "app: room on the target",
                           "1 GB free for 3 GB of tables alone", "a larger"
                           " volume", gates=True)])
    said = []
    with pytest.raises(SystemExit) as e:
        ready.gate(_hop(), _Eng(), None, "full", False, said.append)
    text = str(e.value)
    assert "nothing was written - 2 things known now would stop this" \
        in text, text
    assert "grant select on public.secret to app" in text, text
    assert "1 GB free for 3 GB of tables alone" in text, text
    # what is only likely does not stop it
    assert "create schemas" not in text, text
    # and it can be told to go on, every one of them said
    monkeypatch.setenv("MIGKIT_PREFLIGHT", "warn")
    got = ready.gate(_hop(), _Eng(), None, "full", False, said.append)
    assert len(got) == 2 and len(said) == 2, said


def test_a_stream_alone_is_held_only_to_what_the_source_must_give(
        monkeypatch):
    from migkit import ready
    from migkit.ready import BEFORE, Item
    _only(monkeypatch,
          _servers=[Item("fail", BEFORE, "source: stream the changes",
                         "wal_level is replica", gates=True)],
          _target=[Item("fail", BEFORE, "app: the target", "rows held",
                        gates=True)])
    got = ready.stops(_hop(), _Eng(), None, "cdc", False)
    assert [i.what for i in got] == ["source: stream the changes"]
    # the cutover has a preflight of its own
    assert ready.stops(_hop(), _Eng(), None, "cdc", True) == []


def test_what_the_servers_lack_is_sorted_into_sure_and_likely(monkeypatch,
                                                              tmp_path):
    from migkit import dba, ready
    from migkit.dba import Need
    found = [Need("source", "read every table of app", False,
                  "1 tables and sequences it may not read",
                  ["-- in database app:",
                   "grant select on public.secret to app;"]),
             Need("target", "create what the source has in app", False,
                  "may not create schemas in it",
                  ["grant create on database app to app;"]),
             Need("target", "a database app to write into", False,
                  "it is not there", ["create database app owner app;"]),
             Need("source", "logical decoding", False, "wal_level is"
                  " replica", ["alter system set wal_level = logical;"],
                  restart=True, follow=True),
             Need("target", "the extensions app uses", None, "timed out"),
             Need("target", "read every table of app", True, "every one")]
    monkeypatch.setattr(dba, "needs", lambda *a, **k: found)
    monkeypatch.setattr(dba, "write",
                        lambda *a, **k: {"source": tmp_path / "s.sql"})
    got = {i.what: i for i in ready._servers(_hop("cutover"), _Eng(),
                                             "full+cdc")}
    assert got["source: read every table of app"].gates
    assert "grant select on public.secret to app;" in \
        got["source: read every table of app"].fix
    assert "s.sql" in got["source: read every table of app"].fix
    assert got["target: a database app to write into"].gates
    assert got["source: logical decoding"].gates
    assert "takes a restart" in got["source: logical decoding"].detail
    # the target may hold its schemas already: likely, said, not held to
    assert not got["target: create what the source has in app"].gates
    assert got["target: create what the source has in app"].level == "warn"
    # asked and not answered: unknown, not clean
    assert got["target: the extensions app uses"].level == "warn"
    assert "target: read every table of app" not in got
    # a hop that only verifies asks nothing of what writes
    assert ready._servers(_hop("verify"), _Eng(), None) == []


def test_what_is_left_to_a_person_is_said_with_its_moment():
    from migkit import ready
    got = ready.by_hand(_hop("cutover"))
    assert len(got) == 1 and got[0].when == ready.CUTOVER
    assert "the operator stops the application's writes" in got[0].detail
    got = ready.by_hand(_hop("cutover", {"cutover": {"budget": 30,
                                                     "freeze": "operator"}}))
    said = " ".join(i.detail for i in got)
    assert "the operator stops the source's writes" in said, said
    assert "pointed at the target by hand" in said, said
    got = ready.by_hand(_hop("cutover", {"cutover": {"budget": 30,
                                                     "flip": "true"}}))
    assert got == []
    assert ready.by_hand(_hop("copy")) == []


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    (tmp_path / "reports").mkdir()
    monkeypatch.setattr(cfg, "load_hops", lambda path=None: {})
    return tmp_path / "reports"


def test_the_rehearsal_is_held_to_how_it_ended(reports, monkeypatch):
    from migkit import ready, rehearsal
    real = _hop("cutover", {"cutover": {"budget": 5}}, name="orders")
    got = ready._rehearsed(real, _Eng(), ["app"])
    assert got[0].level == "warn" and "not been rehearsed" in got[0].detail
    assert "rehearses: orders" in got[0].fix
    uat = _hop("rehearse", {"rehearses": "orders",
                            "cutover": {"budget": 5}}, name="orders-uat")
    rehearsal.mark(uat)
    monkeypatch.setattr(rehearsal, "forecast_lines", lambda *a: [])
    (uat.report_dir() / "verdict.json").write_text(
        json.dumps({"status": "different"}))
    got = ready._rehearsed(real, _Eng(), ["app"])
    stops = [i for i in got if i.gates]
    assert stops and "ended with the two sides not equal" in \
        stops[0].detail, [str(i) for i in got]
    (uat.report_dir() / "verdict.json").write_text(
        json.dumps({"status": "same"}))
    (uat.report_dir() / "cutover-runs.jsonl").write_text(
        json.dumps({"state": "flipped", "outage": 9.5}) + "\n")
    got = ready._rehearsed(real, _Eng(), ["app"])
    assert [i.level for i in got if i.what == "rehearsal"
            and i.when == ready.BEFORE][0] == "pass"
    over = [i for i in got if i.gates]
    assert over and "took 9.50s of outage" in over[0].detail, \
        [str(i) for i in got]
    # the quick part a gate reads says the same
    assert [i.detail for i in ready._rehearsed_quick(real)
            if i.gates][0].startswith("the rehearsal's cutover took 9.50s")


def test_the_interlocks_refusals_are_known_before_the_write(monkeypatch):
    from migkit import interlock
    same = {"engine": "postgres", "address": "10.0.0.5:5432",
            "database": "app"}
    monkeypatch.setattr(interlock, "identity",
                        lambda hop, eng, side, db: dict(same))
    got = interlock.refusals(_hop(), _Eng(), "app")
    assert got and got[0].startswith("the target is the source itself"), got
    other = dict(same, address="10.0.0.6:5432", name="db-prod-1")
    monkeypatch.setattr(interlock, "identity",
                        lambda hop, eng, side, db: dict(
                            same if side == "src" else other))
    assert interlock.refusals(_hop(), _Eng(), "app") == []
    hop = _hop(options={"production": ["db-prod*"]})
    got = interlock.refusals(hop, _Eng(), "app")
    assert got and "looks like production" in got[0], got


# --- PostgreSQL: refused before a row is written ------------------------------

def _sql(name, sql, db="app", user="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", user, "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16"],
                           check=True, capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"], capture_output=True
                                  ).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
            _sql(name, "create database app", "postgres")
        _sql(SRC[0], "create table t (id int primary key, v text);"
                     " insert into t select g, 'v' || g from"
                     " generate_series(1, 1000) g;"
                     " create table secret (id int primary key);"
                     " create role app login password 'test';"
                     " grant select on t to app")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def conf(pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    path.write_text(
        "hops:\n"
        "  app:\n    engine: postgres\n    goal: cutover\n"
        f"    source: {{host: 127.0.0.1, port: {SRC[1]}, user: app,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST[1]}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n"
        "  full:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {SRC[1]}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST[1]}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    return CliRunner().invoke(cli.main, list(argv))


@needs_docker
def test_a_table_the_user_may_not_read_stops_the_move_first(conf):
    got = _run("move", "app", "--mode", "full", "--go")
    assert got.exit_code != 0
    out = " ".join(got.output.split())
    assert "nothing was written" in out, out
    assert "source: read every table of app" in out, out
    assert "grant select on public.secret to app" in out, out
    # nothing reached the target: not a table made
    assert _sql(DST[0], "select count(*) from pg_tables where schemaname ="
                        " 'public'") == "0"
    # the plan without --go says the same, where it would stop
    plan = " ".join(_run("move", "app", "--mode", "full").output.split())
    assert "with --go this stops: source: read every table of app" in plan


@needs_docker
def test_rows_on_the_target_stop_the_move_before_it_starts(conf):
    _sql(DST[0], "create table t (id int primary key, v text);"
                 " insert into t values (1, 'theirs')")
    try:
        got = _run("move", "full", "--mode", "full", "--go")
        assert got.exit_code != 0
        out = " ".join(got.output.split())
        assert "already hold rows on the target: public.t" in out, out
        assert "target_rows: replace or keep" in out, out
        assert _sql(DST[0], "select v from t") == "theirs"
    finally:
        _sql(DST[0], "drop table t")


@needs_docker
def test_assess_says_it_all_and_what_is_done_by_hand(conf):
    got = _run("assess", "app")
    out = " ".join(got.output.split())
    # the servers' own rows, what the user lacks with the statement, the
    # rehearsal missing, and the cutover's steps left to a person
    assert "server version match" in out, out
    assert "grant select on public.secret to app" in out, out
    assert "this path has not been rehearsed" in out, out
    assert "by hand:" in out, out
    assert "the operator stops the application's writes" in out, out
    assert got.exit_code != 0
