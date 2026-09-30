"""A major-version upgrade as a goal (backlog R23, 7): what the new version
changes, checked first from both servers' own catalogues, and the way
chosen - a copy of the pages upgraded on the target, or a logical move
into a target already at the new version - never the source upgraded
where it runs.

The version rules, the ladder of ways and each check's reading run on
stand-ins here; the last part is PostgreSQL 16 against 17: a setting 17
no longer has, an extension 17 does not ship, a function an index runs
without a search path of its own.
"""
import json
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

SRC, DST = ("migkit-test-upgrade-src", 16338), ("migkit-test-upgrade-dst",
                                                16339)


def test_a_major_version_is_what_upgrades_are_told_apart_by():
    from migkit.upgrade import major
    assert major("16.4 (Debian 16.4-1.pgdg120+1)") == (16,)
    assert major("9.6.24") == (9, 6)
    assert major("8.0.36") == (8, 0) and major("8.4.2-log") == (8, 4)
    assert major("16.0.4135.4") == (16,)
    assert major("") is None and major(None) is None
    assert major("8.4.2") > major("8.0.36")
    assert major("17.0") > major("9.6.24")


def test_mysql_upgrades_in_place_a_series_at_a_time():
    from migkit.upgrade import _my_jump_ok
    assert _my_jump_ok((8, 0), (8, 4)) and _my_jump_ok((5, 7), (8, 0))
    assert _my_jump_ok((8, 4), (9, 1))
    assert not _my_jump_ok((5, 7), (8, 4))
    assert not _my_jump_ok((8, 0), (9, 0))


def test_what_each_version_took_away_is_counted_between_the_two():
    from migkit.upgrade import _removed_between
    got = _removed_between("postgres", (14,), (16,))
    assert "pg_start_backup" in got and "pg_switch_xlog" not in got
    assert "pg_switch_xlog" in _removed_between("postgres", (9, 6), (10,))
    assert _removed_between("postgres", (16,), (17,)) == []
    assert "password" in _removed_between("mysql", (5, 7), (8, 0))


def test_the_way_is_climbed_and_the_source_is_never_upgraded():
    from migkit import decide, upgrade

    def climb(seed, settings, versions):
        return decide.climb("the upgrade", upgrade.ways(), decide.Facts(
            {"seed": seed, "settings": settings, "versions": versions}))
    got = climb((None, ""), {}, ("16.4", "17.2"))
    assert got.rung.name == "logical", got
    assert "does not say how the target is upgraded" in got.reason
    got = climb((object(), "the pages"), {"bin": "/usr/lib/pg17"},
                ("16.4", "16.4"))
    assert got.rung.name == "seeded"
    # neither: the target at the old version, and nothing said of how to
    # upgrade it - and no rung upgrades the source itself
    got = climb((None, "the hop allows none"), {}, ("16.4", "16.4"))
    assert got.rung is None
    assert "a logical move goes into a server that already runs the new" \
        " version" in got.reason, got.reason
    assert {r.name for r in upgrade.ways()} == {"seeded", "logical"}


def test_a_copy_of_the_pages_for_an_upgrade_needs_its_upgrade_said():
    from migkit import decide, physical

    class F(dict):
        def __init__(self, goal, options):
            super().__init__(options=options)
            from migkit.config import Endpoint, Hop
            self.hop = Hop(name="u", engine="postgres", source=Endpoint(),
                           target=Endpoint(), goal=goal)
    rung = decide.Rung("p", "a copy", needs=(physical.UPGRADES,))
    kept, out = decide.prune([rung], F("upgrade", {"data": "/d"}))
    assert not kept and "the goal is an upgrade" in out[0][1], out
    kept, _ = decide.prune([rung], F("upgrade", {"upgrade": {"bin": "/b"}}))
    assert kept
    # any other goal takes the copy as it always did
    kept, _ = decide.prune([rung], F("copy", {"data": "/d"}))
    assert kept


def _fake_pg(monkeypatch, answers):
    """`upgrade._pg` answering by the first words of the statement."""
    from migkit import upgrade

    def ask(eng, side, db, sql):
        for (s, words), rows in answers.items():
            if s == side and words in " ".join(sql.split()):
                return rows
        return []
    monkeypatch.setattr(upgrade, "_pg", ask)


def test_settings_gone_and_defaults_changed_are_read_from_both(monkeypatch):
    from migkit import upgrade
    q = "select name || chr(31) || coalesce(boot_val, '')"
    _fake_pg(monkeypatch, {
        ("src", q): [["old_snapshot_threshold", "-1", "configuration file",
                      "sighup", "60"],
                     ["jit", "on", "default", "user", "on"],
                     ["work_mem", "4096", "default", "user", "4096"],
                     ["data_checksums", "off", "override", "internal",
                      "off"]],
        ("dst", q): [["jit", "off", "default", "user", "off"],
                     ["work_mem", "4096", "default", "user", "4096"],
                     ["data_checksums", "on", "override", "internal",
                      "on"]]})
    rows = upgrade._pg_settings(None, "app", False, (17,))
    said = {r["item"]: r for r in rows}
    gone = said["settings the new version does not have"]
    assert "old_snapshot_threshold = 60 (now nothing (17))" in \
        gone["detail"], gone
    changed = said["defaults the new version changed"]
    assert "jit: on by default before, off now" in changed["detail"]
    # what the server keeps for itself is not the operator's to set
    assert "data_checksums" not in changed["detail"]
    assert "work_mem" not in changed["detail"]
    # the same on both: nothing to say but that
    _fake_pg(monkeypatch, {("src", q): [["jit", "on", "default", "user",
                                         "on"]],
                           ("dst", q): [["jit", "on", "default", "user",
                                         "on"]]})
    assert upgrade._pg_settings(None, "app", False, (17,))[0]["level"] == \
        "pass"


def test_each_extension_is_offered_at_its_version_or_a_later_one(
        monkeypatch):
    from migkit import upgrade
    _fake_pg(monkeypatch, {
        ("src", "select extname"): [["plpgsql", "1.0"],
                                    ["postgis", "3.1.0"],
                                    ["adminpack", "2.1"],
                                    ["hstore", "1.9"]],
        ("dst", "from pg_available_extension_versions"): [
            ["postgis", "3.1.0"], ["postgis", "3.4.0"], ["hstore", "1.7"],
            ["hstore", "1.8"]],
        ("dst", "from pg_available_extensions"): [["postgis", "3.4.0"],
                                                 ["hstore", "1.8"]]})
    rows = upgrade._pg_extensions(None, "app", False)
    text = " | ".join(f"{r['level']} {r['detail']}" for r in rows)
    assert "fail the target does not offer adminpack at all" in text, text
    assert "hstore: the source runs 1.9, the target offers up to 1.8" \
        in text, text
    assert "postgis 3.1.0 (updated to 3.4.0 after)" in text, text


def test_a_type_is_held_against_the_way_that_refuses_it(monkeypatch):
    from migkit import upgrade
    _fake_pg(monkeypatch, {("src", "join pg_type t"): [
        ["public.acl.a", "aclitem"], ["public.old.t", "abstime"]]})
    # a copy of the pages from 15 into 16: its own upgrade refuses both
    got = upgrade._pg_types(None, "app", (11,), (16,), True)[0]
    assert got["level"] == "fail"
    assert "public.acl.a (aclitem" in got["detail"], got
    assert "public.old.t (abstime" in got["detail"], got
    # a logical move carries aclitem's values; abstime is gone either way
    got = upgrade._pg_types(None, "app", (11,), (16,), False)[0]
    assert "aclitem" not in got["detail"] and "abstime" in got["detail"]
    # from 16 the aclitem's storage is the new one already
    _fake_pg(monkeypatch, {("src", "join pg_type t"): [
        ["public.acl.a", "aclitem"]]})
    assert upgrade._pg_types(None, "app", (16,), (17,), True)[0][
        "level"] == "pass"


def test_mysql_settings_and_sign_in_are_held_to_the_new_version(
        monkeypatch):
    from migkit import upgrade
    got = {"src": {"expire_logs_days": ("7", False), "sql_mode": ("A", True),
                   "server_id": ("1", True)},
           "dst": {"sql_mode": ("B", True), "server_id": ("2", True)}}
    monkeypatch.setattr(upgrade, "_my_vars", lambda eng, side: got[side])
    rows = {r["item"]: r["detail"] for r in upgrade._my_settings(None)}
    assert "expire_logs_days = 7 (now binlog_expire_logs_seconds (8.4))" \
        in rows["settings the new version does not have"], rows
    assert "sql_mode: A by default before, B now" in \
        rows["defaults the new version changed"], rows
    assert "server_id" not in " ".join(rows.values())
    monkeypatch.setattr(upgrade, "_my", lambda *a, **k: [("app", "%")])
    assert upgrade._my_auth(None, (8, 0)) == []
    assert upgrade._my_auth(None, (8, 4))[0]["level"] == "warn"
    assert upgrade._my_auth(None, (9, 0))[0]["level"] == "fail"


def test_mysqls_own_checker_is_said_in_migkits_words(monkeypatch):
    from migkit import tools, upgrade
    from migkit.config import Endpoint, Hop
    report = {"checksPerformed": [
        {"id": "reservedKeywords", "title": "Usage of reserved words",
         "description": "names that are reserved",
         "detectedProblems": [{"level": "Warning", "dbObject": "app.rank",
                               "description": "column name"}]},
        {"id": "removedSysVars", "title": "Removed system variables",
         "detectedProblems": [{"level": "Error", "dbObject": "query_cache",
                               "description": "removed in 8.0"}]}]}
    monkeypatch.setattr(tools, "which", lambda n: "/bin/" + n)

    class Done:
        returncode = 0
        stderr = ""
        stdout = "Please wait...\n" + json.dumps(report) + "\n"
    seen = {}

    def run(argv, **kw):
        seen.update(argv=argv, input=kw.get("input"))
        return Done()
    monkeypatch.setattr(subprocess, "run", run)
    hop = Hop(name="u", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=3306, user="app",
                              password="CHANGE_ME"), target=Endpoint())
    rows = upgrade.checker(hop, None, "app", "8.4.2")
    assert [r["level"] for r in rows] == ["warn", "fail"], rows
    assert rows[0]["item"] == "Usage of reserved words"
    assert "app.rank column name" in rows[0]["detail"]
    # the password goes in on standard input, never on the command line
    assert "CHANGE_ME" not in " ".join(seen["argv"])
    assert seen["input"].startswith("CHANGE_ME")
    assert "targetVersion: \"8.4.2\"" in " ".join(seen["argv"])


def test_the_in_place_step_measured_is_said_as_the_downtime_avoided(
        tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import planner, upgrade
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    hop = Hop(name="u", engine="postgres", source=Endpoint(),
              target=Endpoint(), goal="upgrade")
    assert upgrade.in_place_line(hop) == ""
    planner.record_rate(hop, upgrade.IN_PLACE, 1000, 42.0)
    said = upgrade.in_place_line(hop)
    assert "would stop it for ~42s" in said, said
    assert "the downtime is the cutover's alone" in said


def test_the_goal_is_one_engine_and_its_settings_its_own():
    from migkit import hopkeys
    g = hopkeys.GOALS["upgrade"]
    assert g["writes"] and "cutover" not in g["options"]
    got = [(p.level, p.where) for p in hopkeys.check_hop("u", {
        "goal": "upgrade", "source": "mysql://app@10.0.0.1/app",
        "target": "postgres://app@10.0.0.2/app"})]
    assert ("error", "hops.u.goal") in got, got
    got = [(p.level, p.where) for p in hopkeys.check_hop("u", {
        "goal": "copy", "source": "postgres://app@10.0.0.1/app",
        "target": "postgres://app@10.0.0.2/app",
        "options": {"physical": {"data": "/d", "upgrade": {"bin": "/b"}}}})]
    assert ("warn", "hops.u.options.physical.upgrade") in got, got


# --- PostgreSQL 16 against 17 ------------------------------------------------------

def _sql(name, sql, db="app"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair(tmp_path_factory):
    try:
        for (name, port), image, extra in (
                (SRC, "postgres:16", ["-c", "old_snapshot_threshold=60"]),
                (DST, "postgres:17", [])):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", image, *extra],
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
        _sql(SRC[0], "create extension adminpack;"
                     " create function f(i int) returns int language sql"
                     " immutable as 'select i + 1';"
                     " create table e (i int); create index on e (f(i));"
                     " create table t (id int primary key, v text);"
                     " insert into t select g, 'v' || g from"
                     " generate_series(1, 1000) g")
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
        "hops:\n  up:\n    engine: postgres\n    goal: upgrade\n"
        f"    source: {{host: 127.0.0.1, port: {SRC[1]}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST[1]}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


@needs_docker
def test_what_17_changes_is_said_before_anything_moves(conf):
    from migkit import upgrade
    from migkit.config import get_hop
    from migkit.engines import get_engine
    hop = get_hop("up")
    rows = upgrade.checks(hop, get_engine(hop), "app", quick=True)
    by = {}
    for r in rows:
        by.setdefault(r["item"], []).append(r)
    assert by["versions"][0]["level"] == "pass"
    assert " to 17" in by["versions"][0]["detail"], by["versions"]
    ext = " ".join(r["detail"] for r in by["extensions"])
    assert "the target does not offer adminpack at all" in ext, ext
    assert any(r["level"] == "fail" for r in by["extensions"])
    gone = by["settings the new version does not have"][0]["detail"]
    assert "old_snapshot_threshold = 60 (now nothing (17))" in gone, gone
    code = " ".join(r["detail"] for r in by["code"])
    assert "have no search path of their own: public.f" in code, code
    # the move is not started while an extension the target lacks stands
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "up", "--mode", "full",
                                        "--go"])
    assert got.exit_code != 0
    assert "upgrade: extensions" in " ".join(got.output.split()), got.output
    assert _sql(DST[0], "select count(*) from pg_tables where schemaname ="
                        " 'public'") == "0"


@needs_docker
def test_the_plan_says_the_way_and_why_not_the_other(conf):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "up", "--mode", "full+cdc"])
    out = " ".join(got.output.split())
    assert "upgrade: a logical move into the new version" in out, out
    assert "not a copy of the pages upgraded on the target: the hop does" \
        " not say how the target is upgraded" in out, out
