"""Free space from whoever can say it, and a release never run on refused
in words (backlog 7.free, 26.versions).

The free space comes from the first source that answers
(`decide.free_space_source`): the server's own report, this machine's
look at a data directory proved the server's own, the provider's metric -
and nobody, said so, never a guess. A server's release is held to the
releases migkit has run its engine on (`decide.version_seen`), and the
version-keyed choices it takes are listed (`decide.version_choices`)."""
import shutil
import types

import pytest

from migkit import decide, ready, sizing
from tests import servers
from tests.conftest import needs_docker


class _Eng:
    hop = types.SimpleNamespace(source=None, target=None, options={})


def test_the_servers_own_report_is_taken_first(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free",
                        lambda e, s: pytest.fail("the provider was asked"))
    e = _Eng()
    runs = []

    def server():
        runs.append(1)
        return 5 << 30, None
    got = sizing.free_bytes(e, "dst", "d", server,
                            lambda: pytest.fail("the directory was looked"))
    assert got == 5 << 30
    assert e.free_choice[("dst", "d")][1].path == "server"
    # the answer stands for the run: the server's program runs once
    assert sizing.free_bytes(e, "dst", "d", server, None) == 5 << 30
    assert runs == [1]


def test_a_server_that_will_not_say_falls_to_the_directory(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free", lambda e, s: (None, "x"))
    e = _Eng()
    got = sizing.free_bytes(e, "dst", "d",
                            lambda: (None, "the account may not run it"),
                            lambda: (7 << 20, None))
    assert got == 7 << 20
    c = e.free_choice[("dst", "d")][1]
    assert c.path == "directory" and "may not run it" in c.reason


def test_nobody_saying_is_none_with_every_why_never_a_guess(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free",
                        lambda e, s: (None, "not a managed service"))

    def broken():
        raise RuntimeError("permission denied for function pg_stat_file")
    e = _Eng()
    assert sizing.free_bytes(e, "dst", "d", lambda: (None, "no right"),
                             broken) is None
    c = e.free_choice[("dst", "d")][1]
    assert c.path == "none"
    for why in ("no right", "permission denied", "not a managed service"):
        assert why in c.reason, c.reason


def test_a_directory_of_the_same_name_that_is_not_the_servers_is_not_read(
        tmp_path):
    got, why = sizing.local_free(str(tmp_path), lambda: False)
    assert got is None and "not the server's own" in why
    got, why = sizing.local_free(str(tmp_path / "absent"), lambda: True)
    assert got is None and "not on this machine" in why
    got, why = sizing.local_free(str(tmp_path), lambda: True)
    assert got == shutil.disk_usage(tmp_path).free and why is None


def test_a_release_run_on_is_taken_and_one_outside_is_refused_in_words():
    assert decide.version_seen({"family": "postgres",
                                "version": (16, 4)}).path == "seen"
    newer = decide.version_seen({"family": "postgres", "version": (19, 0)})
    assert newer.rung is None and "newer than 17" in newer.reason
    older = decide.version_seen({"family": "mariadb", "version": (10, 6, 1)})
    assert older.rung is None and "older than 10.11" in older.reason
    assert decide.version_seen({"family": "mysql",
                                "version": None}).path == "unread"


def test_the_version_keyed_choices_are_listed_against_the_release():
    got = {w: t for _, w, t in decide.version_choices("postgres", (14, 2))}
    assert got["procedures told from functions"] is True
    assert got["the subscriber applies streamed transactions in"
               " parallel"] is False
    assert not any(t for _, _, t in decide.version_choices("postgres", None))


def _gate(src, dst):
    e = types.SimpleNamespace(server_release=lambda side, db: (
        dst if side == "dst" else src))
    return {i.what: i for i in ready._releases(e, ["d"])}


def test_a_release_not_run_on_goes_on_said_unproved_with_its_choices():
    by = _gate(("postgres", (16, 1)), ("postgres", (19, 0)))
    assert by["the source's release"].level == "pass"
    assert "procedures told from functions" in by["the source's"
                                                  " release"].detail
    t = by["the target's release"]
    assert t.level == "warn" and not t.gates
    assert t.detail.startswith("release 19.0 not run on here; the choices"
                               " keyed on version are: ")
    assert "unproved on this run" in t.detail
    assert "in parallel (taken)" in t.detail
    # MariaDB keys nothing on its version: an old one goes on, said so
    m = _gate(("mariadb", (10, 6, 1)), ("mariadb", (11, 4, 2)))
    assert m["the source's release"].level == "warn"
    assert m["the target's release"].level == "pass"


def test_only_a_release_below_every_version_keyed_choice_is_refused():
    by = _gate(("postgres", (9, 6)), ("mysql", (8, 0, 12)))
    s, t = by["the source's release"], by["the target's release"]
    assert s.level == "fail" and s.gates and "older than 10.0" in s.detail
    assert t.level == "fail" and "older than 8.0.17" in t.detail
    assert _gate(("mysql", (8, 0, 17)), ("mysql", (8, 4, 3)))[
        "the source's release"].level == "pass"


# ---- on the lanes (not run in the ticket: built, owed a lane run) --------

pg = servers.fixture("pg")
my = servers.fixture("mysql")
maria = servers.fixture("mariadb")


@needs_docker
def test_on_postgres_the_server_says_its_free_space_and_release(pg):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="free", engine="postgres", source=pg.src.endpoint(),
              target=pg.dst.endpoint(), databases=[pg.name])
    eng = PostgresEngine(hop)
    got = eng.free_bytes("dst", pg.name)
    assert eng.free_choice[("dst", pg.name)][1].path == "server"
    data = pg.dst.sql("show data_directory")[0][0]
    df = pg.dst.exec("df", "-Pk", data).stdout.splitlines()[-1].split()
    assert abs(got - int(df[3]) * 1024) < 64 << 20
    fam, ver = eng.server_release("dst", pg.name)
    assert decide.version_seen({"family": fam, "version": ver}).path == "seen"


@needs_docker
@pytest.mark.parametrize("which", ["my", "maria"])
def test_on_mysql_and_mariadb_the_release_is_held_and_free_space_said(
        which, request):
    from migkit.config import Hop
    from migkit.engines.mysql import MySQLEngine
    s = request.getfixturevalue(which)
    hop = Hop(name="free", engine="mysql", source=s.src.endpoint(),
              target=s.dst.endpoint(), databases=[s.name])
    eng = MySQLEngine(hop)
    fam, ver = eng.server_release("dst", s.name)
    assert fam == ("mariadb" if which == "maria" else "mysql")
    assert decide.version_seen({"family": fam, "version": ver}).path == "seen"
    assert eng.free_bytes("dst", s.name) is None
    assert "does not report" in eng.free_choice[("dst", s.name)][1].reason
