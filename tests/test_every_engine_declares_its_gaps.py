"""Every engine either does a thing or says why it does not.

Owner's rule (2026-09-24): one hop, one command, one result on every
engine. What an engine does is read off the code; what it does not do is
declared in `migkit/capabilities.py`. These tests hold the two together in
both directions, so a gap can neither appear nor close without the
declaration changing with it.
"""
import re

import pytest
from pathlib import Path

from migkit import capabilities as caps
from migkit.engines import NAMES
from tests.test_the_report_does_not_name_its_tools import TOOLS

BACKLOG = (Path(__file__).parent.parent / "docs" / "backlog.md").read_text()


def test_no_engine_lacks_anything_without_saying_so():
    assert caps.undeclared() == []


def test_no_declared_gap_is_one_the_code_has_closed():
    assert caps.stale() == []


def test_every_engine_and_every_capability_is_in_the_table():
    assert set(caps.GAPS) == set(NAMES)
    for name, gaps in caps.GAPS.items():
        assert set(gaps) <= set(caps.CAPABILITIES), (name, gaps)
    assert set(caps.PROBES) == set(caps.CAPABILITIES)


def test_a_gap_carries_its_reason_or_its_backlog_item():
    for name, gaps in caps.GAPS.items():
        for cap, (state, why) in gaps.items():
            assert state in (caps.NOT_APPLICABLE, caps.NOT_YET), (name, cap)
            if state == caps.NOT_YET:
                assert re.search(rf"^\*\*{re.escape(why)}\. ", BACKLOG,
                                 re.M), (name, cap, why)
            else:
                assert len(why.split()) >= 4, (name, cap, why)


def test_the_words_an_operator_reads_name_no_program():
    said = list(caps.CAPABILITIES.values()) + [
        why for gaps in caps.GAPS.values()
        for state, why in gaps.values() if state == caps.NOT_APPLICABLE]
    for text in said:
        low = text.lower()
        assert not [t for t in TOOLS if t in low], text


def test_removing_a_capability_is_noticed(monkeypatch):
    """The probes have teeth: take PostgreSQL's fence away and the table
    no longer matches the code."""
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.delattr(PostgresEngine, "fence_wait")
    assert ("postgres", "fence") in caps.undeclared()


def test_closing_a_gap_is_noticed(monkeypatch):
    """Give the generic engine a table copier and its declared gap goes
    stale. (Redis, then Kafka, were this example until each was given
    one.)"""
    from migkit.engines.generic import GenericEngine
    monkeypatch.setattr(GenericEngine, "move_table",
                        lambda self, *a: None, raising=False)
    assert ("generic", "table-copy") in caps.stale()


def test_the_base_class_placeholder_is_not_a_capability(monkeypatch):
    """`Engine.check_deep` only says there are no deep checks. An engine
    inheriting it has none. (The pair used to be that engine; it has deep
    checks of its own now - the target's set-aside objects, orphans, zero
    dates.)"""
    from migkit.engines.base import Engine

    class Bare(Engine):
        pass
    assert Bare.check_deep is Engine.check_deep
    assert not caps._own(Bare, "check_deep")
    assert caps.implemented("hetero", "deep")


def test_users_is_read_from_its_dispatch():
    got = caps._users_engines()
    assert {"postgres", "mysql", "mongodb", "redis"} <= got, got
    assert "kafka" not in got, got


def _refused(engine, cap):
    import pytest
    with pytest.raises(SystemExit) as e:
        caps.require(engine, cap)
    said = str(e.value)
    low = said.lower()
    assert not [t for t in TOOLS if t in low], said
    return said


def test_a_capability_the_engine_has_is_not_refused():
    caps.require("postgres", "fence")
    caps.require("mongodb", "stream")


def test_not_yet_says_what_to_do_meanwhile():
    said = _refused("mssql", "users")
    assert said.startswith("Carrying users and their grants is not"
                           " available for mssql hops yet"), said
    assert "backlog item 0e" in said, said
    assert caps.INSTEAD["users"] in said, said


def test_not_applicable_says_why():
    said = _refused("sqlite", "stream")
    assert "does not apply to sqlite hops" in said, said
    assert "moves offline" in said, said


def test_the_name_the_operator_used_is_the_one_said_back():
    """`azure-sql` is SQL Server underneath; the operator wrote azure-sql.
    (This used MySQL's fence, then MongoDB's, then MongoDB's collection
    copy, then SQL Server's stream; all four have one now.)"""
    said = _refused("azure-sql", "users")
    assert "for azure-sql hops" in said, said


def test_an_unknown_engine_is_refused_not_guessed():
    said = _refused("oracle-ish", "counts")
    assert "oracle-ish" in said, said


def test_every_capability_has_something_to_do_meanwhile():
    assert set(caps.INSTEAD) == set(caps.CAPABILITIES)
    for text in caps.INSTEAD.values():
        low = text.lower()
        assert not [t for t in TOOLS if t in low], text


@pytest.fixture
def sqlite_hop(tmp_path, monkeypatch):
    """A configured SQLite hop, the one engine with gaps and no server."""
    import sqlite3
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key)")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return "lite"


@pytest.mark.parametrize("argv,cap", [
    (["move", "{hop}", "--mode", "cdc", "--go"], "stream"),
    (["move", "{hop}", "--mode", "full+cdc", "--go"], "stream"),
    (["watch", "{hop}", "--verify", "--delta", "--count", "1"], "delta"),
    (["users", "{hop}", "test"], "users"),
])
def test_every_command_refuses_in_the_same_words(sqlite_hop, argv, cap):
    """The sentence each command prints is the one the table holds, so a
    gap reads the same whichever command ran into it."""
    from click.testing import CliRunner
    from migkit import cli
    got = CliRunner().invoke(cli.main,
                             [a.format(hop=sqlite_hop) for a in argv])
    assert got.exit_code != 0, got.output
    said = " ".join((got.output + str(got.exception or "")).split())
    words = caps.CAPABILITIES[cap]
    assert (words[0].upper() + words[1:]) in said, said
    assert "sqlite hops" in said, said
    assert "Traceback" not in said, said
    low = said.lower()
    assert not [t for t in TOOLS if t in low], said


def test_a_sqlite_sync_keeps_the_target_file_first(sqlite_hop, tmp_path):
    """`sync --go` refused on SQLite for want of a restore point; it now
    keeps the whole target file before it repairs anything."""
    import sqlite3

    from click.testing import CliRunner
    from migkit import cli
    con = sqlite3.connect(tmp_path / "b.db")
    con.execute("insert into t values (41)")
    con.commit()
    con.close()
    got = CliRunner().invoke(cli.main, ["sync", sqlite_hop, "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    kept = list((tmp_path / "reports").rglob("dst.sqlite"))
    assert len(kept) == 1, kept
    con = sqlite3.connect(kept[0])
    assert con.execute("select id from t").fetchall() == [(41,)]
    con.close()


def test_a_method_that_only_refuses_is_not_a_capability(monkeypatch):
    """Redis had a `delta_verify` whose whole body returned an error, and
    the matrix counted it as verifying deltas."""
    from migkit.engines.base import Result
    from migkit.engines.redis import RedisEngine

    def delta_verify(self, db, limit=20000, log=None):
        return [Result("delta", db, "error", "cannot")]

    monkeypatch.setattr(RedisEngine, "delta_verify", delta_verify,
                        raising=False)
    assert not caps.implemented("redis", "delta")
    assert caps.implemented("kafka", "delta")


def test_a_move_on_an_engine_with_no_copier_refuses_in_the_same_words(
        tmp_path, monkeypatch):
    """The generic engine still has no copier. SQLite, Redis and Kafka,
    which this used to run on, now copy table by table, keyspace by
    keyspace and topic by topic."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  r:\n    engine: generic\n"
        "    source: {host: 10.0.0.1, port: 5000, user: x, password: x}\n"
        "    target: {host: 10.0.0.2, port: 5000, user: x, password: x}\n"
        "    databases: ['0']\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["move", "r", "--mode", "full",
                                        "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code != 0, said
    assert "Copying table by table, resumably is not available for" \
        " generic hops yet" in said, said
