"""Every option a wrapped tool offers is used, or its reason is written
(backlog 0f).

The owner's rule: every tool used whole, not one feature and done. So the
account in `migkit/tooluse.py` is held against the tool itself - the
streaming copy's installed build, asked subcommand by subcommand; the
connector's pinned release, as documented - in both directions: an option
the tool has that nobody wrote about fails, and so does an entry for an
option the tool no longer has. And what the account calls used is what
the command lines and the configuration really carry: an option passed
and called unused, or called used and passed nowhere, fails too.
"""
import subprocess

import pytest

from migkit import movers, tooluse
from migkit.config import Endpoint, Hop
from tests.test_the_report_does_not_name_its_tools import TOOLS

pgcopydb_here = pytest.mark.skipif(
    subprocess.run(["which", "pgcopydb"], capture_output=True).returncode,
    reason="the streaming copy is not installed here")


@pgcopydb_here
def test_the_installed_copy_has_nothing_unaccounted_for():
    have = tooluse.surface("pgcopydb")
    # read off the build: a surface of nothing would pass for the wrong
    # reason
    assert sum(len(v) for v in have.values()) > 100, have
    assert "--split-tables-larger-than" in have["copy table-data"]
    assert tooluse.unexplained("pgcopydb") == []
    assert tooluse.stale("pgcopydb") == []


def test_the_connector_has_nothing_unaccounted_for():
    have = tooluse.surface("debezium")
    assert set(have) == {"postgres", "mysql", "jdbc-sink", "signals"}
    assert tooluse.unexplained("debezium") == []
    assert tooluse.stale("debezium") == []


@pgcopydb_here
def test_an_option_nobody_wrote_about_is_caught(monkeypatch):
    tool = tooluse.REGISTRY["pgcopydb"]
    options = dict(tool.options)
    del options[("copy table-data", "--split-max-parts")]
    monkeypatch.setattr(tool, "options", options)
    assert tooluse.unexplained("pgcopydb") == [
        ("copy table-data", "--split-max-parts")]


@pgcopydb_here
def test_an_entry_for_an_option_that_is_gone_is_caught(monkeypatch):
    tool = tooluse.REGISTRY["pgcopydb"]
    monkeypatch.setattr(tool, "options", {
        **tool.options,
        ("copy table-data", "--renamed-away"): (tooluse.USED,
                                                "a use for a flag gone")})
    assert tooluse.stale("pgcopydb") == [("copy table-data",
                                          "--renamed-away")]


def test_a_reason_has_to_be_written():
    with pytest.raises(ValueError, match="not written"):
        tooluse.register("x", licence="MIT",
                         options={("", "--a"): (tooluse.UNUSED, "no")})
    tooluse.REGISTRY.pop("x", None)


def test_no_reason_names_a_program():
    """The account is migkit's words about the tools; the names are its
    keys, never its reasons."""
    for name, row in tooluse.tools_used().items():
        for state in tooluse.STATES:
            for option, why in row[state].items():
                assert not [t for t in TOOLS if t in why.lower()], \
                    (name, option, why)


def test_every_tool_has_a_licence_and_terms():
    from migkit import tools
    rows = tooluse.tools_used()
    assert {"pgcopydb", "debezium", "redpanda"} <= set(rows)
    for name, row in rows.items():
        assert row["licence"], name
        assert row["terms"], name
    # a program `doctor --install` puts in place: its licence is `tools`'s,
    # read from there and never said twice
    assert rows["pgcopydb"]["licence"] == tools.PROGRAM_LICENCES["pgcopydb"]
    assert rows["pgcopydb"]["terms"] == "open"
    with pytest.raises(ValueError, match="`tools`'s"):
        tooluse.register("pgcopydb", licence="MIT", options={})
    assert tooluse.REGISTRY["pgcopydb"].options, "the refusal replaced it"
    # a closed licence says what it allows
    assert "hosted" in rows["redpanda"]["terms"]


def test_a_closed_programs_terms_gate_it_until_accepted(monkeypatch):
    """A program `tools` holds closed terms for is allowed only once the
    operator accepted them there, and says what runs instead."""
    from migkit import tools
    closed = next(iter(tools.TERMS))
    tooluse.surface("pgcopydb")          # every contributor registered
    was = tooluse.REGISTRY.pop(closed, None)
    tooluse.register(closed, options={})
    try:
        monkeypatch.setattr(tools, "terms_accepted", lambda n: False)
        ok, why = tooluse.allowed(closed, _hop("postgres"))
        assert not ok and tools.TERMS[closed].otherwise in why, why
        monkeypatch.setattr(tools, "terms_accepted", lambda n: True)
        assert tooluse.allowed(closed, _hop("postgres")) == (True, "")
    finally:
        tooluse.REGISTRY.pop(closed, None)
        if was is not None:
            tooluse.REGISTRY[closed] = was


def test_terms_gate_a_task_where_they_say_so():
    hop = _hop("postgres")
    assert tooluse.allowed("pgcopydb", hop) == (True, "")
    tooluse.register("closed-x", licence="proprietary",
                     terms=tooluse.Terms(open=False, allows="Redis only",
                                         check=lambda h: "" if h.engine ==
                                         "redis" else "Redis only"),
                     options={})
    try:
        assert tooluse.allowed("closed-x", hop) == (False, "Redis only")
        assert tooluse.allowed("closed-x", _hop("redis")) == (True, "")
    finally:
        tooluse.REGISTRY.pop("closed-x", None)


def _hop(engine, **kw):
    return Hop(name="acct", engine=engine,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["appdb"], **kw)


def _flags(argv):
    return {a for a in argv if isinstance(a, str) and a.startswith("--")}


def test_what_the_copy_is_passed_is_what_the_account_calls_used():
    """Every shape the table copy's command line takes - whole or split,
    fresh or going on, filtered or not, one table under a held snapshot
    or not - against the account."""
    split = movers._Split("split", "why", 1 << 20, 5, ("public.t",))
    whole = movers._Split("whole", "why")
    passed = {"--dir", "--source", "--target"}
    for s in (split, whole):
        for resume in (False, True):
            for filters in (None, "/tmp/f.ini"):
                for snapshot in (None, "00000003-00000048-1"):
                    passed |= _flags(movers._copy_args(4, s, resume, filters,
                                                       snapshot))
    used = {o for (c, o), (state, _) in
            tooluse.REGISTRY["pgcopydb"].options.items()
            if c == "copy table-data" and state == tooluse.USED}
    assert passed == used, (passed ^ used)


def test_what_the_leg_is_passed_is_what_the_account_calls_used(
        tmp_path, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    hop = _hop("postgres", exclude=["audit"])
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(PostgresEngine, "_all_tables",
                        lambda self, side, db: ["public.t", "public.audit"])
    steps = movers.pgcopydb_follow(hop, "appdb", go=False)
    assert any("left out of the stream" in s for s in steps), steps
    used = {o for (c, o), (state, _) in
            tooluse.REGISTRY["pgcopydb"].options.items()
            if c == "follow" and state == tooluse.USED}
    # the leg's state directory and its two databases' addresses: the
    # addresses reach it through its environment, never its command line
    passed = _flags(steps[0].argv) | {"--source", "--target"}
    assert passed == used, (passed ^ used)


def _configs(tmp_path, monkeypatch, engine, **kw):
    import json

    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: True)
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: True)
    monkeypatch.setattr(movers, "_stream_facts", lambda hop, dbs, my: (
        {d: (["t", "audit"] if my else ["public.t", "public.audit"])
         for d in dbs}, 120.0, 900.0))
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    for cls in (MySQLEngine, PostgresEngine):
        monkeypatch.setattr(cls, "column_catalog",
                            lambda self, side, db: {"public.t": [("a", "t"),
                                                                 ("b", "t")],
                                                    "t": [("a", "t"),
                                                          ("b", "t")]})
    hop = _hop(engine, **kw)
    out = movers.stream_codegen(hop, ["appdb"], engine)
    return (json.loads((out / "source-connector.json").read_text())["config"],
            json.loads((out / "sink-connector.json").read_text())["config"])


def _written(tmp_path, monkeypatch):
    """{connector: keys written} over the shapes the pipeline takes."""
    got = {"postgres": set(), "mysql": set(), "jdbc-sink": set()}
    for engine, name in (("postgres", "postgres"), ("mysql", "mysql")):
        for kw in ({}, {"exclude": ["audit"],
                        "mapping": {"columns": {"t": {"drop": ["b"]}}}}):
            src, sink = _configs(tmp_path / f"{engine}{len(kw)}",
                                 monkeypatch, engine, **kw)
            got[name] |= set(src)
            got["jdbc-sink"] |= set(sink)
    return got


def _as_entry(connector, key):
    tool = tooluse.REGISTRY["debezium"]
    for fam in tool.families:
        if key.startswith(fam):
            return fam
    return key


def test_what_the_connector_is_given_is_what_the_account_calls_used(
        tmp_path, monkeypatch):
    got = _written(tmp_path, monkeypatch)
    tool = tooluse.REGISTRY["debezium"]
    for connector, keys in got.items():
        written = {_as_entry(connector, k) for k in keys}
        used = {o for (c, o), (state, _) in tool.options.items()
                if c == connector and state == tooluse.USED}
        assert written == used, (connector, written ^ used)
        # and every key is one the release has
        have = tooluse.surface("debezium")[connector]
        assert written <= have, (connector, written - have)


def test_the_signals_sent_are_the_ones_the_account_calls_used():
    used = {o for (c, o), (state, _) in
            tooluse.REGISTRY["debezium"].options.items()
            if c == "signals" and state == tooluse.USED}
    assert set(movers.SIGNALS) == used
    # each one migkit sends is one it can build
    for signal in movers.SIGNALS:
        assert movers.signal_message("h", signal, ["t"])[2]["type"] == signal
