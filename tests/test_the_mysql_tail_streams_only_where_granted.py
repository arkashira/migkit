"""The MySQL tail streams the binlog only where the user's replication
grants were read and hold (`MySQLEngine._change_read`), through its active
roles where roles give them; otherwise it reads the log to its end each
batch and names the GRANT that would let it stream. The probes are kept
with the run's (`diagnostics.accumulator`). Fake server only."""
import types

import pytest

from migkit import diagnostics
from migkit.engines.mysql import MySQLEngine

pytest.importorskip("pymysqlreplication")

FULL = "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `m`@`%`"


def _engine(name, grants, using=None, role="NONE"):
    eng = MySQLEngine.__new__(MySQLEngine)
    eng.hop = types.SimpleNamespace(name=name, options={})
    eng._d = lambda side, db: db
    asked = []

    def q(side, sql, args=None):
        asked.append(sql)
        if sql == "show grants":
            return [(g,) for g in grants]
        if sql.startswith("show grants for current_user() using"):
            return [(g,) for g in using or ()]
        if sql == "select current_role()":
            return [(role,)]
        return [({"select @@version": "8.0.36",
                 "select @@global.binlog_format": "ROW",
                 "select @@global.binlog_row_image": "FULL",
                 "select @@global.gtid_mode": "ON"}.get(sql),)]
    eng._q = q
    diagnostics.forget(eng.hop)
    return eng, asked


def test_granted_streams_and_the_probes_are_kept():
    eng, asked = _engine("my-stream-yes", [FULL])
    choice = eng._change_read("src", "d")
    assert choice.path == "stream"
    got = diagnostics.accumulator(eng.hop).get("change-read", "src:d").value
    assert got["binlog_format"] == "ROW" and got["gtid_mode"] == "ON"
    assert {"REPLICATION SLAVE", "REPLICATION CLIENT"} <= set(
        got["privileges"])
    n = len(asked)
    assert eng._change_read("src", "d") is choice and len(asked) == n


def test_not_granted_polls_and_names_the_grant():
    eng, _ = _engine("my-stream-no", ["GRANT SELECT ON *.* TO `m`@`%`"])
    choice = eng._change_read("src", "d")
    assert choice.path == "poll"
    assert "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.*" \
        in choice.reason


def test_half_granted_names_only_what_is_missing():
    eng, _ = _engine("my-stream-half", [
        "GRANT REPLICATION CLIENT ON *.* TO `m`@`%`"])
    choice = eng._change_read("src", "d")
    assert choice.path == "poll"
    assert "GRANT REPLICATION SLAVE ON *.*" in choice.reason


def test_a_role_s_grants_are_read_through_the_active_role():
    eng, asked = _engine(
        "my-stream-role", ["GRANT USAGE ON *.* TO `m`@`%`",
                           "GRANT `repl`@`%` TO `m`@`%`"],
        using=[FULL], role="`repl`@`%`")
    assert eng._change_read("src", "d").path == "stream"
    assert "show grants for current_user() using `repl`@`%`" in asked


def test_a_role_not_active_is_not_assumed_to_grant():
    eng, _ = _engine("my-stream-norole", ["GRANT USAGE ON *.* TO `m`@`%`",
                                          "GRANT `repl`@`%` TO `m`@`%`"])
    choice = eng._change_read("src", "d")
    assert choice.path == "poll"
    assert "could not be read" in choice.reason
