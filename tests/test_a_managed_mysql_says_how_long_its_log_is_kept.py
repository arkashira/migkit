"""A managed MySQL's binlog retention, read from the service (backlog 32.rds).

On RDS and Aurora MySQL the time a binary log is kept is the service's own
setting (`mysql.rds_show_configuration`, `binlog retention hours`), not
`binlog_expire_logs_seconds`. This drives the readers with the server's
answers given (no server is reached): what is read, what the tail's room
becomes, what the assessment and the DBA's list say, and that an account
without the right to read the setting is never taken as fine.

The procedure was never run on a real RDS or Aurora here.
"""
import time

import pytest

from migkit import dba, ship
from migkit.config import build_hop
from migkit.engines.mysql import MySQLEngine, OWN_PURGE, managed_retention


class Server:
    """A MySQL server's answers, by the words in the statement."""

    def __init__(self, listed=1, aurora=None, config=("binlog retention hours",
                                                       None, ""),
                 denied=None, expire="86400", files=("bin.000007",)):
        self.listed, self.aurora, self.config = listed, aurora, config
        self.denied, self.expire, self.files = denied, expire, files

    def ask(self, sql):
        if "information_schema.routines" in sql:
            return [(self.listed,)]
        if "aurora_version" in sql:
            return [("aurora_version", self.aurora)] if self.aurora else []
        if sql == "call mysql.rds_show_configuration":
            if self.denied:
                raise RuntimeError(self.denied)
            return [("binlog retention hours", self.config[1], ""),
                    ("target delay", 0, "")] if self.config else []
        raise AssertionError(sql)


def read(server, hinted=False):
    return managed_retention(server.ask, hinted)


def test_a_server_that_is_not_managed_has_no_such_setting():
    assert read(Server(listed=0)) is None


def test_the_hours_set_are_the_room_a_stopped_tail_has():
    got = read(Server(config=("x", "48", "")))
    assert got["state"] == "hours" and got["hours"] == 48
    assert got["seconds"] == 48 * 3600
    assert "48 hours" in got["why"]


@pytest.mark.parametrize("value", [None, "NULL", ""])
def test_no_hours_set_is_a_log_removed_as_soon_as_it_may_be(value):
    got = read(Server(config=("x", value, "")))
    assert got["state"] == "unset" and got["seconds"] == OWN_PURGE
    assert "cannot be stopped at all" in got["why"]


@pytest.mark.parametrize("value", [0, "0"])
def test_zero_hours_is_the_same_and_never_read_as_no_expiry(value):
    got = read(Server(config=("x", value, "")))
    assert got["state"] == "zero" and got["seconds"] == OWN_PURGE
    assert got["hours"] == 0


def test_an_account_that_may_not_run_the_procedure_is_unknown_not_fine():
    got = read(Server(denied="(1370, \"execute command denied to user\")"))
    assert got["state"] == "unknown" and got["seconds"] is None
    assert "could not read" in got["why"] and "denied" in got["why"]


def test_an_account_that_cannot_see_the_procedure_listed_is_asked_anyway():
    # not listed to this account, and the address names a service
    server = Server(listed=0, denied="(1370, \"execute command denied\")")
    assert read(server) is None
    got = read(server, hinted=True)
    assert got["state"] == "unknown"


def test_a_setting_not_listed_or_not_a_number_is_unknown():
    assert read(Server(config=None))["state"] == "unknown"
    assert read(Server(config=("x", "forever", "")))["state"] == "unknown"


def test_aurora_is_said_and_what_is_not_known_of_it_is_too():
    got = read(Server(listed=0, aurora="3.05.2", config=("x", "24", "")))
    assert got["service"] == "Aurora MySQL" and got["hours"] == 24
    assert "has not measured" in got["caveat"]
    assert read(Server())["caveat"] == ""


# --- the tail's room ------------------------------------------------------

class Engine:
    """The pieces of `MySQLEngine` `stream_room` and `assess` read."""

    OWN_PURGE = OWN_PURGE

    def __init__(self, server, started=None):
        self.server, self.started = server, started
        self.hop = build_hop("h", {"source": "mysql://app@10.0.0.1/shop",
                                   "target": "mysql://app@10.0.0.2/shop"})

    managed_retention = MySQLEngine.managed_retention
    stream_room = MySQLEngine.stream_room
    _retention_row = MySQLEngine._retention_row

    def _q(self, side, sql, args=None):
        s = self.server
        if sql == "select @@binlog_expire_logs_seconds":
            return [(s.expire,)]
        if sql == "select @@binlog_expire_logs_auto_purge":
            return [(1,)]
        if sql == "show binary logs":
            return [(f,) for f in s.files]
        if sql.startswith("show variables like"):
            return [("binlog_expire_logs_seconds", s.expire)]
        return s.ask(sql)

    def _binlog_started(self, side, name):
        return self.started


TOKEN = {"log_file": "bin.000007", "log_pos": 4}


def test_a_tail_on_the_file_being_written_has_the_hours_set():
    eng = Engine(Server(config=("x", "24", "")))
    assert eng.stream_room("src", "shop", TOKEN) == {"seconds": 24 * 3600}


def test_the_room_on_a_closed_file_counts_from_when_it_closed():
    eng = Engine(Server(config=("x", "24", ""), files=(
        "bin.000007", "bin.000008")), started=time.time() - 3600)
    got = eng.stream_room("src", "shop", TOKEN)["seconds"]
    assert 23 * 3600 - 5 <= got <= 23 * 3600


@pytest.mark.parametrize("value", [None, 0])
def test_no_retention_hours_gives_minutes_never_none_which_says_nothing_purges(
        value):
    eng = Engine(Server(config=("x", value, "")))
    assert eng.stream_room("src", "shop", TOKEN) == {"seconds": OWN_PURGE}


def test_a_server_not_managed_is_held_to_its_variable_as_before():
    eng = Engine(Server(listed=0, expire="3600"))
    assert eng.stream_room("src", "shop", TOKEN) == {"seconds": 3600}


def test_a_denied_account_gives_no_number_and_says_why():
    eng = Engine(Server(denied="(1370, \"execute command denied\")"))
    got = eng.stream_room("src", "shop", TOKEN)
    assert "seconds" not in got and "could not read" in got["unknown"]


def test_the_seed_plan_holds_nothing_to_an_unknown_room_and_says_so():
    eng = Engine(Server(denied="(1370, \"execute command denied\")"))
    ok, words = ship.room(eng, "shop", TOKEN, 600)
    assert ok is None and "not known" in words
    assert ship.lost(eng, "shop", TOKEN) is None


def test_the_seed_plan_is_told_a_log_kept_minutes_cannot_outlast_it():
    eng = Engine(Server(config=("x", None, "")))
    ok, words = ship.room(eng, "shop", TOKEN, 3600)
    assert ok is False and "keeps its log" in words


# --- the assessment and the DBA's list -------------------------------------

def test_the_assessment_fails_a_managed_server_that_keeps_no_log():
    row = Engine(Server(config=("x", None, "")))._retention_row(None)
    assert row["level"] == "fail"
    assert "cannot be stopped at all" in row["detail"]
    assert "rds_set_configuration" in row["detail"]


def test_the_assessment_passes_a_day_and_warns_on_less():
    row = Engine(Server(config=("x", "168", "")))._retention_row(None)
    assert row["level"] == "pass" and "168 hours" in row["detail"]
    row = Engine(Server(config=("x", "6", "")))._retention_row(None)
    assert row["level"] == "warn" and "6 hours" in row["detail"]


def test_the_assessment_says_unknown_where_the_account_may_not_read_it():
    row = Engine(Server(denied="(1370, \"execute command denied\")")
                 )._retention_row(None)
    assert row["level"] == "warn" and "not known" in row["detail"]


def test_the_assessment_on_aurora_carries_what_is_not_known():
    row = Engine(Server(listed=0, aurora="3.05.2", config=("x", "48", ""))
                 )._retention_row(None)
    assert row["level"] == "pass" and "Aurora MySQL" in row["detail"]
    assert "has not measured" in row["detail"]


def test_the_assessment_of_a_server_not_managed_reads_the_variable():
    eng = Engine(Server(listed=0, expire="3600"))
    row = eng._retention_row(lambda side, name: "3600")
    assert row["level"] == "warn" and "binlog_expire_logs_seconds" in \
        row["detail"]
    row = eng._retention_row(lambda side, name: "604800")
    assert row["level"] == "pass"


class Listed:
    """`dba._my`'s engine: a server that answers by the words asked."""

    CANON_ENGINE = "mysql"

    def __init__(self, server):
        self.server = server

    def databases(self):
        return ["shop"]

    def _repl_name(self):
        return "migkit_h"

    def _q(self, side, sql, args=None):
        if side == "dst":
            if "information_schema.schemata" in sql:
                return []
            return [("app", "%")] if "current_user" in sql else [
                ("GRANT ALL PRIVILEGES ON *.* TO `app`@`%`",)]
        if "substring_index(current_user()" in sql:
            return [("app", "%")]
        if "show grants" in sql:
            return [("GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"
                     " WITH GRANT OPTION",)]
        if "current_role()" in sql:
            return [("NONE",)]
        if "variable_name in ('log_bin'" in sql:
            return [("log_bin", "ON"), ("binlog_format", "ROW"),
                    ("binlog_row_image", "FULL"),
                    ("binlog_expire_logs_seconds", "3600")]
        return self.server.ask(sql)


def _retention_need(server, cloud="aws"):
    from migkit.clouds import Where
    hop = build_hop("h", {"source": "mysql://app@m1/shop",
                          "target": "mysql://app@m2/shop"})
    found = dba.needs(hop, Listed(server), "cutover",
                      {"source": Where(cloud, "rds"),
                       "target": Where(cloud, "rds")})
    return next(n for n in found if n.what == "a log kept a day or more")


def test_the_dba_is_told_a_log_not_kept_on_a_managed_server():
    need = _retention_need(Server(config=("x", None, "")))
    assert need.ok is False and "not kept" in need.now
    need = _retention_need(Server(config=("x", 0, "")))
    assert need.ok is False


def test_the_dba_is_told_the_hours_and_unknown_is_not_ok_either_way():
    assert _retention_need(Server(config=("x", "48", ""))).ok is True
    need = _retention_need(Server(denied="(1370, \"denied\")"))
    assert need.ok is None and "could not read" in need.now
