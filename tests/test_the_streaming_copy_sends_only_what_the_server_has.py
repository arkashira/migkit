"""The streaming copy and the range copier send a server only the session
settings it has (W15J-PG16): the target's `server_version_num` and the
build's version are facts, and `decide.session_settings` sends them all,
leaves out the ones the server lacks, or takes the range copier instead.

Measured before: the restore built for PostgreSQL 18 opened the schema
step with `SET transaction_timeout = 0`, the 16 target refused it, and
the copy of the pair stopped - the streaming copy, then the range copier
after its range, both on that one statement."""
import re
import subprocess
import time
from types import SimpleNamespace

import pytest

from migkit import decide, movers, toolversion, util
from migkit.config import Endpoint, Hop
from tests.conftest import _pg_container, needs_docker, psql

TIMEOUT = ("transaction_timeout", 170000)


def _choice(server, build=18, can_drop=True):
    return decide.session_settings({"server": server, "build": build,
                                    "sends": (TIMEOUT,),
                                    "can drop": can_drop})


def test_a_server_that_has_the_setting_is_sent_it():
    got = _choice(170000)
    assert got.path == "send"
    assert "170000" in got.reason and "18" in got.reason


def test_a_server_without_it_has_it_left_out_where_the_program_can_be_told():
    got = _choice(160000)
    assert got.path == "drop"
    assert "lacks transaction_timeout" in got.reason
    assert "160000" in got.reason and "18" in got.reason
    # the send way was passed over, and said why
    assert any(r.name == "send" and "160000" in why
               for r, why in got.passed)


def test_a_program_that_cannot_be_told_gives_way_to_the_range_copier():
    got = _choice(160000, can_drop=False)
    assert got.path == "copier"
    assert "cannot be told" in " ".join(why for _, why in got.passed)


def test_a_server_not_read_is_not_taken_for_one_that_has_it():
    assert _choice(None).path == "drop"


def test_the_build_is_read_from_what_it_says():
    assert toolversion.compiled_for(
        "pgcopydb version 0.18\ncompiled with PostgreSQL 18.4 on aarch64",
        "pgcopydb") == 18
    assert toolversion.compiled_for("pg_restore (PostgreSQL) 18.6",
                                    "pg_restore") == 18


def test_the_text_loses_only_the_settings_the_server_lacks():
    text = ("SET statement_timeout = 0;\nSET transaction_timeout = 0;\n"
            "SELECT pg_catalog.set_config('transaction_timeout', '0', false);"
            "\nSET lock_timeout = 0;\nCREATE TABLE t (transaction_timeout"
            " int);\n")
    got = movers._without_settings(text, ["transaction_timeout"])
    assert "SET transaction_timeout" not in got
    assert "set_config('transaction_timeout'" not in got
    assert "SET statement_timeout = 0;" in got
    assert "SET lock_timeout = 0;" in got
    assert "CREATE TABLE t (transaction_timeout int);" in got


def test_the_range_copier_session_carries_no_such_setting():
    from migkit.engines import postgres
    assert "transaction_timeout" not in postgres.SESSION
    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["app"])
    _, env = postgres.PostgresEngine(hop)._client("dst", "app", "psql")
    assert "transaction_timeout" not in env["PGOPTIONS"]


def test_the_move_says_its_forecast_with_a_band():
    eta = util.ETA(seconds=42.0, lower_bound=30.0, upper_bound=61.0,
                   confidence=0.8, formatted="about 42s", basis="measured")
    line = movers._forecast_said(None, "app", eta)
    assert line == ("forecast: p50 42s - p90 61s (measured,"
                    " confidence 80%)")
    none = util.ETA(seconds=None, lower_bound=None, upper_bound=None,
                    confidence=0.0, formatted="not estimated - nothing"
                    " measured yet", basis="insufficient")
    assert movers._forecast_said(None, "app", none).startswith(
        "forecast: no band yet - not estimated")


def test_the_move_reads_the_forecast_from_the_engine(monkeypatch):
    seen = []

    class Engine:
        def forecast(self, kind, db):
            seen.append((kind, db))
            return util.ETA(seconds=5.0, lower_bound=4.0, upper_bound=9.0,
                            confidence=0.5, formatted="", basis="measured")
    monkeypatch.setattr("migkit.engines.get_engine", lambda hop: Engine())
    assert movers._forecast_said(SimpleNamespace(), "app") == (
        "forecast: p50 5s - p90 9s (measured, confidence 50%)")
    assert seen == [("move", "app")]


# ---- on the PostgreSQL 16 sandbox --------------------------------------------

TABLES = ("a", "b")


def _hop(pg_pair, tmp_path):
    hop = Hop(name="w16k", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _digest(port, table):
    return psql(port, f"select count(*) || ':' || coalesce(md5(string_agg("
                      f"id || '|' || v, ',' order by id)), '') from {table}"
                ).stdout.strip()


def _refused_settings(port, since):
    logs = subprocess.run(["docker", "logs", "--since", since,
                           _pg_container(port)],
                          capture_output=True, text=True)
    return [x for x in (logs.stdout + logs.stderr).splitlines()
            if "unrecognized configuration parameter" in x]


@needs_docker
@pytest.mark.skipif(movers.pgcopydb_runner() != "local",
                    reason="the streaming copy's local build is not"
                           " installed here")
def test_a_pair_on_16_copies_through_the_streaming_copy(pg_pair, tmp_path):
    src, dst = pg_pair["src"], pg_pair["dst"]
    for port in (src, dst):
        psql(port, "drop table if exists a, b cascade")
    # the target lacks both tables: the schema step makes them, then their
    # keys after the rows - the restore that sent the setting, both times
    psql(src, "create table a (id int primary key, v text);"
              " create table b (id int primary key, v text);"
              " insert into a select g, 'a' || g from generate_series(1, 5000) g;"
              " insert into b select g, md5(g::text) from"
              " generate_series(1, 3000) g; analyze")
    hop = _hop(pg_pair, tmp_path)
    assert movers._settings_choice(hop, "postgres", "pg_restore",
                                   True)[0].path == "drop"
    since = time.strftime("%Y-%m-%dT%H:%M:%S",
                          time.gmtime(time.time() - 1))
    said = []
    movers.pgcopydb_move(hop, "postgres", 2, True, said.append)
    for t in TABLES:
        assert _digest(src, t) == _digest(dst, t), t
    assert _digest(dst, "a").startswith("5000:")
    assert _digest(dst, "b").startswith("3000:")
    # the keys of the tables it made came after the rows
    assert psql(dst, "select count(*) from pg_constraint where contype = 'p'"
                     " and conrelid in ('a'::regclass, 'b'::regclass)"
                ).stdout.strip() == "2"
    time.sleep(1)
    assert _refused_settings(dst, since) == []
    assert any("loaded without transaction_timeout" in str(x) for x in said)
    # the facts the choice stood on: the server's 160000, the build's 18
    choice, lacks = movers._settings_choice(hop, "postgres", "pg_restore",
                                            True)
    assert lacks == ["transaction_timeout"]
    # measured as the server says it: 16.x is 1600xx
    assert re.search(r"the server is 1600\d\d,", choice.reason), choice.reason
    assert "compiled for 18" in choice.reason
    gate, _ = movers._settings_choice(
        hop, "postgres", "pgcopydb", False,
        said=movers._pgcopydb_said("local"))
    assert gate.path == "send" and "compiled for 18" in gate.reason
    for port in (src, dst):
        psql(port, "drop table if exists a, b cascade")
