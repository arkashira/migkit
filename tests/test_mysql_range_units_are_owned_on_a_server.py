"""Every range MySQL's range copier moves is a unit of the attempt ledger
(`ranges.each_owned`, backlog P0-ETA-ALL, P0-CONNECTION-PROGRESS): opened
before its load, the target session it runs on recorded off the connection
itself, closed only once its checkpoint holds it - and a range whose
connection the server kills is told stalled, tried again on a new session,
the target holding every row once. The range is held mid-load by a key
written and not committed on the target, never by a sleep. Beside it, on
fakes: a unit is refused a second statement, a stall's reason names the
idle seconds and what the server lists, a move's forecast reads the ranges
run, and the deep check's ways are costed and keep the ranges they prove."""
import glob
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from migkit import attempts, ranges
from migkit.attempts import Ledger, Monitor, View
from migkit.durable import read_state
from tests.conftest import needs_docker

ROOT = Path(__file__).resolve().parent.parent
DDL = "create table mk.t (id bigint primary key, payload text)"
Q = ("select concat(count(*), ':', md5(group_concat(concat(id, '|',"
     " payload) order by id separator ','))) from mk.t")


def _ranges(tmp):
    out = {}
    for path in glob.glob(str(tmp / "reports" / "**" / "attempts*.json"),
                          recursive=True):
        for name, rec in ((read_state(path, "state") or {}).get("units")
                          or {}).items():
            if rec.get("level") == "range" and rec.get("kind") == "move":
                out[name] = rec
    return out


@needs_docker
def test_a_killed_range_connection_is_told_stalled_and_replayed(tmp_path):
    from tests import mysql_pair
    from tests.mysql_pair import my
    with mysql_pair.servers.space(mysql_pair.PROFILE, __name__,
                                  own=False) as got:
        src, dst = got.src, got.dst
        for side in ("src", "dst"):
            my(side, "set session group_concat_max_len = 4294967295;"
                     " drop database if exists mk; create database mk;"
                     f" {DDL};")
        my("src", "insert into mk.t with recursive g(n) as (select 1 union"
                  " all select n + 1 from g where n < 30000) select n,"
                  " concat('row-', n) from g")
        (tmp_path / "hops.yaml").write_text(
            "hops:\n  owned:\n    engine: mysql\n"
            f"    source: {{host: 127.0.0.1, port: {src.port},"
            " user: root, password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {dst.port},"
            " user: root, password: test}\n"
            "    databases: [mk]\n    workers: 1\n"
            "    options:\n      target_rows: replace\n")
        holder = subprocess.Popen(
            ["docker", "exec", "-i", dst.container, "mysql", "-uroot",
             "-ptest", "mk"], stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        holder.stdin.write("begin; insert into t values (15000, 'held');"
                           " select sleep(600);\n")
        holder.stdin.flush()
        time.sleep(1)
        env = dict(os.environ, MIGKIT_CONF=str(tmp_path / "hops.yaml"),
                   MIGKIT_REPORTS=str(tmp_path / "reports"),
                   MIGKIT_MOVER="builtin", COLUMNS="200")
        env.pop("MIGKIT_FAILPOINT", None)
        move = subprocess.Popen(
            [sys.executable, "-c", "from migkit.cli import main; main()",
             "move", "owned", "--go", "--chunk", "10000"], env=env,
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True)
        try:
            end, held = time.time() + 120, None
            while time.time() < end and move.poll() is None and not held:
                held = next((n for n, r in sorted(_ranges(tmp_path).items())
                             if r.get("state") == "open"
                             and r.get("sessions")
                             and r.get("started") and len(
                                 _ranges(tmp_path)) >= 2), None)
                time.sleep(0.2)
            assert held, (_ranges(tmp_path), move.poll())
            side, conn = _ranges(tmp_path)[held]["sessions"][-1]
            assert side == "dst"
            my("dst", f"kill connection {int(conn)}")
        finally:
            holder.kill()
            holder.wait()
        out, _ = move.communicate(timeout=600)
        assert move.returncode == 0, out[-3000:]
        units = _ranges(tmp_path)
        assert len(units) == 3, units
        assert all(r["state"] == "done" and r["sessions"]
                   and r["statement"].startswith("load ")
                   for r in units.values()), units
        two = units[held]
        assert two.get("stalls") == 1 and "no longer lists" in two["stalled"]
        assert two["sessions"][0] == ["dst", str(conn)]
        assert len(two["sessions"]) >= 2, two
        assert all(not r.get("stalls") and len(r["sessions"]) == 1
                   for n, r in units.items() if n != held), units
        assert "stalled on the target's session" in out
        a, b = my("src", Q), my("dst", Q)
        assert a == b and a.startswith("30000:")
        for side in ("src", "dst"):
            my(side, "drop database if exists mk")


def test_a_unit_is_refused_a_second_statement():
    led = Ledger(None)
    name = ranges.unit_of("mk.t", 10000)
    led.unit_opened(name, "move", "range", "mk.t", 3)
    led.unit_statement(name, "load a1", "dst", 7)
    # its retry, on a new session, is the same statement
    led.unit_statement(name, "load a1", "dst", 9)
    assert led.unit_record(name)["sessions"] == [["dst", "7"], ["dst", "9"]]
    with pytest.raises(attempts.Spanning):
        led.unit_statement(name, "load b2", "dst", 9)
    assert led.unit_record(name)["statement"] == "load a1"


class _Server:
    """A server listing one session as `view`, the hop's account's."""

    SESSION_VIEW = "the process list"

    def __init__(self, view):
        self.view, self.closed = view, []

    def session_view(self, side, session):
        return self.view

    def owns_session(self, side, view):
        return True

    def close_session(self, side, session, view):
        self.closed.append(session)
        return True


def test_a_stall_is_decided_from_idle_seconds_and_the_servers_listing():
    led, said = Ledger(None), []
    name = ranges.unit_of("mk.t", 10000)
    led.unit_opened(name, "move", "range", "mk.t", 3)
    led.unit_statement(name, "load a1", "dst", 7)
    srv = _Server(View(7, "root", "idle", 40.0))
    got = Monitor(srv, led, say=said.append).unit_health(
        name, keepalive=10.0, observed="the server's idle setting")
    assert got.path == "close" and srv.closed == ["7"]
    assert "idle 40.0s" in got.reason
    assert "the server lists the session" in got.reason
    assert led.unit_record(name)["stalls"] == 1
    assert led.unit_record(name)["keepalive"] == {
        "seconds": 10.0, "from": "the server's idle setting"}
    # one the server no longer lists is replayed, nothing closed
    srv2 = _Server(View(7, present=False))
    got = Monitor(srv2, led, say=said.append).unit_health(name,
                                                          keepalive=10.0)
    assert got.path == "replay" and srv2.closed == []
    assert "the server no longer lists the session" in got.reason


def _engine(tmp_path, monkeypatch):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setenv("MIGKIT_REPORTS", str(tmp_path))
    ep = Endpoint(host="10.0.0.1", port=3306, user="u", password="CHANGE_ME")
    return MySQLEngine(Hop(name="h", engine="mysql", source=ep, target=ep,
                           databases=["mk"]))


def test_the_forecast_reads_the_ranges_run_where_there_are_any(
        tmp_path, monkeypatch):
    from migkit import util
    eng = _engine(tmp_path, monkeypatch)
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["t", "u"])
    monkeypatch.setattr(eng, "in_scope", lambda db, t: True)
    monkeypatch.setattr(eng, "_rows_of", lambda side, db, t: 1000)
    seen = []
    real = util.forecast

    def caught(units, **kw):
        seen.append([u.name for u in units])
        return real(units, **kw)
    monkeypatch.setattr(util, "forecast", caught)
    eng.forecast("move", "mk")
    # no range run: each table is its one unit
    assert seen[-1] == ["t", "u"]
    led = Ledger(None)
    eng.__dict__["_attempts"] = Monitor(eng, led)
    first = ranges.unit_of("t", 0)
    led.unit_opened(first, "move", "range", "t", 3, at=100.0)
    led.unit_closed(first, True, at=102.0)
    got = eng.forecast("move", "mk")
    assert seen[-1] == [first, "t owed 0", "t owed 1", "u"]


def test_the_deep_check_is_costed_and_keeps_each_range_it_proves(
        tmp_path, monkeypatch):
    from migkit import decide, scanplan
    eng = _engine(tmp_path, monkeypatch)
    monkeypatch.setattr(eng, "_row_expr", lambda db, t: "x")
    monkeypatch.setattr(eng, "_pk_cols", lambda db, t: ["id"])
    monkeypatch.setattr(eng, "_checksum_ranges",
                        lambda db, t, pks: ([(None, None)], None))
    seen = {}

    def caught(work, run, facts=None, **kw):
        seen.update(kw)
        raise LookupError("seen")
    monkeypatch.setattr(decide, "row_parity", caught)
    with pytest.raises(LookupError):
        eng._diff_table("mk", "t")
    assert isinstance(seen["costs"], decide.Layered)
    texts = {}

    def kept(read, preds, **kw):
        texts.update(kw)
        raise LookupError("seen")
    monkeypatch.setattr(scanplan, "plan_most", lambda *a, **k: 1)
    monkeypatch.setattr(scanplan, "row_text", kept)
    with pytest.raises(LookupError):
        eng._row_text_table("mk", "t", ["id"], "x", [(None, None)], None)
    assert texts["kept"] is eng.row_texts("mk")
    assert isinstance(texts["kept"], scanplan.RangeTexts)
    assert texts["table"] == "mk.t" and texts["eng"] is eng
