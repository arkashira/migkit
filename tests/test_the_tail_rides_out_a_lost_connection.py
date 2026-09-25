"""A change tail rides out a connection it loses for a while (backlog 44).

Measured: the target paused for 30 seconds under a running tail, and the
tail ended on `timeout expired` for good - a tail meant to run for days,
ended by a blip. It now reads again from the last saved position once the
server answers, with a wait that doubles to a minute, and says so each
time. An error that is not about the connection still stops it
(`test_alerts_and_notifications.py`).
"""
import ctypes
import subprocess
import threading
import time

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def test_a_paused_target_does_not_end_the_tail(pg_pair, tmp_path,
                                               monkeypatch):
    import migkit.config as cfg
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for port in pg_pair.values():
        assert psql(port, "create table public.blip (id int primary key,"
                          " v text)").returncode == 0
    hop = Hop(name="blip", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              options={"source_engine": "postgres",
                       "target_engine": "postgres"})
    eng = HeteroEngine(hop)
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("postgres", True,
                           hop.report_dir("postgres") / "tail-token.json",
                           said.append)
        except BaseException as e:
            ended["e"] = e

    def rows():
        return psql(pg_pair["dst"], "select count(*) from public.blip"
                    ).stdout.strip()

    thread = threading.Thread(target=run, daemon=True)
    try:
        thread.start()
        time.sleep(4)
        psql(pg_pair["src"], "insert into public.blip select g, 'a'"
                             " from generate_series(1, 10) g")
        for _ in range(60):
            if rows() == "10":
                break
            time.sleep(0.5)
        assert rows() == "10", said
        subprocess.run(["docker", "pause", "migkit-test-pg-dst"], check=True)
        try:
            psql(pg_pair["src"], "insert into public.blip select g, 'b'"
                                 " from generate_series(11, 20) g")
            # paused until the tail has lost a connection to it: a fixed
            # wait was not always long enough on a loaded machine, and the
            # rows then went through after the pause as if nothing happened
            for _ in range(180):
                if any(m.startswith("lost a connection") for m in said):
                    break
                time.sleep(0.5)
            assert thread.is_alive(), ended
        finally:
            subprocess.run(["docker", "unpause", "migkit-test-pg-dst"],
                           check=True)
        for _ in range(180):
            if rows() == "20":
                break
            time.sleep(0.5)
        assert rows() == "20", said
        assert thread.is_alive(), ended
        lost = [m for m in said if m.startswith("lost a connection")]
        assert lost and "from the last saved position" in lost[0], said
        assert "port" in lost[0] and "test" not in lost[0], lost
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(timeout=60)
        psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name)"
                             " from pg_replication_slots where not active")
        for port in pg_pair.values():
            psql(port, "drop table if exists public.blip")
