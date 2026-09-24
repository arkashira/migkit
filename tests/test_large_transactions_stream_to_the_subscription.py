"""A transaction larger than the source's decoding memory reaches migkit's
subscription while it runs, instead of through the source's disk.

Measured on 16 with the source's `logical_decoding_work_mem` at 64kB and
20,000 rows in one transaction: the subscription migkit made spilled
3,460,000 bytes to the source's disk and sent them at commit. With
`streaming = parallel` nothing spilled and the same bytes were streamed.
The option is set from the two servers' versions, read at the time.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine
from tests.conftest import psql


def _engine(src=55432, dst=55433):
    hop = Hop(name="big", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=src, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=dst, user="postgres",
                              password="test"),
              databases=["postgres"])
    return PostgresEngine(hop)


@pytest.mark.parametrize("src, dst, option", [
    (160000, 160000, ", streaming = parallel);"),
    (140000, 160000, ", streaming = parallel);"),
    (160000, 150000, ", streaming = on);"),
    (140000, 140000, ", streaming = on);"),
    # the source cannot send a transaction before it commits until 14
    (130000, 160000, None),
    (160000, 130000, None),
    # a side that cannot be asked leaves the server's own default
    (None, 160000, None),
    (160000, None, None),
])
def test_the_option_follows_both_servers(monkeypatch, src, dst, option):
    versions = {"src": src, "dst": dst}
    monkeypatch.setattr(PostgresEngine, "_server_version",
                        lambda self, side, db: versions[side])
    stmt = _engine().replicate_sql("postgres", copy_data=False)["dst"][0]
    if option:
        assert stmt.endswith(option), stmt
    else:
        assert "streaming" not in stmt, stmt
        assert stmt.endswith("with (copy_data = false);"), stmt


def test_a_server_that_is_not_there_is_asked_once_and_briefly():
    eng = _engine(src=1, dst=1)
    t0 = time.monotonic()
    assert eng._server_version("src", "postgres") is None
    assert time.monotonic() - t0 < 10
    assert "streaming" not in eng.replicate_sql("postgres")["dst"][0]


def _one(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def test_a_large_transaction_is_streamed_not_spilled(pg_pair):
    eng = _engine(pg_pair["src"], pg_pair["dst"])
    assert eng._server_version("src", "postgres") >= 160000
    ip = subprocess.run(["docker", "inspect", "-f",
                         "{{range .NetworkSettings.Networks}}"
                         "{{.IPAddress}}{{end}}",
                         "migkit-test-pg-src"],
                        capture_output=True, text=True).stdout.strip()
    assert ip, "the source container has no bridge address"
    name = eng._repl_name()
    sql = eng.replicate_sql("postgres", copy_data=False)
    # the address migkit reaches the source on is not the one the target's
    # container does; only that part of the generated statement is changed
    sub = sql["dst"][0].replace(
        f"host=127.0.0.1 port={pg_pair['src']}", f"host={ip} port=5432")
    assert sub != sql["dst"][0] and "streaming = parallel" in sub, sub
    for port in pg_pair.values():
        _one(port, "create table big (id int primary key, pad text)")
    _one(pg_pair["src"], "alter system set logical_decoding_work_mem ="
                         " '64kB'")
    _one(pg_pair["src"], "select pg_reload_conf()")
    try:
        for stmt in sql["src"]:
            eng.apply_replication_stmt("src", "postgres", stmt)
        eng.apply_replication_stmt("dst", "postgres", sub)
        assert _one(pg_pair["dst"], "select substream from pg_subscription"
                                    f" where subname = '{name}'") == "p"
        for _ in range(60):
            if _one(pg_pair["src"], "select count(*) from"
                                    " pg_replication_slots where slot_name ="
                                    f" '{name}' and active") == "1":
                break
            time.sleep(1)
        else:
            pytest.fail("the subscription never started")
        _one(pg_pair["src"], f"select pg_stat_reset_replication_slot('{name}')")
        _one(pg_pair["src"], "insert into big select g, repeat('x', 100)"
                             " from generate_series(1, 20000) g")
        for _ in range(90):
            if _one(pg_pair["dst"], "select count(*) from big") == "20000":
                break
            time.sleep(1)
        else:
            pytest.fail("the rows never arrived")
        spilled, streamed = _one(
            pg_pair["src"], "select spill_txns, stream_txns from"
                            " pg_stat_replication_slots where slot_name ="
                            f" '{name}'").split("|")
        assert int(streamed) >= 1, (spilled, streamed)
        assert int(spilled) == 0, (spilled, streamed)
    finally:
        psql(pg_pair["dst"], f"drop subscription if exists {name}")
        psql(pg_pair["src"], f"drop publication if exists {name}")
        psql(pg_pair["src"], "alter system reset logical_decoding_work_mem")
        psql(pg_pair["src"], "select pg_reload_conf()")
        for port in pg_pair.values():
            psql(port, "drop table if exists big")
