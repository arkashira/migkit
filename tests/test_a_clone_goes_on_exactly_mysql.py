"""A MySQL target that clones itself from the source (`CLONE INSTANCE
FROM`), then carried on by migkit's tail from the binlog position the
clone records - proved against the source's GTIDs before the tail trusts
it - with a writer on the source throughout: during the clone, while the
target restarts into it, while the tail catches up.

Every row the writer committed arrives, and the rows on both sides are
the same, table by table; migkit's own check agrees, and says "different"
once a row is changed on the target alone. The clone's position is proved
exact by GTID: nothing the source's log holds after it is in the clone,
and the clone and that log together hold everything the source ran.
"""
import socket
import subprocess
import threading
import time

import pytest
from click.testing import CliRunner

NET = "migkit-test-phys-mynet"
SRC, DST = "migkit-test-phys-mysrc", "migkit-test-phys-mydst"
SRC_PORT, DST_PORT = 16122, 16123
ARGS = ["--gtid-mode=ON", "--enforce-gtid-consistency=ON",
        "--binlog-row-metadata=FULL", "--plugin-load-add=mysql_clone.so"]


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(name, sql, db=""):
    cmd = ["docker", "exec", "-i", name, "mysql", "-uroot", "-ptest", "-N",
           "-B"] + (["-D", db] if db else [])
    got = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, port, most=180):
    end = time.time() + most
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                             "select 1"], capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


def _rm():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                   capture_output=True)


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    _rm()
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name, port, sid in ((SRC, SRC_PORT, 61), (DST, DST_PORT, 62)):
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:3306", "mysql:8.4",
                        f"--server-id={sid}"] + ARGS,
                       check=True, capture_output=True)
    try:
        _ready(SRC, SRC_PORT)
        _ready(DST, DST_PORT)
        my(SRC, "set session cte_max_recursion_depth = 100000;"
                " create database app;"
                " create table app.t (id bigint primary key, v varchar(64),"
                " n int not null default 0, key (v));"
                " create table app.log (seq bigint auto_increment primary"
                " key, id bigint not null);"
                " insert into app.t (id, v) with recursive g(i) as (select 1"
                " union all select i + 1 from g where i < 50000)"
                " select i, md5(i) from g;"
                " insert into app.log (id) with recursive g(i) as (select 1"
                " union all select i + 1 from g where i < 1000) select i"
                " from g")
        yield
    finally:
        _rm()
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


@pytest.fixture(scope="module")
def hop(pair, tmp_path_factory):
    import migkit.config as cfg
    tmp_path = tmp_path_factory.mktemp("clone")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  myseed:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: root,"
        " password: test}\n"
        "    databases: [app]\n"
        "    options:\n      physical:\n"
        f"        source: {SRC}:3306\n"
        f"        restart: docker restart {DST}\n")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cfg, "CONF", str(conf))
        mp.setattr(cfg, "REPORTS", tmp_path / "reports")
        from migkit.config import get_hop
        yield get_hop("myseed")


class _Writer(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.stop, self.ids, self.error = threading.Event(), [], None

    def run(self):
        import pymysql
        try:
            conn = pymysql.connect(host="127.0.0.1", port=SRC_PORT,
                                   user="root", password="test",
                                   database="app", autocommit=False)
            i = 100000
            with conn.cursor() as cur:
                while not self.stop.is_set():
                    i += 1
                    cur.execute("insert into log (id) values (%s)", (i,))
                    cur.execute("update t set n = n + 1, v = concat(left(v,"
                                " 40), 'u') where id = %s", (i % 5000 + 1,))
                    cur.execute("insert into t (id, v) values (%s, 'new')",
                                (1_000_000 + i,))
                    conn.commit()
                    self.ids.append(i)
            conn.close()
        except Exception as e:  # noqa: BLE001 - asserted by the test
            self.error = e


def test_a_target_that_is_not_empty_is_refused(hop):
    from migkit import physical
    from migkit.engines import get_engine
    my(DST, "create database stray; create table stray.x (id int primary"
            " key)")
    try:
        rung, why = physical.choose(hop, get_engine(hop), "full+cdc")
        assert rung is None and "the target holds 1 tables (stray.x)" in \
            why, why
    finally:
        my(DST, "drop database stray")


def test_cloned_and_carried_on_exactly(hop, monkeypatch):
    from migkit import cli, physical
    from migkit.engines import get_engine
    from migkit.engines.hetero import HeteroEngine
    writer = _Writer()
    real_tail = HeteroEngine.tail_apply

    def tail_until_quiet(self, db, go, token_path, log):
        engine = self.src_engine
        real_changes = type(engine).neutral_changes

        def changes(side, db, token=None, limit=1000):
            got, token = real_changes(engine, side, db, token, limit)
            if not got and writer.stop.is_set():
                raise KeyboardInterrupt
            return got, token
        monkeypatch.setattr(engine, "neutral_changes", changes)
        # the writer goes on for a while after the tail has started
        threading.Timer(6, writer.stop.set).start()
        return real_tail(self, db, go, token_path, log)
    monkeypatch.setattr(HeteroEngine, "tail_apply", tail_until_quiet)
    writer.start()
    time.sleep(2)
    got = CliRunner().invoke(cli.main, ["move", "myseed", "--mode",
                                        "full+cdc", "--db", "app", "--go"])
    out = " ".join(got.output.split())
    writer.stop.set()
    writer.join(30)
    assert got.exit_code == 0, out + str(got.exception or "")
    assert writer.error is None, writer.error
    assert "the target cloning itself from the source" in out, out
    assert "proved against the source's executed set" in out, out
    rec = physical.seeded(hop)
    assert rec["rung"] == "physical-clone" and rec["log_pos"] > 0, rec
    # a write before the tail's position and after it: both arrive, once
    want = 1000 + len(writer.ids)
    assert len(writer.ids) > 20, len(writer.ids)
    for side in (SRC, DST):
        count, distinct = my(side, "select count(*), count(distinct id)"
                                   " from app.log").split()
        assert (int(count), int(distinct)) == (want, want), (side, count)
    same = ("set session group_concat_max_len = 1073741824; select"
            " md5(group_concat(concat(id, ':', v, ':', n) order by id"
            " separator ',')) from app.t")
    assert my(SRC, same) == my(DST, same)
    # migkit's own check, table by table
    eng = get_engine(hop)
    data = [r for r in eng.check_data("app") if r.check == "data"]
    assert sorted((r.scope, r.status) for r in data) == [
        ("app.log", "ok"), ("app.t", "ok")], [(r.scope, r.status, r.detail)
                                             for r in data]
    my(DST, "update app.t set v = 'changed on the target' where id = 7")
    data = {r.scope: r.status for r in eng.check_data("app")
            if r.check == "data"}
    assert data == {"app.log": "ok", "app.t": "diff"}, data


def test_the_clone_position_would_be_refused_if_it_were_not_exact(hop):
    """The proof is the one the move ran, asked of a position one
    transaction too early: the transaction it would read again is named."""
    from migkit import physical
    from migkit.engines import get_engine
    eng = get_engine(hop)
    rec = physical.seeded(hop)
    token = {"log_file": rec["log_file"], "log_pos": rec["log_pos"]}
    assert physical.position_proof(eng, token, rec["gtids"]) is None
    # the clone's set without its last transaction: that one is in the
    # log after the position and nowhere else, so it would be missed
    have = physical.gtid_set(rec["gtids"])
    key = next(iter(have))
    last = have[key][-1][1]
    fewer = physical.gtid_minus(have, {key: [(last, last)]})
    why = physical.position_proof(eng, token, fewer)
    assert why and f"{key}:{last}" in why and "missed" in why, why
