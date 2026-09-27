"""A server migkit cannot reach directly, reached through a tunnel it
opens itself (`migkit.tunnel`): through an SSH bastion, and through any
forwarding program the endpoint names - the way an SSM session, an IAP
tunnel or a port-forward is started. The tunnel is up before anything
connects, started again if it dies, and closed when the run ends; one that
cannot come up says why before anything is copied.
"""
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
BASTION, SSH_PORT = "migkit-test-bastion", 15886

FORWARDER = r"""
import socket, sys, threading
listen, host, port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", listen)); srv.listen(64)
def pipe(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d: break
            b.sendall(d)
    except OSError: pass
    finally:
        for s in (a, b):
            try: s.shutdown(socket.SHUT_RDWR)
            except OSError: pass
while True:
    c, _ = srv.accept()
    u = socket.create_connection((host, port))
    threading.Thread(target=pipe, args=(c, u), daemon=True).start()
    threading.Thread(target=pipe, args=(u, c), daemon=True).start()
"""


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def bastion(tmp_path_factory):
    keys = tmp_path_factory.mktemp("keys")
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(keys / "id")], check=True)
    subprocess.run(["docker", "rm", "-f", BASTION], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", BASTION, "-e",
                    "USER_NAME=migkit", "-e",
                    f"PUBLIC_KEY={(keys / 'id.pub').read_text().strip()}",
                    "-p", f"{SSH_PORT}:2222",
                    "lscr.io/linuxserver/openssh-server:latest"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 60
        while time.time() < end:
            got = subprocess.run(["docker", "exec", BASTION, "test", "-f",
                                  "/config/sshd/sshd_config"])
            if got.returncode == 0:
                break
            time.sleep(1)
        # a bastion forwards: the image's own setting says it does not
        subprocess.run(["docker", "exec", BASTION, "sh", "-c",
                        "sed -i 's/^AllowTcpForwarding no/AllowTcpForwarding"
                        " yes/' /config/sshd/sshd_config && s6-svc -r"
                        " /run/service/svc-openssh-server"], check=True)
        time.sleep(3)
        yield keys
    finally:
        subprocess.run(["docker", "rm", "-f", BASTION], capture_output=True)


def _migkit(tmp, *args):
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               COLUMNS="200")
    return subprocess.run([sys.executable, "-c",
                           "from migkit.cli import main; main()", *args],
                          env=env, cwd=ROOT, capture_output=True, text=True,
                          timeout=600)


Q = ("select count(*) || ':' || md5(string_agg(id || '|' || payload, ','"
     " order by id)) from public.big")


def _seed(pg_pair):
    got = psql(pg_pair["src"], "drop table if exists public.big; create table"
                               " public.big (id bigint primary key, payload"
                               " text); insert into public.big select g,"
                               " repeat('y', 100) || g from generate_series(1,"
                               " 80000) g; analyze")
    assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "drop table if exists public.big")


def _hop(tmp, pg_pair, tunnel, workers=""):
    (tmp / "hops.yaml").write_text(
        "hops:\n  far:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target:\n      host: {_ip('migkit-test-pg-dst')}\n"
        "      port: 5432\n      user: postgres\n      password: test\n"
        f"      tunnel: {tunnel}\n    databases: [postgres]\n" + workers)


def _ssh(keys, key="id"):
    return (f"{{ssh: 127.0.0.1, port: {SSH_PORT}, user: migkit, key:"
            f" '{keys / key}', known_hosts: '{keys / 'known'}'}}")


#: the tunnel's ssh, by the end of its command line (a pattern that
#: starts with "-" is read by pgrep as one of its own options)
OURS = f"{SSH_PORT} migkit@127.0.0.1"


def _ours():
    return subprocess.run(["pgrep", "-f", OURS], capture_output=True,
                          text=True).stdout.split()


def test_a_move_through_an_ssh_bastion(pg_pair, bastion, tmp_path):
    _seed(pg_pair)
    _hop(tmp_path, pg_pair, _ssh(bastion))
    seen = []

    def watch():
        end = time.time() + 60
        while time.time() < end and not seen:
            seen.extend(_ours())
            time.sleep(0.1)
    threading.Thread(target=watch, daemon=True).start()
    got = _migkit(tmp_path, "move", "far", "--go", "--chunk", "20000")
    assert got.returncode == 0, got.stdout[-2500:] + got.stderr[-1500:]
    # the tunnel was there while it ran
    assert seen, "no tunnel was seen running"
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout
    # and nothing of it left running
    assert _ours() == [], _ours()


def test_a_tunnel_that_dies_mid_move_is_started_again(pg_pair, bastion,
                                                      tmp_path):
    _seed(pg_pair)
    _hop(tmp_path, pg_pair, _ssh(bastion), "    workers: 1\n")
    killed = {}

    def kill_once_rows_arrive():
        end = time.time() + 120
        while time.time() < end:
            # as soon as the copy has made its table through the tunnel
            got = psql(pg_pair["dst"], "select to_regclass('public.big')"
                       ).stdout.strip()
            if got and _ours():
                subprocess.run(["pkill", "-9", "-f", OURS])
                killed["at"] = time.time()
                return
            time.sleep(0.05)
    threading.Thread(target=kill_once_rows_arrive, daemon=True).start()
    first = _migkit(tmp_path, "move", "far", "--go", "--chunk", "10000")
    assert killed, "the move ended before the tunnel could be killed"
    if first.returncode != 0:
        again = _migkit(tmp_path, "move", "far", "--go", "--chunk", "10000")
        assert again.returncode == 0, again.stdout[-2500:]
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout
    assert _ours() == []


def test_a_forwarding_program_the_endpoint_names(pg_pair, tmp_path):
    _seed(pg_pair)
    script = tmp_path / "forward.py"
    script.write_text(FORWARDER)
    # the forwarder reaches the target as the far side of an SSM session
    # would: here, by the port the sandbox publishes
    cmd = (f"'{sys.executable} {script} {{local_port}} 127.0.0.1"
           f" {pg_pair['dst']}'")
    _hop(tmp_path, pg_pair, f"{{command: {cmd}}}")
    got = _migkit(tmp_path, "move", "far", "--go", "--chunk", "20000")
    assert got.returncode == 0, got.stdout[-2500:] + got.stderr[-1500:]
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout


def test_a_tunnel_that_cannot_come_up_says_why_first(pg_pair, bastion,
                                                     tmp_path):
    _seed(pg_pair)
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(tmp_path / "stranger")], check=True)
    (tmp_path / "stranger").rename(bastion / "stranger")
    _hop(tmp_path, pg_pair, _ssh(bastion, "stranger"))
    got = _migkit(tmp_path, "move", "far", "--go")
    said = got.stdout + got.stderr
    assert got.returncode != 0
    assert "the tunnel to the target" in said and "did not come up" in said
    assert "Permission denied" in said, said[-1500:]
    assert "Traceback" not in said


def test_a_far_bastion_is_crossed_by_several_connections(pg_pair, bastion,
                                                         monkeypatch):
    """One SSH connection moves at most its window (about 2 MB) a round
    trip, whatever the run's workers: 20 MB/s at 100 ms. Where the round
    trip to the bastion says so, several connections are opened and each
    new connection of the run goes through the next one."""
    from migkit import tunnel
    from migkit.config import Endpoint, Hop
    assert tunnel.legs_for(0.001, 8) == 1
    assert tunnel.legs_for(0.1, 8) == 5
    assert tunnel.legs_for(0.1, 3) == 3
    monkeypatch.setattr(tunnel, "_round_trip", lambda host, port: 0.1)
    _seed(pg_pair)
    spec = {"ssh": "127.0.0.1", "port": SSH_PORT, "user": "migkit",
            "key": str(bastion / "id"),
            "known_hosts": str(bastion / "known")}
    ep = Endpoint(host=_ip("migkit-test-pg-dst"), port=5432,
                  user="postgres", password="test",
                  options={"tunnel": spec})
    hop = Hop(name="far", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=ep, databases=["postgres"], workers=4)
    try:
        tunnel.through(hop)
        opened = tunnel._OPEN[id(ep)]
        assert len(opened.more) == 3 and len(_ours()) >= 4, _ours()
        import psycopg2
        for _ in range(8):
            conn = psycopg2.connect(host=ep.host, port=ep.port,
                                    user="postgres", password="test",
                                    dbname="postgres", connect_timeout=15)
            with conn.cursor() as cur:
                cur.execute("select 1")
                assert cur.fetchone() == (1,)
            conn.close()
        assert opened.splitter.carried == [2, 2, 2, 2], \
            opened.splitter.carried
    finally:
        tunnel.close_all()
    assert not _ours()


def test_rows_are_read_beside_the_source_and_cross_compressed(
        pg_pair, bastion, tmp_path, monkeypatch):
    """Where the source is reached through an ssh tunnel whose machine has
    psql and zstd, the rows are read there, next to the database, and
    cross the link once, compressed, inside the ssh connection - the
    password on the read's standard input, its end said back. Without them
    the rows cross as COPY's text, as before. Measured, 200,000 rows over a
    link held to 5 MB/s and 10 ms: 7.1s as text, 4.3s read beside the
    source."""
    from migkit import tunnel
    from migkit.cli import _Checkpoint
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    got = subprocess.run(["docker", "exec", BASTION, "apk", "add",
                          "--no-cache", "postgresql16-client", "zstd"],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    _seed(pg_pair)
    psql(pg_pair["dst"], "create table public.big (id bigint primary key,"
                         " payload text)")
    spec = {"ssh": "127.0.0.1", "port": SSH_PORT, "user": "migkit",
            "key": str(bastion / "id"),
            "known_hosts": str(bastion / "known")}
    src = Endpoint(host=_ip("migkit-test-pg-src"), port=5432,
                   user="postgres", password="test",
                   options={"tunnel": spec})
    hop = Hop(name="relay", engine="postgres", source=src,
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    relayed = []
    real = PostgresEngine._relay

    def spy(self, *a):
        got = real(self, *a)
        relayed.append(got is not None)
        return got
    monkeypatch.setattr(PostgresEngine, "_relay", spy)
    try:
        tunnel.through(hop)
        PostgresEngine(hop).move_table("postgres", "public", "big", 500_000,
                                       _Checkpoint(tmp_path / "m.json"),
                                       [].append)
    finally:
        tunnel.close_all()
    assert relayed and all(relayed), relayed
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout
