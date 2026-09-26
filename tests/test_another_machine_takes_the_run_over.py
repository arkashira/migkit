"""A run's lease, checkpoints and change position in the state bucket,
where another machine can take the run over (backlog 30).

The bucket is S3-compatible object storage in a container here. The lease
is written only over the version that was read, the bucket's own
conditional write. So two machines deciding at once cannot both hold it,
and a holder that stopped renewing is taken over once its term lapses.
Checkpoints are kept there too: a second machine sees what the first had
copied and resumes from it.
"""
import socket
import sqlite3
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

S3 = "migkit-test-runstate-s3"
PORT = 15824
BUCKET = "migkit-state"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def bucket():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", S3], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", S3, "-e",
                    "MINIO_ROOT_USER=minioadmin", "-e",
                    "MINIO_ROOT_PASSWORD=minioadmin", "-p", f"{PORT}:9000",
                    "bitnamilegacy/minio:latest"], check=True,
                   capture_output=True)
    try:
        import boto3
        end = time.time() + 60
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        client = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT}",
                              aws_access_key_id="minioadmin",
                              aws_secret_access_key="minioadmin",
                              region_name="us-east-1")
        for _ in range(30):
            try:
                client.create_bucket(Bucket=BUCKET)
                break
            except Exception:
                time.sleep(1)
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", S3], capture_output=True)


@pytest.fixture
def machine(tmp_path, monkeypatch, bucket):
    """This process as one machine: its own report directory."""
    import migkit.config as cfg
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    def switch(name):
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / name / "reports")
    switch("a")
    return switch


STATE = {"backend": "s3", "bucket": BUCKET, "prefix": "t/",
         "endpoint_url": f"http://127.0.0.1:{PORT}"}


def _hop(name="run"):
    ep = Endpoint(host="x", user="x", password="x")
    return Hop(name=name, engine="sqlite", source=ep, target=ep,
               options={"state": dict(STATE)})


def _lease(hop):
    from migkit.lease import Lease
    from migkit.state import run_state
    return Lease(hop.report_dir() / "lease.json", remote=run_state(hop))


def test_one_holder_at_a_time_across_machines(machine):
    from migkit.lease import Held
    hop = _hop("one")
    first = _lease(hop).acquire()
    machine("b")
    with pytest.raises(Held) as e:
        _lease(hop).acquire()
    assert "another migkit" in str(e.value)
    first.release()
    _lease(hop).acquire().release()


def test_two_deciding_at_once_do_not_both_win(machine):
    from migkit.lease import Held
    hop = _hop("race")
    won, lost, go = [], [], threading.Barrier(8)

    def contend():
        lease = _lease(hop)
        go.wait()
        try:
            won.append(lease.acquire())
        except Held:
            lost.append(lease)
    threads = [threading.Thread(target=contend) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert (len(won), len(lost)) == (1, 7), (won, lost)
    won[0].release()


def test_a_holder_that_stopped_renewing_is_taken_over(machine, monkeypatch):
    monkeypatch.setenv("MIGKIT_LEASE_SECONDS", "1.5")
    hop = _hop("lapse")
    gone = _lease(hop).acquire()
    # the machine died: its heartbeat stops, nothing is released
    gone._stop.set()
    gone._beat.join()
    machine("b")
    time.sleep(2)
    took = _lease(hop).acquire()
    assert took.took_over and took.took_over["holder"] == gone.me
    # the one that was gone cannot remove what the new holder took
    gone.release()
    from migkit.state import run_state
    have, _ = run_state(hop).read_record(hop.report_dir() / "lease.json")
    assert have["holder"] == took.me, have
    took.release()


def test_a_checkpoint_is_where_the_next_machine_resumes(machine, bucket,
                                                         monkeypatch):
    from migkit.cli import _checkpoint
    monkeypatch.setenv("MIGKIT_STATE_KEY", "a passphrase")
    hop = _hop("ck")
    ck = _checkpoint(hop, hop.report_dir("db") / "move.json")
    ck["db.orders"] = {"last": ["customer-4411"], "moved": 5000}
    ck.save()
    raw = bucket.get_object(Bucket=BUCKET,
                            Key="t/ck/run/db/move.json")["Body"].read()
    # sealed: the last key copied is data, and the bucket is not only ours
    assert raw.startswith(b"MKS1") and b"customer-4411" not in raw
    machine("b")
    assert not (hop.report_dir("db") / "move.json").exists()
    again = _checkpoint(hop, hop.report_dir("db") / "move.json")
    assert again["db.orders"] == {"last": ["customer-4411"], "moved": 5000}
    again.discard()
    machine("c")
    assert not _checkpoint(hop, hop.report_dir("db") / "move.json")


def test_a_move_finished_on_one_machine_is_done_on_the_next(machine,
                                                           tmp_path,
                                                           monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    for name, rows in (("a.db", 3), ("b.db", 0)):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key)")
        con.executemany("insert into t values (?)",
                        [(i,) for i in range(rows)])
        con.commit()
        con.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        f"    options: {{state: {{backend: s3, bucket: {BUCKET},"
        f" prefix: t/, endpoint_url: 'http://127.0.0.1:{PORT}'}}}}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
    machine("b")
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full"])
    said = " ".join(got.output.split())
    assert "main: 1 tables, 1 already done in checkpoint" in said, said


def _two_machines(tmp_path, tables, rows, extra_env=None):
    """Two processes running the same move at once, each with a report
    directory of its own - two machines, as far as migkit can tell."""
    import os
    import sys
    src = sqlite3.connect(tmp_path / "a.db")
    for i in range(tables):
        src.execute(f"create table t{i} (id integer primary key, v text)")
        src.executemany(f"insert into t{i} values (?, ?)",
                        [(n, f"row {n}") for n in range(rows)])
    src.commit()
    src.close()
    dst = sqlite3.connect(tmp_path / "b.db")
    for i in range(tables):
        dst.execute(f"create table t{i} (id integer primary key, v text)")
    dst.commit()
    dst.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  shared:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        f"    options: {{share_tables: true, state: {{backend: s3, bucket:"
        f" {BUCKET}, prefix: {tmp_path.name}/, endpoint_url:"
        f" 'http://127.0.0.1:{PORT}'}}}}\n")
    runs = []
    for name in ("one", "two"):
        env = {**os.environ, "MIGKIT_CONF": str(conf),
               "MIGKIT_REPORTS": str(tmp_path / name),
               "AWS_ACCESS_KEY_ID": "minioadmin",
               "AWS_SECRET_ACCESS_KEY": "minioadmin",
               "AWS_DEFAULT_REGION": "us-east-1", **(extra_env or {})}
        runs.append(subprocess.Popen(
            [sys.executable, "-m", "migkit.cli", "move", "shared", "--mode",
             "full", "--go", "--chunk", "200"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True))
    said = [" ".join(r.communicate(timeout=300)[0].split()) for r in runs]
    return runs, said


def _copied(tmp_path, name):
    import json
    log = tmp_path / name / "shared" / "changelog.jsonl"
    return [json.loads(line)["table"] for line in log.read_text().splitlines()
            if json.loads(line).get("op") == "move"]


def test_two_machines_share_one_move(machine, tmp_path):
    runs, said = _two_machines(tmp_path, 6, 3000)
    assert [r.returncode for r in runs] == [0, 0], said
    one, two = _copied(tmp_path, "one"), _copied(tmp_path, "two")
    # every table copied, none twice
    assert sorted(one + two) == sorted(f".t{i}" for i in range(6)), said
    assert not set(one) & set(two), (one, two)
    assert one and two, ("each machine took some", one, two, said)
    # one of them completed the move; the other said so
    completed = [s for s in said if "main: move complete" in s]
    assert len(completed) == 1, said
    dst = sqlite3.connect(tmp_path / "b.db")
    for i in range(6):
        assert dst.execute(f"select count(*) from t{i}").fetchone()[0] == \
            3000
    dst.close()


def test_a_machine_that_dies_mid_table_is_taken_over(machine, tmp_path,
                                                     bucket):
    """The first machine is killed partway through the only table. The
    second takes the table over once the lease lapses, and carries on from
    the first machine's last saved chunk rather than emptying the table and
    starting again."""
    import os
    import signal
    import sys
    src = sqlite3.connect(tmp_path / "a.db")
    src.execute("create table big (id integer primary key, v text)")
    src.executemany("insert into big values (?, ?)",
                    [(n, f"row {n}") for n in range(20000)])
    src.commit()
    src.close()
    dst = sqlite3.connect(tmp_path / "b.db")
    dst.execute("create table big (id integer primary key, v text)")
    dst.commit()
    dst.close()
    conf = tmp_path / "hops.yaml"
    prefix = f"{tmp_path.name}/"
    conf.write_text(
        "hops:\n  shared:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        f"    options: {{share_tables: true, state: {{backend: s3, bucket:"
        f" {BUCKET}, prefix: {prefix}, endpoint_url:"
        f" 'http://127.0.0.1:{PORT}'}}}}\n")

    def start(name):
        env = {**os.environ, "MIGKIT_CONF": str(conf),
               "MIGKIT_REPORTS": str(tmp_path / name),
               "MIGKIT_LEASE_SECONDS": "3",
               "AWS_ACCESS_KEY_ID": "minioadmin",
               "AWS_SECRET_ACCESS_KEY": "minioadmin",
               "AWS_DEFAULT_REGION": "us-east-1"}
        return subprocess.Popen(
            [sys.executable, "-m", "migkit.cli", "move", "shared", "--mode",
             "full", "--go", "--chunk", "100"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True)
    import json
    first = start("one")
    key = f"{prefix}shared/run/main/move.json"
    end = time.time() + 120
    moved = 0
    while time.time() < end and moved < 1000:
        try:
            got = json.loads(bucket.get_object(Bucket=BUCKET, Key=key)
                             ["Body"].read())
            moved = int(got.get("main.big", {}).get("moved") or 0)
        except Exception:
            pass
        time.sleep(0.2)
    assert moved >= 1000, moved
    os.kill(first.pid, signal.SIGKILL)
    first.communicate()
    second = start("two")
    said = " ".join(second.communicate(timeout=300)[0].split())
    assert second.returncode == 0, said
    assert "main.big: taken over from" in said, said
    assert "emptied" not in said, said
    assert "main: move complete" in said, said
    dst = sqlite3.connect(tmp_path / "b.db")
    assert dst.execute("select count(*), count(distinct id) from big"
                       ).fetchone() == (20000, 20000)
    dst.close()
