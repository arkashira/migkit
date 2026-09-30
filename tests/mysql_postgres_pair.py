"""A MySQL 8.4 and a PostgreSQL 16 in containers, for the cross-engine
table copies (`migkit.crossmove`): started once a test module, removed
after it, on the host ports given to these tests."""
import socket
import subprocess
import time

MY, PG = "migkit-test-xe-my", "migkit-test-xe-pg"
MY_PORT, PG_PORT = 16190, 16191


def docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def my_sql(sql, db="cx"):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


def ok(got):
    assert got.returncode == 0, got.stderr
    return got.stdout


def down():
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def up():
    down()
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MY_PORT}:3306", "mysql:8", "--local-infile=1"],
                   check=True, capture_output=True)
    assert _wait(PG_PORT) and _wait(MY_PORT)
    for _ in range(60):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("postgres never answered")
    for _ in range(90):
        if my_sql("select 1", None).returncode == 0:
            break
        time.sleep(2)
    else:
        raise AssertionError("mysql never answered")


def fresh(db="cx"):
    """`db` empty on both servers."""
    ok(pg_sql(f"drop database if exists {db}", "postgres"))
    ok(pg_sql(f"create database {db}", "postgres"))
    ok(my_sql(f"drop database if exists {db}; create database {db}", None))


def hop(tmp_path, src="mysql", dst="postgres", workers=2, db="cx",
        mapping=None, **opts):
    from migkit.config import Endpoint, Hop
    ends = {"mysql": Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
            "postgres": Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test")}
    h = Hop(name="xe", engine="hetero", source=ends[src], target=ends[dst],
            db_map={db: db}, workers=workers, mapping=mapping or {},
            options={"source_engine": src, "target_engine": dst, **opts})
    h.report_dir = lambda d=None, _p=tmp_path: _p
    return h


class Checkpoint(dict):
    """The shape a move hands the copier: a dict that saves itself."""

    def save(self):
        pass
