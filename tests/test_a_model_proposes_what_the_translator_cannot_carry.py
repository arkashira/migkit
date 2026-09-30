"""A model's proposal for code the translator cannot carry, held to the
same proof as anything converted (backlog 43).

A MySQL function whose body is statements has no one-expression form for
the translator. With a provider configured, and `MIGKIT_AI_SHARE`
including `code`, its definition goes to the provider, and never a row.
The statement that comes back is marked as a proposal in the converted
file. Once it is on the target, `check` asks it the same inputs as the
source's function. The provider here is a local stand-in. It proposes
one function correctly and gets one wrong, and the proof names the
wrong one.
"""
import json
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

MY, MY_PORT = "migkit-test-code-propose-my", 16086

PROPOSALS = {
    "steps": "```sql\nCREATE FUNCTION steps(x integer) RETURNS integer"
             " LANGUAGE plpgsql AS $$ DECLARE y integer; BEGIN y := x + 1;"
             " RETURN y; END $$;\n```",
    # doubles where the source's function doubles and adds one
    "twice": "CREATE FUNCTION twice(x integer) RETURNS integer LANGUAGE sql"
             " AS $$ SELECT x * 2 $$",
}


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture
def model(monkeypatch):
    asked = []

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            prompt = json.loads(self.rfile.read(n))["messages"][0]["content"]
            asked.append(prompt)
            name = next(k for k in PROPOSALS if f", {k}," in prompt)
            out = json.dumps({"choices": [{"message": {
                "content": PROPOSALS[name]}}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)
    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "stand-in")
    yield asked
    server.shutdown()
    server.server_close()


@pytest.fixture(scope="module")
def mysql():
    import pymysql
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "create database if not exists cx"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        with socket.socket() as s:
            s.settimeout(5)
            assert s.connect_ex(("127.0.0.1", MY_PORT)) == 0
        conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                               password="test", database="cx",
                               autocommit=True)
        with conn.cursor() as cur:
            cur.execute("create table t (id int primary key)")
            cur.execute("create function steps(x int) returns int"
                        " deterministic begin declare y int;"
                        " set y = x + 1; return y; end")
            cur.execute("create function twice(x int) returns int"
                        " deterministic begin declare y int;"
                        " set y = x * 2 + 1; return y; end")
        conn.close()
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _verdicts(tmp_path):
    got = json.loads((tmp_path / "code-proof.json").read_text())
    return {v["routine"]: v for v in got["verdicts"]}


def _pair(pg_port, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="prop", engine="hetero",
              options={"source_engine": "mysql",
                       "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_port,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_without_leave_to_send_code_nothing_is_sent(mysql, pg_pair, model,
                                                     tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_AI_SHARE", "detail")
    got = {n: s for n, s, _ in _pair(pg_pair["dst"],
                                     tmp_path).converted_code("cx")}
    assert got["steps"].startswith("-- function steps not converted"), got
    assert model == [], model


def test_proposals_are_marked_and_proven(mysql, pg_pair, model, tmp_path,
                                         monkeypatch):
    monkeypatch.setenv("MIGKIT_AI_SHARE", "code")
    port = pg_pair["dst"]
    psql(port, "drop database if exists cx")
    assert psql(port, "create database cx").returncode == 0
    eng = _pair(port, tmp_path)
    got = {n: s for n, s, _ in eng.converted_code("cx")}
    for name in ("steps", "twice"):
        assert got[name].endswith(eng.PROPOSED), got[name]
    # what went: the definition, from the source's own text
    assert any("CREATE DEFINER" in p and "FUNCTION `steps`" in p
               for p in model), model
    for name in ("steps", "twice"):
        eng.dst_engine.neutral_create_code("dst", "cx", got[name])
    proved = eng.prove_converted("cx")
    assert proved.status == "diff", proved.__dict__
    by = _verdicts(tmp_path)
    assert by["function twice"]["status"] == "diff", by
    assert by["function twice"]["reason"].startswith("called with (x=0):"), \
        by
    psql(port, "create or replace function twice(x integer) returns integer"
               " language sql as $$ select x * 2 + 1 $$", db="cx")
    proved = eng.prove_converted("cx")
    # both now answer every call the same; bodies of statements, asked on
    # the hop's own servers with no sandbox named, are not measured for
    # which of their statements ran - so not `ok`, and said why
    assert proved.status == "warn", proved.__dict__
    by = _verdicts(tmp_path)
    for name in ("function steps", "function twice"):
        assert by[name]["status"] == "warn", by
        assert "answered the same" in by[name]["reason"], by
