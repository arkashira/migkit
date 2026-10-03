"""The wrappers around the programs migkit drives, held to what the one
runner and the audit of 2026-10-01 ask of them (W9, W10, W16-W20, SEC1,
SEC2, SEC19, DU25, LG6): the hop's TLS on every connection a program makes,
no password where an API or a command line hands it out, file names that
stay inside the reports, the way that ran recorded as itself, paid work
taken up again rather than paid for twice, output read as it comes.
"""
import fnmatch
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from migkit import movers, runner
from migkit.config import Endpoint, Hop


def _pg_hop(tmp_path, tls=None, **kw):
    opts = dict(tls or {})
    hop = Hop(name="w", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=5432, user="app",
                              password="CHANGE_ME_src", options=dict(opts)),
              target=Endpoint(host="127.0.0.2", port=5432, user="app",
                              password="CHANGE_ME_dst", options=dict(opts)),
              databases=["appdb"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


# ---- DU25: the fast keyspace copy leaves out what the check leaves out ------

def _go_match(rx, key):
    """Whether the program's Go expression `rx` matches `key`: its `$`
    matches at the end of the text alone, as Python's `\\Z` does."""
    assert rx.endswith("$")
    return re.match(rx[:-1] + r"\Z", key) is not None


@pytest.mark.parametrize("pattern,key", [
    ("cache:*", "cache:a\nb"), ("*", "x\ny"), ("a?c", "a\nc"),
    ("s:[!x]*", "s:\n1"), ("[^a]x", "^x"), ("[^a]x", "ax"),
    ("[^a]x", "bx"), ("abc", "abc\n"), ("cache:*", "other\ncache:x"),
])
def test_a_key_is_left_out_exactly_as_the_hop_leaves_it_out(pattern, key):
    """Four keys holding a line break matched the hop's pattern and not
    the program's filter (measured), and were copied though the check left
    them out; a set starting `^` was a negated one. The filter now says
    what `fnmatch` says, both ways."""
    from migkit import redisshake
    rx = redisshake._glob_re(pattern)
    assert _go_match(rx, key) == fnmatch.fnmatchcase(key, pattern), \
        (pattern, key, rx)


def test_the_newline_keys_did_differ_before(monkeypatch):
    """The control: without the dot-all flag the line break is missed."""
    from migkit import redisshake
    rx = redisshake._glob_re("cache:*")
    assert rx.startswith("^(?s:")
    plain = "^" + rx[len("^(?s:"):-len(")$")] + "$"
    assert not _go_match(plain, "cache:a\nb")


def test_the_programs_own_log_says_what_went_wrong(tmp_path):
    from migkit import redisshake
    (tmp_path / "shake.log").write_text(
        "2026-10-01 INF read 10 keys\n"
        "2026-10-01 ERR redis-shake: write to 10.0.0.2:6379 failed:"
        " OOM command not allowed\n")
    said = redisshake._log_words(tmp_path)
    assert "OOM command not allowed" in said, said
    assert "redis-shake" not in said.lower(), said


# ---- SEC1: the pipeline's passwords, out of its API --------------------------

def _pipeline(tmp_path, monkeypatch, engine="postgres", tls=None):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: False)
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: False)
    monkeypatch.setattr(movers, "_stream_facts", lambda hop, dbs, my: (
        {d: (["t"] if my else ["public.t"]) for d in dbs}, 120.0, 900.0))
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    for cls in (MySQLEngine, PostgresEngine):
        monkeypatch.setattr(cls, "column_catalog",
                            lambda self, side, db: {"public.t": [("a", "t")],
                                                    "t": [("a", "t")]})
    hop = Hop(name="sp", engine=engine,
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME_src", options=dict(
                                  tls or {})),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="CHANGE_ME_dst", options=dict(
                                  tls or {})),
              databases=["appdb"])
    return movers.stream_codegen(hop, ["appdb"], engine)


def test_the_connectors_carry_no_password(tmp_path, monkeypatch):
    out = _pipeline(tmp_path, monkeypatch)
    for name in ("source-connector.json", "sink-connector.json"):
        text = (out / name).read_text()
        assert "CHANGE_ME" not in text, (name, text)
    src = json.loads((out / "source-connector.json").read_text())["config"]
    sink = json.loads((out / "sink-connector.json").read_text())["config"]
    assert src["database.password"] == "${env:MIGKIT_SOURCE_PASSWORD}"
    assert sink["connection.password"] == "${env:MIGKIT_TARGET_PASSWORD}"
    env = (out / "connect.env").read_text()
    assert "MIGKIT_SOURCE_PASSWORD='CHANGE_ME_src'" in env, env
    assert "MIGKIT_TARGET_PASSWORD='CHANGE_ME_dst'" in env, env
    assert (out / "connect.env").stat().st_mode & 0o777 == 0o600
    compose = (out / "docker-compose.yml").read_text()
    assert "env_file: [connect.env]" in compose
    assert "CONNECT_CONFIG_PROVIDERS: env" in compose
    assert movers.ENV_PROVIDER in compose


def test_an_env_file_value_is_taken_as_it_is_written():
    got = movers._env_file({"A": "p@ss w$rd", "B": "it's"})
    assert got == "A='p@ss w$rd'\nB=\"it's\"\n", got


# ---- SEC2: the hop's TLS on every connection a program makes -----------------

PG_TLS = {"sslmode": "verify-full", "sslrootcert": "/etc/ca.pem"}


def test_an_address_a_program_is_given_carries_the_hops_tls(tmp_path):
    hop = _pg_hop(tmp_path, PG_TLS)
    got = movers._pg_uri(hop.source, "appdb")
    assert got.startswith("postgresql://app@127.0.0.1:5432/appdb?"), got
    assert "sslmode=verify-full" in got and "sslrootcert=%2Fetc%2Fca.pem" \
        in got, got
    assert "CHANGE_ME" not in got
    secret = movers._pg_uri(hop.target, "appdb", secret=True,
                            quiet_triggers=True)
    assert "app:CHANGE_ME_dst@" in secret, secret
    assert "session_replication_role%3Dreplica" in secret, secret
    # the automatic answer: a server whose certificate verifies is checked
    plain = _pg_hop(tmp_path)
    from migkit import tls
    plain.source.host = "db.example.com"
    tls._SEEN[("db.example.com", 5432, "postgres", "")] = (
        "verified", "/sys/ca.pem")
    try:
        got = movers._pg_uri(plain.source, "appdb")
    finally:
        tls._SEEN.pop(("db.example.com", 5432, "postgres", ""), None)
    assert "sslmode=verify-full" in got and "sys%2Fca.pem" in got, got


def test_the_mysql_bulk_programs_take_the_automatic_answer(monkeypatch):
    ep = Endpoint(host="db.example.com", port=3306, user="u", password="p")
    from migkit import tls
    monkeypatch.setattr(tls, "auto", lambda ep, how, ca=None: "/sys/ca.pem")
    assert movers._my_tls(ep, "mydumper")[:4] == [
        "--ssl-mode", "VERIFY_IDENTITY", "--ca", "/sys/ca.pem"]
    assert movers._my_tls(ep, "mysqlsh")["ssl-ca"] == "/sys/ca.pem"
    monkeypatch.setattr(tls, "auto", lambda ep, how, ca=None: None)
    assert movers._my_tls(ep, "mydumper") == []


def test_the_pipeline_connects_with_the_hops_tls(tmp_path, monkeypatch):
    out = _pipeline(tmp_path, monkeypatch, tls=PG_TLS)
    src = json.loads((out / "source-connector.json").read_text())["config"]
    sink = json.loads((out / "sink-connector.json").read_text())["config"]
    assert src["database.sslmode"] == "verify-full"
    assert src["database.sslrootcert"] == "/etc/ca.pem"
    assert "sslmode=verify-full" in sink["connection.url"], sink
    compose = (out / "docker-compose.yml").read_text()
    assert '"/etc/ca.pem:/etc/ca.pem:ro"' in compose, compose
    # and with none asked of it, none is written
    bare = _pipeline(tmp_path / "bare", monkeypatch)
    src = json.loads((bare / "source-connector.json").read_text())["config"]
    assert not [k for k in src if k.startswith("database.ssl")], src


def test_a_way_that_cannot_check_a_certificate_is_not_taken(tmp_path,
                                                            monkeypatch):
    hop = _pg_hop(tmp_path, PG_TLS)
    hop.engine = "hetero"
    emptied = []
    monkeypatch.setattr(movers, "_pg_truncate_target",
                        lambda *a, **k: emptied.append(True))
    with pytest.raises(runner.Blocked) as e:
        movers.pgloader_move(hop, "appdb", 2, True, None)
    assert "certificates" in str(e.value), e.value
    assert not emptied, "the target was emptied for a way that cannot run"


# ---- SEC19: names from tables stay inside the reports ------------------------

def test_a_file_name_from_a_table_stays_where_it_is_put():
    from migkit.util import report_name
    for bad in ("../../etc/passwd", "a/b", "x\ny", ".hidden"):
        got = report_name(bad)
        assert "/" not in got and "\n" not in got and not \
            got.startswith("."), got
    assert report_name("a/b") != report_name("a_b")
    assert report_name("orders") == report_name("orders")


def test_a_table_the_filter_file_cannot_name_is_refused(tmp_path):
    hop = _pg_hop(tmp_path, exclude=["public.*x*"])
    with pytest.raises(runner.Blocked) as e:
        movers.pgcopydb_filters(hop, "appdb", ["public.x\n[include-only-table]",
                                               "public.t"])
    assert "line break" in str(e.value), e.value
    assert movers.pgcopydb_filters(hop, "appdb", ["public.xy", "public.t"]) \
        == "[exclude-table]\npublic.xy\n"


# ---- W10: the way that ran is recorded as itself -----------------------------

def test_a_streaming_copy_that_hands_its_work_on_says_the_way_that_ran(
        tmp_path, monkeypatch):
    hop = _pg_hop(tmp_path)
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_pgcopydb_options", lambda *sub: frozenset())
    monkeypatch.setattr(movers, "_SIZES", {"public.t": 1})
    monkeypatch.setattr(movers, "_pg_references_into",
                        lambda h, db, routed=(): ["public.child"])
    monkeypatch.setattr(movers, "_pg_missing_tables",
                        lambda h, db: ([], False))
    monkeypatch.setattr(movers, "pgdump_move",
                        lambda *a: ["# the dump and load"])
    got = movers.pgcopydb_move(hop, "appdb", 2, True, None)
    assert isinstance(got, movers.Ran) and got.via == "pgdump", got
    assert list(got) == ["# the dump and load"]
    # a way that ran as asked is itself
    assert movers.Ran(["x"], "pgcopydb").via == "pgcopydb"


# ---- W17: the flags, not a rehearsal of the whole path -----------------------

def test_the_measured_build_is_not_rehearsed(tmp_path, monkeypatch):
    hop = _pg_hop(tmp_path)
    rehearsed = []
    monkeypatch.setattr(movers, "which", lambda p: "/bin/" + p)
    monkeypatch.setattr(movers, "program_version",
                        lambda p: movers.MEASURED.get(p))
    monkeypatch.setattr(movers, "options_missing",
                        lambda h, d, v: rehearsed.append(v) or {})
    monkeypatch.setattr(movers, "_source_sizes", lambda h, d: {})
    monkeypatch.setattr(movers, "pgdump_move", lambda *a: ["# ran"])
    assert list(movers.run_via("pgdump", hop, "appdb", 1, True, None)) == \
        ["# ran"]
    assert rehearsed == []
    monkeypatch.setattr(movers, "program_version", lambda p: "9.6")
    movers.run_via("pgdump", hop, "appdb", 1, True, None)
    assert rehearsed == ["pgdump"]


# ---- LG6: a stream's readings are its own hop's -------------------------------

def test_the_readings_of_another_hops_stream_are_not_this_ones():
    asked = []
    beans = {
        "debezium.postgres:type=connector-metrics,context=streaming,"
        "server=migkit-a": {"MilliSecondsBehindSource": 1000,
                            "TotalNumberOfEventsSeen": 5},
        "debezium.postgres:type=connector-metrics,context=streaming,"
        "server=migkit-ab": {"MilliSecondsBehindSource": 90000,
                             "TotalNumberOfEventsSeen": 700},
    }

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            asked.append(self.path)
            body = json.dumps({"value": beans}).encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        got = movers.stream_readings(
            ("MilliSecondsBehindSource", "TotalNumberOfEventsSeen"),
            srv.server_port, server="migkit-a")
        everyone = movers.stream_readings(
            ("TotalNumberOfEventsSeen",), srv.server_port)
    finally:
        srv.shutdown()
        srv.server_close()
    assert got == {"MilliSecondsBehindSource": 1000,
                   "TotalNumberOfEventsSeen": 5}, got
    assert "server%3Dmigkit-a%2C" in asked[0], asked
    assert everyone == {"TotalNumberOfEventsSeen": 705}


# ---- W16: paid work taken up again, not paid for twice ----------------------------

class _Exports:
    """A stand-in for the service's export API: what it was asked, and an
    export that finishes on the second look."""

    def __init__(self):
        self.asked, self.looks = [], 0

    def describe_table(self, TableName):
        return {"Table": {"TableArn": f"arn:table/{TableName}",
                          "TableSizeBytes": 10}}

    def export_table_to_point_in_time(self, **kw):
        self.asked.append(kw)
        return {"ExportDescription": {"ExportArn": "arn:export/1"}}

    def describe_export(self, ExportArn):
        self.looks += 1
        state = "COMPLETED" if self.looks > 1 else "IN_PROGRESS"
        return {"ExportDescription": {"ExportArn": ExportArn,
                                      "ExportStatus": state,
                                      "ItemCount": 3}}


def _ddb(tmp_path, client):
    import types
    hop = Hop(name="ddb", engine="dynamodb", source=Endpoint(),
              target=Endpoint(), databases=["app"],
              options={"export_bucket": "s3://exports/migkit"})
    hop.report_dir = lambda db=None: tmp_path
    return types.SimpleNamespace(hop=hop, _client=lambda side: client,
                                 _name=lambda side, db, t: f"{db}.{t}")


def test_an_export_is_asked_once_and_taken_up_again(tmp_path, monkeypatch):
    from migkit import ddbexport
    monkeypatch.setattr(ddbexport.time, "sleep", lambda s: None)
    client = _Exports()
    eng = _ddb(tmp_path, client)

    class Stop(Exception):
        pass
    real = ddbexport._wait

    def stopped(*a, **k):
        raise Stop()
    monkeypatch.setattr(ddbexport, "_wait", stopped)
    with pytest.raises(Stop):
        ddbexport.export(eng, "app", "orders", None)
    (asked,) = client.asked
    token = asked["ClientToken"]
    assert len(token) == 36 and asked["S3Prefix"].startswith(
        "migkit/migkit-ddb-app-orders-"), asked
    kept = json.loads((tmp_path / ddbexport.EXPORTS).read_text())
    assert kept["orders"]["arn"] == "arn:export/1", kept
    assert kept["orders"]["token"] == token
    # the next run waits for that export: no second one is paid for
    monkeypatch.setattr(ddbexport, "_wait", real)
    done, place = ddbexport.export(eng, "app", "orders", None)
    assert len(client.asked) == 1, client.asked
    assert done["ExportStatus"] == "COMPLETED"
    assert place == asked["S3Prefix"]


def test_a_finished_copy_forgets_its_export(tmp_path, monkeypatch):
    from migkit import ddbexport
    monkeypatch.setattr(ddbexport.time, "sleep", lambda s: None)
    client = _Exports()
    eng = _ddb(tmp_path, client)
    forgot = []
    monkeypatch.setattr(ddbexport, "forget",
                        lambda eng, place: forgot.append(place))
    monkeypatch.setattr(ddbexport, "load", lambda *a: 3)
    assert ddbexport.carry(eng, "app", "orders", False, None) == 3
    assert forgot and json.loads(
        (tmp_path / ddbexport.EXPORTS).read_text()) == {}


def _snapshot_eng(tmp_path, shards):
    import types
    calls = []
    hop = Hop(name="os", engine="opensearch", source=Endpoint(),
              target=Endpoint(), databases=["c"],
              options={})
    hop.report_dir = lambda db=None: tmp_path

    def call(side, method, path, body=None, missing_ok=False):
        calls.append((side, method, path))
        if "_cat/shards" in path:
            return [{"prirep": "p", "store": "10"} for _ in range(shards)]
        if path.endswith("/_count"):
            return {"count": 7}
        return {}
    return types.SimpleNamespace(hop=hop, _call=call,
                                 _index=lambda side, db, t: t), calls


def test_the_snapshot_readers_run_side_by_side(tmp_path, monkeypatch):
    from migkit import fromsnapshot
    starts = tmp_path / "starts"
    prog = tmp_path / "reader"
    prog.write_text(f"#!/bin/sh\necho x >> {starts}\nsleep 0.5\nexit 3\n")
    prog.chmod(0o755)
    monkeypatch.setattr(fromsnapshot, "program", lambda: ([str(prog)], {}))
    monkeypatch.setattr(fromsnapshot, "args",
                        lambda eng, db, t, w, lucene, s3: ["--index", t])
    eng, calls = _snapshot_eng(tmp_path, shards=3)
    assert fromsnapshot.copy_index(eng, "c", "logs", 8, None) == 7
    assert starts.read_text().count("x") == 3
    # done: the reader's working index goes
    assert ("dst", "DELETE", f"/{fromsnapshot.WORKING}") in calls


def test_a_stopped_snapshot_copy_keeps_the_work_it_did(tmp_path,
                                                       monkeypatch):
    from migkit import fromsnapshot
    prog = tmp_path / "reader"
    prog.write_text("#!/bin/sh\necho 'lease lost: the target refused'"
                    " >&2\nexit 1\n")
    prog.chmod(0o755)
    monkeypatch.setattr(fromsnapshot, "program", lambda: ([str(prog)], {}))
    monkeypatch.setattr(fromsnapshot, "args",
                        lambda eng, db, t, w, lucene, s3: ["--index", t])
    eng, calls = _snapshot_eng(tmp_path, shards=2)
    with pytest.raises(runner.Failed) as e:
        fromsnapshot.copy_index(eng, "c", "logs", 2, None)
    assert "the target refused" in str(e.value), e.value
    # what the next run takes up is still there
    assert not [c for c in calls if c[1] == "DELETE"], calls


# ---- W9/W20: the mirror's end and its stall, said one way ---------------------

def test_a_mirror_that_ends_first_is_said_in_its_words(monkeypatch):
    from migkit import mirror

    class Ended:
        returncode = 1

        def poll(self):
            return 1

    class Said:
        lines = ["ERROR org.apache.kafka: authentication failed for user\n"]
        thread = threading.Thread(target=lambda: None)
    Said.thread.start()
    with pytest.raises(runner.Refused) as e:
        mirror._wait(None, Ended(), {}, 0, 0, None, Said(), None)
    assert "authentication failed" in str(e.value), e.value


def test_a_mirror_that_carries_nothing_is_ended_as_stalled(monkeypatch):
    from migkit import mirror, stall

    class Running:
        def poll(self):
            return None
    monkeypatch.setattr(stall, "seconds", lambda: 0.5)
    monkeypatch.setattr(mirror, "read_table", lambda eng, name: {})
    monkeypatch.setattr(mirror.time, "sleep", lambda s: None)
    with pytest.raises(stall.Stalled) as e:
        mirror._wait(None, Running(), {("t", 0): (0, 10, 9)}, 0, 10, None,
                     None, lambda *a, **k: "")
    assert "t[0]" in str(e.value), e.value


# ---- W20: the second reading's answer on a stream of its own -----------------

def test_the_second_readers_answer_is_read_past_what_it_prints(tmp_path):
    from migkit import second_reader
    python = tmp_path / "python"
    python.write_text("#!/bin/sh\ncat > /dev/null\n"
                      "echo 'a library says hello' >&2\n"
                      "echo '{\"ok\": true, \"results\": []}'\n")
    python.chmod(0o755)
    assert second_reader.run({"validations": []}, python=str(python)) == {
        "ok": True, "results": []}
    broken = tmp_path / "broken"
    broken.write_text("#!/bin/sh\ncat > /dev/null\n"
                      "echo 'data_validation: no module named ibis' >&2\n"
                      "echo 'not an answer'\nexit 1\n")
    broken.chmod(0o755)
    got = second_reader.run({"validations": [], "source": {
        "password": "CHANGE_ME"}}, python=str(broken))
    assert got["ok"] is False
    assert "no module named" in got["error"], got
    assert "data_validation" not in got["error"], got


def test_the_second_reader_sends_everything_else_to_the_other_stream():
    src = (movers.__file__.rsplit("/", 1)[0] + "/runners/second_reader.py")
    text = open(src).read()
    assert "os.dup2(2, 1)" in text and "sys.stdout = sys.stderr" in text
    assert os.path.exists(src)
