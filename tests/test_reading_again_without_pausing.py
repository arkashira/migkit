"""A table the stream has to read again is read without pausing the stream,
where the source allows it.

The standard incremental re-read brackets each chunk with rows written to a
signalling table in the source, so migkit sent a blocking one instead and
the stream stopped while the table was read. MySQL with GTID gives the
connector its watermarks without a table. Measured on this pipeline, 50
rows, `read.only=true`, no signalling table anywhere:

    initial snapshot messages:        50
    after the incremental snapshot:   101   (50 again, 1 streamed update)
    id 49, changed before its chunk:  49 (snapshot) -> -1 (update) -> -1 (re-read)
    connector:                        RUNNING throughout

So the pipeline turns it on whenever the source runs GTID, and the repair
asks for the incremental kind wherever the pipeline was written with it.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop


def _hop(tmp_path, port=1):
    hop = Hop(name="ro", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=port, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="root",
                              password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _config(out):
    return json.loads((out / "source-connector.json").read_text())["config"]


def test_a_gtid_source_gets_the_read_only_re_read(tmp_path, monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: True)
    out = movers.stream_codegen(_hop(tmp_path), ["app"], "mysql")
    assert _config(out)["read.only"] == "true"
    assert movers.stream_reads_again_in_place(out)


def test_without_gtid_the_pipeline_is_what_it_was(tmp_path, monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: False)
    out = movers.stream_codegen(_hop(tmp_path), ["app"], "mysql")
    assert "read.only" not in _config(out)
    assert not movers.stream_reads_again_in_place(out)


def test_the_repair_asks_for_the_kind_the_pipeline_can_do(tmp_path,
                                                         monkeypatch):
    from migkit import movers
    from migkit.engines.base import RepairAction
    from migkit.engines.mysql import MySQLEngine
    sent = []
    monkeypatch.setattr(movers, "send_resnapshot",
                        lambda out, name, tables, kind="blocking", log=None:
                        sent.append(kind))
    for gtid in (True, False):
        monkeypatch.setattr(movers, "_gtid_on", lambda hop, g=gtid: g)
        eng = MySQLEngine(_hop(tmp_path))
        out = movers.stream_codegen(eng.hop, ["app"], "mysql")
        (out / "docker-compose.yml").write_text("services: {}\n")
        action = eng._resnapshot_action("app", "t", {})
        assert ("keeps running" in action.note) is gtid, action.note
        eng._apply_resnapshot(RepairAction("app.t", "resnapshot", [], [],
                                           ""))
    assert sent == ["incremental", "blocking"], sent


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.mark.docker
@pytest.mark.skipif(not _docker(), reason="docker not available")
@pytest.mark.parametrize("gtid", [True, False])
def test_the_source_is_asked_not_assumed(tmp_path, gtid):
    from migkit import movers
    name, port = "migkit-test-gtidq", 15694
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    args = ["--gtid-mode=ON", "--enforce-gtid-consistency=ON"] if gtid else []
    subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                    "mysql:8", *args], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(2)
        assert movers._gtid_on(_hop(tmp_path, port)) is gtid
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def test_a_source_that_cannot_be_asked_keeps_the_pause(tmp_path):
    """Pausing is slower, never wrong; guessing read-only on a source that
    cannot give watermarks would be."""
    from migkit import movers
    assert movers._gtid_on(_hop(tmp_path, port=1)) is False


# ---- PostgreSQL: the watermarks come from `pg_current_snapshot()` ----
#
# Measured with the connector shipped here (3.0.8) and PostgreSQL 16, no
# signalling table anywhere, 50 rows:
#     without read.only    Incremental snapshot is not properly configured
#     with read.only       will end at position [50] ... finished; 50 again
#     id 49, updated before the re-read: 49 -> -1 (stream) -> -1 (re-read)
#     connector            RUNNING throughout; no table written to the source


def _pg_hop(tmp_path, port=1):
    hop = Hop(name="ro", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.mark.parametrize("watermarks", [True, False])
def test_postgres_gets_the_read_only_re_read_where_it_can(
        tmp_path, monkeypatch, watermarks):
    from migkit import movers
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: watermarks)
    out = movers.stream_codegen(_pg_hop(tmp_path), ["postgres"], "postgres")
    assert (_config(out).get("read.only") == "true") is watermarks
    assert movers.stream_reads_again_in_place(out) is watermarks


def test_the_postgres_repair_asks_for_the_kind_the_pipeline_can_do(
        tmp_path, monkeypatch):
    from migkit import movers
    from migkit.engines.base import RepairAction
    from migkit.engines.postgres import PostgresEngine
    sent = []
    monkeypatch.setattr(movers, "send_resnapshot",
                        lambda out, name, tables, kind="blocking", log=None:
                        sent.append(kind))
    for watermarks in (True, False):
        monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                            lambda hop, db, w=watermarks: w)
        eng = PostgresEngine(_pg_hop(tmp_path))
        out = movers.stream_codegen(eng.hop, ["postgres"], "postgres")
        (out / "docker-compose.yml").write_text("services: {}\n")
        action = eng._resnapshot_action("postgres", "public.t", {})
        assert ("keeps running" in action.note) is watermarks, action.note
        eng._apply_resnapshot(RepairAction("postgres.public.t", "resnapshot",
                                           [], [], ""))
    assert sent == ["incremental", "blocking"], sent


@pytest.mark.docker
@pytest.mark.skipif(not _docker(), reason="docker not available")
def test_the_postgres_source_is_asked_not_assumed(pg_pair, tmp_path):
    from migkit import movers
    assert movers._pg_snapshot_watermarks(
        _pg_hop(tmp_path, pg_pair["src"]), "postgres") is True
    assert movers._pg_snapshot_watermarks(_pg_hop(tmp_path), "postgres") \
        is False
