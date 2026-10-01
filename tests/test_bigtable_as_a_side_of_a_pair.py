"""Google Cloud Bigtable as a side of a pair (backlog P0-CLOUDS), against
its emulator (the Cloud SDK's, `gcloud beta emulators bigtable`), on this
machine only.

A table copied cell for cell - every version with its timestamp, a
qualifier that is not text - and compared; a version added on the target
is a difference. A SQLite table copied into Bigtable is described on this
machine and read back by its columns' classes. The emulator serves no
change stream: its follow is proved against recorded answers
(`test_the_cloud_native_services_as_sides`).
"""
import datetime
import sqlite3
import time


import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

EMU, PORT = "migkit-test-bigtable", 16451
IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
PROJECT = "migkit-test"
UTC = datetime.timezone.utc


def _client():
    from google.cloud import bigtable

    from migkit.engines import gcp
    return gcp.client(bigtable.Client, "BIGTABLE_EMULATOR_HOST",
                      Endpoint(host=PROJECT, options={
                          "emulator": f"127.0.0.1:{PORT}"}),
                      project=PROJECT, admin=True)


@pytest.fixture(scope="module")
def emulator():
    import subprocess
    subprocess.run(["docker", "rm", "-f", "-v", EMU], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", EMU, "-p",
                    f"127.0.0.1:{PORT}:8086", IMAGE, "gcloud", "beta",
                    "emulators", "bigtable", "start",
                    "--host-port=0.0.0.0:8086"], check=True,
                   capture_output=True)
    try:
        client = _client()
        end = time.time() + 180
        ready = False
        while time.time() < end and not ready:
            try:
                list(client.instance("probe").list_tables())
                ready = True
            except Exception:
                time.sleep(2)
        assert ready
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", EMU],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _ep():
    return Endpoint(host=PROJECT, options={"emulator": f"127.0.0.1:{PORT}"})


def _engine(tmp_path):
    from migkit.engines.bigtable import BigtableEngine
    hop = Hop(name="bt", engine="bigtable", source=_ep(), target=_ep(),
              databases=["src"], db_map={"src": "dst"})
    hop.report_dir = lambda db=None: tmp_path
    return BigtableEngine(hop)


def _at(ms):
    return datetime.datetime(2026, 10, 1, tzinfo=UTC) + \
        datetime.timedelta(milliseconds=ms)


@pytest.fixture
def events(emulator):
    from google.cloud.bigtable import column_family
    for inst in ("src", "dst"):
        for t in emulator.instance(inst).list_tables():
            t.delete()
    table = emulator.instance("src").table("events")
    table.create(column_families={
        "a": column_family.MaxVersionsGCRule(3),
        "b": column_family.MaxAgeGCRule(datetime.timedelta(days=30))})
    rows = []
    for i in range(12):
        row = table.direct_row(f"row{i:03d}".encode())
        row.set_cell("a", b"x", f"v{i}-old".encode(), timestamp=_at(1))
        row.set_cell("a", b"x", f"v{i}-new".encode(), timestamp=_at(2))
        row.set_cell("b", b"\xff\x00", bytes([i]), timestamp=_at(3))
        rows.append(row)
    for status in table.mutate_rows(rows):
        assert not status.code
    return table


def _move(eng, tmp_path, db="src"):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing(db, lambda m: None)
    for sch, t in eng.list_move_tables(db):
        eng.move_table(db, sch, t, 5, ck, lambda m: None)


def _data(eng, db="src"):
    return {r.scope: r.status for r in eng.check_data(db)
            if r.check == "data"}


def test_a_table_is_copied_cell_for_cell_every_version_kept(
        events, emulator, tmp_path):
    eng = _engine(tmp_path)
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    copy = emulator.instance("dst").table("events")
    row = copy.read_row(b"row004")
    versions = [(c.value, c.timestamp_micros) for c in row.cells["a"][b"x"]]
    assert versions == [(b"v4-new", 1790812800002000),
                        (b"v4-old", 1790812800001000)]
    params = {r.scope: r.status for r in eng.check_params("src")}
    assert set(params.values()) == {"ok"}, params
    # one version more on the target, the value the same as the newest:
    # a different row
    extra = copy.direct_row(b"row004")
    extra.set_cell("a", b"x", b"v4-new", timestamp=_at(5))
    assert not copy.mutate_rows([extra])[0].code
    got = _data(_engine(tmp_path))
    assert list(got.values()) == ["diff"], got


def test_a_table_is_digested_by_its_own_key_ranges(events, tmp_path):
    eng = _engine(tmp_path)
    cols = [(n, c) for n, c in eng._classes("src", "src", "events").items()]
    whole = eng.neutral_digest("src", "src", "events", cols)
    assert whole[0] == 12
    row = events.direct_row(b"row007")
    row.set_cell("b", b"\xff\x00", b"\x63", timestamp=_at(3))
    assert not events.mutate_rows([row])[0].code
    assert eng.neutral_digest("src", "src", "events", cols) != whole


def test_another_engines_table_is_described_and_read_back_by_class(
        emulator, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    for t in emulator.instance("dst").list_tables():
        t.delete()
    src = tmp_path / "s.db"
    con = sqlite3.connect(src)
    con.execute("create table accounts (id integer primary key, name text,"
                " balance numeric(12, 2), opened text, raw blob)")
    con.executemany("insert into accounts values (?, ?, ?, ?, ?)",
                    [(i, f"n{i}", f"{i}.50", f"2026-01-0{i % 9 + 1}",
                      bytes([i, 255])) for i in range(1, 9)])
    con.commit()
    con.close()
    hop = Hop(name="lite", engine="hetero",
              source=Endpoint(host=str(src)), target=_ep(),
              databases=["main"], db_map={"main": "dst"},
              options={"source_engine": "sqlite",
                       "target_engine": "bigtable"})
    hop.report_dir = lambda db=None: tmp_path
    pair = HeteroEngine(hop)
    _move(pair, tmp_path, "main")
    got = {r.scope: r.status for r in pair.check_data("main")
           if r.check == "data"}
    assert got and set(got.values()) == {"ok"}, got
    cols = pair.dst_engine.neutral_columns("dst", "main", "accounts")
    rows, _ = pair.dst_engine.neutral_read("dst", "main", "accounts", cols,
                                           None, 3)
    names = [n for n, _ in cols]
    first = dict(zip(names, rows[0]))
    assert first["id"] == 1 and first["raw"] == bytes([1, 255])
    assert isinstance(first["id"], int) and first["name"] == "n1"
    # a value changed in a described cell is a difference
    table = emulator.instance("dst").table("accounts")
    row = table.direct_row(b"2")
    row.set_cell("c", b"name", b"changed", timestamp=_at(0))
    assert not table.mutate_rows([row])[0].code
    got = {r.scope: r.status for r in HeteroEngine(hop).check_data("main")
           if r.check == "data"}
    assert "diff" in got.values(), got
