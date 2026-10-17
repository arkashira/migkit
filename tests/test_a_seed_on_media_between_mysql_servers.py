"""A seed shipped on media between two MySQL servers: where the target
already has the tables, a table with an integer key goes as ranges of the
bulk load's lines - rendered by the source under one consistent snapshot a
range, its digest line first - and each is loaded by the target's own bulk
load in one transaction, committed only where the target's digest of the
range is the manifest's; a part changed on the way is read again from the
source over the link.

Two servers in containers of this test's own, on its lane's private
ports, removed after.
"""
import socket
import subprocess
import time

import pytest

from migkit import carriers, ship
from migkit.config import Endpoint, Hop
from migkit.util import which
from tests import servers
from tests.conftest import needs_docker

SRC = servers.name("migkit-ship-my-src", 7)
DST = servers.name("migkit-ship-my-dst", 7)
PORT = {SRC: servers.port(servers.PRIVATE + 0, 7),
        DST: servers.port(servers.PRIVATE + 1, 7)}

TABLE = ("create table orders (id bigint primary key, item varchar(40)"
         " not null, qty int, at datetime(6), note varbinary(16),"
         " amount decimal(12,2))")


def _my(name, sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, most=300):
    end = time.time() + most
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            up = s.connect_ex(("127.0.0.1", PORT[name])) == 0
        if up and subprocess.run(
                ["docker", "exec", name, "mysql", "-uroot", "-ptest",
                 "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                capture_output=True).returncode == 0:
            return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture
def pair():
    subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                   capture_output=True)
    try:
        for name in (SRC, DST):
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{PORT[name]}:3306", "mysql:8.4",
                            "--local-infile=1"],
                           check=True, capture_output=True)
        for name in (SRC, DST):
            _ready(name)
            _my(name, "create database app", "mysql")
            _my(name, TABLE)
        _my(SRC, "set session cte_max_recursion_depth = 20000;"
                 "insert into orders with recursive g(n) as (select 1 union"
                 " all select n + 1 from g where n < 12000) select n,"
                 " concat('item-', n), n % 7, timestamp('2026-10-01') +"
                 " interval n minute, unhex(md5(n)), n / 3 from g")
        _my(SRC, "analyze table orders")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", SRC, DST],
                       capture_output=True)


@pytest.fixture
def hop(pair, tmp_path, monkeypatch):
    import pyrage
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    monkeypatch.setattr(ship, "PART_BYTES", 64 * 1024)
    monkeypatch.setattr(ship._Work, "workers", lambda self, units: 1)
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    got = Hop(name="myseed", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=PORT[SRC], user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT[DST], user="root",
                              password="test"),
              databases=["app"],
              options={"at_rest": {"recipients": [str(who.to_public())]},
                       "target_rows": "replace",
                       "ship": {"carrier": "disk",
                                "path": str(tmp_path / "carrier"),
                                "shipping": "1d"}})
    got.report_dir = report_dir
    return got


def _carry(hop):
    from migkit.engines.mysql import MySQLEngine
    said = []
    got = ship.carry(hop, MySQLEngine(hop), "app", "full", True, said.append,
                     via="builtin")
    return got, said


def _same():
    sql = ("select count(*), md5(group_concat(concat_ws('|', id, item, qty,"
           " at, hex(note), amount) order by id separator ',')) from orders")
    return _my(SRC, "set session group_concat_max_len = 1073741824;" + sql) \
        == _my(DST, "set session group_concat_max_len = 1073741824;" + sql)


@needs_docker
@pytest.mark.skipif(not which("mysql"),
                    reason="the source's own client is not installed here")
def test_ranges_go_as_the_bulk_loads_lines_and_are_proved_by_the_target_lane_7(
        hop):
    got, said = _carry(hop)
    assert got["app"].half == "export", said
    from migkit.engines.mysql import MySQLEngine
    store = carriers.LocalStore(hop.options["ship"]["path"])
    w = ship._Work(hop, MySQLEngine(hop), "app", "full", lambda m: None)
    manifest = ship.read_manifest(w, store, ship.seal_at(store, hop, "app"))
    (orders,) = manifest["tables"]
    assert (orders["cut"], orders["format"]) == ("ranges", "load lines")
    assert len(orders["parts"]) > 2
    # nothing is on the target yet
    assert _my(DST, "select count(*) from orders") == "0"
    got, said = _carry(hop)
    assert got["app"].half == "load", said
    assert _same()
    # a part changed on the way is read again from the source, and the
    # target ends as the source is
    part = orders["parts"]["1"]
    path = store.local(part["name"])
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF
    path.write_bytes(bytes(raw))
    # loaded again from its start, as a far end with no record of it would
    (hop.report_dir("app") / ship.LOAD_STATE).unlink()
    got, said = _carry(hop)
    assert any("read again from the source over the link" in m
               for m in said), said
    assert _same()


@needs_docker
@pytest.mark.skipif(not which("mysql"),
                    reason="the source's own client is not installed here")
def test_the_export_reads_nothing_of_the_target_and_the_schema_travels_lane_7(
        hop):
    """The target is down, and its table not there, while the seed is
    exported beside the source: the lines' columns come from the source
    alone and travel in the manifest, and the load beside the target holds
    its table to them."""
    from migkit.engines.mysql import MySQLEngine
    _my(DST, "drop table orders")
    subprocess.run(["docker", "stop", DST], check=True, capture_output=True)
    try:
        got, said = _carry(hop)
        assert got["app"].half == "export", said
    finally:
        subprocess.run(["docker", "start", DST], check=True,
                       capture_output=True)
        _ready(DST)
    store = carriers.LocalStore(hop.options["ship"]["path"])
    w = ship._Work(hop, MySQLEngine(hop), "app", "full", lambda m: None)
    manifest = ship.read_manifest(w, store, ship.seal_at(store, hop, "app"))
    (orders,) = manifest["tables"]
    assert orders["format"] == "load lines", orders["because"]
    assert orders["load"]["types"]["amount"] == "decimal"
    assert orders["load"]["names"] == ["id", "item", "qty", "at", "note",
                                       "amount"]
    got, said = _carry(hop)
    assert got["app"].half == "load", said
    assert _same()
    # a target that cannot take the lines is refused by name, not loaded
    # wrongly: its column is another type than the manifest's
    _my(DST, "drop table orders")
    _my(DST, "create table orders (id bigint primary key, item varchar(40),"
             " qty int, at datetime(6), note varbinary(16), amount text)")
    (hop.report_dir("app") / ship.LOAD_STATE).unlink()
    with pytest.raises(SystemExit) as e:
        _carry(hop)
    assert "amount" in str(e.value), e.value


def test_decide_reads_parquet_read_from_capability_table():
    from migkit import decide
    assert hasattr(decide, "TARGET_CAPABILITIES")
    assert hasattr(decide, "READS_PARQUET")
    expected = tuple(k for k, v in decide.TARGET_CAPABILITIES.items()
                     if v.get("reads_parquet"))
    assert decide.READS_PARQUET == expected
    assert set(decide.READS_PARQUET) == {
        "parquet", "iceberg", "delta", "duckdb", "clickhouse",
        "bigquery", "redshift"
    }


def test_object_store_gather_across_moto_azurite_and_fake_gcs(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace
    import boto3
    from moto import mock_aws
    from azure.core.exceptions import ResourceNotFoundError
    import azure.storage.blob
    import google.cloud.storage

    class FakeBlobClient:
        def __init__(self, data):
            self._data = data

        def get_blob_properties(self):
            return SimpleNamespace(size=len(self._data))

    class FakeBlobDownload:
        def __init__(self, data):
            self._data = data

        def readall(self):
            return self._data

        def readinto(self, f):
            f.write(self._data)
            return len(self._data)

    class FakeContainerClient:
        def __init__(self):
            self.blobs = {}

        def upload_blob(self, name, data, overwrite=True, **kw):
            if hasattr(data, "read"):
                self.blobs[name] = data.read()
            else:
                self.blobs[name] = bytes(data)

        def download_blob(self, name, offset=None, length=None, **kw):
            if name not in self.blobs:
                raise ResourceNotFoundError(name)
            d = self.blobs[name]
            if offset is not None:
                d = d[offset: offset + length if length is not None else None]
            return FakeBlobDownload(d)

        def get_blob_client(self, name):
            if name not in self.blobs:
                raise ResourceNotFoundError(name)
            return FakeBlobClient(self.blobs[name])

        def list_blobs(self, name_starts_with=""):
            return [SimpleNamespace(name=k, size=len(v))
                    for k, v in self.blobs.items()
                    if k.startswith(name_starts_with)]

        def delete_blob(self, name):
            self.blobs.pop(name, None)

    class FakeBlobService:
        def __init__(self, *a, **k):
            self.containers = {}

        def get_container_client(self, name):
            if name not in self.containers:
                self.containers[name] = FakeContainerClient()
            return self.containers[name]

    class FakeGcsBlob:
        def __init__(self, bucket, name):
            self.bucket = bucket
            self.name = name

        def upload_from_filename(self, path):
            from pathlib import Path
            self.bucket.blobs[self.name] = Path(path).read_bytes()

        def upload_from_string(self, data):
            self.bucket.blobs[self.name] = bytes(data)

        def download_to_filename(self, path):
            from pathlib import Path
            Path(path).write_bytes(self.bucket.blobs[self.name])

        def download_as_bytes(self, start=None, end=None):
            d = self.bucket.blobs[self.name]
            if start is not None:
                d = d[start: end + 1 if end is not None else None]
            return d

        def delete(self):
            self.bucket.blobs.pop(self.name, None)

        @property
        def size(self):
            return len(self.bucket.blobs.get(self.name, b""))

    class FakeGcsBucket:
        def __init__(self, name, client):
            self.name = name
            self.client = client
            self.blobs = {}

        def blob(self, name):
            return FakeGcsBlob(self, name)

        def get_blob(self, name):
            if name in self.blobs:
                return FakeGcsBlob(self, name)
            return None

    class FakeGcsClient:
        def __init__(self, **kw):
            self.buckets = {}

        def bucket(self, name):
            if name not in self.buckets:
                self.buckets[name] = FakeGcsBucket(name, self)
            return self.buckets[name]

        def list_blobs(self, bucket, prefix=""):
            return [FakeGcsBlob(bucket, k) for k in bucket.blobs
                    if k.startswith(prefix)]

    monkeypatch.setattr(azure.storage.blob, "BlobServiceClient", FakeBlobService)
    monkeypatch.setattr(google.cloud.storage, "Client", FakeGcsClient)

    class Work:
        def __init__(self):
            self.db = "app"
            self.dir = tmp_path / "reports"
            self.dir.mkdir(parents=True, exist_ok=True)
            self.spool = tmp_path / "spool"
            self.spool.mkdir(parents=True, exist_ok=True)
            self.said = []

        def prefix(self):
            return "myseed/app/"

        def log(self, msg):
            self.said.append(msg)

    w = Work()
    main_store = carriers.LocalStore(tmp_path / "carrier")
    parts_data = {
        "myseed/app/parts/orders-0": b"part-data-zero-17",
        "myseed/app/parts/orders-1": b"part-data-one--17",
        "myseed/app/parts/orders-2": b"part-data-two--17",
    }
    manifest = {
        "tables": [{
            "table": "orders",
            "parts": {
                str(i): {
                    "name": f"myseed/app/parts/orders-{i}",
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
                for i, data in enumerate(parts_data.values())
            },
        }]
    }
    seal = {"manifest_sha256": hashlib.sha256(b"manifest").hexdigest()}

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="gather-s3")
        s3.put_object(Bucket="gather-s3", Key="myseed/app/parts/orders-0",
                      Body=parts_data["myseed/app/parts/orders-0"])

        s = {
            "gather": [
                "s3://gather-s3/",
                "http://127.0.0.1:10000/seedacct/gather-container/",
                "gs://gather-gcs/",
            ]
        }
        others = ship.others_of(s)
        others[1].put_bytes("myseed/app/parts/orders-1",
                            parts_data["myseed/app/parts/orders-1"])
        others[2].put_bytes("myseed/app/parts/orders-2",
                            parts_data["myseed/app/parts/orders-2"])
        others[2].put_bytes("myseed/app/parts/orders-0",
                            b"corrupt-data-0-17")

        gathered = ship.gather(w, main_store, manifest, seal, others)
        assert len(gathered) == 3
        assert "gathered from 4 places" in w.said[-1]
        assert "1 copies refused" in w.said[-1]

