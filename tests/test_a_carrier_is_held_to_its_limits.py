"""The carriers a seed goes on, as rungs, each held to its limits before a
part is written (`carriers`): the providers' published capacity, largest
file and file count, and where a carrier is a directory here, what its
file system says of itself. A carrier that fills stops the export where it
is and the next one goes on from there; an S3-compatible endpoint (the AWS
appliance's own, stood in for by moto's server) carries a seed the far end
loads from the object store it was imported into.
"""
import json
import os
import sqlite3
import threading

import pytest

from migkit import carriers, ship
from migkit.config import Endpoint, Hop
from tests.test_a_seed_is_shipped_on_media import _files, _keys, _rows

TB, TIB = 10 ** 12, 2 ** 40


def test_a_size_is_read_as_the_operator_writes_it():
    assert carriers.size_of("80TB") == 80 * TB
    assert carriers.size_of("120 TiB") == 120 * TIB
    assert carriers.size_of("500gb") == 500 * 10 ** 9
    assert carriers.size_of(1024) == 1024
    assert carriers.size_of("a lot") is None


def test_each_appliance_is_held_to_what_its_provider_publishes():
    got = carriers.published("snowball", {})
    assert (got.capacity, got.file_bytes) == (210 * TB, 5 * TB)
    got = carriers.published("databox", {"model": "525"})
    assert (got.capacity, got.files) == (525 * TIB, 1_000_000_000)
    assert carriers.published("databox", {}).capacity == 80 * TIB
    assert carriers.published("databox", {}, "block").file_bytes == 14 * TIB
    assert carriers.published("databox", {}, "file").file_bytes == 4 * TIB
    got = carriers.published("transfer_appliance", {"model": "ta300"})
    assert (got.capacity, got.file_bytes) == (300 * TB, 5 * TIB)
    # the hop's own capacity stands in for the model's
    assert carriers.published("snowball", {"capacity": "80TB"}).capacity \
        == 80 * TB
    with pytest.raises(SystemExit, match="is not one of"):
        carriers.published("transfer_appliance", {"model": "ta9000"})


@pytest.mark.parametrize("path,why", [
    ("/mnt/box/acct_PageBlob/seed", "512"),
    ("/mnt/box/acct_BlockBlob", "root"),
    ("/mnt/box/acct_BlockBlob/Seed_Parts", "container"),
    ("/mnt/box/acct_BlockBlob/seed-parts", ""),
    ("/mnt/box/acct_AzFile/seed", ""),
])
def test_an_azure_share_that_would_not_upload_a_part_is_refused(path, why):
    got = carriers._share_problem(path)
    if why:
        assert why in got, got
    else:
        assert got == "", got
    assert carriers.share_kind(path)[0] in ("page", "block", "file")


def test_a_carrier_is_proved_before_a_part_is_trusted_to_it(tmp_path):
    got = carriers.for_export({"carrier": "disk",
                               "path": str(tmp_path / "c")})
    assert got.rung.name == "disk"
    # the probe is written, read back and taken away again
    assert not (tmp_path / "c" / carriers.PROBE).exists()
    assert got.limits.name_chars
    if os.geteuid() == 0:
        pytest.skip("root writes through a read-only directory")
    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    try:
        with pytest.raises(SystemExit, match="probe file"):
            carriers.for_export({"carrier": "disk", "path": str(locked)})
    finally:
        locked.chmod(0o700)
    with pytest.raises(SystemExit, match="one of"):
        carriers.for_export({"carrier": "tape", "path": str(tmp_path)})
    with pytest.raises(SystemExit, match="no path"):
        carriers.for_export({"carrier": "transfer_appliance"})


def test_a_location_names_its_store():
    got = carriers.at("s3://seed/big/")
    assert isinstance(got, carriers.S3Store)
    assert (got.bucket, got.prefix) == ("seed", "big/")
    got = carriers.at("gs://seed")
    assert isinstance(got, carriers.GcsStore) and got.prefix == ""
    got = carriers.at("https://acct.blob.core.windows.net/seed/big")
    assert (got.account_url, got.container, got.prefix) == (
        "https://acct.blob.core.windows.net", "seed", "big/")
    # an emulator names its account in the path
    got = carriers.at("http://127.0.0.1:16414/devstoreaccount1/seed/big")
    assert (got.account_url, got.container, got.prefix) == (
        "http://127.0.0.1:16414/devstoreaccount1", "seed", "big/")
    assert carriers._account_of(got.account_url) == "devstoreaccount1"
    assert isinstance(carriers.at("/mnt/seed"), carriers.LocalStore)


def _hop(tmp_path, src, dst, shipped):
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"],
              options={"at_rest": {"recipients": [_keys(tmp_path)]},
                       "ship": shipped})
    hop.report_dir = report_dir
    return hop


def _carry(hop):
    from migkit.engines.sqlite import SQLiteEngine
    said = []
    got = ship.carry(hop, SQLiteEngine(hop), "main", "full", True,
                     said.append, via="builtin")
    return got, said


@pytest.fixture
def small(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    monkeypatch.setattr(ship, "PART_ROWS", 100)
    monkeypatch.setattr(ship._Work, "workers", lambda self, units: 1)
    return tmp_path


def test_a_full_carrier_stops_the_export_and_the_next_goes_on(small):
    tmp_path = small
    src, dst = _files(tmp_path)
    one, two = tmp_path / "one", tmp_path / "two"
    hop = _hop(tmp_path, src, dst, {"carrier": "disk", "path": str(one),
                                    "capacity": "5KB", "shipping": "1d"})
    with pytest.raises(ship.Full, match="is full") as e:
        _carry(hop)
    assert "Put another carrier in its place" in str(e.value)
    on_one = sorted(p.name for p in one.rglob("*.part"))
    assert on_one and not (one / "lite" / "main" / ship.SEALED).exists()
    written = sum(p.stat().st_size for p in one.rglob("*.part"))
    assert written <= 5_000
    hop.options["ship"] = {"carrier": "disk", "path": str(two),
                           "shipping": "1d"}
    got, said = _carry(hop)
    assert got["main"].said.startswith("sealed"), said
    # no range the first carrier holds was written again on the second (a
    # table in one pass begins again from its start)
    tid = ship._tid("orders")
    ranges = {p.relative_to(one) for p in one.rglob(f"{tid}/*.part")}
    assert not ranges & {p.relative_to(two)
                         for p in two.rglob(f"{tid}/*.part")}
    # the far end reads both carriers' parts from one place
    import shutil
    both = tmp_path / "both"
    shutil.copytree(one, both)
    shutil.copytree(two, both, dirs_exist_ok=True)
    hop.options["ship"]["load_from"] = str(both)
    got, said = _carry(hop)
    assert got["main"].half == "load", said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table


MOTO = 16416


@pytest.fixture
def endpoint(small, monkeypatch):
    """moto's S3 on a port of its own: an endpoint reached by its address,
    as the appliance's is."""
    from moto.server import ThreadedMotoServer
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=MOTO)
    server.start()
    try:
        import boto3
        boto3.client("s3", endpoint_url=f"http://127.0.0.1:{MOTO}",
                     region_name="us-east-1").create_bucket(Bucket="seed")
        yield f"http://127.0.0.1:{MOTO}"
    finally:
        server.stop()


def test_a_seed_goes_by_the_appliances_endpoint_and_is_read_from_the_store(
        small, endpoint):
    tmp_path = small
    src, dst = _files(tmp_path)
    hop = _hop(tmp_path, src, dst, {
        "carrier": "snowball", "endpoint": endpoint, "bucket": "seed",
        "prefix": "job-1", "region": "us-east-1", "shipping": "5d",
        # where the provider imports it: the same bucket here
        "load_from": "s3://seed/job-1", "load_endpoint": endpoint,
        "load_region": "us-east-1"})
    got, said = _carry(hop)
    assert got["main"].half == "export", said
    assert any("the AWS appliance through its object endpoint" in m
               for m in said), said
    import boto3
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    keys = [o["Key"] for o in s3.list_objects_v2(
        Bucket="seed", Prefix="job-1/")["Contents"]]
    assert "job-1/lite/main/sealed.json" in keys
    assert not any(k.endswith(carriers.PROBE) for k in keys), keys
    body = s3.get_object(Bucket="seed", Key=next(
        k for k in keys if k.endswith(".part")))["Body"].read()
    assert b"item-1" not in body
    seal = json.loads(s3.get_object(
        Bucket="seed", Key="job-1/lite/main/sealed.json")["Body"].read())
    assert seal["parts"] == sum(1 for k in keys if k.endswith(".part"))
    got, said = _carry(hop)
    assert got["main"].half == "load", said
    assert any("the provider's object store" in m for m in said), said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table


def test_a_part_the_endpoint_holds_short_is_written_again(small, endpoint,
                                                          monkeypatch):
    tmp_path = small
    src, dst = _files(tmp_path)
    hop = _hop(tmp_path, src, dst, {
        "carrier": "snowball", "endpoint": endpoint, "bucket": "seed",
        "region": "us-east-1", "shipping": "5d"})
    stop = threading.Event()
    put = carriers.S3Store.put

    def stopping(self, name, path):
        put(self, name, path)
        if name.endswith("000001.part") and not stop.is_set():
            stop.set()
            raise RuntimeError("the endpoint went away")
    monkeypatch.setattr(carriers.S3Store, "put", stopping)
    with pytest.raises(RuntimeError):
        _carry(hop)
    import boto3
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    first = next(o["Key"] for o in s3.list_objects_v2(
        Bucket="seed", Prefix="lite/main/parts/")["Contents"]
        if o["Key"].endswith("000000.part"))
    # the carrier holds a part short of what was written to it
    s3.put_object(Bucket="seed", Key=first, Body=b"short")
    got, said = _carry(hop)
    assert any("is not whole on" in m for m in said), said
    assert got["main"].said.startswith("sealed"), said
    got, said = _carry(hop)
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table
    with sqlite3.connect(dst) as con:
        assert con.execute("select count(*) from orders").fetchone() == (300,)
