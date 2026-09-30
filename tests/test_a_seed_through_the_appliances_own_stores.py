"""A seed through the stores the Azure and Google appliances are reached
by or imported into, each stood in for by its provider's own emulator in
a container of this test's own: the Azure appliance's blob endpoint and
the storage account it is imported into (the blob emulator, 127.0.0.1:
16414), and the Cloud Storage bucket the Google appliance's parts land in
(the Cloud Storage emulator, 127.0.0.1:16415) - its NFS share a directory
here. SQLite on both sides; one container at a time, removed after.
"""
import base64
import os
import socket
import subprocess
import time

import pytest

from migkit import ship
from tests.conftest import needs_docker
from tests.test_a_seed_is_shipped_on_media import _files, _keys, _rows

pytestmark = needs_docker

BLOB, GCS = "migkit-ship-blob", "migkit-ship-gcs"
BLOB_PORT, GCS_PORT = 16414, 16415


def _up(port, most=120):
    end = time.time() + most
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(1)
    pytest.fail(f"nothing answered on {port}")


def _hop(tmp_path, src, dst, shipped):
    from migkit.config import Endpoint, Hop
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


@pytest.fixture
def blob(small, monkeypatch):
    """The blob emulator with an account of this test's own, its key drawn
    here."""
    key = base64.b64encode(os.urandom(64)).decode()
    monkeypatch.setenv("SEED_BLOB_KEY", key)
    subprocess.run(["docker", "rm", "-f", "-v", BLOB], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", BLOB, "-e",
                        f"AZURITE_ACCOUNTS=seedacct:{key}", "-p",
                        f"127.0.0.1:{BLOB_PORT}:10000",
                        "mcr.microsoft.com/azure-storage/azurite",
                        "azurite-blob", "--blobHost", "0.0.0.0",
                        "--blobPort", "10000", "--skipApiVersionCheck"],
                       check=True, capture_output=True)
        _up(BLOB_PORT)
        url = f"http://127.0.0.1:{BLOB_PORT}/seedacct"
        from azure.storage.blob import BlobServiceClient
        service = BlobServiceClient(url, credential={
            "account_name": "seedacct", "account_key": key})
        for name in ("device", "account"):
            for _ in range(30):
                try:
                    service.create_container(name)
                    break
                except Exception:  # noqa: BLE001 - not up yet
                    time.sleep(1)
        yield url, service
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", BLOB],
                       capture_output=True)


def test_the_azure_appliance_by_its_blob_endpoint(small, blob):
    tmp_path = small
    url, service = blob
    src, dst = _files(tmp_path)
    hop = _hop(tmp_path, src, dst, {
        "carrier": "databox", "endpoint": url, "container": "device",
        "credential": "env:SEED_BLOB_KEY", "model": "120", "shipping": "10d",
        # the storage account the appliance is uploaded into
        "load_from": f"{url}/account/",
        "load_credential": "env:SEED_BLOB_KEY"})
    got, said = _carry(hop)
    assert got["main"].half == "export", said
    assert any("through its blob endpoint" in m for m in said), said
    device = service.get_container_client("device")
    names = list(device.list_blob_names())
    assert "lite/main/sealed.json" in names, names
    assert not any(n.endswith(".migkit-probe") for n in names), names
    for n in names:
        if n.endswith(".part"):
            assert b"item-1" not in device.download_blob(n).readall()
    # the provider uploads the appliance into the account
    account = service.get_container_client("account")
    for n in names:
        account.upload_blob(n, device.download_blob(n).readall())
    got, said = _carry(hop)
    assert got["main"].half == "load", said
    assert any("the provider's object store" in m for m in said), said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table


@pytest.fixture
def gcs(small, monkeypatch):
    subprocess.run(["docker", "rm", "-f", "-v", GCS], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", GCS, "-p",
                        f"127.0.0.1:{GCS_PORT}:4443",
                        "fsouza/fake-gcs-server", "-scheme", "http",
                        "-public-host", f"127.0.0.1:{GCS_PORT}"],
                       check=True, capture_output=True)
        _up(GCS_PORT)
        monkeypatch.setenv("STORAGE_EMULATOR_HOST",
                           f"http://127.0.0.1:{GCS_PORT}")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "seed-test")
        from google.cloud import storage
        client = storage.Client(project="seed-test")
        for _ in range(30):
            try:
                client.create_bucket("seed-import")
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)
        yield client.bucket("seed-import")
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", GCS],
                       capture_output=True)


def test_the_google_appliance_by_its_share_and_the_bucket_it_lands_in(
        small, gcs):
    tmp_path = small
    share = tmp_path / "appliance-share"
    src, dst = _files(tmp_path)
    hop = _hop(tmp_path, src, dst, {
        "carrier": "transfer_appliance", "path": str(share),
        "model": "ta40", "shipping": "14d",
        "load_from": "gs://seed-import/"})
    got, said = _carry(hop)
    assert got["main"].half == "export", said
    assert any("through its NFS share" in m for m in said), said
    # what the provider does with the appliance: every file into the bucket
    files = [p for p in share.rglob("*") if p.is_file()]
    assert any(p.name == ship.SEALED for p in files), files
    for p in files:
        gcs.blob(p.relative_to(share).as_posix()).upload_from_filename(
            str(p))
    # the far end has the bucket, not the appliance
    import shutil
    shutil.rmtree(share)
    got, said = _carry(hop)
    assert got["main"].half == "load", said
    assert any("the provider's object store" in m for m in said), said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table
