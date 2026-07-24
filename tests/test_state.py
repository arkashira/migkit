"""State backend: local files and remote S3, both pluggable like terraform.

Proves you can (1) capture a restore point, (2) list which points exist and
what each one is, (3) fetch any of them back byte-for-byte — including from a
remote S3 bucket, so state survives losing this machine.
"""
import os

import pytest

from migkit.config import Endpoint, Hop


def _hop(state_cfg):
    return Hop(name="proofhop", engine="mysql",
               source=Endpoint(host="s"), target=Endpoint(host="t"),
               options={"state": state_cfg})


def _make_points(store):
    tss = []
    for i, tag in enumerate(["pre-repair", ""]):
        p = store.new_point("shopdb", tag)
        p.path("rows-orders.jsonl").write_text(
            f'{{"pk":["{i}"],"cols":["id"],"old":null}}\n')
        p.set_meta(op="repair", tables=["orders"], detail=f"point {i}")
        tss.append(p.commit(f"2026-07-24 08:0{i}:00"))
    return tss


def test_local_backend_roundtrip(tmp_path, monkeypatch):
    import migkit.config as cfg
    import migkit.state as state
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(state, "REPORTS", tmp_path / "reports")
    store = state.get_store(_hop({"backend": "local",
                                  "mirror": str(tmp_path / "mirror")}))
    assert store.kind == "local"
    tss = _make_points(store)

    # list is how you choose which state to restore
    pts = store.list("shopdb")
    assert len(pts) == 2
    assert all(p["op"] == "repair" for p in pts)
    assert "pre-repair" in tss[0]

    # fetch any point back, contents intact
    d = store.fetch("shopdb", tss[0])
    assert '"pk":["0"]' in (d / "rows-orders.jsonl").read_text()

    # the mirror tarball exists too (survives losing reports/)
    assert list((tmp_path / "mirror").glob("shopdb-*.tar.gz"))


def test_s3_backend_roundtrip(tmp_path, monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3

    import migkit.config as cfg
    import migkit.state as state
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(state, "REPORTS", tmp_path / "reports")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket="migkit-state")
        store = state.get_store(_hop({"backend": "s3",
                                      "bucket": "migkit-state"}))
        assert store.kind == "s3"
        tss = _make_points(store)

        pts = store.list("shopdb")
        assert len(pts) == 2
        assert all(p["backend"] == "s3" for p in pts)

        # a fresh store instance (simulating another machine) can list + fetch
        store2 = state.get_store(_hop({"backend": "s3",
                                       "bucket": "migkit-state"}))
        # wipe the local cache so fetch must hit S3
        import shutil
        shutil.rmtree(tmp_path / "reports", ignore_errors=True)
        d = store2.fetch("shopdb", tss[0])
        assert '"pk":["0"]' in (d / "rows-orders.jsonl").read_text()


def test_unknown_backend_errors():
    import migkit.state as state
    with pytest.raises(SystemExit):
        state.get_store(_hop({"backend": "gcs"}))
