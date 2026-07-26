"""delta_verify must never silently skip a gap. If the saved change-stream
token has fallen off the oplog window (ChangeStreamHistoryLost, code 286),
resuming from a fresh token would report 'in sync' while missing every change
in the gap. The guard drops the token and demands a full re-baseline instead.
Tested deterministically with a fake client that raises code 286 - no replica
set / oplog manipulation needed."""
import pytest

pymongo = pytest.importorskip("pymongo")

import migkit.config as cfg
from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine


def test_expired_token_forces_reverify(tmp_path, monkeypatch):
    from pymongo.errors import OperationFailure
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    hop = Hop(name="m", engine="mongodb",
              source=Endpoint(host="h", port=1),
              target=Endpoint(host="h", port=2),
              databases=["db"])
    eng = MongoEngine(hop)
    d = eng.hop.report_dir("db")
    d.mkdir(parents=True, exist_ok=True)
    (d / "delta-token.json").write_text('{"_data": "resumetoken"}')

    class FakeDB:
        def watch(self, **kw):
            raise OperationFailure(
                "resume point may no longer be in the oplog", 286)

    class FakeClient:
        def __getitem__(self, name):
            return FakeDB()

    monkeypatch.setattr(eng, "_client", lambda side: FakeClient())

    r = eng.delta_verify("db")
    assert r[0].status == "diff", r[0].__dict__
    assert "oplog" in r[0].detail.lower() or "expired" in r[0].detail.lower()
    # the stale token is dropped so the next run re-baselines, not skips
    assert not (d / "delta-token.json").exists()
