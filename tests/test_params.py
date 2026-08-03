"""params check: every server setting from both sides is dumped to params.json
(the objects.json parallel), and only behavior-critical mismatches (timezone,
encoding, collation, ...) fail the check. Instance-specific settings that always
differ on managed databases (memory, paths, limits) are counted but stay ok, so
the check is signal not noise. Pure-unit: exercises the shared reducer with no
live database, so it always runs."""
import json

from migkit.config import Endpoint, Hop
from migkit.engines.postgres import PostgresEngine


def _eng(tmp_path):
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="s"), target=Endpoint(host="t"))
    eng = PostgresEngine(hop)
    eng.hop.report_dir = lambda db="": tmp_path
    return eng


def test_critical_diff_fails_and_dumps_all(tmp_path):
    eng = _eng(tmp_path)
    src = {"TimeZone": "UTC", "work_mem": "4MB"}
    dst = {"TimeZone": "Asia/Bangkok", "work_mem": "64MB"}
    r = eng._param_result("shop", src, dst, PostgresEngine.PARAM_CRITICAL, "fix")[0]
    assert r.status == "diff"
    assert "TimeZone" in r.detail
    data = json.loads((tmp_path / "params.json").read_text())
    # every setting from both sides is on disk, not just the critical one
    assert data["TimeZone"] == {"src": "UTC", "dst": "Asia/Bangkok"}
    assert data["work_mem"] == {"src": "4MB", "dst": "64MB"}


def test_only_noncritical_diff_stays_ok(tmp_path):
    eng = _eng(tmp_path)
    r = eng._param_result("shop", {"work_mem": "4MB"}, {"work_mem": "64MB"},
                          PostgresEngine.PARAM_CRITICAL, "fix")[0]
    assert r.status == "ok"
    assert "none" in r.detail and "behavior-critical" in r.detail


def test_all_equal_ok(tmp_path):
    eng = _eng(tmp_path)
    r = eng._param_result("shop", {"TimeZone": "UTC"}, {"TimeZone": "UTC"},
                          PostgresEngine.PARAM_CRITICAL, "fix")[0]
    assert r.status == "ok"
    assert "all equal" in r.detail
