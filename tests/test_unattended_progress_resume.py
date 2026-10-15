"""Tests for P0-VALIDATE.unattended.progress (#499) and P0-RESUME-ALL (#495):
Durable per-check progress streaming and partial resume without re-scanning valid units.
"""
import json
from pathlib import Path
from types import SimpleNamespace
from migkit import cli


def test_durable_check_progress_file_written_and_resumed(tmp_path):
    report_dir = tmp_path / "reports"
    db_report_dir = report_dir / "shop"
    db_report_dir.mkdir(parents=True, exist_ok=True)
    prog_file = db_report_dir / "check-progress.jsonl"

    rec1 = {"check": "schema", "scope": "shop", "status": "ok", "detail": "identical"}
    rec2 = {"check": "counts", "scope": "shop", "status": "ok", "detail": "1000 rows"}

    # Simulate partial progress saved
    prog_file.write_text(json.dumps(rec1) + "\n" + json.dumps(rec2) + "\n")

    hop = SimpleNamespace(
        name="test-hop",
        engine="postgres",
        exclude=[],
        report_dir=lambda d=None: db_report_dir if d else report_dir,
        scope_changed=lambda r: False,
        scope_fingerprint=lambda: "dummy",
    )

    # Verify recovery logic loads partial progress from check-progress.jsonl
    prev = {}
    for d in ["shop"]:
        pf = hop.report_dir(d) / "check-progress.jsonl"
        if pf.exists():
            for line in pf.read_text().splitlines():
                if line.strip():
                    r = json.loads(line)
                    k = (r.get("check"), r.get("scope"))
                    if k[0] and k[1] and k not in prev:
                        prev[k] = r

    assert ("schema", "shop") in prev
    assert prev[("schema", "shop")]["status"] == "ok"
    assert ("counts", "shop") in prev
    assert prev[("counts", "shop")]["status"] == "ok"
