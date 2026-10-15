"""Every wrapped program and library is accounted for, and the account is
written from the code (backlog 0f).

`tools/whole_use.py` sets the tool-use registry (`migkit/tooluse.py`) against
a catalogue of what each tool can do and a walk of the package, and writes
`docs/research/whole-use-2026-10-16.md` and the 0f audit block of
`docs/backlog.md`. This catches a commit that left either behind, a
catalogue entry for an option the registry no longer holds, a candidate that
is used now, and a tool the walk cannot find a call for.
"""
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def whole():
    spec = importlib.util.spec_from_file_location(
        "whole_use", ROOT / "tools" / "whole_use.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def facts(whole):
    return whole.build()


def test_the_registry_is_the_codes():
    got = subprocess.run([sys.executable, "tools/whole_use.py", "--check"],
                         cwd=ROOT, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr or (
        "docs/research/whole-use-2026-10-16.md or the 0f block of"
        " docs/backlog.md is behind: run python tools/whole_use.py")


def test_the_catalogue_agrees_with_the_registry(facts):
    assert facts["problems"] == []


def test_every_catalogued_capability_has_a_computed_state(facts):
    assert facts["caps"], "an empty catalogue passes for the wrong reason"
    for tool, rows in facts["caps"].items():
        assert rows, tool
        for row in rows:
            assert row["state"] in ("used", "topped up", "partly used",
                                    "unused"), (tool, row["id"])
            assert row["covers"], (tool, row["id"])


def test_a_candidate_names_the_rung_it_strengthens(facts):
    for tool, rows in facts["caps"].items():
        for row in rows:
            if row.get("candidate"):
                assert row["rung"].strip(), (tool, row["id"])
                assert row["state"] in ("unused", "partly used"), (
                    tool, row["id"])


def test_every_registered_tool_has_a_call_site(facts):
    assert facts["where"], "a walk that found nothing is not a pass"
    lost = [n for n, files in facts["where"].items() if not files]
    assert lost == []


def test_a_state_is_computed_from_the_options_covered(whole):
    assert whole.capability_state(["used", "used"]) == "used"
    assert whole.capability_state(["used", "topped up"]) == "topped up"
    assert whole.capability_state(["used", "unused"]) == "partly used"
    assert whole.capability_state(["topped up", "unused"]) == "partly used"
    assert whole.capability_state(["unused", "unused"]) == "unused"


def test_a_stale_catalogue_key_is_caught(whole, monkeypatch):
    cat = whole.catalogue()
    first = next(iter(cat["tools"]))
    cat["tools"][first]["capabilities"][0]["covers"] = ["--no-such-option"]
    monkeypatch.setattr(whole, "catalogue", lambda: cat)
    problems = whole.build()["problems"]
    assert any("--no-such-option" in p and first in p for p in problems)


def test_a_candidate_that_is_used_now_is_caught(whole, monkeypatch):
    cat = whole.catalogue()
    row = next(c for c in cat["tools"]["dvt"]["capabilities"]
               if c["id"] == "row-hash")
    row["candidate"], row["rung"] = True, "any rung"
    monkeypatch.setattr(whole, "catalogue", lambda: cat)
    problems = whole.build()["problems"]
    assert any("dvt row-hash" in p and "candidate" in p for p in problems)


def test_a_call_with_no_account_is_listed(facts):
    names = {n for _, n, _ in facts["loose"]}
    assert "pymysql" in names and "mysqldump" in names
    assert "docker" not in names, "a system program needs no account"
    assert "boto3" not in names, "boto3 is the aws-sdk account's"


def test_the_backlog_block_has_a_row_for_each_open_candidate(whole, facts):
    block = whole.backlog_rows(facts)
    for tool, rows in facts["caps"].items():
        for row in rows:
            key = f"`0f.{tool}.{row['id']}`"
            wanted = row.get("candidate") and row["state"] in (
                "unused", "partly used")
            assert (key in block) == bool(wanted), key
