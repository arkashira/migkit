"""A verification holds a lease, refused where another is running (FIELD-OPS02).

Measured: detached background verification and foreground check ran concurrently
on the same hop, driving cumulative query duration to 25,723 seconds within a
21,109-second window. `check`, `sync --mode verify`, and repair `--go` acquire
`lease-check.json`, blocking overlapping processes with exit code 4 (refused)
and releasing it cleanly on exit.
"""
import sqlite3
import pytest
from click.testing import CliRunner
from migkit import cli, lease
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("lease", "active_lease_lock_rungs", "exclusive_lease_acquired", "holds"),
    ("lease", "active_lease_lock_rungs", "exclusive_lease_acquired", "refuses"),
    ("lease", "active_lease_lock_rungs", "stale_holder_evicted", "holds"),
    ("lease", "active_lease_lock_rungs", "stale_holder_evicted", "refuses"),
    ("lease", "active_lease_lock_rungs", "fenced_epoch_monotonically_incremented", "holds"),
    ("lease", "active_lease_lock_rungs", "fenced_epoch_monotonically_incremented", "refuses"),
    ("lease", "active_lease_lock_rungs", "heartbeat_lease_extended", "holds"),
    ("lease", "active_lease_lock_rungs", "heartbeat_lease_extended", "refuses"),
    ("lease", "active_lease_lock_rungs", "refuse_overlapping_lease_collision", "holds"),
    ("lease", "active_lease_lock_rungs", "refuse_overlapping_lease_collision", "refuses"),
]

HOP = ("hops:\n  lite:\n    engine: sqlite\n"
       "    source: {{host: {a}, user: x, password: x}}\n"
       "    target: {{host: {b}, user: x, password: x}}\n"
       "    databases: [main]\n")


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.executescript(
            "create table t (id integer primary key, v text);"
            " insert into t values (1, 'x');")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(HOP.format(a=tmp_path / "a.db", b=tmp_path / "b.db"))
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def test_concurrent_check_is_blocked_by_active_lease(lite):
    runner = CliRunner()
    # First, simulate an active verification holding the lease
    report_dir = lite / "reports" / "lite"
    report_dir.mkdir(parents=True, exist_ok=True)
    lease_file = report_dir / "lease-check.json"

    # Acquire lease externally simulating another process
    held_lease = lease.Lease(lease_file, what="verification").acquire()
    try:
        # A second check should be refused with exit code 4
        got = runner.invoke(cli.main, ["check", "lite"])
        assert got.exit_code == 4, f"Expected refused (4), got {got.exit_code}: {got.output}"
        assert "[BLOCKED]" in got.output or "another migkit verification is running" in got.output
    finally:
        held_lease.release()

    # After release, check should succeed cleanly
    got_after = runner.invoke(cli.main, ["check", "lite"])
    assert got_after.exit_code == 0, f"Expected success (0), got {got_after.exit_code}: {got_after.output}"


def test_sync_verify_is_blocked_by_active_lease(lite):
    runner = CliRunner()
    report_dir = lite / "reports" / "lite"
    report_dir.mkdir(parents=True, exist_ok=True)
    lease_file = report_dir / "lease-check.json"

    held_lease = lease.Lease(lease_file, what="verification").acquire()
    try:
        got = runner.invoke(cli.main, ["sync", "lite", "--mode", "verify"])
        assert got.exit_code == 4, f"Expected refused (4), got {got.exit_code}: {got.output}"
        assert "[BLOCKED]" in got.output or "another migkit verification is running" in got.output
    finally:
        held_lease.release()


def test_repair_go_precheck_is_blocked_by_active_lease(lite):
    runner = CliRunner()
    report_dir = lite / "reports" / "lite"
    report_dir.mkdir(parents=True, exist_ok=True)
    lease_file = report_dir / "lease-check.json"

    held_lease = lease.Lease(lease_file, what="verification").acquire()
    try:
        got = runner.invoke(cli.main, ["sync", "lite", "--go"])
        assert got.exit_code == 4, f"Expected refused (4), got {got.exit_code}: {got.output}"
        assert "[BLOCKED]" in got.output or "another migkit verification is running" in got.output
    finally:
        held_lease.release()


def test_active_lease_lock_decide_rungs_reachability():
    with watched():
        # 1. exclusive_lease_acquired
        r1 = lease.decide_active_lease_lock({"exclusive_lock": True})
        assert r1.rung.name == "exclusive_lease_acquired"

        # 2. stale_holder_evicted
        r2 = lease.decide_active_lease_lock({"exclusive_lock": False, "stale_evicted": True})
        assert r2.rung.name == "stale_holder_evicted"

        # 3. fenced_epoch_monotonically_incremented
        r3 = lease.decide_active_lease_lock({"exclusive_lock": False, "stale_evicted": False, "fenced_epoch": True})
        assert r3.rung.name == "fenced_epoch_monotonically_incremented"

        # 4. heartbeat_lease_extended
        r4 = lease.decide_active_lease_lock({"exclusive_lock": False, "stale_evicted": False, "fenced_epoch": False, "heartbeat_active": True})
        assert r4.rung.name == "heartbeat_lease_extended"

        # 5. refuse_overlapping_lease_collision
        r5 = lease.decide_active_lease_lock({"exclusive_lock": False, "stale_evicted": False, "fenced_epoch": False, "heartbeat_active": False})
        assert r5.rung.name == "refuse_overlapping_lease_collision"

        # 6. refuse fallback
        r6 = lease.decide_active_lease_lock({"exclusive_lock": False, "stale_evicted": False, "fenced_epoch": False, "heartbeat_active": False, "allow_concurrent_lease": True})
        assert r6.rung is None
