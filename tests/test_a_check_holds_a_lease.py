"""A verification holds a lease, refused where another is running (FIELD-OPS02).

Measured: detached background verification and foreground check ran concurrently
on the same hop, driving cumulative query duration to 25,723 seconds within a
21,109-second window. `check` now acquires `lease-check.json`, blocking overlapping
processes with exit code 2 (refused) and releasing it cleanly on exit.
"""
import sqlite3
import pytest
from click.testing import CliRunner
from migkit import cli, lease

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
        # A second check should be refused
        got = runner.invoke(cli.main, ["check", "lite"])
        assert got.exit_code == 4, f"Expected refused (4), got {got.exit_code}: {got.output}"
        assert "[BLOCKED]" in got.output or "another migkit verification is running" in got.output
    finally:
        held_lease.release()

    # After release, check should succeed cleanly
    got_after = runner.invoke(cli.main, ["check", "lite"])
    assert got_after.exit_code == 0, f"Expected success (0), got {got_after.exit_code}: {got_after.output}"
