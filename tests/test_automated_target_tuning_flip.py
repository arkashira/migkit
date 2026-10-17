import json
import threading

from migkit import managed
from migkit.tune import bulk_profile, runtime_profile



class MockEngine:
    def __init__(self, family="postgres", settings=None, tables=None):
        self.family = family
        self.ENGINE_FAMILY = family
        self.settings = dict(settings or {})
        self.tables_list = list(tables or [])
        self.executed = []
        self._lock = threading.Lock()

    def execute_sql(self, side, db, sql):
        with self._lock:
            self.executed.append((side, db, sql))

    def execute(self, sql):
        with self._lock:
            self.executed.append(("dst", "db", sql))

    def user_tables(self, side, db):
        return list(self.tables_list)


def test_postgres_bulk_profile_statements_and_settings(tmp_path):
    journal = tmp_path / "journal.json"
    res = bulk_profile("postgres", "appdb", journal_path=journal, apply=False)

    assert isinstance(res, list)
    assert any("ALTER SYSTEM SET maintenance_work_mem = '2GB';" in s for s in res)
    assert any("ALTER SYSTEM SET max_wal_size = '40GB';" in s for s in res)
    assert any("ALTER SYSTEM SET checkpoint_timeout = '30min';" in s for s in res)
    assert any("ALTER SYSTEM SET synchronous_commit = 'off';" in s for s in res)
    assert any("SELECT pg_reload_conf();" in s for s in res)

    assert res.settings["maintenance_work_mem"] == "2GB"
    assert res.settings["max_wal_size"] == "40GB"
    assert res.settings["checkpoint_timeout"] == "30min"
    assert res.settings["synchronous_commit"] == "off"
    assert journal.exists()


def test_mysql_bulk_profile_statements_and_settings(tmp_path):
    journal = tmp_path / "journal.json"
    res = bulk_profile("mysql", "appdb", journal_path=journal, apply=False)

    assert isinstance(res, list)
    assert any("SET GLOBAL innodb_flush_log_at_trx_commit = 2;" in s for s in res)
    assert any("SET GLOBAL sync_binlog = 0;" in s for s in res)

    assert res.settings["innodb_flush_log_at_trx_commit"] == 2
    assert res.settings["sync_binlog"] == 0
    assert journal.exists()


def test_postgres_durable_journal_round_trip_and_accurate_restore(tmp_path):
    journal = tmp_path / "journal.json"
    eng = MockEngine(
        family="postgres",
        settings={
            "maintenance_work_mem": "128MB",
            "max_wal_size": "2GB",
            "checkpoint_timeout": "10min",
            "synchronous_commit": "local",
        },
    )

    bulk_profile(eng, "appdb", journal_path=journal, apply=True)
    assert len(eng.executed) == 5

    raw_journal = json.loads(journal.read_text())
    assert raw_journal["status"] == "bulk"
    assert raw_journal["previous_settings"]["maintenance_work_mem"] == "128MB"
    assert raw_journal["previous_settings"]["max_wal_size"] == "2GB"
    assert raw_journal["previous_settings"]["checkpoint_timeout"] == "10min"
    assert raw_journal["previous_settings"]["synchronous_commit"] == "local"

    eng.executed.clear()
    restore_res = runtime_profile(eng, "appdb", journal_path=journal, apply=True, background=False)

    assert any("ALTER SYSTEM SET maintenance_work_mem = '128MB';" in s for s in restore_res)
    assert any("ALTER SYSTEM SET max_wal_size = '2GB';" in s for s in restore_res)
    assert any("ALTER SYSTEM SET checkpoint_timeout = '10min';" in s for s in restore_res)
    assert any("ALTER SYSTEM SET synchronous_commit = 'local';" in s for s in restore_res)
    assert any("SELECT pg_reload_conf();" in s for s in restore_res)
    assert any("ANALYZE;" in s for s in restore_res)

    executed_sqls = [stmt for side, db, stmt in eng.executed]
    assert any("ALTER SYSTEM SET maintenance_work_mem = '128MB';" in s for s in executed_sqls)
    assert any("ANALYZE;" in s for s in executed_sqls)

    raw_journal_restored = json.loads(journal.read_text())
    assert raw_journal_restored["status"] == "restored"
    assert "restored_at" in raw_journal_restored


def test_mysql_runtime_profile_restore_and_analyze(tmp_path):
    journal = tmp_path / "journal.json"
    eng = MockEngine(
        family="mysql",
        settings={
            "innodb_flush_log_at_trx_commit": 1,
            "sync_binlog": 1,
        },
        tables=["users", "orders"],
    )

    bulk_profile(eng, "shop", journal_path=journal, apply=True)
    eng.executed.clear()

    restore_res = runtime_profile(eng, "shop", journal_path=journal, apply=True, background=False)

    assert any("SET GLOBAL innodb_flush_log_at_trx_commit = 1;" in s for s in restore_res)
    assert any("SET GLOBAL sync_binlog = 1;" in s for s in restore_res)
    assert any("ANALYZE" in s for s in restore_res)

    executed_sqls = [stmt for side, db, stmt in eng.executed]
    assert any("ANALYZE TABLE users, orders;" in s for s in executed_sqls)


def test_runtime_profile_without_journal_falls_back_to_safe_defaults(tmp_path):
    nonexistent = tmp_path / "nonexistent.json"
    res_pg = runtime_profile("postgres", "db", journal_path=nonexistent, apply=False)

    assert any("ALTER SYSTEM SET maintenance_work_mem = '64MB';" in s for s in res_pg)
    assert any("ALTER SYSTEM SET max_wal_size = '1GB';" in s for s in res_pg)
    assert any("ALTER SYSTEM SET checkpoint_timeout = '5min';" in s for s in res_pg)
    assert any("ALTER SYSTEM SET synchronous_commit = 'on';" in s for s in res_pg)
    assert any("ANALYZE;" in s for s in res_pg)

    res_my = runtime_profile("mysql", "db", journal_path=nonexistent, apply=False)
    assert any("SET GLOBAL innodb_flush_log_at_trx_commit = 1;" in s for s in res_my)
    assert any("SET GLOBAL sync_binlog = 1;" in s for s in res_my)
    assert any("ANALYZE" in s for s in res_my)


def test_background_analyze_thread(tmp_path):
    journal = tmp_path / "journal.json"
    eng = MockEngine(family="postgres")

    bulk_profile(eng, "db", journal_path=journal, apply=True)
    eng.executed.clear()

    res = runtime_profile(eng, "db", journal_path=journal, apply=True, background=True)
    assert res.analyze_thread is not None
    assert isinstance(res.analyze_thread, threading.Thread)

    res.analyze_thread.join(timeout=2.0)
    assert not res.analyze_thread.is_alive()

    executed_sqls = [stmt for side, db, stmt in eng.executed]
    assert any("ANALYZE;" in s for s in executed_sqls)


def test_bulk_profile_reentrancy_preserves_original_settings(tmp_path):
    journal = tmp_path / "journal.json"
    eng = MockEngine(family="postgres", settings={"maintenance_work_mem": "256MB"})

    bulk_profile(eng, "db", journal_path=journal, apply=True)
    # Simulate that engine settings became the bulk settings
    eng.settings["maintenance_work_mem"] = "2GB"

    # Second call should not overwrite previous_settings in journal with 2GB
    bulk_profile(eng, "db", journal_path=journal, apply=True)
    raw = json.loads(journal.read_text())
    assert raw["previous_settings"]["maintenance_work_mem"] == "256MB"

    res = runtime_profile(eng, "db", journal_path=journal, apply=True, background=False)
    assert any("ALTER SYSTEM SET maintenance_work_mem = '256MB';" in s for s in res)


def test_managed_module_integration(tmp_path):
    journal = tmp_path / "journal.json"
    res_bulk = managed.bulk_profile("postgres", "db", journal_path=journal, apply=False)
    assert any("maintenance_work_mem = '2GB'" in s for s in res_bulk)

    res_runtime = managed.runtime_profile("postgres", "db", journal_path=journal, apply=False)
    assert any("ANALYZE;" in s for s in res_runtime)


def test_target_hardware_tuner_interface_and_overrides(tmp_path):
    from migkit.tune import TargetHardwareTuner

    journal = tmp_path / "tuner-journal.json"
    eng = MockEngine(family="postgres", settings={"maintenance_work_mem": "512MB"})
    tuner = TargetHardwareTuner(journal_path=journal, apply=True, background=False, overrides={"maintenance_work_mem": "4GB"})

    # Bulk apply
    res_b = tuner.apply_bulk(eng, db="appdb")
    assert any("maintenance_work_mem = '4GB'" in s for s in res_b.applied)
    executed_sqls = [stmt for side, db, stmt in eng.executed]
    assert any("maintenance_work_mem = '4GB'" in s for s in executed_sqls)

    eng.executed.clear()
    # Runtime restore
    res_r = tuner.apply_runtime(eng, db="appdb")
    assert any("maintenance_work_mem = '512MB'" in s for s in res_r.applied)
    executed_sqls_r = [stmt for side, db, stmt in eng.executed]
    assert any("maintenance_work_mem = '512MB'" in s for s in executed_sqls_r)


def test_runtime_profile_without_journal_does_not_overwrite_target_by_default(tmp_path):
    nonexistent = tmp_path / "absent.json"
    eng = MockEngine(family="postgres", settings={"maintenance_work_mem": "16GB"})

    # apply=True without journal and without force_defaults should NOT run restore_stmts on eng
    runtime_profile(eng, "appdb", journal_path=nonexistent, apply=True, background=False, force_defaults=False)
    executed_sqls = [stmt for side, db, stmt in eng.executed]
    # No ALTER SYSTEM SET should be executed
    assert not any("ALTER SYSTEM SET" in s for s in executed_sqls)
    # But ANALYZE should still run
    assert any("ANALYZE;" in s for s in executed_sqls)

