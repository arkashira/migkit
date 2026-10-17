"""Automated Target Hardware & Parameter Tuning Flip (AUTO.target-tune)."""
import threading
import time
from pathlib import Path

from . import config, durable

POSTGRES_BULK = {
    "maintenance_work_mem": "2GB",
    "max_wal_size": "40GB",
    "checkpoint_timeout": "30min",
    "synchronous_commit": "off",
}

POSTGRES_RUNTIME_DEFAULTS = {
    "maintenance_work_mem": "64MB",
    "max_wal_size": "1GB",
    "checkpoint_timeout": "5min",
    "synchronous_commit": "on",
}

MYSQL_BULK = {
    "innodb_flush_log_at_trx_commit": 2,
    "sync_binlog": 0,
}

MYSQL_RUNTIME_DEFAULTS = {
    "innodb_flush_log_at_trx_commit": 1,
    "sync_binlog": 1,
}

BULK_SETTINGS = {
    "postgres": POSTGRES_BULK,
    "mysql": MYSQL_BULK,
}

RUNTIME_DEFAULTS = {
    "postgres": POSTGRES_RUNTIME_DEFAULTS,
    "mysql": MYSQL_RUNTIME_DEFAULTS,
}


def _family(eng):
    if isinstance(eng, str):
        return eng.lower()
    for attr in ("ENGINE_FAMILY", "family", "name", "kind"):
        val = getattr(eng, attr, None)
        if isinstance(val, str) and val:
            return val.lower()
    return "postgres"


def _journal_path(eng, db, journal_path=None):
    if journal_path is not None:
        return Path(journal_path)
    if hasattr(eng, "hop") and hasattr(eng.hop, "report_dir"):
        try:
            return Path(eng.hop.report_dir(db)) / "target-tune-journal.json"
        except Exception:
            pass
    if hasattr(eng, "report_dir") and callable(eng.report_dir):
        try:
            return Path(eng.report_dir(db)) / "target-tune-journal.json"
        except Exception:
            pass
    fam = _family(eng)
    return Path(config.state_home()) / "tune" / f"{fam}-{db}-journal.json"


def _read_current_settings(eng, db, family):
    defaults = RUNTIME_DEFAULTS.get(family, POSTGRES_RUNTIME_DEFAULTS)
    settings = dict(defaults)
    if hasattr(eng, "settings") and isinstance(eng.settings, dict):
        settings.update(eng.settings)
        return settings
    if hasattr(eng, "current_settings") and isinstance(eng.current_settings, dict):
        settings.update(eng.current_settings)
        return settings

    if family == "postgres":
        for k in POSTGRES_BULK:
            val = _query_single(eng, db, f"SELECT current_setting('{k}')")
            if val:
                settings[k] = val
    elif family == "mysql":
        for k in MYSQL_BULK:
            val = _query_single(eng, db, f"SELECT @@GLOBAL.{k}")
            if val is not None and val != "":
                try:
                    settings[k] = int(val)
                except (ValueError, TypeError):
                    settings[k] = val
    return settings


def _query_single(eng, db, sql, side="dst"):
    try:
        if hasattr(eng, "query"):
            res = eng.query(sql)
            if res:
                return str(res).strip()
        if hasattr(eng, "_psql"):
            out = eng._psql(side, db, sql)
            if out:
                return out.strip().splitlines()[0]
        if hasattr(eng, "_mysql"):
            out = eng._mysql(side, db, sql)
            if out:
                return out.strip().splitlines()[0]
    except Exception:
        pass
    return None


def _apply_statements(eng, db, statements, side="dst"):
    if isinstance(eng, str) or eng is None:
        return
    for stmt in statements:
        if hasattr(eng, "execute_sql"):
            eng.execute_sql(side, db, stmt)
        elif hasattr(eng, "execute"):
            try:
                eng.execute(stmt)
            except TypeError:
                eng.execute(side, db, stmt)
        elif hasattr(eng, "execute_ddl"):
            eng.execute_ddl(side, db, stmt)
        elif hasattr(eng, "_psql") and _family(eng) == "postgres":
            eng._psql(side, db, stmt)
        elif hasattr(eng, "_mysql") and _family(eng) == "mysql":
            eng._mysql(side, db, stmt)
        elif hasattr(eng, "run") and callable(eng.run):
            try:
                eng.run(stmt)
            except TypeError:
                eng.run(side, db, stmt)


def _generate_bulk_statements(family, bulk_settings):
    stmts = []
    if family == "postgres":
        for k, v in bulk_settings.items():
            stmts.append(f"ALTER SYSTEM SET {k} = '{v}';")
        stmts.append("SELECT pg_reload_conf();")
    elif family == "mysql":
        for k, v in bulk_settings.items():
            val = v if isinstance(v, (int, float)) else f"'{v}'"
            stmts.append(f"SET GLOBAL {k} = {val};")
    return stmts


def _generate_restore_statements(family, prev_settings):
    stmts = []
    if family == "postgres":
        for k, v in prev_settings.items():
            stmts.append(f"ALTER SYSTEM SET {k} = '{v}';")
        stmts.append("SELECT pg_reload_conf();")
        stmts.append("ANALYZE;")
    elif family == "mysql":
        for k, v in prev_settings.items():
            val = v if isinstance(v, (int, float)) else f"'{v}'"
            stmts.append(f"SET GLOBAL {k} = {val};")
        stmts.append("ANALYZE;")
    return stmts


class ProfileResult(list):
    def __init__(self, statements, settings, previous=None, journal_path=None, analyze_thread=None):
        super().__init__(statements)
        self.statements = list(statements)
        self.settings = dict(settings)
        self.previous = dict(previous or {})
        self.journal_path = journal_path
        self.analyze_thread = analyze_thread


def bulk_profile(eng, db, journal_path=None, side="dst", apply=True, overrides=None, **kw):
    family = _family(eng)
    bulk_cfg = dict(BULK_SETTINGS.get(family, POSTGRES_BULK))
    if overrides and isinstance(overrides, dict):
        bulk_cfg.update(overrides)
    j_path = _journal_path(eng, db, journal_path)

    existing = None
    if j_path.exists():
        try:
            existing = durable.read_state(j_path, "journal")
        except Exception:
            pass

    if existing and existing.get("status") == "bulk" and existing.get("previous_settings"):
        prev = existing["previous_settings"]
    else:
        prev = _read_current_settings(eng, db, family)

    journal_data = {
        "engine": family,
        "db": db,
        "status": "bulk",
        "applied_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "previous_settings": prev,
        "bulk_settings": bulk_cfg,
    }
    durable.write_json(j_path, journal_data)

    statements = _generate_bulk_statements(family, bulk_cfg)
    if apply:
        _apply_statements(eng, db, statements, side=side)

    return ProfileResult(statements, bulk_cfg, previous=prev, journal_path=j_path)


def _run_analyze(eng, db, family, side="dst"):
    sql = "ANALYZE;"
    if family == "mysql":
        try:
            if hasattr(eng, "user_tables") and callable(eng.user_tables):
                tables = eng.user_tables(side, db)
                if tables:
                    sql = f"ANALYZE TABLE {', '.join(tables)};"
        except Exception:
            pass
    try:
        _apply_statements(eng, db, [sql], side=side)
    except Exception:
        pass


def runtime_profile(eng, db, journal_path=None, side="dst", apply=True, background=True, force_defaults=False, **kw):
    family = _family(eng)
    j_path = _journal_path(eng, db, journal_path)

    existing = None
    if j_path.exists():
        try:
            existing = durable.read_state(j_path, "journal")
        except Exception:
            pass

    defaults = RUNTIME_DEFAULTS.get(family, POSTGRES_RUNTIME_DEFAULTS)
    if existing and existing.get("previous_settings"):
        prev = dict(existing["previous_settings"])
    else:
        live = _read_current_settings(eng, db, family) if eng and not isinstance(eng, str) else None
        if live:
            prev = live
        else:
            prev = dict(defaults)

    statements = _generate_restore_statements(family, prev)
    restore_stmts = [s for s in statements if not s.upper().startswith("ANALYZE")]

    if apply:
        if existing or force_defaults:
            _apply_statements(eng, db, restore_stmts, side=side)

    analyze_thread = None
    if apply:
        if background:
            analyze_thread = threading.Thread(
                target=_run_analyze, args=(eng, db, family, side), daemon=True, name="migkit-analyze"
            )
            analyze_thread.start()
        else:
            _run_analyze(eng, db, family, side)

    if existing:
        existing["status"] = "restored"
        existing["restored_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        durable.write_json(j_path, existing)

    return ProfileResult(statements, prev, previous=prev, journal_path=j_path, analyze_thread=analyze_thread)


class TargetHardwareTuner:
    """Automated Target Hardware & Parameter Tuning Flip (AUTO.target-tune)."""

    def __init__(self, journal_path=None, apply=True, background=True, overrides=None):
        self.journal_path = journal_path
        self.apply = apply
        self.background = background
        self.overrides = overrides or {}

    def apply_bulk(self, eng, db=None):
        res = bulk_profile(eng, db, journal_path=self.journal_path, apply=self.apply, overrides=self.overrides)
        res.applied = res.statements
        return res

    def apply_runtime(self, eng, db=None, force_defaults=False):
        res = runtime_profile(
            eng, db, journal_path=self.journal_path, apply=self.apply,
            background=self.background, force_defaults=force_defaults
        )
        res.applied = res.statements
        return res

