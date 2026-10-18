import pytest

from tests.rung_branches import watched
from migkit.lineage_evidence import (
    AssessmentCoverage,
    CallSite,
    DynamicLineageAuditor,
    LineageEvidenceLedger,
    RuntimeTrace,
    SyntaxReference,
    UnknownDynamicLineage,
    VersionedCatalogue,
    decide_lineage_evidence,
    decide_lineage_coverage,
)

PROVES_RUNGS = [
    ("lineage_evidence", "lineage_evidence_rungs", "certified_static_lineage", "holds"),
    ("lineage_evidence", "lineage_evidence_rungs", "certified_static_lineage", "refuses"),
    ("lineage_evidence", "lineage_evidence_rungs", "approved_runtime_trace_lineage", "holds"),
    ("lineage_evidence", "lineage_evidence_rungs", "approved_runtime_trace_lineage", "refuses"),
    ("lineage_evidence", "lineage_evidence_rungs", "rehearsal_observed_lineage", "holds"),
    ("lineage_evidence", "lineage_evidence_rungs", "rehearsal_observed_lineage", "refuses"),
    ("lineage_evidence", "lineage_evidence_rungs", "provisional_bounded_lineage", "holds"),
    ("lineage_evidence", "lineage_evidence_rungs", "provisional_bounded_lineage", "refuses"),
    ("lineage_evidence", "lineage_evidence_rungs", "refuse_untracked_dynamic_lineage", "holds"),
    ("lineage_evidence", "lineage_evidence_rungs", "refuse_untracked_dynamic_lineage", "refuses"),
    ("lineage_evidence", "lineage_coverage_rungs", "full_lineage_coverage", "holds"),
    ("lineage_evidence", "lineage_coverage_rungs", "full_lineage_coverage", "refuses"),
    ("lineage_evidence", "lineage_coverage_rungs", "trace_supplemented_coverage", "holds"),
    ("lineage_evidence", "lineage_coverage_rungs", "trace_supplemented_coverage", "refuses"),
    ("lineage_evidence", "lineage_coverage_rungs", "bounded_gap_coverage", "holds"),
    ("lineage_evidence", "lineage_coverage_rungs", "bounded_gap_coverage", "refuses"),
    ("lineage_evidence", "lineage_coverage_rungs", "honest_partial_coverage", "holds"),
    ("lineage_evidence", "lineage_coverage_rungs", "honest_partial_coverage", "refuses"),
    ("lineage_evidence", "lineage_coverage_rungs", "refuse_opaque_lineage_downcast", "holds"),
    ("lineage_evidence", "lineage_coverage_rungs", "refuse_opaque_lineage_downcast", "refuses"),
]



def test_static_routine_full_assessment():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.orders", "public.customers"},
        views={"public.active_orders"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION get_active_orders()
    RETURNS SETOF public.orders AS $$
    BEGIN
        RETURN QUERY SELECT * FROM public.orders WHERE status = 'active';
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record(routine_name="get_active_orders", code=sql)
    cov = ledger.compute_coverage("get_active_orders")

    assert cov.is_fully_assessed is True
    assert cov.coverage_pct == 100.0
    assert cov.unknown_dynamic_count == 0
    assert cov.unavailable_dependencies == 0
    assert len(cov.blockers) == 0


def test_dynamic_identifier_plpgsql_format():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.orders_2023", "public.orders_2024"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE archive_partition(target_tbl text) AS $$
    BEGIN
        EXECUTE format('DELETE FROM %I WHERE created_at < NOW() - INTERVAL ''1 year''', target_tbl);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record(routine_name="archive_partition", code=sql)
    cov = ledger.compute_coverage("archive_partition")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    assert cov.unknown_dynamic_count > 0

    unknowns = ledger.get_unknown_dynamic_lineages("archive_partition")
    assert len(unknowns) >= 1
    rec = unknowns[0]
    assert "target_tbl" in rec.blocking_identifiers
    assert any("format(" in cs.statement for cs in rec.call_sites)
    assert len(rec.required_runtime_evidence) > 0


def test_dynamic_identifier_oracle_execute_immediate():
    cat = VersionedCatalogue(engine="oracle", version="19c", tables={"SCOTT.EMP"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    plsql = """
    CREATE OR REPLACE PROCEDURE purge_data(p_table VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || p_table;
    END;
    """
    ledger.audit_and_record(routine_name="purge_data", code=plsql)
    cov = ledger.compute_coverage("purge_data")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("purge_data")
    assert len(unknowns) >= 1
    assert "p_table" in unknowns[0].blocking_identifiers
    assert unknowns[0].construct_kind == "dynamic_identifier"


def test_tsql_sp_executesql():
    cat = VersionedCatalogue(engine="mssql", version="2019", tables={"dbo.Employees"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE PROCEDURE dbo.RunQuery @dynSql NVARCHAR(MAX) AS
    BEGIN
        EXEC sp_executesql @dynSql;
    END;
    """
    ledger.audit_and_record(routine_name="dbo.RunQuery", code=sql)
    cov = ledger.compute_coverage("dbo.RunQuery")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("dbo.RunQuery")
    assert len(unknowns) >= 1
    assert "@dynSql" in unknowns[0].blocking_identifiers
    assert unknowns[0].construct_kind == "dynamic_sql"


def test_mysql_prepare_execute():
    cat = VersionedCatalogue(engine="mysql", version="8.0", tables={"analytics.events"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE PROCEDURE dynamic_select(IN tbl_name VARCHAR(64))
    BEGIN
        SET @s = CONCAT('SELECT count(*) FROM ', tbl_name);
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END;
    """
    ledger.audit_and_record(routine_name="dynamic_select", code=sql)
    cov = ledger.compute_coverage("dynamic_select")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("dynamic_select")
    assert len(unknowns) >= 1
    assert any(b in unknowns[0].blocking_identifiers for b in ["tbl_name", "@s"])


def test_application_sql_dynamic_string():
    auditor = DynamicLineageAuditor()
    py_code = 'query = f"SELECT * FROM {tenant_id}_orders WHERE active = 1"'
    unknowns = auditor.audit_code("app_order_fetch", py_code, language="python")

    assert len(unknowns) >= 1
    rec = unknowns[0]
    assert "tenant_id" in rec.blocking_identifiers
    assert rec.construct_kind == "dynamic_identifier"
    assert len(rec.call_sites) > 0


def test_unapproved_runtime_trace_does_not_resolve():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables={"public.orders_q1"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE route_order(tbl text) AS $$
    BEGIN
        EXECUTE format('INSERT INTO %I VALUES (1)', tbl);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("route_order", sql)

    trace = RuntimeTrace(
        trace_id="tr-101",
        caller="route_order",
        concrete_sql="INSERT INTO orders_q1 VALUES (1)",
        accessed_entities=["public.orders_q1"],
        approved=False,
    )
    ledger.record_runtime_trace(trace)
    ledger.attempt_resolution("route_order")

    cov = ledger.compute_coverage("route_order")
    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0

    unknowns = ledger.get_unknown_dynamic_lineages("route_order", unresolved_only=True)
    assert len(unknowns) == 1
    assert unknowns[0].resolved is False


def test_approved_runtime_trace_resolves_dynamic_lineage():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables={"public.orders_q1"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE route_order(tbl text) AS $$
    BEGIN
        EXECUTE format('INSERT INTO %I VALUES (1)', tbl);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("route_order", sql)

    trace = RuntimeTrace(
        trace_id="tr-102",
        caller="route_order",
        concrete_sql="INSERT INTO public.orders_q1 VALUES (1)",
        accessed_entities=["public.orders_q1"],
        approved=True,
    )
    ledger.record_runtime_trace(trace)
    ledger.attempt_resolution("route_order")

    cov = ledger.compute_coverage("route_order")
    assert cov.is_fully_assessed is True
    assert cov.coverage_pct == 100.0

    unresolved = ledger.get_unknown_dynamic_lineages("route_order", unresolved_only=True)
    assert len(unresolved) == 0

    all_unknowns = ledger.get_unknown_dynamic_lineages("route_order", unresolved_only=False)
    assert len(all_unknowns) == 1
    assert all_unknowns[0].resolved is True
    assert all_unknowns[0].resolved_by == "tr-102"


def test_unsupported_extension_blocks_full_assessment():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.locations"},
        supported_extensions={"btree_gist": "1.7"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION calc_distance(a geography, b geography) RETURNS float AS $$
    BEGIN
        RETURN ST_Distance(a, b);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("calc_distance", sql)
    cov = ledger.compute_coverage("calc_distance")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("calc_distance")
    assert any(u.construct_kind == "unsupported_extension" for u in unknowns)
    ext_rec = next(u for u in unknowns if u.construct_kind == "unsupported_extension")
    assert "postgis" in ext_rec.blocking_identifiers


def test_extension_supported_in_target_catalogue():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.locations"},
        supported_extensions={"postgis": "3.3.0"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION calc_distance(a geography, b geography) RETURNS float AS $$
    BEGIN
        RETURN ST_Distance(a, b);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("calc_distance", sql)
    cov = ledger.compute_coverage("calc_distance")

    assert cov.is_fully_assessed is True
    assert cov.coverage_pct == 100.0


def test_extension_version_incompatibility():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.locations"},
        supported_extensions={"postgis": "2.5.0"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION calc_subdivide(geom geometry) RETURNS SETOF geometry AS $$
    BEGIN
        RETURN QUERY SELECT ST_Subdivide(geom, 256);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("calc_subdivide", sql, required_extensions={"postgis": "3.0.0"})
    cov = ledger.compute_coverage("calc_subdivide")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("calc_subdivide")
    assert any("version" in u.reason.lower() or "postgis" in u.blocking_identifiers for u in unknowns)


def test_unavailable_dependencies_prevent_100_percent():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.users"},
        unavailable_dependencies={"remote_dw.fact_sales"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE VIEW v_sales_summary AS
    SELECT u.id, s.amount
    FROM public.users u
    JOIN remote_dw.fact_sales s ON u.id = s.user_id;
    """
    ledger.audit_and_record("v_sales_summary", sql)
    cov = ledger.compute_coverage("v_sales_summary")

    assert cov.is_fully_assessed is False
    assert cov.unavailable_dependencies > 0
    assert cov.coverage_pct < 100.0
    assert any("remote_dw.fact_sales" in b for b in cov.blockers)


def test_multiple_blocking_identifiers_and_call_sites():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables=set())
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE multi_dyn(src_tbl text, dst_tbl text) AS $$
    BEGIN
        EXECUTE format('SELECT * FROM %I', src_tbl);
        EXECUTE 'INSERT INTO ' || quote_ident(dst_tbl) || ' VALUES (1)';
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("multi_dyn", sql)
    unknowns = ledger.get_unknown_dynamic_lineages("multi_dyn")

    assert len(unknowns) >= 2 or (len(unknowns) == 1 and len(unknowns[0].blocking_identifiers) >= 2)
    all_identifiers = {ident for u in unknowns for ident in u.blocking_identifiers}
    assert "src_tbl" in all_identifiers
    assert "dst_tbl" in all_identifiers


def test_unknown_dynamic_lineage_to_dict():
    cs = CallSite(file_path="src/queries.sql", line_number=42, routine_name="dyn_exec", statement="EXECUTE sql")
    rec = UnknownDynamicLineage(
        lineage_id="lineage-1",
        routine_name="dyn_exec",
        blocking_identifiers=["sql"],
        call_sites=[cs],
        required_runtime_evidence=["approved runtime trace of sql statement"],
        construct_kind="dynamic_sql",
        reason="Unresolved dynamic SQL string",
    )
    d = rec.to_dict()
    assert d["lineage_id"] == "lineage-1"
    assert d["routine_name"] == "dyn_exec"
    assert d["blocking_identifiers"] == ["sql"]
    assert d["call_sites"][0]["line_number"] == 42
    assert d["required_runtime_evidence"] == ["approved runtime trace of sql statement"]
    assert d["resolved"] is False


def test_compute_all_coverage():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.t1"},
        supported_extensions={"postgis": "3.3.0"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    ledger.audit_and_record("static_r", "SELECT * FROM public.t1;")
    ledger.audit_and_record("dyn_r", "EXECUTE format('SELECT * FROM %I', dyn_tbl);")

    all_cov = ledger.compute_all_coverage()
    assert "static_r" in all_cov
    assert "dyn_r" in all_cov
    assert all_cov["static_r"].is_fully_assessed is True
    assert all_cov["dyn_r"].is_fully_assessed is False


def test_plpgsql_variable_execute():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables={"public.items"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION run_dynamic(v_query text, v_id int) RETURNS void AS $$
    BEGIN
        EXECUTE v_query USING v_id;
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("run_dynamic", sql)
    cov = ledger.compute_coverage("run_dynamic")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("run_dynamic")
    assert len(unknowns) >= 1
    assert "v_query" in unknowns[0].blocking_identifiers
    assert unknowns[0].construct_kind == "dynamic_sql"


def test_oracle_dynamic_cursor():
    cat = VersionedCatalogue(engine="oracle", version="19c", tables={"SCOTT.EMP"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE open_dyn_cursor(p_sql IN VARCHAR2, p_cur OUT SYS_REFCURSOR) IS
    BEGIN
        OPEN p_cur FOR p_sql;
    END;
    """
    ledger.audit_and_record("open_dyn_cursor", sql)
    cov = ledger.compute_coverage("open_dyn_cursor")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("open_dyn_cursor")
    assert len(unknowns) >= 1
    assert "p_sql" in unknowns[0].blocking_identifiers


def test_oracle_dbms_sql_parse():
    cat = VersionedCatalogue(engine="oracle", version="19c", tables={"SCOTT.EMP"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE run_dbms_sql(c IN NUMBER, v_sql IN VARCHAR2) IS
    BEGIN
        dbms_sql.parse(c, v_sql, dbms_sql.native);
    END;
    """
    ledger.audit_and_record("run_dbms_sql", sql)
    cov = ledger.compute_coverage("run_dbms_sql")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("run_dbms_sql")
    assert len(unknowns) >= 1
    assert "v_sql" in unknowns[0].blocking_identifiers


def test_timescaledb_extension_detection():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.metrics"},
        supported_extensions={},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION setup_ts() RETURNS void AS $$
    BEGIN
        PERFORM create_hypertable('metrics', 'time');
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("setup_ts", sql)
    cov = ledger.compute_coverage("setup_ts")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("setup_ts")
    assert any(u.construct_kind == "unsupported_extension" for u in unknowns)
    ext_rec = next(u for u in unknowns if u.construct_kind == "unsupported_extension")
    assert "timescaledb" in ext_rec.blocking_identifiers


def test_orafce_extension_detection():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.users"},
        supported_extensions={},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE log_message(msg text) AS $$
    BEGIN
        PERFORM dbms_output.put_line(msg);
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("log_message", sql)
    cov = ledger.compute_coverage("log_message")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("log_message")
    assert any("orafce" in u.blocking_identifiers for u in unknowns)


def test_dblink_extension_detection():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.remote_mirror"},
        supported_extensions={},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION sync_remote() RETURNS void AS $$
    BEGIN
        PERFORM dblink_connect('conn', 'dbname=foreign host=10.0.0.5');
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("sync_remote", sql)
    cov = ledger.compute_coverage("sync_remote")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("sync_remote")
    assert any("dblink" in u.blocking_identifiers for u in unknowns)


def test_multi_identifier_format_plpgsql():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables=set())
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE PROCEDURE dyn_insert(sch text, tbl text, col text, val text) AS $$
    BEGIN
        EXECUTE format('INSERT INTO %I.%I (%I) VALUES ($1)', sch, tbl, col) USING val;
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("dyn_insert", sql)
    cov = ledger.compute_coverage("dyn_insert")

    assert cov.is_fully_assessed is False
    assert cov.coverage_pct < 100.0
    unknowns = ledger.get_unknown_dynamic_lineages("dyn_insert")
    assert len(unknowns) >= 1
    idents = unknowns[0].blocking_identifiers
    assert "sch" in idents
    assert "tbl" in idents
    assert "col" in idents


def test_resolution_isolation_across_routines():
    cat = VersionedCatalogue(engine="postgres", version="15.2", tables={"public.t1", "public.t2"})
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    ledger.audit_and_record("proc_a", "EXECUTE format('SELECT * FROM %I', tbl_a);")
    ledger.audit_and_record("proc_b", "EXECUTE format('SELECT * FROM %I', tbl_b);")

    trace_a = RuntimeTrace(
        trace_id="tr-a",
        caller="proc_a",
        concrete_sql="SELECT * FROM public.t1",
        accessed_entities=["public.t1"],
        approved=True,
    )
    ledger.record_runtime_trace(trace_a)
    ledger.attempt_resolution("proc_a")

    cov_a = ledger.compute_coverage("proc_a")
    cov_b = ledger.compute_coverage("proc_b")

    assert cov_a.is_fully_assessed is True
    assert cov_a.coverage_pct == 100.0

    assert cov_b.is_fully_assessed is False
    assert cov_b.coverage_pct < 100.0
    assert len(ledger.get_unknown_dynamic_lineages("proc_b", unresolved_only=True)) == 1


def test_safe_test_ip_usage():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.2",
        tables={"public.audit_log"},
        supported_extensions={"dblink": "1.2"},
    )
    ledger = LineageEvidenceLedger(target_catalogue=cat)

    sql = """
    CREATE OR REPLACE FUNCTION audit_remote() RETURNS void AS $$
    BEGIN
        PERFORM dblink_connect('conn', 'host=127.0.0.1 dbname=test');
    END;
    $$ LANGUAGE plpgsql;
    """
    ledger.audit_and_record("audit_remote", sql)
    cov = ledger.compute_coverage("audit_remote")
    assert cov.is_fully_assessed is True
    assert cov.coverage_pct == 100.0


def test_models_to_dict_and_catalogue_helpers():
    cat = VersionedCatalogue(
        engine="postgres",
        version="15.0",
        tables={"public.users"},
        views={"public.v_users"},
        routines={"public.get_users"},
        supported_extensions={"postgis": "3.3.0"},
        unavailable_dependencies={"ext.legacy_data"},
    )
    assert cat.has_entity("public.users") is True
    assert cat.has_entity("users") is True
    assert cat.has_entity("public.v_users") is True
    assert cat.has_entity("nonexistent") is False
    assert cat.has_extension("postgis") is True
    assert cat.has_extension("postgis", "3.0.0") is True
    assert cat.has_extension("postgis", "3.4.0") is False
    assert cat.has_extension("timescaledb") is False

    d = cat.to_dict()
    assert d["engine"] == "postgres"
    assert "public.users" in d["tables"]

    sr = SyntaxReference(caller="r1", target_type="table", target_name="public.users")
    assert sr.to_dict()["target_name"] == "public.users"

    rt = RuntimeTrace(
        trace_id="t1",
        caller="r1",
        concrete_sql="SELECT * FROM public.users",
        accessed_entities=["public.users"],
        approved=True,
    )
    assert rt.to_dict()["approved"] is True


def test_lineage_evidence_rungs_under_watched():
    with watched():
        # lineage_evidence_rungs:
        # 1. certified_static_lineage
        c1 = decide_lineage_evidence({"static_lineage": True})
        assert c1.rung.name == "certified_static_lineage"

        # 2. approved_runtime_trace_lineage
        c2 = decide_lineage_evidence({"static_lineage": False, "runtime_trace": True})
        assert c2.rung.name == "approved_runtime_trace_lineage"

        # 3. rehearsal_observed_lineage
        c3 = decide_lineage_evidence({"static_lineage": False, "runtime_trace": False, "rehearsal_observed": True})
        assert c3.rung.name == "rehearsal_observed_lineage"

        # 4. provisional_bounded_lineage
        c4 = decide_lineage_evidence({"static_lineage": False, "runtime_trace": False, "rehearsal_observed": False, "provisional_bounded": True})
        assert c4.rung.name == "provisional_bounded_lineage"

        # 5. refuse_untracked_dynamic_lineage
        c5 = decide_lineage_evidence({"static_lineage": False, "runtime_trace": False, "rehearsal_observed": False, "provisional_bounded": False})
        assert c5.rung.name == "refuse_untracked_dynamic_lineage"

        # 6. refuse fallback
        c6 = decide_lineage_evidence({"static_lineage": False, "runtime_trace": False, "rehearsal_observed": False, "provisional_bounded": False, "allow_untracked_dynamic": True})
        assert c6.rung is None

        # lineage_coverage_rungs:
        # 1. full_lineage_coverage
        p1 = decide_lineage_coverage({"full_coverage": True})
        assert p1.rung.name == "full_lineage_coverage"

        # 2. trace_supplemented_coverage
        p2 = decide_lineage_coverage({"full_coverage": False, "trace_supplemented": True})
        assert p2.rung.name == "trace_supplemented_coverage"

        # 3. bounded_gap_coverage
        p3 = decide_lineage_coverage({"full_coverage": False, "trace_supplemented": False, "bounded_gap": True})
        assert p3.rung.name == "bounded_gap_coverage"

        # 4. honest_partial_coverage
        p4 = decide_lineage_coverage({"full_coverage": False, "trace_supplemented": False, "bounded_gap": False, "honest_partial": True})
        assert p4.rung.name == "honest_partial_coverage"

        # 5. refuse_opaque_lineage_downcast
        p5 = decide_lineage_coverage({"full_coverage": False, "trace_supplemented": False, "bounded_gap": False, "honest_partial": False})
        assert p5.rung.name == "refuse_opaque_lineage_downcast"

        # 6. refuse fallback
        p6 = decide_lineage_coverage({"full_coverage": False, "trace_supplemented": False, "bounded_gap": False, "honest_partial": False, "allow_opaque_downcast": True})
        assert p6.rung is None


def test_versioned_catalogue_dynamic_env(monkeypatch):
    monkeypatch.setenv("MIGKIT_TARGET_ENGINE", "mysql")
    monkeypatch.setenv("MIGKIT_TARGET_VERSION", "8.4")
    cat = VersionedCatalogue.from_environment()
    assert cat.engine == "mysql"
    assert cat.version == "8.4"

