"""Tests for Integrated Database, ETL, and BI Migration Wave Planner (backlog RCH22, row 630).

Validates:
1. Parsers for dbt, Airflow, Metabase, and Looker artefacts.
2. Lineage graph compilation and wave prerequisites.
3. Transformed results validation on aligned inputs with Merkle bucket diffing.
4. Connection repointing with separate external deployment authorization.
5. Invariant: A database table used by an ETL job and dashboard cannot be marked
   whole-workload ready while either dependency is unconverted or unverified.
6. Universal typed durable resume in O(1).
7. Mutual exclusion lease lock (.run.lock) with heartbeat.
8. Adaptive AIMD validation batcher and Decision Engine wrapper.
9. Pure UNIX style, zero emojis/glyphs, 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
"""

import datetime
import decimal
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from migkit.etl_bi_waves import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    CAT_BI_ARTEFACT,
    CAT_DATABASE_TABLE,
    CAT_ETL_JOB,
    FAMILY_AIRFLOW,
    FAMILY_DBT,
    FAMILY_LOOKER,
    FAMILY_METABASE,
    FAMILY_TABLE,
    STATE_CONVERTED,
    STATE_FAILED,
    STATE_REPOINTED,
    STATE_UNCONVERTED,
    STATE_VERIFIED,
    AdaptiveValidationBatcher,
    AirflowDagParser,
    ArtefactRegistry,
    DbtModelParser,
    DecisionEngineVerificationWrapper,
    ETLBIWavePlanner,
    LineageGraph,
    LookerModelParser,
    MetabaseModelParser,
    WaveCheckpoint,
)


@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test_etl_bi_waves_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_dbt_model_parser():
    parser = DbtModelParser()
    sql = """
    WITH source_data AS (
        SELECT id, customer_id, amount
        FROM {{ source('raw_store', 'raw_orders') }}
    ),
    customers AS (
        SELECT id, name FROM {{ ref('stg_customers') }}
    )
    SELECT s.id, c.name, s.amount
    FROM source_data s
    JOIN customers c ON s.customer_id = c.id
    """
    node = parser.parse("etl:dbt:orders_fact", sql, metadata={"name": "orders_fact"})
    assert node.name == "orders_fact"
    assert node.category == CAT_ETL_JOB
    assert node.family == FAMILY_DBT
    assert "table:raw_store.raw_orders" in node.inputs
    assert "table:stg_customers" in node.inputs
    assert node.outputs == ["table:orders_fact"]
    assert len(node.content_hash) == 64


def test_airflow_dag_parser():
    parser = AirflowDagParser()
    content = {
        "sql": """
        INSERT INTO analytics.daily_revenue
        SELECT date_trunc('day', created_at) as day, sum(amount) as total
        FROM public.orders
        JOIN public.stores ON orders.store_id = stores.id
        GROUP BY 1
        """,
        "task_id": "aggregate_daily_revenue",
    }
    node = parser.parse("etl:airflow:aggregate_daily_revenue", content)
    assert node.name == "aggregate_daily_revenue"
    assert node.family == FAMILY_AIRFLOW
    assert "table:public.orders" in node.inputs
    assert "table:public.stores" in node.inputs
    assert "table:analytics.daily_revenue" in node.outputs


def test_metabase_model_parser():
    parser = MetabaseModelParser()
    # Native query card
    content = {
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT order_id, product_name, revenue FROM mart.sales WHERE status = 'COMPLETED'",
            },
            "database": 2,
        },
        "name": "completed_sales_card",
    }
    node = parser.parse("bi:metabase:completed_sales_card", content)
    assert node.name == "completed_sales_card"
    assert node.category == CAT_BI_ARTEFACT
    assert node.family == FAMILY_METABASE
    assert "table:mart.sales" in node.inputs
    assert node.metadata.get("database_id") == 2


def test_looker_model_parser():
    parser = LookerModelParser()
    lookml = """
    view: customer_lifetime_value {
      sql_table_name: mart.customer_ltv ;
      dimension: customer_id {
        type: string
        sql: ${TABLE}.customer_id ;;
      }
      measure: total_ltv {
        type: sum
        sql: ${TABLE}.ltv_amount ;;
      }
    }
    """
    node = parser.parse("bi:looker:customer_lifetime_value", lookml, metadata={"name": "customer_lifetime_value"})
    assert node.name == "customer_lifetime_value"
    assert node.category == CAT_BI_ARTEFACT
    assert node.family == FAMILY_LOOKER
    assert "table:mart.customer_ltv" in node.inputs


def test_lineage_graph_and_cycle_detection():
    g = LineageGraph()
    # Circular dependency A -> B -> C -> A
    from migkit.etl_bi_waves import LineageNode
    n1 = LineageNode("n1", "n1", CAT_ETL_JOB, FAMILY_DBT, inputs=["n3"], outputs=["n1"])
    n2 = LineageNode("n2", "n2", CAT_ETL_JOB, FAMILY_DBT, inputs=["n1"], outputs=["n2"])
    n3 = LineageNode("n3", "n3", CAT_ETL_JOB, FAMILY_DBT, inputs=["n2"], outputs=["n3"])
    g.add_node(n1)
    g.add_node(n2)
    g.add_node(n3)

    cycles = g.detect_cycles()
    assert len(cycles) > 0


def test_wave_compilation_and_prerequisites(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir, hop_name="sales_migration")

    # Wave 0: Base tables
    planner.add_table_to_scope("users", data_synced=True)
    planner.add_table_to_scope("transactions", data_synced=True)

    # Wave 1: ETL transforming base tables into intermediate mart
    dbt_content = "SELECT t.id, u.name, t.amount FROM {{ ref('transactions') }} t JOIN {{ ref('users') }} u ON t.user_id = u.id"
    planner.add_artefact_to_scope(FAMILY_DBT, "dbt:stg_user_tx", dbt_content, metadata={"name": "stg_user_tx"})

    # Wave 2: BI dashboard querying stg_user_tx
    metabase_content = {
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM stg_user_tx"},
        },
        "name": "user_tx_dashboard",
    }
    planner.add_artefact_to_scope(FAMILY_METABASE, "bi:metabase:user_tx_dashboard", metabase_content)

    waves = planner.compile_waves()
    assert len(waves) >= 3

    # Wave 0 must contain users and transactions
    assert "table:users" in waves[0].tables
    assert "table:transactions" in waves[0].tables

    # Wave 1 must contain dbt:stg_user_tx
    wave_1 = waves[1]
    assert any("stg_user_tx" in etl for etl in wave_1.etl_jobs)
    assert 0 in wave_1.prerequisites

    # Wave 2 (or subsequent) must contain the dashboard
    bi_wave = [w for w in waves if any("user_tx_dashboard" in bi for bi in w.bi_artefacts)][0]
    assert bi_wave.wave_index > 0


def test_whole_workload_readiness_invariant_core(tmp_dir):
    """VERIFIES CORE INVARIANT:

    A database table used by an ETL job and dashboard cannot be marked whole-workload
    ready while either dependency is unconverted or unverified.
    """
    planner = ETLBIWavePlanner(run_dir=tmp_dir, hop_name="core_invariant_test")

    # 1. Base table in scope
    tbl_node = planner.add_table_to_scope("orders", data_synced=True)
    assert tbl_node.data_synced is True

    # 2. Add ETL job depending on 'orders'
    etl_sql = "SELECT id, amount FROM {{ ref('orders') }}"
    etl_node = planner.add_artefact_to_scope(FAMILY_DBT, "dbt:stg_orders", etl_sql, metadata={"name": "stg_orders"})

    # 3. Add BI dashboard depending on 'orders'
    bi_query = {"dataset_query": {"type": "native", "native": {"query": "SELECT count(*) FROM orders"}}, "name": "orders_dash"}
    bi_node = planner.add_artefact_to_scope(FAMILY_METABASE, "bi:metabase:orders_dash", bi_query)

    # Initial state: Both ETL and BI are unconverted and unverified
    st1 = planner.evaluate_table_readiness("orders")
    assert st1.whole_workload_ready is False
    assert st1.care_badge == CARE_ACTION
    assert any("ETL job stg_orders" in r and "unconverted" in r for r in st1.blocking_reasons)
    assert any("BI artefact orders_dash" in r and "unconverted" in r for r in st1.blocking_reasons)

    # Case B: Convert ETL, but do NOT verify it yet
    etl_node.conversion_state = STATE_CONVERTED
    etl_node.verification_passed = False

    st2 = planner.evaluate_table_readiness("orders")
    assert st2.whole_workload_ready is False
    assert st2.care_badge == CARE_ACTION
    assert any("ETL job stg_orders" in r and "unverified" in r for r in st2.blocking_reasons)

    # Case C: Verify ETL, but BI is still unconverted
    etl_node.verification_passed = True
    etl_node.conversion_state = STATE_VERIFIED

    st3 = planner.evaluate_table_readiness("orders")
    assert st3.whole_workload_ready is False
    assert st3.care_badge == CARE_ACTION
    assert any("BI artefact orders_dash" in r and "unconverted" in r for r in st3.blocking_reasons)

    # Case D: Convert BI, but do NOT verify it yet
    bi_node.conversion_state = STATE_CONVERTED
    bi_node.verification_passed = False

    st4 = planner.evaluate_table_readiness("orders")
    assert st4.whole_workload_ready is False
    assert st4.care_badge == CARE_ACTION
    assert any("BI artefact orders_dash" in r and "unverified" in r for r in st4.blocking_reasons)

    # Case E: Now verify BI as well -> ALL MET!
    bi_node.verification_passed = True
    bi_node.conversion_state = STATE_VERIFIED

    st5 = planner.evaluate_table_readiness("orders")
    assert st5.whole_workload_ready is True
    assert st5.care_badge == CARE_PASS
    assert len(st5.blocking_reasons) == 0

    # Case F: If table data itself loses sync, whole workload becomes blocked immediately
    tbl_node.data_synced = False
    st6 = planner.evaluate_table_readiness("orders")
    assert st6.whole_workload_ready is False
    assert st6.care_badge == CARE_ACTION
    assert any("database data for table orders not yet synced" in r for r in st6.blocking_reasons)


def test_transformed_results_validation_merkle_and_metrics(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir)
    etl_node = planner.add_artefact_to_scope(
        FAMILY_DBT,
        "dbt:agg_orders",
        "SELECT id, total, tax FROM orders",
        metadata={"name": "agg_orders"},
    )

    src_records = [
        {"id": 1, "total": 100.0, "tax": 10.0},
        {"id": 2, "total": 200.0, "tax": 20.0},
        {"id": 3, "total": 300.0, "tax": 30.0},
    ]

    # 1. Exact match on aligned inputs
    tgt_records_exact = [
        {"id": 1, "total": 100.0, "tax": 10.0},
        {"id": 2, "total": 200.0, "tax": 20.0},
        {"id": 3, "total": 300.0, "tax": 30.0},
    ]
    res_exact = planner.validate_transformed_results(
        "etl:dbt:agg_orders",
        src_records,
        tgt_records_exact,
        key_column="id",
        metric_columns=["total", "tax"],
    )
    assert res_exact.passed is True
    assert res_exact.care_badge == CARE_PASS
    assert res_exact.merkle_buckets_matched == 16
    assert etl_node.conversion_state == STATE_VERIFIED

    # 2. Mismatch in row counts
    tgt_records_missing = [
        {"id": 1, "total": 100.0, "tax": 10.0},
        {"id": 2, "total": 200.0, "tax": 20.0},
    ]
    res_missing = planner.validate_transformed_results(
        "etl:dbt:agg_orders",
        src_records,
        tgt_records_missing,
        key_column="id",
        metric_columns=["total", "tax"],
    )
    assert res_missing.passed is False
    assert res_missing.care_badge == CARE_ACTION
    assert etl_node.conversion_state == STATE_FAILED

    # 3. Numeric drift within small epsilon / review
    tgt_records_drift = [
        {"id": 1, "total": 100.005, "tax": 10.0},
        {"id": 2, "total": 200.0, "tax": 20.0},
        {"id": 3, "total": 300.0, "tax": 30.0},
    ]
    res_drift = planner.validate_transformed_results(
        "etl:dbt:agg_orders",
        src_records,
        tgt_records_drift,
        key_column="id",
        metric_columns=["total", "tax"],
        epsilon=1e-5,
    )
    assert res_drift.passed is False
    assert res_drift.care_badge == CARE_REVIEW


def test_connection_repointing_separate_authorization(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir)
    bi_node = planner.add_artefact_to_scope(
        FAMILY_METABASE,
        "bi:metabase:exec_report",
        {"dataset_query": {"type": "native", "native": {"query": "SELECT 1"}}, "name": "exec_report"},
        is_external_deployment=True,
        deployment_authorized=False,
    )

    # 1. Attempt repointing without explicit authorization -> BLOCKED
    res_unauth = planner.coordinate_repointing(
        "bi:metabase:exec_report",
        target_endpoint="10.0.0.99",
        authorize_deployment=False,
    )
    assert res_unauth.authorized is False
    assert res_unauth.executed is False
    assert res_unauth.care_badge == CARE_REVIEW
    assert "pending explicit authorization" in res_unauth.care_reason
    assert bi_node.repointed is False

    # 2. Authorize deployment explicitly -> SUCCESS
    res_auth = planner.coordinate_repointing(
        "bi:metabase:exec_report",
        target_endpoint="10.0.0.99",
        authorize_deployment=True,
    )
    assert res_auth.authorized is True
    assert res_auth.executed is True
    assert res_auth.care_badge == CARE_PASS
    assert bi_node.repointed is True
    assert bi_node.target_connection == "10.0.0.99"
    assert bi_node.conversion_state == STATE_REPOINTED


def test_universal_typed_durable_resume_o1(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir)
    planner.add_table_to_scope("accounts", data_synced=True)
    planner.add_artefact_to_scope(
        FAMILY_AIRFLOW,
        "etl:airflow:sync_accounts",
        {"sql": "INSERT INTO target.accounts SELECT * FROM accounts", "task_id": "sync_accounts"},
    )

    # Mark node verified
    planner.graph.nodes["etl:airflow:sync_accounts"].verification_passed = True
    planner.graph.nodes["etl:airflow:sync_accounts"].conversion_state = STATE_VERIFIED

    # Checkpoint with native types (datetime, Decimal, UUID)
    chk_time = datetime.datetime(2026, 10, 17, 10, 0, 0, tzinfo=datetime.timezone.utc)
    chk_uuid = uuid.uuid4()
    chk_dec = decimal.Decimal("12345.67")

    cursors = {
        "sync_time": chk_time,
        "run_uuid": chk_uuid,
        "batch_balance": chk_dec,
    }

    chk = planner.save_checkpoint(checkpoint_dir=tmp_dir, typed_cursors=cursors)
    assert chk.checkpoint_id

    # Load checkpoint and verify exact type preservation
    loaded = planner.load_checkpoint(tmp_dir)
    assert loaded is not None
    assert loaded.typed_cursors["sync_time"] == chk_time
    assert loaded.typed_cursors["run_uuid"] == chk_uuid
    assert loaded.typed_cursors["batch_balance"] == chk_dec

    # Evaluate resume in O(1): Should pass
    can_resume, status, details = planner.evaluate_resume_o1(tmp_dir)
    assert can_resume is True
    assert status == "resume_valid"
    assert details["care_badge"] == CARE_PASS

    # Invalidate by changing content hash of node
    planner.graph.nodes["etl:airflow:sync_accounts"].content_hash = "tampered_hash_value"
    can_resume_bad, status_bad, details_bad = planner.evaluate_resume_o1(tmp_dir)
    assert can_resume_bad is False
    assert status_bad == "revalidation_required"
    assert details_bad["care_badge"] == CARE_ACTION
    assert "etl:airflow:sync_accounts" in details_bad["invalidated_nodes"]


def test_mutual_exclusion_lease_lock(tmp_dir):
    planner1 = ETLBIWavePlanner(run_dir=tmp_dir, hop_name="planner_primary")
    planner2 = ETLBIWavePlanner(run_dir=tmp_dir, hop_name="planner_secondary")

    # Planner 1 acquires lease
    assert planner1.acquire_lease() is True

    # Planner 2 tries to acquire lease on same dir -> rejected
    assert planner2.acquire_lease(timeout_sec=0.2) is False

    # Heartbeat renewal
    planner1.renew_lease()
    lock_file = tmp_dir / ".run.lock"
    assert lock_file.exists()
    payload = json.loads(lock_file.read_text(encoding="ascii"))
    assert payload["hop"] == "planner_primary"
    assert payload["status"] == "active"

    # Planner 1 releases lease
    planner1.release_lease()

    # Planner 2 can now acquire lease
    assert planner2.acquire_lease(timeout_sec=1.0) is True
    planner2.release_lease()


def test_adaptive_validation_batcher_aimd():
    batcher = AdaptiveValidationBatcher(
        initial_batch_size=1000,
        min_batch_size=200,
        max_batch_size=5000,
        additive_step=500,
        backoff_factor=0.5,
        target_latency_sec=0.100,
    )

    # Fast successful checks -> Additive Increase after 2 consecutive
    batcher.record_chunk_outcome(0.050, passed=True)
    assert batcher.current_batch_size == 1000
    b1 = batcher.record_chunk_outcome(0.060, passed=True)
    assert b1 == 1500
    b2 = batcher.record_chunk_outcome(0.070, passed=True)
    assert b2 == 2000

    # Latency breach or failure -> Multiplicative Decrease
    b3 = batcher.record_chunk_outcome(0.250, passed=True)  # Latency breach
    assert b3 == 1000  # 2000 * 0.5
    b4 = batcher.record_chunk_outcome(0.050, passed=False)  # Verification failure
    assert b4 == 500  # 1000 * 0.5

    # Cannot drop below min_batch_size
    batcher.record_chunk_outcome(0.050, passed=False)
    batcher.record_chunk_outcome(0.050, passed=False)
    assert batcher.current_batch_size == 200


def test_decision_engine_verification_wrapper(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir)
    planner.add_artefact_to_scope(
        FAMILY_DBT,
        "dbt:summary",
        "SELECT id, val FROM items",
        metadata={"name": "summary"},
    )
    wrapper = DecisionEngineVerificationWrapper(preferred_engine="datafold")
    src = [{"id": 1, "val": 10}]
    tgt = [{"id": 1, "val": 10}]

    res = wrapper.verify_transformation(planner, "dbt:summary", src, tgt)
    assert res.passed is True
    assert res.care_badge == CARE_PASS
    assert res.diff_details.get("decision_engine") == "datafold"


def test_cold_start_byte_weighted_eta(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir)
    planner.add_table_to_scope("big_orders", data_synced=True)
    planner.add_artefact_to_scope(FAMILY_DBT, "dbt:orders_mart", "SELECT * FROM big_orders", metadata={"name": "orders_mart"})
    waves = planner.compile_waves()

    sizes = {"big_orders": 250 * 1024 * 1024}  # 250MB
    eta = planner.calculate_wave_eta_seconds(waves[0], byte_size_by_table=sizes, measured_mb_per_sec=25.0)
    # 250MB / 25MB/s = 10s + overhead
    assert eta >= 10.0


def test_pure_unix_terse_status_report(tmp_dir):
    planner = ETLBIWavePlanner(run_dir=tmp_dir, hop_name="orders_to_snowflake")
    planner.add_table_to_scope("inventory", data_synced=False)
    planner.add_artefact_to_scope(
        FAMILY_LOOKER,
        "bi:looker:inventory_view",
        "view: inventory { sql_table_name: inventory; }",
        metadata={"name": "inventory_view"},
    )

    report = planner.format_status_report()
    assert "[REPORT] ETL/BI MIGRATION WAVES: hop=orders_to_snowflake" in report
    assert "[ACTION] TABLE inventory: whole_workload=BLOCKED" in report
    # Pure ASCII assertion
    report.encode("ascii")
