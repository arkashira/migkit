import importlib

__version__ = "0.2.0"

__all__ = (
    "ack_lattice",
    "adaptive_bounds",
    "ai_proof_loop",
    "backfill_race",
    "cache_semantics",
    "cancel_recovery",
    "conflict_semantics",
    "control_upgrade",
    "conversion_ledger",
    "cost_envelope",
    "decomp_budget",
    "demand_benchmark",
    "document_model",
    "dynamic_sql_tracer",
    "effect_checkpoint",
    "etl_bi_waves",
    "exit_portability",
    "grants",
    "handoff_overlap",
    "history_contract",
    "identity_semantics",
    "lineage_evidence",
    "lob_transaction",
    "lock_capability",
    "mapping_proof",
    "merkle",
    "offset_diag",
    "partial_access",
    "partial_images",
    "placement_advice",
    "principal_migration",
    "private_proof",
    "process_memory",
    "progress_cause",
    "query_guard",
    "query_regression",
    "quiet_source",
    "resume_scope",
    "retention_controller",
    "reverse_contract",
    "ring_buffer",
    "rollback",
    "rollback_set",
    "rule_generalization",
    "schema_drift",
    "schema_epoch_plan",
    "shadow_resync",
    "shard_ddl",
    "snapshot_state",
    "snapshot_watermark",
    "source_role",
    "stream_metadata",
    "support_reproducer",
    "target_tuning_contract",
    "traffic_state",
)


def __getattr__(name):
    if name in __all__:
        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

