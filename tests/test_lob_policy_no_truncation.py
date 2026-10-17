"""Tests for choosing LOB policy without truncation (Row 610: RCH02.lob-policy).

Proves:
1. Every decide.Rung in `lob_policy.lob_size_probe_rungs` and
   `lob_policy.lob_policy_rungs` is exercised on both `holds` and `refuses`
   under `tests.rung_branches.watched()`, declared in `PROVES_RUNGS`.
2. Inline, streamed, locator, chunked, and separately fetched LOB paths are
   selected under value-size, packet, memory, and consistency facts.
3. Truncating or stale-locator paths are rejected under exact contracts; size
   probes are bounded by measured statistics/spread and unknown maxima stay
   conservative (including when storage compression hides true octet length).
4. Mixed NULL, empty, binary, multibyte UTF-8, compressed, and extreme LOBs
   under concurrent updates and injected failures produce exact final bytes,
   bounded worker memory, and no premature checkpoint.
"""

from __future__ import annotations

import datetime
import decimal
import hashlib
import pathlib
import uuid
from typing import Any, Dict, List, Tuple

import pytest

import tests.rung_branches
from migkit import decide, lob_policy, pieces
from migkit.lob_policy import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    AdaptiveLobPieceController,
    CareBadge,
    ConcurrentLobMutationError,
    LeaseLockError,
    LobKind,
    LobMemoryBudgetExceededError,
    LobPathMode,
    LobPolicyCheckpointStore,
    LobPolicyError,
    LobPolicyLease,
    LobPolicyPlan,
    LobPolicyToolWrapper,
    LobSizeProbeMode,
    LobTruncationError,
    LobUnitCheckpoint,
    NonTruncatingLobExecutor,
    SourceLobRow,
    StaleLocatorError,
    UnsafeEndpointError,
    choose_lob_policy,
    choose_lob_size_probe,
    lob_policy_rungs,
    lob_size_probe_rungs,
    payload_octet_length,
    payload_sha256,
    slice_bytes_by_budget,
    slice_utf8_by_byte_budget,
    validate_safe_endpoint,
    wire_bytes_required,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("lob_policy", "lob_size_probe_rungs", "exact_octet_scan", "holds"),
    ("lob_policy", "lob_size_probe_rungs", "exact_octet_scan", "refuses"),
    ("lob_policy", "lob_size_probe_rungs", "bounded_sample_spread", "holds"),
    ("lob_policy", "lob_size_probe_rungs", "bounded_sample_spread", "refuses"),
    ("lob_policy", "lob_size_probe_rungs", "conservative_unknown_maxima", "holds"),
    ("lob_policy", "lob_size_probe_rungs", "conservative_unknown_maxima", "refuses"),
    ("lob_policy", "lob_policy_rungs", "inline_row_lob", "holds"),
    ("lob_policy", "lob_policy_rungs", "inline_row_lob", "refuses"),
    ("lob_policy", "lob_policy_rungs", "streamed_cursor_lob", "holds"),
    ("lob_policy", "lob_policy_rungs", "streamed_cursor_lob", "refuses"),
    ("lob_policy", "lob_policy_rungs", "locator_snapshot_lob", "holds"),
    ("lob_policy", "lob_policy_rungs", "locator_snapshot_lob", "refuses"),
    ("lob_policy", "lob_policy_rungs", "chunked_piece_lob", "holds"),
    ("lob_policy", "lob_policy_rungs", "chunked_piece_lob", "refuses"),
    ("lob_policy", "lob_policy_rungs", "separate_keyed_fetch_lob", "holds"),
    ("lob_policy", "lob_policy_rungs", "separate_keyed_fetch_lob", "refuses"),
]


# -----------------------------------------------------------------------------
# 1. Rung Branch Coverage Under tests.rung_branches.watched()
# -----------------------------------------------------------------------------

def test_every_lob_size_probe_rung_holds_and_refuses_under_watched() -> None:
    """Exercises both holds and refuses for every rung in lob_size_probe_rungs."""
    with watched() as seen:
        # 1. exact_octet_scan holds (and bounded_sample_spread + conservative_unknown_maxima hold)
        c_exact, res_exact = choose_lob_size_probe(
            {
                "total_rows": 50_000,
                "scan_rate_rows_sec": 200_000.0,
                "probe_budget_seconds": 1.0,
                "supports_octet_length_scan": True,
                "compressed_metadata_only": False,
                "sample_octet_lengths": [1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072],
                "unknown_maxima": False,
            }
        )
        assert c_exact.path == "exact_octet_scan"
        assert res_exact.probe_mode == LobSizeProbeMode.EXACT_OCTET_SCAN
        assert res_exact.exact is True

        # 2. exact_octet_scan refuses (table too large for exact scan budget), bounded_sample_spread holds
        c_sample, res_sample = choose_lob_size_probe(
            {
                "total_rows": 50_000_000,
                "scan_rate_rows_sec": 100_000.0,
                "probe_budget_seconds": 0.5,
                "supports_octet_length_scan": True,
                "compressed_metadata_only": False,
                "sample_octet_lengths": [4000, 8000, 12000, 16000, 20000, 24000, 32000, 64000],
                "unknown_maxima": False,
            }
        )
        assert c_sample.path == "bounded_sample_spread"
        assert res_sample.probe_mode == LobSizeProbeMode.BOUNDED_SAMPLE_SPREAD
        assert res_sample.conservative_max_octets > res_sample.observed_max_octets

        # 3. bounded_sample_spread refuses (compressed_metadata_only=True or unknown_maxima=True),
        #    conservative_unknown_maxima holds
        c_unknown, res_unknown = choose_lob_size_probe(
            {
                "total_rows": 10_000_000,
                "supports_octet_length_scan": False,
                "compressed_metadata_only": True,
                "unknown_maxima": True,
                "protocol_hard_ceiling_bytes": 32 * 1024 * 1024,
            }
        )
        assert c_unknown.path == "conservative_unknown_maxima"
        assert res_unknown.probe_mode == LobSizeProbeMode.CONSERVATIVE_UNKNOWN_MAXIMA
        assert res_unknown.conservative_max_octets >= 32 * 1024 * 1024

        # 4. conservative_unknown_maxima refuses when protocol_hard_ceiling_bytes <= 0
        c_refuse_all = decide.climb(
            "lob size probe",
            lob_size_probe_rungs(),
            decide.Facts(
                {
                    "supports_octet_length_scan": False,
                    "compressed_metadata_only": True,
                    "unknown_maxima": True,
                    "protocol_hard_ceiling_bytes": 0,
                }
            ),
        )
        assert c_refuse_all.rung is None

    for rung_name in (
        "exact_octet_scan",
        "bounded_sample_spread",
        "conservative_unknown_maxima",
    ):
        reached = set(branch(seen, rung_name))
        assert reached == {HOLDS, REFUSES}, f"{rung_name} only reached {reached}"
        assert why(seen, rung_name)


def test_every_lob_policy_rung_holds_and_refuses_under_watched() -> None:
    """Exercises both holds and refuses for all 5 candidate paths in lob_policy_rungs."""
    with watched() as seen:
        # 1. inline_row_lob holds: small 16 KB LOB within packet & memory limits
        plan_inline = choose_lob_policy(
            {
                "conservative_max_octets": 16 * 1024,
                "observed_mean_octets": 4 * 1024,
                "max_returned_packet_bytes": 16 * 1024 * 1024,
                "max_sent_packet_bytes": 16 * 1024 * 1024,
                "worker_memory_budget_bytes": 64 * 1024 * 1024,
                "batch_rows": 50,
                "unknown_maxima": False,
            }
        )
        assert plan_inline.mode == LobPathMode.INLINE_ROW_LOB
        assert plan_inline.choice.path == "inline_row_lob"

        # 2. inline_row_lob refuses (exceeds inline statement packet limit due to SENT_FACTOR=2),
        #    streamed_cursor_lob holds
        plan_stream = choose_lob_policy(
            {
                "conservative_max_octets": 6 * 1024 * 1024,
                "observed_mean_octets": 2 * 1024 * 1024,
                "max_returned_packet_bytes": 16 * 1024 * 1024,
                "max_sent_packet_bytes": 8 * 1024 * 1024,  # 6 MB * 2 = 12 MB > 8 MB sent packet
                "worker_memory_budget_bytes": 64 * 1024 * 1024,
                "supports_streaming_cursor": True,
                "snapshot_consistent": True,
                "unknown_maxima": False,
            }
        )
        assert plan_stream.mode == LobPathMode.STREAMED_CURSOR_LOB
        assert plan_stream.choice.path == "streamed_cursor_lob"

        # 3. inline_row_lob and streamed_cursor_lob refuse, locator_snapshot_lob holds
        plan_locator = choose_lob_policy(
            {
                "conservative_max_octets": 12 * 1024 * 1024,
                "observed_mean_octets": 4 * 1024 * 1024,
                "max_returned_packet_bytes": 4 * 1024 * 1024,
                "max_sent_packet_bytes": 4 * 1024 * 1024,
                "worker_memory_budget_bytes": 32 * 1024 * 1024,
                "supports_streaming_cursor": False,
                "supports_lob_locator": True,
                "locator_snapshot_pinned": True,
                "locator_stale_under_concurrent_writes": False,
                "locator_lifetime_seconds": 300.0,
                "unknown_maxima": False,
            }
        )
        assert plan_locator.mode == LobPathMode.LOCATOR_SNAPSHOT_LOB
        assert plan_locator.choice.path == "locator_snapshot_lob"

        # 4. locator_snapshot_lob refuses (stale under concurrent writes / unpinned),
        #    chunked_piece_lob holds
        plan_chunked = choose_lob_policy(
            {
                "conservative_max_octets": 24 * 1024 * 1024,
                "observed_mean_octets": 4 * 1024 * 1024,
                "max_returned_packet_bytes": 4 * 1024 * 1024,
                "max_sent_packet_bytes": 4 * 1024 * 1024,
                "worker_memory_budget_bytes": 32 * 1024 * 1024,
                "supports_streaming_cursor": False,
                "supports_lob_locator": True,
                "locator_snapshot_pinned": False,
                "locator_stale_under_concurrent_writes": True,
                "has_primary_key": True,
                "supports_piece_read": True,
                "supports_piece_write": True,
                "unknown_maxima": False,
            }
        )
        assert plan_chunked.mode == LobPathMode.CHUNKED_PIECE_LOB
        assert plan_chunked.choice.path == "chunked_piece_lob"

        # 5. chunked_piece_lob refuses (supports_piece_write=False),
        #    separate_keyed_fetch_lob holds
        plan_separate = choose_lob_policy(
            {
                "conservative_max_octets": 20 * 1024 * 1024,
                "observed_mean_octets": 1024,
                "max_returned_packet_bytes": 4 * 1024 * 1024,
                "max_sent_packet_bytes": 4 * 1024 * 1024,
                "worker_memory_budget_bytes": 32 * 1024 * 1024,
                "supports_streaming_cursor": False,
                "supports_lob_locator": False,
                "has_primary_key": True,
                "supports_piece_read": True,
                "supports_piece_write": False,
                "supports_separate_fetch": True,
                "unknown_maxima": False,
            }
        )
        assert plan_separate.mode == LobPathMode.SEPARATE_KEYED_FETCH_LOB
        assert plan_separate.choice.path == "separate_keyed_fetch_lob"

        # 6. separate_keyed_fetch_lob refuses when truncation_limit_bytes < conservative_max_octets
        plan_rejected = choose_lob_policy(
            {
                "conservative_max_octets": 10 * 1024 * 1024,
                "truncation_limit_bytes": 32 * 1024,  # 32 KB truncating cap!
                "has_primary_key": True,
                "supports_piece_read": True,
                "supports_piece_write": True,
                "supports_separate_fetch": True,
                "unknown_maxima": False,
            }
        )
        assert plan_rejected.choice.rung is None
        assert plan_rejected.care_badge == CareBadge.ACTION

    for rung_name in (
        "inline_row_lob",
        "streamed_cursor_lob",
        "locator_snapshot_lob",
        "chunked_piece_lob",
        "separate_keyed_fetch_lob",
    ):
        reached = set(branch(seen, rung_name))
        assert reached == {HOLDS, REFUSES}, f"{rung_name} only reached {reached}"
        assert why(seen, rung_name)


# -----------------------------------------------------------------------------
# 2. Rejecting Truncating & Stale-Locator Paths + Compressed Storage Hole Proof
# -----------------------------------------------------------------------------

def test_compressed_storage_lob_never_fools_size_probe_into_truncating_inline() -> None:
    """A 1 MB compressible LOB stored in 11 KB on disk must be sized by uncompressed
    octet_length (or guarded conservatively when only compressed metadata is known),
    never routed to a 32 KB truncating mode.
    """
    # Case A: Server exposes uncompressed octet_length alongside small stored size
    _, probe_a = choose_lob_size_probe(
        {
            "total_rows": 2000,
            "supports_octet_length_scan": True,
            "sample_octet_lengths": [1024, 2048, 4096, 8192, 16384, 32768, 65536, 1_000_000],
            "stored_compressed_lengths": [512, 1024, 2048, 4096, 6000, 8000, 9000, 11_452],
        }
    )
    assert probe_a.observed_max_octets == 1_000_000
    assert probe_a.stored_compressed_max_bytes == 11_452
    assert probe_a.compression_ratio_observed > 80.0
    assert probe_a.conservative_max_octets >= 1_000_000

    # Attempting to use a 64 KB truncation limit is rejected across all rungs
    plan_with_cap = choose_lob_policy(
        {
            "conservative_max_octets": probe_a.conservative_max_octets,
            "truncation_limit_bytes": 64 * 1024,
            "has_primary_key": True,
            "supports_piece_read": True,
            "supports_piece_write": True,
            "supports_separate_fetch": True,
        }
    )
    assert plan_with_cap.choice.rung is None
    assert plan_with_cap.truncation_free_verified is False

    # Case B: Only compressed storage metadata is available -> forces conservative_unknown_maxima
    _, probe_b = choose_lob_size_probe(
        {
            "total_rows": 2000,
            "supports_octet_length_scan": True,
            "compressed_metadata_only": True,
            "sample_octet_lengths": [11_452],
            "stored_compressed_lengths": [11_452],
            "protocol_hard_ceiling_bytes": 64 * 1024 * 1024,
        }
    )
    assert probe_b.probe_mode == LobSizeProbeMode.CONSERVATIVE_UNKNOWN_MAXIMA
    assert probe_b.conservative_max_octets >= 64 * 1024 * 1024

    # When fed into choose_lob_policy, inline_row_lob is refused because maxima are unknown
    plan_b = choose_lob_policy(
        {
            "total_rows": 2000,
            "compressed_metadata_only": True,
            "has_primary_key": True,
            "supports_piece_read": True,
            "supports_piece_write": True,
            "max_returned_packet_bytes": 16 * 1024 * 1024,
            "max_sent_packet_bytes": 16 * 1024 * 1024,
            "worker_memory_budget_bytes": 64 * 1024 * 1024,
        }
    )
    assert plan_b.mode == LobPathMode.CHUNKED_PIECE_LOB


def test_stale_locator_rejected_in_policy_and_at_runtime() -> None:
    """Unpinned or expiring LOB locators are rejected in planning, and mid-flight
    locator invalidation raises StaleLocatorError without advancing checkpoint.
    """
    # Lifetime too short for batch size -> locator_snapshot_lob refused, falls back to chunked_piece_lob
    plan_short_ttl = choose_lob_policy(
        {
            "conservative_max_octets": 20 * 1024 * 1024,
            "observed_mean_octets": 10 * 1024 * 1024,
            "batch_rows": 20,
            "transfer_rate_bps": 10 * 1024 * 1024,
            "max_returned_packet_bytes": 4 * 1024 * 1024,
            "max_sent_packet_bytes": 4 * 1024 * 1024,
            "worker_memory_budget_bytes": 32 * 1024 * 1024,
            "supports_lob_locator": True,
            "locator_snapshot_pinned": True,
            "locator_stale_under_concurrent_writes": False,
            "locator_lifetime_seconds": 0.0001,  # too short!
            "has_primary_key": True,
            "supports_piece_read": True,
            "supports_piece_write": True,
        }
    )
    assert plan_short_ttl.mode == LobPathMode.CHUNKED_PIECE_LOB

    # Valid locator plan where runtime locator invalidation occurs on key (2,)
    plan_loc = choose_lob_policy(
        {
            "conservative_max_octets": 1024 * 1024,
            "max_returned_packet_bytes": 256 * 1024,
            "max_sent_packet_bytes": 256 * 1024,
            "worker_memory_budget_bytes": 8 * 1024 * 1024,
            "supports_lob_locator": True,
            "locator_snapshot_pinned": True,
            "locator_stale_under_concurrent_writes": False,
            "locator_lifetime_seconds": 600.0,
        }
    )
    assert plan_loc.mode == LobPathMode.LOCATOR_SNAPSHOT_LOB
    _, probe = choose_lob_size_probe({"total_rows": 2, "sample_octet_lengths": [1024, 2048]})
    executor = NonTruncatingLobExecutor(plan_loc, probe)
    target: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    rows = [
        SourceLobRow(key=(1,), columns={"id": 1, "blob": b"a" * 1024}),
        SourceLobRow(key=(2,), columns={"id": 2, "blob": b"b" * 2048}),
    ]
    with pytest.raises(StaleLocatorError, match="became stale"):
        executor.transfer_unit(
            unit_id="u-loc-stale",
            table_name="public.docs",
            source_rows=rows,
            target_store=target,
            lob_columns=("blob",),
            locator_invalidated_keys={(2,)},
        )
    # Target store must remain clean (no partial unit committed)
    assert target == {}


# -----------------------------------------------------------------------------
# 3. Acceptance Evidence: Mixed NULL/Empty/Binary/Multibyte/Extreme LOBs +
#    Concurrent Updates + Injected Failures + Bounded Memory + No Premature Checkpoint
# -----------------------------------------------------------------------------

def test_mixed_lobs_concurrent_updates_and_failures_produce_exact_bytes_and_no_premature_checkpoint(
    tmp_path: pathlib.Path,
) -> None:
    """Proves acceptance criteria on a mixed batch of NULL, empty, binary (with NUL and
    0xFF bytes), multibyte UTF-8 (1/2/3/4-byte codepoints), compressed, and extreme LOBs:
    - Concurrent update during piece 0 triggers automatic re-read and yields exact newest bytes.
    - Transient network/statement failure retries cleanly.
    - Simulated target truncation raises LobTruncationError, leaves target untouched,
      and NEVER writes a premature checkpoint.
    - Peak worker memory stays strictly within worker_memory_budget_bytes.
    """
    # Construct multibyte text using ASCII escapes (1-byte, 2-byte, 3-byte, 4-byte UTF-8)
    multibyte_unit = "ASCII-\u00e9\u00f1-\u0e01\u0e02\u4e2d\u6587-\U0001f680\U0001f525|"
    multibyte_lob = multibyte_unit * 1500  # ~60 KB of mixed 1..4 byte UTF-8 characters

    # Binary payload with embedded NUL (\x00), high bytes (\xff), and non-UTF8 sequences
    binary_lob = (b"\x00\xff\xfe\x80\x01\x7f" * 8000) + hashlib.sha512(b"seed").digest()

    # Extreme LOB (1.2 MB) exceeding 128 KB piece size -> transferred in ~10 pieces
    extreme_lob = (b"EXTREME-BLOCK-" + bytes(range(256))) * 4500

    rows = [
        SourceLobRow(
            key=(1,),
            columns={"id": 1, "doc": None, "payload": None},
        ),
        SourceLobRow(
            key=(2,),
            columns={"id": 2, "doc": "", "payload": b""},
        ),
        SourceLobRow(
            key=(3,),
            columns={"id": 3, "doc": multibyte_lob, "payload": binary_lob},
        ),
        SourceLobRow(
            key=(4,),
            columns={"id": 4, "doc": "compressible-" * 5000, "payload": extreme_lob},
            compressed_storage_sizes={"payload": 8192},
        ),
    ]

    max_octets = max(
        payload_octet_length(r.columns["doc"]) + payload_octet_length(r.columns["payload"])
        for r in rows
    )

    # Configure tight packet limits (256 KB) and worker memory budget (2 MB) so
    # chunked_piece_lob is chosen with bounded piece size (~64 KB - 128 KB).
    facts = {
        "total_rows": 4,
        "sample_octet_lengths": [0, 0, len(binary_lob), len(extreme_lob)],
        "conservative_max_octets": max_octets,
        "is_multibyte_text": True,
        "max_returned_packet_bytes": 256 * 1024,
        "max_sent_packet_bytes": 256 * 1024,
        "worker_memory_budget_bytes": 2 * 1024 * 1024,
        "supports_streaming_cursor": False,
        "supports_lob_locator": False,
        "has_primary_key": True,
        "supports_piece_read": True,
        "supports_piece_write": True,
    }
    _, probe = choose_lob_size_probe(facts)
    plan = choose_lob_policy(facts)
    assert plan.mode == LobPathMode.CHUNKED_PIECE_LOB
    assert plan.piece_size_bytes <= 128 * 1024

    cp_path = tmp_path / "lob_checkpoints.json"
    cp_store = LobPolicyCheckpointStore(cp_path)
    executor = NonTruncatingLobExecutor(plan, probe, checkpoint_store=cp_store)
    target_store: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    # 1. Prove that target truncation is caught and NEVER checkpoints or commits partial data
    with pytest.raises(LobTruncationError, match="LOB truncation or length mismatch"):
        executor.transfer_unit(
            unit_id="unit-001",
            table_name="public.mixed_lobs",
            source_rows=rows,
            target_store=target_store,
            lob_columns=("doc", "payload"),
            simulated_target_truncation_bytes=32768,
        )
    assert not cp_store.is_completed("unit-001")
    assert cp_store.completed_count == 0
    assert target_store == {}

    # 2. Inject a concurrent source update on key (3,) mid-piece-read AND a transient
    #    fault on key (4,) piece 2, then verify exact final bytes and bounded memory.
    updated_multibyte_lob = multibyte_lob + "\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35-\U0001f30d-UPDATED"
    mutation_triggered = {"count": 0}
    fault_triggered = {"count": 0}

    def _concurrent_writer(src_row: SourceLobRow, piece_idx: int) -> None:
        if src_row.key == (3,) and mutation_triggered["count"] == 0:
            mutation_triggered["count"] += 1
            src_row.columns["doc"] = updated_multibyte_lob
            src_row.version += 1

    def _transient_fault(key: Tuple[Any, ...], col: str, piece_idx: int) -> None:
        if key == (4,) and col == "payload" and piece_idx == 2 and fault_triggered["count"] == 0:
            fault_triggered["count"] += 1
            raise RuntimeError("simulated transient connection reset during piece 2")

    report = executor.transfer_unit(
        unit_id="unit-001",
        table_name="public.mixed_lobs",
        source_rows=rows,
        target_store=target_store,
        lob_columns=("doc", "payload"),
        source_watermark=decimal.Decimal("10492.55"),
        concurrent_mutation_hook=_concurrent_writer,
        fault_injection_hook=_transient_fault,
    )

    assert report.care_badge == CareBadge.PASS
    assert report.rows_transferred == 4
    assert report.concurrent_re_reads >= 1
    assert report.transient_retries == 1
    assert report.truncation_incidents == 0
    assert report.premature_checkpoints == 0
    assert report.peak_worker_memory_bytes <= plan.worker_memory_budget_bytes

    # Verify exact final bytes for all 4 rows
    assert target_store[(1,)]["doc"] is None
    assert target_store[(1,)]["payload"] is None
    assert target_store[(2,)]["doc"] == ""
    assert target_store[(2,)]["payload"] == b""
    assert target_store[(3,)]["doc"] == updated_multibyte_lob
    assert payload_sha256(target_store[(3,)]["doc"]) == payload_sha256(updated_multibyte_lob)
    assert target_store[(3,)]["payload"] == binary_lob
    assert payload_sha256(target_store[(3,)]["payload"]) == payload_sha256(binary_lob)
    assert target_store[(4,)]["payload"] == extreme_lob
    assert payload_sha256(target_store[(4,)]["payload"]) == payload_sha256(extreme_lob)

    # Verify checkpoint was recorded only after full verification and reloads in O(1)
    assert cp_store.is_completed("unit-001")
    reloaded_store = LobPolicyCheckpointStore(cp_path)
    assert reloaded_store.is_completed("unit-001")
    cp_entry = reloaded_store.get("unit-001")
    assert cp_entry is not None
    assert cp_entry.source_watermark == decimal.Decimal("10492.55")
    assert cp_entry.min_key == (1,)
    assert cp_entry.max_key == (4,)

    # Re-running the completed unit skips in O(1) without touching target_store
    report_skip = executor.transfer_unit(
        unit_id="unit-001",
        table_name="public.mixed_lobs",
        source_rows=rows,
        target_store=target_store,
        lob_columns=("doc", "payload"),
    )
    assert "skipped in O(1)" in report_skip.summary


def test_utf8_byte_budget_slicing_never_cuts_mid_codepoint() -> None:
    """Proves slice_utf8_by_byte_budget respects byte limits on 4-byte UTF-8 characters."""
    text = "\U0001f680\U0001f525\u4e2d\u6587\u00e9A"  # 4 + 4 + 3 + 3 + 2 + 1 = 17 bytes
    pieces_out = slice_utf8_by_byte_budget(text, max_piece_bytes=7)
    for part in pieces_out:
        assert payload_octet_length(part) <= 7
        # Must encode cleanly without surrogate errors
        assert len(part.encode("utf-8")) <= 7
    assert "".join(pieces_out) == text


def test_persistent_concurrent_mutation_raises_explicit_error() -> None:
    """If a row keeps mutating on every re-read cycle, ConcurrentLobMutationError is raised."""
    facts = {
        "conservative_max_octets": 64 * 1024,
        "max_returned_packet_bytes": 16 * 1024,
        "max_sent_packet_bytes": 16 * 1024,
        "worker_memory_budget_bytes": 512 * 1024,
        "has_primary_key": True,
        "supports_piece_read": True,
        "supports_piece_write": True,
    }
    _, probe = choose_lob_size_probe(facts)
    plan = choose_lob_policy(facts)
    executor = NonTruncatingLobExecutor(plan, probe)
    row = SourceLobRow(key=(99,), columns={"id": 99, "blob": b"x" * 32768})
    target: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def _always_mutate(r: SourceLobRow, piece_idx: int) -> None:
        r.columns["blob"] = bytes(r.columns["blob"]) + b"!"
        r.version += 1

    # Notice our hook in _read_and_write_in_pieces triggers on cycle 0; let's also test
    # when fault or mutation persists across retries:
    def _mutate_via_fault(key: Tuple[Any, ...], col: str, piece_idx: int) -> None:
        row.columns["blob"] = bytes(row.columns["blob"]) + b"!"
        row.version += 1

    with pytest.raises(LobPolicyError, match="kept changing across 4 piece re-read cycles"):
        executor.transfer_unit(
            unit_id="u-always-mutating",
            table_name="public.t",
            source_rows=[row],
            target_store=target,
            lob_columns=("blob",),
            fault_injection_hook=_mutate_via_fault,
            max_retries=1,
        )
    assert target == {}


# -----------------------------------------------------------------------------
# 4. Lease Lock (.run.lock), AIMD Controller, Host Safety & External Tool Wrapper
# -----------------------------------------------------------------------------

def test_lease_lock_mutual_exclusion_and_heartbeat(tmp_path: pathlib.Path) -> None:
    """Proves active .run.lock mutual exclusion and expiration takeover."""
    now = [1000.0]
    lease1 = LobPolicyLease(tmp_path, owner_id="worker-a", ttl_seconds=10.0, clock=lambda: now[0])
    lease2 = LobPolicyLease(tmp_path, owner_id="worker-b", ttl_seconds=10.0, clock=lambda: now[0])

    with lease1:
        with pytest.raises(LeaseLockError, match="active LOB policy lease held by 'worker-a'"):
            lease2.acquire()
        now[0] += 5.0
        lease1.heartbeat()
        now[0] += 6.0
        # Still held because heartbeat extended expiry to 1015.0
        with pytest.raises(LeaseLockError):
            lease2.acquire()
        now[0] += 5.0
        # Expired at 1015.0, now at 1016.0 -> worker-b can take over
        lease2.acquire()
        lease2.release()


def test_adaptive_aimd_piece_controller_and_eta() -> None:
    """Proves AIMD piece sizing and cold-start vs measured byte-weighted ETA."""
    ctrl = AdaptiveLobPieceController(
        initial_piece_bytes=128 * 1024,
        min_piece_bytes=16 * 1024,
        max_piece_bytes=512 * 1024,
        additive_step_bytes=32 * 1024,
        multiplicative_factor=0.5,
        target_latency_seconds=0.25,
    )
    # Cold-start ETA before any piece is observed
    cold_eta = ctrl.estimate_eta_seconds(10 * 1024 * 1024, cold_start_bps=10 * 1024 * 1024)
    assert cold_eta == pytest.approx(1.0)

    # Fast piece -> additive increase
    next_size = ctrl.observe_piece(128 * 1024, 0.05)
    assert next_size == 160 * 1024

    # Throttled/slow piece -> multiplicative decrease
    back_off_size = ctrl.observe_piece(160 * 1024, 0.80, throttled_or_pressured=True)
    assert back_off_size == 80 * 1024

    measured_eta = ctrl.estimate_eta_seconds(288 * 1024)
    assert measured_eta == pytest.approx(0.85, rel=1e-2)


def test_external_tool_wrapper_and_endpoint_validation() -> None:
    """Proves LobPolicyToolWrapper rejects truncating caps and maps cleanly."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.1")

    plan = choose_lob_policy(
        {
            "conservative_max_octets": 512 * 1024,
            "max_returned_packet_bytes": 128 * 1024,
            "max_sent_packet_bytes": 128 * 1024,
            "worker_memory_budget_bytes": 4 * 1024 * 1024,
            "has_primary_key": True,
            "supports_piece_read": True,
            "supports_piece_write": True,
        }
    )
    assert plan.mode == LobPathMode.CHUNKED_PIECE_LOB

    dms_cfg = LobPolicyToolWrapper.build_tool_config("dms", plan)
    assert dms_cfg["FullLobMode"] is True
    assert dms_cfg["LimitedSizeLobMode"] is False

    qlik_cfg = LobPolicyToolWrapper.build_tool_config("qlik", plan)
    assert qlik_cfg["allow_unlimited_lob_size"] is True

    with pytest.raises(LobTruncationError, match="refusing truncating cap"):
        LobPolicyToolWrapper.build_tool_config("dms", plan, requested_truncation_kb=64)
