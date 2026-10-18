import io
import os
import shutil
import tempfile
import zlib
import bz2
import pytest

from migkit.decomp_budget import (
    BudgetLimits,
    DecompressionBudgetGovernor,
    DecompressionBudgetError,
    DecompressionLimitExceeded,
    DecoderLimitExceeded,
    TransactionAtomicityError,
    LimitAction,
    SpillPolicy,
    TransactionStatus,
    decompression_budget_rungs,
    decide_decompression_budget,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("decomp_budget", "decompression_budget_rungs", "streaming_in_memory_decode", "holds"),
    ("decomp_budget", "decompression_budget_rungs", "streaming_in_memory_decode", "refuses"),
    ("decomp_budget", "decompression_budget_rungs", "spill_chunk_to_temp_storage", "holds"),
    ("decomp_budget", "decompression_budget_rungs", "spill_chunk_to_temp_storage", "refuses"),
    ("decomp_budget", "decompression_budget_rungs", "scale_decoder_buffer_dynamically", "holds"),
    ("decomp_budget", "decompression_budget_rungs", "scale_decoder_buffer_dynamically", "refuses"),
    ("decomp_budget", "decompression_budget_rungs", "quarantine_runaway_transaction", "holds"),
    ("decomp_budget", "decompression_budget_rungs", "quarantine_runaway_transaction", "refuses"),
    ("decomp_budget", "decompression_budget_rungs", "refuse_decompression_bomb", "holds"),
]



@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="migkit_test_decomp_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_governor_initialization_and_default_limits(temp_dir):
    gov = DecompressionBudgetGovernor(spill_dir=temp_dir)
    assert gov.limits.max_memory_bytes == 64 * 1024 * 1024
    assert gov.limits.max_uncompressed_bytes == 2 * 1024 * 1024 * 1024
    assert gov.limits.spill_threshold_bytes == 8 * 1024 * 1024
    assert gov.compressed_bytes == 0
    assert gov.expanded_bytes == 0
    assert gov.spilled_bytes == 0
    assert gov.spill_count == 0
    assert gov.nested_allocations == 0
    assert gov.active_memory_bytes == 0
    gov.close()


def test_in_memory_decompression_below_threshold(temp_dir):
    gov = DecompressionBudgetGovernor(spill_dir=temp_dir)
    original = b"payload test string 1234567890" * 50
    compressed = zlib.compress(original)

    payload = gov.decompress(compressed, codec="zlib", tx_id="tx_mem_1")
    assert not payload.is_spilled
    assert payload.size == len(original)
    assert payload.read() == original
    assert payload.compressed_bytes == len(compressed)
    assert payload.expanded_bytes == len(original)

    payload.commit()
    assert payload.status == TransactionStatus.COMMITTED
    payload.acknowledge()
    assert payload.partial_effects_acknowledged
    payload.close()
    gov.close()


def test_automatic_spill_to_disk_when_threshold_exceeded(temp_dir):
    limits = BudgetLimits(
        spill_threshold_bytes=1024,
        max_memory_bytes=4096,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    original = b"spill payload repeated data " * 200
    assert len(original) > limits.spill_threshold_bytes
    compressed = zlib.compress(original)

    payload = gov.decompress(compressed, codec="zlib", tx_id="tx_spill_1")
    assert payload.is_spilled
    assert payload.spill_path is not None
    assert os.path.exists(payload.spill_path)
    assert payload.size == len(original)
    assert payload.read() == original
    assert gov.spilled_bytes >= len(original)
    assert gov.spill_count == 1

    spill_file = payload.spill_path
    payload.commit()
    payload.acknowledge()
    assert payload.partial_effects_acknowledged
    payload.close()
    assert not os.path.exists(spill_file)
    gov.close()


def test_transaction_atomicity_commit_and_acknowledge(temp_dir):
    gov = DecompressionBudgetGovernor(spill_dir=temp_dir)
    data = zlib.compress(b"atomicity test")

    payload1 = gov.decompress(data, codec="zlib", tx_id="tx_atom_1")
    with pytest.raises(TransactionAtomicityError):
        payload1.acknowledge()
    payload1.commit()
    payload1.acknowledge()
    assert payload1.partial_effects_acknowledged
    payload1.close()

    payload2 = gov.decompress(data, codec="zlib", tx_id="tx_atom_2")
    payload2.rollback()
    assert payload2.status == TransactionStatus.ROLLED_BACK
    assert not payload2.partial_effects_acknowledged
    with pytest.raises(TransactionAtomicityError):
        payload2.commit()
    with pytest.raises(TransactionAtomicityError):
        payload2.acknowledge()
    payload2.close()
    gov.close()


def test_refuse_when_uncompressed_limit_exceeded(temp_dir):
    limits = BudgetLimits(
        max_uncompressed_bytes=2048,
        default_limit_action=LimitAction.REFUSE,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    original = b"x" * 10000
    compressed = zlib.compress(original)

    with pytest.raises(DecompressionLimitExceeded) as exc_info:
        gov.decompress(compressed, codec="zlib", tx_id="tx_refuse_1")

    err = exc_info.value
    assert err.limit_type == "uncompressed_bytes"
    assert err.limit_bytes == 2048
    assert not err.reseed_required
    assert err.action == "refuse"
    assert not err.partial_effects_acknowledged
    assert gov.metrics.transactions_refused == 1

    refusal_record = gov.handle_refusal(err)
    assert refusal_record["action"] == "refuse"
    assert not refusal_record["partial_effects_acknowledged"]
    assert len(list(os.scandir(temp_dir))) == 0
    gov.close()


def test_reseed_when_uncompressed_limit_exceeded(temp_dir):
    limits = BudgetLimits(
        max_uncompressed_bytes=2048,
        default_limit_action=LimitAction.RESEED,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    original = b"y" * 10000
    compressed = zlib.compress(original)

    with pytest.raises(DecompressionLimitExceeded) as exc_info:
        gov.decompress(compressed, codec="zlib", tx_id="tx_reseed_1")

    err = exc_info.value
    assert err.limit_type == "uncompressed_bytes"
    assert err.reseed_required
    assert err.action == "reseed"
    assert not err.partial_effects_acknowledged
    assert gov.metrics.transactions_reseeded == 1

    reseed_record = gov.handle_reseed(err, watermark="0/16B3748")
    assert reseed_record["action"] == "reseed"
    assert reseed_record["watermark"] == "0/16B3748"
    assert not reseed_record["partial_effects_acknowledged"]
    assert len(list(os.scandir(temp_dir))) == 0
    gov.close()


def test_compressed_bytes_limit_exceeded(temp_dir):
    limits = BudgetLimits(
        max_compressed_bytes=256,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    original = bytes([i % 256 for i in range(10000)])
    compressed = zlib.compress(original)
    assert len(compressed) > 256

    with pytest.raises(DecompressionLimitExceeded) as exc_info:
        gov.decompress(compressed, codec="zlib")

    err = exc_info.value
    assert err.limit_type == "compressed_bytes"
    assert not err.partial_effects_acknowledged
    gov.close()


def test_decoder_chunk_limits(temp_dir):
    limits = BudgetLimits(
        max_decoder_buffer_bytes=1024,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    assert gov.metrics.decoder_limit_violations == 0
    gov.close()


def test_nested_allocations_tracking_and_limits(temp_dir):
    limits = BudgetLimits(
        max_nested_depth=2,
        max_nested_allocations=3,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)

    with gov.nested_allocation(tx_id="tx1"):
        assert gov.nested_allocations == 1
        with gov.nested_allocation(tx_id="tx1"):
            assert gov.nested_allocations == 2
            with pytest.raises(DecompressionLimitExceeded) as exc_info:
                with gov.nested_allocation(tx_id="tx1"):
                    pass
            assert exc_info.value.limit_type == "nested_depth"
            assert not exc_info.value.partial_effects_acknowledged

        assert gov.nested_allocations == 1
    assert gov.nested_allocations == 0
    gov.close()


def test_spill_policy_always_spill(temp_dir):
    limits = BudgetLimits(
        spill_policy=SpillPolicy.ALWAYS_SPILL,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    compressed = zlib.compress(b"tiny")

    payload = gov.decompress(compressed, codec="zlib")
    assert payload.is_spilled
    assert payload.read() == b"tiny"
    payload.commit()
    payload.close()
    gov.close()


def test_spill_policy_memory_only_exceeds(temp_dir):
    limits = BudgetLimits(
        spill_policy=SpillPolicy.MEMORY_ONLY,
        max_memory_bytes=512,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    compressed = zlib.compress(b"a" * 2048)

    with pytest.raises(DecompressionLimitExceeded) as exc_info:
        gov.decompress(compressed, codec="zlib")
    assert exc_info.value.limit_type in ("memory_budget", "spill_forbidden")
    assert not exc_info.value.partial_effects_acknowledged
    gov.close()


def test_stream_and_iter_chunks(temp_dir):
    limits = BudgetLimits(
        spill_threshold_bytes=512,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)
    original = b"0123456789abcdef" * 100
    compressed = zlib.compress(original)

    payload = gov.decompress(compressed, codec="zlib")
    assert payload.is_spilled
    chunks = list(payload.iter_chunks(chunk_size=64))
    assert b"".join(chunks) == original
    assert payload.tell() == len(original)
    payload.seek(0)
    assert payload.tell() == 0
    assert payload.read(16) == original[:16]
    payload.commit()
    payload.close()
    gov.close()


def test_multiple_codecs_supported(temp_dir):
    gov = DecompressionBudgetGovernor(spill_dir=temp_dir)
    data = b"migkit multi codec decompression test" * 10

    # zlib
    c_zlib = zlib.compress(data)
    p_zlib = gov.decompress(c_zlib, codec="zlib")
    assert p_zlib.read() == data
    p_zlib.close()

    # gzip
    buf = io.BytesIO()
    import gzip
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(data)
    c_gzip = buf.getvalue()
    p_gzip = gov.decompress(c_gzip, codec="gzip")
    assert p_gzip.read() == data
    p_gzip.close()

    # bz2
    c_bz2 = bz2.compress(data)
    p_bz2 = gov.decompress(c_bz2, codec="bz2")
    assert p_bz2.read() == data
    p_bz2.close()

    # raw / identity
    p_raw = gov.decompress(data, codec="none")
    assert p_raw.read() == data
    p_raw.close()

    gov.close()


def test_corrupt_stream_safe_cleanup(temp_dir):
    gov = DecompressionBudgetGovernor(spill_dir=temp_dir)
    corrupt = b"\x78\x9c\x00\x00\xff\xff_corrupted_payload"

    with pytest.raises(DecompressionBudgetError):
        gov.decompress(corrupt, codec="zlib", tx_id="tx_corrupt")

    assert len(list(os.scandir(temp_dir))) == 0
    assert gov.active_memory_bytes == 0
    gov.close()


def test_highly_compressible_2gb_transaction_stays_within_budget(temp_dir):
    limits = BudgetLimits(
        max_memory_bytes=32 * 1024 * 1024,
        spill_threshold_bytes=2 * 1024 * 1024,
        max_uncompressed_bytes=3 * 1024 * 1024 * 1024,
        decoder_chunk_size=64 * 1024,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)

    target_bytes = 2147483648  # 2 GiB
    chunk_raw = b"\x00" * (16 * 1024 * 1024)
    c = zlib.compressobj()
    comp_parts = []
    for _ in range(target_bytes // len(chunk_raw)):
        comp_parts.append(c.compress(chunk_raw))
    comp_parts.append(c.flush())
    compressed = b"".join(comp_parts)

    payload = gov.decompress(compressed, codec="zlib", tx_id="tx_2gb")
    assert payload.is_spilled
    assert payload.size == target_bytes
    assert gov.active_memory_bytes <= limits.spill_threshold_bytes

    count = 0
    sample = None
    for chunk in payload.iter_chunks(chunk_size=1024 * 1024):
        count += len(chunk)
        if sample is None:
            sample = chunk
    assert count == target_bytes
    assert sample == b"\x00" * len(sample)

    payload.commit()
    payload.acknowledge()
    assert payload.partial_effects_acknowledged
    payload.close()
    gov.close()


def test_highly_compressible_2gb_transaction_aborts_at_limit_without_oom(temp_dir):
    limits = BudgetLimits(
        max_memory_bytes=32 * 1024 * 1024,
        spill_threshold_bytes=2 * 1024 * 1024,
        max_uncompressed_bytes=10 * 1024 * 1024,  # 10 MB limit
        decoder_chunk_size=64 * 1024,
        default_limit_action=LimitAction.REFUSE,
    )
    gov = DecompressionBudgetGovernor(limits=limits, spill_dir=temp_dir)

    target_bytes = 2147483648  # 2 GiB
    chunk_raw = b"\x00" * (16 * 1024 * 1024)
    c = zlib.compressobj()
    comp_parts = []
    for _ in range(target_bytes // len(chunk_raw)):
        comp_parts.append(c.compress(chunk_raw))
    comp_parts.append(c.flush())
    compressed = b"".join(comp_parts)

    with pytest.raises(DecompressionLimitExceeded) as exc_info:
        gov.decompress(compressed, codec="zlib", tx_id="tx_2gb_abort")

    err = exc_info.value
    assert err.limit_type == "uncompressed_bytes"
    assert err.limit_bytes == 10 * 1024 * 1024
    assert not err.partial_effects_acknowledged
    assert len(list(os.scandir(temp_dir))) == 0
    assert gov.metrics.transactions_refused == 1
    gov.close()


def test_budget_limits_from_facts_and_evaluate_strategy():
    facts = {
        "max_memory_bytes": 128 * 1024 * 1024,
        "max_uncompressed_bytes": 4 * 1024 * 1024 * 1024,
        "spill_threshold_bytes": 16 * 1024 * 1024,
        "spill_policy": SpillPolicy.AUTO,
        "default_limit_action": LimitAction.RESEED,
    }
    limits = BudgetLimits.from_facts(facts)
    assert limits.max_memory_bytes == 128 * 1024 * 1024
    assert limits.spill_threshold_bytes == 16 * 1024 * 1024
    assert limits.default_limit_action == LimitAction.RESEED

    choice = limits.evaluate_strategy(active_memory=1024, incoming_chunk=2048)
    assert choice.rung.name == "streaming_in_memory_decode"


def test_decompression_budget_rungs_under_watched():
    with watched():
        # 1. streaming_in_memory_decode holds
        f1 = {
            "active_memory_bytes": 1024,
            "chunk_bytes": 4096,
            "max_memory_bytes": 67108864,
            "observed_expansion_ratio": 4.0,
            "max_safe_expansion_ratio": 50.0,
            "spill_permitted": True,
            "spill_storage_available": True,
            "spilled_bytes": 0,
            "max_disk_spill_bytes": 10737418240,
            "dynamic_scaling_enabled": True,
            "current_buffer_bytes": 65536,
            "max_buffer_ceiling": 33554432,
            "reseed_handler_registered": False,
            "atomicity_recoverable": True,
        }
        c1 = decide_decompression_budget(f1)
        assert c1.rung.name == "streaming_in_memory_decode"

        # 2. streaming_in_memory_decode refuses, spill_chunk_to_temp_storage holds
        f2 = {
            "active_memory_bytes": 100000000,
            "chunk_bytes": 4096,
            "max_memory_bytes": 67108864,
            "observed_expansion_ratio": 4.0,
            "max_safe_expansion_ratio": 50.0,
            "spill_permitted": True,
            "spill_storage_available": True,
            "spilled_bytes": 1024,
            "max_disk_spill_bytes": 10737418240,
            "dynamic_scaling_enabled": True,
            "current_buffer_bytes": 65536,
            "max_buffer_ceiling": 33554432,
            "reseed_handler_registered": False,
            "atomicity_recoverable": True,
        }
        c2 = decide_decompression_budget(f2)
        assert c2.rung.name == "spill_chunk_to_temp_storage"

        # 3. spill_chunk_to_temp_storage refuses, scale_decoder_buffer_dynamically holds
        f3 = {
            "active_memory_bytes": 100000000,
            "chunk_bytes": 4096,
            "max_memory_bytes": 67108864,
            "observed_expansion_ratio": 4.0,
            "max_safe_expansion_ratio": 50.0,
            "spill_permitted": False,
            "spill_storage_available": False,
            "spilled_bytes": 100 * 1024 * 1024 * 1024,
            "max_disk_spill_bytes": 10737418240,
            "dynamic_scaling_enabled": True,
            "current_buffer_bytes": 65536,
            "max_buffer_ceiling": 33554432,
            "reseed_handler_registered": False,
            "atomicity_recoverable": True,
        }
        c3 = decide_decompression_budget(f3)
        assert c3.rung.name == "scale_decoder_buffer_dynamically"

        # 4. scale_decoder_buffer_dynamically refuses, quarantine_runaway_transaction holds
        f4 = {
            "active_memory_bytes": 100000000,
            "chunk_bytes": 4096,
            "max_memory_bytes": 67108864,
            "observed_expansion_ratio": 4.0,
            "max_safe_expansion_ratio": 50.0,
            "spill_permitted": False,
            "spill_storage_available": False,
            "spilled_bytes": 100 * 1024 * 1024 * 1024,
            "max_disk_spill_bytes": 10737418240,
            "dynamic_scaling_enabled": False,
            "current_buffer_bytes": 100000000,
            "max_buffer_ceiling": 33554432,
            "reseed_handler_registered": True,
            "atomicity_recoverable": True,
        }
        c4 = decide_decompression_budget(f4)
        assert c4.rung.name == "quarantine_runaway_transaction"

        # 5. quarantine_runaway_transaction refuses, refuse_decompression_bomb holds
        f5 = {
            "active_memory_bytes": 100000000,
            "chunk_bytes": 4096,
            "max_memory_bytes": 67108864,
            "observed_expansion_ratio": 4.0,
            "max_safe_expansion_ratio": 50.0,
            "spill_permitted": False,
            "spill_storage_available": False,
            "spilled_bytes": 100 * 1024 * 1024 * 1024,
            "max_disk_spill_bytes": 10737418240,
            "dynamic_scaling_enabled": False,
            "current_buffer_bytes": 100000000,
            "max_buffer_ceiling": 33554432,
            "reseed_handler_registered": False,
            "atomicity_recoverable": False,
        }
        c5 = decide_decompression_budget(f5)
        assert c5.rung.name == "refuse_decompression_bomb"

