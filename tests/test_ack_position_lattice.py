import os
import shutil
import tempfile
import pytest

from migkit.ack_lattice import (
    PipelineStage,
    READ,
    DURABLY_RETAINED,
    APPLIED,
    PROVED,
    PositionLattice,
    MonotoneRegressionError,
    SourceAckRefusedError,
    CutoverRefusedError,
    CutoverAuthorization,
    DurableAckPipeline,
    normalize_position,
    format_lsn,
)


def test_pipeline_stage_ordering():
    assert READ < DURABLY_RETAINED
    assert DURABLY_RETAINED < APPLIED
    assert APPLIED < PROVED
    assert READ < PROVED
    assert PipelineStage.from_string("read") == READ
    assert PipelineStage.from_string("durably_retained") == DURABLY_RETAINED
    assert PipelineStage.from_string("applied") == APPLIED
    assert PipelineStage.from_string("proved") == PROVED


def test_position_normalization():
    assert normalize_position(100) == 100
    assert normalize_position("100") == 100
    assert normalize_position("0/16B3748") == 0x16B3748
    assert normalize_position("1/0") == (1 << 32)
    assert normalize_position(None) == 0
    assert format_lsn(0x16B3748) == "0/16B3748"


def test_lattice_monotone_positions():
    lat = PositionLattice()
    assert lat.read_pos == 0
    assert lat.retained_pos == 0
    assert lat.durably_retained_pos == 0
    assert lat.applied_pos == 0
    assert lat.proved_pos == 0
    assert lat.source_ack_pos == 0

    lat.advance_read(100)
    assert lat.read_pos == 100
    lat.advance_read(50)
    assert lat.read_pos == 100

    lat.advance_retained(80)
    assert lat.retained_pos == 80
    assert lat.durably_retained_pos == 80
    lat.advance_retained(70)
    assert lat.retained_pos == 80

    lat.advance_applied(60)
    assert lat.applied_pos == 60
    lat.advance_applied(50)
    assert lat.applied_pos == 60

    lat.advance_proved(40)
    assert lat.proved_pos == 40
    lat.advance_proved(30)
    assert lat.proved_pos == 40


def test_lattice_strict_regression_rejection():
    lat = PositionLattice(strict=True)
    lat.advance_read(100)
    with pytest.raises(MonotoneRegressionError):
        lat.advance_read(50)

    lat.advance_retained(80)
    with pytest.raises(MonotoneRegressionError):
        lat.advance_retained(60)

    lat.advance_applied(50)
    with pytest.raises(MonotoneRegressionError):
        lat.advance_applied(40)

    lat.advance_proved(30)
    with pytest.raises(MonotoneRegressionError):
        lat.advance_proved(20)


def test_lattice_join_and_meet():
    a = PositionLattice(read_pos=100, retained_pos=80, applied_pos=60, proved_pos=40, source_ack_pos=70)
    b = PositionLattice(read_pos=120, retained_pos=70, applied_pos=70, proved_pos=30, source_ack_pos=60)

    j = a.join(b)
    assert j.read_pos == 120
    assert j.retained_pos == 80
    assert j.applied_pos == 70
    assert j.proved_pos == 40
    assert j.source_ack_pos == 70

    m = a.meet(b)
    assert m.read_pos == 100
    assert m.retained_pos == 70
    assert m.applied_pos == 60
    assert m.proved_pos == 30
    assert m.source_ack_pos == 60


def test_lattice_partial_ordering():
    low = PositionLattice(read_pos=10, retained_pos=10, applied_pos=10, proved_pos=10, source_ack_pos=10)
    high = PositionLattice(read_pos=20, retained_pos=20, applied_pos=20, proved_pos=20, source_ack_pos=20)
    incomparable = PositionLattice(read_pos=5, retained_pos=25, applied_pos=15, proved_pos=15, source_ack_pos=15)

    assert low <= high
    assert low < high
    assert not (high <= low)
    assert not (low <= incomparable)
    assert not (incomparable <= low)
    assert low == PositionLattice(read_pos=10, retained_pos=10, applied_pos=10, proved_pos=10, source_ack_pos=10)


def test_source_ack_requires_durable_retention():
    lat = PositionLattice()
    lat.advance_read(500)
    lat.advance_retained(300)

    assert lat.can_ack_source(200) is True
    assert lat.can_ack_source(300) is True
    assert lat.can_ack_source(301) is False
    assert lat.can_ack_source(500) is False

    ack_val = lat.ack_source(250)
    assert ack_val == 250
    assert lat.source_ack_pos == 250

    with pytest.raises(SourceAckRefusedError):
        lat.ack_source(350)
    assert lat.source_ack_pos == 250


def test_cutover_requires_target_committed_prefix_and_proof():
    lat = PositionLattice()
    lat.advance_read(1000)
    lat.advance_retained(1000)
    lat.ack_source(1000)

    assert lat.source_ack_pos == 1000
    assert lat.applied_pos == 0
    assert lat.proved_pos == 0

    can, reason = lat.can_authorize_cutover(1000)
    assert can is False
    assert "target committed-prefix" in reason or "applied" in reason

    with pytest.raises(CutoverRefusedError):
        lat.authorize_cutover(1000)

    lat.advance_applied(1000)
    assert lat.applied_pos == 1000
    assert lat.proved_pos == 0

    can, reason = lat.can_authorize_cutover(1000)
    assert can is False
    assert "proof" in reason or "proved" in reason

    with pytest.raises(CutoverRefusedError):
        lat.authorize_cutover(1000)

    lat.advance_proved(1000)
    can, reason = lat.can_authorize_cutover(1000)
    assert can is True

    auth = lat.authorize_cutover(1000)
    assert isinstance(auth, CutoverAuthorization)
    assert auth.authorized is True
    assert auth.required_pos == 1000


def test_source_ack_alone_cannot_authorize_cutover():
    lat = PositionLattice()
    lat.advance_read(500)
    lat.advance_retained(500)
    lat.ack_source(500)
    lat.advance_applied(400)
    lat.advance_proved(400)

    can, reason = lat.can_authorize_cutover(500)
    assert can is False
    assert "applied" in reason

    can, reason = lat.can_authorize_cutover(400)
    assert can is True


def test_crash_after_read_unspooled_data_lost_source_unacked():
    tmp_dir = tempfile.mkdtemp()
    try:
        pipeline = DurableAckPipeline(tmp_dir)
        pipeline.read(100, {"k": 1, "v": "a"})
        pipeline.read(200, {"k": 2, "v": "b"})

        assert pipeline.lattice.read_pos == 200
        assert pipeline.lattice.retained_pos == 0
        assert pipeline.lattice.source_ack_pos == 0

        pipeline.crash()

        recovered = DurableAckPipeline(tmp_dir)
        assert recovered.lattice.retained_pos == 0
        assert recovered.lattice.source_ack_pos == 0
        assert recovered.lattice.applied_pos == 0

        assert recovered.reconstruct() == []
        can, _ = recovered.lattice.can_authorize_cutover(200)
        assert can is False
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_crash_after_spool_durable_reconstructability():
    tmp_dir = tempfile.mkdtemp()
    try:
        pipeline = DurableAckPipeline(tmp_dir)
        pipeline.read(100, {"k": 1, "v": "a"})
        pipeline.spool(100)

        assert pipeline.lattice.retained_pos == 100
        assert pipeline.lattice.source_ack_pos == 0

        pipeline.crash()

        recovered = DurableAckPipeline(tmp_dir)
        assert recovered.lattice.retained_pos == 100
        assert recovered.lattice.source_ack_pos == 0

        records = recovered.reconstruct()
        assert len(records) == 1
        assert records[0]["pos"] == 100
        assert records[0]["payload"] == {"k": 1, "v": "a"}

        assert recovered.lattice.can_ack_source(100) is True
        recovered.ack(100)
        assert recovered.lattice.source_ack_pos == 100
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_crash_after_ack_source_purged_spool_reconstructs():
    tmp_dir = tempfile.mkdtemp()
    try:
        pipeline = DurableAckPipeline(tmp_dir)
        pipeline.read(100, {"k": 1, "v": "a"})
        pipeline.read(200, {"k": 2, "v": "b"})
        pipeline.spool(200)
        pipeline.ack(200)

        assert pipeline.lattice.source_ack_pos == 200
        assert pipeline.lattice.applied_pos == 0

        pipeline.crash()

        recovered = DurableAckPipeline(tmp_dir)
        assert recovered.lattice.source_ack_pos == 200
        assert recovered.lattice.applied_pos == 0

        can, reason = recovered.lattice.can_authorize_cutover(200)
        assert can is False

        reconstructed = recovered.reconstruct()
        assert len(reconstructed) == 2
        assert [r["pos"] for r in reconstructed] == [100, 200]

        for rec in reconstructed:
            recovered.apply(rec["pos"])
        assert recovered.lattice.applied_pos == 200

        can, reason = recovered.lattice.can_authorize_cutover(200)
        assert can is False

        recovered.prove(200)
        can, _ = recovered.lattice.can_authorize_cutover(200)
        assert can is True
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_crash_after_apply_target_committed_awaiting_proof():
    tmp_dir = tempfile.mkdtemp()
    try:
        pipeline = DurableAckPipeline(tmp_dir)
        pipeline.read(100, {"k": 1, "v": "a"})
        pipeline.spool(100)
        pipeline.ack(100)
        pipeline.apply(100)

        assert pipeline.lattice.applied_pos == 100
        assert pipeline.lattice.proved_pos == 0

        pipeline.crash()

        recovered = DurableAckPipeline(tmp_dir)
        assert recovered.lattice.applied_pos == 100
        assert recovered.lattice.proved_pos == 0

        can, reason = recovered.lattice.can_authorize_cutover(100)
        assert can is False
        assert "proof" in reason or "proved" in reason

        recovered.prove(100)
        assert recovered.lattice.proved_pos == 100
        can, _ = recovered.lattice.can_authorize_cutover(100)
        assert can is True
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_dict_roundtrip_and_summary():
    lat = PositionLattice(read_pos=1000, retained_pos=900, applied_pos=800, proved_pos=700, source_ack_pos=900)
    d = lat.to_dict()
    restored = PositionLattice.from_dict(d)
    assert restored == lat
    summary = lat.summary()
    assert "read=1000" in summary
    assert "retained=900" in summary
    assert "applied=800" in summary
    assert "proved=700" in summary
    assert "ack=900" in summary
