"""What an engine can do is read off that engine, and said in one set of
words (audit 2026-10-01, DU17, SP17).

Measured before: asking whether PostgreSQL could do six things imported
all 34 engine modules and read their methods' source, 360-420 ms a
command; Kafka's bulk move was declared not applicable while the cluster
mirror copied a topic whole; the table of ways by shape left Iceberg and
Delta undeclared; and the shapes said their gaps in words of their own.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_asking_one_engine_reads_that_engine_only():
    code = ("import sys\n"
            "from migkit import capabilities as c\n"
            "for cap in c.CAPABILITIES:\n"
            "    c.unavailable('postgres', cap)\n"
            "print(sorted(m for m in sys.modules"
            " if m.startswith('migkit.engines.')))\n")
    got = subprocess.run([sys.executable, "-c", code], cwd=ROOT,
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 0, got.stderr
    loaded = eval(got.stdout.strip().splitlines()[-1])
    assert "migkit.engines.postgres" in loaded, loaded
    for other in ("migkit.engines.mysql", "migkit.engines.oracle",
                  "migkit.engines.cassandra", "migkit.engines.warehouse"):
        assert other not in loaded, (other, loaded)


def test_kafkas_bulk_move_is_read_off_the_code(monkeypatch):
    """Like Redis's: declared nowhere, read off the mover that does it,
    and a gap nobody declared once that mover is taken away."""
    from migkit import capabilities as caps
    from migkit import movers
    assert caps.implemented("kafka", "bulk-move")
    assert "bulk-move" not in caps.GAPS["kafka"]
    assert caps.stale() == []
    real = movers.supported
    monkeypatch.setattr(movers, "supported", lambda engine, via: False
                        if engine == "kafka" and via == "mirror"
                        else real(engine, via))
    assert ("kafka", "bulk-move") in caps.undeclared()


def test_the_shapes_say_their_gaps_in_the_capabilities_words():
    from migkit import capabilities as caps
    from migkit import decide
    assert decide.NOT_YET is caps.NOT_YET
    assert decide.NOT_APPLICABLE is caps.NOT_APPLICABLE
    assert decide.WAYS["keyless"]["kinesis"] == (
        caps.NOT_APPLICABLE, caps.STREAM_ONLY.format("Kinesis", "kinesis"))
    assert decide.uncovered() == []
    for lake in ("iceberg", "delta"):
        assert decide.cell(lake, "keyless")[0] == "yes"
        assert decide.cell(lake, "wide key")[0] == caps.NOT_APPLICABLE


def test_a_removed_way_is_still_noticed(monkeypatch):
    """The lakes' keyless cell has teeth: take their reader away and the
    cell goes stale."""
    from migkit import decide
    from migkit.engines.lake import _Lake
    monkeypatch.delattr(_Lake, "neutral_batches")
    assert ("iceberg", "keyless") in decide.uncovered()

