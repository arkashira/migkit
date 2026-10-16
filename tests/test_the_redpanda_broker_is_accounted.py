"""The streaming pipeline's broker is in the whole-use registry with the
options migkit gives it, each with the reason it is used."""
from migkit import tooluse


def test_the_broker_start_flags_are_written_as_used():
    assert tooluse.used("redpanda", "start") == sorted([
        "--advertise-kafka-addr", "--kafka-addr", "--memory", "--node-id",
        "--overprovisioned", "--smp"])


def test_the_consumed_topic_is_read_from_its_end_in_values_only():
    assert tooluse.used("redpanda", "topic consume") == [
        "--format", "--offset"]
    assert tooluse.entry("redpanda", "topic consume", "--offset")[0] \
        == tooluse.USED


def test_the_signal_is_produced_one_tab_separated_line_at_a_time():
    assert tooluse.used("redpanda", "topic produce") == ["-f"]


def test_every_written_option_has_a_state_and_a_reason():
    rows = tooluse.accounts("redpanda")
    assert len(rows) == 9
    for key, (state, why) in rows.items():
        assert state in tooluse.STATES, key
        assert len(str(why).split()) >= 3, key
