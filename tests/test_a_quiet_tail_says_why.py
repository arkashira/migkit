"""A tail that carries nothing says why, per database (field run
2026-10-16: "no delta any more" on two databases, and the operator could
not tell a detached reader from a source busy elsewhere): attached at
where, the source at where, what it applied and since when, and what the
quiet is (`decide.tail_state`, `tailctl.quiet_line`)."""
import itertools

from migkit import decide, tailctl


def _run(where_now, rounds=40):
    said, n = [], itertools.count()

    def look(token):
        return token, 0, True
    tailctl.run_loop(None, "payment", look, said.append, "changes",
                     go=False, token={"log_file": "b.000001",
                                      "log_pos": 100},
                     until_="caught_up",
                     reached=lambda t: next(n) >= rounds,
                     idle=(0.001, 0.005), where_now=where_now)
    return said


def test_the_source_writing_elsewhere_is_said_with_the_counts():
    said = _run(lambda token: {"at": "b.000001:100",
                               "server": "b.000001:4200", "moved": 4100,
                               "attached": True})
    quiet = [s for s in said if "quiet" in s]
    assert len(quiet) == 1, said
    assert quiet[0].startswith("payment: attached at b.000001:100, the"
                               " source at b.000001:4200; 0 changes")
    assert "the source wrote 4,100 bytes of its log elsewhere, none on" \
           " your tables" in quiet[0]


def test_a_detached_reader_is_said_with_its_reason_not_silence():
    said = _run(lambda token: {"at": "b.000001:100", "attached": False,
                               "why_detached": "the source lists no reader"
                               " under server_id 2147483700"})
    assert any(s.startswith("payment: the reader is not attached (the"
                            " source lists no reader under server_id"
                            " 2147483700)") for s in said), said


def test_a_reader_that_cannot_be_asked_is_said_detached():
    def boom(token):
        raise ConnectionError("Lost connection to server during query")
    said = _run(boom)
    assert any("not attached (Lost connection" in s for s in said), said


def test_a_still_source_is_quiet_and_a_change_within_the_interval_is_not():
    assert decide.tail_state({"attached": True, "moved": 0, "quiet_for": 9,
                              "interval": 3}).path == "idle"
    assert decide.tail_state({"attached": True, "moved": 900,
                              "quiet_for": 1, "interval": 3}
                             ).path == "following"


def test_the_providers_writes_on_the_target_are_said():
    line = tailctl.quiet_line("shipping", "idle", {
        "at": "b.000002:4", "server": "b.000002:4",
        "sync_back": ["__dst__tencentdb__sync_6fve1pd0"]}, 12, 0.0)
    assert "quiet: the source wrote nothing" in line
    assert "also writes the target: __dst__tencentdb__sync_6fve1pd0" in line
