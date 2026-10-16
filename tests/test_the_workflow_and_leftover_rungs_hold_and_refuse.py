"""The workflow's own rungs each held and refused on the facts that decide
them (backlog AUD-Z.rungs-owed): a phase order, a phase's next step, a
range whose session was read, a hold or a dump left behind, a lost link's
wait and a keyed read's page - each taken only on what was read, and each
passed over with the reason its need gives. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("phase_order", "schema-whole", "holds"),
    ("phase_order", "schema-whole", "refuses"),
    ("phase_order", "constraints-after", "holds"),
    ("phase_step", "enter", "holds"), ("phase_step", "enter", "refuses"),
    ("phase_step", "blocked", "holds"), ("phase_step", "blocked", "refuses"),
    ("phase_step", "wait", "holds"),
    ("range_stall", "replay", "holds"), ("range_stall", "replay", "refuses"),
    ("range_stall", "close", "holds"), ("range_stall", "close", "refuses"),
    ("range_stall", "wait", "holds"),
    ("lift_hold", "lift", "holds"), ("lift_hold", "lift", "refuses"),
    ("lift_hold", "wait", "holds"),
    ("leftover_dump", "remove", "holds"),
    ("leftover_dump", "remove", "refuses"),
    ("leftover_dump", "keep", "holds"),
    ("lost_link_wait", "measured", "holds"),
    ("lost_link_wait", "measured", "refuses"),
    ("lost_link_wait", "as long again", "holds"),
    ("lost_link_wait", "as long again", "refuses"),
    ("lost_link_wait", "at once", "holds"),
    ("page_rows", "measured", "holds"), ("page_rows", "measured", "refuses"),
    ("page_rows", "by width", "holds"), ("page_rows", "by width", "refuses"),
    ("page_rows", "unmeasured", "holds"),
]


def test_the_schema_goes_whole_only_where_no_cycle_of_keys_was_read():
    with watched() as seen:
        got = decide.phase_order({"fk cycle": False, "target keys": True})
    assert got.path == "schema-whole"
    assert branch(seen, "schema-whole") == [HOLDS]
    assert branch(seen, "constraints-after") == [HOLDS]
    with watched() as seen:
        got = decide.phase_order({"fk cycle": None, "target keys": True})
    assert got.path == "constraints-after"
    assert branch(seen, "schema-whole") == [REFUSES]
    assert "were not read" in why(seen, "schema-whole")
    with watched() as seen:
        got = decide.phase_order({"fk cycle": True, "target keys": True})
    assert got.path == "constraints-after"
    assert "close a cycle" in why(seen, "schema-whole")


def test_a_phase_enters_waits_or_is_blocked_by_its_prerequisites():
    with watched() as seen:
        got = decide.phase_step({})
    assert got.path == "enter"
    assert branch(seen, "enter") == [HOLDS]
    assert branch(seen, "blocked") == [REFUSES]
    assert branch(seen, "wait") == [HOLDS]
    with watched() as seen:
        got = decide.phase_step({"prerequisites owed": ["schema"]})
    assert got.path == "wait"
    assert why(seen, "enter") == "it waits for ['schema']"
    assert branch(seen, "blocked") == [REFUSES]
    with watched() as seen:
        got = decide.phase_step({"prerequisite failed": "schema failed",
                                 "prerequisites owed": ["schema"]})
    assert got.path == "blocked"
    assert branch(seen, "blocked") == [HOLDS]
    assert branch(seen, "enter") == [REFUSES]
    with watched() as seen:
        got = decide.phase_step({"entry unmet": "no target"})
    assert got.path == "blocked"
    assert "its entry criteria do not hold: no target" == why(seen, "enter")


SESSION = {"open": True, "listed": True, "state": "idle",
           "idle_seconds": 100.0, "keepalive": 30.0, "owned": True}


def test_a_range_is_replayed_closed_or_waited_on_what_the_server_lists():
    with watched() as seen:
        got = decide.range_stall(dict(SESSION, listed=False))
    assert got.path == "replay"
    assert branch(seen, "replay") == [HOLDS]
    assert branch(seen, "close") == [REFUSES]
    assert why(seen, "close").startswith("not idle:")
    with watched() as seen:
        got = decide.range_stall(SESSION)
    assert got.path == "close"
    assert branch(seen, "close") == [HOLDS]
    assert why(seen, "replay").startswith("not gone:")
    with watched() as seen:
        got = decide.range_stall(dict(SESSION, idle_seconds=10.0))
    assert got.path == "wait"
    assert branch(seen, "wait") == [HOLDS]
    assert why(seen, "close").startswith("not idle long enough")
    with watched() as seen:
        got = decide.range_stall(dict(SESSION, owned=False))
    assert got.path == "wait"
    assert why(seen, "close").startswith("the session is not the hop's")
    with watched() as seen:
        got = decide.range_stall(dict(SESSION, open=False, listed=False))
    assert got.path == "wait"
    assert why(seen, "replay") == "the unit has closed: nothing is owed"


def test_a_hold_is_lifted_only_where_its_holder_is_gone():
    with watched() as seen:
        got = decide.lift_hold({"side": "dst", "holder gone": True})
    assert got.path == "lift"
    assert branch(seen, "lift") == [HOLDS]
    assert branch(seen, "wait") == [HOLDS]
    with watched() as seen:
        got = decide.lift_hold({"side": "dst", "holder gone": False})
    assert got.path == "wait"
    assert branch(seen, "lift") == [REFUSES]
    assert "still runs" in why(seen, "lift")
    with watched() as seen:
        got = decide.lift_hold({"side": "dst", "holder gone": None,
                                "age": 50.0, "longest step": 10.0})
    assert got.path == "lift"
    with watched() as seen:
        got = decide.lift_hold({"side": "dst", "holder gone": None,
                                "age": 5.0})
    assert got.path == "wait"
    assert "no run was kept" in why(seen, "lift")
    with watched() as seen:
        got = decide.lift_hold({"side": None, "holder gone": True})
    assert got.path == "wait"
    assert why(seen, "lift") == "the record names no side to give back"


def test_a_dump_is_removed_only_where_nothing_may_still_write_it():
    with watched() as seen:
        got = decide.leftover_dump({"owner gone": True})
    assert got.path == "remove"
    assert branch(seen, "remove") == [HOLDS]
    assert branch(seen, "keep") == [HOLDS]
    with watched() as seen:
        got = decide.leftover_dump({"owner gone": None, "age": 5.0,
                                    "longest step": 10.0})
    assert got.path == "keep"
    assert branch(seen, "remove") == [REFUSES]
    assert "not older than" in why(seen, "remove")
    with watched() as seen:
        got = decide.leftover_dump({"owner gone": False})
    assert got.path == "keep"
    assert "still runs" in why(seen, "remove")


def test_a_lost_link_waits_what_was_measured_then_as_long_again():
    with watched() as seen:
        seconds, got = decide.lost_link_wait([5.0, 2.0], down=1.0)
    assert got.path == "measured" and seconds == 4.0
    assert branch(seen, "measured") == [HOLDS]
    assert branch(seen, "as long again") == [HOLDS]
    assert branch(seen, "at once") == [HOLDS]
    with watched() as seen:
        seconds, got = decide.lost_link_wait([5.0, 2.0], down=6.0)
    assert got.path == "as long again" and seconds == 6.0
    assert why(seen, "measured") == ("this loss has outlasted every"
                                     " reconnect measured")
    with watched() as seen:
        seconds, got = decide.lost_link_wait((), down=0.0)
    assert got.path == "at once" and seconds == 0.0
    assert why(seen, "measured") == ("no lost link of this loop has come"
                                     " back yet")
    assert branch(seen, "as long again") == [REFUSES]
    assert why(seen, "as long again") == "the link was lost just now"


def test_a_page_is_sized_by_what_the_read_measured():
    with watched() as seen:
        rows, got = decide.page_rows({"rate": 1000.0, "width": 100,
                                      "free": 10 ** 9, "readers": 1,
                                      "seconds": 2.0})
    assert got.path == "measured" and rows == 2000
    assert branch(seen, "measured") == [HOLDS]
    assert branch(seen, "by width") == [HOLDS]
    assert branch(seen, "unmeasured") == [HOLDS]
    with watched() as seen:
        rows, got = decide.page_rows({"width": 10 ** 6, "free": 10 ** 9,
                                      "readers": 1})
    assert got.path == "by width"
    assert rows <= decide.PAGE_UNMEASURED
    assert why(seen, "measured") == "no page of this read has been timed"
    with watched() as seen:
        rows, got = decide.page_rows({"rate": 1000.0})
    assert got.path == "unmeasured" and rows == decide.PAGE_UNMEASURED
    assert branch(seen, "by width") == [REFUSES]
    assert why(seen, "by width") == "no row of this read has been measured"
    assert why(seen, "measured") == "no row of this read has been measured"
