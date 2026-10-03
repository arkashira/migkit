"""The plan says, before a bulk copy starts, what a stop costs it.

A managed service says whether its dump goes on after a restart
("resumable dump status"); migkit's plan named the way and nothing of
that, and a way that starts over after a stop - every table it had
copied copied again - was found out only by stopping it. Each bulk way
says it now, in the plan's words and without naming a program.
"""
from migkit import movers
from tests.test_the_report_does_not_name_its_tools import TOOLS


def test_every_bulk_way_says_what_a_stop_costs_it():
    for via in movers.VIAS:
        said = movers.after_a_stop(via)
        assert said.startswith("a stop "), via
        assert not any(t in said.lower() for t in TOOLS), (via, said)


def test_the_ways_that_go_on_say_how_far():
    assert "ranges" in movers.after_a_stop("builtin")
    assert "tables it had finished" in movers.after_a_stop("pgcopydb")
    assert "starts over" in movers.after_a_stop("mydumper")
    assert "starts over" in movers.after_a_stop("pgdump")
