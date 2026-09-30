"""The ranges of one table climb the ways a range can go on that table's own
measurements (`decide.Trials`): each way never timed is tried on one range,
ranges started meanwhile go to the next way never timed rather than a
second trial of the same one, then every range takes the cheapest; a way
that failed on the table is passed over for the rest of it, with why. No
server: the rungs are built for the purpose."""
import threading

from migkit import decide as d

R = d.Rung


def _ways():
    return [R("pass", "server to server"), R("slow pass", "unawaited"),
            R("pipe", "through migkit")]


def test_ranges_started_side_by_side_try_each_way_once():
    t = d.Trials("public.big", _ways())
    # three ranges started before any finished: each way once
    got = [t.pick().path for _ in range(3)]
    assert got == ["pass", "slow pass", "pipe"], got
    t.took("pass", 50_000, 1.0)
    t.took("slow pass", 50_000, 2.0)
    # the pipe is still being timed: the cheapest timed way, not a second
    # trial of the pipe
    assert t.pick().path == "pass"
    t.took("pipe", 50_000, 0.5)
    got = t.pick()
    assert got.path == "pipe" and "measured the cheapest" in got.reason


def test_a_way_given_up_on_is_passed_over_with_why():
    t = d.Trials("public.big", _ways())
    assert t.pick().path == "pass"
    t.give_up("pass", "both servers digest a range differently")
    t.give_up("slow pass", "both servers digest a range differently")
    got = t.pick()
    assert got.path == "pipe", got
    assert got.reason == "both servers digest a range differently", got
    # given up for good on this table, however cheap it measured
    t.took("pass", 50_000, 0.001)
    assert t.pick().path == "pipe"


def test_a_way_that_cannot_hold_is_never_tried():
    need = d.Need("a binary encoding", lambda f: not f["binary"],
                  lambda f: f["binary"])
    ways = [R("pass", "server to server", needs=(need,)),
            R("pipe", "through migkit")]
    t = d.Trials("public.geo", ways,
                 d.Facts({"binary": "shape has no binary encoding"}))
    got = t.pick()
    assert got.path == "pipe"
    assert got.reason == "shape has no binary encoding"


def test_picks_from_threads_are_each_counted_once():
    t = d.Trials("public.big", _ways())
    seen, lock = [], threading.Lock()

    def one():
        got = t.pick().path
        with lock:
            seen.append(got)
    threads = [threading.Thread(target=one) for _ in range(3)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert sorted(seen) == ["pass", "pipe", "slow pass"], seen


def test_costs_read_from_the_run_first_and_written_to_every_layer():
    run, kept = d.Costs(), d.Costs({"pass": 9.0})
    both = d.Layered(run, kept)
    assert both.get("pass") == 9.0
    both.saw("pass", 10, 1.0)
    assert run["pass"] == 0.1 and kept["pass"] == 0.1
    assert both.get("pipe") is None


def test_the_ranges_ways_are_the_engines_own_chooser_and_report():
    """A table's ranges climb on the costs the engine keeps for every way
    it times (`_cheaper`, `_took`), and `measured_ways` reports them - with
    the needs the ways read answered from the table's own facts."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.base import Engine
    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    eng = Engine(hop)
    need = d.Need("a binary encoding", lambda f: not f["binary"],
                  lambda f: f["binary"])
    ways = [R("pass", "server to server", needs=(need,)),
            R("pipe", "through migkit")]
    key = eng._work_key("range copy", "public.big")
    t = eng._trials(key, ways, d.Facts({"binary": ""}))
    assert eng._trials(key, ways) is t
    assert [t.pick().path, t.pick().path] == ["pass", "pipe"]
    t.took("pass", 10_000, 1.0)
    t.took("pipe", 10_000, 3.0)
    got = [w for w in eng.measured_ways() if w["work"] == "range copy"]
    assert got == [{"work": "range copy", "table": "public.big",
                    "rows_from": None,
                    "seconds_a_row": {"pass": 1e-4, "pipe": 3e-4},
                    "chosen": "pass"}], got
    # the table's facts said again: a way that no longer holds is not
    # chosen, nor reported as chosen
    eng._trials(key, ways, d.Facts({"binary": "n is int on one side"}))
    assert t.pick().path == "pipe"
    assert [w["chosen"] for w in eng.measured_ways()] == ["pipe"]
