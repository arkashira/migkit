"""One progress vocabulary for every path (backlog 0c): tables, bytes,
the rate and the time left, from the source's own catalogue. The rate and
the time left are said only once there is something to divide."""
from migkit import wording


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_a_bulk_path_says_how_far_how_fast_and_how_long_left():
    clock = Clock()
    tally = wording.Tally({"public.orders": 100 * 2 ** 20,
                           "public.lines": 300 * 2 ** 20}, clock=clock)
    clock.now += 10
    # a program names the table its own way: found by its last part
    said = tally.reached("app.orders")
    assert said == ("1 of 2 tables, 100.0 MB of 400.0 MB (25%), 10.0 MB/s,"
                    " about 30s left"), said
    assert tally.reached("app.orders") is None      # counted once
    clock.now += 30
    assert tally.reached("public.lines") == (
        "2 of 2 tables, 400.0 MB of 400.0 MB (100%), 10.0 MB/s"), tally.seen


def test_without_sizes_it_counts_tables_and_guesses_nothing():
    tally = wording.Tally(None, total=12, clock=Clock())
    assert tally.reached("t") == "1 of 12 tables"
    assert wording.Tally(None).reached("t") == "1 tables"


def test_a_restart_rates_its_own_rows_only():
    line = wording.progress("t", 150_000, 200_000, started=0, now=10,
                            since=100_000)
    # 50,000 rows this run in 10s, not 150,000
    assert "5,000 rows/s" in line and "about 10s left" in line, line


def test_the_bulk_readers_use_it(monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "_SIZES", {"public.t": 2 ** 30})
    read = movers._tables_done(movers.PG_DUMP_TABLE, "read")
    said = read('pg_dump: dumping contents of table "public.t"')
    assert said.startswith("public.t: read (1 of 1 tables, 1.0 GB of 1.0 GB"
                           " (100%)"), said
