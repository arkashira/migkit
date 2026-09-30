"""One stamp, one duration, one size, one table-name splitter, one retry
policy and one account of what a deep check could not read (audit
2026-10-01, DU10, DU11, DU14, DU21, DU24, W11, LG2).

Each was done two to seven ways that disagreed - measured: 59.6 s said
as `1m00s`, `59s`, `60 seconds`, `60s`; 10^12 bytes as "931.3 GB";
`public."v1.orders"` copied by one path and compared by another; a
password that failed retried four times with fifteen seconds of sleeps;
a deep check that could not read a column saying `ok`.
"""
import pytest

from migkit import util, wording


def test_a_time_is_utc_with_its_zone():
    assert util.stamp(0) == "1970-01-01T00:00:00+00:00"
    assert util.stamp(1759311000.7) == "2025-10-01T09:30:00+00:00"
    assert util.stamp().endswith("+00:00")


@pytest.mark.parametrize("secs,said", [(0.4, "0s"), (59.6, "1m00s"),
                                       (61, "1m01s"), (3599.7, "1h00m"),
                                       (7322, "2h02m")])
def test_a_duration_is_said_one_way(secs, said):
    assert util.human_secs(secs) == said == wording._duration(secs)


@pytest.mark.parametrize("n,said", [(512, "512 bytes"),
                                    (1536, "1.5 KiB"),
                                    (10 ** 12, "931.3 GiB"),
                                    (78.75e6, "75.1 MiB"),
                                    (3 * 2 ** 50, "3.0 PiB")])
def test_a_size_is_named_in_the_units_it_is_counted_in(n, said):
    assert wording.human_bytes(n) == said


@pytest.mark.parametrize("engine,name,parts", [
    ("postgres", 'public."v1.orders"', ("public", "v1.orders")),
    ("postgres", "public.v1.orders", ("public", "v1.orders")),
    ("postgres", '"we""ird".t', ('we"ird', "t")),
    ("postgres", "orders", ("", "orders")),
    ("mysql", "a.b", ("", "a.b")),
    ("mariadb", "`shop`.`a.b`", ("shop", "a.b")),
    ("mysql", "`a``b`", ("", "a`b")),
    ("mssql", "[dbo].[x]]y]", ("dbo", "x]y")),
    ("mongodb", "system.profile", ("", "system.profile")),
])
def test_a_table_name_is_split_one_way(engine, name, parts):
    assert util.split_table(engine, name) == parts


def test_a_table_name_is_quoted_one_way():
    assert util.split_table("postgres", "orders", "public") == \
        ("public", "orders")
    assert util.qualify("postgres", "public", "v1.orders") == \
        '"public"."v1.orders"'
    assert util.qualify("mysql", "", "a`b") == "`a``b`"
    assert util.qualify("mssql", "dbo", "x]y") == "[dbo].[x]]y]"
    # what one splits the other puts back
    for engine, name in (("postgres", '"public"."v1.orders"'),
                         ("mysql", "`shop`.`a``b`")):
        assert util.qualify(engine, *util.split_table(engine, name)) == name


def test_what_writes_is_tried_again_only_while_connecting():
    lost = "(2013, 'Lost connection to MySQL server during query')"
    refused = ("could not connect to server: Connection refused\n"
               "\tIs the server running on host \"10.0.0.5\"?")
    assert util.retryable(lost) and not util.retryable(lost, writes=True)
    assert util.retryable(refused) and util.retryable(refused, writes=True)
    assert not util.retryable("password authentication failed for user x")
    # a driver's own wait running out was read as permanent
    assert util.is_transient("h1:27017: timed out")
    calls = []

    def once():
        calls.append(1)
        raise RuntimeError(lost)
    with pytest.raises(RuntimeError):
        util.with_retry(once, tries=4, base=0, writes=True)
    assert len(calls) == 1
    calls.clear()
    with pytest.raises(RuntimeError):
        util.with_retry(once, tries=4, base=0)
    assert len(calls) == 4


def test_a_command_that_runs_out_of_time_is_not_run_again(monkeypatch):
    import subprocess
    import sys
    starts = []
    real = subprocess.run

    def counted(*a, **k):
        starts.append(1)
        return real(*a, **k)
    monkeypatch.setattr(util.subprocess, "run", counted)
    with pytest.raises(subprocess.TimeoutExpired):
        util.run([sys.executable, "-c", "import time; time.sleep(5)"],
                 timeout=0.2)
    assert starts == [1]


def test_a_deep_check_counts_what_it_could_not_read():
    p = util.Probes("columns", planned=["t.a", "t.b", "t.c", "t.d"])
    with p.reading("t.a"):
        pass
    with p.reading("t.b"):
        raise TimeoutError("read timed out")
    p.done("t.c")
    status, detail = p.verdict("ok", "charsets agree")
    assert status == "warn", status
    assert detail == ("charsets agree; compared 2 of 4 columns; not read:"
                      " t.b (read timed out), t.d (not reached)"), detail
    # a difference found stands, and says what it did not see
    assert p.verdict("diff", "t.a differs")[0] == "diff"
    nothing = util.Probes("shards", planned=["s1"])
    nothing.failed("s1", "not authorized")
    assert nothing.verdict("ok") == ("skip", "compared 0 of 1 shards; not"
                                             " read: s1 (not authorized)")
    whole = util.Probes("columns", planned=["x"])
    whole.done("x")
    assert whole.verdict("ok", "fine") == ("ok", "fine")


def test_a_failure_that_is_not_an_error_goes_on():
    p = util.Probes()
    with pytest.raises(KeyboardInterrupt):
        with p.reading("t.a"):
            raise KeyboardInterrupt
