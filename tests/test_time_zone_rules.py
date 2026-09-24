"""Whether the two servers agree about what a zone name means.

A zone name is not a constant. `America/Sao_Paulo` observed DST until Brazil
abolished it in 2019; `Asia/Tehran` until Iran abolished it in 2022. Two
servers carrying different rule sets answer the same conversion differently,
and the one carrying none answers NULL.

Measured before any of this was written, on the official `mysql:8` image -
which arrives with **1,795** zones already loaded, so the problem is easy to
believe is somebody else's:

    emptied and restarted
      convert_tz('2026-07-01 12:00:00','UTC','America/New_York')  -> NULL
      show warnings                                               -> nothing
      convert_tz('2026-07-01 12:00:00','+00:00','-04:00')         -> 08:00:00

The offset form still works, so a smoke test written that way passes while
every named zone answers NULL. And a **stored generated column** built on
`CONVERT_TZ` wrote NULL to disk for a row whose source value was present.

The fingerprint migkit compares is "what wall clock does this zone show at
these instants", which turns out to be portable between engines: PostgreSQL
16 and MySQL 8 were measured producing the **same md5** for
`America/New_York` (`789e2da8…`), `America/Sao_Paulo`, `Asia/Tehran` and
`UTC`. A test below pins that rather than taking it on trust.

What could *not* be reproduced here: two PostgreSQL images four major
versions apart (13 and 16) agreed on all 487 zones at these probes, so
tzdata drift between them is not demonstrable in this sandbox. The drift
path is proven against MySQL, where the rules can be made to differ for
real.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

A_PORT, B_PORT = 13403, 13404
NAMES = {A_PORT: "migkit-test-tz-a", B_PORT: "migkit-test-tz-b"}


@pytest.fixture(scope="module")
def tz_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        _wait(port)
    yield {"a": A_PORT, "b": B_PORT}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _wait(port):
    for _ in range(90):
        if subprocess.run(["docker", "exec", NAMES[port], "mysql", "-uroot",
                           "-ptest", "-h127.0.0.1",
                           "--protocol=tcp", "-e", "select 1"],
                          capture_output=True).returncode == 0:
            return
        time.sleep(1)
    pytest.fail(f"mysql on {port} never answered")


def my(port, sql):
    got = subprocess.run(["docker", "exec", NAMES[port], "mysql", "-uroot",
                          "-ptest", "-D", "mysql", "-N", "-B", "-e", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _restart(port):
    """The rules are cached, so loading or removing them is not enough on
    its own - which is the same reason an operator's fix appears not to
    work."""
    subprocess.run(["docker", "restart", NAMES[port]], capture_output=True)
    _wait(port)


def _engine(tz_pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="tz", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=tz_pair["a"],
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=tz_pair["b"],
                              user="root", password="test"),
              databases=["mysql"])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def test_the_three_ways_two_servers_can_disagree(tz_pair, tmp_path):
    """One walk, because each step needs a restart and the progression is
    the story: agreeing, then one zone meaning something else, then one
    zone missing, then none at all."""
    eng = _engine(tz_pair, tmp_path)

    agreed = eng._time_zone_rules("mysql")
    assert agreed.status == "ok", agreed.detail
    assert "1795 named zones" in agreed.detail, agreed.detail

    # 1. the rules for one zone differ. Stripping its transitions drops it
    # to local mean time, which is exactly what an older rule set looks
    # like to this check: the same name, a different answer.
    before = my(tz_pair["b"], "select convert_tz('2018-01-15 12:00:00','UTC',"
                              "'America/Sao_Paulo')")
    zone_id = my(tz_pair["b"], "select Time_zone_id from"
                               " mysql.time_zone_name where Name ="
                               " 'America/Sao_Paulo'")
    my(tz_pair["b"], "delete from mysql.time_zone_transition where"
                     f" Time_zone_id = {zone_id}")
    _restart(tz_pair["b"])
    after = my(tz_pair["b"], "select convert_tz('2018-01-15 12:00:00','UTC',"
                             "'America/Sao_Paulo')")
    assert before != after, (before, after)

    drifted = eng._time_zone_rules("mysql")
    assert drifted.status == "warn", drifted.detail
    assert "America/Sao_Paulo" in drifted.detail, drifted.detail
    assert "two answers" in drifted.detail, drifted.detail

    # 2. a zone the target does not have at all outranks that
    my(tz_pair["b"], "delete from mysql.time_zone_name where Name ="
                     " 'Asia/Tehran'")
    _restart(tz_pair["b"])
    assert my(tz_pair["b"], "select ifnull(convert_tz('2026-07-01 12:00:00',"
                            "'UTC','Asia/Tehran'),'NULL')") == "NULL"

    missing = eng._time_zone_rules("mysql")
    assert missing.status == "diff", missing.detail
    assert "Asia/Tehran" in missing.detail, missing.detail
    assert "returns NULL on the target" in missing.detail, missing.detail

    # 3. and a server that can resolve nothing is the worst of the three
    my(tz_pair["b"], "delete from mysql.time_zone_name")
    _restart(tz_pair["b"])
    empty = eng._time_zone_rules("mysql")
    assert empty.status == "diff", empty.detail
    assert "cannot resolve a single zone name" in empty.detail, empty.detail
    assert "writes NULL to disk" in empty.detail, empty.detail

    # the offset form still works there, which is why this goes unnoticed
    assert my(tz_pair["b"], "select convert_tz('2026-07-01 12:00:00',"
                            "'+00:00','-04:00')") == "2026-07-01 08:00:00"
    # ... and `show warnings` says nothing about any of it
    assert my(tz_pair["b"], "select ifnull(convert_tz('2026-07-01 12:00:00',"
                            "'UTC','America/New_York'),'NULL');"
                            " show warnings") == "NULL"


def test_postgres_compares_its_own_zones(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="tz", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    got = eng._time_zone_rules("postgres")
    assert got.status == "ok", got.detail
    # it really read them rather than comparing two empty sets
    zones = eng._zone_fingerprints("src", "postgres")
    assert len(zones) > 100, len(zones)
    assert "America/New_York" in zones and "UTC" in zones


def test_the_probes_see_a_rule_that_really_changed(pg_pair):
    """The fingerprint is only worth having if the instants it asks about
    are ones where zones actually differ. Brazil abolished DST in 2019:
    summer in Sao Paulo was UTC-2 before and UTC-3 after, and both probes
    are on the right side of that line."""
    from tests.conftest import psql
    got = psql(pg_pair["src"],
               "select (timestamptz '2018-01-15 12:00+00' at time zone"
               " 'America/Sao_Paulo')::text||' / '||"
               "(timestamptz '2020-01-15 12:00+00' at time zone"
               " 'America/Sao_Paulo')::text")
    assert got.stdout.strip() == \
        "2018-01-15 10:00:00 / 2020-01-15 09:00:00", got.stdout


def test_the_two_engines_fingerprint_a_zone_identically(tz_pair, pg_pair,
                                                         tmp_path):
    """Measured, then pinned: the reading is "what wall clock does this
    zone show at this instant", which both engines render the same way. It
    is what lets this check work across a hop between two engines rather
    than only within one."""
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine

    pg_hop = Hop(name="tz", engine="postgres",
                 source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                                 user="postgres", password="test"),
                 target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                                 user="postgres", password="test"),
                 databases=["postgres"], workers=2)
    pg_hop.report_dir = lambda db=None: tmp_path
    my_hop = Hop(name="tz", engine="mysql",
                 source=Endpoint(host="127.0.0.1", port=tz_pair["a"],
                                 user="root", password="test"),
                 target=Endpoint(host="127.0.0.1", port=tz_pair["a"],
                                 user="root", password="test"),
                 databases=["mysql"])
    my_hop.report_dir = lambda db=None: tmp_path

    pg_zones = PostgresEngine(pg_hop)._zone_fingerprints("src", "postgres")
    my_zones = MySQLEngine(my_hop)._zone_fingerprints("src", "mysql")
    shared = ["UTC", "America/New_York", "Asia/Tehran", "Europe/Lisbon"]
    for zone in shared:
        assert zone in pg_zones and zone in my_zones, zone
        assert pg_zones[zone] == my_zones[zone], (
            zone, pg_zones[zone], my_zones[zone])
    # and the fingerprints are not all the same value, which would make the
    # agreement above meaningless
    assert len({pg_zones[z] for z in shared}) == len(shared)


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="z", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    empty = eng._time_zone_result("x", 600, 0, [], [], "hint")
    assert empty.status == "diff"
    assert "target cannot resolve a single zone name" in empty.detail

    # a source that cannot resolve anything is just as wrong
    other = eng._time_zone_result("x", 0, 600, [], [], "hint")
    assert other.status == "diff" and "source cannot" in other.detail

    missing = eng._time_zone_result("x", 600, 599, ["Asia/Tehran"],
                                    ["Europe/Kyiv"], "hint")
    assert missing.status == "diff", missing.detail
    assert "Asia/Tehran" in missing.detail
    # the harder failure wins rather than both being averaged into a warning
    assert "Europe/Kyiv" not in missing.detail, missing.detail

    drift = eng._time_zone_result("x", 600, 600, [], ["Europe/Kyiv"], "hint")
    assert drift.status == "warn" and "Europe/Kyiv" in drift.detail

    fine = eng._time_zone_result("x", 600, 600, [], [], "hint")
    assert fine.status == "ok" and "600 named zones" in fine.detail


def test_an_engine_that_cannot_be_asked_says_so(tmp_path):
    """The base returns None rather than an empty mapping, because "no
    zones" and "no way to look" must not read the same."""
    from migkit.engines.base import Engine
    hop = Hop(name="z", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Engine(hop)
    assert eng._zone_fingerprints("src", "x") is None
    got = eng._time_zone_rules("x")
    assert got.status == "skip", got.detail
    assert "does not expose what its zone names mean" in got.detail
