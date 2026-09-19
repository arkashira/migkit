"""The statistics check's own reasoning, measured.

`_planner_stats_result` justifies itself like this: "Autoanalyze is
threshold-driven rather than event-driven, so on a large table it will not
run until a tenth of the rows have changed again - which, on a table that
was migrated and is now only read, may be never."

The bulk load *is* those modifications. Measured on PostgreSQL 16, a
5,000-row table loaded and then left completely alone:

    at load    reltuples=-1  ever_analyzed=0  n_mod_since_analyze=5000
               threshold = autovacuum_analyze_threshold 50
                         + autovacuum_analyze_scale_factor 0.1 * 5000 = 550
    90s later  ever_analyzed=1  last_autoanalyze=10:21:45  n_mod=0

Autoanalyze ran by itself one naptime after the load. It does not wait for
a *second* round of changes, so any migrated table above about 55 rows is
in the same position.

What it costs: on a pair where every parity check passes - schema identical
by five separate differs, counts equal, checksums equal - the run says

    deep postgres statistics: DIFF 1 tables the planner has no statistics for
    verdict: different

for about a minute, then says `OK` with nothing changed but time. `diff`
everywhere else in migkit means *the two sides do not match*.

The check still has a real job: with `autovacuum=off` the same table was
still unanalyzed after 75 seconds, and always will be. These tests pin both
halves so the distinction cannot be lost when the check is fixed, and pin
the transience itself so the refuted claim cannot quietly come back.

**Deliberately slow.** Two tests wait out an autovacuum naptime, because
the only honest way to ask "does the server fix this by itself" is to let
it try. They are the reason this lives in its own file.
"""
import socket
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

ON, OFF = 15535, 15537
NAMES = {ON: "migkit-test-stat-on", OFF: "migkit-test-stat-off"}

#: One naptime (60s) plus room for the worker to get to this table.
NAPTIME_WAIT = 100

SEED = ("create table orders (id int primary key, amt numeric, note text);"
        " insert into orders select g, g*1.5, 'note'||g"
        " from generate_series(1,5000) g;")


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", "-i", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


def _analyzed(port):
    got = q(port, "select (last_analyze is not null"
                  " or last_autoanalyze is not null)::int"
                  " from pg_stat_user_tables where relname='orders'")
    return got.stdout.strip() == "1"


@pytest.fixture(scope="module")
def stat_pair():
    """One server with autovacuum as it ships, one with it off. Seeded at
    the same moment so the waits below cover both."""
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        cmd = ["docker", "run", "-d", "--name", name, "-e",
               "POSTGRES_PASSWORD=test", "-p", f"{port}:5432", "postgres:16"]
        if port == OFF:
            cmd += ["-c", "autovacuum=off"]
        subprocess.run(cmd, check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
        got = q(port, SEED)
        assert got.returncode == 0, got.stderr
    loaded_at = time.time()
    yield {"on": ON, "off": OFF, "loaded_at": loaded_at}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def test_the_load_alone_already_passes_the_threshold(stat_pair):
    """The arithmetic the refuted claim got wrong: the bulk load counts as
    modifications, and 5000 is already far past 50 + 0.1 * 5000."""
    settings = dict(
        line.split("=", 1) for line in
        q(ON, "select name||'='||setting from pg_settings where name in"
              " ('autovacuum','autovacuum_naptime',"
              " 'autovacuum_analyze_threshold',"
              " 'autovacuum_analyze_scale_factor') order by 1"
          ).stdout.splitlines() if line)
    assert settings["autovacuum"] == "on", settings
    threshold = (float(settings["autovacuum_analyze_threshold"])
                 + float(settings["autovacuum_analyze_scale_factor"]) * 5000)
    assert threshold == 550, (threshold, settings)

    modified = int(q(ON, "select n_mod_since_analyze from"
                         " pg_stat_user_tables where relname='orders'"
                     ).stdout.strip())
    assert modified == 5000, modified
    assert modified > threshold, (modified, threshold)


def test_the_server_analyzes_it_without_being_asked(stat_pair):
    """The refutation. Nothing touches the table; only time passes."""
    assert not _analyzed(ON) or time.time() - stat_pair["loaded_at"] > 60, \
        "already analyzed before the wait - the measurement needs a fresh load"
    deadline = stat_pair["loaded_at"] + NAPTIME_WAIT
    while time.time() < deadline and not _analyzed(ON):
        time.sleep(5)
    assert _analyzed(ON), (
        "autoanalyze did not run within %ds of the load; the check's claim"
        " that it 'may be never' would then hold" % NAPTIME_WAIT)
    assert q(ON, "select n_mod_since_analyze from pg_stat_user_tables"
                 " where relname='orders'").stdout.strip() == "0"


def test_with_autovacuum_off_it_really_is_never(stat_pair):
    """The other half, and the reason the check is worth keeping. Same
    table, same wait, a server that will not do it."""
    assert q(OFF, "select setting from pg_settings where name='autovacuum'"
             ).stdout.strip() == "off"
    deadline = stat_pair["loaded_at"] + NAPTIME_WAIT
    while time.time() < deadline:
        time.sleep(5)
    assert not _analyzed(OFF), "autovacuum was supposed to be off"
    assert q(OFF, "select n_mod_since_analyze from pg_stat_user_tables"
                  " where relname='orders'").stdout.strip() == "5000"


def test_the_two_servers_are_told_apart_by_something_readable(stat_pair):
    """What the fix will key on, pinned now so it is known to be available
    from the target's own catalog: the global setting, and the per-table
    reloption `migkit move --go` already sets."""
    assert q(ON, "select setting from pg_settings where name='autovacuum'"
             ).stdout.strip() == "on"
    assert q(OFF, "select setting from pg_settings where name='autovacuum'"
             ).stdout.strip() == "off"

    got = q(ON, "select coalesce((select option_value from"
                " pg_options_to_table(c.reloptions)"
                " where option_name = 'autovacuum_enabled'), 'unset')"
                " from pg_class c where c.relname = 'orders'")
    assert got.returncode == 0, got.stderr
    assert got.stdout.strip() == "unset", got.stdout
