"""Naming the software behind the protocol, from what it actually reported.

Every string in this file was read off a running container rather than a
changelog, and is quoted here as it came back. The live pairings are in
`test_variants_live.py`; this one pins the reading of the answers, including
the two that a version comparison gets wrong.
"""
from migkit import variants as v

# Measured. `docker exec ... INFO server`, trimmed to the fields that matter
# for identification - the full replies are in the live test.
REDIS_7 = {"redis_version": "7.4.11", "redis_mode": "standalone",
           "executable": "/data/redis-server", "atomicvar_api": "c11-builtin"}
VALKEY_8 = {"redis_version": "7.2.4", "server_name": "valkey",
            "valkey_version": "8.1.10", "valkey_release_stage": "ga",
            "server_mode": "standalone", "executable": "/data/valkey-server"}
KEYDB = {"redis_version": "6.3.4", "redis_mode": "standalone",
         "executable": "/data/keydb-server", "server_threads": 2,
         "mvcc_depth": 0, "features": "cluster_mget"}
DRAGONFLY = {"redis_version": "7.4.0", "dragonfly_version": "df-v2.0.0",
             "redis_mode": "standalone", "thread_count": 2,
             "executable": "dragonfly"}

PG_16 = {"version": "PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on"
                    " aarch64-unknown-linux-gnu, compiled by gcc (Debian"
                    " 14.2.0-19) 14.2.0, 64-bit",
         "server_version": "16.15 (Debian 16.15-1.pgdg13+2)"}
COCKROACH = {"version": "CockroachDB CCL v23.2.5 (aarch64-unknown-linux-gnu,"
                        " built 2024/05/04 00:06:07, go1.21.9"
                        " X:nocoverageredesign)",
             "server_version": "13.0.0"}

MYSQL_8 = {"version": "8.4.11", "version_comment": "MySQL Community Server"
                                                   " - GPL"}
MARIADB_11 = {"version": "11.8.9-MariaDB-ubu2404",
              "version_comment": "mariadb.org binary distribution"}


def test_each_redis_fork_is_named_from_what_only_it_says():
    assert v.identify("redis", REDIS_7).name == "redis"
    assert v.identify("redis", VALKEY_8).name == "valkey"
    assert v.identify("redis", KEYDB).name == "keydb"
    assert v.identify("redis", DRAGONFLY).name == "dragonfly"


def test_the_version_reported_is_the_forks_own_not_the_protocol_number():
    """The whole point. Valkey 8.1.10 answers `redis_version:7.2.4`, so a
    reader that takes that field gets a number describing compatibility with
    software this server is not."""
    assert v.identify("redis", VALKEY_8).version == "8.1.10"
    assert v.identify("redis", DRAGONFLY).version == "df-v2.0.0"
    assert v.identify("redis", REDIS_7).version == "7.4.11"


def test_keydb_has_no_version_of_its_own_and_that_is_recorded():
    """Measured: KeyDB's INFO carries `redis_version:6.3.4` and no field
    naming itself, so the release genuinely cannot be read. The brand is
    still identified - by `server_threads`, which stock Redis does not
    have - and the missing version is a recorded limit rather than a gap."""
    b = v.identify("redis", KEYDB)
    assert b.name == "keydb"
    assert b.version == "6.3.4"
    assert "compatibility number" in b.cannot(v.VERSION_IS_REAL)


def test_redis_is_not_named_by_the_absence_of_a_fork_field():
    """Detection is positive-only: a reply with no `redis_version` at all is
    an unknown server, not Redis by elimination."""
    b = v.identify("redis", {"executable": "/data/redis-server"})
    assert b.name == "unknown"
    assert not b.identified


def test_a_postgres_wire_protocol_fork_is_not_called_postgres():
    """CockroachDB's banner names it; its `server_version` says 13.0.0, which
    is the field every version comparison in migkit reads."""
    b = v.identify("postgres", COCKROACH)
    assert b.name == "cockroachdb"
    assert "v23.2.5" in b.version
    assert v.identify("postgres", PG_16).name == "postgres"


def test_cockroach_is_recorded_as_unable_to_carry_a_change_marker():
    """Measured against the container: `pg_stat_all_tables` is present and
    empty, and `pg_class.relfilenode` is 0 for every relation. A marker built
    from those never changes, which would skip every table's scan forever."""
    why = v.identify("postgres", COCKROACH).cannot(v.CHANGE_MARKER)
    assert "relfilenode" in why and "empty" in why


def test_mariadb_is_told_apart_from_mysql():
    assert v.identify("mysql", MARIADB_11).name == "mariadb"
    assert v.identify("mysql", MYSQL_8).name == "mysql"


def test_mariadb_carries_the_measured_gtid_finding():
    """`show variables like 'gtid_mode'` returns zero rows on MariaDB 11.8 -
    not OFF, nothing - and `select @@gtid_executed` raises ERROR 1193. An
    empty result read as "GTID is off" is the quiet direction."""
    why = v.identify("mysql", MARIADB_11).cannot(v.CDC_POSITION)
    assert "gtid_mode" in why and "zero rows" in why


def test_an_unrecognised_reply_is_unknown_rather_than_the_family():
    for fam, raw in (("postgres", {"version": "Totally New DB 1.0"}),
                     ("mysql", {"version": "x", "version_comment": "y"}),
                     ("redis", {})):
        b = v.identify(fam, raw)
        assert b.name == "unknown", (fam, b)
        assert not b.measured


def test_two_different_brands_are_called_out_even_when_the_numbers_agree():
    src = v.identify("redis", {"redis_version": "7.2.4"})
    dst = v.identify("redis", VALKEY_8)
    why = v.mismatch(src, dst)
    assert "redis 7.2.4" in why and "valkey 8.1.10" in why
    assert v.mismatch(src, src) == ""


def test_an_unidentified_side_does_not_manufacture_a_pairing_finding():
    """Saying two brands differ requires knowing both. One unreadable side is
    already reported as unknown; adding a mismatch on top would be inventing
    a fact out of an absence."""
    known = v.identify("redis", VALKEY_8)
    blank = v.identify("redis", {})
    assert v.mismatch(known, blank) == ""
    assert v.mismatch(blank, known) == ""


def test_an_identified_but_unmeasured_brand_is_a_warn_not_a_pass():
    """Knowing the name is worth reporting; claiming the limits are known is
    not. `mysql` and `redis` themselves are the measured baseline, so this
    uses a fork migkit has detection for and no findings against."""
    rows = v.rows(v.identify("mysql", {"version": "8.0.11-TiDB-v7.5.0"}),
                  v.identify("mysql", MYSQL_8))
    src = [r for r in rows if r["item"] == "source brand"][0]
    assert src["level"] == "warn"
    assert "not been measured" in src["detail"]
    assert "unknown rather than none" in src["detail"]


def test_the_rows_name_every_measured_limit_of_both_sides():
    rows = v.rows(v.identify("postgres", PG_16),
                  v.identify("postgres", COCKROACH))
    text = " ".join(r["item"] + r["detail"] for r in rows)
    assert "cockroachdb" in text
    assert "relfilenode" in text
    assert any(r["level"] == "warn" and "brand pairing" in r["item"]
               for r in rows), rows
