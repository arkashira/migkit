"""A Redis finding names its table - a database's keys, `db0` as the copy
names them - counts the keys apart, kind by kind, and says how the table
came out (P0-PARITY.verify). Run against a real server, started here."""
from migkit import verdict
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "redis"), ("verify.tables", "redis"),
          ("verify.table", "redis")]

pytestmark = needs_server


def _apart(pair):
    src, dst = (p.client() for p in pair)
    for k in "abcd":
        src.set(k, k)
    # a and d the same, b changed, c missing, x on the target alone
    dst.set("a", "a")
    dst.set("b", "other")
    dst.set("d", "d")
    dst.set("x", "x")


def test_the_keys_apart_are_counted_and_the_table_named(clean, tmp_path):
    _apart(clean)
    eng = engine(clean, tmp_path, options={"deep": True})
    found = {r.scope: r for r in eng.check_data("0") if r.status == "diff"}
    main, extra = found["db0"], found["db0 extra keys"]
    assert main.table == extra.table == "db0"
    assert main.rows_apart == {"db0": {"missing": 1, "extra": 0,
                                       "changed": 1, "exact": True}}
    assert extra.rows_apart == {"db0": {"missing": 0, "extra": 1,
                                        "changed": 0, "exact": True}}
    assert main.tables == extra.tables == {"db0": "diff"}


def test_a_sample_counts_the_least_there_are(clean, tmp_path):
    _apart(clean)
    got = engine(clean, tmp_path).check_data("0")
    assert not any(r.rows_apart["db0"]["exact"] for r in got
                   if r.status == "diff"), got


def test_a_table_that_matches_says_so(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    src.set("k", "v")
    dst.set("k", "v")
    eng = engine(clean, tmp_path)
    for r in eng.check_data("0") + eng.check_counts("0"):
        assert r.status == "ok" and r.table == "db0"
        assert r.tables == {"db0": "ok"}


def test_the_counts_name_the_table_too(clean, tmp_path):
    src, _ = (p.client() for p in clean)
    src.set("k", "v")
    got = engine(clean, tmp_path).check_counts("0")[0]
    assert (got.status, got.table, got.tables) == ("diff", "db0",
                                                   {"db0": "diff"})


def test_a_hop_that_keeps_deletes_holds_the_extra_keys_as_kept(
        clean, tmp_path):
    """The counts are what the verdict holds a hop's carried changes to:
    a key the source deleted and the target keeps is not a difference."""
    src, dst = (p.client() for p in clean)
    src.set("k", "v")
    dst.set("k", "v")
    dst.set("deleted-on-the-source", "v")
    eng = engine(clean, tmp_path, options={"deep": True},
                 mapping={"ops": {"db0": "insert, update"}})
    rows = [r.__dict__ for r in eng.check_data("0")]
    assert verdict.decide(rows, eng.hop).status == "same", rows
    assert rows[0]["kept_by_hop"].startswith("kept by the hop: 1 row"), rows
