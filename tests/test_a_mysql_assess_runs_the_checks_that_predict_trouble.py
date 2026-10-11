"""MySQL's assessment runs the deep checks that predict what the move will
do, before it moves anything (P0-PARITY.all).

migkit could already say, with `check --deep`, that a target column was too
narrow for the rows about to be sent and that a `timestamp` was landing in a
`datetime`. That is the command run afterwards. `assess` now asks the same
checks - one implementation, read in two places - and says them as rows
"before the move", each with its fix. The checks that compare what is on
the target (duplicate keys, counts) stay out: the target is empty before a
move and they would fail every pre-flight.

Pure: the catalogue reads are answered in-process.
"""
from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.preflight", "mysql")]

COLUMNS = {
    "src": [("note", "varchar(255)"), ("at", "timestamp")],
    "dst": [("note", "varchar(50)"), ("at", "datetime")],
}


def _engine(tmp_path, columns=None):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint(),
              databases=["appdb"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    cols = columns or COLUMNS

    def ask(side, sql, args=None, fresh=False):
        if "version()" in sql:
            return [("8.0.36",)]
        if sql.strip() == "select 1":
            return [(1,)]
        if sql.startswith("select count(*)") and "char_length" in sql:
            return [(3, 120)]
        if "mysql.time_zone_name" in sql:
            return []
        return []
    eng._q = ask
    eng.neutral_tables = lambda side, db: ["people"]
    eng.neutral_columns = lambda side, db, table: cols[side]
    eng._mojibake = lambda db: _ok(db, "mojibake")
    # the writers beside migkit are not what this asks, and the zone rules
    # are the same on both sides
    eng.stream_writers = lambda db: []
    eng._zone_fingerprints = lambda side, db: {"UTC": "a", "Asia/Tokyo": "b",
                                               "America/New_York": "c"}
    return eng


def _ok(db, name):
    from migkit.engines.base import Result
    return Result("deep", f"{db} {name}", "ok", "nothing found")


def _before_the_move(items):
    return [i for i in items if i["scope"] == "before the move"
            and i["item"] != "mapping"]


def test_it_names_what_will_stop_the_move_and_how_to_put_it_right(tmp_path):
    rows = _before_the_move(_engine(tmp_path).assess())
    failed = {i["item"]: i for i in rows if i["level"] == "fail"}
    assert "appdb target capacity" in failed, rows
    assert "appdb temporal meaning" in failed, rows
    assert "no room for" in failed["appdb target capacity"]["detail"]
    assert "[fix]" in failed["appdb target capacity"]["detail"]


def test_a_pair_that_agrees_adds_no_failures(tmp_path):
    """The cry-wolf guard: a pre-flight that fails on a healthy pair is one
    people learn to skip."""
    same = {"src": COLUMNS["src"], "dst": COLUMNS["src"]}
    rows = _before_the_move(_engine(tmp_path, same).assess())
    assert rows, "the pre-flight section is still there"
    assert [i for i in rows if i["level"] == "fail"] == [], rows


def test_the_checks_that_compare_the_target_stay_out():
    from migkit.engines.mysql import MySQLEngine
    assert set(MySQLEngine.PREFLIGHT) == {
        "_capacity_gaps", "_temporal_meaning", "_time_zone_rules",
        "_mojibake"}
    for name in MySQLEngine.PREFLIGHT:
        assert hasattr(MySQLEngine, name), name
    # what is on the target, and what only says MySQL cannot have it
    for name in ("_duplicate_keys", "_collation_versions"):
        assert name not in MySQLEngine.PREFLIGHT, name
