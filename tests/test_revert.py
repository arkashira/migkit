"""Generating the way back, and being honest about its limits.

The rule being defended is one-sided: a revert may claim less than it can do,
never more. Calling a reversible statement irreversible costs the reader a
second look; the opposite tells someone their rollback is complete when their
rows are gone.
"""
from migkit import revert


def test_a_dropped_column_is_named_as_unrecoverable():
    out = revert.irreversible("ALTER TABLE t DROP COLUMN c;")
    assert len(out) == 1
    assert "values are gone" in out[0][1]


def test_dropping_and_truncating_are_both_caught():
    sql = ("DROP TABLE t;\n"
           "TRUNCATE u;\n"
           "DROP MATERIALIZED VIEW mv;\n"
           "DROP SCHEMA s;\n")
    assert len(revert.irreversible(sql)) == 4


def test_a_type_change_is_treated_as_lossy_without_asking_which_way():
    """Widening is safe and narrowing is not, and the script does not say
    which it is. Erring towards lossy is the direction that cannot hurt."""
    out = revert.irreversible("ALTER TABLE t ALTER COLUMN c TYPE varchar(10);")
    assert len(out) == 1
    assert "truncates" in out[0][1]
    out = revert.irreversible("ALTER TABLE t MODIFY COLUMN c varchar(10);")
    assert len(out) == 1


def test_additive_statements_are_not_called_irreversible():
    sql = ("CREATE INDEX i ON t (a);\n"
           "ALTER TABLE t ADD COLUMN c int;\n"
           "GRANT SELECT ON t TO app;\n"
           "COMMENT ON TABLE t IS 'x';\n")
    assert revert.irreversible(sql) == []


def test_an_unknown_drop_still_counts_as_irreversible():
    """A DROP this file has never seen is a reason to warn, not to relax."""
    out = revert.irreversible("DROP PUBLICATION p;")
    assert len(out) == 1


def test_a_purely_additive_forward_script_says_the_undo_is_complete():
    h = revert.header("ALTER TABLE t ADD COLUMN c int;", "structural-fix.sql")
    assert "returns the target to where it started" in h
    assert "cannot be" not in h


def test_a_destructive_forward_script_names_every_statement_it_cannot_undo():
    fwd = "ALTER TABLE t DROP COLUMN c;\nDROP TABLE u;\n"
    h = revert.header(fwd, "structural-fix.sql")
    assert "2 statement(s)" in h
    assert "DROP COLUMN c" in h and "DROP TABLE u" in h
    assert "needs a backup, not this file" in h
    # and it says when to take it, because after is too late
    assert "before you apply" in h.replace("\n-- ", " ")


def test_no_reverse_sql_means_no_file_and_no_claim():
    assert revert.script("DROP TABLE t;", "", "f.sql") == ""
    assert revert.script("DROP TABLE t;", "   \n", "f.sql") == ""
    assert revert.summary("DROP TABLE t;", "") == ""


def test_the_script_is_the_header_followed_by_the_sql():
    out = revert.script("ALTER TABLE t ADD COLUMN c int;",
                        "ALTER TABLE t DROP COLUMN c;\n",
                        "structural-fix.sql")
    assert out.startswith("-- Undo for structural-fix.sql.")
    assert out.rstrip().endswith("ALTER TABLE t DROP COLUMN c;")


def test_the_summary_warns_only_when_data_is_at_stake():
    plain = revert.summary("ALTER TABLE t ADD COLUMN c int;",
                           "ALTER TABLE t DROP COLUMN c;")
    assert "undo was written" in plain and "backup" not in plain
    lossy = revert.summary("ALTER TABLE t DROP COLUMN c;",
                           "ALTER TABLE t ADD COLUMN c int;")
    assert "take a backup first" in lossy
    assert "1 of the forward statements destroy data" in lossy


def test_statement_splitting_is_shared_with_the_lock_report():
    """Two splitters would eventually disagree about how many statements a
    script has, and the two reports sit next to each other in one directory."""
    from migkit.locks import split
    assert revert.split is split
