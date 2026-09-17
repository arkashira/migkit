"""Classifying what repair DDL will lock.

The classification is static, so most of it is testable without a server. The
part that matters - that the static verdict agrees with what PostgreSQL
actually takes - is verified against a live server in
`test_locks_live_pg.py`; an invented lock table would be worse than none.
"""
from migkit.locks import SEVERITY, classify, report, split, summary


def _mode(stmt):
    return classify(stmt)[0]


def test_concurrent_index_is_the_non_blocking_one():
    assert _mode("CREATE INDEX CONCURRENTLY i ON t (a);") == \
        "ShareUpdateExclusiveLock"
    assert _mode("CREATE INDEX i ON t (a);") == "ShareLock"
    # and the plain form is told how to become the safe one
    assert "CONCURRENTLY" in classify("CREATE INDEX i ON t (a);")[2]


def test_not_valid_is_cheap_and_validate_is_cheaper():
    assert _mode("ALTER TABLE t ADD CONSTRAINT c CHECK (n > 0) NOT VALID;") \
        == "AccessExclusiveLock"
    assert _mode("ALTER TABLE t VALIDATE CONSTRAINT c;") == \
        "ShareUpdateExclusiveLock"
    # the scanning form suggests the two-step
    _, _, safer = classify("ALTER TABLE t ADD CONSTRAINT c CHECK (n > 0);")
    assert "NOT VALID" in safer and "VALIDATE" in safer


def test_foreign_key_without_not_valid_is_flagged():
    mode, meaning, safer = classify(
        "ALTER TABLE a ADD CONSTRAINT fk FOREIGN KEY (b_id) REFERENCES b (id);")
    assert mode == "AccessExclusiveLock"
    assert "both tables" in meaning
    assert "NOT VALID" in safer


def test_set_not_null_names_the_postgres_12_shortcut():
    _, _, safer = classify("ALTER TABLE t ALTER COLUMN c SET NOT NULL;")
    assert "CHECK" in safer and "12" in safer


def test_a_type_change_is_named_as_a_rewrite():
    mode, meaning, _ = classify(
        "ALTER TABLE t ALTER COLUMN c TYPE bigint;")
    assert mode == "AccessExclusiveLock"
    assert "rewrites" in meaning


def test_grants_and_creates_do_not_touch_existing_rows():
    for stmt in ("GRANT SELECT ON t TO app;",
                 "CREATE TABLE t (id int);",
                 "COMMENT ON TABLE t IS 'x';"):
        assert SEVERITY[_mode(stmt)] < SEVERITY["ShareLock"], stmt


def test_unrecognised_ddl_is_assumed_to_block():
    """An unclassified statement is a reason to look, not to relax."""
    mode, meaning, _ = classify("CLUSTER t USING i;")
    assert mode == "AccessExclusiveLock"
    assert "not recognised" in meaning


def test_split_ignores_comments_and_blank_lines():
    sql = """
    -- a comment
    CREATE INDEX i ON t (a);

    ALTER TABLE t ADD COLUMN c int;
    """
    assert len(split(sql)) == 2


def test_report_counts_and_summarises():
    sql = ("CREATE INDEX CONCURRENTLY i ON t (a);\n"
           "CREATE INDEX j ON t (b);\n"
           "ALTER TABLE t ALTER COLUMN c TYPE bigint;\n"
           "GRANT SELECT ON t TO app;\n")
    text, counts = report(sql)
    assert counts == {"total": 4, "blocks_writes": 2, "blocks_everything": 1}
    assert "block reads and writes" in text
    assert "safer:" in text            # the plain CREATE INDEX got advice
    s = summary(counts)
    assert "1 of 4" in s and "block writes" in s


def test_an_all_safe_script_says_so_plainly():
    text, counts = report("GRANT SELECT ON t TO app;\n"
                          "CREATE INDEX CONCURRENTLY i ON t (a);\n")
    assert counts["blocks_writes"] == 0
    assert summary(counts) == "2 statements, none block concurrent access"
    assert "none block" in summary(counts)


def test_empty_script_has_nothing_to_say():
    assert summary(report("")[1]) == ""
