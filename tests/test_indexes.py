"""Moving indexes out of the way of a bulk load.

The speed is the easy part. What these defend is the window between the drop
and the rebuild, where the table has no indexes and a unique one that was
enforcing something is simply gone.
"""
import json

from migkit import indexes as ix


def test_constraints_are_never_dropped():
    """A primary key or unique constraint enforces something. Dropping it
    changes what the database accepts, and a foreign key elsewhere may depend
    on it."""
    rows = [("t_pkey", "CREATE UNIQUE INDEX t_pkey ON t (id)", True),
            ("t_uq", "CREATE UNIQUE INDEX t_uq ON t (email)", True),
            ("ix_a", "CREATE INDEX ix_a ON t (a)", False)]
    drop, ddl = ix.plan(rows)
    assert drop == ["ix_a"]
    assert list(ddl) == ["ix_a"]


def test_an_index_with_no_readable_definition_is_left_alone():
    """Dropping something that cannot be recreated is the one mistake this
    must never make."""
    drop, ddl = ix.plan([("ix_a", None, False), ("ix_b", "", False),
                         (None, "CREATE INDEX ...", False)])
    assert drop == [] and ddl == {}


def test_saving_confirms_the_file_reads_back(tmp_path):
    """A caller that gets False must not drop anything, so this reports on
    the file being on disk rather than on the write having been attempted."""
    p = tmp_path / "indexes.json"
    ddl = {"ix_a": "CREATE INDEX ix_a ON t (a)"}
    assert ix.saved(p, ddl)
    assert json.loads(p.read_text()) == ddl


def test_saving_nothing_is_success_and_writes_no_file(tmp_path):
    p = tmp_path / "indexes.json"
    assert ix.saved(p, {})
    assert not p.exists()


def test_saving_into_an_unwritable_place_reports_failure(tmp_path):
    bad = tmp_path / "afile"
    bad.write_text("x")
    assert not ix.saved(bad / "nested" / "indexes.json",
                        {"ix_a": "CREATE INDEX ix_a ON t (a)"})


def test_definitions_survive_for_a_later_run(tmp_path):
    """The file is the point: a different process, an hour later, a person."""
    p = tmp_path / "indexes.json"
    ddl = {"ix_a": "CREATE INDEX ix_a ON t (a)"}
    ix.saved(p, ddl)
    assert ix.restore_from(p) == ddl


def test_a_missing_or_broken_file_reads_as_nothing(tmp_path):
    assert ix.restore_from(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json")
    assert ix.restore_from(bad) == {}


def test_the_summary_shouts_about_an_index_that_did_not_come_back():
    s = ix.summary(["a", "b"], ["a"], ["b"])
    assert "STILL MISSING: b" in s
    assert "recreate them before the target is used" in s


def test_a_clean_round_trip_says_so_quietly():
    s = ix.summary(["a", "b"], ["a", "b"], [])
    assert "STILL MISSING" not in s
    assert "2 indexes" in s


def test_nothing_to_do_is_said_plainly():
    assert ix.summary([], [], []) == \
        "no secondary indexes to move out of the way"
