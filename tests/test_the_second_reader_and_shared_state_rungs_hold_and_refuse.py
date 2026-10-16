"""The rungs a table's second read and a hop's shared run state are
chosen by, each reached on its branches on fakes (backlog
AUD-Z.rungs-outside): a second reader walks only the groups that differ
where it is installed and a column splits the table, else the table's own
walk reads every row; the run state goes to a bucket, a directory every
machine sees, or the target, each only where the hop gives it - and the
reason a rung is passed over is that rung's own need."""
from migkit import decide, second_reader, shared
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("second_reader", "_walks", "groups", "holds"),
    ("second_reader", "_walks", "groups", "refuses"),
    ("second_reader", "_walks", "walk", "holds"),
    ("shared", "_rungs", "bucket", "holds"),
    ("shared", "_rungs", "bucket", "refuses"),
    ("shared", "_rungs", "directory", "holds"),
    ("shared", "_rungs", "directory", "refuses"),
    ("shared", "_rungs", "target", "holds"),
    ("shared", "_rungs", "target", "refuses"),
]


def _walk(facts):
    with watched() as seen:
        got = decide.climb("the second read", second_reader._walks(), facts)
    return got, seen


def test_the_groups_are_walked_where_the_reader_and_a_column_split_it():
    got, seen = _walk({"reader": True, "group column": "region",
                       "groups failed": None})
    assert got.rung.name == "groups"
    assert branch(seen, "groups") == [HOLDS]
    assert branch(seen, "walk") == [HOLDS]


def test_without_the_reader_the_table_is_walked_and_says_why():
    got, seen = _walk({"reader": False, "group column": "region"})
    assert got.rung.name == "walk"
    assert branch(seen, "groups") == [REFUSES]
    assert "needs the second reader" in why(seen, "groups")
    assert "needs the second reader" in got.reason


def test_without_a_splitting_column_the_table_is_walked():
    got, seen = _walk({"reader": True, "group column": None})
    assert got.rung.name == "walk"
    assert branch(seen, "groups") == [REFUSES]
    assert "statistics to split it" in why(seen, "groups")


def test_the_groups_that_fail_their_proof_fall_to_the_walk():
    got, seen = _walk({"reader": True, "group column": "region",
                       "groups failed": "the reader stopped"})
    assert branch(seen, "groups") == [HOLDS]
    assert got.rung.name == "walk" and "the reader stopped" in got.reason


GIVEN = {"backend": "s3", "shared": "/mnt/hop", "target_keeps": True,
         "target_engine": "postgres"}
NONE = {"backend": "local", "shared": None, "target_keeps": False,
        "target_engine": "redis"}


def test_every_run_state_place_the_hop_gives_holds():
    with watched() as seen:
        got = decide.climb("the run state", shared._rungs(), GIVEN)
    assert got.rung.name == "bucket"
    for name in ("bucket", "directory", "target"):
        assert branch(seen, name) == [HOLDS], name


def test_no_place_the_hop_gives_refuses_each_with_its_own_need():
    with watched() as seen:
        got = decide.climb("the run state", shared._rungs(), NONE)
    assert got.rung is None
    for name in ("bucket", "directory", "target"):
        assert branch(seen, name) == [REFUSES], name
    assert "not in a bucket" in why(seen, "bucket")
    assert "names no directory every machine sees" in why(seen, "directory")
    assert why(seen, "target") == ("the target keeps no table of migkit's"
                                   " (redis)")


def test_a_hop_on_one_machine_falls_from_the_bucket_to_its_directory():
    with watched() as seen:
        got = decide.climb("the run state", shared._rungs(),
                           dict(GIVEN, backend="local"))
    assert got.rung.name == "directory"
    assert branch(seen, "bucket") == [REFUSES]
    assert why(seen, "bucket") in got.reason
