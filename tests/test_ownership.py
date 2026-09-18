"""Grouping ownership and definer drift so a systematic change reads as one.

The whole value of this check is signal-to-noise: a mover that rewrites the
owner of every object produces hundreds of differences that are one decision.
Printing them one per line buries the single object that drifted on its own,
which is the one worth looking at.
"""
from migkit import ownership as own


def test_only_real_differences_are_kept():
    ch = own.group([("t1", "app", "app"), ("t2", "app", "migrator")])
    assert own.total(ch) == 1
    assert list(ch) == [("app", "migrator")]


def test_identical_everywhere_is_nothing_at_all():
    ch = own.group([("t1", "app", "app"), ("t2", "app", "app")])
    assert ch == {} and own.total(ch) == 0
    assert own.describe(ch) == ""
    assert not own.systematic(ch)


def test_one_substitution_across_many_objects_is_named_systematic():
    ch = own.group([(f"t{i}", "app", "migrator") for i in range(20)])
    assert own.systematic(ch)
    assert own.total(ch) == 20
    line = own.describe(ch)
    assert line.startswith("app -> migrator on 20:")
    assert "and 16 more" in line


def test_a_single_object_drifting_on_its_own_is_not_systematic():
    ch = own.group([("t1", "app", "migrator")])
    assert not own.systematic(ch)


def test_mixed_drift_is_not_systematic_and_lists_each_change():
    ch = own.group([("t1", "app", "migrator"),
                    ("t2", "app", "migrator"),
                    ("v1", "app", "postgres")])
    assert not own.systematic(ch)
    line = own.describe(ch)
    # the bigger group first, so the odd one out is still visible after it
    assert line.index("app -> migrator") < line.index("app -> postgres")
    assert "on 2:" in line and "on 1:" in line


def test_the_security_mode_travels_with_the_identity():
    """MySQL changes both at once, and reporting only the account would hide
    that the routine now runs with the caller's privileges."""
    ch = own.group([("v", "app@%/DEFINER", "migrator@%/INVOKER")])
    assert "app@%/DEFINER -> migrator@%/INVOKER" in own.describe(ch)


def test_names_are_sorted_so_two_runs_read_the_same():
    a = own.describe(own.group([("b", "x", "y"), ("a", "x", "y")]))
    b = own.describe(own.group([("a", "x", "y"), ("b", "x", "y")]))
    assert a == b
