"""Keys distinct on the source that the target's collation takes for one
key stop the move, named, before a row is written (type-fidelity G8).

MySQL 8's default collation, `utf8mb4_0900_ai_ci`, compares `a` and `A` as
one, and `é` written as one code point and as `e` with a combining accent
as one; the older PAD SPACE ones (`utf8mb4_unicode_ci`) take `a` and `a `
for one as well. The copier writes MySQL with `insert ... on duplicate key
update`, so the second of two such keys overwrites the first. Measured
before, into a table made beforehand: the batch was written, its read-back
found two rows missing and two different, and the copy stopped there with
the overwritten rows on the target - and without the read-back (a hop's
`verify_batches: false`) nothing looked at all. A table the copier built
for them could not be made: a text key built as `varchar(1024)` is past
InnoDB's 3,072-byte key.
"""
import pytest

from tests.typepair import (engine, fresh, move, my, pg, servers,  # noqa
                            verdict)

pytestmark = [pytest.mark.docker]

KEYS = ("a", "A", "a ", "é", "é", "b")


def _seed():
    pg("create table k (pk text primary key, v int); insert into k values "
       + ", ".join(f"('{k}', {i})" for i, k in enumerate(KEYS)))
    assert pg("select count(distinct pk) from k") == str(len(KEYS))


def test_keys_the_default_collation_merges_are_refused(fresh, tmp_path):
    _seed()
    assert my("select default_collation_name from information_schema"
              ".schemata where schema_name = 'cx'") == "utf8mb4_0900_ai_ci"
    with pytest.raises(SystemExit) as got:
        move(engine(tmp_path), "k", "public")
    said = " ".join(str(got.value).split())
    assert "k.pk: 2 groups of keys distinct here that mysql's" \
        " utf8mb4_0900_ai_ci takes for one key" in said, said
    groups = said.split(" - ", 1)[1]
    assert "'A'" in groups and "'a'" in groups, said
    assert repr(KEYS[3]) in groups and repr(KEYS[4]) in groups, said
    # NO PAD: a trailing space is a key of its own there
    assert "'a '" not in said, said
    assert my("select count(*) from information_schema.tables where"
              " table_schema = 'cx' and table_name = 'k'") == "0"


def test_a_table_made_before_is_asked_by_its_own_collation(fresh, tmp_path):
    _seed()
    my("create table k (pk varchar(10) primary key, v int)")
    with pytest.raises(SystemExit) as got:
        move(engine(tmp_path), "k", "public")
    said = " ".join(str(got.value).split())
    assert "k.pk: 2 groups of keys distinct here that mysql's" \
        " utf8mb4_0900_ai_ci takes for one key" in said, said
    assert my("select count(*) from k") == "0"


def test_a_pad_space_collation_merges_trailing_spaces_too(fresh, tmp_path):
    _seed()
    my("create database cy character set utf8mb4 collate"
       " utf8mb4_unicode_ci", db=None)
    try:
        eng = engine(tmp_path)
        eng.hop.db_map = {"cx": "cy"}
        with pytest.raises(SystemExit) as got:
            move(eng, "k", "public")
        said = " ".join(str(got.value).split())
        assert "2 groups" in said and "utf8mb4_unicode_ci" in said, said
        assert "'a '" in said, said
    finally:
        my("drop database cy", db=None)


def test_a_target_column_the_keys_fit_is_moved(fresh, tmp_path):
    """The same keys into a column compared by its bytes: nothing merges,
    nothing is refused, and every row arrives."""
    _seed()
    my("create table k (pk varchar(10) collate utf8mb4_0900_bin"
       " primary key, v int)")
    eng = engine(tmp_path)
    move(eng, "k", "public")
    assert my("select count(*) from k") == str(len(KEYS))
    assert verdict(eng, "k").status == "ok"


def test_keys_that_stay_apart_are_not_refused(fresh, tmp_path):
    pg("create table k (pk text primary key, v int);"
       " insert into k values ('a', 1), ('b', 2), ('c', 3)")
    eng = engine(tmp_path)
    move(eng, "k", "public")
    assert my("select count(*) from k") == "3"
    assert verdict(eng, "k").status == "ok"
