"""A column written other than as it was read, and proved as written.

A masked column used to be either compared (and every row called
different) or left out (and nothing proved of it). Each transform is one
function both readers take: the copy writes it, the check asks it of the
source's value and compares with the target's. Its SQL forms are held to
the Python one on the same values - on paper here, on the servers below.
"""
import hashlib

import pytest

from migkit import transforms
from tests import servers
from tests.conftest import needs_docker

KEY = "k3y-for-tests"
VALUES = ["alice@example.com", "", "ข้อมูลไทย", "O'Brien", None]


def test_each_transform_writes_what_it_says():
    assert transforms.apply("null", "x") is None
    assert transforms.apply("constant masked@example.com", "x") == \
        "masked@example.com"
    assert transforms.apply("constant masked@example.com", None) == \
        "masked@example.com"
    assert transforms.apply("truncate 3", "abcdef") == "abc"
    assert transforms.apply("truncate 3", None) is None
    want = hashlib.sha256((KEY + "alice").encode()).hexdigest()
    assert transforms.apply("hash", "alice", KEY) == want
    # the same value, the same digest: a join on it still lines up
    assert transforms.apply("hash", "alice", KEY) == \
        transforms.apply("hash", "alice", KEY)
    assert transforms.apply("hash", None, KEY) is None


def test_what_cannot_be_proved_or_is_said_wrong_is_refused():
    for rule in ("random", "shuffle", "constant", "constant it's",
                 "truncate", "truncate 0", "truncate x", "hash sha1",
                 "null now"):
        with pytest.raises(SystemExit):
            transforms.parse(rule)
    with pytest.raises(SystemExit, match="mask_key"):
        transforms.apply("hash", "alice", None)


def test_the_sql_forms_say_the_same_function():
    assert transforms.sql("null", '"c"', "postgres") == "NULL"
    assert transforms.sql("constant x", '"c"', "mysql") == "'x'"
    assert transforms.sql("truncate 4", "`c`", "mysql") == "LEFT(`c`, 4)"
    assert "sha256" in transforms.sql("hash", '"c"', "postgres", KEY)
    # a key is never left out of a digest, and an engine whose SQL cannot
    # take it alike leaves it to Python
    assert transforms.sql("hash", '"c"', "postgres") is None
    assert transforms.sql("hash", "[c]", "mssql", KEY) is None
    assert transforms.text_only("hash") and not transforms.text_only("null")


pg = servers.fixture("pg")
my = servers.fixture("mysql")


@needs_docker
@pytest.mark.parametrize("rule", ["hash", "truncate 4", "null",
                                  "constant masked"])
def test_postgres_computes_what_python_computes(pg, rule):
    for v in VALUES:
        lit = "null" if v is None else "'" + v.replace("'", "''") + "'"
        got = pg.src.sql(f"select {transforms.sql(rule, f'({lit}::text)', 'postgres', KEY)}",
                         pg.name)[0][0]
        assert got == transforms.apply(rule, v, KEY), (rule, v)


@needs_docker
@pytest.mark.parametrize("rule", ["hash", "truncate 4", "null",
                                  "constant masked"])
def test_mysql_computes_what_python_computes(my, rule):
    for v in VALUES:
        lit = "null" if v is None else "'" + v.replace("'", "''") + "'"
        got = my.src.sql(f"select {transforms.sql(rule, f'CAST({lit} AS CHAR CHARACTER SET utf8mb4)', 'mysql', KEY)}",
                         my.name)[0][0]
        assert got == transforms.apply(rule, v, KEY), (rule, v)
