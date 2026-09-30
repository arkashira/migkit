"""`canon`'s hot paths made faster give the same answers, and what it
builds is sized by the server where the server was asked.

* `sql_rows` chooses a writer once a column: a `str`, `bytes` or number
  goes through untouched where the class has no form of its own, and every
  other value still through `write_value` (2.12 -> 0.15 us a row of eight
  columns, measured 2026-10-01).
* `digest_step` reads its number off the digest's bytes, `uuid_text`
  matches a precompiled form: the same numbers and texts.
* `limits`: a MariaDB or 5.7 server has `utf8mb4_bin`, not
  `utf8mb4_0900_bin`; a key's text is sized by the server's key bytes; an
  Oracle with MAX_STRING_SIZE=EXTENDED takes 32,767.
* SQL Server's lengths and precisions are capacities, so a narrower column
  is named with why; PostgreSQL's `citext` is text.
"""
import datetime
import hashlib
import uuid
from decimal import Decimal

import pytest

from migkit import canon, verdict

VALUES = ["abc", b"\x00\xff", 7, 1.5, True, Decimal("1.50"), None,
          {"k": [1, "x"]}, [1, 2], memoryview(b"ab"), bytearray(b"cd"),
          datetime.datetime(2024, 1, 1, 0, 0, 0, 123456),
          datetime.time(1, 2, 3, 400000), uuid.UUID(int=5)]


@pytest.mark.parametrize("engine", ["mysql", "postgres", "mssql", "oracle",
                                    "cassandra", "clickhouse", "mongodb"])
@pytest.mark.parametrize("cls", ["text", "bytes", "json", "uuid",
                                 canon.BSON, canon.INTERVAL, "inet"])
def test_a_writer_chosen_once_writes_what_write_value_writes(engine, cls):
    """Every value, through the writer `sql_rows` chose for the column,
    comes out as `write_value` makes it - the same object where it hands
    it on, an equal one where it makes one."""
    classes = [cls, "integer"]
    rows = [(v, 1) for v in VALUES]
    try:
        want = [[canon.write_value(engine, cls, v), 1] for v, _ in rows]
    except Exception as e:  # noqa: BLE001 - both must refuse alike
        with pytest.raises(type(e)):
            canon.sql_rows(classes, rows, engine=engine)
        return
    got = canon.sql_rows(classes, rows, engine=engine)
    for (v, _), g, w in zip(rows, got, want):
        assert type(g[0]) is type(w[0]) and g[0] == w[0], (v, g, w)


def test_a_value_that_needs_its_form_still_gets_it():
    rows = [(datetime.datetime(2024, 1, 1),)]
    # a class every value of which reaches a writer as it is: the rows
    # themselves, untouched
    assert canon.sql_rows(["timestamp"], rows, engine="mysql") is rows
    doc = {"a": 1}
    (row,) = canon.sql_rows(["json"], [(doc,)], engine="mysql")
    assert row[0] == canon.json_write(doc) and isinstance(row[0], str)


@pytest.mark.parametrize("text", ["", "abc|def|1.50", None, "ü" * 300, 7])
def test_the_digest_step_is_the_same_number(text):
    h = hashlib.md5(("" if text is None else str(text)).encode(),
                    usedforsecurity=False).hexdigest()
    assert canon.digest_step(11, text) == 11 + int(h[:canon.DIGEST_HEX], 16)


def test_a_uuid_text_is_the_same_text():
    u = uuid.UUID("b632ab33-a6a0-4056-adcb-a654618533f4")
    assert canon.uuid_text(u) == str(u)
    assert canon.uuid_text(str(u).upper()) == str(u)
    for other in ("not a uuid", "B632AB33A6A04056ADCBA654618533F4",
                  str(u).upper() + "x", "Z" * 36):
        assert canon.uuid_text(other) == other


def test_limits_follow_the_server():
    assert canon.limits("mysql", "8.4.2").binary_collation == \
        "utf8mb4_0900_bin"
    assert canon.limits("mysql", "10.11.6-MariaDB").binary_collation == \
        "utf8mb4_bin"
    assert canon.limits("mysql", "5.7.44-log").binary_collation == \
        "utf8mb4_bin"
    assert canon.limits("postgres").identifier == 63
    compact = canon.limits("mysql", key_bytes=767)
    assert canon.ddl_type("mysql", "text", (), True, compact) == \
        "varchar(191)"
    # what nobody asked stays what it was
    assert canon.ddl_type("mysql", "text", (), True) == "varchar(768)"
    assert canon.ddl_type("oracle", "text", (), False, canon.limits(
        "oracle", max_string=32767)) == "VARCHAR2(32767 CHAR)"
    assert canon.ddl_type("oracle", "text") == "VARCHAR2(4000 CHAR)"


def test_sql_server_lengths_are_capacities_and_citext_is_text():
    assert verdict.narrowing("mssql", "nvarchar(10)", "nvarchar(5)") == \
        "nvarchar(10) -> nvarchar(5) (longer values are cut)"
    assert verdict.narrowing("mssql", "nvarchar(max)", "nvarchar(50)")
    assert verdict.narrowing("mssql", "decimal(12,4)", "decimal(12,2)") == \
        "decimal(12,4) -> decimal(12,2) (scale 4 -> 2: rounds)"
    assert verdict.narrowing("mssql", "nvarchar(5)", "nvarchar(10)") is None
    assert canon.type_class("postgres", "citext") == "text"
