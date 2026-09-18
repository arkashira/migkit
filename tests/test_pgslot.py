"""Parsing what `test_decoding` prints, against lines it actually printed.

Every string below was read off a PostgreSQL 16 slot rather than written from
the format documentation. The three that matter are the ones a split-on-spaces
parser gets wrong: a quoted value carrying the separator, a `''` escape, and
the difference between the token `null` and the string `'null'`.
"""
import pytest

from migkit import canon, pgslot

# --- measured output ------------------------------------------------------

INSERT = ("table public.t: INSERT: id[bigint]:1"
          " name[character varying]:'ca''fé' amount[numeric]:-0.0500"
          " b[bytea]:'\\x00ff41' doc[jsonb]:'{\"a\": 2, \"b\": 1}'"
          " made[timestamp without time zone]:'2026-01-01 00:00:00.123456'"
          " flag[boolean]:true")
INSERT_NULLS = ("table public.t: INSERT: id[bigint]:2"
                " name[character varying]:null amount[numeric]:null"
                " b[bytea]:null doc[jsonb]:null"
                " made[timestamp without time zone]:null flag[boolean]:null")
NASTY = ("table public.t: INSERT: id[bigint]:9"
         " name[character varying]:'has '' quote and : colon and [bracket]'"
         " amount[numeric]:1.0000 b[bytea]:null doc[jsonb]:null"
         " made[timestamp without time zone]:null flag[boolean]:false")
UPDATE_PLAIN = ("table public.t: UPDATE: id[bigint]:3"
                " name[character varying]:'plain' amount[numeric]:-0.0500"
                " b[bytea]:'\\x00ff41' flag[boolean]:true")
UPDATE_MOVED = ("table public.t: UPDATE: old-key: id[bigint]:1"
                " new-tuple: id[bigint]:3 name[character varying]:'moved'"
                " amount[numeric]:-0.0500 flag[boolean]:true")
UPDATE_FULL = ("table public.t: UPDATE: old-key: id[bigint]:3"
               " name[character varying]:'plain' amount[numeric]:-0.0500"
               " new-tuple: id[bigint]:3 name[character varying]:'full-id'"
               " amount[numeric]:-0.0500")
DELETE = "table public.t: DELETE: id[bigint]:2"
DELETE_FULL = ("table public.t: DELETE: id[bigint]:9"
               " name[character varying]:'has '' quote and : colon and"
               " [bracket]' amount[numeric]:1.0000 flag[boolean]:false")


def _fields(line, side="new"):
    return {n: (t, v, q) for n, t, v, q in pgslot.parse_line(line)[side]}


def test_transaction_boundaries_are_not_changes():
    """`BEGIN` and `COMMIT` are real output, not errors, and carry nothing to
    apply."""
    for line in ("BEGIN 733", "COMMIT 733", "", "   "):
        assert pgslot.parse_line(line) is None


def test_a_quoted_value_may_contain_the_separator_and_the_brackets():
    """The line this test exists for. A split on spaces, on `:` or on `[`
    tears this value into pieces that each look like a column."""
    got = _fields(NASTY)
    assert got["name"][1] == "has ' quote and : colon and [bracket]"
    assert got["name"][2] is True          # it was quoted
    assert got["amount"][1] == "1.0000"
    assert got["flag"][1] == "false"
    assert len(got) == 7, sorted(got)


def test_a_doubled_apostrophe_is_one_apostrophe():
    assert _fields(INSERT)["name"][1] == "ca'fé"


def test_the_token_null_and_the_string_null_are_different_things():
    """The only thing separating them is the quoting, so the quoting is
    carried out of the parser rather than thrown away."""
    assert _fields(INSERT_NULLS)["name"] == ("character varying", "null",
                                             False)
    assert pgslot.value("character varying", "null", quoted=False) is None
    assert pgslot.value("character varying", "null", quoted=True) == "null"


def test_values_come_back_as_things_a_driver_can_take():
    """Text passed straight through would put `\\x00ff41` into a binary
    column as nine characters - the memoryview bug arriving from the other
    direction."""
    fields = pgslot.parse_line(INSERT)["new"]
    got = {n: pgslot.value(t, v, q) for n, t, v, q in fields}
    assert got["id"] == 1
    assert got["b"] == b"\x00\xffA"
    assert str(got["amount"]) == "-0.0500"
    assert got["flag"] is True
    assert got["name"] == "ca'fé"


def test_an_update_that_did_not_move_the_key_has_only_a_new_tuple():
    parsed = pgslot.parse_line(UPDATE_PLAIN)
    assert parsed["op"] == "update"
    assert parsed["old"] == []
    assert [n for n, _, _, _ in parsed["new"]][0] == "id"


def test_an_update_that_moved_the_key_carries_both_tuples():
    parsed = pgslot.parse_line(UPDATE_MOVED)
    old = {n: v for n, _, v, _ in parsed["old"]}
    new = {n: v for n, _, v, _ in parsed["new"]}
    assert old == {"id": "1"}
    assert new["id"] == "3" and new["name"] == "moved"


def test_replica_identity_full_puts_the_whole_old_row_in_old_key():
    parsed = pgslot.parse_line(UPDATE_FULL)
    old = {n: v for n, _, v, _ in parsed["old"]}
    new = {n: v for n, _, v, _ in parsed["new"]}
    assert old["name"] == "plain" and new["name"] == "full-id"
    assert old["id"] == new["id"] == "3"


def test_a_delete_reads_its_columns_as_the_old_tuple():
    parsed = pgslot.parse_line(DELETE)
    assert parsed["op"] == "delete"
    assert [n for n, _, _, _ in parsed["old"]] == ["id"]
    assert parsed["new"] == []


def test_a_change_record_takes_its_key_from_the_catalogue():
    """Not from the line: an UPDATE that did not move the key has no old
    tuple at all, so there is nothing there to read a key out of."""
    rec = pgslot.change(pgslot.parse_line(UPDATE_PLAIN), ["id"])
    assert rec["op"] == "update"
    assert rec["key"] == {"id": 3}
    assert rec["values"]["name"] == "plain"

    moved = pgslot.change(pgslot.parse_line(UPDATE_MOVED), ["id"])
    assert moved["key"] == {"id": 1}, "the key is the one being left"
    assert moved["values"]["id"] == 3, "and the values carry the new one"


def test_a_delete_under_full_identity_drops_its_null_columns():
    """Measured: the old tuple of a DELETE omits the columns that were NULL.
    A parser that read a missing column as "no such column" would build the
    key out of whatever happened to be non-null."""
    parsed = pgslot.parse_line(DELETE_FULL)
    names = [n for n, _, _, _ in parsed["old"]]
    assert names == ["id", "name", "amount", "flag"]
    assert "b" not in names and "doc" not in names
    rec = pgslot.change(parsed, ["id"])
    assert rec == {"op": "delete", "table": "public.t", "key": {"id": 9},
                   "values": {}}


def test_a_line_whose_key_is_not_in_it_is_refused_with_the_fix():
    """Nothing to guess from, so nothing is guessed - and the message says
    which setting would have carried it."""
    with pytest.raises(ValueError) as e:
        pgslot.change(pgslot.parse_line(DELETE), ["id", "tenant"])
    assert "did not carry tenant" in str(e.value)
    assert "REPLICA IDENTITY" in str(e.value)


def test_a_malformed_line_raises_rather_than_returning_half_a_row():
    for bad in ("table public.t: INSERT: id:1",
                "table public.t: INSERT: id[bigint]",
                "table public.t: MERGE: id[bigint]:1",
                "table public.t INSERT: id[bigint]:1",
                "table public.t: INSERT: name[text]:'unterminated"):
        with pytest.raises(ValueError):
            pgslot.parse_line(bad)


def test_every_parsed_record_is_a_valid_change_record():
    """`canon.change` is what rejects an op or an empty key, so going through
    it is what keeps a parse bug from becoming a write."""
    for line, keys in ((INSERT, ["id"]), (NASTY, ["id"]),
                       (UPDATE_MOVED, ["id"]), (DELETE, ["id"])):
        rec = pgslot.change(pgslot.parse_line(line), keys)
        assert rec["op"] in canon.CHANGE_OPS
        assert rec["key"] and rec["table"] == "public.t"
