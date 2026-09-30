"""Records read as IBM documents their bytes, and a value that is not one
refused (backlog 34, W5).

The corpus (`tests/copybook_corpus.py`) was written from IBM's documented
encodings, not from migkit: each value reads as the comment beside its
bytes says, and each must-fail value is refused naming its field. Every
record written back from what was read is the same bytes. Then the parts
around the values: the copybook's own syntax, LOW-VALUES, the record
formats a transferred dataset arrives in.
"""
import io
from decimal import Decimal
from fractions import Fraction

import pytest

from migkit import copybook as cb
from tests import copybook_corpus as C

CASES = {c[0]: c for c in C.CASES}


def _layout(case):
    _, text, cp, rules, _, _, _ = CASES[case]
    return cb.Layout(cb.parse(text)[0], cp, rules)


def _col(name, subs):
    return cb._column(name) + "".join(f"_{s}" for s in subs)


def _same(got, want):
    """Equal and of the same kind: a Decimal at the same scale, an int, a
    bool, text; a hexadecimal float as the exact fraction it holds."""
    if isinstance(want, Fraction):
        return got is not None and Fraction(got) == want
    if isinstance(want, bool) or want is None:
        return got is want
    if isinstance(want, Decimal):
        return isinstance(got, Decimal) and str(got) == str(want)
    return type(got) is type(want) and got == want


def _misread(case, raw, want):
    lay = _layout(case)
    values, bad = lay.decode(raw)
    got = dict(zip([c for c, _ in lay.columns], values))
    wrong = [(k, got.get(_col(*k), "absent"), v) for k, v in want.items()
             if not _same(got.get(_col(*k), "absent"), v)]
    return wrong, bad


@pytest.mark.parametrize("case", list(CASES))
def test_every_value_reads_as_ibm_documents_it(case):
    for raw, want in CASES[case][5]:
        wrong, bad = _misread(case, raw, want)
        assert not bad, bad
        assert not wrong, wrong


@pytest.mark.parametrize("case", list(CASES))
def test_a_value_that_is_not_one_is_refused_by_name(case):
    for raw, field in CASES[case][6]:
        _, bad = _layout(case).decode(raw)
        named = [b for b in bad if b.field in (cb._column(field), field)]
        assert named, (field, bad)
        assert named[0].why and str(named[0]).startswith(named[0].field)


def test_the_comparison_above_can_say_different():
    """One byte changed in a record is a value that no longer reads as
    expected: the corpus test is not passing for want of looking."""
    raw, want = C.ELEMENTARY_RECORDS[0]
    assert _misread("elementary", raw, want) == ([], [])
    changed = raw[:23] + b"\x99" + raw[24:]        # a digit of AMT
    wrong, bad = _misread("elementary", changed, want)
    assert [w[0] for w in wrong] == [("AMT", ())] and not bad, (wrong, bad)
    thai, twant = C.ACCOUNT_RECORDS[0]
    # the same bytes read as cp037 are not the Thai text
    lay = cb.Layout(cb.parse(C.ACCOUNT)[0], "cp037", C.ACCOUNT_RULE)
    got = dict(zip([c for c, _ in lay.columns], lay.decode(thai)[0]))
    assert got["owner_th"] != twant[("OWNER-TH", ())]


@pytest.mark.parametrize("case", list(CASES))
def test_what_is_read_writes_back_as_the_same_values(case):
    """The same bytes where the record carries IBM's preferred signs (C and
    D); a -0 or a sign of F or A writes back as the preferred sign of the
    same value."""
    lay = _layout(case)
    for n, (raw, _) in enumerate(CASES[case][5]):
        values, bad = lay.decode(raw)
        assert not bad
        again = lay.encode(values)
        assert lay.decode(again) == (values, [])
        if case != "elementary" or n == 0:
            assert again == raw
        else:
            assert again != raw


def test_the_columns_and_their_classes():
    lay = _layout("elementary")
    cols = dict(lay.columns)
    assert cols["bal"] == "zoned decimal(7,2)"
    assert cols["amt"] == "packed decimal(9,2)"
    assert cols["cnt"] == "binary integer(5)"
    assert cols["hundreds"] == "zoned integer(5)"
    assert cols["short_flt"] == "hex float"
    assert cols["long_flt"] == "long hex float"
    classes = dict(zip([c for c, _ in lay.columns], lay.classes))
    assert classes["long_flt"] == "decimal" and classes["short_flt"] == \
        "float" and classes["big"] == "integer" and classes["rate"] == \
        "decimal"
    acct = dict(_layout("account").columns)
    # the rule makes both alternatives columns, the base none
    assert "detail" not in acct and "tax_id" in acct and "birth_date" in acct
    assert acct["is_alnum"] == "condition" and "top_score_2" in acct
    assert "bal_amt_3" in acct and "bal_amt_4" not in acct


def test_without_a_rule_the_redefined_layout_is_read():
    lay = cb.Layout(cb.parse(C.ACCOUNT)[0], "cp838")
    cols = [c for c, _ in lay.columns]
    assert "detail" in cols and "tax_id" not in cols
    raw, _ = C.ACCOUNT_RECORDS[1]
    got = dict(zip(cols, lay.decode(raw)[0]))
    assert got["detail"].startswith("0105556789")


def test_low_values_are_null_and_null_is_low_values():
    lay = _layout("elementary")
    raw, _ = C.ELEMENTARY_RECORDS[0]
    blank = raw[:10] + b"\x00" * 5 + raw[15:22] + b"\x00" * 5 + raw[27:]
    values, bad = lay.decode(blank)
    got = dict(zip([c for c, _ in lay.columns], values))
    assert not bad and got["qty"] is None and got["amt"] is None
    assert lay.encode(values) == blank
    # a binary field of x'00' is the number 0, not NULL
    assert got["cnt"] == 65534
    zero = raw[:29] + b"\x00\x00" + raw[31:]
    assert dict(zip([c for c, _ in lay.columns],
                    lay.decode(zero)[0]))["cnt"] == 0


def test_a_value_that_does_not_fit_is_refused_not_rounded():
    lay = _layout("elementary")
    raw, _ = C.ELEMENTARY_RECORDS[0]
    values, _ = lay.decode(raw)
    cols = [c for c, _ in lay.columns]
    for col, value, why in (("amt", Decimal("1.234"), "decimal places"),
                            ("amt", Decimal("12345678.90"), "digits"),
                            ("uamt", -1, "negative"),
                            ("cust_name", "X" * 11, "bytes of text")):
        v = list(values)
        v[cols.index(col)] = value
        with pytest.raises(cb.Unreadable) as e:
            lay.encode(v)
        assert why in str(e.value), (col, str(e.value))


def test_a_hexadecimal_float_written_is_the_nearest_it_holds():
    lay = _layout("elementary")
    raw, _ = C.ELEMENTARY_RECORDS[0]
    values, _ = lay.decode(raw)
    cols = [c for c, _ in lay.columns]
    v = list(values)
    v[cols.index("short_flt")] = 0.1
    back = lay.decode(lay.encode(v))[0][cols.index("short_flt")]
    # 0.1 in a 24-bit fraction: x'4019999A', IBM's own short 0.1
    assert lay.encode(v)[62:66] == bytes.fromhex("4019999A")
    assert back != 0.1 and abs(back - 0.1) < 1e-7


SYNTAX = """\
      * free of sequence numbers, lower case, clauses in any order
       01  sales-rec.
           05  sale-id       usage is computational-3 picture is s9(5).
           05  sale-kind     pic x.
               88  refund         values are 'R' 'X'.
               88  normal-sale    value 'A' through 'M'.
           05  sale-note     pic x(20) value 'THIS IS A LONG LITERA
      -    'L'.
           copy money replacing ==:pfx:== by ==sale==.
"""

MONEY = """\
           05  :pfx:-amount  pic s9(7)v99 comp-3.
"""


def test_the_copybook_syntax_a_real_one_uses():
    rec = cb.parse(SYNTAX, member=lambda n: MONEY if n.lower() == "money"
                   else None)[0]
    lay = cb.Layout(rec)
    assert [c for c, _ in lay.columns] == [
        "sale_id", "sale_kind", "sale_note", "sale_amount", "refund",
        "normal_sale"]
    raw = (bytes.fromhex("12345C") + "R".encode("cp037")
           + "X".ljust(20).encode("cp037") + bytes.fromhex("000012345D"))
    got = dict(zip([c for c, _ in lay.columns], lay.decode(raw)[0]))
    assert got == {"sale_id": 12345, "sale_kind": "R", "sale_note": "X",
                   "sale_amount": Decimal("-123.45"), "refund": True,
                   "normal_sale": False}
    with pytest.raises(cb.CopybookError) as e:
        cb.parse(SYNTAX)
    assert "COPY money" in str(e.value)


def test_free_format_and_a_copybook_below_01():
    rec = cb.parse("*> no 01 level\n05 A PIC X(2). *> trailing comment\n"
                   "05 B PIC 9(2).\n", source_format="free")[0]
    lay = cb.Layout(rec)
    assert [c for c, _ in lay.columns] == ["a", "b"] and lay.length == 4


@pytest.mark.parametrize("text,why", [
    ("01 R. 05 A PIC G(2).", "DBCS"),
    ("01 R. 05 A PIC X. 66 B RENAMES A.", "RENAMES"),
    ("01 R. 05 A PIC X(2). 05 A PIC X(2).", "no group name"),
    ("01 R. 05 N PIC 9. 05 T OCCURS 1 TO 3 DEPENDING ON M. 10 X PIC X.",
     "is not a field before it"),
    ("01 R. 05 A PIC X COMP-3.", "numeric USAGE"),
])
def test_what_is_not_read_is_said(text, why):
    with pytest.raises(cb.CopybookError) as e:
        cb.Layout(cb.parse(text)[0])
    assert why in str(e.value), str(e.value)


def test_a_code_page_that_is_not_ebcdic_is_refused():
    with pytest.raises(cb.CopybookError) as e:
        cb.Layout(cb.parse("01 R. 05 A PIC 9.")[0], "latin-1")
    assert "not EBCDIC" in str(e.value)


def _orders():
    return [raw for raw, _ in C.ORDER_RECORDS]


@pytest.mark.parametrize("recfm,build", [
    ("V", lambda rs: b"".join(C.rdw(r) for r in rs)),
    ("VB", lambda rs: C.bdw(rs[0], rs[1]) + C.bdw(rs[2])),
    ("ZOSMF", lambda rs: b"".join(len(r).to_bytes(4, "big") + r for r in rs)),
])
def test_variable_records_in_each_format(recfm, build):
    data = build(_orders())
    got = [r for _, r in cb.records(io.BytesIO(data), recfm, 0)]
    assert got == _orders()
    lay = _layout("orders")
    assert [dict(zip([c for c, _ in lay.columns],
                     lay.decode(r)[0]))["note"] for r in got] == \
        ["NONE", "TWO", "MAX"]
    # writing frames them back the way they were read, but for VB, which
    # is written one record to a block
    if recfm != "VB":
        assert b"".join(cb.frame(recfm, lay.encode(lay.decode(r)[0]), 0)
                        for r in got) == data


def test_a_file_cut_short_says_where():
    data = b"".join(C.rdw(r) for r in _orders())
    with pytest.raises(cb.Unreadable) as e:
        list(cb.records(io.BytesIO(data[:-3]), "V", 0))
    assert "ends after" in str(e.value)
    fixed = C.ELEMENTARY_RECORDS[0][0] * 2 + b"\x40" * 10
    got = []
    with pytest.raises(cb.Unreadable) as e:
        for _, r in cb.records(io.BytesIO(fixed), "F", 74):
            got.append(r)
    assert len(got) == 2 and "10 bytes after the last whole record" in \
        str(e.value)
    spanned = (12).to_bytes(2, "big") + b"\x01\x00" + b"\x40" * 8
    with pytest.raises(cb.Unreadable) as e:
        list(cb.records(io.BytesIO(spanned), "V", 0))
    assert "spanned records are not read" in str(e.value)


def test_ranges_of_a_file_start_on_records_and_cover_it_once():
    rs = [raw for raw, _ in C.ORDER_RECORDS] * 7
    data = b"".join(C.rdw(r) for r in rs)
    f = io.BytesIO(data)
    at = cb.starts(f, "V", 0, len(data), 5)
    assert at[0] == 0 and len(at) == 5
    seen = []
    for a, b in zip(at, at[1:] + [None]):
        seen += [r for _, r in cb.records(f, "V", 0, a, b)]
    assert seen == rs
    fixed = b"".join(raw for raw, _ in C.ELEMENTARY_RECORDS) * 3
    at = cb.starts(io.BytesIO(fixed), "F", 74, len(fixed), 4)
    assert at == [0, 296, 592]
    # a range that does not start on a record starts at the next one
    assert [o for o, _ in cb.records(io.BytesIO(fixed), "F", 74, 1, 300)] \
        == [74, 148, 222, 296]
