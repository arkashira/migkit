"""Copybooks and records whose values are known, to choose and hold a
reader by.

Nothing here comes from migkit: each record is the bytes IBM documents for
its values, written out by hand field by field (a packed +123 is x'123C',
a zoned -123 is x'F1F2D3', `SIGN LEADING SEPARATE` -12 is x'60F0F1F2', an
unsigned halfword of 65534 is x'FFFE', `COMP-2` 0.1 is x'401999999999999A'),
with text through Python's own code pages. A reader is right on this
corpus when every value reads as the comment beside its bytes says, and
every value in `MUST_FAIL` is refused rather than read as something.

Values are keyed by (COBOL name, subscripts); a hexadecimal float's value
is given as the exact fraction the format holds.
"""
from decimal import Decimal
from fractions import Fraction

import ebcdic  # noqa: F401 - registers cp838 with Python's codecs


def text(s, width, cp="cp037"):
    return s.encode(cp).ljust(width, " ".encode(cp))


def hx(s):
    return bytes.fromhex(s.replace(" ", ""))


# ---- A: one of each kind of field IBM stores --------------------------------

ELEMENTARY = """\
000100* one elementary item of each kind IBM stores
000200 01  CUST-REC.
000300     05  CUST-NAME       PIC X(10).
000400     05  QTY             PIC 9(5).
000500     05  BAL             PIC S9(5)V99.
000600     05  AMT             PIC S9(7)V99 COMP-3.
000700     05  UAMT            PIC 9(3) COMP-3.
000800     05  CNT             PIC 9(4) COMP.
000900     05  DELTA           PIC S9(9) COMP.
001000     05  BIG             PIC S9(18) BINARY.
001100     05  NATIVE          PIC S9(4) COMP-5.
001200     05  LEAD-SEP        PIC S9(3) SIGN LEADING SEPARATE.
001300     05  TRAIL-SEP       PIC S9(3) SIGN IS TRAILING
001310                         SEPARATE CHARACTER.
001400     05  LEAD-OVER       PIC S9(3) SIGN LEADING.
001500     05  RATE            PIC SV9(4) COMP-3.
001600     05  HUNDREDS        PIC 9(3)PP.
001700     05  SHORT-FLT       COMP-1.
001800     05  LONG-FLT        COMP-2.
"""


def _elementary(name, qty, bal, amt, uamt, cnt, delta, big, native, lsep,
                tsep, lover, rate, hundreds, f1, f2):
    return (text(name, 10) + hx(qty) + hx(bal) + hx(amt) + hx(uamt)
            + hx(cnt) + hx(delta) + hx(big) + hx(native) + hx(lsep)
            + hx(tsep) + hx(lover) + hx(rate) + hx(hundreds) + hx(f1)
            + hx(f2))


ELEMENTARY_RECORDS = [
    (_elementary("JOHN SMITH",
                 "F1F2F3F4F5",            # 12345
                 "F0F0F1F2F3F4D5",        # -00123.45
                 "123456789C",            # +1234567.89
                 "123F",                  # 123, unsigned packed
                 "FFFE",                  # 65534, unsigned halfword
                 "FFFFFFFE",              # -2
                 "0DE0B6B3A763FFFF",      # 999999999999999999
                 "8000",                  # -32768: COMP-5 is the halfword
                 "60F0F1F2",              # -12, sign leading separate
                 "F0F0F74E",              # +7, sign trailing separate
                 "D1F2F3",                # -123, sign leading overpunched
                 "01234C",                # .1234
                 "F1F2F3",                # 123 hundreds: 12300
                 "41100000",              # COMP-1 1.0
                 "401999999999999A"),     # COMP-2 0.1 (not IEEE's 0.1)
     {("CUST-NAME", ()): "JOHN SMITH", ("QTY", ()): 12345,
      ("BAL", ()): Decimal("-123.45"), ("AMT", ()): Decimal("1234567.89"),
      ("UAMT", ()): 123, ("CNT", ()): 65534, ("DELTA", ()): -2,
      ("BIG", ()): 999999999999999999, ("NATIVE", ()): -32768,
      ("LEAD-SEP", ()): -12, ("TRAIL-SEP", ()): 7, ("LEAD-OVER", ()): -123,
      ("RATE", ()): Decimal("0.1234"), ("HUNDREDS", ()): 12300,
      ("SHORT-FLT", ()): Fraction(1),
      ("LONG-FLT", ()): Fraction(0x1999999999999A, 2 ** 56)}),
    (_elementary("ANNA",
                 "F0F0F0F0F0",            # 0
                 "F0F0F0F0F0F0D0",        # -0.00: zero
                 "000000000D",            # packed -0: zero
                 "000F",                  # 0
                 "0000",                  # 0
                 "7FFFFFFF",              # 2147483647
                 "FFFFFFFFFFFFFFFF",      # -1
                 "7FFF",                  # 32767
                 "4EF0F0F0",              # +0
                 "F0F0F060",              # -0: zero
                 "C0F0F5",                # +5, sign in the first byte
                 "00001D",                # -.0001
                 "F0F0F0",                # 0
                 "C2640000",              # COMP-1 -100.0
                 "0000000000000000"),     # COMP-2 0
     {("CUST-NAME", ()): "ANNA", ("QTY", ()): 0, ("BAL", ()): Decimal("0.00"),
      ("AMT", ()): Decimal("0.00"), ("UAMT", ()): 0, ("CNT", ()): 0,
      ("DELTA", ()): 2147483647, ("BIG", ()): -1, ("NATIVE", ()): 32767,
      ("LEAD-SEP", ()): 0, ("TRAIL-SEP", ()): 0, ("LEAD-OVER", ()): 5,
      ("RATE", ()): Decimal("-0.0001"), ("HUNDREDS", ()): 0,
      ("SHORT-FLT", ()): Fraction(-100), ("LONG-FLT", ()): Fraction(0)}),
    (_elementary("  LEADING",             # leading blanks are data
                 "F9F9F9F9F9",            # 99999
                 "F0F0F1F2F3F4F5",        # +123.45, sign F
                 "000000100F",            # +1.00, sign F
                 "999F",                  # 999
                 "0001",                  # 1
                 "80000000",              # -2147483648
                 "0000000000000000",      # 0
                 "0000",                  # 0
                 "4EF9F9F9",              # +999
                 "F9F9F960",              # -999
                 "A1F2F3",                # +123, sign A
                 "09999C",                # .9999
                 "F9F9F9",                # 99900
                 "C1100000",              # COMP-1 -1.0
                 "C110000000000000"),     # COMP-2 -1.0
     {("CUST-NAME", ()): "  LEADING", ("QTY", ()): 99999,
      ("BAL", ()): Decimal("123.45"), ("AMT", ()): Decimal("1.00"),
      ("UAMT", ()): 999, ("CNT", ()): 1, ("DELTA", ()): -2147483648,
      ("BIG", ()): 0, ("NATIVE", ()): 0, ("LEAD-SEP", ()): 999,
      ("TRAIL-SEP", ()): -999, ("LEAD-OVER", ()): 123,
      ("RATE", ()): Decimal("0.9999"), ("HUNDREDS", ()): 99900,
      ("SHORT-FLT", ()): Fraction(-1), ("LONG-FLT", ()): Fraction(-1)}),
]

_GOOD = ["F1F2F3F4F5", "F0F0F1F2F3F4D5", "123456789C", "123F", "FFFE",
         "FFFFFFFE", "0DE0B6B3A763FFFF", "8000", "60F0F1F2", "F0F0F74E",
         "D1F2F3", "01234C", "F1F2F3", "41100000", "401999999999999A"]


def _with(i, bad):
    fields = list(_GOOD)
    fields[i] = bad
    return _elementary("BAD", *fields)


#: (record, the field that must be refused): each holds one value that is
#: not one, and every other field as record 1 of ELEMENTARY_RECORDS
ELEMENTARY_MUST_FAIL = [
    (_with(2, "12345A789C"), "AMT"),        # a digit nibble of A
    (_with(2, "1234567890"), "AMT"),        # sign nibble 0
    (_with(0, "F140F3F4F5"), "QTY"),        # a blank inside a number
    (_with(0, "F1F2F3F4D5"), "QTY"),        # negative in a field with no S
    (_with(3, "123D"), "UAMT"),             # negative packed, no S
    (_with(9, "F0F0F740"), "TRAIL-SEP"),    # separate sign that is a blank
    (_with(1, "F0F0C1F2F3F4D5"), "BAL"),    # a letter where a digit goes
]


# ---- B: REDEFINES, 88 levels, OCCURS, Thai text -------------------------------

ACCOUNT = """\
       01  ACCOUNT.
           05  ACCT-TYPE        PIC X.
               88  IS-PERSONAL      VALUE 'P'.
               88  IS-BUSINESS      VALUE 'B' 'C'.
               88  IS-ALNUM         VALUE 'A' THRU '9'.
           05  ACCT-ID          PIC 9(6).
           05  DETAIL           PIC X(12).
           05  PERSONAL REDEFINES DETAIL.
               10  BIRTH-DATE   PIC 9(8).
               10  GENDER       PIC X.
               10  FILLER       PIC X(3).
           05  BUSINESS REDEFINES DETAIL.
               10  TAX-ID       PIC X(10).
               10  EMPLOYEES    PIC S9(3) COMP-3.
           05  BALANCES OCCURS 3 TIMES.
               10  BAL-AMT      PIC S9(5)V99 COMP-3.
               10  BAL-CCY      PIC X(3).
           05  SCORE            PIC 9 OCCURS 2.
               88  TOP-SCORE    VALUE 9.
           05  OWNER-TH         PIC X(6).
"""

ACCOUNT_CODEPAGE = "cp838"
ACCOUNT_RULE = {"DETAIL": {"by": "ACCT-TYPE",
                           "when": {"P": "PERSONAL", "B": "BUSINESS",
                                    "C": "BUSINESS"}}}


def _account(kind, ident, detail, balances, scores, owner):
    cp = ACCOUNT_CODEPAGE
    out = text(kind, 1, cp) + hx(ident) + detail
    for amt, ccy in balances:
        out += hx(amt) + text(ccy, 3, cp)
    return out + hx(scores) + text(owner, 6, cp)


ACCOUNT_RECORDS = [
    (_account("P", "F1F2F3F4F5F6",
              hx("F1F9F8F0F0F2F2F9") + text("F", 1, "cp838")
              + text("", 3, "cp838"),
              [("0010050C", "USD"), ("0000001D", "THB"), ("0000000C", "EUR")],
              "F9F3", "สมชาย"),
     {("ACCT-TYPE", ()): "P", ("ACCT-ID", ()): 123456,
      ("BIRTH-DATE", ()): 19800229, ("GENDER", ()): "F",
      ("TAX-ID", ()): None, ("EMPLOYEES", ()): None,
      ("BAL-AMT", (1,)): Decimal("100.50"), ("BAL-CCY", (1,)): "USD",
      ("BAL-AMT", (2,)): Decimal("-0.01"), ("BAL-CCY", (2,)): "THB",
      ("BAL-AMT", (3,)): Decimal("0.00"), ("BAL-CCY", (3,)): "EUR",
      ("SCORE", (1,)): 9, ("SCORE", (2,)): 3, ("OWNER-TH", ()): "สมชาย",
      ("IS-PERSONAL", ()): True, ("IS-BUSINESS", ()): False,
      # 'A' THRU '9' in EBCDIC runs x'C1' to x'F9', which holds 'P' (x'D7');
      # in Unicode the same range is empty
      ("IS-ALNUM", ()): True,
      ("TOP-SCORE", (1,)): True, ("TOP-SCORE", (2,)): False}),
    (_account("C", "F0F0F0F0F4F2",
              text("0105556789", 10, "cp838") + hx("005D"),
              [("9999999C", "JPY"), ("0000000C", "SGD"), ("0000100D", "GBP")],
              "F0F0", "กข"),
     {("ACCT-TYPE", ()): "C", ("ACCT-ID", ()): 42,
      ("BIRTH-DATE", ()): None, ("GENDER", ()): None,
      ("TAX-ID", ()): "0105556789", ("EMPLOYEES", ()): -5,
      ("BAL-AMT", (1,)): Decimal("99999.99"), ("BAL-CCY", (1,)): "JPY",
      ("BAL-AMT", (2,)): Decimal("0.00"), ("BAL-CCY", (2,)): "SGD",
      ("BAL-AMT", (3,)): Decimal("-1.00"), ("BAL-CCY", (3,)): "GBP",
      ("SCORE", (1,)): 0, ("SCORE", (2,)): 0, ("OWNER-TH", ()): "กข",
      ("IS-PERSONAL", ()): False, ("IS-BUSINESS", ()): True,
      ("IS-ALNUM", ()): True,
      ("TOP-SCORE", (1,)): False, ("TOP-SCORE", (2,)): False}),
]

#: a type the rule names no alternative for: the detail cannot be read
ACCOUNT_MUST_FAIL = [
    (_account("Z", "F0F0F0F0F0F1", text("?", 12, "cp838"),
              [("0000000C", "USD")] * 3, "F0F0", "x"), "DETAIL"),
]


# ---- C: OCCURS DEPENDING ON in variable records ------------------------------

ORDERS = """\
       01  ORDER-REC.
           05  ORDER-NO         PIC 9(5).
           05  LINE-COUNT       PIC S9(4) COMP.
           05  ORDER-LINE OCCURS 0 TO 5 TIMES DEPENDING ON LINE-COUNT.
               10  SKU          PIC X(4).
               10  PRICE        PIC S9(5)V99 COMP-3.
           05  NOTE             PIC X(4).
"""


def _order(no, lines, note, count=None):
    out = hx(no) + (len(lines) if count is None else count).to_bytes(
        2, "big", signed=True)
    for sku, price in lines:
        out += text(sku, 4) + hx(price)
    return out + text(note, 4)


def _order_values(no, lines, note):
    got = {("ORDER-NO", ()): no, ("LINE-COUNT", ()): len(lines),
           ("NOTE", ()): note}
    for i in range(5):
        sku, price = lines[i] if i < len(lines) else (None, None)
        got[("SKU", (i + 1,))] = sku
        got[("PRICE", (i + 1,))] = price
    return got


ORDER_RECORDS = [
    (_order("F0F0F0F0F1", [], "NONE"), _order_values(1, [], "NONE")),
    (_order("F0F0F0F0F2", [("A001", "0001000C"), ("B002", "0000250D")],
            "TWO "),
     _order_values(2, [("A001", Decimal("10.00")),
                       ("B002", Decimal("-2.50"))], "TWO")),
    # the most the table holds
    (_order("F0F0F0F0F3", [(f"S{i:03d}", f"00{i:05d}C") for i in range(5)],
            "MAX "),
     _order_values(3, [(f"S{i:03d}", Decimal(i).scaleb(-2))
                       for i in range(5)], "MAX")),
]

#: a count past the table's most: nothing after it can be placed
ORDER_MUST_FAIL = [
    (_order("F0F0F0F0F4", [("A001", "0001000C")] * 5, "OVER", count=6),
     "LINE-COUNT"),
]


def rdw(rec):
    """A record as RECFM=V keeps it: a 4-byte descriptor, its length
    counting itself, then two zero bytes."""
    return (len(rec) + 4).to_bytes(2, "big") + b"\x00\x00" + rec


def bdw(*recs):
    """A block of RECFM=VB: a 4-byte descriptor, then the records with
    theirs."""
    body = b"".join(rdw(r) for r in recs)
    return (len(body) + 4).to_bytes(2, "big") + b"\x00\x00" + body


# ---- D: SYNC, BLANK WHEN ZERO, national, group usage, nested OCCURS -----------

MISC = """\
       01  MISC-REC.
           05  FLAG             PIC X.
           05  WORD             PIC S9(9) COMP SYNC.
           05  CODE-2           PIC XX.
           05  HALF             PIC S9(4) COMP SYNC.
           05  PRICE-BWZ        PIC 9(3)V99 BLANK WHEN ZERO.
           05  NAME-N           PIC N(3).
           05  PACKED-GROUP     COMP-3.
               10  P1           PIC S9(3).
               10  P2           PIC S9(4).
           05  GRID OCCURS 2.
               10  CELL         PIC X OCCURS 2.
"""

MISC_RECORDS = [
    (text("Y", 1) + hx("404040")          # three slack bytes before WORD
     + hx("12345678")                     # 305419896
     + text("AB", 2)
     + hx("FED4")                         # -300, already on a halfword
     + hx("4040404040")                   # blank when zero: 0.00
     + "ไทย".encode("utf-16-be")
     + hx("007D") + hx("01234C")          # -7, 1234
     + text("abcd", 4),
     {("FLAG", ()): "Y", ("WORD", ()): 305419896, ("CODE-2", ()): "AB",
      ("HALF", ()): -300, ("PRICE-BWZ", ()): Decimal("0.00"),
      ("NAME-N", ()): "ไทย", ("P1", ()): -7, ("P2", ()): 1234,
      ("CELL", (1, 1)): "a", ("CELL", (1, 2)): "b", ("CELL", (2, 1)): "c",
      ("CELL", (2, 2)): "d"}),
]

MISC_MUST_FAIL = [
    # five digits in a PIC S9(4) packed field
    (MISC_RECORDS[0][0][:25] + hx("12345C") + MISC_RECORDS[0][0][28:], "P2"),
]


#: every case: (name, copybook, code page, table rules, record format,
#: [(record, expected)], [(record, field that must fail)])
CASES = [
    ("elementary", ELEMENTARY, "cp037", {}, "F", ELEMENTARY_RECORDS,
     ELEMENTARY_MUST_FAIL),
    ("account", ACCOUNT, ACCOUNT_CODEPAGE, ACCOUNT_RULE, "F", ACCOUNT_RECORDS,
     ACCOUNT_MUST_FAIL),
    ("orders", ORDERS, "cp037", {}, "V", ORDER_RECORDS, ORDER_MUST_FAIL),
    ("misc", MISC, "cp037", {}, "F", MISC_RECORDS, MISC_MUST_FAIL),
]
