"""Db2 for i and Db2 for z/OS, held to what does not need a server (backlog
34, W5): no IBM i and no z/OS runs in a container, and neither may be
connected to from here - PUB400.com is where the operator can prove the
IBM i side on a real system.

What is held: which platform a server is by the name its driver gives,
how each is reached, the catalogue's rows as each engine turns them into
columns and classes, a Mapepire reading's exactness, a journal entry's
record image cut into its values, the journal read into changes in commit
order - a rolled-back transaction carrying nothing, a key change carried
as a delete and an insert, nothing carried twice across two reads - the
receivers that make a position lost, the refusals that name their fix,
`HASH_ROW` deciding which tables are read row by row, users and object
authorities compared, and z/OS saying how deep it is reached.
"""
import datetime
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.users", "db2i"), ("assess.engine-checks", "db2zos")]


def _hop(engine="db2i", **src_options):
    hop = Hop(name="i", engine=engine,
              source=Endpoint(host="10.0.0.7", port=8076, user="MIGKIT",
                              password="CHANGE_ME", options=src_options),
              target=Endpoint(host="10.0.0.8", port=8076, user="MIGKIT",
                              password="CHANGE_ME"),
              databases=["shop"])
    hop.report_dir = lambda db=None: __import__("pathlib").Path("/tmp")
    return hop


def _db2i(**src_options):
    from migkit.engines.db2i import Db2iEngine
    return Db2iEngine(_hop(**src_options))


class _Cursor:
    def __init__(self, answer, said):
        self.answer, self.said, self.rows = answer, said, []
        self.description, self.rowcount = None, -1

    def execute(self, sql, params=()):
        self.said.append((sql, tuple(params or ())))
        self.rows = [list(r) for r in self.answer(sql, tuple(params or ()))]

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, n):
        rows, self.rows = self.rows[:n], self.rows[n:]
        return rows

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class _Conn:
    def __init__(self, answer, said, name="DB2/400 SQL"):
        self.answer, self.said, self.name = answer, said, name

    def cursor(self):
        return _Cursor(self.answer, self.said)

    def server_info(self):
        return (self.name, "07.05.0000")

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _answering(eng, monkeypatch, answer):
    said = []
    monkeypatch.setattr(type(eng), "_connect",
                        lambda self, side, db: _Conn(answer, said))
    return said


# ---- which Db2 it is, and how it is reached ---------------------------------

@pytest.mark.parametrize("name,platform", [
    ("DB2/LINUXX8664", "luw"), ("DB2/NT64", "luw"), ("DB2", "zos"),
    ("DSN12015", "zos"), ("AS", "i"), ("DB2/400 SQL", "i"),
    ("QSQ07050", "i"), ("PostgreSQL", None)])
def test_the_platform_by_the_name_the_driver_gives(name, platform):
    from migkit.engines.db2 import platform_of
    assert platform_of(name) == platform


def test_a_server_of_another_platform_is_named_as_its_engine(monkeypatch):
    import ibm_db_dbi

    from migkit.engines.db2 import Db2Engine
    hop = _hop("db2", database="SAMPLE")
    monkeypatch.setattr(ibm_db_dbi, "connect",
                        lambda *a: _Conn(lambda s, p: [], [], name="DB2"))
    with pytest.raises(SystemExit) as e:
        Db2Engine(hop)._connect("src", "shop")
    assert "Db2 for z/OS" in str(e.value) and "db2zos" in str(e.value)
    # the platform it is for passes
    monkeypatch.setattr(ibm_db_dbi, "connect",
                        lambda *a: _Conn(lambda s, p: [], [],
                                         name="DB2/LINUXX8664"))
    assert Db2Engine(hop)._connect("src", "shop") is not None


def test_a_refusal_for_want_of_db2_connect_is_said_as_that(monkeypatch):
    import ibm_db_dbi

    from migkit.engines.db2zos import Db2ZosEngine

    def refuse(*a):
        raise Exception("[IBM][CLI Driver] SQL1598N  An attempt to connect"
                        " to the database server failed because of a"
                        " licensing problem.  SQLSTATE=42968")
    monkeypatch.setattr(ibm_db_dbi, "connect", refuse)
    with pytest.raises(SystemExit) as e:
        Db2ZosEngine(_hop("db2zos", database="DSNLOC1"))._connect("src",
                                                                    "shop")
    said = str(e.value)
    assert "Db2 Connect" in said and "migkit doctor --install" in said
    # any other failure is not dressed up as one
    monkeypatch.setattr(ibm_db_dbi, "connect",
                        lambda *a: (_ for _ in ()).throw(
                            OSError("connection refused")))
    with pytest.raises(OSError):
        Db2ZosEngine(_hop("db2zos", database="DSNLOC1"))._connect("src",
                                                                    "shop")


def test_the_way_in_is_the_driver_then_drda_then_mapepire(monkeypatch):
    from migkit.engines import db2i
    monkeypatch.setattr(db2i, "odbc_driver",
                        lambda: "IBM i Access ODBC Driver 64-bit")
    assert _db2i().rung("src") == ("odbc", "IBM i Access ODBC Driver 64-bit")
    monkeypatch.setattr(db2i, "odbc_driver", lambda: None)
    monkeypatch.setattr(db2i, "db2_connect_licence", lambda: "/x/db2consv"
                                                             "_is.lic")
    assert _db2i(database="S1234567").rung("src") == ("drda", None)
    # a licence without the relational database to name is not a way in
    assert _db2i().rung("src") == ("mapepire", None)
    monkeypatch.setattr(db2i, "db2_connect_licence", lambda: None)
    assert _db2i(database="S1234567").rung("src") == ("mapepire", None)


def test_the_odbc_connection_says_what_it_asks_for(monkeypatch):
    import sys
    import types

    from migkit.engines import db2i
    asked = {}
    fake = types.SimpleNamespace(
        SQL_DBMS_NAME=17,
        connect=lambda text, **kw: asked.update(text=text, **kw) or types
        .SimpleNamespace(getinfo=lambda k: "DB2/400 SQL",
                         close=lambda: None))
    monkeypatch.setitem(sys.modules, "pyodbc", fake)
    monkeypatch.setattr(db2i, "odbc_driver",
                        lambda: "IBM i Access ODBC Driver")
    eng = db2i.Db2iEngine(_hop(tls=True))
    eng.hop.source.password = "p;w}d"
    eng._connect("src", "shop")
    parts = dict(p.split("=", 1) for p in
                 asked["text"].replace("}}", "\x00").split(";") if "=" in p)
    assert parts["DRIVER"] == "{IBM i Access ODBC Driver}"
    assert parts["NAM"] == "0" and parts["CMT"] == "1" and \
        parts["CCSID"] == "1208" and parts["TRANSLATE"] == "0" and \
        parts["SSL"] == "1"
    # a password with the separators in it is braced, not split
    assert "PWD={p;w}}d}" in asked["text"]
    assert asked["autocommit"] is False


def test_an_address_names_each_platform():
    from migkit import addresses
    assert addresses.parse("ibmi://MIGKIT@10.0.0.7/SHOP,ORDERS") == (
        "db2i", {"user": "MIGKIT", "host": "10.0.0.7"}, ["SHOP", "ORDERS"])
    assert addresses.parse("db2zos://MIGKIT@10.0.0.9:446/DSNLOC1/SHOP") == (
        "db2zos", {"user": "MIGKIT", "host": "10.0.0.9", "port": 446,
                   "database": "DSNLOC1"}, ["SHOP"])
    # an option the engine does not read is refused where it was written
    with pytest.raises(SystemExit) as e:
        addresses.parse("ibmi://MIGKIT@10.0.0.7/SHOP?recfm=VB")
    assert "no option 'recfm'" in str(e.value)


# ---- the catalogue --------------------------------------------------------

def test_tables_columns_and_key_from_qsys2(monkeypatch):
    from migkit import canon

    def answer(sql, params):
        if "qsys2.systables" in sql:
            assert params == ("SHOP",)
            return [("ORDERS",), ("Mixed",)]
        if "qsys2.syscolumns" in sql:
            assert "ccsid = 65535" in sql and "datetime_precision" in sql
            return [("ID", "INTEGER"), ("AMT", "DECIMAL(9,2)"),
                    ("QTY", "NUMERIC(5,0)"), ("RAW", "VARBINARY(8)"),
                    ("AT", "TIMESTAMP(6)"), ("NOTE", "VARG(20)"),
                    ("ROW_ID", "ROWID")]
        if "qsys2.syscst" in sql:
            assert "'PRIMARY KEY'" in sql
            return [("ID",)]
        raise AssertionError(sql)
    eng = _db2i()
    _answering(eng, monkeypatch, answer)
    assert eng.neutral_tables("src", "shop") == ["orders", "Mixed"]
    cols = dict(eng.neutral_columns("src", "shop", "orders"))
    classes = {n: canon.type_class("db2i", t) for n, t in cols.items()}
    assert classes == {"id": "integer", "amt": "decimal", "qty": "decimal",
                       "raw": "bytes", "at": "timestamp", "note": "text",
                       "row_id": None}
    assert eng.neutral_key("src", "shop", "orders") == ["id"]
    assert eng._qualified("src", "shop", "orders") == '"SHOP"."ORDERS"'


def test_a_table_without_a_key_is_read_by_relative_record_number(
        monkeypatch):
    eng = _db2i()
    _answering(eng, monkeypatch, lambda s, p: [(1, 2500)])
    spans = eng.position_spans("src", "shop", "log", 1000)
    assert [s for s, _ in spans] == [1, 1001, 2001]
    assert spans[0][1] == ('rrn("SHOP"."LOG") >= 1 and rrn("SHOP"."LOG")'
                           ' < 1001')
    # the last is open-ended: rows put after the plan are read too
    assert spans[-1][1] == 'rrn("SHOP"."LOG") >= 2001'


def test_over_mapepire_a_value_is_read_as_its_exact_text(monkeypatch):
    from migkit.engines import db2i
    monkeypatch.setattr(db2i, "odbc_driver", lambda: None)
    monkeypatch.setattr(db2i, "db2_connect_licence", lambda: None)
    eng = _db2i()
    eng.__dict__["_reading"] = "src"
    expr, parse = eng._read_as('"AMT"', "DECIMAL(31,2)")
    assert expr == 'varchar("AMT")'
    # 31 digits: a JSON number would have kept 17 of them
    assert parse("12345678901234567890123456789.01") == \
        Decimal("12345678901234567890123456789.01")
    assert str(parse("0.10")) == "0.10"
    expr, parse = eng._read_as('"D"', "DATE")
    assert parse("2024-02-29") == datetime.date(2024, 2, 29)
    expr, parse = eng._read_as('"B"', "VARBINARY(4)")
    assert expr == 'hex("B")' and parse("00FF41") == b"\x00\xffA"
    # over IBM's own driver nothing is read as text
    monkeypatch.setattr(db2i, "odbc_driver", lambda: "IBM i Access ODBC"
                                                     " Driver")
    other = _db2i()
    other.__dict__["_reading"] = "src"
    assert other._read_as('"AMT"', "DECIMAL(31,2)") is None


# ---- a journal entry's record image ----------------------------------------

COLUMNS = [  # name, type, length, scale, ccsid, storage, precision
    ("ID", "INTEGER", 4, 0, None, 4, None),
    ("NAME", "CHAR", 10, None, 37, 10, None),
    ("AMT", "DECIMAL", 9, 2, None, 5, None),
    ("QTY", "NUMERIC", 5, 0, None, 5, None),
    ("NOTE", "VARCHAR", 20, None, 838, 22, None),
    ("D", "DATE", 10, None, None, 10, None),
    ("TS", "TIMESTMP", 26, None, None, 26, 6),
]


def _image(ident, name, amt_hex, qty_hex, note, day, stamp):
    note_b = note.encode("cp838")
    return (ident.to_bytes(4, "big", signed=True)
            + name.ljust(10).encode("cp037") + bytes.fromhex(amt_hex)
            + bytes.fromhex(qty_hex)
            + len(note_b).to_bytes(2, "big") + note_b.ljust(20, b"\x40")
            + day.encode("cp037") + stamp.encode("cp037"))


def test_a_record_image_is_cut_into_its_values():
    import ebcdic  # noqa: F401

    from migkit.engines.db2i import Image
    img = Image(COLUMNS)
    got = img.cut(_image(-7, "ANNA", "012345678D", "F0F0F0F4F2", "ไทย",
                         "2024-02-29", "2024-02-29-13.14.15.500000"),
                  "0000000")
    assert got == {"ID": -7, "NAME": "ANNA",
                   "AMT": Decimal("-123456.78"), "QTY": 42,
                   "NOTE": "ไทย", "D": datetime.date(2024, 2, 29),
                   "TS": datetime.datetime(2024, 2, 29, 13, 14, 15, 500000)}
    # the null indicators, as the journal writes them untranslated too
    nulled = img.cut(_image(1, "B", "000000000C", "F0F0F0F0F0", "",
                            "0001-01-01", "0001-01-01-00.00.00.000000"),
                     b"\xf0\xf0\xf0\xf0\xf1\xf1\xf0")
    assert nulled["NOTE"] is None and nulled["D"] is None and \
        nulled["AMT"] == Decimal("0.00")
    # a packed value that is not one stops, never reads as a number
    with pytest.raises(SystemExit) as e:
        img.cut(_image(1, "B", "0000A0000C", "F0F0F0F0F0", "", "0001-01-01",
                       "0001-01-01-00.00.00.000000"), "")
    assert "AMT" in str(e.value)


def test_what_follows_a_lob_in_the_image_is_read_again():
    from migkit.engines.db2i import Image, Reread
    img = Image([("ID", "INTEGER", 4, 0, None, 4, None),
                 ("DOC", "CLOB", 1000, None, 1208, 32, None),
                 ("QTY", "INTEGER", 4, 0, None, 4, None)])
    assert not img.whole
    raw = (5).to_bytes(4, "big") + b"\x00" * 40
    assert img.cut(raw, "", {"ID"}) == {"ID": 5}
    with pytest.raises(Reread):
        img.cut(raw, "", {"QTY"})


# ---- the journal, read into changes ----------------------------------------

def _entry(seq, code, etype, cycle, image=None, rrn=0, table="ORDERS"):
    obj = f"{table:<10}{'SHOP':<10}{table:<10}"
    return (seq, code, etype, rrn, cycle, image, "0000000" if image
            else None, obj if code == "R" else None, "0")


def _followed(monkeypatch, journal):
    """An engine whose journal holds `journal` (a list of entries, grown by
    the test), every other catalogue answer recorded."""
    from migkit.engines.db2i import Db2iEngine, Image
    eng = _db2i()
    tables = {"orders": {"system": ("ORDERS", "SHOP"),
                         "journal": "SHOP/QSQJRN", "images": "*BOTH"}}
    monkeypatch.setattr(Db2iEngine, "_journaled",
                        lambda self, side, db: tables)
    monkeypatch.setattr(Db2iEngine, "_key",
                        lambda self, side, db, t: ["ID"])
    monkeypatch.setattr(Db2iEngine, "_image",
                        lambda self, side, db, t: Image(COLUMNS))
    monkeypatch.setattr(Db2iEngine, "_receivers", lambda self, side, db, j: [
        ("SHOP", "QSQJRN0001", 1, max(e[0] for e in journal), "ATTACHED",
         "NO", "NO")])
    asked = []

    def answer(sql, params):
        asked.append(sql)
        assert "qsys2.display_journal" in sql
        assert "starting_receiver_name => '*CURCHAIN'" in sql
        assert "journal_entry_types => 'PT,PX,UB,UP,DL,BR,UR,DR,SC,CM,RB'"\
            in sql
        lib, name, first, last = params
        assert (lib, name) == ("SHOP", "QSQJRN")
        return [e for e in journal if first <= e[0] <= last]
    _answering(eng, monkeypatch, answer)
    return eng, asked


def _row(ident, name="N", amt="000000100C"):
    return _image(ident, name, amt, "F0F0F0F0F1", "x", "2024-01-01",
                  "2024-01-01-00.00.00.000000")


def test_the_journal_is_read_into_changes_in_commit_order(monkeypatch):
    journal = [
        _entry(10, "C", "SC", 10),
        _entry(11, "R", "PT", 10, _row(1)),
        _entry(12, "C", "SC", 12),
        _entry(13, "R", "PT", 12, _row(2)),
        _entry(14, "R", "UB", 10, _row(1), rrn=1),
        _entry(15, "R", "UP", 10, _row(1, "M"), rrn=1),
        _entry(16, "C", "RB", 12),            # nothing of 12 happened
        _entry(17, "R", "DL", 0, _row(9)),    # outside a transaction
        _entry(18, "C", "CM", 10),
        _entry(19, "C", "SC", 19),
        _entry(20, "R", "PT", 19, _row(5)),   # 19 still open
    ]
    eng, _ = _followed(monkeypatch, journal)
    changes, token = eng.neutral_changes("src", "shop",
                                         {"SHOP/QSQJRN": [9, []]})
    said = [(c["op"], c["key"]["ID"], c["values"].get("NAME"))
            for c in changes]
    # the delete outside a transaction lands where it was; transaction 10
    # at its commit, in its own order; 12 not at all; 19 not yet
    assert said == [("delete", 9, None), ("insert", 1, "N"),
                    ("update", 1, "M")], said
    at, done = token["SHOP/QSQJRN"]
    assert at == 18, token     # before 19, which is still open
    assert ["s17", 17] not in done and ["c10", 18] not in done
    # read again with more of the journal: 19 commits, and nothing of the
    # first read is carried twice
    journal += [_entry(21, "R", "UB", 19, _row(5), rrn=5),
                _entry(22, "R", "UP", 19, _row(55), rrn=5),
                _entry(23, "C", "CM", 19)]
    changes, token = eng.neutral_changes("src", "shop", token)
    said = [(c["op"], c["key"]["ID"]) for c in changes]
    # a key changed inside it: the old row goes, the new one comes
    assert said == [("insert", 5), ("delete", 5), ("insert", 55)], said
    assert token["SHOP/QSQJRN"][0] == 23


def test_a_position_before_an_open_transaction_carries_nothing_twice(
        monkeypatch):
    journal = [_entry(10, "C", "SC", 10),
               _entry(11, "R", "PT", 0, _row(3)),
               _entry(12, "R", "PT", 10, _row(4))]
    eng, _ = _followed(monkeypatch, journal)
    first, token = eng.neutral_changes("src", "shop",
                                       {"SHOP/QSQJRN": [9, []]})
    assert [c["key"]["ID"] for c in first] == [3]
    assert token["SHOP/QSQJRN"][0] == 9       # 10 is still open
    journal.append(_entry(13, "C", "CM", 10))
    again, token = eng.neutral_changes("src", "shop", token)
    # 11 was carried already and is named done: only 10's row now
    assert [c["key"]["ID"] for c in again] == [4]


def test_a_savepoint_rolled_back_is_carried_as_the_rows_now_are(
        monkeypatch):
    from migkit.engines.db2i import Db2iEngine
    journal = [_entry(10, "C", "SC", 10),
               _entry(11, "R", "PT", 10, _row(7)),
               _entry(12, "R", "DR", 10, _row(7)),
               _entry(13, "R", "PT", 10, _row(8)),
               _entry(14, "C", "CM", 10)]
    eng, _ = _followed(monkeypatch, journal)
    asked = []

    def now(self, side, db, table, keys):
        asked.append(sorted(k["ID"] for k in keys))
        return []
    monkeypatch.setattr(Db2iEngine, "_as_now", now)
    eng.neutral_changes("src", "shop", {"SHOP/QSQJRN": [9, []]})
    assert asked == [[7, 8]]


def test_a_lost_position_is_said_not_stepped_over(monkeypatch):
    from migkit.engines.db2i import Db2iEngine
    eng = _db2i()
    receivers = [("SHOP", "QSQJRN0003", 200, 299, "ONLINE", "NO", "NO"),
                 ("SHOP", "QSQJRN0004", 300, 350, "ATTACHED", "NO", "NO"),
                 ("SHOP", "QSQJRN0002", 100, 199, "FREED", "NO", "NO")]
    monkeypatch.setattr(Db2iEngine, "_receivers",
                        lambda self, side, db, j: receivers)
    assert eng.position_lost("src", "shop", {"SHOP/QSQJRN": [250, []]}) \
        is None
    gone = eng.position_lost("src", "shop", {"SHOP/QSQJRN": [150, []]})
    assert "no longer holds entry 151" in gone and "QSQJRN0003" in gone
    # a gap in the chain
    receivers[1] = ("SHOP", "QSQJRN0004", 320, 350, "ATTACHED", "NO", "NO")
    gap = eng.position_lost("src", "shop", {"SHOP/QSQJRN": [250, []]})
    assert "not a chain" in gap and "320" in gap
    # sequence numbers reset since the position was taken
    receivers[:] = [("SHOP", "QSQJRN0009", 1, 40, "ATTACHED", "NO", "NO")]
    assert "reset" in eng.position_lost("src", "shop",
                                        {"SHOP/QSQJRN": [250, []]})


@pytest.mark.parametrize("tables,key,fix", [
    ({"orders": {"system": ("ORDERS", "SHOP"), "journal": None,
                 "images": ""}}, ["ID"], "STRJRNPF"),
    ({"orders": {"system": ("ORDERS", "SHOP"), "journal": "SHOP/QSQJRN",
                 "images": "*AFTER"}}, ["ID"], "CHGJRNOBJ"),
    ({"orders": {"system": ("ORDERS", "SHOP"), "journal": "SHOP/QSQJRN",
                 "images": "*BOTH"}}, [], "no primary key"),
])
def test_what_cannot_be_followed_is_refused_with_its_fix(monkeypatch, tables,
                                                         key, fix):
    from migkit.engines.db2i import Db2iEngine
    monkeypatch.setattr(Db2iEngine, "_journaled",
                        lambda self, side, db: tables)
    monkeypatch.setattr(Db2iEngine, "_key", lambda self, side, db, t: key)
    with pytest.raises(SystemExit) as e:
        _db2i()._followable("src", "shop")
    assert fix in str(e.value) and "orders" in str(e.value)


def test_a_journal_keeping_minimized_entries_is_refused(monkeypatch):
    from migkit.engines.db2i import Db2iEngine
    monkeypatch.setattr(Db2iEngine, "_receivers", lambda self, side, db, j: [
        ("SHOP", "QSQJRN0001", 1, 50, "ATTACHED", "YES", "NO")])
    with pytest.raises(SystemExit) as e:
        _db2i()._ends("src", "shop", ["SHOP/QSQJRN"])
    assert "MINENTDTA(*NONE)" in str(e.value)


def test_the_starting_point_waits_for_a_transaction_still_open(monkeypatch):
    from migkit.engines.db2i import Db2iEngine
    monkeypatch.setattr(Db2iEngine, "_followable", lambda self, side, db: {
        "orders": {"system": ("ORDERS", "SHOP"), "journal": "SHOP/QSQJRN",
                   "images": "*BOTH"}})
    monkeypatch.setattr(Db2iEngine, "_receivers", lambda self, side, db, j: [
        ("SHOP", "QSQJRN0001", 1, 90, "ATTACHED", "NO", "YES")])
    eng = _db2i()
    _answering(eng, monkeypatch, lambda sql, p: [
        (40, "SC", 40), (41, "SC", 41), (45, "CM", 40)])
    # 41 began and has not ended: a copy made now cannot see it
    assert eng.change_point("src", "shop") == {"SHOP/QSQJRN": [40, []]}


def test_the_dba_is_handed_the_commands_that_let_it_be_followed(
        monkeypatch):
    from migkit import dba
    from migkit.engines.db2i import Db2iEngine
    tables = {"orders": {"system": ("ORDERS", "SHOP"), "journal": None,
                         "images": ""},
              "lines": {"system": ("LINES", "SHOP"),
                        "journal": "SHOP/QSQJRN", "images": "*AFTER"}}
    minimized = ["YES"]
    monkeypatch.setattr(Db2iEngine, "_journaled",
                        lambda self, side, db: tables)
    monkeypatch.setattr(Db2iEngine, "_receivers", lambda self, side, db, j: [
        ("SHOP", "QSQJRN0001", 1, 9, "ATTACHED", minimized[0], "NO")])
    eng = _db2i()

    def ask():
        return dba.missing(dba._db2i(eng.hop, eng, "src", "source", ["shop"],
                                     None, read=True, write=False,
                                     follow=True, native=False))
    lack = ask()
    fixes = [f for n in lack for f in n.fix]
    assert "call qsys2.qcmdexc('STRJRNPF FILE(SHOP/ORDERS) JRN(SHOP/QSQJRN)"\
           " IMAGES(*BOTH)');" in fixes
    assert any("CHGJRNOBJ OBJ((SHOP/LINES *FILE))" in f for f in fixes)
    assert any("MINENTDTA(*NONE)" in f for f in fixes)
    assert all(n.follow for n in lack)
    tables["orders"].update(journal="SHOP/QSQJRN", images="*BOTH")
    tables["lines"]["images"] = "*BOTH"
    minimized[0] = "NO"
    assert ask() == []


# ---- two IBM i systems, compared on the servers ------------------------------

def test_equal_hashes_pass_and_different_ones_are_read_row_by_row(
        monkeypatch):
    from migkit.engines.db2i import Db2iEngine
    from migkit.engines.hetero import HeteroEngine
    sums = {"src": {"orders": (3, 99), "lines": (5, 7)},
            "dst": {"orders": (3, 99), "lines": (5, 8)}}
    monkeypatch.setattr(Db2iEngine, "neutral_tables",
                        lambda self, side, db: ["lines", "orders"])
    monkeypatch.setattr(Db2iEngine, "_hash_rows",
                        lambda self, side, db, t: sums[side][t])
    read = []
    monkeypatch.setattr(HeteroEngine, "check_data",
                        lambda self, db, t=None, stream=None:
                        read.append(t) or [])
    monkeypatch.setattr(HeteroEngine, "_row_scope",
                        lambda self, db, t: (None, None))
    got = _db2i().check_data("shop")
    assert [r.scope for r in got] == ["shop.orders"]
    assert "every row's hash equal" in got[0].detail
    assert read == ["lines"]


def test_the_hash_sql_asks_for_one_number(monkeypatch):
    eng = _db2i()
    said = _answering(eng, monkeypatch, lambda s, p: [(3, 12345)])
    assert eng._hash_rows("src", "shop", "orders") == (3, 12345)
    sql = said[0][0]
    assert "hash_row(t)" in sql and "substr(hash_row(t), 1, 7)" in sql
    assert 'from "SHOP"."ORDERS" t' in sql


# ---- users and their authorities ---------------------------------------------

def test_profiles_and_object_authorities_are_compared(monkeypatch, tmp_path):
    from migkit import users
    from migkit.engines.db2i import Db2iEngine
    hop = _hop()
    hop.report_dir = lambda db=None: tmp_path
    profiles = {
        "src": [("APPUSR", "*ENABLED", "*USER", "*JOBCTL", "APPGRP", ""),
                ("OPS", "*ENABLED", "*SYSOPR", "*SAVSYS *JOBCTL", "*NONE",
                 "")],
        "dst": [("APPUSR", "*ENABLED", "*USER", "*JOBCTL", "APPGRP", "")]}
    grants = {"src": [("ORDERS", "APPUSR", "*CHANGE"),
                      ("ORDERS", "*PUBLIC", "*USE")],
              "dst": [("ORDERS", "APPUSR", "*USE"),
                      ("ORDERS", "*PUBLIC", "*USE")]}

    def rows(self, side, db, sql, args=None):
        if "qsys2.user_info" in sql:
            assert "user_creator <> '*IBM'" in sql
            return profiles[side]
        assert "qsys2.object_privileges" in sql and args == ("SHOP",)
        return grants[side]
    monkeypatch.setattr(Db2iEngine, "_rows", rows)
    out, _ = users.compare(hop, say=lambda m: None)
    assert out["missing_on_target"] == ["profile:OPS"]
    assert out["grants_missing"] == ["profile:APPUSR"]
    assert out["result"] == "gap"
    grants["dst"][0] = ("ORDERS", "APPUSR", "*CHANGE")
    profiles["dst"].append(profiles["src"][1])
    out, _ = users.compare(hop, say=lambda m: None)
    assert out["result"] == "pass", out


# ---- Db2 for z/OS -------------------------------------------------------------

def test_zos_reads_sysibm_and_says_how_deep_it_reaches(monkeypatch):
    from migkit import canon
    from migkit.engines.db2zos import Db2ZosEngine

    def answer(sql, params):
        if "sysibm.systables" in sql and "cardf" in sql:
            return [("ORDERS", 1200.0, 64), ("EMPTY", -1.0, -1)]
        if "sysibm.systables" in sql:
            assert params == ("SHOP",)
            return [("ORDERS",)]
        if "sysibm.syscolumns" in sql:
            assert "foreignkey = 'B'" in sql
            return [("ID", "INTEGER"), ("AMT", "DECIMAL(15,2)"),
                    ("RAW", "VARBINARY(8)"), ("AT", "TIMESTAMP(6)")]
        if "sysibm.sysindexes" in sql:
            assert "uniquerule = 'P'" in sql
            return [("ID",)]
        raise AssertionError(sql)
    eng = Db2ZosEngine(_hop("db2zos", database="DSNLOC1"))
    _answering(eng, monkeypatch, answer)
    assert eng.neutral_tables("src", "shop") == ["orders"]
    assert {n: canon.type_class("db2zos", t) for n, t in
            eng.neutral_columns("src", "shop", "orders")} == {
        "id": "integer", "amt": "decimal", "raw": "bytes",
        "at": "timestamp"}
    assert eng.neutral_key("src", "shop", "orders") == ["id"]
    facts = eng.table_facts("src", "shop")
    assert facts["orders"] == {"rows": 1200, "bytes": 65536, "key": None}
    assert facts["empty"]["rows"] is None
    items = eng.reach_items("src")
    assert any(i["item"] == "changes are not followed" and
               "no open reader" in i["detail"] for i in items)
    assert not any(i["item"] == "changes are not followed"
                   for i in eng.reach_items("dst"))


def test_the_capabilities_say_what_each_platform_reaches():
    from migkit import capabilities as caps
    m = caps.matrix()
    for cap in ("stream", "fence", "confirm", "delta", "users"):
        assert m["db2i"][cap] == "yes", cap
    for cap in ("stream", "fence", "confirm", "delta"):
        assert m["db2zos"][cap] == caps.NOT_APPLICABLE, cap
    said = caps.unavailable("db2zos", "stream")
    assert "licensed code" in said and "no open reader" in said
    # LUW is as it was
    assert m["db2"]["stream"] == caps.NOT_YET


# ---- the terms of IBM's components -------------------------------------------

def test_the_db2_connect_licence_is_put_in_place_once_accepted(
        tmp_path, monkeypatch):
    import ibm_db

    from migkit import tools
    from migkit.engines.db2 import db2_connect_licence
    monkeypatch.setattr(ibm_db, "__file__", str(tmp_path / "ibm_db.so"))
    assert db2_connect_licence() is None
    lic = tmp_path / "db2consv_zs.lic"
    lic.write_bytes(b"licence")
    said = []
    monkeypatch.delenv("MIGKIT_DB2_CONNECT_LICENSE", raising=False)
    assert not tools.install_ibm(tools.DB2_CONNECT, said.append)
    assert "MIGKIT_DB2_CONNECT_LICENSE" in said[0]
    monkeypatch.setenv("MIGKIT_DB2_CONNECT_LICENSE", str(lic))
    assert tools.install_ibm(tools.DB2_CONNECT, said.append)
    assert db2_connect_licence() == str(tmp_path / "clidriver" / "license"
                                        / "db2consv_zs.lic")


def test_ibms_driver_is_not_installed_without_its_terms(tmp_path,
                                                        monkeypatch):
    from migkit import tools
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("MIGKIT_ACCEPT_TERMS", raising=False)
    said = []
    got = tools.accept_terms([tools.IBMI_ODBC, tools.DB2_CONNECT], None,
                             said.append)
    assert got == {tools.IBMI_ODBC: False, tools.DB2_CONNECT: False}
    text = " ".join(said)
    assert "Mapepire" in text and tools.TERMS[tools.IBMI_ODBC].url in text
