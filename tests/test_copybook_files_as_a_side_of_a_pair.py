"""Files described by COBOL copybooks as a side of a pair (backlog 34, W5),
held without a server: the file is the database.

A copy between two directories writes records the same bytes they were
read as; the check compares them through the renderer every engine
without a server shares, and a changed byte on the target is a
difference. A file is read by ranges of whole records - the ranges' folds
add up to the whole table's, in one process or ten - and a value that is
not one stops the read, or lands as NULL and is listed, as the table
says. A file holding several record layouts is several tables.
"""
import json
import shutil

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from migkit.engines import copybook as E
from tests import copybook_corpus as C


def _files(root, orders=None):
    src, dst = root / "src" / "bank", root / "dst" / "bank"
    src.mkdir(parents=True)
    dst.mkdir(parents=True)
    (src / "account.cpy").write_text(C.ACCOUNT)
    (src / "account.dat").write_bytes(b"".join(r for r, _ in
                                               C.ACCOUNT_RECORDS))
    (src / "account.json").write_text(json.dumps(
        {"codepage": "cp838", "redefines": C.ACCOUNT_RULE,
         "key": ["ACCT-ID"]}))
    (src / "orders.cpy").write_text(C.ORDERS)
    (src / "orders.dat").write_bytes(orders if orders is not None else
                                     b"".join(C.rdw(r) for r, _ in
                                              C.ORDER_RECORDS * 40))
    (src / "orders.json").write_text(json.dumps({"recfm": "V"}))
    for n in ("account.cpy", "account.json", "orders.cpy", "orders.json"):
        shutil.copy(src / n, dst / n)
    return src, dst


def _engine(root, **src_options):
    hop = Hop(name="cb", engine="copybook",
              source=Endpoint(options={"path": str(root / "src"),
                                       **src_options}),
              target=Endpoint(options={"path": str(root / "dst")}),
              databases=["bank"])
    hop.report_dir = lambda db=None: root
    return E.CopybookEngine(hop)


def _cols(eng, table, side="src"):
    return [(n, canon.type_class("copybook", t))
            for n, t in eng.neutral_columns(side, "bank", table)]


def _move(eng, root, chunk=7):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(root / "move.json")
    said = []
    for sch, t in eng.list_move_tables("bank"):
        eng.move_table("bank", sch, t, chunk, ck, said.append)
    return said


def test_a_copy_between_directories_is_the_same_records(tmp_path):
    src, dst = _files(tmp_path)
    eng = _engine(tmp_path)
    said = _move(eng, tmp_path)
    assert any("spans copied" in s for s in said), said
    data = {r.scope: r.status for r in eng.check_data("bank")}
    assert data == {"bank.account": "ok", "bank.orders": "ok"}, data
    assert (dst / "orders.dat").read_bytes() == \
        (src / "orders.dat").read_bytes()
    assert (dst / "account.dat").read_bytes() == \
        (src / "account.dat").read_bytes()
    # one byte of a price changed on the target is a difference
    raw = bytearray((dst / "orders.dat").read_bytes())
    at = raw.index("A001".encode("cp037")) + 4
    raw[at + 1] ^= 0x10
    (dst / "orders.dat").write_bytes(bytes(raw))
    eng = _engine(tmp_path)
    got = {r.scope: r for r in eng.check_data("bank")}
    assert got["bank.orders"].status == "diff", got["bank.orders"].detail
    assert got["bank.account"].status == "ok"


def test_the_ranges_add_up_to_the_whole_in_one_process_or_many(
        tmp_path, monkeypatch):
    _files(tmp_path)
    eng = _engine(tmp_path)
    cols = _cols(eng, "orders")
    whole = eng.neutral_digest("src", "bank", "orders", cols)
    assert whole[0] == 120
    spans = eng.position_spans("src", "bank", "orders", 25)
    assert len(spans) == 5 and spans[0][0] == 0
    parts = [eng.neutral_digest("src", "bank", "orders", cols, where=w)
             for _, w in spans]
    assert sum(n for n, _ in parts) == 120
    assert str(sum(int(t) for _, t in parts)) == whole[1]
    monkeypatch.setattr(E, "PARALLEL_BYTES", 1)
    assert len(eng._fold_parts("src", "bank", "orders", 0, None)) > 1
    assert eng.neutral_digest("src", "bank", "orders", cols) == whole
    # and a different file is a different number, whichever way it is read
    raw = (tmp_path / "src" / "bank" / "orders.dat").read_bytes()
    (tmp_path / "src" / "bank" / "orders.dat").write_bytes(
        raw.replace("MAX ".encode("cp037"), "MAY ".encode("cp037"), 1))
    assert _engine(tmp_path).neutral_digest("src", "bank", "orders",
                                            cols) != whole


def test_a_filter_that_is_not_a_range_of_records_is_refused(tmp_path):
    _files(tmp_path)
    eng = _engine(tmp_path)
    with pytest.raises(SystemExit) as e:
        list(eng.neutral_batches("src", "bank", "orders",
                                 _cols(eng, "orders"), where="order_no > 1"))
    assert "row filter is SQL" in str(e.value)


def _bad_orders():
    rows = [r for r, _ in C.ORDER_RECORDS]
    bad = bytearray(rows[1])
    bad[11:15] = bytes.fromhex("0001A00C")         # PRICE of line 1
    return b"".join(C.rdw(r) for r in (rows[0], bytes(bad), rows[2]))


def test_a_value_that_is_not_one_stops_the_read_and_says_where(tmp_path):
    _files(tmp_path, orders=_bad_orders())
    eng = _engine(tmp_path)
    with pytest.raises(SystemExit) as e:
        eng.neutral_digest("src", "bank", "orders", _cols(eng, "orders"))
    said = str(e.value)
    assert "record at byte 15 of orders.dat" in said, said
    assert "price_1" in said and "0001A00C" in said and \
        "unreadable: null" in said, said


def test_or_lands_as_null_and_every_one_is_listed(tmp_path):
    _files(tmp_path, orders=_bad_orders())
    eng = _engine(tmp_path, unreadable="null")
    rows = [r for b in eng.neutral_batches("src", "bank", "orders",
                                           _cols(eng, "orders")) for r in b]
    names = [n for n, _ in _cols(eng, "orders")]
    assert [r[names.index("price_1")] for r in rows] == [None, None,
                                                          rows[2][names.index(
                                                              "price_1")]]
    assert rows[1][names.index("sku_1")] == "A001"
    deep = {r.scope: r for r in eng.check_deep("bank")}
    assert deep["bank source files"].status == "diff", deep
    assert "1 values that are not one - first at byte 15" in \
        deep["bank source files"].detail
    listed = (tmp_path / "unreadable-source-orders.txt").read_text()
    assert listed.startswith("15\tprice_1\t0001A00C\t")


def test_a_file_cut_short_is_a_deep_finding(tmp_path):
    data = b"".join(C.rdw(r) for r, _ in C.ORDER_RECORDS)
    _files(tmp_path, orders=data[:-5])
    deep = {r.scope: r for r in _engine(tmp_path).check_deep("bank")}
    assert deep["bank source files"].status == "diff"
    assert "file ends after" in deep["bank source files"].detail


SEGMENTED = """\
       01  HEADER-REC.
           05  REC-TYPE      PIC X.
           05  BATCH-NO      PIC 9(4).
           05  FILLER        PIC X(5).
       01  DETAIL-REC.
           05  REC-TYPE      PIC X.
           05  ITEM          PIC X(4).
           05  QTY           PIC S9(5) COMP-3.
           05  FILLER        PIC X(2).
"""


def test_a_file_of_several_layouts_is_several_tables(tmp_path):
    folder = tmp_path / "src" / "bank"
    folder.mkdir(parents=True)
    (folder / "SALES.cpy").write_text(SEGMENTED)
    e = "cp037"
    head = "H".encode(e) + bytes.fromhex("F0F0F0F7") + b"\x40" * 5
    det = [("D" + name).encode(e) + bytes.fromhex(q) + b"\x40" * 2
           for name, q in (("AAAA", "00012C"), ("BBBB", "00003D"))]
    other = "X".encode(e) + b"\x40" * 9
    (folder / "SALES.DAT").write_bytes(head + det[0] + other + det[1])
    tables = {
        "batches": {"file": "SALES.DAT", "copybook": "SALES.cpy",
                    "record": "HEADER-REC",
                    "segment": {"field": "REC-TYPE", "value": "H"}},
        "items": {"file": "SALES.DAT", "copybook": "SALES.cpy",
                  "record": "DETAIL-REC",
                  "segment": {"field": "REC-TYPE", "value": "D"}}}
    eng = _engine(tmp_path, tables=tables)
    assert eng.neutral_tables("src", "bank") == ["batches", "items"]
    items = [r for b in eng.neutral_batches("src", "bank", "items",
                                            _cols(eng, "items")) for r in b]
    names = [n for n, _ in _cols(eng, "items")]
    assert [(r[names.index("item")], r[names.index("qty")])
            for r in items] == [("AAAA", 12), ("BBBB", -3)]
    assert eng.neutral_digest("src", "bank", "batches",
                              _cols(eng, "batches"))[0] == 1
    deep = {r.scope: r for r in eng.check_deep("bank")}
    assert deep["bank source files"].status == "warn"
    assert "1 records no table's segment claims" in \
        deep["bank source files"].detail


def test_a_keyed_batch_written_again_replaces_itself(tmp_path):
    src, dst = _files(tmp_path)
    eng = _engine(tmp_path)
    cols = _cols(eng, "account")
    rows = [r for b in eng.neutral_batches("src", "bank", "account", cols)
            for r in b]
    eng.neutral_create("dst", "bank", "account", [(n, c, ())
                                                  for n, c in cols])
    for _ in range(3):
        eng.neutral_write("dst", "bank", "account", cols, rows[:1])
    eng.neutral_write("dst", "bank", "account", cols, rows[1:])
    assert (dst / "account.dat").read_bytes() == \
        (src / "account.dat").read_bytes()
    assert eng.neutral_empty("dst", "bank", "account") == 2
    assert (dst / "account.dat").read_bytes() == b""


def test_a_target_is_made_only_from_its_own_copybook(tmp_path):
    _files(tmp_path)
    eng = _engine(tmp_path)
    with pytest.raises(SystemExit) as e:
        eng.neutral_create("dst", "bank", "orders",
                           [("order_no", "integer", ()),
                            ("shipped", "boolean", ())])
    assert "no field for shipped" in str(e.value)
    (tmp_path / "dst" / "bank" / "orders.dat").write_bytes(b"")
    with pytest.raises(SystemExit) as e:
        eng.neutral_create("dst", "bank", "orders",
                           [("order_no", "integer", ())])
    assert "already exists" in str(e.value)


def test_the_directory_is_its_identity_and_an_address_names_it(tmp_path):
    """The interlock's question - is the target the source itself - is
    answered by the directory, however it is reached."""
    from migkit import addresses
    _files(tmp_path)
    eng = _engine(tmp_path)
    src = eng.server_identity("src", "bank")
    assert src["id"] and src["bytes"] > 0
    assert eng.server_identity("dst", "bank")["id"] != src["id"]
    (tmp_path / "alias").symlink_to(tmp_path / "src")
    other = _engine(tmp_path)
    other.hop.target.options["path"] = str(tmp_path / "alias")
    assert other.server_identity("dst", "bank")["id"] == src["id"]
    shutil.rmtree(tmp_path / "dst")
    assert _engine(tmp_path).server_identity("dst", "bank")["absent"]
    assert addresses.parse("copybook:///data/extract?codepage=cp838") == (
        "copybook", {"path": "/data/extract", "codepage": "cp838"}, [])


def test_the_files_copied_as_they_are(tmp_path):
    src, dst = _files(tmp_path)
    eng = _engine(tmp_path)
    steps = eng.native_bulk("bank", ["orders", "account"], True, None)
    assert len(steps) == 2
    assert (dst / "orders.dat").read_bytes() == \
        (src / "orders.dat").read_bytes()
    # described differently on the target: not copied as bytes
    (dst / "orders.json").write_text(json.dumps({"recfm": "VB"}))
    with pytest.raises(SystemExit) as e:
        _engine(tmp_path).native_bulk("bank", ["orders"], True, None)
    assert "describes it differently" in str(e.value)
