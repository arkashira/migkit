"""Mainframe extracts moved into PostgreSQL and proved there row by row
(backlog 34, W5).

A file of Thai accounts in cp838 with a REDEFINES picked by the record's
type and 88-level conditions, and a variable-blocked file of orders whose
lines are an OCCURS DEPENDING ON table: each moved into PostgreSQL, read a
range of whole records at a time, and compared by migkit's check - equal,
and different once one value on the target is. A packed negative zero
arrives as the zero PostgreSQL holds; decimals arrive at their scale,
never through a float. A copy stopped after its second range resumes from
the record offset it had reached.
"""
import json
import random
import subprocess
import time
from decimal import Decimal

import pytest

from migkit import copybook as cb
from migkit.config import Endpoint, Hop
from tests import copybook_corpus as C
from tests.conftest import needs_docker

pytestmark = needs_docker

PG, PORT = "migkit-test-mf-pg", 16160


def _psql(sql, db="postgres"):
    return subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG,
                           "psql", "-U", "postgres", "-d", db, "-At", "-c",
                           sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    try:
        end = time.time() + 90
        while _psql("select 1").returncode != 0:
            if time.time() > end:
                raise RuntimeError("PostgreSQL did not come up")
            time.sleep(1)
        yield PORT
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


ACCOUNTS = 2400
ORDERS = 3000


def _extract(root):
    """The two files, written by the layout the corpus proved, one value
    set to a packed negative zero by hand."""
    folder = root / "extract" / "bank"
    folder.mkdir(parents=True)
    rnd = random.Random(7)
    acct = cb.Layout(cb.parse(C.ACCOUNT)[0], "cp838", C.ACCOUNT_RULE)
    cols = [c for c, _ in acct.columns]
    thai = ["สมชาย", "กข", "ไทย", "มานี", "ชูใจ"]
    records = []
    for i in range(ACCOUNTS):
        kind = "PBC"[i % 3]
        v = dict.fromkeys(cols)
        v.update(acct_type=kind, acct_id=100000 + i,
                 owner_th=thai[i % len(thai)], score_1=i % 10,
                 score_2=(i * 7) % 10)
        if kind == "P":
            v.update(birth_date=19500101 + i % 5000, gender="MF"[i % 2])
        else:
            v.update(tax_id=f"{i:010d}", employees=rnd.randint(-999, 999))
        for k in (1, 2, 3):
            v[f"bal_amt_{k}"] = Decimal(rnd.randint(-9999999, 9999999)) \
                .scaleb(-2)
            v[f"bal_ccy_{k}"] = rnd.choice(["THB", "USD", "EUR"])
        records.append(acct.encode([v[c] for c in cols]))
    # BAL-AMT(1) of the first account as a packed -0, as IBM can store it
    records[0] = records[0][:19] + bytes.fromhex("0000000D") + records[0][23:]
    (folder / "accounts.dat").write_bytes(b"".join(records))
    (folder / "accounts.cpy").write_text(C.ACCOUNT)
    (folder / "accounts.json").write_text(json.dumps(
        {"codepage": "cp838", "redefines": C.ACCOUNT_RULE,
         "key": ["ACCT-ID"]}))
    orders = cb.Layout(cb.parse(C.ORDERS)[0])
    ocols = [c for c, _ in orders.columns]
    blocks, block = [], []
    for i in range(ORDERS):
        n = i % 6
        v = dict.fromkeys(ocols)
        v.update(order_no=i, line_count=n, note=f"N{i % 997:03d}")
        for k in range(1, n + 1):
            v[f"sku_{k}"] = f"S{k}{i % 100:02d}"
            v[f"price_{k}"] = Decimal(rnd.randint(-99999, 9999999)).scaleb(-2)
        block.append(orders.encode([v[c] for c in ocols]))
        if len(block) == 9:
            blocks.append(C.bdw(*block))
            block = []
    if block:
        blocks.append(C.bdw(*block))
    (folder / "orders.dat").write_bytes(b"".join(blocks))
    (folder / "orders.cpy").write_text(C.ORDERS)
    (folder / "orders.json").write_text(json.dumps({"recfm": "VB"}))
    return folder


def _hop(root, port, name="mf"):
    hop = Hop(name=name, engine="hetero",
              options={"source_engine": "copybook",
                       "target_engine": "postgres"},
              source=Endpoint(options={"path": str(root / "extract")}),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              databases=["bank"], db_map={"bank": name})
    (root / name).mkdir(exist_ok=True)
    hop.report_dir = lambda db=None: root / name
    return hop


def _move(hop, chunk=500):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(hop)
    ck = _Checkpoint(hop.report_dir() / "move.json")
    said = []
    eng.create_missing("bank", said.append)
    for sch, t in eng.list_move_tables("bank"):
        eng.move_table("bank", sch, t, chunk, ck, said.append)
    return eng, said


def test_extracts_move_into_postgresql_and_are_proved_equal(pg, tmp_path):
    _extract(tmp_path)
    assert _psql("create database mf").returncode == 0
    eng, said = _move(_hop(tmp_path, pg))
    data = {r.scope: r for r in eng.check_data("bank")}
    assert {s: r.status for s, r in data.items()} == {
        "bank.accounts": "ok", "bank.orders": "ok"}, \
        [r.detail for r in data.values()]
    assert f"rows {ACCOUNTS:,} and every compared column equal" in \
        data["bank.accounts"].detail
    assert f"rows {ORDERS:,}" in data["bank.orders"].detail
    counts = [r for r in eng.check_counts("bank") if r.check == "counts"]
    assert {r.status for r in counts} == {"ok"}, [r.detail for r in counts]
    # the types PostgreSQL was given, from the copybook's own numbers
    types = dict(line.split("|") for line in _psql(
        "select column_name, data_type || coalesce('(' ||"
        " numeric_precision || ',' || numeric_scale || ')', '') from"
        " information_schema.columns where table_name = 'accounts'",
        "mf").stdout.split())
    assert types["bal_amt_1"] == "numeric(7,2)", types
    assert types["is_business"] == "boolean" and types["acct_id"] == \
        "bigint(64,0)", types
    row = _psql("select acct_type, bal_amt_1, owner_th, is_personal,"
                " is_alnum, tax_id is null from accounts where acct_id ="
                " 100000", "mf").stdout.strip()
    # the packed -0 is the zero PostgreSQL holds, at its scale
    assert row == "P|0.00|สมชาย|t|t|t", row
    biz = _psql("select tax_id, employees is not null, birth_date is null"
                " from accounts where acct_id = 100001", "mf").stdout.strip()
    assert biz == "0000000001|t|t", biz
    lines = _psql("select line_count, sku_5, price_5 is null, note from"
                  " orders where order_no in (0, 5) order by order_no",
                  "mf").stdout.split()
    assert lines[0] == "0||t|N000" and lines[1].startswith("5|S505|f|N005"), \
        lines
    # the deep check reads every record of the source's files too
    deep = [r for r in eng.check_deep("bank")
            if r.scope == "bank source files"]
    assert [r.status for r in deep] == ["ok"], [r.detail for r in deep]
    # one value different on the target is a difference, found by key
    assert _psql("update accounts set bal_amt_2 = bal_amt_2 + 0.01 where"
                 " acct_id = 100123", "mf").returncode == 0
    data = {r.scope: r for r in eng.check_data("bank")}
    assert data["bank.accounts"].status == "diff"
    assert "100123" in data["bank.accounts"].detail, \
        data["bank.accounts"].detail
    assert data["bank.orders"].status == "ok"


def test_a_copy_stopped_part_way_resumes_at_its_record_offset(
        pg, tmp_path, monkeypatch):
    from migkit import failpoint
    folder = _extract(tmp_path)
    (folder / "accounts.dat").unlink()
    assert _psql("create database mfresume").returncode == 0
    hop = _hop(tmp_path, pg, "mfresume")
    monkeypatch.setenv("MIGKIT_FAILPOINT", "span.committed:3:raise")
    failpoint._counts.clear()
    with pytest.raises(RuntimeError):
        _move(hop)
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    failpoint._counts.clear()
    state = json.loads((hop.report_dir() / "move.json").read_text())
    entry = next(v for k, v in state.items() if k.endswith("orders"))
    assert len(entry["spans_done"]) == 2 and not entry.get("done"), entry
    eng, said = _move(hop)
    # the third range committed before the stop: counted in, not copied
    # again, and the rest copied after it
    copied = [s for s in said if "spans copied" in s]
    assert copied and copied[0].startswith(
        f"bank.orders: 4 of {len(entry['spans'])}"), said
    data = {r.scope: r.status for r in eng.check_data("bank")}
    assert data == {"bank.orders": "ok"}, data
    assert _psql("select count(*) from orders", "mfresume").stdout.strip() \
        == str(ORDERS)
