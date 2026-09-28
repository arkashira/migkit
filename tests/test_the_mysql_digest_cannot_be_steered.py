"""Two different MySQL tables cannot be made to sum alike.

The digest summed two 32-bit lanes per row: `crc32` of the row and the
first 32 bits of its md5. A CRC is linear, so strings of one length with the
same CRC are found by solving 32 equations, and among them two whose md5
starts alike by trying ~2^16 - a fraction of a second here. Measured before
the fix: a source holding (1, x) and a target holding (1, y), x and y such a
pair, were reported `rows 1==1, checksum ...==...` - identical - and would
have been on every run after, since nothing in the sum changed between runs.
Now one lane of 64 bits of md5, behind a salt each run draws: the same pair
is found different, and the sums of one table differ from run to run while
both sides of one run agree.

And a difference no row holds is no longer "checksum flicker settled": the
sums are asked again, and a difference that stays is said.
"""
import hashlib
import socket
import subprocess
import time
import zlib

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-f0v-mysql", 16052


def _mysql(sql, db="mysql"):
    r = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                        "-N", db, "-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        for _ in range(90):
            r = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                "-e", "select 1"], capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("MySQL never accepted a connection")
        with socket.socket() as s:
            s.settimeout(2)
            assert s.connect_ex(("127.0.0.1", PORT)) == 0
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine(tmp_path, slice_rows=None):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="f0v", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_new"})
    hop.report_dir = lambda db=None: tmp_path
    if slice_rows:
        hop.slice = slice_rows
    return MySQLEngine(hop)


def _fresh(table="create table t (id int primary key, v varchar(64))"):
    for d in ("shop", "shop_new"):
        _mysql(f"drop database if exists {d}; create database {d};"
               f" use {d}; {table}")


def _twins(text, at, width=16):
    """Two strings the length of `text`, differing from it only in the 16
    bytes from `at` (which must be '@': only their low four bits change,
    so every one stays a letter), with the same crc32 and md5 starting
    with the same eight hex digits - what the old sum's two lanes read."""
    n = len(text)
    zero = zlib.crc32(bytes(n))
    bits, pivots, kernel = [], [], []
    for p in range(at, at + width):
        for b in range(4):
            e = bytearray(n)
            e[p] = 1 << b
            img, combo = zlib.crc32(bytes(e)) ^ zero, 1 << len(bits)
            bits.append((p, b))
            for pimg, pcombo, pbit in pivots:
                if img & pbit:
                    img ^= pimg
                    combo ^= pcombo
            if img:
                pivots.append((img, combo, img & -img))
            else:
                kernel.append(combo)
    steps = []
    for combo in kernel:
        d = bytearray(n)
        for i, (p, b) in enumerate(bits):
            if combo >> i & 1:
                d[p] ^= 1 << b
        steps.append([(p, v) for p, v in enumerate(d) if v])
    seen, cur = {}, bytearray(text)
    for i in range(1, 1 << min(len(steps), 22)):
        for p, v in steps[(i & -i).bit_length() - 1]:
            cur[p] ^= v
        s = bytes(cur)
        h = hashlib.md5(s).hexdigest()[:8]
        if seen.get(h, s) != s:
            return seen[h], s
        seen[h] = s
    raise AssertionError("no pair found")


def test_two_rows_the_old_lanes_read_alike_differ(server, tmp_path):
    _fresh()
    eng = _engine(tmp_path)
    base = "@" * 48
    _mysql(f"insert into t values (1, '{base}')", "shop")
    expr = eng._row_expr("shop", "t")
    text = eng._q("src", f"select {expr} from `shop`.`t`")[0][0].encode()
    x, y = _twins(text, text.index(base.encode()))
    at = text.index(base.encode())
    vx, vy = (s[at:at + 48].decode() for s in (x, y))
    _mysql(f"update t set v = '{vx}'", "shop")
    _mysql(f"insert into t values (1, '{vy}')", "shop_new")
    # the premise, on the server: the two lanes the sum was made of agree
    lanes = [_mysql(f"select crc32({expr}), substring(md5({expr}), 1, 8)"
                    " from t", d) for d in ("shop", "shop_new")]
    assert vx != vy and lanes[0] == lanes[1], (vx, vy, lanes)
    r = [x for x in eng.check_data("shop") if x.scope == "shop.t"]
    assert r and r[0].status == "diff", [(x.scope, x.detail) for x in r]
    assert "changed=1" in r[0].detail, r[0].detail


def test_equal_tables_still_check_out(server, tmp_path):
    _fresh()
    for d in ("shop", "shop_new"):
        _mysql("insert into t values (1, 'a|b'), (2, NULL), (3, ''),"
               " (4, '1:x')", d)
    r = [x for x in _engine(tmp_path).check_data("shop")
         if x.scope == "shop.t"]
    assert r and r[0].status == "ok", [(x.scope, x.detail) for x in r]


def test_each_run_sums_afresh_and_both_sides_alike(server, tmp_path):
    _fresh()
    for d in ("shop", "shop_new"):
        _mysql("insert into t values (1, 'a'), (2, 'b')", d)
    one, two = _engine(tmp_path), _engine(tmp_path)
    expr = one._row_expr("shop", "t")
    a1, b1 = (one._checksum(s, "shop", "t", expr) for s in ("src", "dst"))
    a2, b2 = (two._checksum(s, "shop", "t", expr) for s in ("src", "dst"))
    assert a1 == b1 and a2 == b2, (a1, b1, a2, b2)
    assert a1[1] != a2[1], "two runs summed with the same salt"


def test_a_resumed_check_sums_with_the_salt_it_began_with(server, tmp_path,
                                                          monkeypatch):
    import json

    from migkit.engines.mysql import MySQLEngine
    _fresh()
    for d in ("shop", "shop_new"):
        _mysql("insert into t (id, v) with recursive s(i) as (select 1"
               " union all select i + 1 from s where i < 400)"
               " select i, concat('v', i) from s", d)
    import re
    real = MySQLEngine._checksum

    def dies(self, side, db, t, expr, where="", *a, **kw):
        # the connection is lost at the range starting from 200
        low = re.search(r">= (\d+)", where)
        if low and int(low.group(1)) >= 200:
            raise RuntimeError("connection reset")
        return real(self, side, db, t, expr, where, *a, **kw)
    monkeypatch.setattr(MySQLEngine, "_checksum", dies)
    first = _engine(tmp_path, slice_rows=50)
    with pytest.raises(RuntimeError):
        first.check_data("shop")
    left = json.loads((tmp_path / "checkpoint.json").read_text())
    entry = left["tables"]["shop.t"]
    assert entry["done"] and entry["salt"] == first.digest_salt(), entry
    monkeypatch.setattr(MySQLEngine, "_checksum", real)
    again = _engine(tmp_path, slice_rows=50)
    assert again.digest_salt() != first.digest_salt()
    r = [x for x in again.check_data("shop") if x.scope == "shop.t"][0]
    assert r.status == "ok" and "resumed" in r.detail, r.detail
    # the total of the resumed ranges is the whole table's under one salt
    whole = again._checksum("src", "shop", "t", again._row_expr("shop", "t"),
                            salt=first.digest_salt())
    assert f"checksum {int(whole[1]):x}==" in r.detail, (r.detail, whole)


def _lying_target(monkeypatch, times):
    """The target's sums read one higher for the first `times` asks - a
    difference that no row holds."""
    from migkit.engines.mysql import MySQLEngine
    real = MySQLEngine._checksum
    asked = {"n": 0}

    def checksum(self, side, *a, **kw):
        got = real(self, side, *a, **kw)
        if side == "dst":
            asked["n"] += 1
            if asked["n"] <= times:
                return (got[0], int(got[1]) + 1) + tuple(got[2:])
        return got
    monkeypatch.setattr(MySQLEngine, "_checksum", checksum)
    return asked


def test_a_difference_no_row_holds_is_not_ok(server, tmp_path, monkeypatch):
    _fresh()
    for d in ("shop", "shop_new"):
        _mysql("insert into t values (1, 'a'), (2, 'b')", d)
    asked = _lying_target(monkeypatch, times=10 ** 6)
    r = [x for x in _engine(tmp_path).check_data("shop")
         if x.scope == "shop.t"][0]
    assert asked["n"] >= 2, "the sums were not asked again"
    assert r.status == "diff", r.detail
    assert r.detail.startswith("differs, not localized"), r.detail


def test_a_difference_that_settles_is_ok_and_says_so(server, tmp_path,
                                                      monkeypatch):
    _fresh()
    for d in ("shop", "shop_new"):
        _mysql("insert into t values (1, 'a'), (2, 'b')", d)
    _lying_target(monkeypatch, times=1)
    r = [x for x in _engine(tmp_path).check_data("shop")
         if x.scope == "shop.t"][0]
    assert r.status == "ok", r.detail
    assert "equal asked again" in r.detail, r.detail
