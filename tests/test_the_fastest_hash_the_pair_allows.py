"""The fastest hash the pair allows (backlog R20.10): a table's leaves are
hashed by the way measured fastest on its own first rows on both sides,
only among the ways whose SQL equals the fold here on those rows, md5
where nothing else holds - chosen by what was timed, never by engine; the
choice is logged with the rates it stood on; and a side's range leaves are
read by as many statements at once as `sizing.Plan` says.

The server tests use the pair of `test_one_scan_leaves_on_servers` (MySQL
8.4 and PostgreSQL 16 in docker, removed after).
"""
import hashlib

import pytest

from migkit import decide, leaves, runlog
from migkit.engines import warehouse_sql


# --- this process ------------------------------------------------------------

@pytest.mark.parametrize("way", sorted(leaves.HASHES))
def test_each_way_is_its_hash_first_sixty_bits(way):
    text = "migkit\x1f2:é|0:"
    want = int(getattr(hashlib, way)(("salt" + text).encode())
               .hexdigest()[:leaves.HEX], 16)
    assert leaves.text_hash("salt", text, way) == want
    assert want < 1 << leaves.BITS
    if way != "md5":
        assert leaves.text_hash("salt", text, way) != \
            leaves.text_hash("salt", text)


def test_a_kept_spec_keeps_its_hash_and_an_old_one_is_md5():
    spec = leaves.plan(5000, "range", 1, 5000, salt="ab", way="sha256")
    assert leaves.Spec.of(spec.as_dict()) == spec
    old = {k: v for k, v in spec.as_dict().items() if k != "hash"}
    assert leaves.Spec.of(old).hash == "md5"


def test_the_hasher_hashes_by_the_spec():
    from migkit import rowtext
    md5 = leaves.Hasher(leaves.Spec("hash", 4, salt="s"), ["integer", "text"],
                        [0])
    sha = leaves.Hasher(leaves.Spec("hash", 4, salt="s", hash="sha256"),
                        ["integer", "text"], [0])
    row = [7, "x"]
    assert md5.of(row) != sha.of(row)
    _, rh, kh = sha.of(row)
    assert rh == leaves.text_hash("s", rowtext.encode(sha.texts(row)),
                                  "sha256")
    assert kh == leaves.text_hash("s", rowtext.encode(sha.texts(row)[:1]),
                                  "sha256")


def test_a_hash_a_side_has_not_is_refused_not_replaced():
    assert leaves._h("postgres", "t", "s", "sha1") is None
    assert "sha2(" in leaves._h("mysql", "t", "s", "sha256")
    assert "sha256(convert_to(" in leaves._h("postgres", "t", "s", "sha256")
    with pytest.raises(ValueError, match="no sha1"):
        warehouse_sql.salted_hash("duckdb", "x", "s", "sha1")
    assert "SHA1(" in warehouse_sql.salted_hash("bigquery", "x", "s", "sha1")
    assert "MD5(" in warehouse_sql.salted_hash("bigquery", "x", "s")


def _climb(probe, keyed=True):
    costs = decide.Costs()
    rows = probe["rows"][0]
    for way, (took, _) in probe.items():
        if way in leaves.HASHES and took is not None:
            costs.saw(way, rows, took)
    return decide.climb("t", leaves.HASH_WAYS, decide.Facts(
        {"keyed": keyed, "probe": probe}), costs=costs)


def test_the_measured_faster_way_wins_and_an_unequal_one_never_does():
    got = _climb({"md5": (2.0, None), "sha256": (1.0, None),
                  "sha1": (0.5, "sha1 in the dst side's SQL does not equal"
                                " the fold here on its first 10,000 rows"),
                  "rows": (10_000, None)})
    assert got.rung.name == "sha256"
    assert "does not equal the fold here" in got.reason
    got = _climb({"md5": (1.0, None), "sha256": (3.0, None),
                  "sha1": (None, "the src side's SQL does not hash by sha1"),
                  "rows": (10_000, None)})
    assert got.rung.name == "md5"


def test_a_table_without_a_key_takes_md5_untimed():
    got = decide.climb("t", leaves.HASH_WAYS, decide.Facts(
        {"keyed": False},
        probe=lambda: pytest.fail("a keyless table is never timed")),
        costs=decide.Costs())
    assert got.rung.name == "md5"


def test_every_hash_way_says_its_loads():
    assert all(r.loads for r in leaves.HASH_WAYS)


class _Eng:
    """A side whose server answers a scan's statements by range: each
    statement returns the leaves its key predicate holds."""
    CANON_ENGINE = "postgres"
    SQL_DIALECT = "postgres"

    def __init__(self, tree):
        self.tree, self.asked = tree, []

    def _quote_ident(self, n):
        return f'"{n}"'

    def _qualified(self, side, db, t):
        return t

    def run_rule(self, side, db, sql):
        import re
        self.asked.append(sql)
        spec = leaves.Spec("range", base=1, width=10)
        lo = re.search(r'"id" >= (-?\d+) and "id" < (-?\d+)', sql)
        out = re.search(r'"id" < (-?\d+) or "id" >= (-?\d+)', sql)
        got = []
        for b, v in self.tree.items():
            k = spec.bounds(b)[0]
            if lo and int(lo.group(1)) <= k < int(lo.group(2)):
                got.append((b,) + v)
            elif out and not (int(out.group(1)) <= k < int(out.group(2))):
                got.append((b,) + v)
            elif not lo and not out:
                got.append((b,) + v)
        return got


def _side(eng, side="src"):
    return leaves.Side(eng, side, "d", "t", [("id", "integer"),
                                             ("v", "text")], ["id"])


def test_range_leaves_are_gathered_by_as_many_statements_as_the_plan():
    tree = {b: (10, b * 3, b * 5) for b in range(0, 100)}
    tree[-2] = (1, 1, 1)        # a key below the planned range
    tree[140] = (2, 2, 2)       # and one written since above it
    eng = _Eng(tree)
    spec = leaves.Spec("range", base=1, width=10)
    got = leaves.gather(_side(eng), spec, workers=4, lohi=(1, 1000))
    assert got.leaves == tree
    assert len(eng.asked) == 5
    one = _Eng(tree)
    assert leaves.gather(_side(one), spec, workers=1,
                         lohi=(1, 1000)).leaves == tree
    assert len(one.asked) == 1


def test_the_gather_plan_is_sizing_s(monkeypatch):
    from migkit import sizing
    seen = {}

    def estimate(hop, eng, db, units=None, sides=None, **kw):
        seen.update(units=units, sides=sides)
        return sizing.Plan(3, 6, {"the work there is": (units, False)})
    monkeypatch.setattr(sizing, "estimate", estimate)
    monkeypatch.setattr(sizing, "capacity", lambda eng, side, db: {
        "cpus": 8, "side": side})

    class P:
        hop = None
    got = leaves.gather_plan(P(), "d", _side(_Eng({})), _side(_Eng({})),
                             leaves=5000)
    assert got.start == 3
    assert seen["units"] == 5
    assert seen["sides"]["dst"]["side"] == "dst"


def test_the_choice_is_logged_with_its_rates(monkeypatch):
    said = []
    monkeypatch.setattr(runlog, "event",
                        lambda name, msg="", **kw: said.append((name, msg,
                                                                kw)))

    def probe(s, spec, ways, n=leaves.PROBE_ROWS):
        took = {"md5": 0.4, "sha256": 0.2, "sha1": None}
        return 1000, {w: (took[w], None if took[w] else
                          f"the {s.side} side's SQL does not hash by {w}")
                      for w in ways}
    monkeypatch.setattr(leaves, "probe", probe)
    way, got = leaves.fastest_hash("d.t", _side(_Eng({})),
                                   _side(_Eng({}), "dst"), "salt")
    assert way == "sha256" and got.rung.name == "sha256"
    (name, msg, kw), = said
    assert name == "leaf hash"
    assert "leaves hashed by sha256" in msg and "rows/s" in msg
    assert kw["rates"] == {"md5": 2500, "sha256": 5000}
    assert "sha1" in kw["refused"]


# --- a PostgreSQL and MySQL pair ---------------------------------------------

from tests.conftest import needs_docker  # noqa: E402
from tests.test_one_scan_leaves_on_servers import (  # noqa: E402,F401
    InProcess, _hetero, _twin, servers)

MY_ROWS = """
set session cte_max_recursion_depth = 100000;
create table h (id int primary key, t varchar(40), e varchar(10),
  u varchar(60) character set utf8mb4, j json);
insert into h with recursive g(n) as (select 1 union all select n + 1
  from g where n < 1000)
  select n, if(n % 5 = 0, null, concat('r', n)), if(n % 4 = 0, '', null),
         concat('สวัสดี é 漢字 ', n), json_object('k', n, 'a', 'x')
    from g;
"""

PG_ROWS = """
create table public.h (id bigint primary key, t varchar(40),
  e varchar(10), u varchar(60), j jsonb);
insert into public.h
  select g, case when g % 5 = 0 then null else 'r' || g end,
         case when g % 4 = 0 then '' end, 'สวัสดี é 漢字 ' || g,
         jsonb_build_object('k', g, 'a', 'x')
    from generate_series(1, 1000) g;
"""


def _pair_sides(tmp_path, cols=None):
    eng = _hetero(tmp_path)
    sc, dc, _ = eng._column_plan("h", "public.h", "cx")
    if cols:
        sc = [c for c in sc if c[0] in cols]
        dc = [c for c in dc if c[0] in cols]
    return leaves.sides(eng, "cx", "h", "public.h", sc, dc, (None, None))


def _here(s):
    return leaves.Side(InProcess(s.eng), s.side, s.db, s.table, s.cols,
                       s.key)


@needs_docker
def test_the_chosen_hash_is_the_fold_here_on_both_servers(servers, tmp_path,
                                                          monkeypatch):
    _twin(MY_ROWS, PG_ROWS)
    said = []
    monkeypatch.setattr(runlog, "event",
                        lambda name, msg="", **kw: said.append((name, kw)))
    # the columns both engines render alike in SQL: NULL, empty, unicode
    a, b = _pair_sides(tmp_path, {"id", "t", "e", "u"})
    assert a.sql and b.sql
    way, got = leaves.fastest_hash("cx.h", a, b, "c0ffee")
    assert way in leaves.HASHES and got.rung.name == way
    (name, kw), = said
    assert name == "leaf hash" and kw["way"] == way and kw["rows"] == 1000
    assert {"md5", "sha256"} <= set(kw["rates"])
    assert "sha1" in kw["refused"]        # PostgreSQL has none of its own
    for w in ("md5", "sha256", way):
        spec = leaves.plan(1000, "hash", salt="c0ffee", way=w)
        ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
        assert ta.root[0] == 1000
        assert ta.leaves == leaves.scan(_here(a), spec).leaves == \
            tb.leaves, w
    spec = leaves.plan(1000, "range", 1, 1000, salt="c0ffee", way=way)
    assert leaves.gather(a, spec, 4, (1, 1000)).leaves == \
        leaves.scan(a, spec).leaves == leaves.gather(b, spec, 3,
                                                     (1, 1000)).leaves


@needs_docker
def test_a_json_column_is_hashed_as_the_fold_here_on_each_side(
        servers, tmp_path, monkeypatch):
    _twin(MY_ROWS, PG_ROWS)
    monkeypatch.setattr(runlog, "event", lambda *a, **k: None)
    a, b = _pair_sides(tmp_path)
    assert "j" in [n for n, _ in a.cols]
    way, _ = leaves.fastest_hash("cx.h", a, b, "c0ffee")
    spec = leaves.plan(1000, "hash", salt="c0ffee", way=way)
    for s in (a, b):
        got = leaves.scan(s, spec)
        assert got.root[0] == 1000
        assert got.leaves == leaves.scan(_here(s), spec).leaves, s.side


@needs_docker
def test_a_way_whose_sql_differs_from_the_fold_is_refused(servers, tmp_path,
                                                          monkeypatch):
    _twin(MY_ROWS, PG_ROWS)
    monkeypatch.setattr(runlog, "event", lambda *a, **k: None)
    a, b = _pair_sides(tmp_path, {"id", "t", "u"})
    real = leaves._h

    def wrong(engine, expr, salt, way="md5"):
        # sha256 on the target hashed over another text than the fold's
        if way == "sha256" and engine == "postgres":
            expr = f"({expr} || 'x')"
        return real(engine, expr, salt, way)
    monkeypatch.setattr(leaves, "_h", wrong)
    _, timed = leaves.probe(b, leaves.Spec("hash", salt="c0ffee"),
                            ["md5", "sha256"])
    assert timed["md5"][1] is None
    assert "does not equal the fold here" in timed["sha256"][1]
    way, _ = leaves.fastest_hash("cx.h", a, b, "c0ffee")
    assert way != "sha256"
