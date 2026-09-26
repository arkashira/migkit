"""How much of a row each comparison path actually compares.

That a type with no agreed rendering is refused rather than guessed is
documented and deliberate. What was never written down is **how big that
refusal is**, and the two paths are not the same size.

Measured on one PostgreSQL table of 37 columns carrying the types a real
schema carries - `interval`, `enum`, `hstore`, `tsvector`, `bit`, ranges,
the geometric types - with the source and target identical except that the
target's exotic columns were changed:

    same-engine (pg -> pg)      data: DIFF   every change caught
    cross-engine (_comparable_columns, the hetero path)
                                compared 20 of 37, 17 refused

The same-engine path hashes the whole row inside the server, so nothing is
outside it. The cross-engine path has to render both sides into text two
different engines agree on, and for seventeen of these types no such
rendering exists - so it declines, and says which columns it declined.

Declining is right: comparing two renderings nobody checked agree is worse
than not comparing. But the flagship leg - any-source to any-target -
therefore verifies fewer columns than the leg between two of the same
engine, and `enum` and `interval` are in the gap, which is not a corner
of the type system.

These tests pin the size, the naming, and the asymmetry, so the number
cannot drift without somebody noticing.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15541, 15542
NAMES = {SRC: "migkit-test-cmp-src", DST: "migkit-test-cmp-dst"}

WIDE = """
create extension if not exists hstore;
drop table if exists wide;
drop type if exists mood;
create type mood as enum ('a','b');
create table wide (
  id int primary key, t text, n numeric(10,2), i bigint, b boolean,
  ts timestamp, tz timestamptz, d date, tm time, iv interval,
  j json, jb jsonb, u uuid, by bytea, arr int[], tarr text[],
  ip inet, cidr_ cidr, mac macaddr, m money, bits bit(8), vbits varbit,
  pt point, box_ box, circle_ circle, ln lseg, poly polygon,
  rng int4range, mrng int4multirange, x xml, h hstore, e mood,
  tsv tsvector, tsq tsquery, oid_ oid, txid txid_snapshot, pgl pg_lsn
);
insert into wide (id,t,n,iv,e,tsv,bits,pt,rng,h) values
  (1,'x',1.00,'1 day','a','cat'::tsvector,B'10101010','(1,2)','[1,5)','k=>v');
"""

#: Changed on the target only, and every one of them is a type the
#: cross-engine path refuses. If the same-engine path missed these it would
#: be missing them silently.
ONLY_REFUSED_TYPES = """
update wide set iv='999 days', e='b', tsv='dog'::tsvector,
  bits=B'00000001', pt='(9,9)', rng='[7,9)', h='k=>DIFFERENT'
where id=1;
"""


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", "-i", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


@pytest.fixture(scope="module")
def cmp_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
        got = q(port, WIDE)
        assert got.returncode == 0, got.stderr
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _engine(pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="cmp", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_the_table_really_carries_all_of_them(cmp_pair):
    """The control. A shorter table would make the counts below mean
    something else."""
    n = q(cmp_pair["src"], "select count(*) from information_schema.columns"
                           " where table_name='wide'").stdout.strip()
    assert n == "37", n


def test_same_engine_compares_the_whole_row(cmp_pair, tmp_path):
    """The good half, and the reason the gap below is specific to the
    cross-engine path rather than to migkit. Only refused-type columns are
    changed, and the checksum still catches them - it is computed over the
    whole row inside the server, so nothing is outside it."""
    q(cmp_pair["dst"], ONLY_REFUSED_TYPES)
    try:
        got = _engine(cmp_pair, tmp_path).check_data("postgres")[0]
        assert got.status == "diff", got.detail
        assert "public.wide" in got.detail, got.detail
    finally:
        q(cmp_pair["dst"], "update wide set iv='1 day', e='a',"
                           " tsv='cat'::tsvector, bits=B'10101010',"
                           " pt='(1,2)', rng='[1,5)', h='k=>v' where id=1;")


def _elsewhere(eng, monkeypatch):
    """The same catalogue read as if the other side were another engine:
    what a pair across engines is left with from PostgreSQL's side. (The
    same engine on both sides compares every column by its own text now;
    the next test.)"""
    from migkit import canon
    monkeypatch.setitem(canon.TYPES, "elsewhere", canon.TYPES["postgres"])
    monkeypatch.setitem(canon.BUILDERS, "elsewhere",
                        canon.BUILDERS["postgres"])

    class Elsewhere(type(eng)):
        CANON_ENGINE = "elsewhere"
    return Elsewhere(eng.hop)


def test_the_same_engine_on_both_sides_compares_all_of_them(cmp_pair,
                                                            tmp_path):
    """A pair of one engine - a column mapping on a PostgreSQL hop goes
    through the pair - compares a type with no shared rendering by the
    engine's own text of it, so nothing is left out."""
    eng = _engine(cmp_pair, tmp_path)
    src, dst, notes = eng._comparable_columns("postgres", eng, "public.wide",
                                              eng, "public.wide")
    assert notes == [], notes
    assert len(src) == len(dst) == 37, (len(src), len(dst))


def test_the_cross_engine_path_refuses_seventeen_of_them(cmp_pair, tmp_path,
                                                         monkeypatch):
    """The size of the refusal, which the docs described without measuring.

    Not an exact-number assertion on the excluded list - a new canonical
    rendering should not break this test, it should move the number - but
    the four that matter most in real schemas are named, because they are
    the argument for closing the gap.
    """
    eng = _engine(cmp_pair, tmp_path)
    src, dst, notes = eng._comparable_columns(
        "postgres", eng, "public.wide", _elsewhere(eng, monkeypatch),
        "public.wide")
    assert len(src) == len(dst), (len(src), len(dst))
    assert len(src) + len(notes) == 37, (len(src), len(notes))
    # the honest part: every refusal is named, none is dropped quietly
    assert len(notes) == len({n.split(":")[0] for n in notes}), notes
    for column in ("iv",):
        assert any(n.startswith(f"{column}:") for n in notes), (column, notes)
        hit = [n for n in notes if n.startswith(f"{column}:")][0]
        assert "no canonical rendering" in hit, hit
    # an enum is compared as the text of its label now
    # (`test_enums_and_domains_across_engines.py`), and a key/value set as
    # a JSON object and a text search vector as its text
    # (`test_hstore_and_tsvector_across_engines.py`), which moved the number
    compared = {n: cls for n, cls in src}
    assert compared.get("e") and compared.get("tsv") == "text", src
    assert compared.get("h") == "json", src


def test_a_refused_column_is_not_quietly_dropped(cmp_pair, tmp_path,
                                                monkeypatch):
    """The failure this design exists to avoid: a column that is neither
    compared nor mentioned. Every name that is not in the compared list
    must appear in the notes."""
    eng = _engine(cmp_pair, tmp_path)
    src, _, notes = eng._comparable_columns(
        "postgres", eng, "public.wide", _elsewhere(eng, monkeypatch),
        "public.wide")
    compared = {n for n, _ in src}
    named = {n.split(":")[0] for n in notes}
    declared = set(q(cmp_pair["src"],
                     "select column_name from information_schema.columns"
                     " where table_name='wide'").stdout.split())
    assert declared - compared - named == set(), declared - compared - named


def test_an_engine_without_deep_checks_skips_rather_than_passes(tmp_path):
    """"Nothing to check" and "I have no checks" are different answers, and
    only one of them may look like a clean bill of health. sqlite, generic
    and hetero inherit this."""
    from migkit.engines.base import Engine
    hop = Hop(name="x", engine="sqlite",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    got = Engine(hop).check_deep("x")
    assert len(got) == 1, got
    assert got[0].status == "skip", got[0]
    assert got[0].status != "ok"
    assert "no deep checks" in got[0].detail, got[0].detail
