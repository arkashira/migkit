"""A column's collation held to the source's by the column's own values
(leapfrog research P13, backlog CV.P13).

MySQL's utf8mb4_0900_ai_ci takes 'Bob' and 'bob', and 'Emile' and
'Émile', for one; a PostgreSQL column under the database's deterministic
collation keeps each apart - a unique key that held on the source takes
both rows on the target, and a GROUP BY counts twice. Here the check
finds that from the column's own values, asked of the two sandbox
servers: it names the column, the kinds of difference and the values
(masked where the hop asks), and names a collation of the target that
agrees. A column whose values hold none of the kinds its collations part
on is not the reason for the verdict.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-coll-my", "migkit-test-coll-pg"
SB_MY, SB_PG = "migkit-test-coll-sbmy", "migkit-test-coll-sbpg"
MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT = 16294, 16295, 16296, 16297
#: the sandbox's PostgreSQL is built from this major's image, with ICU
PG_MAJOR = 17
#: small servers: four of them run at once here
MY_SMALL = ["--log-bin-trust-function-creators=1", "--performance-schema=OFF",
            "--innodb-buffer-pool-size=16M"]


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, db="cx"):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d", db,
              "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, db="cx"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database=db, charset="utf8mb4",
                           autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


def _up_mysql(name, port):
    _sh("docker", "rm", "-f", "-v", name)
    _sh("docker", "run", "-d", "--name", name, "-e",
        "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
        "mysql:8.4", *MY_SMALL)


def _up_pg(name, port, image):
    _sh("docker", "rm", "-f", "-v", name)
    _sh("docker", "run", "-d", "--name", name, "-e",
        "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)


def _wait_mysql(name):
    for _ in range(90):
        if _sh("docker", "exec", name, "mysql", "-uroot", "-ptest",
               "-h127.0.0.1", "--protocol=tcp", "-e",
               "select 1").returncode == 0:
            return
        time.sleep(2)
    raise AssertionError(f"{name} did not come up")


def _wait_pg(name):
    for _ in range(60):
        if _sh("docker", "exec", name, "pg_isready", "-U",
               "postgres").returncode == 0 and _sh(
                "docker", "exec", name, "psql", "-U", "postgres", "-c",
                "select 1").returncode == 0:
            return
        time.sleep(1)
    raise AssertionError(f"{name} did not come up")


def _sandbox_image():
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(PG_MAJOR)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        built = _sh("docker", "build", "-t", tag, "-",
                    input=PG_DOCKERFILE.format(v=PG_MAJOR), timeout=900)
        assert built.returncode == 0, built.stderr[-400:]
    return tag


#: `name` under a collation blind to case and accents; `code` under a
#: binary one that pads, holding only lowercase ASCII codes - which sort
#: alike under any collation, so the kinds its collations part on (case,
#: trailing spaces) are held by none of its values
SOURCE = [
    "create table people (id int primary key,"
    " name varchar(40) character set utf8mb4 collate utf8mb4_0900_ai_ci,"
    " code varchar(10) character set utf8mb4 collate utf8mb4_bin)",
    "insert into people values (1, 'Bob', 'ab'), (2, 'bob', 'ac'),"
    " (3, 'Émile', 'ba'), (4, 'Emile', 'bb'), (5, 'zoe', 'zz')",
]


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    image = _sandbox_image()
    _up_mysql(MY, MY_PORT)
    _up_mysql(SB_MY, SB_MY_PORT)
    _up_pg(PG, PG_PORT, f"postgres:{PG_MAJOR}")
    _up_pg(SB_PG, SB_PG_PORT, image)
    try:
        for n in (MY, SB_MY):
            _wait_mysql(n)
        for n in (PG, SB_PG):
            _wait_pg(n)
        for port in (MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _my("create database cx", db=None)
        _my(*SOURCE)
        _pg("create database cx", db="postgres")
        # the database's own collation: deterministic
        _pg("create table people (id integer primary key,"
            " name varchar(40), code varchar(10));")
        yield
    finally:
        for n in (MY, PG, SB_MY, SB_PG):
            _sh("docker", "rm", "-f", "-v", n)


def _hop(tmp_path, **more):
    options = {"source_engine": "mysql", "target_engine": "postgres",
               "proof_sandbox": {
                   "source": {"host": "127.0.0.1", "port": SB_MY_PORT,
                              "user": "root", "password": "test"},
                   "target": {"host": "127.0.0.1", "port": SB_PG_PORT,
                              "user": "postgres", "password": "test"}},
               **more}
    hop = Hop(name="coll", engine="hetero", options=options,
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _proved(tmp_path, **more):
    from migkit.engines.hetero import HeteroEngine
    from migkit.storedcode import collations
    from migkit.storedcode.bench import Bench
    from migkit.storedcode.prove import Proof
    p = Proof(HeteroEngine(_hop(tmp_path, **more)), "cx")
    p.open(True)
    try:
        assert p.sandbox is not None, p.why_not
        bench = Bench(p)
        try:
            got = collations.prove(bench)
        finally:
            bench.close()
    finally:
        p.close()
    assert len(got) == 1, got
    return got[0]


def _column(detail, said):
    """The verdict line of one column in the database's detail."""
    got = [part for part in detail.split("; ")
           if part.startswith(said + ": ")]
    assert len(got) == 1, detail
    return got[0]


def test_values_the_source_takes_for_one_and_the_target_keeps_apart(
        servers, tmp_path):
    r = _proved(tmp_path)
    assert (r.check, r.scope, r.status) == ("schema", "cx collations",
                                            "diff"), r.detail
    name = _column(r.detail, "people.name")
    assert name.startswith("people.name: diff - its own values compare or"
                           " sort differently on the target"), name
    assert "case" in name and "accents" in name, name
    assert ("'Bob' and 'bob' are one value on the source and two on the"
            " target") in name, name
    assert ("'Emile' and 'Émile' are one value on the source and two on"
            " the target") in name, name
    # the fix: a collation of the target that agrees, made to agree by
    # the servers' own answers, not taken from a table of names
    assert "give people.name the collation migkit_ci_ai on the target" \
        in r.fix_hint, r.fix_hint
    assert "provider = icu, locale = 'und-u-ks-level1', deterministic =" \
        " false" in r.fix_hint, r.fix_hint


def test_a_column_whose_values_hold_none_of_the_kinds_is_not_the_reason(
        servers, tmp_path):
    r = _proved(tmp_path)
    code = _column(r.detail, "people.code")
    # utf8mb4_bin pads and sorts upper case first; the database's own
    # collation does neither - but no two of the codes differ that way
    assert code.startswith("people.code: warn - every value read (5"
                           " distinct values) compares and sorts alike"), code
    assert "which none of its values holds: values written later would" \
        " part" in code, code
    assert "differently" not in code, code
    assert "people.code" not in r.fix_hint, r.fix_hint


def test_the_values_shown_are_masked_where_the_hop_asks(servers, tmp_path):
    r = _proved(tmp_path, mask=["people.name"])
    assert r.status == "diff", r.detail
    name = _column(r.detail, "people.name")
    assert name.startswith("people.name: diff"), name
    assert name.count("masked:") == 4, name
    shown = f"{r.detail} {r.fix_hint}"
    assert not [v for v in ("Bob", "bob", "Émile", "Emile", "zoe")
                if v in shown], shown


# ---- the verdicts themselves, from answers made up here ---------------------


def _layout():
    from migkit.storedcode import collations
    return collations._layout(sorted(["Bob", "bob", "Emile", "Émile",
                                      "zoe"]))


def _reading(lay, together=(), order=None, pairs=None, held=None,
             refused=""):
    """A side's answers: every class pair unequal and in order, the values
    in `together` taken for one."""
    from migkit.storedcode import collations
    answers = {}
    for got in lay.pairs.values():
        for x, y in got:
            answers[(x, y)], answers[(y, x)] = (0, 1, 0), (0, 0, 1)
    answers.update(pairs or {})
    group = {i: i for i in lay.texts}
    for g in together:
        for i in g:
            group[i] = min(g)
    return collations.Reading(
        "made up", set(lay.texts) if held is None else held,
        dict(lay.texts), answers, {k: False for k in answers}, refused,
        group, list(order or lay.data))


def test_each_verdict_is_reached_and_each_can_say_different():
    from migkit.storedcode import collations as C
    lay = _layout()
    bob, emile, low_bob, zoe, e_acute = lay.data
    src = _reading(lay, [[bob, low_bob], [emile, e_acute]],
                   [bob, low_bob, emile, e_acute, zoe])
    # the same answers: ok, and nothing parts
    same = C.compare(src, src, lay)
    assert (same.count, same.classes, same.like) == (0, {}, "")
    assert C.judge("t.c", same, C.present([src], lay), "5 distinct values",
                   "x").status == "ok"
    # the target keeps apart what the source takes for one: diff
    apart = C.compare(src, _reading(
        lay, order=[low_bob, bob, emile, e_acute, zoe]), lay)
    assert apart.count == 2 and apart.kinds == {"case", "accent"}
    assert C.judge("t.c", apart, set(), "5", "x").status == "diff"
    # the target sorts two values the other way round: diff
    turned = C.compare(_reading(lay), _reading(
        lay, order=[bob, low_bob, emile, zoe, e_acute]), lay)
    assert turned.count == 1 and turned.examples == [
        "'Emile' sorts before 'bob' on the source and after it on the"
        " target"]
    # a value the target cannot hold: diff
    lost = C.compare(src, _reading(
        lay, [[bob, low_bob], [emile, e_acute]],
        [bob, low_bob, emile, e_acute, zoe],
        held=set(lay.texts) - {zoe}), lay)
    assert lost.count == 1 and lost.kinds == {"held"}
    # a kind the values hold none of parts: warn, said as such
    x, y = lay.pairs["trailing"][0]
    padded = C.compare(src, _reading(
        lay, [[bob, low_bob], [emile, e_acute]],
        [bob, low_bob, emile, e_acute, zoe],
        pairs={(x, y): (1, 0, 0), (y, x): (1, 0, 0)}), lay)
    assert (padded.count, list(padded.classes)) == (0, ["trailing"])
    v = C.judge("t.c", padded, C.present([src], lay), "5", "x")
    assert v.status == "warn" and "which none of its values holds" in v.text
    # the target refuses a LIKE the source answers: warn
    refused = C.compare(src, _reading(
        lay, [[bob, low_bob], [emile, e_acute]],
        [bob, low_bob, emile, e_acute, zoe], refused="no LIKE here"), lay)
    assert refused.like == "no LIKE here"
    assert C.judge("t.c", refused, set(), "5", "x").status == "warn"


def test_the_kinds_the_values_hold_are_the_servers_answers():
    from migkit.storedcode import collations as C
    lay = _layout()
    bob, emile, low_bob, zoe, e_acute = lay.data
    # nothing taken for one, and in code point order: no two neighbours
    # differ by case or accents
    apart = C.present([_reading(lay)], lay)
    assert "case" not in apart and "accent" not in apart, apart
    held = C.present([_reading(lay, [[bob, low_bob], [emile, e_acute]],
                               [bob, low_bob, emile, e_acute, zoe])], lay)
    assert {"case", "accent"} <= held, held


def test_the_kind_of_a_difference_is_named():
    from migkit.storedcode.collations import kinds
    assert kinds("Bob", "bob") == {"case"}
    assert kinds("Emile", "Émile") == {"accent"}
    assert kinds("a", "a ") == {"trailing"}
    assert kinds("co-op", "coop") == {"punct"}
    assert kinds("\u00e9", "e\u0301") == {"compose"}
    assert kinds("A", "\uff21") == {"width"}
    assert kinds("a1", "a10") == {"digits"}
    assert kinds("bob", "Emile") == {"upper"}
    assert kinds("abc", "abd") == set()


def test_a_column_not_asked_keeps_the_database_from_ok():
    from migkit.storedcode import collations as C
    ok = C._Verdict("t.a", "ok", "every value read compares alike")
    assert C._result("cx collations", [ok], []).status == "ok"
    got = C._result("cx collations",
                    [ok, C._Verdict("t.b", "not asked", "no sandbox")], [])
    assert got.status == "warn" and "t.b: not asked - no sandbox" \
        in got.detail
