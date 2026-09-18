"""A PostgreSQL `numeric` column on its way into MongoDB.

SQLite refused a `Decimal` outright and that was fixed a few changes ago;
MongoDB turns out to do the same thing, measured rather than assumed:

    insert_one({"n": Decimal("1.50")})
    InvalidDocument: cannot encode object: Decimal('1.50'),
                     of type: <class 'decimal.Decimal'>

so a move or a repair from any SQL source with a numeric column died on the
first document. BSON has the right type for it, and a `Decimal128` keeps the
scale it was handed - `1.50` comes back `1.50` - which is what lets the two
sides render to the same text and the digest match.

The rendering has the same exponent trap the SQL side had. `str(Decimal128(
"0.0000000001"))` is `1E-10`, while PostgreSQL's own `::text` for that number
is `0.0000000001`, so the value is taken through `to_decimal()` and formatted
in fixed point.

Until this change MongoDB's `decimal` was not in the type map at all, so the
column was not merely uncomparable - it was silently left out of the
comparison while everything else was reported as equal.
"""
import decimal
import pathlib
import socket
import subprocess
import time

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MG = "migkit-test-dec-mg"
MG_PORT = 27090
AWKWARD = 'a:b["x"]'
VALUES = ("1.5000", "0.0001", "12345678901234.5678", "-0.5000", "0.0000")


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def mongo():
    subprocess.run(["docker", "rm", "-f", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    assert _wait(MG_PORT)
    for _ in range(40):
        r = subprocess.run(["docker", "exec", MG, "mongosh", "--quiet",
                            "--eval", "db.runCommand({ping:1}).ok"],
                           capture_output=True, text=True)
        if r.returncode == 0 and "1" in r.stdout:
            break
        time.sleep(1)
    else:
        pytest.fail("mongo never answered")
    yield
    subprocess.run(["docker", "rm", "-f", MG], capture_output=True)


def _client():
    from pymongo import MongoClient
    return MongoClient(f"mongodb://127.0.0.1:{MG_PORT}/"
                       "?directConnection=true")


def _engine(pg_port, tmp_path, db_map=None):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=MG_PORT, user="",
                              password="",
                              options={"uri_options":
                                       "directConnection=true"}),
              databases=["postgres"],
              db_map=db_map or {"postgres": "moved"},
              options={"source_engine": "postgres",
                       "target_engine": "mongodb"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


class _Checkpoint(dict):
    def save(self):
        pass


def _seed_pg(port):
    values = ", ".join(f"({i}, $${AWKWARD}$$, {v})"
                       for i, v in enumerate(VALUES, start=1))
    psql(port, "create table t (_id bigint primary key, label text,"
               f" n numeric(20,4)); insert into t values {values};")


def test_pymongo_will_not_take_a_plain_decimal(mongo):
    """The measurement the conversion exists for. If a future pymongo accepts
    one, this fails and the conversion can be reconsidered."""
    from pymongo.errors import InvalidDocument
    coll = _client()["probe"]["t"]
    coll.drop()
    with pytest.raises(InvalidDocument) as caught:
        coll.insert_one({"_id": 1, "n": decimal.Decimal("1.50")})
    assert "cannot encode object" in str(caught.value), caught.value


def test_a_numeric_column_crosses_and_the_two_sides_compare_equal(
        pg_pair, mongo, tmp_path):
    _seed_pg(pg_pair["src"])
    client = _client()
    client.drop_database("moved")
    eng = _engine(pg_pair["src"], tmp_path)
    eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                   lambda m: None)

    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "every compared column equal" in got[0].detail

    # the scale survived, which is what makes the texts match
    from bson.decimal128 import Decimal128
    stored = {d["_id"]: d["n"] for d in client["moved"]["t"].find()}
    assert len(stored) == len(VALUES), stored
    for i, value in enumerate(VALUES, start=1):
        assert isinstance(stored[i], Decimal128), stored[i]
        assert str(stored[i]) == value, (i, stored[i], value)


def test_the_column_is_compared_rather_than_quietly_left_out(pg_pair, mongo,
                                                             tmp_path):
    """It used to be unmapped, so the check passed on everything else and
    said nothing about the number."""
    _seed_pg(pg_pair["src"])
    client = _client()
    client.drop_database("moved")
    eng = _engine(pg_pair["src"], tmp_path)
    eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                   lambda m: None)
    from bson.decimal128 import Decimal128
    client["moved"]["t"].update_one({"_id": 2},
                                    {"$set": {"n": Decimal128("9.9999")}})
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert got[0].status == "diff", got[0].detail
    assert "1 with different values (2)" in got[0].detail, got[0].detail
    assert "not compared" not in got[0].detail, got[0].detail


def test_a_repair_carries_the_decimal_back(pg_pair, mongo, tmp_path):
    _seed_pg(pg_pair["src"])
    client = _client()
    client.drop_database("moved")
    eng = _engine(pg_pair["src"], tmp_path)
    eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                   lambda m: None)
    from bson.decimal128 import Decimal128
    client["moved"]["t"].update_one({"_id": 3},
                                    {"$set": {"n": Decimal128("0.0000")}})
    client["moved"]["t"].delete_one({"_id": 4})
    eng.check_data("postgres")
    for action in eng.repair_plan("postgres", "rows"):
        eng.apply("postgres", action)

    after = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]
    stored = {d["_id"]: str(d["n"]) for d in client["moved"]["t"].find()}
    assert stored[3] == "12345678901234.5678", stored
    assert stored[4] == "-0.5000", stored


def test_the_rendering_matches_what_postgres_prints(pg_pair, mongo):
    """One query returns both the value and the server's own text, so the
    two cannot drift apart between reads."""
    import psycopg2
    from bson.decimal128 import Decimal128
    _seed_pg(pg_pair["src"])
    conn = psycopg2.connect(host="127.0.0.1", port=pg_pair["src"],
                            user="postgres", password="test",
                            dbname="postgres")
    try:
        cur = conn.cursor()
        cur.execute("select n, n::text from t order by _id")
        rows = cur.fetchall()
    finally:
        conn.close()
    assert len(rows) == len(VALUES)
    for value, text in rows:
        mongo_side = Decimal128(value)
        assert canon.render_value("decimal", mongo_side) == text, (
            value, text, canon.render_value("decimal", mongo_side))
        assert canon.render_value("decimal", value) == text


def test_the_exponent_form_is_not_what_gets_compared():
    """`str` on either type says `1E-10`; no SQL engine prints that, so a
    rendering built on it would report a difference that is not there."""
    from bson.decimal128 import Decimal128
    small = Decimal128("0.0000000001")
    assert str(small) == "1E-10", str(small)
    assert canon.render_value("decimal", small) == "0.0000000001"
    assert canon.render_value("decimal",
                              decimal.Decimal("0.0000000001")) \
        == "0.0000000001"


def test_mongodb_decimal_is_a_mapped_class_now():
    assert canon.type_class("mongodb", "decimal") == "decimal"
    # and the types that are still unmapped stay unmapped rather than
    # being swept along with it
    for unmapped in ("object", "array"):
        assert canon.type_class("mongodb", unmapped) is None, unmapped
