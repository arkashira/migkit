"""Vectors held in a database migkit already moves are compared value for
value.

Measured before: a pgvector column (`vector`, `halfvec`, `sparsevec`) had
no rendering, so `check` left it out of the comparison and called a target
whose vectors differed equal. pgvector prints each element as the shortest
decimal that reads back to the same float, so the column's text is the
value itself, and it is now compared as such.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

PG, PORT = "migkit-test-pgvector", 15892


def sql(text, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-tA", "-v",
                          "ON_ERROR_STOP=1"], input=text,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-p",
                    f"{PORT}:5432", "-e", "POSTGRES_PASSWORD=test",
                    "pgvector/pgvector:pg16"], check=True,
                   capture_output=True)
    try:
        end = time.time() + 90
        while time.time() < end:
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                with socket.socket() as s:
                    if s.connect_ex(("127.0.0.1", PORT)) == 0:
                        break
            time.sleep(1)
        time.sleep(2)
        for db in ("app", "app_copy"):
            sql(f"create database {db}")
            sql("create extension vector; create table public.items (id int"
                " primary key, emb vector(4), half halfvec(3), sparse"
                " sparsevec(6))", db)
        sql("insert into public.items select g, array[g * 0.1, 1.0 / 3,"
            " g, -g * 1e-7]::vector, array[g, 0.5, 1]::halfvec,"
            " ('{1:' || g || ',4:0.25}/6')::sparsevec from generate_series(1,"
            " 500) g", "app")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def _eng(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                  password="test")
    hop = Hop(name="v", engine="postgres", source=ep, target=ep,
              databases=["app"], db_map={"app": "app_copy"})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _data(eng):
    return [(r.status, r.detail) for r in eng.check_data("app")
            if r.scope.endswith("items") or r.scope == "app"]


def test_vectors_move_and_compare_exactly(server, tmp_path):
    from migkit.cli import _Checkpoint
    eng = _eng(tmp_path)
    eng.move_table("app", "public", "items", 500_000,
                   _Checkpoint(tmp_path / "m.json"), [].append)
    assert sql("select md5(string_agg(emb::text || half::text ||"
               " sparse::text, ',' order by id)) from public.items",
               "app") == sql("select md5(string_agg(emb::text ||"
                             " half::text || sparse::text, ',' order by"
                             " id)) from public.items", "app_copy")
    got = _data(eng)
    assert got and all(s == "ok" for s, _ in got), got
    assert not any("could not compare" in d or "not compared" in d
                   for _, d in got), got


@pytest.mark.parametrize("column,change", [
    ("emb", "array[7 * 0.1, 1.0 / 3, 7, -7e-7 * 1.0000001]::vector"),
    ("half", "array[7, 0.5, 0.99]::halfvec"),
    ("sparse", "'{1:7,4:0.26}/6'::sparsevec")])
def test_a_vector_that_differs_is_found(server, tmp_path, column, change):
    sql(f"update public.items set {column} = {change} where id = 7",
        "app_copy")
    try:
        got = _data(_eng(tmp_path))
        assert any(s == "diff" for s, _ in got), got
    finally:
        # the row put back as the source has it
        sql("delete from public.items where id = 7", "app_copy")
        row = sql("select emb::text || '|' || half::text || '|' ||"
                  " sparse::text from public.items where id = 7", "app")
        emb, half, sparse = row.split("|")
        sql(f"insert into public.items values (7, '{emb}', '{half}',"
            f" '{sparse}')", "app_copy")
