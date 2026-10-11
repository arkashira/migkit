"""CockroachDB's and Aurora DSQL's tables, indexes and defaults
(P0-PARITY.schema), without a server: each engine's own methods over what
the catalogue answers.

* a table is made by PostgreSQL's definition, a key a primary key; DSQL
  keeps no `jsonb`, so a document column is made `json` there;
* an index is read by `pg_index` with its columns, and a partial or
  expression one is listed and said not to be carried; DSQL builds an
  index on a table with rows only `ASYNC`, as a job of its own;
* a default is read as the catalogue keeps it, and a column filled from a
  sequence or a row id is an identity whose default is not carried.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import pgwire
from migkit.engines.cockroachdb import CockroachEngine
from migkit.engines.dsql import DsqlEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.indexes", ("cockroachdb", "dsql")),
          ("schema.defaults", ("cockroachdb", "dsql")),
          ("schema.create", "dsql")]

EP = Endpoint(host="x", port=1, user="x", password="x")
ENGINES = [("cockroachdb", CockroachEngine), ("dsql", DsqlEngine)]


def _engine(name, cls, monkeypatch, catalogue=None):
    eng = cls(Hop(name="t", engine=name, source=EP, target=EP,
                  databases=["d"]))
    asked = []

    def rows(self, side, db, sql, args=None):
        asked.append((side, " ".join(sql.split()), tuple(args or ())))
        for word, got in (catalogue or {}).items():
            if word in sql:
                return got
        return []
    monkeypatch.setattr(pgwire.PgWireEngine, "_rows", rows)
    eng.asked = asked
    return eng


COLS = [("id", "integer", (64,)), ("name", "text", (40,)),
        ("amount", "decimal", (12, 2))]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_table_is_made_by_postgresqls_definition(monkeypatch, name, cls):
    eng = _engine(name, cls, monkeypatch)
    got = eng.neutral_create_sql("dst", "d", "public.t", COLS, key=("id",))
    assert got == ('create table "public"."t" ("id" bigint, "name"'
                   ' varchar(40), "amount" numeric(12,2), primary key'
                   ' ("id"))')


def test_dsql_makes_a_document_column_json_not_jsonb(monkeypatch):
    eng = _engine("dsql", DsqlEngine, monkeypatch)
    got = eng.neutral_create_sql(
        "dst", "d", "public.t", [("id", "integer", (64,)),
                                 ("doc", "json", ())], key=("id",))
    assert '"doc" json' in got and "jsonb" not in got, got


def test_dsql_builds_an_index_async_and_cockroachdb_does_not(monkeypatch):
    dsql = _engine("dsql", DsqlEngine, monkeypatch)
    assert dsql.neutral_create_index_sql(
        "dst", "d", "public.t", "ix", True, ["a", "b"]) == (
        'create unique index async "ix" on "public"."t" ("a", "b")')
    crdb = _engine("cockroachdb", CockroachEngine, monkeypatch)
    assert crdb.neutral_create_index_sql(
        "dst", "d", "public.t", "ix", False, ["a"]) == (
        'create index "ix" on "public"."t" ("a")')


@pytest.mark.parametrize("name,cls", ENGINES)
def test_an_index_is_read_by_pg_index_and_a_partial_one_is_not_carried(
        monkeypatch, name, cls):
    eng = _engine(name, cls, monkeypatch, {"pg_catalog.pg_index": [
        ("ix_a", True, "a", True), ("ix_a", True, "b", True),
        ("ix_p", False, "n", False)]})
    assert eng.neutral_indexes("src", "d", "public.t") == [
        ("ix_a", True, ["a", "b"], True), ("ix_p", False, ["n"], False)]
    sent = eng.asked[0]
    assert sent[2] == ("public", "t") and "not x.indisprimary" in sent[1]


@pytest.mark.parametrize("name,cls", ENGINES)
def test_a_default_and_an_identity_are_read_as_the_catalogue_keeps_them(
        monkeypatch, name, cls):
    eng = _engine(name, cls, monkeypatch, {"information_schema.columns": [
        ("id", "NO", "nextval('t_id_seq'::REGCLASS)", "NO"),
        ("rid", "NO", "unique_rowid()", "NO"),
        ("gen", "NO", None, "YES"),
        ("n", "YES", "'x'::STRING", "NO"),
        ("c", "NO", "1:::INT8", "NO")]})
    got = eng.neutral_column_rules("src", "d", "public.t")
    assert got["id"] == {"null": False, "default": None, "identity": True}
    assert got["rid"] == {"null": False, "default": "unique_rowid()",
                          "identity": True}
    assert got["gen"] == {"null": False, "default": None, "identity": True}
    assert got["n"] == {"null": True, "default": "'x'::STRING",
                        "identity": False}
    assert got["c"]["default"] == "1:::INT8" and not got["c"]["identity"]
