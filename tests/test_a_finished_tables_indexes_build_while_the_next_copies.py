"""The index builds of a table the streaming copy has finished start while
it goes on with the next, where `decide` admits it from the facts the
move's catalogues give (`guarantees.breaks`), and wait for the copy's end
where it does not (R20.5, first step). Fakes: no server is reached; the
copy's own catalogue is read-only evidence of which table is whole.

The server tests at the end run the streaming copy on the shared
PostgreSQL 16 pair (names of this module alone: `w16e_*`) and read the
run's own evidence (`overlap.json`) for the timestamps."""
import json
import subprocess
import threading

import pytest

from migkit import guarantees, jointplan, movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

PROVES = [("move.load-window", "postgres")]


class Fake(movers._IndexWindow):
    def __init__(self):
        self.hop = self.db = self.log = self.table = None
        self.workers, self.only_left = 2, False
        self.dropped = ["public.a_v", "public.a_w", "public.b_v"]
        self.of = {"public.a_v": "public.a", "public.a_w": "public.a",
                   "public.b_v": "public.b"}
        self.ddl = {i: f"create index {i}" for i in self.dropped}
        self.built = {}
        self._lock = threading.Lock()
        self.ran = []

    def _build(self, names):
        self.ran += names
        return list(names), []


def test_a_finished_tables_indexes_are_built_and_the_rest_stay_owed():
    w = Fake()
    assert sorted(w.build_table("public.a")) == ["public.a_v", "public.a_w"]
    assert w.dropped == ["public.b_v"] and "public.a" in w.built
    began, ended = w.built["public.a"]
    assert began <= ended
    assert w.build_table("public.a") == []      # nothing built twice
    assert w.ran == ["public.a_v", "public.a_w"]


def test_a_build_that_fails_stays_owed_to_the_end():
    w = Fake()
    w._build = lambda names: ([], list(names))
    assert w.build_table("public.b") == []
    assert w.dropped.count("public.b_v") == 1


def _sem(**gaps):
    s = {t: guarantees.Semantics(table=t, key=("id",)) for t in ("a", "b")}
    for t, why in gaps.items():
        s[t].gaps.append(guarantees.Gap(t, "dependencies", why))
    return s


def test_the_admit_reads_the_catalogues_facts_not_a_flag(monkeypatch):
    asked = []
    monkeypatch.setattr(jointplan, "semantics_of",
                        lambda eng, db, **kw: asked.append(db) or _sem())
    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine",
                        lambda hop: object())
    said = []
    got = movers._builds_beside_the_copy(None, "app", 2, True, said.append)
    assert asked == ["app"] and got is not None and got.way in (
        "free", "waves")
    assert any(x.startswith("index builds:") for x in said)


def test_a_catalogue_that_cannot_be_read_overlaps_nothing(monkeypatch):
    def boom(eng, db, **kw):
        raise RuntimeError("no catalogue")
    monkeypatch.setattr(jointplan, "semantics_of", boom)
    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine",
                        lambda hop: object())
    assert movers._builds_beside_the_copy(None, "app", 2, True) is None


# ---- on a server ---------------------------------------------------------

needs_copy = pytest.mark.skipif(
    movers.pgcopydb_runner() != "local",
    reason="the streaming copy's local build is not installed here")
TABLES = "w16e_a, w16e_b"
#: A small with two secondary indexes; B large, slowed on the target (a
#: pause every 500th row, firing as a replica too) so the copy of B lasts
#: well past the moment A is seen whole
SHAPE = ("create table w16e_a (id int primary key, x int, y int not null);"
         " create index w16e_a_x on w16e_a (x);"
         " create index w16e_a_y on w16e_a (y);"
         " create table w16e_b (id int primary key, v text)")
SLOW = ("create function w16e_slow() returns trigger language plpgsql as $$"
        " begin if new.id % 500 = 0 then perform pg_sleep(0.05); end if;"
        " return new; end $$; create trigger w16e_slow before insert on"
        " w16e_b for each row execute function w16e_slow();"
        " alter table w16e_b enable always trigger w16e_slow")


def _hop(pg_pair, tmp_path):
    hop = Hop(name="w16e", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.fixture(autouse=True)
def libpq_on_the_path(monkeypatch):
    import os
    for bin_ in ("/opt/homebrew/opt/libpq/bin", "/usr/local/opt/libpq/bin"):
        if os.path.isdir(bin_):
            monkeypatch.setenv("PATH", bin_ + os.pathsep + os.environ["PATH"])


@pytest.fixture
def pair(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"drop table if exists {TABLES} cascade")
        assert psql(port, SHAPE).returncode == 0
    assert psql(pg_pair["dst"], SLOW).returncode == 0
    psql(pg_pair["src"],
         "insert into w16e_a select g, g, g from generate_series(1, 2000) g;"
         " insert into w16e_b select g, 'b' || g from"
         " generate_series(1, 100000) g; analyze")
    yield pg_pair
    # a copier a stopped test left behind must not write into the next
    subprocess.run(["pkill", "-TERM", "-f", f"pgcopydb.*{tmp_path}"])
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"drop table if exists {TABLES}, w16e_c cascade")


def _indexes(port):
    return psql(port, "select string_agg(indexname, ',' order by indexname)"
                " from pg_indexes where tablename like 'w16e\\_%'"
                ).stdout.strip()


@needs_docker
@needs_copy
def test_a_finished_tables_indexes_build_before_the_next_table_is_in(
        pair, tmp_path):
    said = []
    movers.pgcopydb_move(_hop(pair, tmp_path), "postgres", 2, True,
                         said.append)
    ev = json.loads((tmp_path / "overlap.json").read_text())
    assert ev["way"] in ("free", "waves"), ev
    began, ended = ev["built"]["public.w16e_a"]
    # A's build began while B was still being copied (the copy ended with B)
    assert began < ev["copy_ended"], ev
    assert ev["copied"]["public.w16e_a"] < ev["copy_ended"]
    # and nothing was lost to the overlap: every index is there, B's from
    # the window's end
    assert _indexes(pair["dst"]) == _indexes(pair["src"])
    assert psql(pair["dst"], "select count(*) from w16e_b").stdout.strip() \
        == "100000"
    assert any(x.startswith("index builds:") for x in said), said
    assert any("w16e_a: 2 indexes built while the copy goes on" in x
               for x in said), said


@needs_docker
@needs_copy
def test_a_gap_decide_finds_leaves_every_build_to_the_copys_end(
        pair, tmp_path):
    # a column that takes NULL on the source and not on the target is a
    # gap no order of the phases mends: the overlap is refused with that
    # reason, and A's indexes are built after B's copy
    assert psql(pair["src"], "alter table w16e_a alter column y drop not"
                " null").returncode == 0
    said = []
    movers.pgcopydb_move(_hop(pair, tmp_path), "postgres", 2, True,
                         said.append)
    # nothing ran beside the copy, so no evidence of a build beside it is
    # kept; the log has the refusal's reason, then B's rows in, then every
    # build, once
    assert not (tmp_path / "overlap.json").exists()
    why = [i for i, x in enumerate(said)
           if x.startswith("index builds wait for")]
    assert why and "takes NULL on the source and not on the target" in said[
        why[0]]
    assert not any("indexes built while the copy goes on" in x for x in said)
    b_in = [i for i, x in enumerate(said) if x.startswith("public.w16e_b: copied")]
    built = [i for i, x in enumerate(said) if "indexes dropped for the load"
             " and rebuilt after" in x]
    assert b_in and len(built) == 1, said
    assert why[0] < b_in[0] < built[0], said
    assert _indexes(pair["dst"]) == _indexes(pair["src"])


@needs_docker
def test_keys_between_the_tables_decide_the_overlap_on_the_real_catalogue(
        pair, tmp_path):
    """The same decision on the catalogues a server gives: a key from B to
    A is kept by the waves where the target's keys hold during the load,
    owes no order where the window took them off, and a cycle with keys
    held loads in no order at all - the builds then wait."""
    hop = _hop(pair, tmp_path)
    for port in (pair["src"], pair["dst"]):
        assert psql(port, "alter table w16e_b add column a int references"
                    " w16e_a (id)").returncode == 0
    said = []
    got = movers._builds_beside_the_copy(hop, "postgres", 2, False,
                                         said.append)
    assert got is not None and got.way == "waves", (got, said)
    for port in (pair["src"], pair["dst"]):
        assert psql(port, "alter table w16e_a add column b int references"
                    " w16e_b (id)").returncode == 0
    said = []
    assert movers._builds_beside_the_copy(hop, "postgres", 2, False,
                                          said.append) is None
    assert said and "point at each other" in said[0], said
    said = []
    got = movers._builds_beside_the_copy(hop, "postgres", 2, True,
                                         said.append)
    assert got is not None and got.way in ("free", "waves"), (got, said)
