"""The streaming copy loads inside the foreign-key window: the target's
keys on the tables it fills are off while rows load, back and validated
after, a key that cannot go back stops the move naming its statement, and
a copy stopped part way leaves the keys off and recorded for the one that
goes on (backlog 6.fkwin).

On the shared PostgreSQL 16 pair with the streaming copy's local build:
`child` points at `parent`, and a trigger on the target's `child` pauses
every 1,000th row so the load lasts long enough to be looked into.
"""
import subprocess
import threading
import time

import psycopg
import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from migkit.indexes import ForeignKeyWindow, NotPutBack
from tests.conftest import needs_docker, psql

PROVES = [("move.load-window", "postgres")]

pytestmark = [needs_docker, pytest.mark.skipif(
    movers.pgcopydb_runner() != "local",
    reason="the streaming copy's local build is not installed here")]

SCHEMA = ("create table parent (id int primary key, v text);"
          " create table child (id int primary key, parent_id int"
          " references parent (id), v text)")
SLOW = ("create function slow() returns trigger language plpgsql as $$ begin"
        " if new.id % 1000 = 0 then perform pg_sleep(0.05); end if;"
        " return new; end $$; create trigger slow before insert on child"
        " for each row execute function slow();"
        " alter table child enable always trigger slow")
FKS = ("select string_agg(conname || ' ' || pg_get_constraintdef(oid) || ' '"
       " || convalidated, ';' order by conname) from pg_constraint"
       " where contype = 'f' and conrelid = 'public.child'::regclass")


def _hop(pg_pair, tmp_path):
    hop = Hop(name="fkwin", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.fixture
def pair(pg_pair):
    src, dst = pg_pair["src"], pg_pair["dst"]
    for port in (src, dst):
        psql(port, "drop table if exists child, parent cascade")
        assert psql(port, SCHEMA).returncode == 0
    assert psql(dst, SLOW).returncode == 0
    psql(src, "insert into parent select g, 'p' || g from"
              " generate_series(1, 1000) g;"
              " insert into child select g, 1 + g % 1000, 'c' || g from"
              " generate_series(1, 100000) g; analyze")
    return pg_pair


def _digest(port):
    return psql(port, "select (select count(*) from parent) || ':' ||"
                      " (select count(*) from child) || ':' || (select"
                      " sum(hashtext(id || v)) from child)"
                ).stdout.strip()


class Watch(threading.Thread):
    """Looks into the target while the copy loads: the keys on `child`
    and the rows a COPY into it has taken, every 50 ms; `then(keys,
    copying, conn)` may act on what it sees."""

    def __init__(self, port, then=None):
        super().__init__(daemon=True)
        self.port, self.then = port, then
        self.samples, self.stop = [], threading.Event()

    def run(self):
        with psycopg.connect(host="127.0.0.1", port=self.port, dbname="postgres",
                             user="postgres", password="test",
                             autocommit=True) as c:
            while not self.stop.is_set():
                try:
                    keys = c.execute(
                        "select count(*) from pg_constraint where contype ="
                        " 'f' and conrelid = 'public.child'::regclass"
                    ).fetchone()[0]
                    copying = c.execute(
                        "select coalesce(max(tuples_processed), 0) from"
                        " pg_stat_progress_copy").fetchone()[0]
                except psycopg.Error:
                    continue
                self.samples.append((keys, copying))
                if self.then:
                    self.then(keys, copying, c)
                time.sleep(0.05)


def test_the_keys_are_off_while_the_rows_load_and_back_validated_after(
        pair, tmp_path):
    before = psql(pair["dst"], FKS).stdout.strip()
    assert before.endswith(" true") and "REFERENCES parent(id)" in before
    watch = Watch(pair["dst"])
    watch.start()
    try:
        movers.pgcopydb_move(_hop(pair, tmp_path), "postgres", 2, True, None)
    finally:
        watch.stop.set()
        watch.join(5)
    during = [k for k, copying in watch.samples if copying > 0]
    assert during, "the copy was never seen loading"
    assert set(during) == {0}, "a key was on the table while rows loaded"
    # back, as defined and validated, and the record is gone
    assert psql(pair["dst"], FKS).stdout.strip() == before
    assert not list(tmp_path.rglob("dropped-fks.*.json"))
    assert _digest(pair["dst"]) == _digest(pair["src"])


def test_a_key_that_cannot_go_back_stops_the_move_with_its_statement(
        pair, tmp_path):
    hit = []

    def row_without_a_parent(keys, copying, conn):
        # inside the window: the target takes a row its key would refuse
        if copying > 0 and keys == 0 and not hit:
            conn.execute("insert into child values (-1, 999999, 'orphan')")
            hit.append(True)
    watch = Watch(pair["dst"], row_without_a_parent)
    watch.start()
    try:
        with pytest.raises(NotPutBack) as stopped:
            movers.pgcopydb_move(_hop(pair, tmp_path), "postgres", 2, True,
                                 None)
    finally:
        watch.stop.set()
        watch.join(5)
    assert hit
    said = str(stopped.value)
    assert 'alter table "public"."child" validate constraint' in said, said
    assert "child_parent_id_fkey" in said and "violates" in said, said
    # the definitions are kept for a person, or the next load
    assert list(tmp_path.rglob("dropped-fks.*.json"))
    # the next window puts the key back and validates it once the row is
    # gone - as the dead load saved it, not as the target holds it now
    # (added, not yet valid)
    assert psql(pair["dst"], FKS).stdout.strip().endswith(" NOT VALID false")
    psql(pair["dst"], "delete from child where id = -1")
    with ForeignKeyWindow(_hop(pair, tmp_path), "postgres",
                          tables=["public.child", "public.parent"]):
        pass
    assert psql(pair["dst"], FKS).stdout.strip().endswith(" true")
    assert not list(tmp_path.rglob("dropped-fks.*.json"))
    assert _digest(pair["dst"]) == _digest(pair["src"])


def test_a_copy_stopped_mid_window_leaves_the_keys_off_for_the_next_window(
        pair, tmp_path):
    before = psql(pair["dst"], FKS).stdout.strip()
    stopped = []

    def stop_the_copy(keys, copying, conn):
        if 0 < copying < 90000 and not stopped:
            subprocess.run(["pkill", "-TERM", "-f", f"pgcopydb.*{tmp_path}"])
            stopped.append(True)
    watch = Watch(pair["dst"], stop_the_copy)
    watch.start()
    try:
        with pytest.raises(Exception):
            movers.pgcopydb_move(_hop(pair, tmp_path), "postgres", 2, True,
                                 None)
    finally:
        watch.stop.set()
        watch.join(5)
    assert stopped, "the copy ended before it could be stopped"
    # off, and the definitions on disk: the cutover waits for them
    assert psql(pair["dst"], FKS).stdout.strip() == ""
    assert list(tmp_path.rglob("dropped-fks.*.json"))
    assert movers._fks_off(_hop(pair, tmp_path), "postgres")
    # the next window of this process takes them over and puts them back
    # as saved (the copy itself going on from a stopped one is
    # test_the_streaming_copy_goes_on_after_a_stop)
    said = []
    with ForeignKeyWindow(_hop(pair, tmp_path), "postgres",
                          tables=["public.child", "public.parent"],
                          log=said.append):
        pass
    assert any("foreign keys an earlier load took off" in x for x in said)
    assert psql(pair["dst"], FKS).stdout.strip() == before
    assert not list(tmp_path.rglob("dropped-fks.*.json"))
    assert not movers._fks_off(_hop(pair, tmp_path), "postgres")
