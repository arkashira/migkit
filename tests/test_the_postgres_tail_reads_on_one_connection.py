"""The PostgreSQL change tail reads its slot on one connection it keeps.

It started the client program three times a batch - the slot moved on, the
log's end asked, the changes peeked - each a process and a connection of
its own, and read the changes back as the program printed them: a line a
change. A text value holding a line break was printed across two lines,
and the tail stopped on half a value (`unterminated value`) at every try
from then on; the program's output was also read with its line endings
rewritten, so a carriage return would have arrived as a newline had the
parse got that far.

The changes are now read as the two columns they are, over a connection
held from one batch to the next, and the slot still moves only when the
position comes back from the tail that applied up to it.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

#: a line break of every kind a line-by-line reader splits on
SPLITS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"
BREAKS = {1: "two\nlines", 2: "carriage\rreturn", 3: "both\r\nends",
          4: "vertical\x0btab", 5: "form\x0cfeed", 6: "file\x1csep",
          7: "group\x1dsep", 8: "record\x1esep", 9: "next\x85line",
          10: "line\u2028sep", 11: "paragraph\u2029sep",
          12: "ends in a newline\n", 13: "\n", 14: "it's\n'quoted'"}


@pytest.fixture
def eng(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=pg_pair["src"], user="postgres",
                  password="test")
    got = PostgresEngine(Hop(name="one-conn", engine="postgres", source=ep,
                             target=ep, databases=["postgres"]))
    assert psql(pg_pair["src"], "create table public.t (id int primary key,"
                                " v text)").returncode == 0
    yield got
    psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name) from"
                         " pg_replication_slots where slot_name ="
                         f" '{got.slot_name()}'")


def _literal(text):
    """SQL for `text` with its line breaks written as `chr()`, so nothing
    on the way into the server rewrites them."""
    parts, plain = [], ""
    for ch in text:
        if ch in SPLITS:
            parts += ["'" + plain.replace("'", "''") + "'", f"chr({ord(ch)})"]
            plain = ""
        else:
            plain += ch
    parts.append("'" + plain.replace("'", "''") + "'")
    return "(" + " || ".join(parts) + ")"


def test_a_value_holding_a_line_break_arrives_whole(pg_pair, eng):
    token = eng.change_point("src", "postgres")
    got = psql(pg_pair["src"], "insert into public.t values "
               + ", ".join(f"({i}, {_literal(v)})"
                           for i, v in sorted(BREAKS.items())))
    assert got.returncode == 0, got.stderr
    changes, _ = eng.neutral_changes("src", "postgres", token)
    assert {c["key"]["id"]: c["values"]["v"] for c in changes} == BREAKS


def test_batches_are_read_on_one_connection_and_start_no_program(
        pg_pair, eng, monkeypatch):
    from migkit.engines import postgres
    token = eng.change_point("src", "postgres")
    started = []
    real = postgres.run

    def counted(cmd, *a, **kw):
        started.append(cmd[0])
        return real(cmd, *a, **kw)
    monkeypatch.setattr(postgres, "run", counted)
    pids = set()
    for i in range(3):
        assert psql(pg_pair["src"], f"insert into public.t values ({i},"
                                    f" 'r{i}')").returncode == 0
        changes, token = eng.neutral_changes("src", "postgres", token)
        assert [c["key"]["id"] for c in changes] == [i], changes
        pids.add(eng._slot_session("src", "postgres").get_backend_pid())
    assert started == [], started
    assert len(pids) == 1, pids


def test_a_connection_lost_between_batches_is_opened_again(pg_pair, eng):
    token = eng.change_point("src", "postgres")
    assert psql(pg_pair["src"], "insert into public.t values (1, 'a')"
                ).returncode == 0
    first, _ = eng.neutral_changes("src", "postgres", token)
    pid = eng._slot_session("src", "postgres").get_backend_pid()
    assert psql(pg_pair["src"], f"select pg_terminate_backend({pid})"
                ).stdout.strip() == "t"
    # not applied, so not handed back: the same change again, on a new
    # connection, and nothing thrown away on the way
    again, _ = eng.neutral_changes("src", "postgres", token)
    assert again == first and len(first) == 1, (first, again)
    assert eng._slot_session("src", "postgres").get_backend_pid() != pid


def test_the_slot_moves_only_when_the_position_comes_back(pg_pair, eng):
    token = eng.change_point("src", "postgres")
    name = eng.slot_name()

    def confirmed():
        return psql(pg_pair["src"], "select confirmed_flush_lsn from"
                                    " pg_replication_slots where slot_name"
                                    f" = '{name}'").stdout.strip()
    at = confirmed()
    assert psql(pg_pair["src"], "insert into public.t values (1, 'a')"
                ).returncode == 0
    got, after = eng.neutral_changes("src", "postgres", token)
    assert len(got) == 1 and confirmed() == at
    again, _ = eng.neutral_changes("src", "postgres", token)
    assert again == got and confirmed() == at
    later, _ = eng.neutral_changes("src", "postgres", after)
    assert later == [] and confirmed() == after, (confirmed(), after)
