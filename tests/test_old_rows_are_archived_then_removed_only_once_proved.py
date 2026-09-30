"""Old rows to cheaper storage (backlog P0-SCENARIOS): the rows a rule
picks added to an archive and proved there, removed from the source only
once proved and only where the hop and the source's own answer allow it,
behind the approvals, every removed row kept in a ledger - and put back
from the archive when asked.

The hop's words, the order rows leave in and the answer a hop must give
by name run without a server; the rest archives a PostgreSQL 16 table
into another PostgreSQL 16.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import archive, sourcechange
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-archive-src", 16468), ("migkit-test-archive-dst",
                                               16469)


def _hop(opts=None, goal="archive"):
    return Hop(name="arc", engine="postgres", source=Endpoint(),
               target=Endpoint(), goal=goal, databases=["shop"],
               options=dict(opts or {}))


def test_the_hop_says_what_it_does_with_the_source():
    assert archive.settings(_hop()) == {"remove": False, "restore": False,
                                        "batch": archive.BATCH}
    assert archive.settings(_hop({"archive": {"remove": True,
                                              "batch": "50"}}))["batch"] == 50
    with pytest.raises(SystemExit):
        archive.settings(_hop({"archive": {"remove": True,
                                           "restore": True}}))
    assert archive.settings(_hop(goal="copy")) is None


def test_rows_leave_children_first():
    edges = [("items", ["order_id"], "orders", ["id"]),
             ("orders", ["customer_id"], "customers", ["id"])]
    got = archive._order(["customers", "orders", "items"], edges)
    assert got == ["items", "orders", "customers"]


def test_the_sources_answer_is_asked_by_its_own_name(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    said = []
    # a word given for every change is not an answer for the rows
    every = _hop({"source_changes": "yes"})
    assert sourcechange.settle_named(every, "archived_rows", ["offer"],
                                     True, None, said.append) is None
    assert "offer" in said
    named = _hop({"source_changes": {"archived_rows": "yes"}})
    assert sourcechange.settle_named(named, "archived_rows", ["offer"],
                                     True, None, said.append) == "yes"
    # asked at a terminal, recorded for the next run
    asked = _hop()
    assert sourcechange.settle_named(asked, "archived_rows", ["offer"],
                                     True, lambda q: "no",
                                     said.append) == "no"
    assert sourcechange.answers(asked)["archived_rows"][0] == "no"


# --- PostgreSQL into PostgreSQL -------------------------------------------------

def _sql(name, sql, db="shop"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table events (id int primary key, at date not null, v text);
insert into events select i, date '2024-01-01' + i, 'e' || i
  from generate_series(1, 20) i;
create table accounts (id int primary key, closed date);
insert into accounts values (1, date '2020-01-01'), (2, null);
create table entries (id int primary key, account_id int references
                      accounts, at date);
insert into entries values (1, 1, date '2025-06-01');
"""


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16"],
                           check=True, capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"], capture_output=True
                                  ).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
        _sql(SRC[0], "create database shop", "postgres")
        _sql(SRC[0], SEED)
        for db in ("shop", "shop_acc"):
            _sql(DST[0], f"create database {db}", "postgres")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _conf(tmp_path, monkeypatch, opts):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    head = ("    engine: postgres\n    goal: archive\n"
            f"    source: {side.format(port=SRC[1])}\n"
            f"    target: {side.format(port=DST[1])}\n"
            "    databases: [shop]\n")
    path.write_text(
        "hops:\n"
        "  old:\n" + head + "    exclude: [accounts, entries]\n"
        "    mapping:\n      where: {events: \"at < date '2024-01-11'\"}\n"
        f"    options: {opts}\n"
        "  closed:\n" + head + "    db_map: {shop: shop_acc}\n"
        "    exclude: [events, entries]\n"
        "    mapping:\n      where: {accounts: \"closed is not null\"}\n"
        "    options: {archive: {remove: true}, source_changes:"
        " {archived_rows: yes}}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


@needs_docker
def test_the_archive_is_added_to_and_the_source_kept(pair, tmp_path,
                                                      monkeypatch):
    _conf(tmp_path, monkeypatch, "{archive: {}}")
    got, out = _run("move", "old", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert "public.events: 10 rows added to the archive, 0 there already" \
        in out, out
    assert _sql(DST[0], "select count(*) from events") == "10"
    assert _sql(SRC[0], "select count(*) from events") == "20"
    # run again: nothing new, nothing overwritten
    got, out = _run("move", "old", "--mode", "full", "--go")
    assert "0 rows added to the archive, 10 there already" in out, out
    got, out = _run("check", "old")
    assert got.exit_code == 0, out
    assert "10 rows the rule picks on the source in the archive" in out, out


@needs_docker
def test_removed_only_once_proved_and_put_back_on_asking(pair, tmp_path,
                                                         monkeypatch):
    _conf(tmp_path, monkeypatch, "{archive: {}}")
    assert _run("move", "old", "--mode", "full", "--go")[0].exit_code == 0
    # a row the archive holds otherwise than the source has it now stays
    _sql(SRC[0], "update events set v = 'changed' where id = 3")
    _conf(tmp_path, monkeypatch, "{archive: {remove: true},"
                                 " source_changes: {archived_rows: yes}}")
    got, out = _run("move", "old", "--mode", "full", "--go")
    # the changed row is a row the archive holds otherwise: never
    # overwritten, and nothing removed
    assert got.exit_code != 0, out
    assert "the archive holds 1 of the rows the rule picks otherwise" in \
        out, out
    assert _sql(SRC[0], "select count(*) from events") == "20"
    _sql(SRC[0], "update events set v = 'e3' where id = 3")
    got, out = _run("move", "old", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert "10 archived rows removed from the source, each proved in the" \
        " archive first" in out, out
    assert _sql(SRC[0], "select min(id) || ':' || count(*) from events") == \
        "11:10"
    assert _sql(DST[0], "select count(*) from events") == "10"
    got, out = _run("check", "old")
    assert got.exit_code == 0, out
    assert "10 removed from the source, each in the archive with its" \
        " digest" in out, out
    # a removed row gone from the archive too is found
    _sql(DST[0], "delete from events where id = 5")
    got, out = _run("check", "old")
    assert got.exit_code != 0, out
    assert "1 rows removed from the source are gone from the archive" \
        " ((5))" in out, out
    _sql(DST[0], "insert into events values (5, date '2024-01-06', 'e5')")
    # put back, under the same answer
    _conf(tmp_path, monkeypatch, "{archive: {restore: true},"
                                 " source_changes: {archived_rows: yes}}")
    got, out = _run("move", "old", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert "10 rows put back on the source from the archive" in out, out
    assert _sql(SRC[0], "select count(*) from events") == "20"


@needs_docker
def test_a_row_other_rows_still_name_is_never_removed(pair, tmp_path,
                                                      monkeypatch):
    _conf(tmp_path, monkeypatch, "{archive: {}}")
    got, out = _run("move", "closed", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "public.accounts: 1 of the rows its rule picks are named by rows" \
        " of public.entries that stay on the source" in out, out
    assert _sql(SRC[0], "select count(*) from accounts") == "2"
