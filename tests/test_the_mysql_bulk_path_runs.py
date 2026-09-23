"""The MySQL bulk path, which could not run at all.

Measured against the build installed here (mydumper/myloader 1.0.5), three
of the things the path handed its programs were wrong, and any one of them
stopped it:

* The password went on attached, `-p<secret>`. This build does not accept
  that form, so the characters after `-p` were read as more short options:
  `-ptest` is `-p -t -e -s -t`, and the run died on `Error parsing option
  -t` - a flag nobody typed. A password made only of letters that happen to
  be flags parsed cleanly and switched them on instead: `-pmd` is
  `--no-schemas --no-data`, and then the login failed because no password
  was sent. And the command line is logged as given, so every run printed
  the source password before it failed.
* `--trx-consistency-only` is `--trx-tables` in this build.
* The loader's `--purge-mode` is gone. Its modes moved to `--drop-table`,
  and on a data-only dump neither `--drop-table=TRUNCATE` nor `=DELETE`
  empties anything: the load appends.

So the password travels in `MYSQL_PWD`, the flag spellings are asked of the
binary that is about to run, and migkit empties the target itself - after
the dump is complete, and leaving alone what the hop excludes.
"""
import ast
import pathlib
import shutil
import socket
import subprocess
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = "migkit-test-mybulk-src", "migkit-test-mybulk-dst"
SRC_PORT, DST_PORT = 13481, 13482
USER, SECRET = "mover", "not-a-real-secret-42"

have_tools = pytest.mark.skipif(
    not (shutil.which("mydumper", path=movers.tool_env()["PATH"])
         and shutil.which("myloader", path=movers.tool_env()["PATH"])),
    reason="the MySQL bulk programs are not installed")


def _sql(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    names = (SRC, DST)
    try:
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                            "mysql:8"], check=True, capture_output=True)
        for n, p in zip(names, (SRC_PORT, DST_PORT)):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                     "-ptest", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", p)) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
            _sql(n, f"create user '{USER}'@'%' identified by '{SECRET}';"
                    f" grant all on *.* to '{USER}'@'%';")
        yield
    finally:
        for n in names:
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, exclude=(), db_map=None, src_port=SRC_PORT):
    hop = Hop(name="my", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=src_port, user=USER,
                              password=SECRET),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user=USER,
                              password=SECRET),
              databases=["appdb"], exclude=list(exclude),
              db_map=dict(db_map or {}), workers=2)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _seed(target_db="appdb"):
    """Source: three orders and one audit row. Target: a stale order the
    source does not have, and two audit rows only the target has - which
    reference orders, so emptying `orders` meets a foreign key."""
    schema = ("create table orders (id int primary key, v text);"
              " create table audit_log (id int primary key, note text,"
              " oid int, foreign key (oid) references orders(id));")
    _sql(SRC, "drop database if exists appdb; create database appdb;"
              f" use appdb; {schema}"
              " insert into orders values (1,'s1'),(2,'s2'),(3,'s3');"
              " insert into audit_log values (100,'source-side',1);")
    _sql(DST, f"drop database if exists appdb;"
              f" drop database if exists {target_db};"
              f" create database {target_db}; use {target_db}; {schema}"
              " insert into orders values (1,'t1'),(2,'t2'),(99,'stale');"
              " insert into audit_log values (7,'own',1),(8,'own2',2);")


def _ids(table, db="appdb"):
    return _sql(DST, f"select group_concat(id order by id)"
                     f" from {db}.{table}")


@needs_docker
@have_tools
def test_the_move_replaces_the_target_rows(pair, tmp_path):
    """The whole point, and what the appending load got wrong: the stale
    row the source does not have is gone, and nothing arrives twice."""
    _seed()
    movers.mydumper_move(_hop(tmp_path), "appdb", 2, True, None)
    assert _ids("orders") == "1,2,3"
    assert _sql(DST, "select v from appdb.orders where id = 1") == "s1"


@needs_docker
@have_tools
def test_an_excluded_table_keeps_its_rows_and_is_not_carried(pair,
                                                             tmp_path):
    """Two things, measured together: the dump skips it, and the emptying
    skips it - even though it references a table that is emptied, which
    a plain `TRUNCATE` refuses with ERROR 1701."""
    _seed()
    lines = []
    steps = movers.mydumper_move(_hop(tmp_path, ["audit_log"]), "appdb", 2,
                                 True, lines.append)
    assert _ids("audit_log") == "7,8", "the target's own rows were touched"
    assert _ids("orders") == "1,2,3"
    assert any("1 tables the hop excludes are not dumped" in s
               for s in steps), steps
    assert any("except the ones the hop excludes" in s for s in steps), steps


@needs_docker
@have_tools
def test_the_password_is_never_printed(pair, tmp_path):
    """Measured before: one line of `move --go` output carried it."""
    _seed()
    lines = []
    steps = movers.mydumper_move(_hop(tmp_path, ["audit_log"]), "appdb", 2,
                                 True, lines.append)
    said = "\n".join(lines + steps)
    assert lines, "nothing was logged, so there was nothing to look at"
    assert SECRET not in said, [ln for ln in lines if SECRET in ln]


@needs_docker
@have_tools
def test_the_plan_is_the_command_that_runs(pair, tmp_path):
    """Built once. A plan assembled separately from the command line is how
    the plan kept saying `--trx-consistency-only` after the run could not."""
    _seed()
    hop = _hop(tmp_path, ["audit_log"])
    planned = movers.mydumper_move(hop, "appdb", 2, False, None)
    ran = movers.mydumper_move(hop, "appdb", 2, True, None)
    assert planned[:-1] == ran, (planned, ran)
    # the words and the command both: the plan shows one and runs the other
    assert [getattr(s, "argv", None) for s in planned[:-1]] == \
        [getattr(s, "argv", None) for s in ran]
    assert any(getattr(s, "argv", None) for s in ran), ran
    assert planned[-1] == "# dry-run, add --go to execute"


@needs_docker
@have_tools
def test_the_load_goes_to_the_targets_name_for_the_database(pair, tmp_path):
    """`-B` on the loader was the source's name, so a hop whose `db_map`
    lands the database under another name loaded into the wrong one."""
    _seed(target_db="appdb_new")
    movers.mydumper_move(_hop(tmp_path, db_map={"appdb": "appdb_new"}),
                         "appdb", 2, True, None)
    assert _ids("orders", "appdb_new") == "1,2,3"
    assert _sql(DST, "select count(*) from information_schema.schemata"
                     " where schema_name = 'appdb'") == "0", \
        "the loader created the source's name on the target instead"


@needs_docker
@have_tools
def test_an_unreachable_source_leaves_the_target_as_it_was(pair, tmp_path):
    """The target is emptied only once a complete dump is on disk. Emptied
    first, a source that cannot be read leaves nothing, and nothing to
    load."""
    _seed()
    with pytest.raises(RuntimeError):
        movers.mydumper_move(_hop(tmp_path, src_port=1), "appdb", 2, True,
                             None)
    assert _ids("orders") == "1,2,99"


@needs_docker
@have_tools
def test_an_exclusion_it_cannot_resolve_stops_before_anything(pair,
                                                               tmp_path):
    """With the target's excluded tables kept, a dump that carried them
    would load the source's rows on top of the target's own."""
    _seed()
    with pytest.raises(SystemExit) as e:
        movers.mydumper_move(_hop(tmp_path, ["audit_log"], src_port=1),
                             "appdb", 2, True, None)
    assert "cannot be told to skip them" in str(e.value), str(e.value)
    assert "Nothing has been changed" in str(e.value)
    assert _ids("orders") == "1,2,99" and _ids("audit_log") == "7,8"


# ---- asking the binary, not remembering it ----

@have_tools
def test_the_resolver_reads_only_the_option_column():
    """`--overwrite-tables` appears in the loader's help inside another
    option's description; the program itself answers `Unknown option`."""
    have = movers._long_options("myloader")
    assert "--overwrite-tables" not in have
    assert "--overwrite-unsafe" in have
    assert "--threads" in have
    text = subprocess.run(["myloader", "--help"], capture_output=True,
                          text=True, env=movers.tool_env(None))
    assert "--overwrite-tables" in text.stdout + text.stderr, \
        "the trap this guards against is no longer in the help text"


@have_tools
def test_the_spelling_comes_from_the_installed_build():
    assert movers.tool_flag("mydumper", "--trx-consistency-only",
                            "--trx-tables") in _dumper_has("trx")


def _dumper_has(word):
    return {o for o in movers._long_options("mydumper") if word in o}


@have_tools
def test_a_flag_the_build_lacks_is_refused_without_naming_it():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    with pytest.raises(SystemExit) as e:
        movers.tool_flag("myloader", "--purge-mode")
    said = str(e.value).lower()
    assert "--purge-mode" in said, said
    assert not [t for t in TOOLS if t in said], said


def _passwords_on_argv(src):
    """Lines where `_sh` is handed a command line that reads `.password`."""
    bad = []
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "_sh" and node.args):
            continue
        if any(isinstance(sub, ast.Attribute) and sub.attr == "password"
               for sub in ast.walk(node.args[0])):
            bad.append(node.lineno)
    return bad


def test_no_command_line_carries_a_password():
    """The guard. A secret on argv reaches the log `_sh` writes, the
    process list, and whatever the operator pastes into a ticket. The
    environment - `env`, the second argument - is where it belongs."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    assert not _passwords_on_argv(src), _passwords_on_argv(src)


def test_the_guard_would_have_caught_the_old_call():
    """Fed the line that used to be there, the same check finds it - and
    does not mistake the environment for the command line."""
    old = ('_sh(["mydumper", "-u", s.user, f"-p{s.password}", "-B", db],'
           ' None, log)')
    assert _passwords_on_argv(old) == [1]
    new = '_sh(dump, {"MYSQL_PWD": hop.source.password}, log)'
    assert _passwords_on_argv(new) == []


# ---- what an excluded table is left pointing at ----

@needs_docker
@have_tools
def test_rows_left_pointing_at_nothing_are_reported(pair, tmp_path):
    """The excluded table keeps its rows - that is the point - but a row of
    it can reference a row that only the target had. The load replaces the
    referenced table with the source's rows, that one does not come back,
    and `check` will never look: the hop excludes the table it is in.
    Nothing else can say it, so the move does."""
    _seed()
    _sql(DST, "insert into appdb.audit_log values (9,'points at a row only"
              " the target had',99)")
    lines = []
    movers.mydumper_move(_hop(tmp_path, ["audit_log"]), "appdb", 2, True,
                         lines.append)
    assert _ids("orders") == "1,2,3", "the stale row is still there"
    orphan = _sql(DST, "select count(*) from appdb.audit_log a left join"
                       " appdb.orders o on o.id = a.oid"
                       " where a.oid is not null and o.id is null")
    assert orphan == "1", "the premise did not hold"
    said = [ln for ln in lines if "audit_log" in ln and "orders" in ln]
    assert said, lines
    assert "1 row" in said[0] and "no longer exist" in said[0], said[0]


@needs_docker
@have_tools
def test_nothing_is_said_when_every_reference_still_resolves(pair,
                                                             tmp_path):
    _seed()
    lines = []
    movers.mydumper_move(_hop(tmp_path, ["audit_log"]), "appdb", 2, True,
                         lines.append)
    assert not [ln for ln in lines if "no longer exist" in ln], lines
