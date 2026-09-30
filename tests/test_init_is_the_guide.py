"""`migkit init` is the guide (backlog R21), end to end.

A PostgreSQL source (the shared `pg_pair`) and a MySQL target
(`migkit-test-ux-mysql` on 16200): `init` in a terminal - a real pty,
driven by pexpect - is asked two addresses and two passwords and where
to keep them, and nothing else; it says what is there, proposes the
plan, writes the smallest configuration and says the next command.
Without a terminal it takes the same from the environment; with neither
it writes the template, as it always has. Users that may not do what the
goal needs get a script for the DBA, and `doctor` asks again after it
has been run.

Measured here: 5 questions, and a hop of 4 lines (its name, the goal,
two addresses).
"""
import json
import os
import subprocess
import time
from pathlib import Path

import pytest
import yaml

from tests.conftest import migkit_cmd, needs_docker, psql

ROOT = Path(__file__).resolve().parents[1]
MYSQL = "migkit-test-ux-mysql"
MYSQL_PORT = 16200


def _my(sql, db=""):
    return subprocess.run(
        ["docker", "exec", "-i", MYSQL, "mysql", "-uroot", "-ptest", "-N",
         "-B"] + ([db] if db else []), input=sql, capture_output=True,
        text=True)


@pytest.fixture(scope="module")
def mysql_ux():
    subprocess.run(["docker", "rm", "-f", "-v", MYSQL], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MYSQL, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MYSQL_PORT}:3306", "mysql:8.4"],
                   check=True, capture_output=True)
    deadline = time.time() + 150
    while time.time() < deadline:
        if _my("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql did not come up")
    yield MYSQL_PORT
    subprocess.run(["docker", "rm", "-f", "-v", MYSQL], capture_output=True)


@pytest.fixture
def shop(pg_pair, mysql_ux):
    """A source database with a keyed table, a table without a key, and
    columns that do not cross to MySQL as they are."""
    src = pg_pair["src"]
    psql(src, "drop database if exists shop with (force)")
    psql(src, "create database shop")
    got = psql(src, """
        create table orders (id bigint primary key,
                             placed timestamptz not null,
                             total numeric(12,2), note text,
                             wait interval);
        insert into orders select g, now() - g * interval '1 hour',
               g * 1.5, 'n' || g, interval '1 day'
          from generate_series(1, 500) g;
        create table events (at timestamp, what varchar(40));
        insert into events select now(), 'e' || g
          from generate_series(1, 50) g;
        analyze;""", db="shop")
    assert got.returncode == 0, got.stderr
    yield {"pg": src, "my": mysql_ux}
    psql(src, "drop database if exists shop with (force)")
    for role in ("reader",):
        psql(src, f"drop role if exists {role}")
    _my("drop database if exists shop; drop database if exists shop2;"
        " drop database if exists shop3; drop user if exists 'app'@'%';")


def _env(tmp_path, **extra):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("MIGKIT_")}
    env.update(PYTHONPATH=str(ROOT), MIGKIT_REPORTS=str(tmp_path / "r"),
               MIGKIT_TEST_KEYCHAIN=str(tmp_path / "keys.json"),
               PYTHON_KEYRING_BACKEND="tests.keychain_for_tests.FileKeyring",
               COLUMNS="300", **extra)
    return env


def _plain(text):
    """What a person reads: the console's markup and a terminal's escape
    codes taken out, its line ends made one."""
    import re
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
    text = re.sub(r"\[/?(bold|yellow|red|green|dim)\]", "", text)
    return text.replace("\r\n", "\n")


def _no_program_named(text):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    return [t for t in TOOLS if t in text.lower()]


@needs_docker
def test_a_terminal_is_asked_two_addresses_and_two_passwords(shop,
                                                              tmp_path):
    import pexpect
    cmd = migkit_cmd()
    child = pexpect.spawn(cmd[0], cmd[1:] + ["init", "--here"],
                          cwd=str(tmp_path), env=_env(tmp_path),
                          encoding="utf-8", timeout=180,
                          dimensions=(60, 300))
    for prompt, answer in (
            ("Where is the data now\\?",
             f"postgres://postgres@127.0.0.1:{shop['pg']}/shop"),
            ("Where should it go\\?",
             f"mysql://root@127.0.0.1:{shop['my']}/shop"),
            ("Password for postgres@127.0.0.1", "test"),
            ("Password for root@127.0.0.1", "test"),
            ("keychain \\(k\\)", "k")):
        child.expect(prompt)
        child.sendline(answer)
    # nothing else is asked: a sixth question would wait on the terminal
    # until the timeout, and this would fail on it instead of the end
    child.expect(pexpect.EOF)
    out = _plain(child.before)
    child.close()
    assert child.exitstatus == 0, out
    for said in ("what is there", "source: postgres 16", "target: mysql 8.4",
                 "run by its owner", "a primary",
                 "2 tables in shop: about 550 rows",
                 "1 without a key (public.events)",
                 "public.orders.wait interval: migkit cannot compare it",
                 "public.orders.placed timestamp with time zone: becomes"
                 " datetime(6) - read as instant on the source, as wall"
                 " clock on the target",
                 "the source's changes can be followed",
                 "the users may read and write what migkit needs",
                 "the plan: cutover", "carries 2 tables",
                 "nothing for the DBA to do first",
                 "wrote hop shop", "(4 lines)",
                 "migkit sync shop --mode migrate"):
        assert said in out, (said, out)
    assert not _no_program_named(out), out
    conf = tmp_path / "conf" / "hops.yaml"
    assert oct(conf.stat().st_mode)[-3:] == "600"
    hop = yaml.safe_load(conf.read_text())["hops"]["shop"]
    assert hop == {
        "goal": "cutover",
        "source": f"postgres://postgres@127.0.0.1:{shop['pg']}/shop",
        "target": f"mysql://root@127.0.0.1:{shop['my']}/shop"}, hop
    # the passwords are in the keychain, and nowhere in the file
    kept = json.loads((tmp_path / "keys.json").read_text())
    assert kept == {"migkit/shop.source": "test",
                    "migkit/shop.target": "test"}
    assert "test" not in conf.read_text().replace("migkit-test", "")
    # and the file it wrote works: doctor reaches both with them
    r = subprocess.run(migkit_cmd() + ["doctor"], capture_output=True,
                       text=True, env=_env(tmp_path, MIGKIT_CONF=str(conf)))
    assert "shop src: ok" in r.stdout and "shop dst: ok" in r.stdout, \
        r.stdout + r.stderr
    assert "the servers allow everything goal cutover needs" in r.stdout, \
        r.stdout


@needs_docker
def test_users_that_may_not_are_given_a_script_and_asked_again(shop,
                                                               tmp_path):
    psql(shop["pg"], "drop role if exists reader;"
                     " create role reader login password 'rpw'")
    assert _my("create user 'app'@'%' identified by 'apw'").returncode == 0
    env = _env(tmp_path,
               MIGKIT_SOURCE=f"postgres://reader@127.0.0.1:{shop['pg']}"
                             "/shop",
               MIGKIT_TARGET=f"mysql://app@127.0.0.1:{shop['my']}/shop2",
               MIGKIT_SOURCE_PASSWORD="rpw", MIGKIT_TARGET_PASSWORD="apw",
               MIGKIT_GOAL="cutover", MIGKIT_HOP="locked")
    r = subprocess.run(migkit_cmd() + ["init", "--here"], cwd=tmp_path,
                       stdin=subprocess.DEVNULL, capture_output=True,
                       text=True, env=env)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "Where" not in out, out
    hop = yaml.safe_load((tmp_path / "conf" / "hops.yaml").read_text())[
        "hops"]["locked"]
    # without a terminal the passwords stay where they were given
    assert hop["source"] == (f"postgres://reader:env:MIGKIT_SOURCE_PASSWORD"
                             f"@127.0.0.1:{shop['pg']}/shop"), hop
    src = tmp_path / "r" / "locked" / "for-the-dba-source.sql"
    dst = tmp_path / "r" / "locked" / "for-the-dba-target.sql"
    assert f"the source's script, for the DBA: {src}" in out, out
    script = src.read_text()
    for line in ("grant select on public.orders to reader;",
                 "grant select on public.events to reader;",
                 "alter role reader with replication;"):
        assert line in script, script
    # least privilege: nothing broader than what was missing
    assert "superuser" not in script and "all tables" not in script
    target = dst.read_text()
    assert "create database if not exists `shop2`;" in target, target
    assert "on `shop2`.* to 'app'@'%';" in target, target
    assert not _no_program_named(out + script + target)
    # the DBA runs them; doctor asks again, and the scripts go
    got = subprocess.run(["docker", "exec", "-i", "migkit-test-pg-src",
                          "psql", "-U", "postgres", "-d", "shop", "-v",
                          "ON_ERROR_STOP=1"], input=script,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    got = _my(target)
    assert got.returncode == 0, got.stderr
    r = subprocess.run(migkit_cmd() + ["doctor"], capture_output=True,
                       text=True, env={**env, "MIGKIT_CONF": str(
                           tmp_path / "conf" / "hops.yaml")})
    assert "the servers allow everything goal cutover needs" in r.stdout, \
        r.stdout + r.stderr
    assert not src.exists() and not dst.exists()
    psql(shop["pg"], "revoke all on all tables in schema public from reader",
         db="shop")


@needs_docker
def test_another_tools_task_as_the_source(shop, tmp_path):
    task = json.loads((ROOT / "tests" / "samples" / "dms-task.json")
                      .read_text())
    task["Endpoints"][0].update(ServerName="127.0.0.1", Port=shop["pg"],
                                Username="postgres", DatabaseName="shop",
                                SslMode="none")
    task["Endpoints"][1].update(EngineName="mysql", ServerName="127.0.0.1",
                                Port=shop["my"], Username="root",
                                DatabaseName="shop3", SslMode="none")
    rules = json.loads(task["ReplicationTasks"][0]["TableMappings"])
    # the sample's rename and filter are for its own tables
    rules["rules"] = [r for r in rules["rules"] if r["rule-id"] in ("1",)]
    rules["rules"][0].pop("filters")
    task["ReplicationTasks"][0]["TableMappings"] = json.dumps(rules)
    path = tmp_path / "dms-task.json"
    path.write_text(json.dumps(task))
    env = _env(tmp_path, MIGKIT_SOURCE=str(path),
               MIGKIT_SOURCE_PASSWORD="test", MIGKIT_TARGET_PASSWORD="test")
    r = subprocess.run(migkit_cmd() + ["init", "--here"], cwd=tmp_path,
                       stdin=subprocess.DEVNULL, capture_output=True,
                       text=True, env=env)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "read " in out and "as hop orders-to-aurora" in out, out
    assert "the task took 1 of the source's 2 tables; left out:" \
        " public.events" in out, out
    hop = yaml.safe_load((tmp_path / "conf" / "hops.yaml").read_text())[
        "hops"]["orders-to-aurora"]
    assert hop["goal"] == "cutover"
    assert hop["exclude"] == ["public.events"], hop
    assert hop["target"].endswith(f"@127.0.0.1:{shop['my']}/shop3"), hop
    assert not _no_program_named(out)


def test_no_terminal_and_no_addresses_writes_the_template(tmp_path,
                                                         monkeypatch):
    from click.testing import CliRunner

    from migkit import cli
    from migkit.config import load_hops
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MIGKIT_SOURCE", raising=False)
    r = CliRunner().invoke(cli.main, ["init", "--here"])
    assert r.exit_code == 0, r.output
    conf = tmp_path / "conf" / "hops.yaml"
    text = conf.read_text()
    assert text == cli.STARTER_CONFIG
    body = [l for l in text.splitlines() if not l.strip().startswith("#")]
    for knob in ("workers", "big_rows", "slice", "password", "engine"):
        assert not [l for l in body if knob in l], (knob, body)
    # the hop is its name and two addresses (it was 16 lines)
    assert body == ["hops:", "  my-hop:",
                    "    source: postgres://app@source.example.com:5432/shop",
                    "    target: postgres://app@target.example.com:5432/shop"]
    hop = load_hops(conf)["my-hop"]
    assert hop.engine == "postgres" and not hop.workers_set
    # and a second run refuses to overwrite it, as it always has
    r = CliRunner().invoke(cli.main, ["init", "--here"])
    assert r.exit_code != 0 and "already exists" in r.output


def _sqlite(path, rows):
    import sqlite3
    conn = sqlite3.connect(path)
    conn.execute("create table items (id integer primary key, name text)")
    conn.execute("create table log (at text, what text)")
    conn.executemany("insert into items values (?, ?)",
                     [(i, f"n{i}") for i in range(rows)])
    conn.commit()
    conn.close()


def _guide(tmp_path, answers, monkeypatch):
    import migkit.config as cfg
    from migkit import guide
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "r")
    asked, said = [], []

    def ask(question, secret=False, default=""):
        asked.append(question)
        return answers.pop(0)
    got = guide.run(ask, said.append, tmp_path / "conf" / "hops.yaml",
                    env={})
    return got, asked, _plain("\n".join(map(str, said)))


def test_a_pair_of_files_asks_only_the_two_addresses(tmp_path, monkeypatch):
    """Files sign in with nothing: two questions, a hop of 3 lines, a copy
    (the target holds nothing yet)."""
    from migkit.config import load_hops
    _sqlite(tmp_path / "a.db", 20)
    got, asked, out = _guide(tmp_path, [f"sqlite://{tmp_path}/a.db",
                                        f"sqlite://{tmp_path}/b.db"],
                             monkeypatch)
    assert asked == [
        "Where is the data now? An address like"
        " postgres://app@db1:5432/shop, or a task file from another tool",
        "Where should it go? An address like mysql://app@db2/shop"], asked
    assert got["goal"] == "copy" and got["name"] == "a"
    assert "the plan: copy" in out and "carries 2 tables" in out, out
    assert "1 without a key" in out, out
    assert "wrote hop a" in out and "(3 lines)" in out, out
    assert "migkit move a --mode full" in out
    hop = load_hops(tmp_path / "conf" / "hops.yaml")["a"]
    assert hop.engine == "sqlite" and hop.goal == ""


def test_a_target_that_already_holds_the_tables_is_verified(tmp_path,
                                                            monkeypatch):
    from migkit.config import load_hops
    _sqlite(tmp_path / "a.db", 20)
    _sqlite(tmp_path / "b.db", 20)
    got, asked, out = _guide(tmp_path, [f"sqlite://{tmp_path}/a.db",
                                        f"sqlite://{tmp_path}/b.db"],
                             monkeypatch)
    assert got["goal"] == "verify", out
    assert "the plan: verify" in out and "migkit check a" in out, out
    hop = load_hops(tmp_path / "conf" / "hops.yaml")["a"]
    assert hop.goal == "verify"
    # a second hop is added to the file beside the first, not over it
    got, _, out = _guide(tmp_path, [f"sqlite://{tmp_path}/b.db",
                                    f"sqlite://{tmp_path}/c.db"],
                         monkeypatch)
    hops = load_hops(tmp_path / "conf" / "hops.yaml")
    assert set(hops) == {"a", "b"} and hops["b"].goal == ""


def test_a_wrong_address_is_asked_again(tmp_path, monkeypatch):
    _sqlite(tmp_path / "a.db", 3)
    got, asked, out = _guide(tmp_path, ["postgress://app@db1/shop",
                                        f"sqlite://{tmp_path}/a.db",
                                        f"sqlite://{tmp_path}/b.db"],
                             monkeypatch)
    assert len(asked) == 3 and asked[0] == asked[1]
    assert "did you mean postgres://" in out
