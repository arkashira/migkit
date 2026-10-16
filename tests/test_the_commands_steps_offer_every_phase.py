"""`migrate`'s own steps (`cli._Steps`) offer what the flow
(`autonomous.run`) composes from: the pair whose gaps are said before any
costly step, the dependency facts the order of the phases is chosen from
(`decide.phase_order`), and a schema and a constraints phase apart from
the copy where the engine has its own ways for them. The copy's schedule
reads the source's one shared moment once a database (backlog
P0-MINIMAL-INPUT.ownership, P0-DECISION-ALL.engines)."""
import json
import os
import subprocess
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from migkit import autonomous, cli, decide, durable
from tests.test_autonomous_workflow import Fake, _run

FK = ("public.orders", ["customer"], "public.customers", ["id"])


class Apart(Fake):
    def __init__(self, carries=None, **kw):
        super().__init__(dbs=("a",), **kw)
        self.carries = carries
        self.eng = None

    def pair(self):
        return ("postgres", "postgres")

    def order(self, db):
        return {"foreign keys": ["public.orders -> public.customers"],
                "fk cycle": False,
                "parents first": ["public.customers", "public.orders"],
                "target keys": True, "schema carries keys": self.carries}

    def schema(self, db):
        self.calls.append(("schema", db))

    def constraints(self, db):
        self.calls.append(("constraints", db))


def test_the_order_is_chosen_from_the_foreign_keys_handed_in(tmp_path):
    ops = Apart()
    got, said = _run("copy", ops, tmp_path)
    assert got.complete
    # the pair's gaps are said before any step
    assert said[0].startswith("postgres (source and target) lacks")
    order = next(d for d in autonomous.evidence(tmp_path / "workflow.json")
                 if d["what"] == "order")
    # no cycle among the keys: the schema whole, parents loaded first
    assert order["way"] == "schema-whole"
    assert order["facts"]["fk cycle"] is False
    assert order["keys"] == ["public.orders -> public.customers"]
    first = order["first"]
    assert first.index("public.customers") < first.index("public.orders")
    steps = [c[0] for c in ops.calls]
    assert steps.index("schema") < steps.index("move")
    # the schema way carries the keys: no phase of its own after the rows
    assert "constraints" not in steps


def test_the_copy_carries_no_schema_work_where_the_schema_step_ran():
    facts = Apart(carries=False).order("a")
    _, phases = autonomous.compose("copy", ["a"], ["schema", "constraints"],
                                   {"a": facts})
    by = {p.step: p for p in phases}
    assert "made by the copy" not in by["move"].outcome
    keys = [p.step for p in phases]
    assert keys.index("schema") < keys.index("move") < \
        keys.index("constraints")
    # without the steps the copy says it carries both
    _, carried = autonomous.compose("copy", ["a"], [], {"a": facts})
    assert "made by the copy" in {p.step: p for p in carried}["move"].outcome


def test_keys_left_for_after_by_the_schema_way_get_their_own_phase(
        tmp_path):
    ops = Apart(carries=False)
    got, _ = _run("copy", ops, tmp_path)
    steps = [c[0] for c in ops.calls]
    assert steps.index("schema") < steps.index("move") < \
        steps.index("constraints")
    units = durable.read_state(tmp_path / "workflow.json", "state")["units"]
    assert units["schema:a"]["state"] == units["constraints:a"]["state"] \
        == "done"


class _Eng:
    LOAD_SKIPS_KEYS = False

    def __init__(self, root):
        self.root, self.finished, self.made = root, [], []

    def foreign_keys(self, db):
        return [FK, ("public.self", ["up"], "public.self", ["id"])]

    def create_missing(self, db, log=None):
        self.made.append(db)
        (self.root / "created-tables.json").write_text(json.dumps(
            {"tables": ["public.customers", "public.orders"],
             "whole": True}))

    def finish_created(self, db, log=None):
        self.finished.append(db)
        (self.root / "created-tables.json").unlink()


def _steps(tmp_path, engine="postgres"):
    s = cli._Steps.__new__(cli._Steps)
    s.name, s.goal = "t", "copy"
    s.hop = SimpleNamespace(engine=engine, report_dir=lambda db=None: tmp_path)
    s.eng = _Eng(tmp_path)
    return s


def test_the_command_s_steps_say_the_pair_the_order_and_the_phases(
        tmp_path):
    s = _steps(tmp_path)
    assert s.pair() == ("postgres", "postgres")
    f = s.order("a")
    assert f["foreign keys"] == ["public.orders -> public.customers"]
    assert f["parents first"] == ["public.customers", "public.orders"]
    assert f["fk cycle"] is False and f["target keys"] is True
    assert f["schema carries keys"] is False
    c = decide.phase_order(f)
    assert c.path == "schema-whole" and "fk cycle" in c.asked
    assert callable(s.schema) and callable(s.constraints)
    assert s.schema("a") == {"tables made": 2}
    # the record the copy reads is not there while it runs: it does not
    # put the keys on itself
    assert not (tmp_path / "created-tables.json").exists()
    assert (tmp_path / "created-tables.flow.json").exists()
    assert s.constraints("a") == {"tables finished": 2}
    assert s.eng.finished == ["a"]
    assert not (tmp_path / "created-tables.flow.json").exists()


def test_a_cycle_or_an_unread_key_is_not_taken_as_none(tmp_path):
    s = _steps(tmp_path)
    s.eng.foreign_keys = lambda db: [("a", [], "b", []), ("b", [], "a", [])]
    assert s.order("x")["fk cycle"] is True
    assert decide.phase_order(s.order("x")).path == "constraints-after"

    def unread(db):
        raise RuntimeError("catalogue not read")
    s.eng.foreign_keys = unread
    assert s.order("x")["fk cycle"] is None


def test_an_engine_without_its_own_ways_offers_no_phase_apart(tmp_path):
    s = _steps(tmp_path, engine="mysql")
    assert s.schema is None and s.constraints is None
    assert s.order("a")["schema carries keys"] is None


def test_the_copy_schedule_reads_the_source_s_moment_once_a_database(
        monkeypatch):
    from migkit import guarantees
    asked, sched = [], []

    class Src:
        def shared_snapshot(self, db):
            asked.append(db)
            return f"one moment of {db}"
    eng = SimpleNamespace(src_engine=Src(), dst_engine=SimpleNamespace())
    monkeypatch.setattr(guarantees, "copy_schedule",
                        lambda *a: sched.append(a) or SimpleNamespace(
                            checkpoint="after durable"))
    monkeypatch.setattr(decide, "admit", lambda *a: (True, ""))
    jp = SimpleNamespace(semantics=object())
    assert cli._copy_admitted(None, eng, "a", jp, 4, False, print) == 4
    assert sched[0][1] == "one moment of a"
    assert cli._snapshot_of(eng.src_engine, "a") == "one moment of a"
    cli._snapshot_of(eng.src_engine, "b")
    assert asked == ["a", "b"]


from tests.conftest import migkit_cmd, needs_docker, psql  # noqa: E402

DB = "w16c"
SEED = """
create table customers (id int primary key, name text);
create table orders (id int primary key,
                     customer int not null references customers(id));
insert into customers select g, 'c' || g from generate_series(1, 200) g;
insert into orders select g, 1 + g % 200 from generate_series(1, 1000) g;
"""


@needs_docker
@pytest.mark.xfail(strict=True, reason=(
    "W16C-SCHEMA: on the pg sandbox the schema phase's dump of the source"
    " ends with 'the server could not be reached ... before or while"
    " processing the request', then 'a table or column changed under the"
    " work', and the phase is held after 4 tries"))
def test_migrate_runs_the_schema_and_the_keys_as_phases_on_a_server(
        pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"drop database if exists {DB}")
        assert psql(port, f"create database {DB}").returncode == 0
    assert psql(pg_pair["src"], SEED, DB).returncode == 0
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [{DB}]
    """))
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("MIGKIT_")}
    env.update(MIGKIT_CONF=str(conf), MIGKIT_REPORTS=str(tmp_path / "r"),
               PYTHONPATH=str(Path(__file__).resolve().parent.parent))
    r = subprocess.run(migkit_cmd() + ["migrate", "--hop", "t", "--goal",
                                       "copy", "--go"],
                       capture_output=True, text=True, env=env,
                       stdin=subprocess.DEVNULL, timeout=600)
    out = r.stdout.splitlines()
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-2000:]
    gap = next(i for i, x in enumerate(out) if "lacks the" in x)
    copy = next(i for i, x in enumerate(out) if "copy" in x.lower())
    assert gap < copy, out[:20]
    wf = next((tmp_path / "r").rglob("workflow.json"))
    units = durable.read_state(wf, "state")["units"]
    s, m, k = (units[f"{x}:{DB}"] for x in ("schema", "move", "constraints"))
    assert s["state"] == m["state"] == k["state"] == "done", units
    # the keys put on after the copy ended
    assert s["at"] <= m["at"] <= k["at"] - k["took"]
    dst = pg_pair["dst"]
    assert psql(dst, "select count(*) from pg_constraint where contype ="
                " 'f'", DB).stdout.strip() == "1"
    assert psql(dst, "select (select count(distinct id) from customers),"
                " (select count(*) from customers), (select count("
                "distinct id) from orders), (select count(*) from orders)",
                DB).stdout.strip() == "200|200|1000|1000"
    for port in (pg_pair["src"], dst):
        psql(port, f"drop database if exists {DB}")
