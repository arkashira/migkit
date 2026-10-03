"""A target that runs the source's own code, as one more candidate proved
like any other (backlog CV.P9): Oracle's PL/SQL made on MariaDB in its
Oracle mode, as Oracle wrote it, and held to Oracle's answers.

The source is the shared Oracle Free, its sandbox an Oracle of the
module's own; the target and its sandbox the shared MariaDB pair. A
procedure no other candidate converts to MySQL's SQL is proved by the
emulation; one the emulation reads otherwise than Oracle is not
accepted for having been made.
"""
import json

import pytest

from tests import servers
from tests.conftest import needs_docker
from tests.waits import until

ora = servers.fixture("oracle")
maria = servers.fixture("mariadb")

SOURCE = [
    "create table notes (id number(10) primary key, msg varchar2(40))",
    "insert into notes values (1, 'first')",
    """create or replace procedure add_note(p_id number, p_msg varchar2) is
begin
  insert into notes (id, msg) values (p_id, p_msg);
end add_note;""",
    """create or replace function shout(p_msg varchar2) return varchar2 is
  v varchar2(60);
begin
  v := upper(p_msg) || '!';
  return v;
end shout;""",
]


def _run(server, statements):
    import oracledb
    conn = oracledb.connect(user=server.user, password=server.password,
                            dsn=f"{server.host}:{server.port}/FREEPDB1")
    try:
        cur = conn.cursor()
        for s in statements:
            cur.execute(s)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def converted(ora, maria, tmp_path_factory):
    from migkit.config import Hop
    from migkit.engines.hetero import HeteroEngine
    from migkit.storedcode import convert
    tmp = tmp_path_factory.mktemp("emulated")
    db = ora.name
    _run(ora.one, [f'alter session set current_schema = "{db.upper()}"']
         + SOURCE)
    maria.src.sql("create table notes (id int primary key, msg"
                  " varchar(40))", db)
    maria.src.sql("insert into notes values (1, 'first')", db)
    image = servers.PROFILES["oracle"]
    with servers.private(__name__, "sandbox", image.image, ports=(1521,),
                         env=dict(image.env)) as (_, ports):
        sandbox = servers.Server(image, "one", "sandbox", ports[1521])
        until(lambda: _run(sandbox, ["select 1 from dual"]) or True,
              image.start_s, "the Oracle sandbox")
        hop = Hop(name="emulated", engine="hetero", options={
            "source_engine": "oracle", "target_engine": "mysql",
            "proof_sandbox": {
                "source": {"host": servers.HOST, "port": ports[1521],
                           "user": image.user, "password": image.password},
                "target": {"host": maria.dst.host, "port": maria.dst.port,
                           "user": maria.dst.user,
                           "password": maria.dst.password}},
            "code_dir": str(tmp / "none")},
            source=ora.one.endpoint(), target=maria.src.endpoint(),
            databases=[db])
        hop.report_dir = lambda d=None: tmp
        eng = HeteroEngine(hop)
        # the target says what it is
        assert convert.emulating(eng.dst_engine, "dst", db) == {"mariadb"}
        eng.converted_code(db)
        yield json.loads((tmp / "code-conversion.json").read_text())


@needs_docker
def test_a_procedure_is_proved_as_oracle_wrote_it(converted):
    assert converted["sandbox"] == "used", converted["sandbox"]
    record = {r["routine"]: r for r in converted["routines"]}
    r = record["procedure add_note"]
    assert r["class"] in ("proved", "proved with caveats"), r
    assert r["rung"] == "emulator", r


@needs_docker
def test_a_function_the_emulation_reads_otherwise_is_not_taken_on_trust(
        converted):
    record = {r["routine"]: r for r in converted["routines"]}
    r = record["function shout"]
    # proved by calls made on both sides, or not converted with the
    # emulation's own failure said - never accepted without a run
    proved = r["class"] in ("proved", "proved with caveats") and \
        r["evidence"]["calls"] > 0
    refused = r["class"] == "not converted" and any(
        "the target's own emulation" in f for f in r.get("reasons", []))
    assert proved or refused, r
