"""A MySQL event the target lacks is created by the repair, switched off.

The schema check named a missing event (`event 1/0 missing: ev`), and the
tool that writes the rest of the fix DDL does not model events - measured,
it called the pair clean. So the event was found and never made, and
recreating it was hands work. The repair now creates it from the source's
own definition, in the time zone and sql_mode it was defined in, switched
off: an event running on the target while rows are still being carried
rewrites them under the move. It is switched on at cutover. An event
defined differently is redefined, and the undo puts the target's back.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PORT = "migkit-test-events", 15728


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytestmark.append(pytest.mark.skip(reason="docker not available"))


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _eng(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="ev", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


# the client splits on semicolons, and the body has two of its own
EVENT = ("\ndelimiter //\ncreate event cx.tidy on schedule every 1 hour do"
         " begin delete from cx.t where v < 0; update cx.t set v = v + 1"
         " where v = 0; end //\ndelimiter ;\n")


def test_a_missing_event_is_made_switched_off(server, tmp_path):
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.t (id int primary key, v int);"
       " create table cy.t (id int primary key, v int);"
       " set time_zone = '+07:00';" + EVENT)
    eng = _eng(tmp_path)
    got = [a for a in eng.repair_plan("cx", "schema") if a.kind == "events"]
    assert got and "1 events the target lacks: tidy" in got[0].note, got
    eng.apply("cx", got[0])
    status, zone = my("select status, time_zone from information_schema"
                      ".events where event_schema = 'cy'"
                      " and event_name = 'tidy'").split("\t")
    assert status == "DISABLED", status
    assert zone == "+07:00", zone
    body = my("select event_definition from information_schema.events"
              " where event_schema = 'cy'")
    assert "update cx.t set v = v + 1" in body, body
    # nothing left to do once it is there, switched off or not
    assert not [a for a in eng.repair_plan("cx", "schema")
                if a.kind == "events"]


def test_an_event_defined_differently_is_redefined_and_undone(server,
                                                              tmp_path):
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.t (id int primary key, v int);"
       " create table cy.t (id int primary key, v int);" + EVENT
       + "create event cy.tidy on schedule every 1 day do"
         " delete from cy.t")
    eng = _eng(tmp_path)
    got = [a for a in eng.repair_plan("cx", "schema") if a.kind == "events"]
    assert got and "1 events defined differently: tidy" in got[0].note, got
    eng.apply("cx", got[0])
    assert my("select interval_field from information_schema.events"
              " where event_schema = 'cy'") == "HOUR"
    from migkit.engines.base import RepairAction
    eng.apply("cx", RepairAction("cx", "events", got[0].undo, [], ""))
    assert my("select interval_field from information_schema.events"
              " where event_schema = 'cy'") == "DAY"
