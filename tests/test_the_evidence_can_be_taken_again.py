"""The evidence of a migration, exported so anyone can take it again
(backlog R23.1).

Not a certificate: what migkit measured, per run and for the hop's whole
history, in the forms people and tools take (HTML, Markdown, JSON, CSV).
Every table's rows and digest on each side are written with the way they
were taken, so a reader runs the statement on each server - or migkit's
own reader - and gets the same numbers; the repair's statements, the
decisions and their reasons, the approvals and the record they are all
chained in go with them; the bundle's own digest is added to the record,
and it is signed and encrypted where the hop asks.
"""
import hashlib
import json
import sqlite3
import subprocess

import pytest
from click.testing import CliRunner

from migkit import evidence, export
from tests.conftest import needs_docker, psql


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _lite(tmp_path, monkeypatch, src_rows, dst_rows, options=""):
    import migkit.config as cfg
    for name, rows in (("a.db", src_rows), ("b.db", dst_rows)):
        with sqlite3.connect(tmp_path / name) as con:
            con.execute("create table people (id integer primary key,"
                        " name text)")
            con.executemany("insert into people values (?, ?)", rows)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}}}\n"
        "    databases: [main]\n"
        + (f"    options: {options}\n" if options else ""))
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path / "reports" / "lite"


def _runs(root):
    return sorted(p for p in (root / "evidence").iterdir() if p.is_dir())


def _record(root):
    return [json.loads(x) for x in
            (root / "changelog.jsonl").read_text().splitlines()]


ROWS = [(1, "ann"), (2, "bo"), (3, "cy")]


def test_a_check_leaves_the_evidence_of_its_run(tmp_path, monkeypatch):
    root = _lite(tmp_path, monkeypatch, ROWS, ROWS[:2])
    got, said = _run("check", "lite")
    assert got.exit_code == 1, said
    runs = _runs(root)
    assert len(runs) == 1, runs
    for name in export.FILES:
        assert (runs[0] / name).is_file(), name
    doc = json.loads((runs[0] / "evidence.json").read_text())
    assert doc["scope"] == "run" and doc["session"] == runs[0].name
    assert doc["verdict"]["status"] == "different", doc["verdict"]
    table = next(t for t in doc["tables"] if t["table"] == "people")
    assert table["status"] == "different", table
    assert (table["src"]["rows"], table["dst"]["rows"]) == (3, 2), table
    assert table["method"] == "read" and table["src"]["recompute"], table
    # the numbers are the ones migkit's own reader takes again
    from migkit.config import get_hop
    from migkit.engines.sqlite import SQLiteEngine
    eng = SQLiteEngine(get_hop("lite"))
    for side, rows in (("src", 3), ("dst", 2)):
        digest, n, err = eng._hash(side, "people")
        assert not err and n == rows, (side, err)
        assert table[side]["digest"] == digest, (side, table)
    # the finding is said beside the numbers, and how to take them again
    assert any(c["status"] == "diff" for c in doc["checks"]), doc["checks"]
    assert doc["method"]["methods"]["read"], doc["method"]
    csv = (runs[0] / "tables.csv").read_text().splitlines()
    assert csv[0].startswith("db,table,where,status,method"), csv
    assert any(line.startswith("main,people,,different,read,sqlite,3,")
               for line in csv), csv
    md = (runs[0] / "evidence.md").read_text()
    assert "| main | people | different | 3 |" in md, md
    page = (runs[0] / "evidence.html").read_text()
    assert "counts and digests of both sides" in page and "people" in page
    # anchored: the bundle's digest is in the hop's record, chained
    anchor = _record(root)[-1]
    assert anchor["op"] == "evidence" and anchor["scope"] == "run", anchor
    raw = (runs[0] / "evidence.json").read_bytes()
    assert anchor["files"]["evidence.json"] == hashlib.sha256(
        raw).hexdigest(), anchor
    assert anchor["plain_sha256"] == hashlib.sha256(raw).hexdigest()
    from migkit import audit
    n, chained, broken = audit.verify(root / "changelog.jsonl")
    assert broken == "" and chained == n, (n, chained, broken)


def test_the_hops_whole_history_and_a_changed_record(tmp_path,
                                                      monkeypatch):
    root = _lite(tmp_path, monkeypatch, ROWS, ROWS[:2])
    _run("check", "lite")
    with sqlite3.connect(tmp_path / "b.db") as con:
        con.execute("insert into people values (3, 'cy')")
    got, said = _run("check", "lite")
    assert got.exit_code == 0, said
    assert len(_runs(root)) == 2
    got, said = _run("report", "lite")
    assert got.exit_code == 0, said
    assert "evidence:" in said, said
    doc = json.loads((root / "evidence" / "evidence.json").read_text())
    assert doc["scope"] == "hop" and doc["session"] is None
    assert doc["verdict"]["status"] == "same", doc["verdict"]
    table = next(t for t in doc["tables"] if t["table"] == "people")
    # the latest digest of each side, from the run that found them equal
    assert table["status"] == "same" and table["dst"]["rows"] == 3, table
    assert doc["record"]["whole"] is True, doc["record"]
    assert [s["op"] for s in doc["steps"]].count("evidence") == 2, \
        doc["steps"]
    # an entry changed afterwards shows in the next export
    lines = (root / "changelog.jsonl").read_text().splitlines()
    first = json.loads(lines[0])
    first["who"] = "someone else"
    lines[0] = json.dumps(first)
    (root / "changelog.jsonl").write_text("\n".join(lines) + "\n")
    from migkit.config import get_hop
    again = export.gather(get_hop("lite"))
    assert again["record"]["whole"] is False, again["record"]
    assert "entry 1 was changed" in again["record"]["broken"], \
        again["record"]
    assert 'class="banner fail"' in export.page(again)


def test_a_repair_carries_its_statements(tmp_path, monkeypatch):
    root = _lite(tmp_path, monkeypatch, ROWS, ROWS[:2])
    _run("check", "lite")
    got, said = _run("sync", "lite", "--apply")
    assert got.exit_code == 0, said
    with sqlite3.connect(tmp_path / "b.db") as con:
        assert con.execute("select count(*) from people").fetchone() == (3,)
    docs = [json.loads((r / "evidence.json").read_text())
            for r in _runs(root)]
    # the check's run, and the repair's
    assert len(docs) == 2, docs
    doc = next(d for d in docs if d["repairs"])
    assert doc["verdict"] is None, doc["verdict"]
    repairs = [r for r in doc["repairs"] if r["op"] == "repair"]
    assert repairs, doc["repairs"]
    applied = repairs[0]["applied"]
    kept = root / applied["file"]
    assert kept.is_file(), applied
    text = kept.read_text()
    assert applied["sha256"] == hashlib.sha256(kept.read_bytes()
                                               ).hexdigest()
    assert applied["statements"] >= 1 and "people" in text, (applied, text)
    # the record holds the statements' digest too
    entry = next(e for e in _record(root) if e.get("op") == "repair")
    assert entry["applied_sha256"] == hashlib.sha256(
        text.encode()).hexdigest(), entry


def test_the_evidence_is_encrypted_where_the_hop_asks(tmp_path,
                                                      monkeypatch):
    import pyrage
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    root = _lite(tmp_path, monkeypatch, ROWS, ROWS[:2],
                 f"{{at_rest: {{recipients: ['{who.to_public()}']}}}}")
    got, said = _run("check", "lite")
    assert got.exit_code == 1, said
    run = _runs(root)[0]
    for name in export.FILES:
        raw = (run / name).read_bytes()
        assert raw.startswith(evidence.AGE), (name, raw[:30])
        assert b"people" not in raw, name
    plain = pyrage.decrypt((run / "evidence.json").read_bytes(), [who])
    anchor = _record(root)[-1]
    assert anchor["plain_sha256"] == hashlib.sha256(plain).hexdigest()
    assert anchor["files"]["evidence.json"] == hashlib.sha256(
        (run / "evidence.json").read_bytes()).hexdigest()
    assert json.loads(plain)["at_rest"]["encrypted_to"] == 1


@pytest.mark.skipif(subprocess.run(["which", "ssh-keygen"],
                                   capture_output=True).returncode,
                    reason="no ssh-keygen here to sign with")
def test_the_evidence_is_signed_with_the_hops_key(tmp_path, monkeypatch):
    key = tmp_path / "signer"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(key), "-C", "ann@example.com"], check=True)
    root = _lite(tmp_path, monkeypatch, ROWS, ROWS,
                 f"{{evidence: {{sign: '{key}'}}}}")
    got, said = _run("check", "lite")
    assert got.exit_code == 0, said
    run = _runs(root)[0]
    body, sig = run / "evidence.json", run / "evidence.json.sig"
    assert sig.is_file(), list(run.iterdir())
    doc = json.loads(body.read_text())
    assert doc["signature"]["namespace"] == export.NAMESPACE
    allowed = tmp_path / "allowed_signers"
    allowed.write_text("ann@example.com "
                       + (tmp_path / "signer.pub").read_text())

    def verify():
        with open(body, "rb") as f:
            return subprocess.run(
                ["ssh-keygen", "-Y", "verify", "-f", str(allowed), "-I",
                 "ann@example.com", "-n", export.NAMESPACE, "-s", str(sig)],
                stdin=f, capture_output=True).returncode
    assert verify() == 0
    body.write_text(body.read_text().replace('"same"', '"different"'))
    assert verify() != 0


def test_the_ways_chosen_are_kept_in_the_operators_words(tmp_path):
    """A way the code labels by a program's name is kept by what it does;
    a way given up for another is a fallback, told once; a way asked of
    in passing is no decision."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import threading

    from migkit import decide, session
    from migkit.config import Endpoint, Hop
    got = []

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            got.append(json.loads(self.rfile.read(n)))
            self.send_response(200)
            self.end_headers()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    ep = Endpoint(host="x", port=0, user="", password="")
    hop = Hop(name="w", engine="sqlite", source=ep, target=ep,
              options={"notify": f"http://127.0.0.1:{srv.server_port}/h"})
    hop.report_dir = lambda db=None: tmp_path
    built = {"here": True}
    rungs = (decide.Rung("pgcopydb", "the streaming copy",
                         needs=(decide.Need("its build", lambda f:
                                            built["here"],
                                            "its build is not here"),)),
             decide.Rung("builtin", "the table copier"))
    kept = session.Decisions(hop, print)
    decide.LISTENERS.append(kept.heard)
    try:
        assert decide.climb("appdb", rungs).path == "pgcopydb"
        built["here"] = False
        decide.climb("appdb", rungs, prove=False)
        assert got == [], got
        assert decide.climb("appdb", rungs).path == "builtin"
        # the same way again is no second fallback
        decide.climb("appdb", rungs)
    finally:
        decide.LISTENERS.remove(kept.heard)
        srv.shutdown()
        srv.server_close()
    text = (tmp_path / session.DECISIONS).read_text()
    assert "pgcopydb" not in text, text
    lines = [json.loads(x) for x in text.splitlines()]
    assert [x["way"] for x in lines] == ["the streaming copy",
                                         "the table copier"], lines
    assert lines[1]["passed"][0]["why"] == "its build is not here", lines
    falls = [b for b in got if b.get("event") == "fallback"]
    assert len(falls) == 1, got
    assert falls[0]["from"] == "the streaming copy" and \
        falls[0]["to"] == "the table copier", falls
    assert "its build is not here" in falls[0]["text"], falls


@needs_docker
def test_a_postgresql_digest_is_taken_again_by_its_statement(
        pg_pair, tmp_path, monkeypatch):
    """The statement kept beside each number, run by hand on each server,
    answers the same number - on one engine (the server's own row text,
    salted) and read as two engines (every value rendered one way)."""
    import migkit.config as cfg
    for port in pg_pair.values():
        psql(port, "create table public.acct (id int primary key, name"
                   " text, amount numeric(12,2), at timestamptz)")
        psql(port, "insert into public.acct select g, 'n'||g, g * 1.5,"
                   " '2026-01-01'::timestamptz + g * interval '1 hour'"
                   " from generate_series(1, 500) g")
    psql(pg_pair["dst"], "update public.acct set name = 'changed' where"
                         " id = 7")
    conf = tmp_path / "hops.yaml"
    side = "{{host: 127.0.0.1, port: {}, user: postgres, password: test}}"
    conf.write_text(
        "hops:\n"
        "  one:\n    engine: postgres\n"
        f"    source: {side.format(pg_pair['src'])}\n"
        f"    target: {side.format(pg_pair['dst'])}\n"
        "    databases: [postgres]\n"
        "  two:\n    engine: hetero\n"
        f"    source: {side.format(pg_pair['src'])}\n"
        f"    target: {side.format(pg_pair['dst'])}\n"
        "    databases: [postgres]\n"
        "    options: {source_engine: postgres, target_engine: postgres}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for hop, method in (("one", "own"), ("two", "neutral")):
        got, said = _run("check", hop, "--only", "data")
        assert got.exit_code == 1, said
        run = _runs(tmp_path / "reports" / hop)[-1]
        doc = json.loads((run / "evidence.json").read_text())
        table = next(t for t in doc["tables"]
                     if t["table"].endswith("acct") and not t["where"])
        assert table["method"] == method, table
        assert table["status"] == "different", table
        for s, port in (("src", pg_pair["src"]), ("dst", pg_pair["dst"])):
            statement = table[s]["statement"]
            assert statement, (hop, s, table)
            again = psql(port, statement).stdout.strip()
            assert again.split("|")[:2] == [str(table[s]["rows"]),
                                            str(table[s]["digest"])], \
                (hop, s, again, table[s])
        assert table["src"]["digest"] != table["dst"]["digest"], table
