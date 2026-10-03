"""Everything migkit sends off the machine goes through one gate (audit
2026-10-01, SEC12, SEC20, W8).

Four things leave, each only where it is configured: notifications, the
run's spans, prompts to a language model, the evidence's digests in the
run state's bucket. Each asks `egress` first - the environment's
`MIGKIT_EGRESS` / `MIGKIT_EGRESS_TO` and the hop's `egress`, the stricter
winning - and is cleaned there of every secret the hop holds, in every
form, and of the programs migkit drives. In a network with no way out,
`MIGKIT_EGRESS=none` means nothing is tried: here every connection and
every name lookup the process makes fails the test.
"""
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

import pytest

from migkit.config import Endpoint, Hop

PW = "Pa/ss@w0rd-CHANGE_ME"


class _Stub:
    """One local server that keeps every request it is sent."""

    def __init__(self, answer=None):
        got = self.got = []
        answer = answer or {"ok": True}

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                got.append((self.path, dict(self.headers),
                            self.rfile.read(n).decode()))
                out = json.dumps(answer).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub():
    s = _Stub()
    yield s
    s.close()


def _hop(tmp_path, **options):
    ep = Endpoint(host="10.0.0.5", port=5432, user="app", password=PW)
    hop = Hop(name="eg", engine="postgres", source=ep, target=ep,
              options=options)
    hop.report_dir = lambda db=None: tmp_path / db if db else tmp_path
    return hop


@pytest.fixture
def no_way_out(monkeypatch):
    """Every connection and every name lookup the process tries, kept and
    refused."""
    tried = []

    def connect(self, address):
        tried.append(("connect", address))
        raise OSError("no way out")

    def connect_ex(self, address):
        tried.append(("connect", address))
        return 111

    def lookup(host, *a, **k):
        tried.append(("lookup", host))
        raise socket.gaierror("no way out")

    def create_connection(address, *a, **k):
        tried.append(("connect", address))
        raise OSError("no way out")
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    monkeypatch.setattr(socket, "create_connection", create_connection)
    return tried


def _run_log(hop):
    from migkit import runlog
    got = []
    for p in (hop.report_dir() / runlog.DIR).glob("*.jsonl"):
        got += [json.loads(x) for x in p.read_text().splitlines()]
    return got


def test_in_a_closed_network_nothing_is_tried(tmp_path, monkeypatch,
                                              no_way_out):
    """W8: every sender configured - receivers of every kind, a
    collector, a model, a bucket - and `MIGKIT_EGRESS=none`. Nothing is
    looked up and nothing connected to, no receiver's address is even
    read, and each refusal is said once in the run's log."""
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import \
        InMemorySpanExporter

    from migkit import assist, export, notify, runlog, session, state, \
        traces
    monkeypatch.setenv("MIGKIT_EGRESS", "none")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                       "https://otel.example.com:4318")
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_URL", "https://llm.example.com/v1")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
    hop = _hop(tmp_path, notify={"to": [
        "https://hooks.example.com/services/T/B/C", "slack://T1/B1/C1",
        "vault:secret/hooks#team", "pagerduty:env:MIGKIT_TEST_PD"]},
        state={"backend": "s3", "bucket": "b",
               "endpoint_url": "https://s3.example.com"})

    def read(receiver):
        raise AssertionError(f"read {receiver!r}")
    monkeypatch.setattr(notify, "_address", read)
    monkeypatch.setattr(state, "run_state",
                        lambda h: pytest.fail("the bucket was reached for"))
    run = session.begin()
    log = runlog.begin(hop, run, "check")
    try:
        said = []
        assert notify.verdict(hop, {"status": "different", "findings": []},
                              said.append) == 0
        assert notify.failed(hop, "move", "RuntimeError: x", said.append) \
            == 0
        assert traces.configure() is not None and not traces.enabled()
        assert assist.ask("what is this?", hop=hop) is None
        export._anchor_remote(hop, tmp_path, {"op": "evidence"},
                              said.append)
        # a test's own exporter stands in for the collector: the hop's
        # egress still keeps its spans here
        spans = InMemorySpanExporter()
        traces.configure(spans)
        try:
            with traces.command("check", hop, {}, run):
                with traces.span("check data", db="app"):
                    pass
        finally:
            traces.configure()
        assert spans.get_finished_spans() == ()
    finally:
        runlog.end(log, "done")
    assert no_way_out == [], no_way_out
    refused = {e.get("kind") for e in _run_log(hop)
               if e["event"] == "egress-refused"}
    assert refused == {"notify", "traces", "assist", "evidence"}, refused
    assert said and all("egress" in s or "EGRESS" in s for s in said), said


def test_only_the_hosts_named_are_sent_to(tmp_path, monkeypatch, stub):
    """`to`: a send whose host is not named is refused; one the egress
    names goes; a channel reached by a token alone - its host its
    vendor's - cannot be told to be one of them, and is refused."""
    from migkit import egress, notify
    hop = _hop(tmp_path, notify={"to": [stub.url + "/hook"]},
               egress={"allow": ["notify"], "to": ["mm.example.com"]})
    said = []
    assert notify.verdict(hop, {"status": "different", "findings": []},
                          said.append) == 0
    assert stub.got == [] and any("receiver 1" in s for s in said), said
    for allowed in (["127.0.0.1"], ["127.0.0.0/8"], ["127.0.*"]):
        hop.options["egress"]["to"] = allowed
        (tmp_path / "notified.json").unlink(missing_ok=True)
        before = len(stub.got)
        assert notify.verdict(hop, {"status": "different", "findings": []},
                              print) == 1, allowed
        assert len(stub.got) == before + 1
    # a token's channel has no host of its own to name
    channel = notify._channel("slack://T1/B1/C1")
    assert notify._channel_host(channel) is None
    assert not egress.decide(hop, "notify", host=None)[0]
    mm = notify._channel("mmost://mm.example.com/token")
    assert notify._channel_host(mm) == "mm.example.com"
    hop.options["egress"]["to"] = ["mm.example.com"]
    assert egress.decide(hop, "notify",
                         host=notify._channel_host(mm))[0]


def test_the_stricter_of_the_environment_and_the_hop_wins(tmp_path,
                                                          monkeypatch):
    from migkit import egress
    hop = _hop(tmp_path, egress={"allow": ["notify", "assist"],
                                 "to": ["*.example.com"]})
    monkeypatch.setenv("MIGKIT_EGRESS", "notify,traces")
    monkeypatch.setenv("MIGKIT_EGRESS_TO", "hooks.example.com,10.0.0.0/8")
    got = {k: egress.decide(hop, k, "https://hooks.example.com/x")[0]
           for k in egress.KINDS}
    assert got == {"notify": True, "traces": False, "assist": False,
                   "evidence": False}, got
    # a host must be in every list that names hosts
    assert not egress.decide(hop, "notify", "https://10.0.0.7/x")[0]
    assert not egress.decide(hop, "notify", "https://mm.example.com/x")[0]
    # left unset, everything configured goes as it did
    monkeypatch.delenv("MIGKIT_EGRESS")
    monkeypatch.delenv("MIGKIT_EGRESS_TO")
    assert all(egress.decide(_hop(tmp_path), k, "https://a.test/")[0]
               for k in egress.KINDS)
    with pytest.raises(SystemExit) as e:
        egress.decide(_hop(tmp_path, egress=["gossip"]), "notify")
    assert "gossip" in str(e.value)


def test_what_is_sent_holds_no_secret_and_no_program(tmp_path, stub):
    """Before, `notify._clean` left a URL-encoded password in the text
    sent to a channel, and the facts beside a message were never cleaned
    at all."""
    from migkit import notify
    hop = _hop(tmp_path, notify={"to": [stub.url + "/hook"],
                                 "detail": True})
    why = (f"RuntimeError: pgcopydb could not reach"
           f" postgres://app:{quote(PW, safe='')}@10.0.0.5/app with"
           f" password={PW}")
    assert notify.failed(hop, "move", why, print) == 1
    assert notify.send(hop, f"a note {PW}", {"note": PW, "api_token": "t",
                                            "event": "done"},
                       False, "d", print) == 1
    sent = " ".join(body for _, _, body in stub.got)
    for leaked in (PW, quote(PW, safe=""), "pgcopydb"):
        assert leaked not in sent, (leaked, sent)
    assert "could not reach" in sent and "10.0.0.5" in sent, sent


def test_a_failure_is_told_by_its_kind(tmp_path, stub):
    """SEC20: an error's words can quote the row it was writing -
    PostgreSQL's `DETAIL: Key (id)=(...)` - and went out whole unless the
    hop masked values. Now its kind goes, and its words stay in the run's
    log, unless `notify: {detail: true}` - never where the hop masks."""
    from migkit import notify
    why = ("IntegrityError: duplicate key value violates unique constraint"
           " DETAIL: Key (email)=(ann@example.com) already exists")
    for options, words in (({}, False), ({"detail": True}, True),
                           ({"detail": True, "mask": "all"}, False)):
        mask = options.pop("mask", None)
        hop = _hop(tmp_path, notify={"to": [stub.url + "/hook"], **options},
                   **({"mask": mask} if mask else {}))
        before = len(stub.got)
        assert notify.failed(hop, "move", why, print) == 1
        text = json.loads(stub.got[before][2])["text"]
        assert "IntegrityError" in text, text
        assert ("ann@example.com" in text) is words, (options, text)
    # a refusal of migkit's own is not a class: it is said to be one
    assert notify.kind_of("run migkit check first") is None
    assert notify.kind_of("psycopg2.errors.UniqueViolation: x") is None
    assert notify.kind_of("psycopg2.errors.UniqueViolationError: x") == \
        "UniqueViolationError"


def test_the_model_is_sent_nothing_over_plain_http(tmp_path, monkeypatch,
                                                   no_way_out):
    """The key and the findings went to any address a hop or the
    environment named, `http` included."""
    from migkit import assist
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
    monkeypatch.setenv("MIGKIT_AI_KEY", "CHANGE_ME-key")
    for url in ("http://llm.example.com/v1", "http://10.0.0.8:8080/v1",
                "ftp://llm.example.com/v1"):
        monkeypatch.setenv("MIGKIT_AI_URL", url)
        got = assist.ask("what is this?", hop=_hop(tmp_path))
        assert got.startswith("(the openai provider was not asked"), got
    assert no_way_out == []
    for url in ("https://llm.example.com/v1", "http://127.0.0.1:1/v1",
                "http://localhost:11434/v1"):
        assert assist.refused(url) is None, url


def test_the_prompt_is_cleaned_at_the_gate(tmp_path, monkeypatch):
    from migkit import assist
    stub = _Stub({"choices": [{"message": {"content": "an answer"}}]})
    try:
        monkeypatch.setenv("MIGKIT_AI", "openai")
        monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
        monkeypatch.setenv("MIGKIT_AI_URL", stub.url + "/v1")
        monkeypatch.setenv("MIGKIT_AI_SHARE", "detail")
        hop = _hop(tmp_path)
        env = {"status": "different", "findings": [{
            "check": "data", "scope": "app.t", "status": "diff",
            "category": "rows-missing",
            "detail": f"mydumper said: password={PW} for app"}]}
        assert assist.explain(hop, env) == "an answer"
    finally:
        stub.close()
    (_, _, body), = stub.got
    assert PW not in body and "mydumper" not in body, body
    assert "rows-missing" in body, body


def test_a_hops_spans_stay_when_its_egress_says_so(tmp_path, monkeypatch):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import \
        InMemorySpanExporter

    from migkit import traces
    spans = InMemorySpanExporter()
    traces.configure(spans)
    try:
        with traces.command("check", _hop(tmp_path, egress=["notify"]),
                            {}, "r1"):
            with traces.span("check data", db="app"):
                pass
        assert spans.get_finished_spans() == ()
        with traces.command("check", _hop(tmp_path), {}, "r2"):
            with traces.span("check data", db="app",
                             note=f"password={PW}"):
                pass
    finally:
        traces.configure()
    got = spans.get_finished_spans()
    assert {s.name for s in got} == {"migkit check", "check data"}, got
    said = json.dumps([dict(s.attributes or {}) for s in got])
    assert PW not in said and "app" in said, said


def test_the_evidence_reaches_only_a_bucket_the_egress_names(tmp_path,
                                                             monkeypatch):
    from migkit import export, state
    put = []

    class Bucket:
        def put(self, path, text):
            put.append(path)
    monkeypatch.setattr(state, "run_state", lambda h: Bucket())
    hop = _hop(tmp_path, state={"backend": "s3", "bucket": "b",
                                "endpoint_url": "http://10.0.0.5:9000"},
               egress={"to": ["minio.example.com"]})
    said = []
    export._anchor_remote(hop, tmp_path, {"op": "evidence"}, said.append)
    assert put == [] and said, said
    hop.options["egress"]["to"] = ["10.0.0.0/24"]
    export._anchor_remote(hop, tmp_path, {"op": "evidence"}, said.append)
    assert len(put) == 1


def test_the_evidence_carries_no_secret_and_keeps_its_digests(tmp_path):
    """SEC12: a record entry's `pwd`, `api_token`, `private_key_passphrase`
    were carried out whole. Now no key naming a secret is carried, every
    text is cleaned of the hop's secrets - and a digest that happens to
    hold a short password's digits is left as it was taken."""
    from migkit import audit, export, session
    hop = _hop(tmp_path)
    hop.source.password = hop.target.password = "4242"
    run = session.begin()
    audit.append(tmp_path / "changelog.jsonl", {
        "op": "repair", "db": "app", "pwd": "p1", "api_token": "t1",
        "private_key_passphrase": "k1",
        "detail": "retried with password 4242 on 10.0.0.5"})
    (tmp_path / "digests.jsonl").write_text("".join(json.dumps({
        "session": run, "db": "app", "table": "t", "side": side,
        "rows": 3, "digest": "424242", "method": "own"}) + "\n"
        for side in ("src", "dst")))
    out = export.write(hop, run)
    text = (out / "evidence.json").read_text()
    for leaked in ('"pwd"', '"api_token"', "private_key_passphrase",
                   "password 4242"):
        assert leaked not in text, leaked
    doc = json.loads(text)
    (row,) = doc["tables"]
    assert row["src"]["digest"] == row["dst"]["digest"] == "424242", row
    assert row["status"] == "same", row


def test_code_is_not_sent_where_the_egress_keeps_it(tmp_path, monkeypatch,
                                                    no_way_out):
    """A routine no converter proved goes to a model round by round - not
    where the hop's egress keeps code on this machine, said as the
    reason no version came back."""
    from migkit.storedcode import model
    from migkit.storedcode.translate import Refused
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
    monkeypatch.setenv("MIGKIT_AI_SHARE", "code")
    monkeypatch.setenv("MIGKIT_AI_URL", "https://llm.example.com/v1")
    with pytest.raises(Refused) as e:
        model.propose(_hop(tmp_path, egress="none"), "mysql", "postgres",
                      None, "create function f() ...", [])
    assert "egress" in str(e.value), e.value
    assert no_way_out == []
