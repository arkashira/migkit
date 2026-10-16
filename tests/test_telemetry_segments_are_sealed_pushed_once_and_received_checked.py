"""Product telemetry's sealed segments go to an S3-compatible bucket the
operator runs, once each, and come back checked (backlog
P0-PRODUCT-TELEMETRY, the store the lead chose 2026-10-16:
docs/decisions/0001-telemetry-store.md).

On the sandbox's object store (`tests/servers.py` profile `objectstore`):
the store answers its health path and refuses a second conditional put of
a key - the two answers the push leans on; a run full of canaries is
sealed, pushed twice and resumed after a cut, and the bucket holds one
object per segment with no canary in it, nor in what `show`, `push` or
`receive` say; a segment edited by hand fails its chain at the receiver,
and a forged one with a field outside the schema has that event refused
there.
"""
import json
import types
import urllib.request

import pytest
from click.testing import CliRunner

from migkit import durable, telemetry
from tests import servers
from tests.test_telemetry_carries_no_customer_data import (CANARIES,
                                                           _no_canary)

store = servers.fixture("objectstore")


@pytest.fixture
def where(store, tmp_path, monkeypatch):
    s = store.one
    # the store's own credential chain: nothing in the hop or the client
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s.user)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s.password)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("MIGKIT_TELEMETRY_SPOOL", str(tmp_path / "spool"))
    monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(tmp_path / "none"))
    for k in ("MIGKIT_TELEMETRY", "MIGKIT_EGRESS", "MIGKIT_EGRESS_TO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(telemetry, "_DISPOSED", False)
    telemetry.flush()
    bucket = store.name.replace("_", "-")
    return types.SimpleNamespace(
        where=f"{bucket}/{tmp_path.name}", bucket=bucket,
        prefix=f"{tmp_path.name}/telemetry/", client=s.client(),
        endpoint=f"http://{s.host}:{s.port}", spool=tmp_path / "spool",
        health=f"http://{s.host}:{s.port}{servers.OBJECT_STORE_HEALTH}")


def _objects(w):
    got = w.client.list_objects_v2(Bucket=w.bucket, Prefix=w.prefix)
    return {o["Key"]: w.client.get_object(Bucket=w.bucket, Key=o["Key"])
            ["Body"].read() for o in got.get("Contents", ())}


def _run_full_of_canaries():
    from migkit import cli
    hop = types.SimpleNamespace(
        name="canary_orders", engine="postgres", databases=["canary_orders"],
        target=types.SimpleNamespace(engine="mysql"))
    command = telemetry.Command("move", {c: c for c in CANARIES}, hop)
    for c in CANARIES:
        for bad in ({"command": c}, {c: 1}, {"seconds": c}):
            with pytest.raises(telemetry.Rejected) as got:
                telemetry.record("run", **bad)
            _no_canary(str(got.value))
    for _ in range(4):        # past one segment, sealed as it fills
        for _ in range(telemetry.QUEUED - 1):
            telemetry.record("progress", phase="move", rows="1e3-1e4",
                             done="<90%")
        assert telemetry.flush(30)
    telemetry.intervened("paused", phase="move")
    command.ended(1, ValueError(" ".join(CANARIES)))
    assert telemetry.flush(10)
    assert cli.main


def _say(*argv):
    from migkit import cli
    return CliRunner().invoke(cli.main, ["telemetry", *argv])


def test_the_sandbox_store_answers_its_health_and_a_conditional_put(where):
    with urllib.request.urlopen(where.health, timeout=5) as got:
        assert got.status == 200
    key = where.prefix + "probe"
    where.client.put_object(Bucket=where.bucket, Key=key, Body=b"one",
                            IfNoneMatch="*")
    with pytest.raises(Exception) as got:
        where.client.put_object(Bucket=where.bucket, Key=key, Body=b"two",
                                IfNoneMatch="*")
    assert got.value.response["Error"]["Code"] == "PreconditionFailed"
    assert where.client.get_object(Bucket=where.bucket, Key=key)[
        "Body"].read() == b"one"


def test_canaries_never_reach_a_segment_the_bucket_or_what_is_said(
        where, tmp_path, monkeypatch):
    # kept locally as long as the bucket: every object has its segment
    policy = tmp_path / "policy.yaml"
    policy.write_text("telemetry_keep: 50\n")
    monkeypatch.setenv("MIGKIT_TELEMETRY_POLICY", str(policy))
    _run_full_of_canaries()
    shown = _say("show")
    assert shown.exit_code == 0 and "sealed segment(s)" in shown.output
    first = _say("push", where.where, "--endpoint", where.endpoint)
    assert first.exit_code == 0, first.output
    sink = telemetry.Local(where.spool)
    held = sink.segments()
    assert len(held) >= 2 and all(sink.pushed(p) for _, p in held)
    objects = _objects(where)
    assert sorted(objects) == sorted(
        f"{where.prefix}{sink.manifest(p)['digest']}.seg" for _, p in held)
    assert {objects[k] for k in objects} == {p.read_bytes()
                                             for _, p in held}
    # a push again sends only what came since - the first push's own end,
    # kept as a `run` event - and one whose marks were lost after the put
    # finds every segment already there: one object each
    again = _say("push", where.where, "--endpoint", where.endpoint)
    assert "pushed: 1 segment(s)" in again.output and "0 already" in \
        again.output
    held = sink.segments()
    for _, p in held:
        p.with_suffix(".pushed").unlink()
    lost = _say("push", where.where, "--endpoint", where.endpoint)
    assert f"; {len(held)} already in the bucket" in lost.output
    held = sink.segments()
    objects = _objects(where)
    assert sorted(objects) == sorted(
        f"{where.prefix}{sink.manifest(p)['digest']}.seg" for _, p in held)
    got = _say("receive", where.where, "--endpoint", where.endpoint)
    assert got.exit_code == 0, got.output
    taken = sum(sink.manifest(p)["count"] for _, p in held)
    assert (f"received: {len(held)} segment(s), {taken} event(s)"
            " taken; 0 segment(s) and 0 event(s) refused;"
            " 0 segment(s) whose one before") in got.output
    assert "run | postgres | - | 1" in got.output
    assert "run | - | - | 3" in got.output  # show and two pushes, their ends
    assert "progress | - | - |" in got.output
    for blob in [*objects.values(), *(p.read_bytes() for p in
                                      where.spool.rglob("*") if p.is_file()),
                 shown.output, first.output, again.output, lost.output,
                 got.output]:
        _no_canary(blob)


def test_a_push_cut_after_its_put_resumes_to_one_object(where, monkeypatch):
    for _ in range(3):
        telemetry.record("intervention", phase="move", did="paused")
        assert telemetry.flush(5)
        telemetry.Local().seal()
    real, cuts = durable.write_atomic, []

    def cut(path, data, *a, **k):
        if str(path).endswith(".pushed") and not cuts:
            cuts.append(path)
            raise KeyboardInterrupt      # the operator's Ctrl-C, mid-push
        return real(path, data, *a, **k)
    monkeypatch.setattr(durable, "write_atomic", cut)
    with pytest.raises(KeyboardInterrupt):
        telemetry.push(where.where, where.endpoint)
    assert len(_objects(where)) == 1
    out = telemetry.push(where.where, where.endpoint)
    assert (out["already"], out["sent"], out["left"]) == (1, 2, 0)
    assert len(_objects(where)) == 3
    assert telemetry.receive(where.where, where.endpoint)["events"] == 3


def test_an_edited_segment_fails_its_chain_and_a_forged_field_is_refused(
        where):
    import hashlib
    for phase in ("move", "verify"):
        telemetry.record("progress", phase=phase, done="<50%")
        assert telemetry.flush(5)
        telemetry.Local().seal()
    telemetry.push(where.where, where.endpoint)
    first = sorted(_objects(where).items(),
                   key=lambda kv: json.loads(kv[1].split(b"\n")[0])["seq"])
    key, data = first[0]
    where.client.put_object(Bucket=where.bucket, Key=key,
                            Body=data.replace(b'"move"', b'"plan"'))
    # forged whole: chain and digest made again, an event with a field
    # outside the schema and one with free text
    lines = [json.dumps({**telemetry.make("progress", phase="move"),
                         "canary_orders": 1}),
             json.dumps({**telemetry.make("progress", phase="move"),
                         "phase": "SELECT canary_ssn_col FROM canary_orders"}),
             json.dumps(telemetry.make("progress", phase="move"))]
    body = "".join(x + "\n" for x in lines).encode()
    seed = hashlib.sha256(b"a forger's own start").hexdigest()
    digest = telemetry._digest(seed, body)
    doc = {"segment": 1, "seq": 1, "schema": telemetry.SCHEMA, "count": 3,
           "first": None, "last": None, "prev": seed,
           "chain": telemetry._chained(seed, lines), "digest": digest}
    where.client.put_object(Bucket=where.bucket,
                            Key=f"{where.prefix}{digest}.seg",
                            Body=(json.dumps(doc) + "\n").encode() + body)
    got = _say("receive", where.where, "--endpoint", where.endpoint)
    assert got.exit_code == 4
    assert "segment 1's chain breaks: it was changed after it was sealed" \
        in got.output
    assert "received: 2 segment(s), 2 event(s) taken; 1 segment(s) and" \
        " 2 event(s) refused" in got.output
    assert "progress has 1 field(s) not in its schema" in got.output
    assert "phase takes one of its codes, not free text" in got.output
    _no_canary(got.output)
