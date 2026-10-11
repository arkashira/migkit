"""The audit record's checkpoints are kept where the record's writer cannot
rewrite them (backlog R5.anchor).

The object store is `moto`'s stand-in, with Object Lock enabled buckets and
buckets without. Proved: a checkpoint is written and read back; the same
one written twice is one object; a record rewritten or cut shows against
the copies; a copy written over in the bucket shows; a bucket without
Object Lock is refused with its reason; a failed write is said, kept and
written at the next run, and stops nothing unless the hop requires it;
what is sent holds no value from a database and no credential. Not
proved: the real store's behaviour (the stand-in is not it).
"""
import json
import shutil

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from migkit import anchor, audit, export
from migkit.config import Endpoint, Hop

PW = "s3cr3t-pw-Zq81"
BUCKET = "audit-anchors"


@pytest.fixture(autouse=True)
def _store(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MIGKIT_EGRESS", "")
    monkeypatch.delenv("MIGKIT_EGRESS")
    monkeypatch.delenv("MIGKIT_EVIDENCE_KEY", raising=False)
    monkeypatch.setattr(anchor, "_sleep", lambda s: None)
    with mock_aws():
        yield


@pytest.fixture
def s3():
    return boto3.client("s3", region_name="us-east-1")


def _locked(s3, name=BUCKET):
    s3.create_bucket(Bucket=name, ObjectLockEnabledForBucket=True)
    return name


def _hop(tmp_path, **anchor_options):
    ep = Endpoint(host="10.0.0.5", port=5432, user="app", password=PW)
    conf = {"bucket": BUCKET, "region": "us-east-1", **anchor_options}
    hop = Hop(name="orders", engine="postgres", source=ep, target=ep,
              options={"anchor": conf})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _record(hop, n=3, text="moved"):
    path = hop.report_dir() / "changelog.jsonl"
    for i in range(n):
        audit.append(path, {"op": "note", "n": i, "text": text})
    return path


def _keys(s3):
    got = s3.list_object_versions(Bucket=BUCKET)
    return [(v["Key"], v["VersionId"]) for v in got.get("Versions", [])]


# ---- written, and read back --------------------------------------------

def test_a_checkpoint_is_written_locked_and_read_back(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path, retention={"mode": "COMPLIANCE", "days": 30})
    path = _record(hop)
    said = []
    got = anchor.checkpoint(hop, said.append)
    assert got == {"anchored": [3], "already": [], "pending": []}, got
    assert said == []
    (key, ver), = _keys(s3)
    head = audit.head(path)
    assert key == f"migkit-anchors/orders/audit/{3:012d}-{head}.json"
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    assert body == {"format": 1, "kind": "migkit-audit-checkpoint",
                    "seq": 3, "head": head}
    held = s3.head_object(Bucket=BUCKET, Key=key, VersionId=ver)
    assert held["ObjectLockMode"] == "COMPLIANCE"
    days = (held["ObjectLockRetainUntilDate"] - anchor._now()).days
    assert 28 <= days <= 30, days
    # and the record is held to it
    res = anchor.verify(hop)
    assert (res["checked"], res["held"], res["problems"]) == (1, 1, []), res
    assert res["latest"] == 3 and res["since"] == 0
    assert "1 of 1" in anchor.lines(res)[0]


def test_a_locked_object_cannot_be_deleted_in_the_bucket(tmp_path, s3):
    """The stand-in does what makes an anchor worth having: the object is
    held, so removing it - the way to hide a rewrite - is refused."""
    _locked(s3)
    hop = _hop(tmp_path, retention={"mode": "COMPLIANCE", "days": 1})
    _record(hop)
    anchor.checkpoint(hop)
    (key, ver), = _keys(s3)
    with pytest.raises(ClientError) as e:
        s3.delete_object(Bucket=BUCKET, Key=key, VersionId=ver)
    assert e.value.response["Error"]["Code"] == "AccessDenied"


def test_the_same_checkpoint_twice_is_one_object(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    assert anchor.checkpoint(hop)["anchored"] == [3]
    again = anchor.checkpoint(hop)
    assert again == {"anchored": [], "already": [3], "pending": []}, again
    assert len(_keys(s3)) == 1
    # put itself, called directly, settles the same way
    conf = anchor.settings(hop)
    head = audit.head(tmp_path / "changelog.jsonl")
    assert anchor.put(hop, conf, 3, head, s3) == "already"
    assert len(_keys(s3)) == 1


def test_a_key_holding_another_checkpoint_is_refused(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    conf = anchor.settings(hop)
    head = audit.head(path)
    s3.put_object(Bucket=BUCKET, Key=anchor.key_of(conf, hop, 3, head),
                  Body=json.dumps({"seq": 3, "head": "0" * 64}).encode())
    with pytest.raises(anchor.Refused, match="holds another checkpoint"):
        anchor.put(hop, conf, 3, head, s3)


def test_only_a_digest_and_numbers_are_sent(tmp_path, s3):
    """No value of a database, no table or column name, no host and no
    credential leaves in the object or its key."""
    _locked(s3)
    hop = _hop(tmp_path)
    path = hop.report_dir() / "changelog.jsonl"
    audit.append(path, {"op": "repair", "table": "public.customers",
                        "detail": "ssn 078-05-1120 for alice@example.com",
                        "password": PW})
    anchor.checkpoint(hop)
    sent = []
    for key, ver in _keys(s3):
        sent.append(key)
        sent.append(s3.get_object(Bucket=BUCKET, Key=key)["Body"]
                    .read().decode())
    text = " ".join(sent)
    for word in ("customers", "078-05-1120", "alice", PW, "10.0.0.5",
                 "repair", "app"):
        assert word not in text.replace("migkit-anchors", ""), word
    assert set(json.loads(sent[1])) == {"format", "kind", "seq", "head"}


# ---- the record held to the copies --------------------------------------

def _rewrite_from_start(hop, entries):
    """What someone who can write the file does: a new record, a whole
    chain, from its first entry."""
    path = hop.report_dir() / "changelog.jsonl"
    path.unlink()
    for e in entries:
        audit.append(path, e)
    return path


def test_a_record_rewritten_whole_shows_against_the_anchor(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    anchor.checkpoint(hop)
    assert audit.verify(path)[2] == ""
    _rewrite_from_start(hop, [{"op": "note", "n": i, "text": "nothing here"}
                              for i in range(3)])
    # the new chain is whole by itself: the chain alone says nothing
    assert audit.verify(path)[2] == ""
    res = anchor.verify(hop)
    assert res["held"] == 0 and len(res["problems"]) == 1, res
    assert "was rewritten" in res["problems"][0]
    assert "checkpoint 3" in res["problems"][0]


def test_entries_taken_out_show_against_the_anchor(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop, 4)
    anchor.checkpoint(hop)
    lines = path.read_text().splitlines(keepends=True)
    path.write_text("".join(lines[:2]))
    res = anchor.verify(hop)
    assert any("taken out" in p for p in res["problems"]), res
    assert res["held"] == 0


def test_entries_added_since_are_counted_not_failed(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    anchor.checkpoint(hop)
    audit.append(path, {"op": "note", "n": 9})
    res = anchor.verify(hop)
    assert res["problems"] == [] and res["since"] == 1, res
    # the next checkpoint covers them
    assert anchor.checkpoint(hop)["anchored"] == [4]
    assert anchor.verify(hop)["since"] == 0


def test_a_copy_written_over_in_the_bucket_shows(tmp_path, s3):
    """A write over a key hides the anchored version from a plain read;
    verification reads every version."""
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    anchor.checkpoint(hop)
    (key, _), = _keys(s3)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(
        {"format": 1, "kind": "migkit-audit-checkpoint", "seq": 3,
         "head": "f" * 64}).encode())
    res = anchor.verify(hop)
    assert any("2 versions" in p for p in res["problems"]), res
    assert any("another checkpoint than its name" in p
               for p in res["problems"]), res
    assert res["held"] == 0


def test_a_copy_without_a_lock_or_with_its_retention_ended_shows(
        tmp_path, s3, monkeypatch):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    head = audit.head(path)
    s3.put_object(Bucket=BUCKET, Key=anchor.key_of(
        anchor.settings(hop), hop, 3, head), Body=json.dumps(
        {"format": 1, "kind": "migkit-audit-checkpoint", "seq": 3,
         "head": head}).encode())
    res = anchor.verify(hop)
    assert any("not held by Object Lock" in p for p in res["problems"]), res
    # a lock that ran out
    other = _hop(tmp_path, bucket=_locked(s3, "second-bucket"),
                 retention={"days": 1})
    anchor.checkpoint(other)
    import datetime
    later = anchor._now() + datetime.timedelta(days=3)
    monkeypatch.setattr(anchor, "_now", lambda: later)
    res = anchor.verify(other)
    assert any("retention ended" in p for p in res["problems"]), res


def test_a_bucket_that_cannot_be_read_is_not_a_pass(tmp_path, s3):
    hop = _hop(tmp_path, bucket="no-such-bucket")
    _record(hop)
    res = anchor.verify(hop)
    assert res["checked"] == 0 and res["held"] == 0
    assert res["problems"] and "could not be read" in res["problems"][0]


def test_a_broken_record_is_not_anchored(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    lines = path.read_text().splitlines(keepends=True)
    path.write_text(lines[0] + lines[1].replace("moved", "edited")
                    + lines[2])
    said = []
    got = anchor.checkpoint(hop, said.append)
    assert got["anchored"] == [] and got["pending"]
    assert any("does not verify" in m for m in said), said
    assert _keys(s3) == []


# ---- a bucket that cannot hold an anchor --------------------------------

def test_a_bucket_without_object_lock_is_refused_with_its_reason(
        tmp_path, s3):
    s3.create_bucket(Bucket=BUCKET)
    hop = _hop(tmp_path)
    _record(hop)
    said = []
    got = anchor.checkpoint(hop, said.append)
    assert got["anchored"] == []
    assert "has no Object Lock" in got["pending"][0]["why"]
    assert "anchor nothing" in got["pending"][0]["why"]
    assert any("has no Object Lock" in m for m in said), said
    assert _keys(s3) == [], "nothing was written to a bucket that cannot hold it"
    with pytest.raises(anchor.Refused, match="has no Object Lock"):
        anchor.check_bucket(s3, anchor.settings(hop))


def test_a_bucket_that_does_not_exist_is_refused(tmp_path, s3):
    hop = _hop(tmp_path, bucket="not-there")
    _record(hop)
    got = anchor.checkpoint(hop)
    assert "does not exist" in got["pending"][0]["why"], got


def test_a_store_that_does_not_apply_the_retention_is_not_an_anchor(
        tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)

    class Forgetful:
        def __getattr__(self, name):
            return getattr(s3, name)

        def head_object(self, **kw):
            got = dict(s3.head_object(**kw))
            got.pop("ObjectLockMode", None)
            return got

    got = anchor.checkpoint(hop, s3=Forgetful())
    assert got["anchored"] == [] and "not held by Object Lock" in \
        got["pending"][0]["why"], got


# ---- failure: said, kept, tried again, and a stop only on request -------

class Flaky:
    """The store, failing its writes `fail` times first with `code`."""

    def __init__(self, s3, fail, code="SlowDown"):
        self.s3, self.fail, self.code, self.tried = s3, fail, code, 0

    def __getattr__(self, name):
        return getattr(self.s3, name)

    def put_object(self, **kw):
        self.tried += 1
        if self.tried <= self.fail:
            raise ClientError({"Error": {"Code": self.code,
                                         "Message": f"password={PW}"}},
                              "PutObject")
        return self.s3.put_object(**kw)


def test_a_transient_failure_is_retried_inside_the_call(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    flaky = Flaky(s3, 2)
    got = anchor.checkpoint(hop, s3=flaky)
    assert got["anchored"] == [3] and flaky.tried == 3, (got, flaky.tried)
    assert len(_keys(s3)) == 1


def test_a_failed_anchor_is_said_kept_and_written_at_the_next_run(
        tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    said = []
    flaky = Flaky(s3, 99)
    got = anchor.checkpoint(hop, said.append, s3=flaky)
    assert got["anchored"] == [] and flaky.tried == anchor.TRIES
    assert any("could not be anchored" in m and "tried again" in m
               for m in said), said
    kept = json.loads((tmp_path / "anchors.json").read_text())
    assert kept["pending"][0]["seq"] == 3
    assert kept["pending"][0]["tries"] == 1
    assert kept["pending"][0]["head"] == audit.head(path)
    # the record grows; the next run writes the head and the one pending
    audit.append(path, {"op": "note", "n": 7})
    got = anchor.checkpoint(hop, s3=s3)
    assert sorted(got["anchored"]) == [3, 4] and got["pending"] == [], got
    kept = json.loads((tmp_path / "anchors.json").read_text())
    assert kept["pending"] == [] and kept["anchored"]["seq"] == 4
    assert len(_keys(s3)) == 2
    assert anchor.verify(hop)["held"] == 2


def test_a_refusal_is_not_retried_inside_the_call(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    flaky = Flaky(s3, 99, code="AccessDenied")
    got = anchor.checkpoint(hop, s3=flaky)
    assert flaky.tried == 1
    assert "AccessDenied" in got["pending"][0]["why"]


def test_an_unlisted_pending_checkpoint_the_record_lost_is_said(
        tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path)
    path = _record(hop)
    anchor.checkpoint(hop, s3=Flaky(s3, 99))
    _rewrite_from_start(hop, [{"op": "note", "n": i} for i in range(4)])
    said = []
    got = anchor.checkpoint(hop, said.append, s3=s3)
    assert any("not in the record any more" in m for m in said), said
    assert got["anchored"] == [4]
    assert path.exists()


def test_what_is_said_holds_no_credential(tmp_path, s3):
    _locked(s3)
    hop = _hop(tmp_path, endpoint_url=f"https://app:{PW}@store.example.com")
    _record(hop)

    class Down:
        def __getattr__(self, name):
            raise ClientError({"Error": {
                "Code": "InternalError",
                "Message": f"password={PW} at {hop.options['anchor']}"}},
                name)

    said = []
    got = anchor.checkpoint(hop, said.append, s3=Down())
    assert got["anchored"] == []
    blob = json.dumps([said, got, (tmp_path / "anchors.json").read_text()])
    assert PW not in blob
    assert "InternalError" in blob


def test_an_anchor_that_is_not_required_never_stops_the_run(tmp_path, s3):
    s3.create_bucket(Bucket=BUCKET)
    hop = _hop(tmp_path)
    _record(hop)
    anchor.require_ready(hop)
    anchor.checkpoint(hop)


def test_a_required_anchor_that_cannot_be_written_ends_the_command(
        tmp_path, s3):
    s3.create_bucket(Bucket=BUCKET)
    hop = _hop(tmp_path, required=True)
    _record(hop)
    said = []
    with pytest.raises(anchor.Blocked, match="has no Object Lock"):
        anchor.checkpoint(hop, said.append)
    # kept for the next run all the same
    kept = json.loads((tmp_path / "anchors.json").read_text())
    assert kept["pending"][0]["seq"] == 3


def test_a_required_anchor_is_checked_before_a_writing_command_begins(
        tmp_path, s3):
    s3.create_bucket(Bucket=BUCKET)
    hop = _hop(tmp_path, required=True)
    with pytest.raises(anchor.Blocked, match="cannot hold them"):
        anchor.require_ready(hop)
    _locked(s3, "good-bucket")
    anchor.require_ready(_hop(tmp_path, required=True, bucket="good-bucket"))


def test_the_session_refuses_a_writing_command_it_cannot_anchor(
        tmp_path, s3, monkeypatch):
    from migkit import session
    s3.create_bucket(Bucket=BUCKET)
    hop = _hop(tmp_path, required=True)
    monkeypatch.setattr(session, "_hop", lambda name: hop)
    ran = []
    with pytest.raises(anchor.Blocked):
        with session.step("move", {"hop_name": "orders", "go": True}):
            ran.append("started")
    assert ran == []
    # a command that writes nothing is not held up
    with session.step("check", {"hop_name": "orders"}):
        ran.append("read")
    assert ran == ["read"]


def test_the_session_ends_a_run_whose_required_anchor_failed(
        tmp_path, s3, monkeypatch):
    from migkit import session
    _locked(s3)
    hop = _hop(tmp_path, required=True)
    monkeypatch.setattr(session, "_hop", lambda name: hop)
    monkeypatch.setattr(anchor, "require_ready", lambda hop: None)
    monkeypatch.setattr(export, "write_run", lambda hop, run: anchor.checkpoint(
        hop, s3=Flaky(s3, 99, code="AccessDenied")))
    _record(hop)
    said = []
    with pytest.raises(anchor.Blocked, match="AccessDenied"):
        with session.step("move", {"hop_name": "orders", "go": True},
                          said.append):
            pass
    assert any("AccessDenied" in m for m in said)


def test_the_egress_gate_decides_first(tmp_path, s3, monkeypatch):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    monkeypatch.setenv("MIGKIT_EGRESS", "none")

    def never(conf):
        raise AssertionError("the store was reached")

    monkeypatch.setattr(anchor, "_client", never)
    said = []
    got = anchor.checkpoint(hop, said.append)
    assert got["anchored"] == [] and got["pending"]
    assert not anchor.verify(hop)["checked"]
    assert any("egress" in m for m in said), said


# ---- the setting --------------------------------------------------------

def test_the_setting_is_read_one_way(tmp_path):
    conf = anchor.settings(_hop(tmp_path))
    assert conf["prefix"] == "migkit-anchors/" and conf["days"] == 365
    assert conf["mode"] == "GOVERNANCE" and conf["required"] is False
    conf = anchor.settings(_hop(tmp_path, prefix="/a/b", required="true",
                                retention={"mode": "compliance",
                                           "days": 7}))
    assert (conf["prefix"], conf["mode"], conf["days"], conf["required"]) \
        == ("a/b/", "COMPLIANCE", 7, True)
    plain = _hop(tmp_path)
    plain.options = {}
    assert anchor.settings(plain) is None
    assert anchor.checkpoint(plain) is None and anchor.verify(plain) is None


@pytest.mark.parametrize("bad,words", [
    ({"prefix": "x", "bucket": ""}, "needs `bucket`"),
    ({"secret_key": "abc"}, "is not kept in the hop"),
    ({"access_key": "abc"}, "is not kept in the hop"),
    ({"retention": {"mode": "FOREVER"}}, "is not one of"),
    ({"retention": {"days": 0}}, "at least 1"),
    ({"retention": {"days": "many"}}, "at least 1"),
])
def test_a_wrong_setting_is_refused_naming_it(tmp_path, bad, words):
    with pytest.raises(SystemExit, match=words):
        anchor.settings(_hop(tmp_path, **bad))


def test_the_configuration_knows_the_key_and_its_parts():
    from migkit import hopkeys
    ok = {"hops": {"h": {"engine": "postgres", "source": "postgres://u:CHANGE_ME@10.0.0.1/a",
                         "target": "postgres://u:CHANGE_ME@10.0.0.2/a",
                         "options": {"anchor": {
                             "bucket": "b", "retention": {"days": 3},
                             "required": True}}}}}
    assert not [p for p in hopkeys.check(ok) if "anchor" in p.where], \
        hopkeys.check(ok)
    ok["hops"]["h"]["options"]["anchor"]["bukket"] = "x"
    assert any("anchor" in p.where and "bukket" in (p.where + p.text)
               for p in hopkeys.check(ok)), hopkeys.check(ok)


# ---- signatures ---------------------------------------------------------

def _fake_signing(monkeypatch, tmp_path):
    """A signer standing in for the SSH tool: a signature that is a digest
    of the namespace and the file, checked the same way."""
    import hashlib
    from pathlib import Path

    def sign(path, key, namespace=export.NAMESPACE):
        Path(str(path) + ".sig").write_text(
            "SIG:" + hashlib.sha256(
                (namespace + Path(path).read_text()).encode()).hexdigest())

    def check(path, signature, allowed, namespace=export.NAMESPACE):
        want = "SIG:" + hashlib.sha256(
            (namespace + Path(path).read_text()).encode()).hexdigest()
        if Path(signature).read_text() != want:
            raise RuntimeError("the signature does not hold")
        return "ops"

    monkeypatch.setattr(export, "_sign", sign)
    monkeypatch.setattr(export, "_verify", check)
    monkeypatch.setenv("MIGKIT_EVIDENCE_KEY", str(tmp_path / "key"))


def test_a_checkpoint_is_signed_and_the_signature_checked(
        tmp_path, s3, monkeypatch):
    _fake_signing(monkeypatch, tmp_path)
    _locked(s3)
    hop = _hop(tmp_path, allowed_signers=str(tmp_path / "allowed"))
    _record(hop)
    anchor.checkpoint(hop)
    (key, _), = _keys(s3)
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    assert body["signature"].startswith("SIG:")
    assert anchor.verify(hop)["held"] == 1
    # an object whose head was swapped under the same signature
    other = _hop(tmp_path, bucket=_locked(s3, "other-bucket"),
                 allowed_signers=str(tmp_path / "allowed"))
    conf = anchor.settings(other)
    path = tmp_path / "changelog.jsonl"
    head = audit.head(path)
    s3.put_object(Bucket="other-bucket", ObjectLockMode="GOVERNANCE",
                  ObjectLockRetainUntilDate=anchor._now()
                  + __import__("datetime").timedelta(days=2),
                  Key=anchor.key_of(conf, other, 3, head),
                  Body=json.dumps({"format": 1, "seq": 3, "head": head,
                                   "kind": anchor.KIND,
                                   "signature": "SIG:forged"}).encode())
    res = anchor.verify(other)
    assert any("signature does not hold" in p for p in res["problems"]), res


def test_a_hop_that_names_a_key_it_cannot_sign_with_writes_nothing(
        tmp_path, s3, monkeypatch):
    """A checkpoint cannot be improved once locked, so an unsigned one is
    not written in place of a signed one: it waits, said."""
    _locked(s3)
    monkeypatch.setenv("MIGKIT_EVIDENCE_KEY", str(tmp_path / "missing-key"))

    def broken(path, key, namespace=None):
        raise RuntimeError("no such key")

    monkeypatch.setattr(export, "_sign", broken)
    hop = _hop(tmp_path)
    _record(hop)
    said = []
    got = anchor.checkpoint(hop, said.append)
    assert got["anchored"] == [] and "no such key" in said[0] + str(got)
    assert _keys(s3) == []


@pytest.mark.skipif(shutil.which("ssh-keygen") is None,
                    reason="no SSH key tool here")
def test_a_checkpoint_is_signed_with_a_real_key(tmp_path, s3, monkeypatch):
    import subprocess
    key = tmp_path / "id"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                    "ops", "-f", str(key)], check=True)
    allowed = tmp_path / "allowed"
    allowed.write_text(f"ops {(tmp_path / 'id.pub').read_text()}")
    monkeypatch.setenv("MIGKIT_EVIDENCE_KEY", str(key))
    _locked(s3)
    hop = _hop(tmp_path, allowed_signers=str(allowed))
    _record(hop)
    anchor.checkpoint(hop)
    res = anchor.verify(hop)
    assert res["held"] == 1 and res["problems"] == [], res


# ---- where it is made ---------------------------------------------------

def test_the_evidence_export_anchors_the_record(tmp_path, s3, monkeypatch):
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop, 2)
    monkeypatch.setattr(export, "gather", lambda hop, session: {
        "scope": "hop", "tables": []})
    for name in ("page", "markdown", "table_rows"):
        monkeypatch.setattr(export, name, lambda doc: "")
    export.write(hop)
    # the export wrote its own entry to the record, then the checkpoint
    path = tmp_path / "changelog.jsonl"
    n = len(audit.hashes(path))
    assert n == 3
    (key, _), = _keys(s3)
    assert key.endswith(f"{n:012d}-{audit.head(path)}.json")
    assert anchor.verify(hop)["held"] == 1


def test_the_history_says_what_the_anchors_show(tmp_path, s3, monkeypatch):
    from click.testing import CliRunner

    from migkit import cli
    _locked(s3)
    hop = _hop(tmp_path)
    _record(hop)
    anchor.checkpoint(hop)
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    out = CliRunner().invoke(cli.main, ["history", "orders"])
    assert out.exit_code == 0, out.output
    assert "audit anchors: 1 of 1" in out.output.replace("\n", " ")
