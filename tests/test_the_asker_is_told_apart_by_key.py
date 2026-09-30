"""The one who asks for a step does not approve it - told apart by key
(audit K9).

Before: the asker was the login name, and signers are named by e-mail in
an allowed_signers list, so the two never matched and the asker's own
signature counted; `count: 0` approved with no signature at all; a step
written `Cutover` held nothing; and a request edited after it was asked
kept the signatures made over it.
"""
import json
import subprocess

import pytest

from migkit import approvals, verdict
from migkit.config import Endpoint, Hop


def _key(where, name):
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                    name, "-f", str(where / name)], check=True)
    return where / name


def _sign(key, request, namespace="migkit-approve"):
    got = subprocess.run(["ssh-keygen", "-Y", "sign", "-n", namespace,
                          "-f", str(key)], input=request.read_bytes(),
                         capture_output=True)
    assert got.returncode == 0, got.stderr
    return got.stdout


@pytest.fixture
def gate(tmp_path, monkeypatch):
    keys = tmp_path / "keys"
    keys.mkdir()
    made = {n: _key(keys, n) for n in ("ann", "bob", "op")}
    signers = keys / "allowed_signers"
    # principals as a team writes them: e-mail addresses, not logins
    signers.write_text("".join(
        f"{n}@example.com namespaces=\"migkit-approve\" "
        + " ".join((keys / f"{n}.pub").read_text().split()[:2]) + "\n"
        for n in made))
    # the operator runs migkit under a login no signer is named by
    monkeypatch.setattr(approvals, "_requester", lambda: "operator")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(made["op"]))

    def hop(**rule):
        h = Hop(name="gate", engine="postgres",
                source=Endpoint(host="10.0.0.1", port=5432),
                target=Endpoint(host="10.0.0.2", port=5432),
                options={"approvals": {"steps": ["repair"],
                                       "signers": str(signers), **rule}})
        h.report_dir = lambda db=None: tmp_path / "reports"
        return h
    return hop, made


def _place(hop, name, sig):
    where = hop.report_dir("app") / "approvals"
    (where / f"repair-app.{name}.sig").write_bytes(sig)


def _request(hop):
    return hop.report_dir("app") / "approvals" / "repair-app.request.json"


def test_the_asker_s_own_signature_does_not_count(gate):
    hop, keys = gate
    h = hop(count=1)
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "repair", "app")
    req = json.loads(_request(h).read_text())
    assert req["asked_by_key"].startswith("SHA256:"), req
    _place(h, "op", _sign(keys["op"], _request(h)))
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "repair", "app")
    _place(h, "ann", _sign(keys["ann"], _request(h)))
    assert approvals.require(h, "repair", "app") == ["ann@example.com"]


def test_a_count_of_nought_still_needs_one(gate):
    hop, keys = gate
    h = hop(count=0)
    with pytest.raises(SystemExit, match="needs 1 approval and has 0"):
        approvals.require(h, "repair", "app")
    _place(h, "bob", _sign(keys["bob"], _request(h)))
    assert approvals.require(h, "repair", "app") == ["bob@example.com"]


def test_a_step_migkit_does_not_hold_is_said(gate):
    hop, _ = gate
    h = hop()
    h.options["approvals"]["steps"] = ["Repair"]
    with pytest.raises(verdict.Usage, match="Repair"):
        approvals.require(h, "repair", "app")
    with pytest.raises(verdict.Usage):
        approvals.needed(h, "repair")


def test_a_request_changed_after_it_was_asked_is_asked_again(gate):
    hop, keys = gate
    h = hop(count=1)
    with pytest.raises(SystemExit):
        approvals.require(h, "repair", "app")
    path = _request(h)
    req = json.loads(path.read_text())
    _place(h, "ann", _sign(keys["ann"], path))
    # someone rewrites who asked, and signs what they wrote
    req["asked_by_key"] = "SHA256:someone-else"
    path.write_text(json.dumps(req, sort_keys=True))
    _place(h, "bob", _sign(keys["bob"], path))
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "repair", "app")
    again = json.loads(path.read_text())
    assert again["nonce"] != req["nonce"], again
