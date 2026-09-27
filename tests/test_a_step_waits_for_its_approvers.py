"""A step the hop says needs approval waits for approvers' signatures, and
the run's record shows a change made to it.

Before this: anyone who could run migkit could cut over, repair or roll
back, and the record of what was done was a text file anyone could edit
without a trace.
"""
import json
import subprocess

import pytest

from migkit.config import Endpoint, Hop


def _key(tmp, name):
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                    name, "-f", str(tmp / name)], check=True)
    return tmp / name


def _sign(key, request):
    got = subprocess.run(["ssh-keygen", "-Y", "sign", "-n", "migkit-approve",
                          "-f", str(key)], input=request.read_bytes(),
                         capture_output=True)
    assert got.returncode == 0, got.stderr
    return got.stdout


@pytest.fixture
def hop(tmp_path):
    keys = tmp_path / "keys"
    keys.mkdir()
    ann, bob, me = (_key(keys, n) for n in ("ann", "bob", "me"))
    signers = keys / "allowed_signers"
    signers.write_text("".join(
        f"{n} namespaces=\"migkit-approve\" "
        f"{(keys / (n + '.pub')).read_text().split(' ')[0]} "
        f"{(keys / (n + '.pub')).read_text().split(' ')[1]}\n"
        for n in ("ann", "bob", "me")))
    h = Hop(name="gate", engine="postgres",
            source=Endpoint(host="10.0.0.1", port=5432),
            target=Endpoint(host="10.0.0.2", port=5432),
            databases=["app"],
            options={"approvals": {"steps": ["cutover", "repair"],
                                   "count": 2, "signers": str(signers)}})
    h.report_dir = lambda db=None: tmp_path / "reports"
    return h, {"ann": ann, "bob": bob, "me": me}


def _place(hop, name, sig):
    where = hop.report_dir("app") / "approvals"
    (where / f"cutover-app.{name}.sig").write_bytes(sig)


def test_a_step_waits_for_every_approver_it_needs(hop, monkeypatch):
    from migkit import approvals
    h, keys = hop
    monkeypatch.setattr(approvals, "_requester", lambda: "me")
    with pytest.raises(SystemExit, match="needs 2 approvals and has 0"):
        approvals.require(h, "cutover", "app")
    request = h.report_dir("app") / "approvals" / "cutover-app.request.json"
    assert json.loads(request.read_text())["asked_by"] == "me"
    # the one who asked does not approve it
    _place(h, "me", _sign(keys["me"], request))
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "cutover", "app")
    _place(h, "ann", _sign(keys["ann"], request))
    with pytest.raises(SystemExit, match=r"has 1 \(ann\)"):
        approvals.require(h, "cutover", "app")
    _place(h, "bob", _sign(keys["bob"], request))
    assert approvals.require(h, "cutover", "app") == ["ann", "bob"]
    # a step the hop does not gate goes ahead at once
    assert approvals.require(h, "rollback", "app") == []


def test_a_signature_counts_only_for_the_request_it_signed(hop, monkeypatch):
    from migkit import approvals
    h, keys = hop
    monkeypatch.setattr(approvals, "_requester", lambda: "me")
    with pytest.raises(SystemExit):
        approvals.require(h, "cutover", "app")
    request = h.report_dir("app") / "approvals" / "cutover-app.request.json"
    sigs = {n: _sign(keys[n], request) for n in ("ann", "bob")}
    # the request changed after it was signed: a later expiry written in
    req = json.loads(request.read_text())
    req["expires"] += 86400
    request.write_text(json.dumps(req, sort_keys=True))
    for n, sig in sigs.items():
        _place(h, n, sig)
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "cutover", "app")
    # and a request that expired is made again, its old signatures gone
    req["expires"] = 1
    request.write_text(json.dumps(req, sort_keys=True))
    with pytest.raises(SystemExit, match="has 0"):
        approvals.require(h, "cutover", "app")
    assert json.loads(request.read_text())["expires"] > 1
    assert not list(request.parent.glob("*.sig"))


def test_the_record_shows_an_entry_changed_or_taken_out(tmp_path):
    from migkit import audit
    path = tmp_path / "changelog.jsonl"
    path.write_text(json.dumps({"at": "2026-01-01", "op": "old"}) + "\n")
    for i in range(4):
        audit.append(path, {"op": "move", "table": f"t{i}"})
    assert audit.verify(path) == (5, 4, "")
    first = json.loads(path.read_text().splitlines()[1])
    assert first["who"] and first["host"] and first["prev"] == ""
    lines = path.read_text().splitlines()
    changed = json.loads(lines[2])
    changed["table"] = "t9"
    path.write_text("\n".join(lines[:2] + [json.dumps(changed)] + lines[3:])
                    + "\n")
    assert audit.verify(path)[2] == "entry 3 was changed"
    path.write_text("\n".join(lines[:2] + lines[3:]) + "\n")
    assert audit.verify(path)[2].startswith("entry 3 does not follow")
    path.write_text("\n".join(lines) + "\n")
    assert audit.verify(path)[2] == ""
