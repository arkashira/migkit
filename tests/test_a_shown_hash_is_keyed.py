"""What a shared report shows of a value is a keyed HMAC, and the key is not
in the report (backlog R17b.hmac).

A salted hash with its salt in the report directory can be checked against a
list of guessed emails by anyone the report is shared with. The token is now
an HMAC-SHA256 under a secret kept in the user's state directory and the
hop's salt; the report holds only the salt.
"""
import hashlib
import hmac
import os
import stat

import pytest

from migkit import masking
from migkit.config import Endpoint, Hop


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    masking._SECRETS.clear()
    yield tmp_path / "state" / "migkit"
    masking._SECRETS.clear()


def _hop(report, mask=("email",)):
    ep = Endpoint(host="x", port=0, user="", password="")
    hop = Hop(name="m", engine="sqlite", source=ep, target=ep,
              options={"mask": list(mask)})
    report.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: report
    return hop


def test_the_token_is_an_hmac_under_the_local_secret_and_the_salt(
        state, tmp_path):
    hop = _hop(tmp_path / "r")
    got = masking.token(hop, "ann@example.com")
    secret = (state / "mask.key").read_bytes()
    salt = (tmp_path / "r" / "mask.salt").read_bytes()
    key = hmac.new(secret, b"migkit-mask-v1|" + salt, hashlib.sha256).digest()
    want = hmac.new(key, b"ann@example.com", hashlib.sha256).hexdigest()
    assert got == "masked:" + want[:10]
    assert masking.keyed(hop, "ann@example.com") == want
    assert masking.keyed(hop, b"ann@example.com") == want


def test_the_report_holds_no_key_and_the_salt_alone_tests_no_guess(
        state, tmp_path):
    report = tmp_path / "r"
    hop = _hop(report)
    got = masking.token(hop, "ann@example.com")
    salt = (report / "mask.salt").read_bytes()
    # what a reader of the report could compute from it: the old salted hash
    old = "masked:" + hashlib.sha256(salt + b"ann@example.com"
                                     ).hexdigest()[:10]
    assert got != old
    assert not (report / "mask.key").exists()
    secret = (state / "mask.key").read_bytes()
    for f in report.rglob("*"):
        assert secret not in f.read_bytes(), f
    assert stat.S_IMODE(os.stat(state / "mask.key").st_mode) == 0o600
    assert len(secret) == 32


def test_another_secret_shows_other_tokens_for_the_same_salt_and_value(
        state, tmp_path, monkeypatch):
    report = tmp_path / "r"
    first = masking.token(_hop(report), "ann@example.com")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "other"))
    masking._SECRETS.clear()
    second = masking.token(_hop(report), "ann@example.com")
    assert (tmp_path / "other" / "migkit" / "mask.key").exists()
    assert first != second


def test_equal_values_still_show_equal_across_runs(state, tmp_path):
    hop = _hop(tmp_path / "r")
    a = masking.token(hop, "ann@example.com")
    masking._SECRETS.clear()  # a new process reads the secret back
    assert masking.token(hop, "ann@example.com") == a
    assert masking.token(hop, "bo@example.com") != a
    assert masking.token(hop, None) is None
    assert masking.token(hop, "").startswith("masked:")


def test_a_state_directory_that_cannot_be_written_still_masks(
        tmp_path, monkeypatch):
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    monkeypatch.setenv("XDG_STATE_HOME", str(blocker))
    masking._SECRETS.clear()
    try:
        hop = _hop(tmp_path / "r")
        a = masking.token(hop, "ann@example.com")
        assert a.startswith("masked:") and len(a) == len("masked:") + 10
        # within the run it still lines rows up
        assert masking.token(hop, "ann@example.com") == a
    finally:
        masking._SECRETS.clear()


def test_a_short_or_torn_secret_is_not_used(state, tmp_path):
    state.mkdir(parents=True)
    (state / "mask.key").write_bytes(b"short")
    hop = _hop(tmp_path / "r")
    masking.token(hop, "x")
    assert len(masking._secret()) == 32


# --- the key's life: rotation and export (backlog R17b.hmac) ----------------

def test_a_rotated_key_refuses_old_tokens_and_says_why(state, tmp_path):
    hop = _hop(tmp_path / "r")
    shown = masking.token(hop, "ann@example.com")
    ok, why = masking.verify(hop, shown, "ann@example.com")
    assert ok and why == ""
    old = masking.key_id()
    got_old, got_new = masking.rotate()
    assert got_old == old and got_new == masking.key_id() != old
    # the token the old key made matches nothing now, whatever the value,
    # and the report that holds only it says why
    ok, why = masking.verify(hop, shown, "ann@example.com")
    assert not ok
    assert old in why and "replaced" in why and "no value now" in why
    assert masking.token(hop, "ann@example.com") != shown
    # once a run has made tokens under the new key, the old token is
    # still told by the key id it carries
    ok, why = masking.verify(hop, shown, "ann@example.com", under=old)
    assert not ok and old in why and "no value now" in why
    # a mismatch under the current key is a different value, not a rotation
    fresh = masking.token(hop, "bob@example.com")
    assert masking.verify(hop, fresh, "ann@example.com") == (
        False, "a different value than the one shown")
    assert masking.verify(hop, fresh, "bob@example.com")[0]
    # the history keeps ids, never keys
    kept = (state / "mask.history").read_text()
    assert old in kept
    assert (state / "mask.key").read_bytes().hex() not in kept
    assert stat.S_IMODE((state / "mask.key").stat().st_mode) == 0o600


def test_the_export_describes_the_key_and_never_holds_it(state, tmp_path):
    import base64
    import json
    from click.testing import CliRunner
    from migkit import cli
    canary = bytes(range(1, 33))
    state.mkdir(parents=True)
    (state / "mask.key").write_bytes(canary)
    os.chmod(state / "mask.key", 0o600)
    got = masking.export()
    assert got["bytes"] == 32 and got["bits"] == 256
    assert got["length_of_text"] == 64 and "0-9a-f" in got["alphabet"]
    outputs = [json.dumps(got), repr(got)]
    for args in (["secrets", "export-mask"],
                 ["secrets", "export-mask", "--json"]):
        r = CliRunner().invoke(cli.main, args)
        assert r.exit_code == 0, r.output
        outputs.append(r.output)
    shapes = [canary.hex(), canary.hex().upper(),
              base64.b64encode(canary).decode(),
              base64.urlsafe_b64encode(canary).decode().rstrip("="),
              canary.decode("latin-1")]
    for text in outputs:
        for shape in shapes:
            assert shape not in text
        assert masking.key_id() in text


def test_rotate_mask_command_replaces_the_key_and_says_so(state):
    from click.testing import CliRunner
    from migkit import cli
    before = masking.key_id()
    r = CliRunner().invoke(cli.main, ["secrets", "rotate-mask", "--yes"])
    assert r.exit_code == 0, r.output
    assert before in r.output and masking.key_id() in r.output
    assert "no longer match" in r.output
    assert masking.key_id() != before
    # without --yes and without an answer, nothing is replaced
    now = masking.key_id()
    r = CliRunner().invoke(cli.main, ["secrets", "rotate-mask"], input="n\n")
    assert r.exit_code != 0 and masking.key_id() == now
