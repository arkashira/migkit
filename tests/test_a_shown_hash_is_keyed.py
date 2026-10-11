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
