"""W8: the offline bundle for Windows - every wheel the lock names resolved
for win_amd64 with no build from source, summed and signed as the others
are, its signature checked the way an install checks it - or, where one
has no Windows wheel, the build refused naming it and nothing of it left
to be taken for whole. No install from it has run on Windows: there is no
such machine here (`tools.WINDOWS_UNPROVED`).
"""
import re
import socket
import subprocess

import pytest

from migkit import runner, tools


def _names(lock):
    """The distributions a lock pins, normalised."""
    out = set()
    for line in lock.read_text().splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+)==", line)
        if m:
            out.add(re.sub(r"[-_.]+", "-", m.group(1)).lower())
    return out


def _wheel(name):
    """(distribution, platform tag) of a wheel's file name."""
    parts = name[:-len(".whl")].split("-")
    return re.sub(r"[-_.]+", "-", parts[0]).lower(), parts[-1]


def _signer(tmp_path):
    key = tmp_path / "signer"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                    "release@example.com", "-f", str(key)], check=True)
    pub = (tmp_path / "signer.pub").read_text().split()
    signers = tmp_path / "allowed_signers"
    signers.write_text(f"release@example.com {pub[0]} {pub[1]}\n")
    return key, signers


def test_a_bundle_for_windows_is_asked_for_its_wheels_only():
    target = tools._platform("windows-x86_64")
    assert target == {"system": "Windows", "machine": "x86_64"}
    assert tools._platform("windows-amd64") == target
    assert tools._wheel_tags(target) == ["win_amd64"]
    with pytest.raises(SystemExit):
        tools._platform("windows-arm64")


def test_a_wheel_missing_for_windows_refuses_the_build_and_names_it(
        tmp_path, monkeypatch):
    asked = []

    def run(argv, env=None, **kw):
        asked.append(argv)
        if "download" in argv:
            return subprocess.CompletedProcess(
                argv, 1, "", "ERROR: Could not find a version that satisfies"
                " the requirement pyarrow==17.0.0\nERROR: No matching"
                " distribution found for pyarrow==17.0.0")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(tools, "_pip", lambda: ["pip"])
    monkeypatch.setattr(runner, "run", run)
    out = tmp_path / "bundle"
    out.mkdir()
    said = []
    with pytest.raises(SystemExit) as got:
        tools._bundle_wheels(out, tools._platform("windows-x86_64"),
                             said.append)
    assert "pyarrow" in str(got.value) and "Windows" in str(got.value)
    # never a half bundle: what was fetched is gone and nothing is summed
    assert not (out / "wheels").exists()
    assert not (out / "second-reader").exists()
    assert not (out / "SHA256SUMS").exists()
    downloads = [a for a in asked if "download" in a]
    assert downloads and all(
        "win_amd64" in a and "--only-binary=:all:" in a for a in downloads)
    assert any("--require-hashes" in a for a in downloads)


def _index_up():
    try:
        socket.create_connection(("pypi.org", 443), timeout=5).close()
        return True
    except OSError:
        return False


@pytest.mark.long
@pytest.mark.skipif(not _index_up(), reason="the package index is not"
                    " reachable from here")
def test_the_windows_bundle_is_built_from_the_lock_and_signed(
        tmp_path, monkeypatch):
    key, signers = _signer(tmp_path)
    out = tmp_path / "bundle"
    out.mkdir()
    said = []
    target = tools._platform("windows-x86_64")
    try:
        tools._bundle_wheels(out, target, said.append)
    except SystemExit as e:
        # refused: it names what lacks a wheel and leaves nothing behind
        assert "win_amd64 of" in str(e), e
        assert not (out / "wheels").exists()
        assert not (out / "second-reader").exists()
        assert not (out / "SHA256SUMS").exists()
        named = str(e).split("win_amd64 of", 1)[1].split(".")[0].split(",")
        assert all(n.strip() for n in named), e
        # the lock pins what has no Windows wheel: the build says so and
        # stops, which is what this machine can prove
        pytest.xfail(f"the lock does not resolve for Windows: {e}")
    for lock, where in ((tools.REQUIREMENTS_LOCK, "wheels"),
                        (tools.SECOND_READER_LOCK, "second-reader")):
        files = [p.name for p in (out / where).glob("*.whl")]
        got = {_wheel(n)[0] for n in files}
        lacking = _names(lock) - got
        assert not lacking, f"{where}: no wheel for {sorted(lacking)}"
        # nothing built for another machine slipped in
        tags = {_wheel(n)[1] for n in files}
        assert tags <= {"any", "win_amd64"}, tags
        assert not list((out / where).glob("*.tar.gz"))
    monkeypatch.setenv("MIGKIT_BUNDLE_KEY", str(key))
    tools._sign_bundle(out, said.append)
    assert (out / "SHA256SUMS.sig").exists(), said
    summed = (out / "SHA256SUMS").read_text()
    assert all(f"wheels/{p.name}" in summed
               for p in (out / "wheels").glob("*.whl"))
    # checked as an install from it checks it
    monkeypatch.setenv("MIGKIT_BUNDLE_SIGNERS", str(signers))
    bundle = tools.Bundle(out, said.append)
    one = sorted(bundle.sums)[0]
    assert bundle.verified(one) is not None
    (out / one).write_bytes((out / one).read_bytes() + b"\0")
    with pytest.raises(SystemExit):
        bundle.verified(one)
    assert tools.WINDOWS_UNPROVED == "built; not run on Windows"
