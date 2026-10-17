"""W8: the offline bundle for Windows - every wheel the lock names resolved
for win_amd64 with no build from source, summed and signed as the others
are, its signature checked the way an install checks it - or, where one
has no Windows wheel, the build refused naming it and nothing of it left
to be taken for whole. No install from it has run on Windows: there is no
such machine here (`tools.WINDOWS_UNPROVED`).
"""
import re
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
    # each lock's pins fetched alone for win_amd64, a wheel first; a pin
    # published only as source is built here into a wheel for every
    # machine or named (`tools._universal`)
    assert len(downloads) == 2 and all(
        "win_amd64" in a and "--no-deps" in a and "--prefer-binary" in a
        and "--require-hashes" in a for a in downloads), downloads


def _fake_pip(skip=()):
    """A pip that reads a lock's markers for the machine it runs on, as pip
    does, and leaves a wheel for Windows of each pin it takes - none for a
    name in `skip`, and still says it did its work."""
    from packaging.markers import Marker
    asked, real = [], runner.run

    def run(argv, env=None, **kw):
        if argv[0] != "pip":
            return real(argv, env, **kw)
        asked.append(argv)
        if "download" in argv and "-r" in argv:
            dest = argv[argv.index("-d") + 1]
            lock = open(argv[argv.index("-r") + 1]).read()
            asked.append(lock)
            for line in lock.splitlines():
                m = re.match(r"^([A-Za-z0-9_.\-]+)==([^ ;\\]+)\s*(?:;([^\\]+))?",
                             line)
                if not m or (m.group(3) and not Marker(
                        m.group(3).strip()).evaluate()):
                    continue
                name = re.sub(r"[-_.]+", "_", m.group(1)).lower()
                if m.group(1).lower() in skip:
                    continue
                if name == "pure_sasl":
                    open(f"{dest}/pure-sasl-{m.group(2)}.tar.gz",
                         "wb").write(b"tarball")
                    continue
                tag = "win_amd64" if name in ("cffi", "psycopg_binary") \
                    else "any"
                abi = "cp312-cp312" if tag != "any" else "py3-none"
                open(f"{dest}/{name}-{m.group(2)}-{abi}-{tag}.whl",
                     "wb").write(b"PK")
        if "wheel" in argv and "-w" in argv:
            dest = argv[argv.index("-w") + 1]
            src = argv[-1]
            if "pure-sasl" in src or "pure_sasl" in src:
                m = re.search(r"pure[-_]sasl-([0-9.]+)", src)
                ver = m.group(1) if m else "0.6.2"
                open(f"{dest}/pure_sasl-{ver}-py3-none-any.whl",
                     "wb").write(b"PK")
        return subprocess.CompletedProcess(argv, 0, "", "")
    return run, asked


def _for_windows(lock):
    """The distributions a lock pins that Windows takes, its markers read
    for Windows."""
    from packaging.markers import Marker
    env = tools._marker_env(tools._platform("windows-x86_64"))
    out = set()
    for line in lock.read_text().splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+)==[^;\\]*(?:;([^\\]+))?", line)
        if m and (not m.group(2) or Marker(m.group(2).strip()).evaluate(env)):
            out.add(re.sub(r"[-_.]+", "-", m.group(1)).lower())
    return out


def test_the_windows_bundle_is_built_from_the_lock_and_signed(
        tmp_path, monkeypatch):
    key, signers = _signer(tmp_path)
    out = tmp_path / "bundle"
    out.mkdir()
    said = []
    run, asked = _fake_pip()
    monkeypatch.setattr(tools, "_pip", lambda: ["pip"])
    monkeypatch.setattr(runner, "run", run)
    target = tools._platform("windows-x86_64")
    tools._bundle_wheels(out, target, said.append)
    # what pip was handed carries no marker it would read for this machine
    handed = [a for a in asked if isinstance(a, str)]
    assert len(handed) == 2 and not any(
        ";" in line for text in handed for line in text.splitlines()
        if "==" in line)
    windows = _for_windows(tools.REQUIREMENTS_LOCK)
    # what only Windows installs is in it, what only Linux does is not
    assert {"pywin32-ctypes", "tzdata"} <= windows
    assert not {"jeepney", "secretstorage"} & windows
    for lock, where in ((tools.REQUIREMENTS_LOCK, "wheels"),
                        (tools.SECOND_READER_LOCK, "second-reader")):
        files = [p.name for p in (out / where).glob("*.whl")]
        got = {_wheel(n)[0] for n in files}
        assert got == _for_windows(lock), (
            where, sorted(_for_windows(lock) - got),
            sorted(got - _for_windows(lock)))
        # nothing built for another machine slipped in
        tags = {_wheel(n)[1] for n in files}
        assert tags <= {"any", "win_amd64"}, tags
        assert not list((out / where).glob("*.tar.gz"))
    assert not (out / "locks").exists()
    # pure-sasl was fetched as source and built into a universal wheel
    wheel_builds = [a for a in asked if isinstance(a, list) and "wheel" in a
                    and any("pure" in x for x in a)]
    assert len(wheel_builds) == 1 and any(
        a.endswith(".tar.gz") for a in wheel_builds[0])
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


def test_a_pip_that_says_done_but_leaves_a_pin_out_is_refused_by_name(
        tmp_path, monkeypatch):
    """pip leaving pins out and returning 0 (what it does reading the
    markers for this machine) is not a whole bundle: each pin without a
    wheel is named and nothing is left to sign."""
    run, _ = _fake_pip(skip=("pywin32-ctypes", "tzdata"))
    monkeypatch.setattr(tools, "_pip", lambda: ["pip"])
    monkeypatch.setattr(runner, "run", run)
    out = tmp_path / "bundle"
    out.mkdir()
    with pytest.raises(SystemExit) as got:
        tools._bundle_wheels(out, tools._platform("windows-x86_64"),
                             lambda m: None)
    assert "pywin32-ctypes" in str(got.value) and "tzdata" in str(got.value)
    assert not (out / "wheels").exists()
    assert not (out / "SHA256SUMS").exists()


def test_a_source_distribution_whose_build_fails_is_refused_by_name(
        tmp_path, monkeypatch):
    """A distribution pinned only as source that fails to build into a
    wheel refuses the Windows bundle by name, leaving nothing signed."""
    run, _ = _fake_pip()

    def run_failing_wheel(argv, env=None, **kw):
        if (argv[0] == "pip" and "wheel" in argv
                and any("pure" in x for x in argv)):
            return subprocess.CompletedProcess(
                argv, 1, "", "ERROR: Failed to build pure-sasl")
        return run(argv, env, **kw)

    monkeypatch.setattr(tools, "_pip", lambda: ["pip"])
    monkeypatch.setattr(runner, "run", run_failing_wheel)
    out = tmp_path / "bundle"
    out.mkdir()
    with pytest.raises(SystemExit) as got:
        tools._bundle_wheels(out, tools._platform("windows-x86_64"),
                             lambda m: None)
    assert "pure-sasl" in str(got.value) and "Windows" in str(got.value)
    assert not (out / "wheels").exists()
    assert not (out / "second-reader").exists()
    assert not (out / "SHA256SUMS").exists()


def test_a_source_distribution_without_universal_wheel_refuses_the_build(
        tmp_path, monkeypatch):
    """A distribution whose source build produces an architecture-specific
    wheel rather than a universal one is refused: Windows cannot build it."""
    run, _ = _fake_pip()

    def run_platform_wheel(argv, env=None, **kw):
        if (argv[0] == "pip" and "wheel" in argv
                and any("pure" in x for x in argv)):
            dest = argv[argv.index("-w") + 1]
            open(f"{dest}/pure_sasl-0.6.2-cp312-cp312-win_amd64.whl",
                 "wb").write(b"PK")
            return subprocess.CompletedProcess(argv, 0, "", "")
        return run(argv, env, **kw)

    monkeypatch.setattr(tools, "_pip", lambda: ["pip"])
    monkeypatch.setattr(runner, "run", run_platform_wheel)
    out = tmp_path / "bundle"
    out.mkdir()
    with pytest.raises(SystemExit) as got:
        tools._bundle_wheels(out, tools._platform("windows-x86_64"),
                             lambda m: None)
    assert "pure-sasl" in str(got.value) and "Windows" in str(got.value)
    assert not (out / "wheels").exists()
    assert not (out / "second-reader").exists()
    assert not (out / "SHA256SUMS").exists()


def test_windows_is_declared_built_and_unproved_in_the_state():
    """The Windows cells are read by the declaration seam into the
    state's "built, unproved" block, with the reason the bundle says."""
    from migkit import capabilities as caps
    assert "tools.py" in caps.DECLARING
    why, cells = caps.UNPROVED_ELSEWHERE["windows"]
    assert why == tools.WINDOWS_UNPROVED == tools.UNPROVED_ON["windows"]
    assert {"bundle.install", "bundle.source-build"} <= set(cells)


def test_a_linux_bundle_takes_wheels_built_for_any_glibc_floor_between():
    """A wheel built for glibc 2.24 (the only aarch64 one some pins ship)
    is asked for, as one for 2.17 and 2.28 are."""
    tags = tools._wheel_tags(tools._platform("linux-arm64"))
    assert {"manylinux_2_17_aarch64", "manylinux_2_24_aarch64",
            "manylinux_2_28_aarch64", "manylinux2014_aarch64"} <= set(tags)
    assert "manylinux_2_29_aarch64" not in tags
