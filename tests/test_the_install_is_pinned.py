"""What `doctor --install` fetches is pinned by version and digest, held to
it before it is used, put in place in one step, and can come from an
offline bundle with no network at all (audit 2026-10-01, W5-W8, W13, W14,
W22, W23, SEC14).

Measured before: no checksum anywhere in the installer, images pulled as
`latest`, a program already on the path never replaced whatever its build,
a replacement made by removing the working install first, and the facts
about each program - its packages, licence, image, version, role - kept in
fifteen tables. Here the registry (`tools.PROGRAM`) is the one source, and
the lock (`tools.lock`) what every fetch is held to.
"""
import hashlib
import io
import json
import os
import re
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from migkit import tools

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("MIGKIT_TOOLS", raising=False)
    monkeypatch.delenv("MIGKIT_BUNDLE", raising=False)
    tools._BUNDLES.clear()
    return tmp_path


@pytest.fixture
def served(home, monkeypatch):
    """A server that hands out whatever the test puts in `files`, and a
    lock the test fills: (files, lock entries, base url)."""
    files, pins = {}, {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            body = files.get(self.path.lstrip("/"))
            self.send_response(200 if body is not None else 404)
            self.end_headers()
            self.wfile.write(body or b"")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}/"
    monkeypatch.setattr(tools, "pinned", lambda url: dict(pins.get(url) or {}))
    yield files, pins, base
    srv.shutdown()
    srv.server_close()


def _sha(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


# ---- held to the lock before anything reads it -----------------------------

def test_a_file_is_held_to_its_digest(served, tmp_path):
    files, pins, base = served
    files["a.tgz"] = b"the build"
    pins[base + "a.tgz"] = {"digest": _sha(b"the build"), "size": 9}
    said = []
    dest = tmp_path / "got"
    assert tools.fetch(base + "a.tgz", dest, said.append)
    assert dest.read_bytes() == b"the build"
    # the file changed where it is published: refused, nothing left
    files["a.tgz"] = b"another build"
    pins[base + "a.tgz"] = {"digest": _sha(b"the build")}
    dest.unlink()
    assert not tools.fetch(base + "a.tgz", dest, said.append)
    assert not dest.exists() and not Path(f"{dest}.part").exists()
    assert "digest differs" in said[-1], said


def test_a_file_the_lock_does_not_know_is_not_fetched(served, tmp_path):
    files, pins, base = served
    files["b.tgz"] = b"x"
    said = []
    assert not tools.fetch(base + "b.tgz", tmp_path / "b", said.append)
    assert "not pinned" in said[-1], said


def test_one_no_vendor_publishes_a_digest_of_is_held_to_its_first(
        served, tmp_path):
    files, pins, base = served
    files["c.zip"] = b"first"
    pins[base + "c.zip"] = {"digest": None}
    said = []
    assert tools.fetch(base + "c.zip", tmp_path / "c1", said.append)
    assert "kept" in said[-1], said
    kept = json.loads((tools.tools_home() / tools.PINS).read_text())
    assert kept[base + "c.zip"] == _sha(b"first")
    files["c.zip"] = b"changed"
    assert not tools.fetch(base + "c.zip", tmp_path / "c2", said.append)
    assert not (tmp_path / "c2").exists()


def test_the_lock_holds_every_file_the_recipes_fetch():
    """Every file `tools/pin_programs.py` enumerates for the machines it
    pins is in the lock, and a digest there is one a vendor published."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "pin_programs", ROOT / "tools" / "pin_programs.py")
    pin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pin)
    lock = tools.lock()
    have = set(lock["files"])
    wanted = pin.wanted()
    # a release that has no build for a machine is left out of the lock
    missing = [u for u in wanted if u not in have
               and not pin.RELEASE.match(u)]
    assert not missing, missing
    for url, rec in lock["files"].items():
        if rec["digest"]:
            assert re.fullmatch(r"(sha256:[0-9a-f]{64}|sha512:[0-9a-f]{128}"
                                r"|sha1:[0-9a-f]{40})", rec["digest"]), url
    # most are pinned by what their vendors publish
    pinned = [u for u, r in lock["files"].items() if r["digest"]]
    assert len(pinned) >= 25, len(pinned)


def test_every_image_runs_by_its_digest():
    lock = tools.lock()["images"]
    for name, p in tools.PROGRAM.items():
        if p.image:
            assert re.fullmatch(re.escape(p.image) + r"@sha256:[0-9a-f]{64}",
                                lock[name]), (name, lock.get(name))
    assert tools.IMAGES["pgcopydb"].startswith("dimitri/pgcopydb:v0.18@")
    assert not [r for r in tools.IMAGES.values() if ":latest" in r]


# ---- the registry, the one source ---------------------------------------------

def test_the_side_tables_are_read_off_the_registry():
    for name, p in tools.PROGRAM.items():
        assert tools.PROGRAM_LICENCES[name] == p.licence
        if p.vendor:
            assert tools.VENDOR[name] == p.vendor
    from migkit import movers, toolversion
    assert movers.MEASURED == {n: p.measured for n, p in
                               tools.PROGRAM.items() if p.measured}
    assert set(movers.DRIVEN) == tools.names()
    assert toolversion.PATTERNS["pg_dump"][0] == "postgres"
    # an engine's name is the database's, and stays in what is said
    assert "mysql" not in tools.names() and "docker" not in tools.names()


def test_the_formula_depends_on_what_the_registry_names():
    text = (ROOT / "packaging" / "homebrew" / "migkit.rb").read_text()
    said = {d.rsplit("/", 1)[-1] for d in
            re.findall(r'depends_on "([^"]+)"', text)}
    brew = {f for p in tools.PROGRAM.values() for f in p.brew.split()}
    assert brew <= said, brew - said
    assert said - brew <= {"python@3.12"}, said - brew


def test_the_image_is_built_from_the_lock():
    text = (ROOT / "deploy" / "Dockerfile").read_text()
    base = re.search(r"^FROM (\S+)$", text, re.M).group(1)
    assert base == tools.lock()["images"]["base"], base
    assert "migkit/data/requirements.txt" in text and \
        "--require-hashes" in text
    # the programs by migkit's own installer, held to the lock
    assert "migkit doctor --install" in text
    assert "|| true" not in text and "latest" not in text


def test_what_never_enters_the_image():
    ignored = (ROOT / ".dockerignore").read_text().split()
    for must in ("conf/", "deploy/conf/", "reports/", ".git", ".venv*"):
        assert must in ignored, must


def test_the_second_reading_is_held_to_its_digests():
    text = tools.SECOND_READER_LOCK.read_text()
    assert "google-pso-data-validator==8.9.3" in text
    # the stack it was measured with
    for pinned in ("numpy==1.26.4", "pandas==2.3.3", "sqlglot==19.9.0"):
        assert pinned in text, pinned
    for block in re.split(r"\n(?=[a-z0-9])", text.strip()):
        if block.startswith("#") or not block.strip():
            continue
        assert "--hash=sha256:" in block, block[:80]


# ---- put in place in one step, replaced where it is not the pinned build ------

def test_a_failed_copy_leaves_the_working_install(home, tmp_path,
                                                  monkeypatch):
    target = tools.tools_home() / "prog-1.0"
    target.mkdir(parents=True)
    (target / "prog").write_text("the working one")
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "prog").write_text("the new one")

    real = tools.shutil.copytree

    def broken(src, dst, symlinks=False):
        Path(dst).mkdir()
        raise OSError("the disk filled")
    monkeypatch.setattr(tools.shutil, "copytree", broken)
    with pytest.raises(OSError):
        tools._swap(tree, target)
    assert (target / "prog").read_text() == "the working one"
    monkeypatch.setattr(tools.shutil, "copytree", real)
    tools._swap(tree, target)
    assert (target / "prog").read_text() == "the new one"
    assert not target.with_name("prog-1.0.old").exists()


def test_a_vendor_build_that_says_another_version_is_replaced(
        home, tmp_path, monkeypatch):
    stub = tmp_path / "atlas"
    stub.write_text('#!/bin/sh\necho "atlas version v1.2.4-9b670a5"\n')
    stub.chmod(0o755)
    monkeypatch.setattr(tools, "which", lambda n: str(stub))
    assert not tools._pinned_here("atlas")
    stub.write_text('#!/bin/sh\necho "atlas community version v1.3.0"\n')
    os.utime(stub, ns=(1, 1))
    assert tools._pinned_here("atlas")
    # one that says no version is left as the operator has it
    stub.write_text("#!/bin/sh\necho hello\n")
    os.utime(stub, ns=(2, 2))
    assert tools._pinned_here("atlas")


def test_a_packaged_build_older_than_migkit_takes_is_said(
        home, tmp_path, monkeypatch):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    stub = tmp_path / "mydumper"
    stub.write_text('#!/bin/sh\necho "mydumper 0.9.5, built against MySQL'
                    ' 5.7"\n')
    stub.chmod(0o755)
    monkeypatch.setattr(tools, "which",
                        lambda n: str(stub) if n == "mydumper" else None)
    monkeypatch.setattr(tools, "_TARGET", {"system": "Darwin",
                                           "machine": "arm64"})
    said = []
    tools._replace_old_builds(["mydumper", "myloader"], said.append)
    assert len(said) == 1, said
    assert "0.9.5" in said[0] and "0.21" in said[0], said
    assert not [t for t in TOOLS if t in said[0].lower()], said


def test_the_pinned_packages_are_named_for_each_machine(monkeypatch):
    monkeypatch.setattr(tools, "_TARGET", {
        "system": "Linux", "machine": "x86_64",
        "release": {"ID": "ubuntu", "VERSION_ID": "22.04",
                    "VERSION_CODENAME": "jammy"}})
    assert tools.package_file("mydumper") == \
        "mydumper_1.0.5-1.jammy_amd64.deb"
    assert tools.package_file("mongodump") == \
        "mongodb-database-tools-ubuntu2204-x86_64-100.16.1.tgz"
    monkeypatch.setattr(tools, "_TARGET", {"system": "Darwin",
                                           "machine": "arm64"})
    assert tools.package_file("mydumper") is None
    assert tools.package_file("mongodump") == \
        "mongodb-database-tools-macos-arm64-100.16.1.zip"


# ---- archives unpacked inside where they are put ------------------------------

def test_an_archive_cannot_write_outside_where_it_is_unpacked(tmp_path):
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as z:
        z.writestr("../escaped", b"x")
    archive = tmp_path / "bad.zip"
    archive.write_bytes(body.getvalue())
    with pytest.raises(OSError):
        tools.extract(archive, tmp_path / "into")
    assert not (tmp_path / "escaped").exists()


def test_a_zip_keeps_the_modes_its_entries_carry(tmp_path):
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as z:
        info = zipfile.ZipInfo("top/bin/prog")
        info.external_attr = 0o755 << 16
        z.writestr(info, b"#!/bin/sh\n")
        z.writestr("top/lib/data", b"x")
    archive = tmp_path / "ok.zip"
    archive.write_bytes(body.getvalue())
    tools.extract(archive, tmp_path / "into")
    assert os.access(tmp_path / "into" / "top" / "bin" / "prog", os.X_OK)


# ---- the offline bundle ---------------------------------------------------------

def _bundle(tmp_path, files):
    root = tmp_path / "bundle"
    (root / "files").mkdir(parents=True)
    index = {}
    for url, (name, body) in files.items():
        (root / "files" / name).write_bytes(body)
        index[url] = f"files/{name}"
    (root / "files.json").write_text(json.dumps(index))
    tools._sign_bundle(root, lambda m: None)
    return root


def test_an_install_from_the_bundle_asks_no_network(served, tmp_path,
                                                    monkeypatch):
    files, pins, base = served
    url = "https://vendor.example.com/prog-1.0.tgz"
    pins[url] = {"digest": _sha(b"the build")}
    root = _bundle(tmp_path, {url: ("prog-1.0.tgz", b"the build")})
    monkeypatch.setenv("MIGKIT_BUNDLE", str(root))

    def no_network(*a, **k):
        raise AssertionError("the network was asked")
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    assert tools.fetch(url, tmp_path / "got", print)
    assert (tmp_path / "got").read_bytes() == b"the build"
    # a file the bundle does not hold is not fetched from anywhere else
    pins["https://vendor.example.com/other"] = {"digest": _sha(b"x")}
    assert not tools.fetch("https://vendor.example.com/other",
                           tmp_path / "other", print)


def test_a_bundle_altered_after_its_sums_is_refused(served, tmp_path,
                                                    monkeypatch):
    files, pins, base = served
    url = "https://vendor.example.com/prog-1.0.tgz"
    pins[url] = {"digest": None}
    root = _bundle(tmp_path, {url: ("prog-1.0.tgz", b"the build")})
    (root / "files" / "prog-1.0.tgz").write_bytes(b"something else")
    monkeypatch.setenv("MIGKIT_BUNDLE", str(root))
    with pytest.raises(SystemExit) as e:
        tools.fetch(url, tmp_path / "got", print)
    assert "not what its sums say" in str(e.value), e.value
    assert not (tmp_path / "got").exists()


def test_a_bundle_asked_to_be_signed_and_not_is_refused(tmp_path,
                                                         monkeypatch, home):
    root = _bundle(tmp_path, {})
    signers = tmp_path / "allowed_signers"
    signers.write_text("ci ssh-ed25519 AAAA\n")
    monkeypatch.setenv("MIGKIT_BUNDLE", str(root))
    monkeypatch.setenv("MIGKIT_BUNDLE_SIGNERS", str(signers))
    with pytest.raises(SystemExit) as e:
        tools._bundle()
    assert "not signed" in str(e.value), e.value


def test_a_bundle_is_signed_and_checked(tmp_path, monkeypatch, home):
    import shutil
    import subprocess
    if not shutil.which("ssh-keygen"):
        pytest.skip("no ssh-keygen here")
    key = tmp_path / "key"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(key)], check=True)
    signers = tmp_path / "allowed_signers"
    signers.write_text("ci " + (tmp_path / "key.pub").read_text())
    monkeypatch.setenv("MIGKIT_BUNDLE_KEY", str(key))
    root = _bundle(tmp_path, {})
    assert (root / "SHA256SUMS.sig").exists()
    monkeypatch.setenv("MIGKIT_BUNDLE", str(root))
    monkeypatch.setenv("MIGKIT_BUNDLE_SIGNERS", str(signers))
    assert tools._bundle() is not None
    # the sums changed after they were signed
    tools._BUNDLES.clear()
    with open(root / "SHA256SUMS", "a") as f:
        f.write("0" * 64 + "  files/planted\n")
    with pytest.raises(SystemExit) as e:
        tools._bundle()
    assert "not signed by anyone" in str(e.value), e.value


def test_an_image_loaded_from_a_bundle_runs_by_its_id(home, monkeypatch):
    ref = tools.image("pgcopydb")
    assert ref.startswith("dimitri/pgcopydb:v0.18@sha256:")
    tools._loaded("pgcopydb", ref, "sha256:" + "a" * 64)
    assert tools.image("pgcopydb") == "sha256:" + "a" * 64
    # loaded for another pin, it is not this one
    tools._loaded("pgcopydb", "dimitri/pgcopydb:v0.17", "sha256:" + "b" * 64)
    assert tools.image("pgcopydb") == ref


def test_a_bundle_names_the_machine_it_is_for():
    got = tools._platform("linux-x86_64-ubuntu-22.04")
    assert got["system"] == "Linux" and got["machine"] == "x86_64"
    assert got["release"]["VERSION_CODENAME"] == "jammy"
    with pytest.raises(SystemExit):
        tools._platform("windows-x86_64")
    assert "manylinux2014_x86_64" in tools._wheel_tags(
        tools._platform("linux-x86_64"))
    assert tools._wheel_tags(None) == []
