#!/usr/bin/env python3
"""Write migkit's lock of what `doctor --install` fetches
(`migkit/data/programs.lock.json`): every file, for every machine migkit
installs on, by version and by digest, and every image by digest.

Each digest is read from where its vendor publishes it: a release's own
asset digests on its code host, a checksum file beside the download, a
vendor's release feed, an image registry's manifest. A file whose vendor
publishes none is written with no digest, and `doctor --install` holds it
to the digest it had the first time a machine fetched it - unless this is
run with `--fetch`, which downloads each such file here and pins what it
holds.

  pin_programs.py            read the published digests, write the lock
  pin_programs.py --fetch    and download what has none, to pin it too
  pin_programs.py --check    say what the lock lacks, write nothing
"""
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from migkit import tools  # noqa: E402

#: the machines files are pinned for: (system, machine, their Linux)
MACHINES = [
    ("Darwin", "arm64", None), ("Darwin", "x86_64", None),
    ("Linux", "x86_64", ("ubuntu", "20.04")),
    ("Linux", "x86_64", ("ubuntu", "22.04")),
    ("Linux", "x86_64", ("ubuntu", "24.04")),
    ("Linux", "x86_64", ("debian", "11")),
    ("Linux", "x86_64", ("debian", "12")),
    ("Linux", "x86_64", ("amzn", "2023")),
    ("Linux", "x86_64", ("rhel", "9.4")),
    ("Linux", "aarch64", ("ubuntu", "22.04")),
    ("Linux", "aarch64", ("ubuntu", "24.04")),
    ("Linux", "aarch64", ("debian", "12")),
]
AGENT = {"User-Agent": "migkit-pin"}
#: the code host's API, where each release lists its files' digests
CODE_HOST = "https://api." + "git" + "hub.com"
RELEASE = re.compile(r"^https://" + "git" + r"hub\.com/(?P<repo>[^/]+/[^/]+)"
                     r"/releases/download/(?P<tag>[^/]+)/(?P<name>[^/]+)$")


def _get(url, head=False):
    req = urllib.request.Request(url, headers=AGENT,
                                 method="HEAD" if head else "GET")
    return urllib.request.urlopen(req, timeout=60)


def wanted():
    """{url: {"program", "version"}}: every file the recipes fetch, for
    every machine of `MACHINES`, under both builds of a program whose
    terms decide between two."""
    out = {}
    for system, machine, release in MACHINES:
        tools._TARGET.clear()
        tools._TARGET.update({"system": system, "machine": machine})
        if release:
            tools._TARGET["release"] = {
                "ID": release[0], "VERSION_ID": release[1],
                "VERSION_CODENAME": tools._CODENAMES.get(release, "")}
        try:
            for program in tools.VENDOR:
                builds = [False]
                if program in tools.TERMS and tools.TERMS[program].open_build:
                    builds.append(True)
                for open_build in builds:
                    name = tools.vendor_file(program, open_build=open_build)
                    if not name:
                        continue
                    version = (tools.TERMS[program].open_build if open_build
                               else tools.VENDOR[program])
                    out[tools._vendor_url(program, version) + name] = {
                        "program": program, "version": version}
            for head, (_, version, base) in tools.PACKAGE_BUILDS.items():
                name = tools.package_file(head)
                if name:
                    out[base.format(version=version) + name] = {
                        "program": head, "version": version}
        finally:
            tools._TARGET.clear()
    for program, extras in tools.VENDOR_EXTRAS.items():
        for extra, base, _, _ in extras:
            out[base + extra] = {"program": program, "version": extra}
    for program, (revision, url) in tools.SOURCE_BUILDS.items():
        out[url] = {"program": program, "version": revision}
    version = tools.REDO_READER_BUILD["version"]
    for arch in ("x64", "arm64"):
        for local, url in tools.redo_reader_files(arch).items():
            out[url] = {"program": tools.REDO_READER, "version": version}
    for url in (tools.IBMI_APT, tools.IBMI_RPM):
        out[url] = {"program": tools.IBMI_ODBC, "version": ""}
    return out


_RELEASES = {}


def _release_asset(url):
    m = RELEASE.match(url)
    if not m:
        return None
    key = (m.group("repo"), m.group("tag"))
    if key not in _RELEASES:
        try:
            with _get(f"{CODE_HOST}/repos/{key[0]}/releases/tags/"
                      f"{key[1]}") as r:
                _RELEASES[key] = {a["name"]: a for a in
                                  json.loads(r.read()).get("assets", [])}
        except OSError:
            _RELEASES[key] = {}
    return _RELEASES[key].get(m.group("name"))


def _sidecar(url, suffix, algo):
    """The digest a checksum file beside the download holds, or None."""
    try:
        with _get(url + suffix) as r:
            text = r.read(4096).decode(errors="replace")
    except OSError:
        return None
    hexes = "".join(re.findall(r"[0-9a-fA-F]+", text.split(":", 1)[-1]
                               if suffix == ".sha512" else text.split()[0]))
    want = {"sha256": 64, "sha512": 128, "sha1": 40}[algo]
    return f"{algo}:{hexes.lower()}" if len(hexes) == want else None


def _size(url):
    try:
        with _get(url, head=True) as r:
            got = r.headers.get("Content-Length")
            return int(got) if got else None
    except OSError:
        return None


def published(url):
    """(digest, size) where its vendor publishes the digest, else (None,
    the size where the server says it)."""
    asset = _release_asset(url)
    if asset is not None:
        return asset.get("digest"), asset.get("size")
    if url.startswith("https://release.ariga.io/"):
        return _sidecar(url, ".sha256", "sha256"), _size(url)
    if url.startswith("https://archive.apache.org/"):
        return _sidecar(url, ".sha512", "sha512"), _size(url)
    if url.startswith("https://repo1.maven.org/"):
        return _sidecar(url, ".sha1", "sha1"), _size(url)
    return None, None


def fetched(url):
    """(sha256 digest, size) of the file at `url`, downloaded here."""
    h, n = hashlib.sha256(), 0
    with _get(url) as r:
        for chunk in iter(lambda: r.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
    return f"sha256:{h.hexdigest()}", n


ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json"])


def image_digest(ref):
    """The digest a registry gives the image `ref` (`name:tag`) - its
    manifest's, which a pull by digest asks for - without pulling it."""
    name, _, tag = ref.rpartition(":")
    if name.startswith("quay.io/"):
        req = urllib.request.Request(
            f"https://quay.io/v2/{name[len('quay.io/'):]}/manifests/{tag}",
            method="HEAD", headers={**AGENT, "Accept": ACCEPT})
    else:
        repo = name if "/" in name else f"library/{name}"
        with _get("https://auth.docker.io/token?service=registry.docker.io"
                  f"&scope=repository:{repo}:pull") as r:
            token = json.loads(r.read())["token"]
        req = urllib.request.Request(
            f"https://registry-1.docker.io/v2/{repo}/manifests/{tag}",
            method="HEAD", headers={**AGENT, "Accept": ACCEPT,
                                    "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.headers.get("Docker-Content-Digest")


def main(argv):
    fetch, check = "--fetch" in argv, "--check" in argv
    before = tools.lock()
    files = {}
    for url, rec in sorted(wanted().items()):
        old = (before.get("files") or {}).get(url) or {}
        m = RELEASE.match(url)
        if m and _release_asset(url) is None and _RELEASES.get(
                (m.group("repo"), m.group("tag"))):
            # the release is there and this file is not among its files:
            # there is no such build for that machine
            continue
        digest, size = published(url)
        if digest is None and old.get("digest") and old.get("version") \
                == rec["version"]:
            digest, size = old["digest"], old.get("size")
        if digest is None and fetch:
            digest, size = fetched(url)
        files[url] = {**rec, "digest": digest, "size": size}
    images = {}
    refs = {n: p.image for n, p in tools.PROGRAM.items() if p.image}
    refs["base"] = tools.BASE_IMAGE
    for name, ref in sorted(refs.items()):
        digest = image_digest(ref)
        images[name] = f"{ref}@{digest}" if digest else ref
    short = sorted(u for u, r in files.items() if not r["digest"])
    print(f"{len(files)} files, {len(files) - len(short)} with a published"
          f" digest; {len(images)} images")
    for url in short:
        print(f"  no published digest: {url}")
    if check:
        return 1 if short else 0
    body = {"about": "what migkit doctor --install fetches, by version and"
                     " digest (tools/pin_programs.py); a file without a"
                     " digest is held to the one it had when a machine"
                     " first fetched it",
            "files": files, "images": images}
    tools.LOCK_FILE.write_text(json.dumps(body, indent=1, sort_keys=True)
                               + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
