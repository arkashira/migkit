"""What every large-value test (`test_large_values_on_*`) holds an engine
to, and the runner it holds it in (backlog P0-PARITY, `migkit.pieces`).

migkit runs from this checkout in a container of its own held to a memory
limit, no swap (`cap_pairs.run_capped`), with the drivers the engine's move
imports; its servers are reached on a docker network by their container
names. A move of values twice what one batch holds (`pieces.BATCH_BYTES`),
or as large as the engine takes one, is held to the memory the resource
model says one worker needs, and room over it for the process itself: it
finishes, is never killed for memory, `check` then finds both sides the
same held to the same memory, and the values read back on each side here
are the same. Held to less than one worker needs, the move says so before
it starts and copies nothing.

`CAP_CALIBRATE=1` measures what one worker holds instead: the move held to
one at a time in a container with room to spare, and its peak - the
numbers `pieces.PAIR_MEMORY` and the engines' `COPY_MEMORY` are set from.
Containers are named `migkit-test-lv-*`, their ports 16370-16379.
"""
import hashlib
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from migkit import pieces
from tests import cap_pairs

MB = 2 ** 20
#: a large value: twice what one batch of a copy holds
WIDE = 2 * pieces.BATCH_BYTES
NETWORK = "migkit-test-lv"
CALIBRATE = bool(os.environ.get("CAP_CALIBRATE"))
HOP = "lv"


def work_dir():
    """A directory the docker machine shares with this one (under the
    home directory, which colima mounts), emptied first."""
    d = Path(os.path.expanduser(f"~/.cache/migkit-lv-{os.getpid()}"))
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    return d


def runner_image(*packages, apt=()):
    """An image that runs migkit from this checkout with the drivers named:
    Python, and what every move imports."""
    tag = "migkit-test-lv-runner:" + hashlib.sha256(
        " ".join(sorted(packages) + ["|"] + sorted(apt)).encode()
    ).hexdigest()[:12]
    if subprocess.run(["docker", "image", "inspect", tag],
                      capture_output=True).returncode == 0:
        return tag
    build = Path(os.path.expanduser(f"~/.cache/{tag.replace(':', '-')}"))
    build.mkdir(parents=True, exist_ok=True)
    (build / "Dockerfile").write_text(
        "FROM python:3.12-slim\n"
        + (f"RUN apt-get update && apt-get install -y"
           f" --no-install-recommends {' '.join(apt)} && rm -rf"
           " /var/lib/apt/lists/*\n" if apt else "")
        + "RUN pip install --no-cache-dir click rich pyyaml psutil"
        + "".join(f" {p}" for p in packages) + "\n")
    got = subprocess.run(["docker", "build", "-t", tag, str(build)],
                         capture_output=True, text=True, timeout=1800)
    assert got.returncode == 0, got.stdout[-2000:] + got.stderr[-2000:]
    return tag


def network():
    """The network the servers and the runner share, made once."""
    if not cap_pairs.docker_up():
        pytest.skip("docker not available")
    subprocess.run(["docker", "network", "create", NETWORK],
                   capture_output=True)
    return NETWORK


def start(name, port, inner, image, *args, env=(), extra=()):
    """A server's container on the shared network, its `inner` port
    published on 127.0.0.1:`port`."""
    cap_pairs.remove(name)
    envs = [x for k in env for x in ("-e", k)]
    got = subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                          NETWORK, "-p", f"127.0.0.1:{port}:{inner}", *envs,
                          *extra, image, *args], capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr[-2000:]


def until(ask, what, seconds=180):
    """`ask()` until it answers truthy, or the test fails naming `what`."""
    end = time.time() + seconds
    last = None
    while time.time() < end:
        try:
            got = ask()
            if got:
                return got
        except Exception as e:  # noqa: BLE001 - not up yet
            last = e
        time.sleep(2)
    pytest.fail(f"{what} never answered: {last}")


def run(work, memory, *args, image, env=None):
    """`migkit <args>` in the runner held to `memory` bytes, on the shared
    network: (exit code, what it printed, killed for memory, its peak)."""
    return cap_pairs.run_capped(work, f"{int(memory) // MB}m", *args,
                                env=env, image=image, network=NETWORK)


def held_to_its_memory(work, hops, eng, db, image, empty, digest=None,
                       env=None, after=None):
    """The move of `hops` (a hops.yaml naming the hop `HOP`) held to what
    `eng` - the same hop as this machine reaches its servers - says one
    worker needs, and a third more or 256 MB for the process itself,
    whichever is more (the room a move sees is its limit less what it
    already uses): finished and never killed; `check` of it held to the
    same, all green; `digest("src") == digest("dst")` where a digest read
    here is given; `after(cap)` for what else the engine is held to. Then,
    the target emptied (`empty`), held to less than one worker needs:
    refused before it starts, nothing copied. Returns what the model said
    one worker holds."""
    from migkit import model
    from migkit.sizing import RESERVE_MEMORY
    one, widest = model.worker_memory(eng, eng.row_sizes("src", db), db=db)
    assert widest and widest[1] > pieces.PIECE, widest
    if CALIBRATE:
        (work / "hops.yaml").write_text(hops + "    workers: 1\n")
        code, out, killed, peak = run(work, 3 * 2 ** 30, "move", HOP,
                                      "--go", image=image, env=env)
        assert code == 0 and not killed, out[-3000:]
        print(f"\n{type(eng).__name__}: one worker peaked at"
              f" {(peak or 0) / MB:,.0f} MB for a widest row of"
              f" {widest[1] / MB:,.0f} MB; the model said"
              f" {one / MB:,.0f} MB")
        return one
    (work / "hops.yaml").write_text(hops)
    cap = int(one + RESERVE_MEMORY + max(one // 3, 256 * MB))
    code, out, killed, peak = run(work, cap, "move", HOP, "--go",
                                  image=image, env=env)
    print(f"\n{type(eng).__name__}: held to {cap / MB:,.0f} MB, peaked at"
          f" {(peak or 0) / MB:,.0f} MB; widest row {widest[1] / MB:,.0f}"
          " MB")
    assert not killed, out[-3000:]
    assert code == 0, out[-3000:]
    assert "memory: the widest row is ~" in out, out[-3000:]
    code, out, killed, _ = run(work, cap, "check", HOP, "--only", "data",
                               image=image, env=env)
    assert not killed, out[-3000:]
    assert code == 0 and "DIFF" not in out and "all green" in out, \
        out[-3000:]
    if digest is not None:
        src = digest("src")
        assert src and src == digest("dst")
    if after is not None:
        after(cap)
    empty()
    if digest is not None:
        assert not digest("dst")
    # less than one worker of these rows needs
    low = int(RESERVE_MEMORY + one - one // 4)
    code, out, killed, _ = run(work, low, "move", HOP, "--go", image=image,
                               env=env)
    assert not killed, out[-3000:]
    assert code != 0 and "not started" in out and \
        "nothing is cut short" in out, out[-3000:]
    if digest is not None:
        assert not digest("dst")
    return one


def md5_of(parts):
    """The md5 of values in order, each after its length - two lists that
    split the same bytes differently are not the same."""
    h = hashlib.md5(usedforsecurity=False)
    for p in parts:
        p = p if isinstance(p, (bytes, bytearray)) else str(p).encode()
        h.update(len(p).to_bytes(8, "big"))
        h.update(p)
    return h.hexdigest()
