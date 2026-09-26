"""The MongoDB dump and load take their passwords from a private file.

Both were handed `--uri=mongodb://user:password@...`: a command line every
process listing on the machine can read. The guard against that looked at
the call, and the command line was built in a variable first, so it saw
nothing (`test_no_program_is_handed_a_password.py` now follows variables
and helpers). Measured with the Database Tools 100.16: `--config` holding
`password:` beside a `--uri` without one signed in, and a wrong one there
exited 1.
"""
import subprocess
import time

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MG, PORT, SECRET = "migkit-test-mongo-auth", 15800, "CHANGE_ME-pw-77"


def test_a_signed_in_move_with_no_password_on_any_command_line(tmp_path,
                                                               monkeypatch):
    from migkit import movers
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                        f"{PORT}:27017", "-e",
                        "MONGO_INITDB_ROOT_USERNAME=root", "-e",
                        f"MONGO_INITDB_ROOT_PASSWORD={SECRET}", "mongo:7"],
                       check=True, capture_output=True)
        shell = ["docker", "exec", MG, "mongosh", "--quiet", "-u", "root",
                 "-p", SECRET, "--authenticationDatabase", "admin"]
        for _ in range(60):
            if subprocess.run(shell + ["--eval", "1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        subprocess.run(shell + ["app", "--eval",
                                "db.t.insertMany([{_id: 1}, {_id: 2}])"],
                       check=True, capture_output=True)
        ep = Endpoint(host="127.0.0.1", port=PORT, user="root",
                      password=SECRET,
                      options={"uri_options": "authSource=admin"})
        hop = Hop(name="ma", engine="mongodb", source=ep, target=ep,
                  databases=["app"], db_map={"app": "app2"})
        hop.report_dir = lambda db=None: tmp_path
        seen = []
        real = subprocess.Popen

        def spy(cmd, *a, **k):
            if str(cmd[0]).startswith("mongo"):
                seen.append(list(cmd))
            return real(cmd, *a, **k)
        monkeypatch.setattr(movers.subprocess, "Popen", spy)
        movers.mongodump_move(hop, "app", 1, True, None)
        n = subprocess.run(shell + ["app2", "--eval",
                                    "db.t.countDocuments()"],
                           capture_output=True, text=True).stdout.strip()
        assert n == "2", n
        assert len(seen) == 2, seen
        assert all(SECRET not in " ".join(c) for c in seen), seen
        assert all(any(a.startswith("--config=") for a in c) for c in seen)
        # and the files went with the programs
        assert not list(tmp_path.glob("mongo-auth-*")), list(
            tmp_path.iterdir())
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
