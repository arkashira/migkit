"""The code map says where everything is, as the code is now.

A session that cannot see where things are searches again, and twice it
wrote over or beside something that already existed. `docs/agents/
CODEMAP.md` is what is read first instead; the pre-commit hook writes it
again when it is behind, and this catches a commit that went round the
hook.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_the_map_is_the_codes():
    got = subprocess.run([sys.executable, "tools/gen_codemap.py", "--check"],
                         cwd=ROOT)
    assert got.returncode == 0, ("docs/agents/CODEMAP.md is behind the code:"
                                 " run python tools/gen_codemap.py")


def test_every_module_is_on_the_map():
    text = (ROOT / "docs" / "agents" / "CODEMAP.md").read_text()
    missing = [p.relative_to(ROOT).as_posix()
               for p in (ROOT / "migkit").rglob("*.py")
               if "__pycache__" not in p.parts
               and f"`{p.relative_to(ROOT).as_posix()}`" not in text]
    assert missing == []
