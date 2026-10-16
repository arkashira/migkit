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


# ---- the source inventory and the layers (P1-SOURCE-HIERARCHY) --------------

sys.path.insert(0, str(ROOT / "tools"))
import gen_codemap  # noqa: E402


def test_no_file_is_left_without_a_package():
    """The table in the generator claims every file of migkit/ and tools/;
    a new file nothing claims is listed as unassigned and fails here."""
    left = [f for f in gen_codemap._files()
            if gen_codemap.package_of(f) is None]
    assert left == [], ("add them to the package table in"
                        " tools/gen_codemap.py (_BY_NAME / _BY_PREFIX): "
                        f"{left}")


def test_a_file_no_package_claims_is_unassigned():
    assert gen_codemap.package_of("migkit/zz_not_yet_placed.py") is None
    assert gen_codemap.package_of("migkit/engines/zz_new.py") == "engines"
    assert gen_codemap.package_of("migkit/data/new.json") == "data"
    assert gen_codemap.package_of("tools/anything.py") == "tools"


def test_every_package_the_table_names_is_a_layer_or_outside():
    named = set(gen_codemap._TOP_OF.values()) | set(
        gen_codemap._BY_PREFIX.values())
    known = set(gen_codemap._RANK) | {p for p, _ in gen_codemap.OUTSIDE}
    assert named <= known, named - known


def test_every_file_is_reached_by_a_test_or_is_an_entry_point():
    from tests.test_every_module_is_reached import ENTRY
    none = [f for f, (kind, _t) in gen_codemap.reached_by_tests().items()
            if kind == "none" and gen_codemap._modname(f) not in ENTRY]
    assert none == [], ("no test imports these, nor a module they import:"
                        f" {none}")


def test_the_inventory_lists_every_file_once():
    text = (ROOT / "docs" / "agents" / "INVENTORY.md").read_text()
    for f in gen_codemap._files():
        assert text.count(f"| `{f}` |") == 1, f
    assert "## Unassigned\n\nNone." in text


def test_the_check_names_a_stale_inventory_or_layers_page(tmp_path,
                                                        monkeypatch):
    """`--check` is behind where any of the four written pages is: the
    inventory and the layers are held as the map and the state are."""
    pages = {"MAP": "map", "STATE": "state", "INVENTORY": "inv",
             "LAYERS_DOC": "layers"}
    for attr, text in pages.items():
        path = tmp_path / attr
        path.write_text(text)
        monkeypatch.setattr(gen_codemap, attr, path)
    for fn, attr in (("codemap", "MAP"), ("state", "STATE"),
                     ("inventory", "INVENTORY"), ("layers", "LAYERS_DOC")):
        monkeypatch.setattr(gen_codemap, fn, lambda t=pages[attr]: t)
    assert gen_codemap.main(["--check"]) == 0
    for attr in pages:
        path = tmp_path / attr
        path.write_text("behind")
        assert gen_codemap.main(["--check"]) == 3, attr
        path.write_text(pages[attr])
    (tmp_path / "INVENTORY").unlink()
    assert gen_codemap.main(["--check"]) == 3
