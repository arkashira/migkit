"""migkit's MD5 and SHA-1 are digests, never security, and say so.

Every one of them folds rows or names into a number to compare or to name
a file - the in-server digests must stay MD5 to match `md5()` on the
server. On a FIPS build of OpenSSL a bare `hashlib.md5()` raises
("unsupported hash type"), so a run on a FIPS host stopped at the first
fold. `usedforsecurity=False` is the flag that tells the library what the
hash is for; it changes nothing in the result.
"""
import ast
import hashlib
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1] / "migkit"
WEAK = ("md5", "sha1")


def _weak_calls():
    for path in sorted(ROOT.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = (fn.attr if isinstance(fn, ast.Attribute)
                    else fn.id if isinstance(fn, ast.Name) else "")
            if name in WEAK or (name == "new" and node.args
                                and isinstance(node.args[0], ast.Constant)
                                and str(node.args[0].value).lower() in WEAK):
                yield path.relative_to(ROOT.parent), node


def _flagged(node):
    return any(k.arg == "usedforsecurity"
               and isinstance(k.value, ast.Constant) and k.value.value is False
               for k in node.keywords)


def test_every_md5_and_sha1_says_it_is_not_for_security():
    calls = list(_weak_calls())
    # the scan reaches the sites it is about: the fold, the tally, the
    # document hash, the stage name, the file names
    assert len(calls) >= 9, calls
    bare = [f"{p}:{n.lineno}" for p, n in calls if not _flagged(n)]
    assert not bare, bare


def test_the_scan_would_notice_a_bare_one():
    planted = ast.parse("import hashlib\nh = hashlib.md5(b'x')\n"
                        "g = hashlib.sha1(b'x', usedforsecurity=False)\n")
    calls = [n for n in ast.walk(planted) if isinstance(n, ast.Call)
             and getattr(n.func, "attr", "") in WEAK]
    assert [_flagged(n) for n in calls] == [False, True]


def test_the_flag_changes_no_digest():
    """The sums the server computes with `md5()` are matched in Python:
    the flag must not move a single bit of them."""
    for data in (b"", b"row\x00text", "ไทย".encode()):
        assert hashlib.md5(data, usedforsecurity=False).hexdigest() \
            == hashlib.md5(data).hexdigest()
    from migkit import canon, tally
    assert canon.digest_step(7, "abc") == 7 + int(
        hashlib.md5(b"abc").hexdigest()[:canon.DIGEST_HEX], 16)
    t = tally.Tally()
    t.row(b"abc")
    assert t.total == int(hashlib.md5(b"abc").hexdigest()[:16], 16)
