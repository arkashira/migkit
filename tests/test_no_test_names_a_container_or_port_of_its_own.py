"""Lane tests reuse the lanes' container names and ports (row 543
AUD-Z.lane-names): a test file names its containers through
`servers.name` and its host ports through `servers.port` /
`servers.private_port` (tests/servers.py), never a literal - a fixed name
or port is one lane's, so two lanes running it at once take each other's
container or port. This fails on a literal container name, published
mapping or host port in a test file outside `tests/servers.py`; the files
written before the lanes are `OWED` (row 560 AUD-Z.lane-names-owed), each
leaving the list as it moves onto the lanes, and none may join it."""
import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
#: the lanes' own module, and the ones that spell around the rule to test it
OWNERS = {"servers.py", "test_the_suite_runs_in_lanes.py",
          pathlib.Path(__file__).name}
CONTAINER = re.compile(r"migkit-test-[a-z0-9][a-z0-9-]*")
MAPPING = re.compile(r"(?:127\.0\.0\.1:)?\d{4,5}:\d{2,5}(?:/tcp)?")
PORTISH = re.compile(r"PORT", re.I)

#: test files that still name a container or a host port of their own
OWED = set()


def literals(src):
    """Lines of a literal container name, published mapping or host port."""
    bad = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and (CONTAINER.fullmatch(node.value)
                     or MAPPING.fullmatch(node.value)):
            bad.append(node.lineno)
        if isinstance(node, ast.Assign):
            names = " ".join(ast.unparse(t) for t in node.targets)
            nums = [n for n in ast.walk(node.value)
                    if isinstance(n, ast.Constant) and type(n.value) is int
                    and 1024 <= n.value <= 65535]
            if nums and PORTISH.search(names):
                bad.append(node.lineno)
    return sorted(set(bad))


def _files():
    for path in sorted((ROOT / "tests").glob("*.py")):
        if path.name not in OWNERS:
            yield path.name, literals(path.read_text())


def test_no_test_names_a_container_or_port_of_its_own():
    found = {name for name, lines in _files() if lines}
    new = sorted(found - OWED)
    assert not new, "name them through tests/servers.py lanes: " + \
        ", ".join(f"{n}:{l}" for n, ls in _files() if n in new for l in ls)
    gone = sorted(OWED - found)
    assert not gone, f"on the lanes now - take them off OWED: {gone}"


def test_the_scan_would_notice_one():
    assert literals('SRC = "migkit-test-x-src"\n') == [1]
    assert literals('MY, PORT = "a", 15757\n') == [1]
    assert literals('args = ["-p", "127.0.0.1:15999:5432"]\n') == [1]
    assert literals('SRC = servers.name(servers.PREFIX + "-x")\n'
                    'PORT = servers.private_port(0)\n'
                    'LIMIT = 20000\n') == []
