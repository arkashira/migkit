"""No program migkit runs is handed a password on its command line.

A command line is readable by every process listing on the machine, and it
reaches whatever log records the commands a run made. The MySQL bulk path
was fixed for this and guarded, in one file (`test_the_mysql_bulk_path_runs`).
The same mistake was then found in four other places, which that guard did
not cover:
* the MySQL schema dump took `-p<password>`
* the table sync took `p=<password>` in both of its connection strings
* the object comparison took `--password=` and `--referencePassword=`
* the schema comparison took both URLs, passwords inside, as arguments
They now pass the password in the environment, in a private defaults file,
or in a config file that reads the environment. This guard covers every
module.
"""
import ast
import pathlib

RUNNERS = {"_sh", "run", "Popen", "check_output", "check_call"}


def _reads_password(node, tainted, helpers):
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == "password":
            return True
        if isinstance(sub, ast.Name) and sub.id in tainted:
            return True
        if isinstance(sub, ast.Call):
            f = sub.func
            name = (f.id if isinstance(f, ast.Name)
                    else f.attr if isinstance(f, ast.Attribute) else None)
            if name in helpers and not any(
                    k.arg == "secret" and isinstance(k.value, ast.Constant)
                    and not k.value.value for k in sub.keywords):
                return True
    return False


def _tainted(fn, helpers):
    """Names in `fn` a password reaches: assigned from `.password`, from
    another such name, or from a helper that returns one - to a fixpoint,
    so a command line built in a variable is still a command line."""
    tainted = set()
    while True:
        grew = False
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign) and _reads_password(
                    node.value, tainted, helpers):
                for t in node.targets:
                    for n in ast.walk(t):
                        if isinstance(n, ast.Name) and n.id not in tainted:
                            tainted.add(n.id)
                            grew = True
        if not grew:
            return tainted


def _helpers(tree):
    """Functions whose return value carries a password (`_mongo_uri`),
    unless asked for without it (`secret=False` is their default)."""
    out = set()
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        names = {a.arg for a in fn.args.args + fn.args.kwonlyargs}
        if "secret" in names:
            continue
        tainted = _tainted(fn, set())
        if any(isinstance(r, ast.Return) and r.value is not None
               and _reads_password(r.value, tainted, set())
               for r in ast.walk(fn)):
            out.add(fn.name)
    return out


def _handed_a_password(src):
    """(line, runner) where a runner's command line reads `.password` -
    directly, through a variable, or through a helper that returns it."""
    tree = ast.parse(src)
    helpers = _helpers(tree)
    bad = []
    scopes = [n for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    for scope in scopes or [tree]:
        tainted = _tainted(scope, helpers)
        for node in ast.walk(scope):
            if not (isinstance(node, ast.Call) and node.args):
                continue
            f = node.func
            name = (f.id if isinstance(f, ast.Name)
                    else f.attr if isinstance(f, ast.Attribute) else None)
            if name not in RUNNERS:
                continue
            if _reads_password(node.args[0], tainted, helpers):
                bad.append((node.lineno, name))
    if not scopes:
        return bad
    # a call at module level, outside any function
    top = [n for n in tree.body
           if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef))]
    for stmt in top:
        for node in ast.walk(stmt):
            if (isinstance(node, ast.Call) and node.args
                    and getattr(node.func, "id",
                                getattr(node.func, "attr", None)) in RUNNERS
                    and _reads_password(node.args[0], set(), helpers)):
                bad.append((node.lineno, "module"))
    return sorted(set(bad))


def test_no_module_hands_a_program_a_password():
    root = pathlib.Path(__file__).resolve().parents[1] / "migkit"
    found = {str(p.relative_to(root)): _handed_a_password(p.read_text())
             for p in sorted(root.rglob("*.py"))}
    assert not {k: v for k, v in found.items() if v}, found


def test_the_guard_finds_each_shape_it_replaced():
    for old in ('run(["mysqldump", "-u", ep.user, f"-p{ep.password}"])',
                'run(["pt-table-sync", f"h={s.host},p={s.password},D=x"])',
                'run(["liquibase", "diff", f"--password={t.password}"])',
                'subprocess.Popen(["psql", f"password={s.password}"])'):
        assert _handed_a_password(old), old
    # built in a variable first - the MongoDB paths did this, and the
    # guard looked only at the call
    assert _handed_a_password(
        'def f(s):\n    cmd = ["mongodump", f"--uri=x:{s.password}@h"]\n'
        '    subprocess.Popen(cmd)\n')
    # through a helper that returns the password inside an address
    assert _handed_a_password(
        'def uri(ep):\n    auth = f"{ep.user}:{ep.password}@"\n'
        '    return f"mongodb://{auth}h/"\n'
        'def f(s):\n    cmd = ["mongodump", f"--uri={uri(s)}"]\n'
        '    run(cmd)\n')
    # and the environment is not the command line
    assert not _handed_a_password(
        'run(["mysqldump", "-u", ep.user], env={"MYSQL_PWD": ep.password})')
