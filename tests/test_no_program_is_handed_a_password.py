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


def _handed_a_password(src):
    """(line, runner) where a runner's command line reads `.password`."""
    bad = []
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.Call) and node.args):
            continue
        f = node.func
        name = (f.id if isinstance(f, ast.Name)
                else f.attr if isinstance(f, ast.Attribute) else None)
        if name not in RUNNERS:
            continue
        if any(isinstance(sub, ast.Attribute) and sub.attr == "password"
               for sub in ast.walk(node.args[0])):
            bad.append((node.lineno, name))
    return bad


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
    # and the environment is not the command line
    assert not _handed_a_password(
        'run(["mysqldump", "-u", ep.user], env={"MYSQL_PWD": ep.password})')
