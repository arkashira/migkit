"""Going back after a cutover does not hand out a key the new primary
already used (research: cutover, section 6).

After the flip the application writes on the new primary, and the stream
back (`reverse: at_cutover`) carries those rows to the old source - rows,
not counters. `rollback` put the *target's* counters back and nothing
raised the old source's, so the first insert once the application was
back on the old source took an id the new primary had already given out.
Measured on PostgreSQL 16 before the fix: `duplicate key value violates
unique constraint "t_pkey"` on that first insert.

Now `rollback --apply`, on a hop that streams back or cuts over, raises
the old source's counters past the new primary's - only ever raises - and
records it; a hop with neither is told that nothing is written to its
source.
"""
import json

from click.testing import CliRunner

from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.rollback", "postgres")]

pytestmark = needs_docker


def _hop(pg_pair, tmp_path, monkeypatch, options):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  back:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        f"    options: {{state: {{mirror: {tmp_path / 'mirror'}}},"
        f" {options[1:]}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _cli(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _after_a_flip(pg_pair):
    """Both sides as a cutover with a stream back leaves them: the source's
    rows copied, the target's counter past them, then three rows the new
    primary took, carried back to the old source by value."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id serial primary key, v text)")
    psql(pg_pair["src"], "insert into public.t (v) values ('a'), ('b'),"
                         " ('c')")
    psql(pg_pair["dst"], "insert into public.t values (1, 'a'), (2, 'b'),"
                         " (3, 'c'); select setval('public.t_id_seq', 3)")


def _new_primary_writes(pg_pair):
    psql(pg_pair["dst"], "insert into public.t (v) values ('d'), ('e'),"
                         " ('f')")
    psql(pg_pair["src"], "insert into public.t values (4, 'd'), (5, 'e'),"
                         " (6, 'f')")


def test_the_old_source_takes_no_key_the_new_primary_used(
        pg_pair, tmp_path, monkeypatch):
    _hop(pg_pair, tmp_path, monkeypatch, "{reverse: at_cutover}")
    _after_a_flip(pg_pair)
    got, said = _cli("sync", "back", "--go", "--kind", "sequences", "--tag",
                     "pre-cutover")
    assert got.exit_code == 0, said
    _new_primary_writes(pg_pair)
    got, said = _cli("rollback", "back", "--db", "postgres", "--apply")
    assert got.exit_code == 0, said
    # the first insert on the old source, as the application's would be
    first = psql(pg_pair["src"], "insert into public.t (v) values ('g')"
                                 " returning id")
    assert first.returncode == 0, first.stderr
    assert first.stdout.strip().splitlines()[0] == "7", first.stdout
    assert "old source: 1 counters raised past the new primary's" in said, \
        said
    record = [json.loads(line) for line in
              (tmp_path / "reports" / "back" / "changelog.jsonl")
              .read_text().splitlines()]
    raised = [r for r in record if r.get("op") == "rollback-counters"]
    assert len(raised) == 1 and raised[0]["n"] == 1, raised
    assert raised[0]["undo"], raised


def test_without_a_stream_back_the_source_is_not_written(
        pg_pair, tmp_path, monkeypatch):
    _hop(pg_pair, tmp_path, monkeypatch, "{}")
    _after_a_flip(pg_pair)
    got, said = _cli("sync", "back", "--go", "--kind", "sequences", "--tag",
                     "pre-cutover")
    assert got.exit_code == 0, said
    _new_primary_writes(pg_pair)
    got, said = _cli("rollback", "back", "--db", "postgres", "--apply")
    assert got.exit_code == 0, said
    assert "the old source's counters are left as they are" in said, said
    assert "migkit writes nothing to the source" in said, said
    assert psql(pg_pair["src"], "select last_value from public.t_id_seq"
                ).stdout.strip() == "3"
