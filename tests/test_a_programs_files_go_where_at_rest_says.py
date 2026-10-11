"""The files a driven program writes (a dump) lie where the hop's `at_rest`
says (backlog R17b.prog).

migkit cannot seal a program's own bytes, so the option names a place:
`at_rest.scratch`, a directory on a volume the operator keeps encrypted or
in memory. The files are private and removed when the move ends either way,
and the plan says what at_rest does and does not reach. No server: the
paths, the copy's lifetime and the words are proved here.
"""
import inspect
import stat

import pytest

from migkit import evidence, movers
from migkit.config import Endpoint, Hop


def _hop(tmp_path, at_rest=None):
    ep = Endpoint(host="x", port=0, user="", password="")
    hop = Hop(name="h1", engine="postgres", source=ep, target=ep,
              options={"at_rest": at_rest} if at_rest is not None else {})
    hop.report_dir = lambda db=None: tmp_path / "reports" / (db or "")
    return hop


def test_without_scratch_the_copy_stays_under_the_report_directory(tmp_path):
    hop = _hop(tmp_path)
    assert evidence.program_path(hop, "shop", "pgdump") \
        == tmp_path / "reports" / "shop" / "pgdump"
    hop = _hop(tmp_path, {"recipients": ["age1x"]})
    assert evidence.program_path(hop, "shop", "pgdump") \
        == tmp_path / "reports" / "shop" / "pgdump"


def test_scratch_takes_the_copy_out_of_the_report_directory(tmp_path):
    vol = tmp_path / "vol" / "enc"
    hop = _hop(tmp_path, {"recipients": ["age1x"], "scratch": str(vol)})
    got = evidence.program_path(hop, "shop", "mydumper")
    assert got == vol / "h1" / "shop" / "mydumper"
    assert not str(got).startswith(str(tmp_path / "reports"))
    for d in (vol, vol / "h1", vol / "h1" / "shop"):
        assert stat.S_IMODE(d.stat().st_mode) == 0o700, d


def test_a_relative_or_unusable_scratch_is_refused_in_words(tmp_path):
    hop = _hop(tmp_path, {"scratch": "relative/dir"})
    with pytest.raises(SystemExit, match="absolute path"):
        evidence.program_path(hop, "shop", "pgdump")
    blocker = tmp_path / "file"
    blocker.write_text("x")
    hop = _hop(tmp_path, {"scratch": str(blocker / "sub")})
    with pytest.raises(SystemExit, match="cannot be used"):
        evidence.program_path(hop, "shop", "pgdump")


def test_the_copy_in_scratch_is_private_and_gone_after_a_failure(tmp_path):
    hop = _hop(tmp_path, {"scratch": str(tmp_path / "vol")})
    where = evidence.program_path(hop, "shop", "pgdump")
    where.mkdir()
    (where / "left-by-a-dead-run").write_text("rows")
    with pytest.raises(RuntimeError):
        with movers._LocalCopy(where):
            assert not (where / "left-by-a-dead-run").exists()
            assert stat.S_IMODE(where.stat().st_mode) == 0o700
            (where / "toc.dat").write_text("rows")
            raise RuntimeError("the load failed")
    assert not where.exists()
    assert (tmp_path / "vol" / "h1" / "shop").is_dir()


def test_the_plan_says_how_far_at_rest_reaches(tmp_path):
    assert evidence.program_files_note(_hop(tmp_path)) is None
    clear = evidence.program_files_note(
        _hop(tmp_path, {"recipients": ["age1x"]}))
    assert "in the clear" in clear and "at_rest.scratch" in clear
    placed = evidence.program_files_note(
        _hop(tmp_path, {"scratch": "/mnt/enc"}))
    assert "/mnt/enc" in placed and "cannot check" in placed
    assert "in the clear" not in placed


def test_every_dump_path_asks_the_one_owner_where_it_lies():
    """The dump-and-load paths place their copy through
    `evidence.program_path`; none builds it under the report directory."""
    for fn in (movers.pgdump_move, movers.mydumper_table,
               movers.mydumper_move):
        src = inspect.getsource(fn)
        assert "evidence.program_path(" in src, fn.__name__
        assert 'report_dir(db) / "pgdump"' not in src
        assert 'root / "mydumper"' not in src
