"""The MySQL bulk programs used whole, and chosen table by table (backlog
0f, the owner's decisions of 2026-09-28).

The dump and load used to be driven with threads, a consistency flag and a
machine log - nothing else they have. Now each table is cut into chunks
sized from its facts, both sides checksum every table and migkit holds the
move to each pair beside its own digest, the columns a hop masks on the
way are masked by the dump where the rule is exact, and the straight copy
and migkit's own table copier are rungs beside it, one table ladder each
table climbs from measured rates (`test_one_table_ladder.py`). These are
the parts that need no server; the ones that do are in
`test_the_mysql_bulk_rungs_run.py`.
"""
import io
import json
import os
import pathlib
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(tmp_path, options=None, mapping=None, exclude=()):
    hop = Hop(name="w", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"], options=dict(options or {}),
              mapping=dict(mapping or {}), exclude=list(exclude))
    root = tmp_path / "reports"

    def report_dir(db=None):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


needs_dump = pytest.mark.skipif(
    not (movers.which("mydumper") and movers.which("myloader")),
    reason="the dump and load programs are not installed here")


# ---- the command lines ------------------------------------------------------

@needs_dump
def test_both_sides_checksum_and_the_load_warns_rather_than_stops(tmp_path):
    """Measured on 1.0.5: without `--checksum-all` on the load as well,
    its checksum step compares nothing - a dumped checksum edited to a
    wrong value loaded without a word. `warn` so every table is named."""
    dump, load = movers._mydumper_commands(_hop(tmp_path), "appdb", 3,
                                           tmp_path / "d", logs=False)
    assert "--checksum-all" in dump and "--checksum-all" in load
    assert load[load.index("--checksum") + 1] == "warn"
    for argv in (dump, load):
        i = argv.index("--max-threads-per-table")
        assert argv[i + 1] == "3", argv
    # written plain unless the disk is short: zstd measured slower here
    assert "--compress" not in dump
    zdump, _ = movers._mydumper_commands(_hop(tmp_path), "appdb", 3,
                                         tmp_path / "d", logs=False,
                                         compress=True)
    assert zdump[zdump.index("--compress") + 1] == "zstd"
    for argv, program in ((zdump, "mydumper"), (load, "myloader")):
        flags = {a for a in argv if str(a).startswith("--")}
        assert flags <= movers._long_options(program), \
            flags - movers._long_options(program)


def test_the_endpoint_tls_reaches_both_programs(tmp_path):
    hop = _hop(tmp_path)
    hop.source.options = {"ssl_ca": "/etc/ca.pem", "ssl_cert": "/c",
                          "ssl_key": "/k"}
    got = movers._my_tls(hop.source, "mydumper")
    assert got[:4] == ["--ssl-mode", "VERIFY_IDENTITY", "--ca", "/etc/ca.pem"]
    assert "--cert" in got and "--key" in got
    shell = movers._my_tls(hop.source, "mysqlsh")
    assert shell["ssl-mode"] == "VERIFY_IDENTITY" and shell["ssl-ca"] \
        == "/etc/ca.pem" and shell["ssl-cert"] == "/c", shell
    hop.source.options = {"ssl_ca": "/ca", "ssl_verify_identity": False}
    assert movers._my_tls(hop.source, "mysqlsh")["ssl-mode"] == "VERIFY_CA"
    hop.source.options = {}
    assert movers._my_tls(hop.source, "mydumper") == []
    assert movers._my_tls(hop.source, "mysqlsh") == {}


def test_the_option_column_reads_an_option_beside_its_alias(tmp_path):
    """`--py, --python`: 81 of the copy program's 91 options were found
    until both halves of such a line were read."""
    fake = tmp_path / "prog"
    fake.write_text("#!/bin/sh\ncat <<'EOF'\n"
                    "  --py, --python                   Start in Python.\n"
                    "  --nw, --no-wizard                Disables wizard.\n"
                    "  -f, --file=<file>                Specify a file.\n"
                    "EOF\n")
    fake.chmod(0o755)
    got = movers._options_of(str(fake), 1)
    assert {"--py", "--python", "--nw", "--no-wizard", "--file"} <= got, got


# ---- chunks, per table ------------------------------------------------------

def test_a_large_table_is_cut_so_every_worker_has_a_part():
    # 10 million rows of 100 bytes, read at 400 MB/s over 4 workers:
    # 100 MB/s a worker, 1.5 s a chunk -> 1.5 million rows, capped at the
    # table split across the workers
    got = movers.my_chunk_rows({"rows": 10_000_000, "bytes": 1_000_000_000},
                               400e6, 4)
    lo, start, hi = (int(x) for x in got.split(":"))
    assert start == 1_500_000 and lo == 150_000 and hi == 15_000_000
    got = movers.my_chunk_rows({"rows": 1_000_000, "bytes": 100_000_000},
                               400e6, 4)
    assert int(got.split(":")[1]) == 250_000, got
    # a slower link, smaller chunks
    slow = movers.my_chunk_rows({"rows": 10_000_000, "bytes": 1_000_000_000},
                                4e6, 4)
    assert int(slow.split(":")[1]) == 15_000, slow


def test_the_straight_copys_chunk_is_four_a_worker_of_the_largest():
    """Measured: 16 MB chunks copied the two tables in 7.25 s, its default
    64 MB in 9.89 s - about four chunks a worker of the 222 MB table."""
    got = movers._shell_chunk(280e6, 4, 222_052_352)
    assert 13 * 2 ** 20 < got < 14 * 2 ** 20, got
    assert movers._shell_chunk(280e6, 4, 1000) == movers.SHELL_CHUNK_FLOOR
    assert movers._shell_chunk(None, 4, None) == movers.SHELL_CHUNK


def test_the_straight_copy_reads_and_writes_at_once_only_where_the_disk_is_short(
        tmp_path, monkeypatch):
    from migkit import tableway
    from migkit.engines.mysql import MySQLEngine
    hop = _hop(tmp_path)
    facts = {"wide": {"rows": 500_000, "bytes": 220_000_000,
                      "binary": True}}
    monkeypatch.setattr(movers, "_disk_free", lambda path: 10 ** 12)
    assert not movers._shell_direct(hop, "appdb", facts)
    monkeypatch.setattr(movers, "_disk_free", lambda path: 10 ** 6)
    assert movers._shell_direct(hop, "appdb", facts)
    # and the table ladder's cost model then costs the straight copy as
    # measured that way: base64 of the binary columns, 9.59 s against
    # 4.64 s on `wide` (MY_BENCH)
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    m = tableway.model(MySQLEngine(hop), "appdb")
    wide = {"bytes": 222e6, "rows": 500_000}
    at_once = tableway._On(m, wide, (movers.STREAM_COPY,))
    through = tableway._On(m, wide)
    assert at_once.get(movers.STREAM_COPY) > \
        2 * through.get(movers.STREAM_COPY)
    # a way with no rate measured that way reads its own
    assert at_once.get(movers.DUMP_LOAD) == through.get(movers.DUMP_LOAD)


def test_a_small_table_is_one_chunk():
    assert movers.my_chunk_rows({"rows": 7_000, "bytes": 700_000}, 4e8,
                                4) is None
    assert movers.my_chunk_rows({}, 4e8, 4) is None


def test_the_chunk_and_the_filter_share_one_section():
    hop = Hop(name="w", engine="mysql", source=Endpoint(), target=Endpoint(),
              mapping={"where": {"orders": "region = 'apac'"}})
    got = movers.mydumper_defaults(hop, "appdb",
                                   {"orders": ["rows = 1:2:3"],
                                    "people": ["`email` = null"]})
    assert got.count("[`appdb`.`orders`]") == 1, got
    orders = got.split("[`appdb`.`orders`]")[1].split("[")[0]
    assert "where = region = 'apac'" in orders and "rows = 1:2:3" in orders
    assert "[`appdb`.`people`]\n`email` = null" in got, got


# ---- reading the programs' own logs ---------------------------------------

def test_a_timestamp_fraction_is_microseconds_without_leading_zeros():
    """`.95765` came after `.142985`'s second began and means .095765:
    read as a decimal it put a chunk before the one that started it."""
    a = movers._my_ts("2026-09-28 18:13:40.95765")
    b = movers._my_ts("2026-09-28 18:13:40.104997")
    c = movers._my_ts("2026-09-28 18:13:40.142985")
    assert a < b < c
    assert abs((b - a) - 0.009232) < 1e-6
    assert movers._my_ts("no time") is None


def _event(seq, ts, **kw):
    return json.dumps({"seq": seq, "ts": f"2026-09-28 10:00:{ts}",
                       "level": "INFO", **kw})


def test_the_load_log_gives_each_tables_checksum_pair_and_time():
    reader = movers._MyLoadLog()
    lines = [
        _event(1, "01.0", event="restore_data_progress", thread_id="1",
               db="appdb", table="a"),
        _event(2, "01.0", event="restore_data_progress", thread_id="2",
               db="appdb", table="b"),
        _event(3, "03.0", event="restore_data_progress", thread_id="1",
               db="appdb", table="a"),
        _event(4, "04.0", event="checksum_run", status="started"),
        _event(5, "04.1", event="checksum_check", status="finished",
               checksum_type="table_data", table="a",
               expected_checksum="11", actual_checksum="11"),
        _event(6, "04.2", event="checksum_check", status="failed",
               checksum_type="table_data", table="b",
               expected_checksum="22", actual_checksum="23"),
        _event(7, "04.3", event="restore_completed", errors="0"),
    ]
    for line in lines:
        reader(line)
    assert reader.sums == {"a": ("11", "11"), "b": ("22", "23")}
    spans = reader.spans.seconds()
    assert spans["a"] == pytest.approx(3.0) and spans["b"] == \
        pytest.approx(3.0), spans
    assert reader.summary["errors"] == "0"


def test_the_straight_copys_output_is_read_not_shown():
    reader = movers._ShellLog()
    said = [reader(line) for line in (
        "\x1b[34mTGT:\x1b[0m Loading data...\n",
        "NOTE: TGT: LOAD DATA error for `appdb`.`narrow` in file 'x.tsv',"
        " error 1265: Data truncated for column 'd' at row 1\n",
        "ERROR: TGT: Checksum verification failed for: `appdb`.`narrow`"
        " (chunk 0) (boundary: (`id` BETWEEN 1 AND 20000)).\n",
        "TGT: 13334 warnings were reported during the load.\n",
        "  Binlog_file: binlog.000002\n", "  Binlog_position: 8905158\n",
        "  Executed_GTID_set: 9911:1-14\n",
        'MIGKIT {"ok": false, "error": "Shell Error (53031): Checksum'
        ' verification failed"}\n')]
    assert said == [None] * len(said), "a line of the program's reached out"
    assert reader.sums == {"narrow"}
    assert reader.warned == 13334
    assert reader.refused and "1265" in reader.refused[0]
    assert movers._shell_position(reader) == {
        "log_file": "binlog.000002", "log_pos": 8905158,
        "gtid_set": "9911:1-14"}
    assert "Checksum verification failed" in reader.failure()


def test_the_dumps_metadata_counts_each_table(tmp_path):
    (tmp_path / "metadata").write_text(
        "[config]\nquote-character = BACKTICK\n\n"
        "[`appdb`.`narrow`]\nreal_table_name=narrow\nrows = 20000\n"
        "data_checksum = 4066241088\n\n"
        "[`appdb`.`wide`]\nreal_table_name=wide\nrows = 7\n[config]\n")
    assert movers._my_dumped(tmp_path) == {"narrow": 20000, "wide": 7}


# ---- masking on the way -----------------------------------------------------

def test_only_exact_rules_on_columns_outside_keys_are_applied(tmp_path):
    hop = _hop(tmp_path, {"mask_on_write": {
        "people.email": "null", "people.name": "constant anonymous",
        "people.id": "null", "people.note": "random",
        "people.city": "hash", "other.people.x": "null",
        "people.bad": "constant it's"}})
    sections, refused = movers.mask_plan(hop, "appdb", {"people": {"id"}})
    assert sections == {"people": ["`email` = null",
                                   "`name` = constant 'anonymous'"]}, sections
    why = dict(refused)
    assert "part of a key" in why["people.id"]
    assert "random value" in why["people.note"]
    assert "not a masking rule" in why["people.city"]
    assert "without quotes" in why["people.bad"]
    assert "people.x" not in why, "another database's rule was applied here"


def test_a_rule_on_a_column_the_source_lacks_is_refused(tmp_path):
    """A typo in a masking rule hides nothing and would say it did."""
    hop = _hop(tmp_path, {"mask_on_write": {"people.emial": "null",
                                            "people.email": "null"}})
    sections, refused = movers.mask_plan(
        hop, "appdb", {}, {"people": {"id", "email"}})
    assert sections == {"people": ["`email` = null"]}, sections
    assert refused == [("people.emial", "the source has no such column")]


def test_a_hop_that_streams_back_masks_nothing(tmp_path):
    hop = _hop(tmp_path, {"mask_on_write": {"people.email": "null"},
                          "reverse": True})
    sections, refused = movers.mask_plan(hop, "appdb", {})
    assert sections == {} and "streams the target's changes back" in \
        refused[0][1], refused


def test_the_display_mask_masks_nothing_written(tmp_path):
    """`mask` hides values where a person reads them; it is not a rule
    about what the target gets."""
    hop = _hop(tmp_path, {"mask": ["people.email"]})
    assert movers.mask_rules(hop, "appdb") == {}


def test_no_other_path_carries_a_table_the_hop_masks(tmp_path):
    hop = _hop(tmp_path, {"mask_on_write": {"people.email": "null"}})
    for via in ("builtin", "mysqlsh", "pgdump"):
        with pytest.raises(SystemExit) as e:
            movers.refuse_unpushable_filters(hop, "appdb", via, "mysql")
        assert "copy the values as they are" in str(e.value)
    movers.refuse_unpushable_filters(hop, "appdb", "mydumper", "mysql")


def test_a_table_is_left_out_of_the_comparison_only_once_it_was_masked(
        tmp_path):
    """A rule alone never takes a table out of the digest: only the move
    that masked it and held it to the rule does."""
    hop = _hop(tmp_path, {"mask_on_write": {"people.email": "null"}})
    assert movers.masked_tables(hop, "appdb") == set()
    (hop.report_dir("appdb") / "move-began.json").write_text(
        json.dumps({"epoch": time.time()}))
    movers._write_json(movers._masked_record(hop, "appdb"),
                       {"at": time.time() + 1, "tables": ["people"]})
    assert movers.masked_tables(hop, "appdb") == {"appdb.people"}
    # a later move began: what the earlier one masked is not this one's
    (hop.report_dir("appdb") / "move-began.json").write_text(
        json.dumps({"epoch": time.time() + 10}))
    assert movers.masked_tables(hop, "appdb") == set()


def test_every_feature_has_a_state_and_a_reason():
    """The used / unused / topped-up table of backlog 0f, for these
    programs, in the one registry every wrapped tool's account is in; an
    unused feature says why."""
    from migkit import tooluse
    assert not hasattr(movers, "TOOL_USE"), "a second table beside the" \
                                            " registry"
    rows = tooluse.tools_used()
    for tool in ("mydumper", "myloader", "mysqlsh"):
        account = tooluse.REGISTRY[tool].options
        assert len(account) > 8, (tool, account)
        for (command, feature), (state, why) in account.items():
            assert command == "" and state in tooluse.STATES, (tool, feature)
            assert len(why) > 15 and "__" not in why, (tool, feature, why)
        # the licence is the one `doctor --install` installs it under
        from migkit import tools
        assert rows[tool]["licence"] == tools.PROGRAM_LICENCES[tool]
    # every reason and every number kept: the measured ones read as they did
    assert "5.60 s against 4.58 s" in tooluse.entry(
        "mydumper", "", "rows (per table)")[1]
    assert tooluse.entry("myloader", "", "--optimize-keys")[0] == \
        tooluse.UNUSED
    assert tooluse.entry("mysqlsh", "", "util.dump_schemas +"
                         " util.load_dump")[0] == tooluse.TOPPED
    assert "6.19 s against 11.41 s" in tooluse.entry(
        "mysqlsh", "", "util.dump_schemas + util.load_dump")[1]


@needs_dump
def test_every_option_the_table_says_is_used_is_in_a_command_line(tmp_path):
    # a hop that leaves something out, so its pattern is the dump's too
    hop = _hop(tmp_path, exclude=["audit_*"])
    dump, load = movers._mydumper_commands(
        hop, "appdb", 2, tmp_path / "d", tmp_path / "c", tmp_path / "o",
        logs=True, compress=True, regex=movers._mydumper_regex(hop, "appdb"))
    from migkit import tooluse
    for tool, argv in (("mydumper", dump), ("myloader", load)):
        flags = [(f, state) for (_, f), (state, _) in
                 tooluse.REGISTRY[tool].options.items() if f.startswith("--")]
        assert len(flags) > 5, (tool, flags)
        for feature, state in flags:
            if state != tooluse.UNUSED:
                assert feature in argv, (tool, feature)
            else:
                assert feature not in argv, (tool, feature)


# ---- installing the straight copy -------------------------------------------

def test_the_straight_copy_is_fetched_whole_and_linked(tmp_path, monkeypatch):
    """It needs its own libraries beside it: the archive is kept whole and
    the program linked from it."""
    from migkit import tools
    body = io.BytesIO()
    top = "mysql-shell-26.7.1-linux-glibc2.28-x86-64bit"
    with tarfile.open(fileobj=body, mode="w:gz") as tar:
        for name, text in ((f"{top}/bin/mysqlsh",
                            "#!/bin/sh\nd=$(dirname \"$(readlink \"$0\")\")\n"
                            "cat \"$d/../lib/ver\"\n"),
                           (f"{top}/lib/ver", "mysqlsh Ver 26.7.1\n")):
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o755
            tar.addfile(info, io.BytesIO(data))
    served = body.getvalue()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(served)
    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(tools, "VENDOR_URLS", {
            **tools.VENDOR_URLS,
            "mysqlsh": f"http://127.0.0.1:{server.server_port}/"})
        monkeypatch.setattr(tools.platform, "system", lambda: "Linux")
        monkeypatch.setattr(tools.platform, "machine", lambda: "x86_64")
        monkeypatch.setattr(tools.platform, "libc_ver",
                            lambda: ("glibc", "2.35"))
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        said = []
        assert tools.install_vendor("mysqlsh", said.append,
                                    into=str(bin_dir))
        link = bin_dir / "mysqlsh"
        assert link.is_symlink() and os.access(link, os.X_OK)
        kept = pathlib.Path(os.path.realpath(link)).parent.parent
        assert (kept / "lib" / "ver").exists()
        # fetched again, the build before it is replaced, not beside it
        assert tools.install_vendor("mysqlsh", said.append,
                                    into=str(bin_dir))
        assert [p.name for p in kept.parent.iterdir()
                if p.name.startswith("mysqlsh")] == ["mysqlsh-26.7.1"]
    finally:
        server.shutdown()
        server.server_close()
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    low = " ".join(said).lower()
    assert not [t for t in TOOLS + ("mysqlsh", "shell") if t in low], said


def test_the_straight_copy_is_installed_by_default_and_open(monkeypatch):
    """Fetched by `doctor --install` with the rest, without a question:
    its licence is open (GPLv2 with the FOSS exception), so no terms are
    asked for it."""
    from migkit import tools
    assert "mysqlsh" in tools.VENDOR and "mysqlsh" not in tools.TERMS
    assert tools.PROGRAM_LICENCES["mysqlsh"] in tools.OPEN_LICENCES
    assert "mysqlsh" in [p for _, _, needs, optional in tools.CAPABILITIES
                         for p in needs + optional]


def test_the_vendor_publishes_a_build_for_each_machine(monkeypatch):
    from migkit import tools
    monkeypatch.setattr(tools.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(tools.platform, "machine", lambda: "arm64")
    assert tools.vendor_file("mysqlsh") == \
        "mysql-shell-26.7.1-macos15-arm64.tar.gz"
    monkeypatch.setattr(tools.platform, "system", lambda: "Linux")
    monkeypatch.setattr(tools.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(tools.platform, "libc_ver",
                        lambda: ("glibc", "2.35"))
    assert tools.vendor_file("mysqlsh") == \
        "mysql-shell-26.7.1-linux-glibc2.28-x86-64bit.tar.gz"
    monkeypatch.setattr(tools.platform, "libc_ver",
                        lambda: ("glibc", "2.17"))
    assert tools.vendor_file("mysqlsh") is None
