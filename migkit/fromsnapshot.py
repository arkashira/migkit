"""An OpenSearch or Elasticsearch index copied from the source's own
snapshot by the snapshot reader of OpenSearch's migration project
(backlog F7.rfs), beside migkit's own sliced scroll.

The reader takes the index's shards one at a time out of a snapshot the
hop names (`snapshot` on the source endpoint: its `name`, its `repository`
on the source, and where its files are - `local_dir`, or `s3_uri` and
`s3_region`), unpacks each into a directory of this machine, reads its
documents from their stored `_source` and writes them into the target
under their own ids. The source cluster is never searched: a source under
load, or one whose version no reindex can read from, is copied off its
snapshot. The reader runs until it says no work is left (exit 3); a run
that hands its lease on (exit 2) or finished one piece (exit 0) is run
again, and a run that goes again after a stop takes the pieces not done.

What migkit adds around it: the target index made first by the source's
mappings and settings (`OpenSearchEngine._made_like`), replicas and
refreshes off while it loads and back after (`OpenSearchEngine._loading`),
the password in an argument file of this user's only (`@file`, which its
command-line reader expands), the machine's disk held to twice the largest
shard before it starts, only the version conflicts a re-run piece meets
allowed, and the reader's own working index removed from the target when
it is done. The snapshot is as of when it was taken: what the source took
after it is found by the comparison every bulk copy is held to
(`cli._held_to_the_source`), and a hop that follows the source goes on
from the copy's own position.
"""
import os
import re
import shutil
import subprocess
import tempfile

from . import tooluse
from .tooluse import UNUSED, USED

PROGRAM = "reindex-from-snapshot"
#: the index the reader keeps its work in on the target
WORKING = ".migrations_working_state"
#: runs of the reader one index gets at most: each ends with a piece done
#: or a lease handed on, and one index has as many pieces as shards
MOST_RUNS = 10_000
#: what the reader's exit codes mean
DONE, AGAIN, NO_WORK = 0, 2, 3
#: the flags migkit passes it, which the build here must take (`prove`)
PASSED = ("--snapshot-name", "--snapshot-local-dir", "--s3-repo-uri",
          "--s3-region", "--s3-local-dir", "--lucene-dir", "--target-host",
          "--target-username", "--target-password", "--target-insecure",
          "--index-allowlist", "--source-version", "--max-connections",
          "--max-shard-size-bytes", "--allowed-doc-exception-types")


def declared(hop):
    """The snapshot the hop names on its source endpoint, or {}."""
    got = (hop.source.options or {}).get("snapshot") or {}
    return dict(got) if isinstance(got, dict) else {}


def program():
    from .tools import built_program
    return built_program(PROGRAM)


_FLAGS = {}


def flags_here():
    """The flags the reader built here takes, read off its own usage once
    a build (a Java program's start costs seconds), or None where it is
    not built."""
    got = program()
    if got is None:
        return None
    argv, env = got
    try:
        stamp = os.stat(argv[0]).st_mtime
    except OSError:
        return None
    if (argv[0], stamp) not in _FLAGS:
        try:
            out = subprocess.run(argv + ["--help"], capture_output=True,
                                 text=True, timeout=60,
                                 env={**os.environ, **env})
        except (OSError, subprocess.TimeoutExpired):
            return None
        _FLAGS[(argv[0], stamp)] = frozenset(re.findall(
            r"--[a-z][a-z0-9-]+", out.stdout + out.stderr))
    return _FLAGS[(argv[0], stamp)]


def prove(facts):
    """Why the reader built here cannot be trusted, or None: a flag migkit
    passes that its usage does not list."""
    have = flags_here()
    if have is None:
        return "the snapshot reader would not say what it takes"
    gone = [f for f in PASSED if f not in have]
    return (f"the build here does not take {', '.join(gone)}" if gone
            else None)


def source_version(eng):
    """The source as the reader names it: `OS_2_19`, `ES_7_10`."""
    got = eng._call("src", "GET", "/") or {}
    v = got.get("version") or {}
    parts = (str(v.get("number") or "0.0").split(".") + ["0"])[:2]
    kind = "OS" if v.get("distribution") == "opensearch" else "ES"
    return f"{kind}_{parts[0]}_{parts[1]}"


def _largest_shard(eng, index):
    got = eng._call("src", "GET", f"/_cat/shards/{index}?format=json"
                                  "&bytes=b&h=prirep,store") or []
    return max([int(r.get("store") or 0) for r in got
                if r.get("prirep") == "p"] or [0])


def unfit(eng, db, table):
    """Why this index is not copied from the snapshot, or None: none
    named, its files nowhere given, it not in the snapshot or the snapshot
    not complete (asked of the source's repository, a read), the index
    renamed on the target, `_source` not kept, or this machine's disk short
    of twice its largest shard."""
    snap = declared(eng.hop)
    if not snap.get("name"):
        return "the hop names no snapshot of the source"
    if not (snap.get("local_dir") or snap.get("s3_uri")):
        return ("the hop's snapshot gives neither local_dir nor s3_uri, so"
                " its files cannot be read")
    src, dst = eng._index("src", db, table), eng._index("dst", db, table)
    if src != dst:
        return (f"it is {dst} on the target, and the snapshot reader writes"
                " an index under its own name")
    repo = snap.get("repository")
    if repo:
        got = eng._call("src", "GET", f"/_snapshot/{repo}/{snap['name']}",
                        missing_ok=True) or {}
        found = (got.get("snapshots") or [None])[0]
        if not found:
            return f"the snapshot {snap['name']} is not in {repo}"
        if found.get("state") != "SUCCESS":
            return (f"the snapshot {snap['name']} is"
                    f" {str(found.get('state')).lower()}, not complete")
        if src not in (found.get("indices") or []):
            return f"the snapshot {snap['name']} does not hold {src}"
    mapping = eng._call("src", "GET", f"/{src}/_mapping") or {}
    kept = ((mapping.get(src) or {}).get("mappings") or {}).get(
        "_source") or {}
    if kept.get("enabled") is False:
        return ("its documents keep no _source, which is all the snapshot"
                " reader reads")
    need = 2 * _largest_shard(eng, src)
    free = shutil.disk_usage(tempfile.gettempdir()).free
    if need > free:
        return (f"its largest shard needs {need:,} bytes of this machine's"
                f" disk to unpack and {free:,} are free")
    return None


def args(eng, db, table, workers, lucene, s3_local):
    """The reader's arguments for one index, the password among them - an
    argument file's lines, never a command line."""
    snap = declared(eng.hop)
    ep = eng.hop.target
    src = eng._index("src", db, table)
    out = ["--snapshot-name", snap["name"], "--lucene-dir", lucene]
    if snap.get("local_dir"):
        out += ["--snapshot-local-dir", snap["local_dir"]]
    else:
        out += ["--s3-repo-uri", snap["s3_uri"], "--s3-region",
                snap.get("s3_region") or "us-east-1",
                "--s3-local-dir", s3_local]
    out += ["--target-host", eng._base("dst"),
            "--index-allowlist", src,
            "--source-version", source_version(eng),
            "--max-connections", str(max(1, int(workers))),
            "--max-shard-size-bytes",
            str(max(1, int(_largest_shard(eng, src) * 1.1))),
            "--allowed-doc-exception-types",
            "version_conflict_engine_exception"]
    if ep.user:
        out += ["--target-username", ep.user, "--target-password",
                ep.password or ""]
    if (ep.options or {}).get("verify") is False:
        out.append("--target-insecure")
    return out


def copy_index(eng, db, table, workers, log):
    """One index out of the snapshot into the target, the reader run until
    it has no work left. Returns the documents the target then holds;
    raises where the reader fails."""
    from . import movers
    from .util import PrivateFile
    from .wording import without_programs
    got = program()
    if got is None:
        raise SystemExit("this way needs a component this machine does not"
                         " have - migkit doctor --install puts it in place")
    argv, env = got
    lucene = tempfile.mkdtemp(prefix="migkit-shards-")
    s3_local = tempfile.mkdtemp(prefix="migkit-snapshot-")
    for d in (lucene, s3_local):
        os.chmod(d, 0o700)
    try:
        text = "\n".join(args(eng, db, table, workers, lucene, s3_local))
        with PrivateFile(text + "\n", ".args") as path:
            cmd = argv + [f"@{path}"]
            for _ in range(MOST_RUNS):
                movers._debug(cmd)
                with movers._spawned(subprocess.Popen(
                        cmd, env={**os.environ, **env}, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT),
                        cmd[0]) as p:
                    out = p.communicate()[0]
                if p.returncode == NO_WORK:
                    break
                if p.returncode not in (DONE, AGAIN):
                    raise RuntimeError(
                        f"{table}: the snapshot reader stopped: "
                        + without_programs(out[-400:], movers.DRIVEN))
            else:
                raise RuntimeError(f"{table}: the snapshot reader still had"
                                   f" work after {MOST_RUNS} runs")
    finally:
        shutil.rmtree(lucene, ignore_errors=True)
        shutil.rmtree(s3_local, ignore_errors=True)
        eng._call("dst", "DELETE", f"/{WORKING}", missing_ok=True)
    index = eng._index("dst", db, table)
    eng._call("dst", "POST", f"/{index}/_refresh")
    return int((eng._call("dst", "GET", f"/{index}/_count") or {})
               .get("count", 0))


# ---- the account of what is used (backlog 0f) ----------------------------

#: the pinned source's documented flags (3.3.8,
#: DocumentsFromSnapshotMigration's README), for where it is not built
SURFACE = {"": frozenset(PASSED + (
    "--initial-lease-duration", "--documents-per-bulk-request",
    "--documents-size-per-bulk-request", "--otel-collector-endpoint",
    "--doc-transformer-config", "--doc-transformer-config-file",
    "--target-aws-region", "--target-aws-service-signing-name",
    "--target-cacert", "--coordinator-host", "--max-initial-lease-duration",
    "--source-supports-snapshot-restore"))}


def _installed():
    have = flags_here()
    return None if have is None else {
        "": frozenset(have) - {"--help", "--version"}}


def _account():
    o = {}

    def put(option, state, why):
        o[("", option)] = (state, why)
    put("--snapshot-name", USED, "the snapshot the hop names")
    put("--snapshot-local-dir", USED, "the snapshot's files where the hop"
        " gives them as a directory of this machine")
    put("--s3-repo-uri", USED, "the snapshot's files where the hop gives"
        " them in object storage")
    put("--s3-region", USED, "the object storage's region the hop gives")
    put("--s3-local-dir", USED, "a directory of this user's only, removed"
        " when the index is done: the snapshot's files hold documents")
    put("--lucene-dir", USED, "a directory of this user's only, removed"
        " when the index is done, on a disk held to twice the largest shard")
    put("--target-host", USED, "the target's address from the hop")
    put("--target-username", USED, "the target's user from the hop")
    put("--target-password", USED, "in an argument file of this user's"
        " only, removed when the reader ends")
    put("--target-insecure", USED, "only where the hop says the target's"
        " certificate is not checked")
    put("--index-allowlist", USED, "one index at a time, the way climbed"
        " for it")
    put("--source-version", USED, "asked of the source itself")
    put("--max-connections", USED, "from migkit's sizing of this machine"
        " and both clusters, never set by hand")
    put("--max-shard-size-bytes", USED, "the index's largest shard, read"
        " from the source, with a tenth more")
    put("--allowed-doc-exception-types", USED, "version conflicts only:"
        " what a piece run again after a handed-on lease meets, and nothing"
        " else passed over")
    put("--initial-lease-duration", UNUSED, "its default ten minutes: a"
        " piece that runs longer is handed on and the reader run again")
    put("--documents-per-bulk-request", UNUSED, "its defaults: the target's"
        " refusals under load are the reader's own retries")
    put("--documents-size-per-bulk-request", UNUSED, "its defaults: the"
        " target's refusals under load are the reader's own retries")
    put("--otel-collector-endpoint", UNUSED, "its metrics are not read:"
        " the move counts what arrived and compares it with the source")
    for f in ("--doc-transformer-config", "--doc-transformer-config-file"):
        put(f, UNUSED, "documents are copied as they are; a document"
            " changed on the way would not match the source")
    for f in ("--target-aws-region", "--target-aws-service-signing-name"):
        put(f, UNUSED, "not yet: a target signed by the cloud's own"
            " credentials is copied by migkit's scroll")
    put("--target-cacert", UNUSED, "not yet: a target checked against an"
        " authority of the hop's is copied by migkit's scroll")
    put("--coordinator-host", UNUSED, "the target keeps the reader's"
        " working index, removed when it is done")
    put("--max-initial-lease-duration", UNUSED, "its default: leases are"
        " the reader's own business")
    put("--source-supports-snapshot-restore", UNUSED, "not a question the"
        " copy asks: the snapshot is read by the reader itself")
    return o


tooluse.register(PROGRAM, options=_account(), declared=SURFACE,
                 installed=_installed)
