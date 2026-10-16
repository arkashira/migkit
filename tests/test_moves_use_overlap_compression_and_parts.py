"""Copies, relay legs and ship use the phase schedule, per-leg compression
and file parts: Azure and Cloud Storage move as parallel ranged parts, a
copy inside an object store is held to the source's own checksum, a file
here is timed on the way up by scratch objects, and the planner runs a
mesh and a fan-out in decided order. Fakes only; no server."""
import base64
import hashlib
import itertools
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from migkit import carriers, legs, planner, scenarios, topology
from migkit.config import Endpoint
from migkit.guarantees import Semantics

DATA = bytes(itertools.islice(itertools.cycle(range(251)), 40 * 1024 + 3))
LINK = carriers.Link(rate=4e5, per_stream=1e5, rtt=0.001)
TINY = carriers.Link(rate=4e5, per_stream=1e3, rtt=0.001)


def test_the_crc_of_parts_joins_to_the_crc_of_the_whole():
    assert carriers.crc32c(b"123456789") == 0xE3069283
    cuts = [0, 7, 1000, 1001, len(DATA)]
    seen = carriers.Streamed(crc=True)
    for i, (a, b) in enumerate(zip(cuts, cuts[1:])):
        seen.saw(i, DATA[a:b])
    assert seen.crc32c() == carriers.crc32c(DATA)
    seen.crcs[2] ^= 1
    assert seen.crc32c() != carriers.crc32c(DATA)


# ---- Azure -----------------------------------------------------------------

class _Blob:
    def __init__(self, box, key):
        self.box, self.key = box, key

    def stage_block(self, block_id, data, length=None, validate_content=False):
        assert validate_content and length == len(data)
        self.box.staged[(self.key, block_id)] = (
            data + b"!" if self.box.corrupt else data)

    def commit_block_list(self, blocks):
        ids = [b.id for b in blocks]
        self.box.blocks[self.key] = [self.box.staged[(self.key, i)]
                                     for i in ids]

    def get_block_list(self, kind):
        assert kind == "committed"
        return ([SimpleNamespace(size=len(b))
                 for b in self.box.blocks[self.key]], [])

    def get_blob_properties(self):
        return SimpleNamespace(
            size=sum(map(len, self.box.blocks[self.key])))


class _Box:
    def __init__(self, corrupt=False):
        self.staged, self.blocks, self.corrupt = {}, {}, corrupt
        self.deleted, self.reads = [], []

    def get_blob_client(self, key):
        return _Blob(self, key)

    def delete_blob(self, key):
        self.deleted.append(key)
        self.blocks.pop(key, None)

    def download_blob(self, key, offset=0, length=None):
        self.reads.append((offset, length))
        whole = b"".join(self.blocks[key])
        return SimpleNamespace(readall=lambda: whole[offset:offset + length])


def _azure(box):
    s = carriers.AzureBlobStore("https://acct.example.com", "c", "p")
    s._client = box
    return s


@pytest.fixture
def small_parts(monkeypatch):
    monkeypatch.setattr(carriers, "S3_PART_LEAST", 1024)


def test_a_file_moves_to_a_blob_as_parallel_blocks_and_back(tmp_path,
                                                            small_parts):
    (tmp_path / "f.bin").write_bytes(DATA)
    box = _Box()
    dst = _azure(box)
    got = carriers.move_file(carriers.LocalStore(tmp_path), "f.bin", dst,
                             link=LINK)
    assert got.way == "parallel ranged parts"
    assert got.plan.parts > 1 and got.plan.streams > 1
    assert b"".join(box.blocks[dst._key("f.bin")]) == DATA
    assert len(box.blocks[dst._key("f.bin")]) == got.plan.parts
    back = tmp_path / "back"
    again = carriers.move_file(dst, "f.bin", carriers.LocalStore(back),
                               link=LINK)
    assert again.way == "parallel ranged parts"
    assert (back / "f.bin").read_bytes() == DATA
    assert len(box.reads) == again.plan.parts


def test_a_blob_that_holds_other_blocks_is_taken_away(tmp_path, small_parts):
    (tmp_path / "f.bin").write_bytes(DATA)
    box = _Box(corrupt=True)
    dst = _azure(box)
    box_sizes = box.blocks
    with pytest.raises(carriers.BadCopy):
        carriers.move_file(carriers.LocalStore(tmp_path), "f.bin", dst,
                           link=LINK)
    assert box.deleted == [dst._key("f.bin")] and not box_sizes


# ---- Cloud Storage ----------------------------------------------------------

class _GBlob:
    def __init__(self, bucket, key):
        self.bucket, self.key = bucket, key

    def upload_from_string(self, data, checksum=None):
        assert checksum == "md5"
        self.bucket.objects[self.key] = bytes(data)

    def compose(self, sources):
        assert len(sources) <= carriers.GCS_COMPOSE
        self.bucket.composes.append(len(sources))
        self.bucket.objects[self.key] = b"".join(
            self.bucket.objects[s.key] for s in sources)

    def reload(self):
        data = self.bucket.objects[self.key]
        if self.bucket.flip:
            data = data[::-1]
        self.size = len(data)
        self.crc32c = base64.b64encode(
            carriers.crc32c(data).to_bytes(4, "big")).decode()

    def delete(self):
        self.bucket.objects.pop(self.key, None)

    def download_as_bytes(self, start=None, end=None):
        return self.bucket.objects[self.key][start:end + 1]


class _Bucket:
    def __init__(self, flip=False):
        self.objects, self.composes, self.flip = {}, [], flip

    def blob(self, key):
        return _GBlob(self, key)


def _gcs(bucket):
    s = carriers.GcsStore("b", "p")
    s._bucket = bucket
    return s


def test_cloud_storage_parts_are_composed_in_tiers_and_checked(tmp_path,
                                                              small_parts):
    (tmp_path / "f.bin").write_bytes(DATA)
    bucket = _Bucket()
    dst = _gcs(bucket)
    got = carriers.move_file(carriers.LocalStore(tmp_path), "f.bin", dst,
                             link=TINY)
    assert got.plan.parts > carriers.GCS_COMPOSE
    assert bucket.objects[dst._key("f.bin")] == DATA
    assert max(bucket.composes) <= carriers.GCS_COMPOSE
    assert len(bucket.composes) > 1
    assert list(bucket.objects) == [dst._key("f.bin")]
    assert "CRC-32C" in got.line()


def test_a_composed_object_that_is_not_the_parts_is_taken_away(tmp_path,
                                                              small_parts):
    (tmp_path / "f.bin").write_bytes(DATA)
    bucket = _Bucket(flip=True)
    with pytest.raises(carriers.BadCopy):
        carriers.move_file(carriers.LocalStore(tmp_path), "f.bin",
                           _gcs(bucket), link=TINY)
    assert bucket.objects == {}


# ---- inside one store: held to the source's own checksum --------------------

class _S3:
    def __init__(self, head, wrong=False):
        self.objects, self.uploads, self.calls = {}, {}, []
        self.head, self.wrong, self.extra = head, wrong, {}

    def head_object(self, Bucket, Key, ChecksumMode=None):
        if Key in self.copied:
            return self.copied[Key]
        return dict(self.head, ContentLength=len(self.objects[Key]))

    copied = {}

    def copy_object(self, Bucket, Key, CopySource):
        self.calls.append("copy_object")
        self.objects[Key] = self.objects[CopySource["Key"]]
        etag = self.head["ETag"] if not self.wrong else '"0"'
        self.copied = {Key: {"ETag": etag,
                             "ContentLength": len(self.objects[Key])}}

    def create_multipart_upload(self, Bucket, Key, **kw):
        self.calls.append("create")
        self.extra = kw
        self.uploads["u"] = {}
        return {"UploadId": "u"}

    def upload_part_copy(self, Bucket, Key, UploadId, PartNumber, CopySource,
                         CopySourceRange):
        a, b = map(int, re.match(r"bytes=(\d+)-(\d+)",
                                 CopySourceRange).groups())
        body = self.objects[CopySource["Key"]][a:b + 1]
        self.uploads[UploadId][PartNumber] = body
        return {"CopyPartResult": {
            "ETag": f'"{hashlib.md5(body).hexdigest()}"'}}

    def complete_multipart_upload(self, Bucket, Key, UploadId,
                                  MultipartUpload):
        parts = self.uploads.pop(UploadId)
        order = sorted(p["PartNumber"] for p in MultipartUpload["Parts"])
        self.objects[Key] = b"".join(parts[n] for n in order)
        md5s = b"".join(hashlib.md5(parts[n]).digest() for n in order)
        etag = f'"{hashlib.md5(md5s).hexdigest()}-{len(order)}"'
        got = {"ETag": etag, "ContentLength": len(self.objects[Key])}
        for field in ("ChecksumCRC64NVME", "ChecksumCRC32C", "ChecksumCRC32"):
            if self.extra.get("ChecksumAlgorithm") == field[8:]:
                got[field] = self.head[field] if not self.wrong else "other"
        self.copied = {Key: got}

    def abort_multipart_upload(self, **kw):
        self.calls.append("abort")

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.copied = {k: v for k, v in self.copied.items() if k != Key}


def _store(client, bucket):
    s = carriers.S3Store(bucket, "p", endpoint="http://store")
    s._client = client
    return s


def _copy(head, wrong=False, size=len(DATA)):
    client = _S3(head, wrong)
    src, dst = _store(client, "a"), _store(client, "b")
    client.objects[src._key("big")] = DATA[:size]
    got = carriers.move_file(src, "big", dst, "copy", link=LINK)
    return client, dst, got


def test_a_copy_is_held_to_the_sources_whole_object_checksum(small_parts):
    head = {"ETag": '"x-3"', "ChecksumType": "FULL_OBJECT",
            "ChecksumCRC64NVME": "AAAA"}
    client, dst, got = _copy(head)
    assert client.extra == {"ChecksumAlgorithm": "CRC64NVME",
                            "ChecksumType": "FULL_OBJECT"}
    assert "CRC64NVME equal to the source object's own" in got.line()
    assert client.objects[dst._key("copy")] == DATA


def test_a_copy_whose_checksum_is_not_the_sources_is_taken_away(small_parts):
    head = {"ETag": '"x-3"', "ChecksumType": "FULL_OBJECT",
            "ChecksumCRC64NVME": "AAAA"}
    with pytest.raises(carriers.BadCopy):
        _copy(head, wrong=True)


def test_an_object_written_whole_is_copied_whole_and_its_digest_compared():
    head = {"ETag": f'"{hashlib.md5(DATA[:100]).hexdigest()}"'}
    client, dst, got = _copy(head, size=100)
    assert client.calls == ["copy_object"]
    assert "digest equal to the source object's own" in got.line()
    with pytest.raises(carriers.BadCopy):
        _copy(head, wrong=True, size=100)


def test_a_source_that_gives_nothing_is_said_so(small_parts):
    client, dst, got = _copy({"ETag": '"x-3"'})
    assert client.extra == {}
    assert "no checksum of its own" in got.line()


# ---- a file here, timed on the way up ---------------------------------------

def test_a_file_going_up_is_timed_by_writing_and_the_scratch_is_removed(
        tmp_path, small_parts):
    (tmp_path / "f.bin").write_bytes(DATA)
    box = _Box()
    dst = _azure(box)
    put, gone = [], []
    dst.put_bytes = lambda name, data: put.append(name)
    dst.delete = lambda name: gone.append(name) or box.blocks.pop(name, None)
    tick = itertools.count()
    got = carriers.move_file(carriers.LocalStore(tmp_path), "f.bin", dst,
                             clock=lambda: next(tick) * 0.01)
    assert put and sorted(put) == sorted(gone[:len(put)])
    assert all("migkit-probe" in n for n in put)
    assert dst.link is not None and "writing" in dst.link.whence
    assert got.way == "parallel ranged parts"
    assert b"".join(box.blocks[dst._key("f.bin")]) == DATA


def test_two_directories_here_are_no_link_and_the_file_is_copied_whole(
        tmp_path):
    (tmp_path / "f.bin").write_bytes(DATA)
    got = carriers.move_file(carriers.LocalStore(tmp_path), "f.bin",
                             carriers.LocalStore(tmp_path / "out"))
    assert got.way == "one stream"
    assert (tmp_path / "out" / "f.bin").read_bytes() == DATA


# ---- the planner and the relay ----------------------------------------------

def _model(rate=100.0, little=2, carried=0):
    bound = lambda v: SimpleNamespace(bound=lambda: v)
    return SimpleNamespace(measured=True, little=little, carried=carried,
                           rate=lambda n: rate,
                           stages={"target": bound(50.0), "source": bound(200.0)})


def test_scheduled_lays_the_move_out_from_the_models_rates():
    facts = {"a": {"bytes": 1000, "index_bytes": 100, "rows": 10},
             "b": {"bytes": 400, "rows": 4}}
    sem = {t: Semantics(table=t) for t in facts}
    got = planner.scheduled(_model(), facts, sem, shared=True)
    at = {t: (s, e) for t, s, e in got.copies}
    assert at["a"] == (0.0, 20.0) and at["b"] == (0.0, 8.0)
    assert [(t, s, e) for t, s, e in got.builds] == [("a", 20.0, 22.0)]
    assert got.makespan < got.serial
    assert planner.scheduled(None, facts, sem) is None
    assert planner.scheduled(_model(rate=0.0), facts, sem) is None


def _pack(clock):
    packed = {}

    def pack(d, k):
        clock[0] += 0.001 * k
        out = d[:max(1, len(d) // (k + 1))] + bytes([k])
        packed[out] = d
        return out

    def unpack(d):
        clock[0] += 0.001
        return packed[d]
    return (pack, unpack, tuple(range(1, 22))), lambda: clock[0]


def test_the_relay_compresses_a_slow_link_and_sends_a_fast_one_plain(
        monkeypatch):
    sample = bytes(range(256)) * 400
    hop = SimpleNamespace(name="h")
    monkeypatch.setattr(planner, "link_speed", lambda h: None)
    assert legs.relay_compression(hop, sample, 100) == {}
    out = {}
    for speed in (0.5, 1e9):
        monkeypatch.setattr(planner, "link_speed", lambda h, v=speed: v)
        clock = [0.0]
        codec, now = _pack(clock)
        got = legs.relay_compression(hop, sample, 100, codec, now,
                                     near_cores=4)
        out[speed] = got["relay to here"].level
    assert out[0.5] is not None
    assert out[1e9] is None


# ---- topology: a mesh and a fan-out ------------------------------------------

def _hop(name, src, dst):
    return SimpleNamespace(name=name, source=Endpoint(host=src, port=5432),
                           target=Endpoint(host=dst, port=5432))


TABLES = {"orders": {"key": ["id"], "columns": ["id", "v"]},
          "items": {"key": ["id"], "columns": ["id", "v"]}}


def test_a_fan_out_runs_the_slowest_hop_first_and_proves_each_part():
    hops = [_hop(f"h{i}", "src", f"t{i}") for i in range(3)]
    rates = {"h0": 1000.0, "h1": 10.0, "h2": 100.0}
    route = {"by": "table", "tables": {"orders": "t0:5432",
                                       "items": "t2:5432"}}
    facts, lay = topology.facts_of(hops, TABLES, route=route,
                                   speed=lambda h: rates[h.name])
    assert lay.kind == "one-to-many"
    whole = {"orders": {("1",): "a"}, "items": {("2",): "b"}}
    rows = {"source": whole, "targets": {}}
    ran = []

    def run_hop(h, spec):
        ran.append(h.name)
        for t, keys in whole.items():
            if route["tables"][t] == h.target.host + ":5432":
                rows["targets"].setdefault(h.target.host + ":5432", {})[t] = keys
    for h in hops:
        rows["targets"][h.target.host + ":5432"] = {}
    out = planner.run_topology(hops, facts, lay, run_hop, lambda: rows,
                               rows_by_hop={"h0": 1000, "h1": 1000,
                                            "h2": 1000})
    assert ran[0] == "h1" and ran[-1] == "h0"
    assert out.order == tuple(ran) and set(ran) == {"h0", "h1", "h2"}
    assert out.ok and len(out.done) == 3


def test_a_fan_out_hop_that_puts_a_row_in_the_wrong_place_stops_the_run():
    hops = [_hop(f"h{i}", "src", f"t{i}") for i in range(3)]
    route = {"by": "table", "tables": {"orders": "t0:5432",
                                       "items": "t2:5432"}}
    facts, lay = topology.facts_of(hops, TABLES, route=route,
                                   speed=lambda h: None)
    whole = {"orders": {("1",): "a"}, "items": {("2",): "b"}}
    rows = {"source": whole,
            "targets": {f"t{i}:5432": {} for i in range(3)}}
    ran = []

    def run_hop(h, spec):
        ran.append(h.name)
        rows["targets"][h.target.host + ":5432"] = {"orders": whole["orders"]}
    out = planner.run_topology(hops, facts, lay, run_hop, lambda: rows)
    assert not out.ok and out.stopped and out.final is None
    assert len(ran) < 3 or out.stopped[0] == ran[-1]


NODES = ("n1", "n2", "n3")


def _mesh_hops():
    return [_hop(f"{a}-{b}", a, b) for a, b in topology.mesh_links(NODES)]


def _mesh_facts(hops, rates=None):
    spaces = {f"{n}:5432": {t: {"stride": [i, 3]} for t in TABLES}
              for i, n in enumerate(NODES)}
    marks = {f"{n}:5432": "origin" for n in NODES}
    return topology.facts_of(hops, TABLES, spaces=spaces, marks=marks,
                             speed=lambda h: (rates or {}).get(h.name))


def test_a_mesh_runs_every_link_and_proves_convergence_only_at_the_end():
    hops = _mesh_hops()
    facts, lay = _mesh_facts(hops, {"n1-n2": 5.0})
    assert lay.kind == "mesh" and lay.hops == 6
    rows = {n: {"orders": {("1",): "a"}} for n in NODES}
    state = {"nodes": {f"{n}:5432": rows[n] for n in NODES}, "applied": []}
    ran = []
    out = planner.run_topology(hops, facts, lay,
                               lambda h, s: ran.append(h.name),
                               lambda: state)
    assert out.ok and sorted(ran) == sorted(h.name for h in hops)
    assert ran[0] == "n1-n2"
    assert any(x.startswith("the whole") for x in out.lines)
    assert sum("no change came round" in x for x in out.lines) == 5


def test_a_change_that_comes_round_stops_a_mesh_before_the_next_link():
    hops = _mesh_hops()
    facts, lay = _mesh_facts(hops)
    state = {"nodes": {f"{n}:5432": {} for n in NODES},
             "applied": [("n1:5432", "n2:5432", "n1:5432", 7)]}
    ran = []
    out = planner.run_topology(hops, facts, lay,
                               lambda h, s: ran.append(h.name),
                               lambda: state)
    assert not out.ok and len(ran) == 1 and out.stopped[0] == ran[0]
    assert "came round" in " ".join(out.lines)
