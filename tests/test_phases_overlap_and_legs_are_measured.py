"""Phases overlapped where the foreign keys allow, compression chosen
from a measured sample against the link, and files moved as parallel
ranged parts hashed while they stream (backlog R20.5, R20.6, R20.7).
Fake clocks and a fake object store; no server."""
import base64
import hashlib
import itertools
import re

import pytest

from migkit import carriers, legs, movers, planner
from migkit.guarantees import Semantics


# ---- R20.5 -----------------------------------------------------------------

def _tied():
    return {"parent": Semantics(table="parent"),
            "child": Semantics(table="child", parents=("parent",)),
            "other": Semantics(table="other")}


def test_a_child_waits_for_its_parent_and_the_free_overlap_is_refused():
    got = planner.overlap(_tied(), {"child": 10, "parent": 4, "other": 2},
                          build={"parent": 3, "other": 1}, at_once=2,
                          shared=True)
    assert got.way == "waves"
    at = {t: (s, e) for t, s, e in got.copies}
    assert at["child"][0] >= at["parent"][1]
    assert any(w.startswith("overlapped, every table") and "child" in why
               for w, why in got.refused)
    built = {t: s for t, s, _ in got.builds}
    assert built["parent"] == at["parent"][1]
    assert built["parent"] < max(e for _, e in at.values())
    assert got.makespan < got.serial
    assert any("start while other tables still copy" in x
               for x in got.lines())


def test_without_keys_between_them_tables_overlap_freely():
    sem = {t: Semantics(table=t) for t in ("a", "b")}
    got = planner.overlap(sem, {"a": 6, "b": 3}, at_once=2, shared=True)
    assert got.way == "free"
    assert {s for _, s, _ in got.copies} == {0.0}
    assert got.refused == ()


def test_the_tail_starts_once_the_first_table_is_in_only_when_followed():
    sem = {t: Semantics(table=t) for t in ("a", "b")}
    on = planner.overlap(sem, {"a": 6, "b": 3}, at_once=1, shared=True,
                         followed=True)
    off = planner.overlap(sem, {"a": 6, "b": 3}, at_once=1, shared=True)
    assert on.tail == 6.0
    assert off.tail == 9.0
    assert any("once the first table is in" in x for x in on.lines())


def test_a_range_is_checked_while_the_next_one_loads():
    sem = {"a": Semantics(table="a")}
    got = planner.overlap(sem, {"a": 8}, check={"a": 4}, ranges={"a": 4},
                          shared=True)
    assert len(got.checks) == 4
    assert got.checks[0][1] == pytest.approx(2.0)
    assert got.checks[0][1] < 8.0
    assert got.makespan == pytest.approx(9.0)
    assert got.serial == pytest.approx(12.0)


def test_no_way_where_every_schedule_loses_what_the_move_keeps():
    got = planner.overlap(_tied(), {"child": 1, "parent": 1, "other": 1},
                          fks="after", shared=False, followed=False)
    assert got.way == ""
    assert len(got.refused) == 3
    assert all("one moment" in why for _, why in got.refused)


# ---- R20.6 -----------------------------------------------------------------

class _Codec:
    """Level k keeps 1/(k+1) of the bytes and takes k ms a sample to
    pack, 1 ms to unpack; `broken` levels unpack wrong."""

    def __init__(self, broken=()):
        self.now, self.packed, self.broken = 0.0, {}, set(broken)
        self.tried = []

    def clock(self):
        return self.now

    def codec(self):
        def pack(d, k):
            self.tried.append(k)
            self.now += 0.001 * k
            out = d[:max(1, len(d) // (k + 1))] + bytes([k])
            self.packed[out] = (d, k)
            return out

        def unpack(d):
            self.now += 0.001
            orig, k = self.packed[d]
            return orig[::-1] if k in self.broken else orig
        return pack, unpack, tuple(range(1, 22))


SAMPLE = bytes(range(256)) * 400


def test_three_levels_across_the_range_are_probed_and_proved():
    c = _Codec(broken={8})
    got = movers.probe_compression(SAMPLE, c.codec(), c.clock)
    assert c.tried == [1, 8, 15]
    assert [s.level for s in got] == [1, 15]
    assert got[0].ratio == pytest.approx(0.5, abs=0.01)


def test_the_probe_stops_where_a_level_is_slower_than_the_link():
    c = _Codec()
    got = movers.probe_compression(SAMPLE, c.codec(), c.clock,
                                   link=1e12, cores=1)
    assert c.tried == [1]
    assert len(got) == 1


def test_a_fast_link_sends_plain_and_a_slow_one_compresses_harder():
    c = _Codec()
    probed = movers.probe_compression(SAMPLE, c.codec(), c.clock)
    fast = movers.Compressing(movers.Leg("to the target", 1e12, 4, 4),
                              probed)
    slow = movers.Compressing(movers.Leg("to the relay", 1e5, 4, 4),
                              probed)
    idle = movers.Compressing(movers.Leg("busy", 1e5, 0, 4), probed)
    assert fast.level is None and fast.binds == "the link"
    assert slow.level == 15
    assert idle.level is None
    assert any("no core free" in p for p in idle.passed)
    assert "compressed at level 15" in slow.line()


def test_the_level_is_chosen_again_at_a_boundary_only_when_it_pays():
    c = _Codec()
    probed = movers.probe_compression(SAMPLE, c.codec(), c.clock)
    got = movers.Compressing(movers.Leg("leg", 1e12, 4, 4), probed)
    assert got.boundary(link=1.02e12) is None
    assert got.changes == []
    assert got.boundary(link=1e5) == 15
    assert got.changes and "sent as it is -> compressed at level 15" \
        in got.changes[0]
    assert got.as_evidence()["level"] == 15


# ---- R20.7 -----------------------------------------------------------------

def test_the_part_plan_comes_from_the_link():
    link = carriers.Link(rate=400e6, per_stream=50e6, rtt=0.02)
    got = carriers.part_plan(10 * 2 ** 30, link)
    assert got.streams == 8
    assert got.part == int(50e6 * 0.02 / carriers.TRIP_SHARE)
    slow = carriers.part_plan(10 * 2 ** 30, carriers.Link(1e6, 1e6, 0.001))
    assert slow.part == carriers.S3_PART_LEAST and slow.streams == 1
    huge = carriers.part_plan(4 * 2 ** 40, carriers.Link(1e6, 1e6, 0.001))
    assert huge.parts <= carriers.S3_PARTS
    tiny = carriers.part_plan(100, link)
    assert tiny.parts == 1 and tiny.streams == 1


def test_streams_are_added_while_the_rate_rises():
    times = iter([0, 0.01, 0, 1.0, 0, 1.0, 0, 1.5, 0, 6.0])
    size = 64 * carriers.S3_PART_LEAST
    got = carriers.measure_link(lambda s, e: b"x" * (e - s), size,
                                clock=lambda: next(times))
    assert got.rtt == pytest.approx(0.01)
    assert got.per_stream == pytest.approx(carriers.S3_PART_LEAST)
    assert got.streams == 4


class _FakeS3:
    def __init__(self, corrupt=None):
        self.objects, self.uploads, self.calls = {}, {}, []
        self.corrupt = corrupt
        self.aborted = []

    def _md5(self, b):
        return hashlib.md5(b).hexdigest()

    def create_multipart_upload(self, Bucket, Key):
        up = f"u{len(self.uploads)}"
        self.uploads[up] = {}
        return {"UploadId": up}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body,
                    ContentMD5):
        self.calls.append("upload_part")
        assert base64.b64decode(ContentMD5).hex() == self._md5(Body)
        if self.corrupt == PartNumber:
            Body = Body[::-1] + b"!"
        self.uploads[UploadId][PartNumber] = Body
        return {"ETag": f'"{self._md5(Body)}"'}

    def upload_part_copy(self, Bucket, Key, UploadId, PartNumber,
                         CopySource, CopySourceRange):
        self.calls.append("upload_part_copy")
        a, b = map(int, re.match(r"bytes=(\d+)-(\d+)",
                                 CopySourceRange).groups())
        body = self.objects[CopySource["Key"]][a:b + 1]
        self.uploads[UploadId][PartNumber] = body
        return {"CopyPartResult": {"ETag": f'"{self._md5(body)}"'}}

    def complete_multipart_upload(self, Bucket, Key, UploadId,
                                  MultipartUpload):
        parts = self.uploads.pop(UploadId)
        nums = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        self.objects[Key] = b"".join(parts[n] for n in sorted(nums))
        md5s = b"".join(bytes.fromhex(self._md5(parts[n]))
                        for n in sorted(nums))
        self.etags = getattr(self, "etags", {})
        self.etags[Key] = f"{hashlib.md5(md5s).hexdigest()}-{len(nums)}"

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append(UploadId)
        self.uploads.pop(UploadId, None)

    def head_object(self, Bucket, Key):
        # a whole PUT's tag is its MD5; a multipart one's, its parts'
        tag = getattr(self, "etags", {}).get(Key) or self._md5(
            self.objects[Key])
        return {"ETag": f'"{tag}"',
                "ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket, Key, Range=None):
        self.calls.append("get_object")
        data = self.objects[Key]
        if Range:
            a, b = map(int, re.match(r"bytes=(\d+)-(\d+)", Range).groups())
            data = data[a:b + 1]

        class Body:
            def read(self_inner):
                return data
        return {"Body": Body()}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


def _s3(client, bucket="b", endpoint="http://store"):
    s = carriers.S3Store(bucket, "p", endpoint=endpoint)
    s._client = client
    return s


DATA = bytes(itertools.islice(itertools.cycle(range(251)), 3 * 2 ** 20 + 7))
LINK = carriers.Link(rate=4e6, per_stream=1e6, rtt=0.05)


def _expected_digest(data, part):
    h = hashlib.sha256()
    for i in range(0, len(data), part):
        h.update(hashlib.sha256(data[i:i + part]).digest())
    return h.hexdigest()


def test_a_local_file_moves_as_parallel_ranged_parts(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dump.bin").write_bytes(DATA)
    src = carriers.LocalStore(tmp_path / "src")
    dst = carriers.LocalStore(tmp_path / "dst")
    link = carriers.Link(rate=4e6, per_stream=1e6, rtt=0.01)
    got = carriers.move_file(src, "dump.bin", dst, link=link)
    assert got.way == "parallel ranged parts"
    assert got.plan.streams == 4 and got.plan.parts == 4
    assert (tmp_path / "dst" / "dump.bin").read_bytes() == DATA
    assert got.digest == _expected_digest(DATA, got.plan.part)
    assert not list((tmp_path / "dst").glob("*.partial"))


def test_a_file_to_an_object_store_is_checked_part_by_part(tmp_path):
    (tmp_path / "f.parquet").write_bytes(DATA * 4)
    fake = _FakeS3()
    dst = _s3(fake)
    got = carriers.move_file(carriers.LocalStore(tmp_path), "f.parquet",
                             dst, link=LINK)
    assert got.way == "parallel ranged parts"
    assert fake.objects[dst._key("f.parquet")] == DATA * 4
    assert fake.calls.count("upload_part") == got.plan.parts > 1


def test_a_part_that_arrives_wrong_stops_the_move(tmp_path):
    (tmp_path / "f.bin").write_bytes(DATA * 4)
    fake = _FakeS3(corrupt=2)
    dst = _s3(fake)
    with pytest.raises(carriers.BadCopy):
        carriers.move_file(carriers.LocalStore(tmp_path), "f.bin", dst,
                           link=LINK)
    assert fake.aborted and dst._key("f.bin") not in fake.objects


def test_one_object_store_copies_inside_itself():
    fake = _FakeS3()
    src, dst = _s3(fake, "a"), _s3(fake, "b")
    fake.objects[src._key("big")] = DATA * 4
    got = carriers.move_file(src, "big", dst, "copy", link=LINK)
    assert got.way == "copied inside the store"
    assert fake.objects[dst._key("copy")] == DATA * 4
    assert "get_object" not in fake.calls
    assert fake.calls.count("upload_part_copy") == got.plan.parts


def test_two_stores_apart_do_not_copy_server_side(tmp_path):
    fake, other = _FakeS3(), _FakeS3()
    src, dst = _s3(fake), _s3(other, endpoint="http://elsewhere")
    fake.objects[src._key("big")] = DATA * 4
    got = carriers.move_file(src, "big", dst, link=LINK)
    assert got.way == "parallel ranged parts"
    assert "upload_part_copy" not in fake.calls + other.calls
    assert other.objects[dst._key("big")] == DATA * 4


def test_the_schedule_the_copy_is_given_is_in_the_run_record(
        tmp_path, monkeypatch):
    """What the planner laid out (the way, the time against one after
    another, the tables started before the first ends) and the relay's
    compression are kept in the run's decision record by the copy."""
    from migkit import config, decide
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = Hop(name="w18k", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    plan = planner.overlap({}, {"a": 8.0, "b": 4.0, "c": 4.0}, at_once=2)

    class Relay:
        def line(self):
            return "the wide leg is sent at level 3"
    entry = legs.record_schedule(hop, plan, "appdb", relay=Relay())
    assert legs.record_schedule(hop, None, "appdb") is None
    got = [e for e in decide.recorded(hop)
           if e["work"] == "appdb: the copy's schedule"]
    assert len(got) == 1 and got[0]["way"] == plan.way
    detail = got[0]["detail"]
    assert detail["tables"] == 3 and detail["relay"].startswith("the wide")
    assert detail["makespan_seconds"] == round(plan.makespan, 3)
    assert detail["makespan_seconds"] <= detail["serial_seconds"]
    assert entry["before_the_first_ends"] >= 1


def test_the_range_path_on_the_overlap_schedule_and_admit():
    from types import SimpleNamespace
    from migkit import decide
    known = {"public.orders": {"bytes": 8000, "rows": 80},
             "public.items": {"bytes": 2000, "rows": 20},
             "public.users": {"bytes": 5000, "rows": 50}}
    tables = ["public.items", "public.orders", "public.users"]
    bound = lambda v: SimpleNamespace(bound=lambda: v)
    model = SimpleNamespace(
        measured=True, little=2, carried=0,
        rate=lambda n: 100.0,
        stages={"target": bound(50.0), "source": bound(200.0)})
    plan = legs.schedule(SimpleNamespace(), "shop", known, tables, 2, model=model)
    assert plan is not None
    assert plan.checks and len(plan.checks) >= 2
    assert plan.makespan < plan.serial
    work = [("public.items", "i1"), ("public.orders", "o1"), ("public.orders", "o2"), ("public.users", "u1")]
    ordered = legs.in_schedule(work, plan)
    assert [w[0] for w in ordered] == ["public.orders", "public.orders", "public.users", "public.items"]
    ok, why = decide.admit(plan, {})
    assert ok and not why


def test_relay_compression_applied_to_streaming_copy_source_read(
        tmp_path, monkeypatch):
    from migkit import config, decide, jointplan
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    monkeypatch.setattr(movers, "cpu_headroom", lambda: 4.0)
    hop = Hop(name="w18k_relay", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    monkeypatch.setattr(jointplan, "semantics_of",
                        lambda eng, db: {"a": Semantics(table="a"),
                                         "b": Semantics(table="b")})
    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine",
                        lambda h: object())
    monkeypatch.setattr(legs, "_relayed", lambda h: True)
    sample = bytes(range(256)) * 40
    monkeypatch.setattr(legs, "_relay_sample", lambda h, db=None: (sample, 100))
    monkeypatch.setattr(planner, "link_speed", lambda h: 1000.0)
    got = movers._builds_beside_the_copy(hop, "appdb", 2, True)
    assert got is not None
    records = [e for e in decide.recorded(hop)
               if e["work"] == "appdb: the copy's schedule"]
    assert len(records) == 1
    assert "relay to here" in records[0]["detail"]["relay"]
    assert "compressed" in records[0]["detail"]["relay"]


def test_record_schedule_through_pull_move_on_a_postgres_fdw_lane(
        tmp_path, monkeypatch):
    from types import SimpleNamespace
    from migkit import config, decide
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = Hop(name="w18k_fdw", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    bound = lambda v: SimpleNamespace(bound=lambda: v)
    fake_model = SimpleNamespace(
        measured=True, little=2, carried=0,
        rate=lambda n: 100.0,
        stages={"target": bound(50.0), "source": bound(200.0)})
    monkeypatch.setattr("migkit.model.load", lambda p: fake_model)

    class FakeEngine:
        def __init__(self, h):
            self.hop = h
        def _all_tables(self, side, db):
            return ["public.t1", "public.t2"]
        def table_facts(self, side, db):
            return {"public.t1": {"bytes": 1000, "rows": 10},
                    "public.t2": {"bytes": 2000, "rows": 20}}
        def list_move_tables(self, db):
            return [("public", "t1"), ("public", "t2")]
        def _split(self, name):
            return name.split(".", 1)

    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine", FakeEngine)
    monkeypatch.setattr(legs, "source_for_target", lambda h: ("10.0.0.1", 5432))
    monkeypatch.setattr(legs, "facts", lambda h, db: {})
    monkeypatch.setattr(movers, "_pg_create_missing", lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_truncate_target", lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_quiet_triggers", lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_carry_sequences", lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_finish_created", lambda *a, **k: None)
    monkeypatch.setattr(legs, "servers_seen", lambda h: 10.0)

    class FakeFootprint:
        def __init__(self, *a, **k):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def import_tables(self, sch, tbls, fetch):
            return {t: t for t in tbls}
        def drop_credentials(self):
            pass

    monkeypatch.setattr(legs, "Footprint", FakeFootprint)
    monkeypatch.setattr(legs, "_table_work",
                        lambda h, eng, db, sch, t, local, known: [(f"{sch}.{t}", "stmt")])
    monkeypatch.setattr(legs, "trials",
                        lambda h, db, lf, log: SimpleNamespace(save=lambda: None))
    executed = []
    monkeypatch.setattr(legs, "_run_ranges",
                        lambda h, db, work, workers, quiet, log, t, lf: executed.extend(work))

    steps = legs.pull_move(hop, "appdb", workers=2, go=True, log=None)
    assert steps and len(executed) == 2
    records = [e for e in decide.recorded(hop)
               if e["work"] == "appdb: the copy's schedule"]
    assert len(records) == 1
    detail = records[0]["detail"]
    assert detail["tables"] == 2
    assert detail["makespan_seconds"] <= detail["serial_seconds"]
