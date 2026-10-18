import os
import threading
from typing import Any, Dict, Optional
import pytest

from migkit.partial_images import (
    FieldState,
    PartialFieldValue,
    PartialRowImage,
    RowVersionCache,
    PartialImageError,
    StaleRowVersionError,
    MissingBeforeImageError,
    PlaceholderLeakError,
    UNCHANGED_TOAST,
    decide_partial_image_cache,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("partial_images", "partial_image_cache_rungs", "retain_in_memory_lru", "holds"),
    ("partial_images", "partial_image_cache_rungs", "retain_in_memory_lru", "refuses"),
    ("partial_images", "partial_image_cache_rungs", "spill_or_compact_history", "holds"),
    ("partial_images", "partial_image_cache_rungs", "spill_or_compact_history", "refuses"),
    ("partial_images", "partial_image_cache_rungs", "demand_refetch_from_source", "holds"),
    ("partial_images", "partial_image_cache_rungs", "demand_refetch_from_source", "refuses"),
    ("partial_images", "partial_image_cache_rungs", "synthesize_tombstone_or_partial", "holds"),
    ("partial_images", "partial_image_cache_rungs", "synthesize_tombstone_or_partial", "refuses"),
    ("partial_images", "partial_image_cache_rungs", "refuse_placeholder_leak", "holds"),
]


def test_field_value_typed_states():
    v_absent = PartialFieldValue.absent()
    v_unchanged = PartialFieldValue.unchanged()
    v_null = PartialFieldValue.null()
    v_val = PartialFieldValue.value(b"test_payload")
    v_empty_bytes = PartialFieldValue.value(b"")
    v_empty_str = PartialFieldValue.value("")
    v_zero = PartialFieldValue.value(0)
    v_false = PartialFieldValue.value(False)

    assert v_absent.is_absent and not v_absent.is_concrete
    assert v_unchanged.is_unchanged and not v_unchanged.is_concrete
    assert v_null.is_null and v_null.is_concrete
    assert v_val.has_value and v_val.is_concrete

    assert v_absent.unwrap(default="default") == "default"
    assert v_unchanged.unwrap(default="default") == "default"
    assert v_null.unwrap(default="default") is None
    assert v_val.unwrap() == b"test_payload"
    assert v_empty_bytes.unwrap() == b""
    assert v_empty_str.unwrap() == ""
    assert v_zero.unwrap() == 0
    assert v_false.unwrap() is False


def test_placeholder_leak_prevention():
    raw_toast = UNCHANGED_TOAST
    field_parsed = PartialFieldValue.from_raw(raw_toast, quoted=False)
    assert field_parsed.is_unchanged
    assert not field_parsed.has_value

    with pytest.raises(PlaceholderLeakError):
        PartialFieldValue.value(UNCHANGED_TOAST)

    with pytest.raises(PlaceholderLeakError):
        PartialFieldValue.value("__debezium_unavailable_value")


def test_partial_row_image_creation_and_inspection():
    fields = {
        "id": PartialFieldValue.value(101),
        "name": PartialFieldValue.value("alice"),
        "lob": PartialFieldValue.unchanged(),
        "notes": PartialFieldValue.null(),
    }
    img = PartialRowImage(table="users", key={"id": 101}, fields=fields, op="update", version="0/1000")

    assert img.has_partial()
    assert img.unchanged_columns == ["lob"]
    assert img.null_columns == ["notes"]
    assert img.concrete_values == {"id": 101, "name": "alice", "notes": None}
    assert img.is_unchanged("lob")
    assert img.is_null("notes")
    assert img.has_concrete_value("name")
    assert img.get("name") == "alice"
    assert img.get("lob") is None

    canon_dict = img.to_canon_change()
    assert canon_dict["op"] == "update"
    assert canon_dict["table"] == "users"
    assert canon_dict["key"] == {"id": 101}
    assert canon_dict["unchanged"] == ["lob"]
    assert canon_dict["values"] == {"id": 101, "name": "alice", "notes": None}

    reconstructed_img = PartialRowImage.from_canon_change(canon_dict)
    assert reconstructed_img.is_unchanged("lob")
    assert reconstructed_img.is_null("notes")
    assert reconstructed_img.get("name") == "alice"


def test_update_of_small_columns_preserves_exact_lob_bytes():
    cache = RowVersionCache()
    large_lob = os.urandom(1024 * 512)

    insert_img = PartialRowImage(
        table="documents",
        key={"doc_id": 42},
        fields={
            "doc_id": PartialFieldValue.value(42),
            "title": PartialFieldValue.value("Initial Title"),
            "data": PartialFieldValue.value(large_lob),
            "status": PartialFieldValue.value("draft"),
            "extra": PartialFieldValue.null(),
        },
        op="insert",
        version="0/1000",
    )
    resolved_ins = cache.resolve(insert_img)
    assert resolved_ins.get("data") == large_lob

    update1 = PartialRowImage(
        table="documents",
        key={"doc_id": 42},
        fields={
            "doc_id": PartialFieldValue.value(42),
            "title": PartialFieldValue.value("Updated Title 1"),
            "data": PartialFieldValue.unchanged(),
            "status": PartialFieldValue.value("published"),
            "extra": PartialFieldValue.unchanged(),
        },
        op="update",
        version="0/2000",
    )
    resolved_up1 = cache.resolve(update1)

    assert resolved_up1.get("title") == "Updated Title 1"
    assert resolved_up1.get("status") == "published"
    assert resolved_up1.get("data") == large_lob
    assert len(resolved_up1.get("data")) == len(large_lob)
    assert resolved_up1.is_null("extra")
    assert resolved_up1.get("extra") is None

    update2 = PartialRowImage(
        table="documents",
        key={"doc_id": 42},
        fields={
            "doc_id": PartialFieldValue.value(42),
            "title": PartialFieldValue.unchanged(),
            "data": PartialFieldValue.unchanged(),
            "status": PartialFieldValue.value("archived"),
            "extra": PartialFieldValue.value("annotated"),
        },
        op="update",
        version="0/3000",
    )
    resolved_up2 = cache.resolve(update2)

    assert resolved_up2.get("title") == "Updated Title 1"
    assert resolved_up2.get("status") == "archived"
    assert resolved_up2.get("data") == large_lob
    assert resolved_up2.get("extra") == "annotated"


def test_true_sql_null_semantics_preserved():
    cache = RowVersionCache()
    lob_bytes = b"non_null_lob_data"

    ins = PartialRowImage(
        table="items",
        key={"id": 1},
        fields={
            "id": PartialFieldValue.value(1),
            "content": PartialFieldValue.value(lob_bytes),
            "optional_text": PartialFieldValue.null(),
        },
        op="insert",
        version=1,
    )
    cache.resolve(ins)

    up_null = PartialRowImage(
        table="items",
        key={"id": 1},
        fields={
            "id": PartialFieldValue.value(1),
            "content": PartialFieldValue.null(),
            "optional_text": PartialFieldValue.unchanged(),
        },
        op="update",
        version=2,
    )
    res_up_null = cache.resolve(up_null)
    assert res_up_null.is_null("content")
    assert res_up_null.get("content") is None
    assert res_up_null.is_null("optional_text")
    assert res_up_null.get("optional_text") is None

    up_restore = PartialRowImage(
        table="items",
        key={"id": 1},
        fields={
            "id": PartialFieldValue.value(1),
            "content": PartialFieldValue.value(b"restored_lob"),
            "optional_text": PartialFieldValue.unchanged(),
        },
        op="update",
        version=3,
    )
    res_restore = cache.resolve(up_restore)
    assert res_restore.get("content") == b"restored_lob"
    assert res_restore.is_null("optional_text")


def test_row_version_cache_version_monotonicity_and_validation():
    cache = RowVersionCache()
    ins = PartialRowImage(
        table="records",
        key={"k": "a"},
        fields={"k": PartialFieldValue.value("a"), "val": PartialFieldValue.value(b"init")},
        op="insert",
        version="0/2000",
    )
    cache.resolve(ins)

    stale_update = PartialRowImage(
        table="records",
        key={"k": "a"},
        fields={"k": PartialFieldValue.value("a"), "val": PartialFieldValue.unchanged()},
        op="update",
        version="0/1000",
    )
    with pytest.raises(StaleRowVersionError):
        cache.resolve(stale_update)

    valid_update = PartialRowImage(
        table="records",
        key={"k": "a"},
        fields={"k": PartialFieldValue.value("a"), "val": PartialFieldValue.unchanged(), "c": PartialFieldValue.value(1)},
        op="update",
        version="0/3000",
    )
    res = cache.resolve(valid_update)
    assert res.get("val") == b"init"
    assert res.get("c") == 1


def test_reconnect_and_refetch_on_cache_miss():
    persisted_db = {
        ("articles", "art-10"): {
            "id": "art-10",
            "body": b"rehydrated_body_from_db",
            "metadata": None,
            "revision": 5,
        }
    }
    fetch_calls = []

    def mock_refetch(table: str, key: Dict[str, Any], version: Optional[Any]) -> Optional[Dict[str, Any]]:
        fetch_calls.append((table, key, version))
        lookup_key = (table, key.get("id"))
        return persisted_db.get(lookup_key)

    reconnected_cache = RowVersionCache(refetch_fn=mock_refetch)

    update_after_reconnect = PartialRowImage(
        table="articles",
        key={"id": "art-10"},
        fields={
            "id": PartialFieldValue.value("art-10"),
            "revision": PartialFieldValue.value(6),
            "body": PartialFieldValue.unchanged(),
            "metadata": PartialFieldValue.unchanged(),
        },
        op="update",
        version="1/5000",
    )
    resolved = reconnected_cache.resolve(update_after_reconnect)

    assert len(fetch_calls) == 1
    assert resolved.get("body") == b"rehydrated_body_from_db"
    assert resolved.is_null("metadata")
    assert resolved.get("revision") == 6

    update_next = PartialRowImage(
        table="articles",
        key={"id": "art-10"},
        fields={
            "id": PartialFieldValue.value("art-10"),
            "revision": PartialFieldValue.value(7),
            "body": PartialFieldValue.unchanged(),
        },
        op="update",
        version="1/6000",
    )
    resolved_next = reconnected_cache.resolve(update_next)
    assert len(fetch_calls) == 1
    assert resolved_next.get("body") == b"rehydrated_body_from_db"
    assert resolved_next.get("revision") == 7

    missing_update = PartialRowImage(
        table="articles",
        key={"id": "art-999"},
        fields={"id": PartialFieldValue.value("art-999"), "body": PartialFieldValue.unchanged()},
        op="update",
        version="1/7000",
    )
    with pytest.raises(MissingBeforeImageError):
        reconnected_cache.resolve(missing_update)


def test_concurrent_lob_updates():
    cache = RowVersionCache()
    initial_lob = b"concurrent_base_lob_" + (b"X" * 1024)
    cache.resolve(
        PartialRowImage(
            table="concurrent_test",
            key={"id": 1},
            fields={"id": PartialFieldValue.value(1), "lob": PartialFieldValue.value(initial_lob), "counter": PartialFieldValue.value(0)},
            op="insert",
            version=100,
        )
    )

    num_threads = 8
    updates_per_thread = 25
    barrier = threading.Barrier(num_threads)
    errors = []

    def worker(worker_id: int):
        try:
            barrier.wait()
            for i in range(updates_per_thread):
                ver = 200 + (worker_id * updates_per_thread) + i
                up = PartialRowImage(
                    table="concurrent_test",
                    key={"id": 1},
                    fields={
                        "id": PartialFieldValue.value(1),
                        "counter": PartialFieldValue.value(ver),
                        "lob": PartialFieldValue.unchanged(),
                    },
                    op="update",
                    version=ver,
                )
                res = cache.resolve(up)
                if res.get("lob") != initial_lob:
                    errors.append(f"Worker {worker_id} got corrupted lob at ver {ver}")
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent execution produced errors: {errors}"


def test_cache_lru_eviction_and_memory_bounds():
    db = {}

    def refetch(table, key, ver):
        return db.get((table, key["id"]))

    cache = RowVersionCache(max_size=3, refetch_fn=refetch)

    for i in range(1, 5):
        data = f"val_{i}".encode("ascii")
        db[("evict", i)] = {"id": i, "data": data}
        cache.resolve(
            PartialRowImage(
                table="evict",
                key={"id": i},
                fields={"id": PartialFieldValue.value(i), "data": PartialFieldValue.value(data)},
                op="insert",
                version=i,
            )
        )

    assert cache.size <= 3

    up_evicted = PartialRowImage(
        table="evict",
        key={"id": 1},
        fields={"id": PartialFieldValue.value(1), "other": PartialFieldValue.value("new"), "data": PartialFieldValue.unchanged()},
        op="update",
        version=10,
    )
    resolved = cache.resolve(up_evicted)
    assert resolved.get("data") == b"val_1"
    assert resolved.get("other") == "new"


def test_state_export_and_import_checkpoint():
    cache1 = RowVersionCache()
    lob = b"checkpointed_large_blob_data"
    cache1.resolve(
        PartialRowImage(
            table="resumes",
            key={"id": "res-1"},
            fields={"id": PartialFieldValue.value("res-1"), "blob": PartialFieldValue.value(lob), "cnt": PartialFieldValue.value(1)},
            op="insert",
            version="0/1000",
        )
    )

    state = cache1.export_state()
    assert len(state) == 1

    cache2 = RowVersionCache()
    cache2.import_state(state)
    assert cache2.size == 1

    up = PartialRowImage(
        table="resumes",
        key={"id": "res-1"},
        fields={"id": PartialFieldValue.value("res-1"), "cnt": PartialFieldValue.value(2), "blob": PartialFieldValue.unchanged()},
        op="update",
        version="0/2000",
    )
    resolved = cache2.resolve(up)
    assert resolved.get("blob") == lob
    assert resolved.get("cnt") == 2


def test_partial_images_rungs_under_watched():
    with watched() as seen:
        # 1. retain_in_memory_lru holds
        f1 = {
            "entry_in_cache": True,
            "within_memory_budget": True,
            "within_entry_capacity": True,
            "low_eviction_pressure": True,
            "history_compactable": True,
            "spill_storage_available": True,
            "refetch_fn_available": True,
            "source_accessible": True,
            "partial_reconstruction_allowed": True,
        }
        c1 = decide_partial_image_cache(f1)
        assert c1.rung.name == "retain_in_memory_lru"

        # 2. retain_in_memory_lru refuses (not in cache), spill_or_compact_history holds
        f2 = {
            "entry_in_cache": False,
            "within_memory_budget": True,
            "within_entry_capacity": True,
            "low_eviction_pressure": True,
            "history_compactable": True,
            "spill_storage_available": True,
            "refetch_fn_available": True,
            "source_accessible": True,
            "partial_reconstruction_allowed": True,
        }
        c2 = decide_partial_image_cache(f2)
        assert c2.rung.name == "spill_or_compact_history"

        # 3. spill_or_compact_history refuses, demand_refetch_from_source holds
        f3 = {
            "entry_in_cache": False,
            "within_memory_budget": False,
            "within_entry_capacity": False,
            "low_eviction_pressure": False,
            "history_compactable": False,
            "spill_storage_available": False,
            "refetch_fn_available": True,
            "source_accessible": True,
            "partial_reconstruction_allowed": True,
        }
        c3 = decide_partial_image_cache(f3)
        assert c3.rung.name == "demand_refetch_from_source"

        # 4. demand_refetch_from_source refuses, synthesize_tombstone_or_partial holds
        f4 = {
            "entry_in_cache": False,
            "within_memory_budget": False,
            "within_entry_capacity": False,
            "low_eviction_pressure": False,
            "history_compactable": False,
            "spill_storage_available": False,
            "refetch_fn_available": False,
            "source_accessible": False,
            "partial_reconstruction_allowed": True,
        }
        c4 = decide_partial_image_cache(f4)
        assert c4.rung.name == "synthesize_tombstone_or_partial"

        # 5. synthesize_tombstone_or_partial refuses, refuse_placeholder_leak holds
        f5 = {
            "entry_in_cache": False,
            "within_memory_budget": False,
            "within_entry_capacity": False,
            "low_eviction_pressure": False,
            "history_compactable": False,
            "spill_storage_available": False,
            "refetch_fn_available": False,
            "source_accessible": False,
            "partial_reconstruction_allowed": False,
        }
        c5 = decide_partial_image_cache(f5)
        assert c5.rung.name == "refuse_placeholder_leak"
