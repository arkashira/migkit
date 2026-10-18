import hashlib
import io
import sqlite3
import pytest

from migkit.lob_transaction import (
    AdapterLobCapability,
    ColumnConstraint,
    CompensationRecord,
    CompensationStatus,
    ConcurrentLobMutationError,
    DelayedFetchHandle,
    ExecutionPath,
    LOBTransactionCoordinator,
    LobStreamTransactionError,
    NotNullConstraintViolation,
    StaleTransactionIdentityError,
    TransactionIdentity,
    probe_adapter_lob_capability,
    lob_execution_rungs,
    decide_lob_execution,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("lob_transaction", "lob_execution_rungs", "inline_lob_write", "holds"),
    ("lob_transaction", "lob_execution_rungs", "inline_lob_write", "refuses"),
    ("lob_transaction", "lob_execution_rungs", "chunked_stream_write", "holds"),
    ("lob_transaction", "lob_execution_rungs", "chunked_stream_write", "refuses"),
    ("lob_transaction", "lob_execution_rungs", "delayed_fetch_from_locator", "holds"),
    ("lob_transaction", "lob_execution_rungs", "delayed_fetch_from_locator", "refuses"),
    ("lob_transaction", "lob_execution_rungs", "stage_to_spill_storage", "holds"),
    ("lob_transaction", "lob_execution_rungs", "stage_to_spill_storage", "refuses"),
    ("lob_transaction", "lob_execution_rungs", "refuse_lob_write", "holds"),
]


def test_probe_adapter_lob_capability():
    ora_cap = probe_adapter_lob_capability("oracle")
    assert ora_cap.empty_string_is_null is True
    assert ora_cap.supports_streaming is True

    pg_cap = probe_adapter_lob_capability("postgres")
    assert pg_cap.empty_string_is_null is False
    assert pg_cap.supports_streaming is True

    sqlite_cap = probe_adapter_lob_capability("sqlite")
    assert sqlite_cap.empty_string_is_null is False
    assert sqlite_cap.supports_savepoints is True


def test_price_inline_vs_streamed_paths():
    cap = AdapterLobCapability(
        engine_name="postgres",
        eager_materialize=False,
        max_inline_bytes=64 * 1024,
        supports_streaming=True,
        supports_savepoints=True,
        empty_string_is_null=False,
    )
    coord = LOBTransactionCoordinator(cap, memory_budget=32 * 1024 * 1024)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=True)

    pricing_small = coord.price_paths(lob_size_bytes=4096, constraint=constraint)
    assert pricing_small.recommended_path == ExecutionPath.INLINE
    assert pricing_small.prices[ExecutionPath.INLINE].is_feasible is True
    assert pricing_small.prices[ExecutionPath.INLINE].round_trips == 1

    pricing_large = coord.price_paths(lob_size_bytes=5 * 1024 * 1024, constraint=constraint)
    assert pricing_large.recommended_path == ExecutionPath.STREAMED
    assert pricing_large.prices[ExecutionPath.STREAMED].is_feasible is True
    assert pricing_large.prices[ExecutionPath.STREAMED].round_trips > 1

    no_stream_cap = AdapterLobCapability(
        engine_name="constrained",
        eager_materialize=True,
        max_inline_bytes=1024,
        supports_streaming=False,
        supports_savepoints=True,
        empty_string_is_null=False,
    )
    coord_no_stream = LOBTransactionCoordinator(no_stream_cap, memory_budget=2048)
    pricing_refused = coord_no_stream.price_paths(lob_size_bytes=100 * 1024, constraint=constraint)
    assert pricing_refused.recommended_path == ExecutionPath.REFUSED
    assert pricing_refused.prices[ExecutionPath.REFUSED].refusal_reason is not None


def test_do_not_relax_not_null_silently():
    cap = probe_adapter_lob_capability("postgres")
    coord = LOBTransactionCoordinator(cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    with pytest.raises(NotNullConstraintViolation):
        coord.coordinate_write(
            con,
            table="docs",
            key={"id": 1},
            column="content",
            lob_value=None,
            constraint=constraint,
        )

    row = con.execute("SELECT * FROM docs WHERE id = 1").fetchone()
    assert row is None


def test_non_null_empty_clob_semantics():
    pg_cap = probe_adapter_lob_capability("postgres")
    coord_pg = LOBTransactionCoordinator(pg_cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    receipt = coord_pg.coordinate_write(
        con,
        table="docs",
        key={"id": 1},
        column="content",
        lob_value="",
        constraint=constraint,
    )
    assert receipt.committed is True

    row = con.execute("SELECT content FROM docs WHERE id = 1").fetchone()
    assert row is not None
    assert row[0] == ""

    ora_cap = probe_adapter_lob_capability("oracle")
    coord_ora = LOBTransactionCoordinator(ora_cap)
    with pytest.raises(NotNullConstraintViolation) as exc_info:
        coord_ora.coordinate_write(
            con,
            table="docs",
            key={"id": 2},
            column="content",
            lob_value="",
            constraint=constraint,
        )
    assert "empty string as null" in str(exc_info.value).lower()


def test_delayed_fetching_retains_transaction_identity():
    cap = probe_adapter_lob_capability("postgres")
    coord = LOBTransactionCoordinator(cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    tx_id = "tx-snap-1001"
    identity = TransactionIdentity(
        tx_id=tx_id,
        snapshot_id="snap-42",
        scn_or_lsn=500200,
        version_hash="hash-abc-123",
        is_active=True,
    )

    handle = DelayedFetchHandle(
        identity=identity,
        column_name="content",
        fetch_fn=lambda ident: "delayed full clob body",
    )

    receipt = coord.coordinate_write(
        con,
        table="docs",
        key={"id": 10},
        column="content",
        lob_value=handle,
        constraint=constraint,
    )
    assert receipt.committed is True
    row = con.execute("SELECT content FROM docs WHERE id = 10").fetchone()
    assert row[0] == "delayed full clob body"


def test_delayed_fetching_stale_identity_aborts():
    cap = probe_adapter_lob_capability("postgres")
    coord = LOBTransactionCoordinator(cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    identity = TransactionIdentity(
        tx_id="tx-old",
        snapshot_id="snap-old",
        is_active=False,
    )

    handle = DelayedFetchHandle(
        identity=identity,
        column_name="content",
        fetch_fn=lambda ident: "never read",
    )

    with pytest.raises(StaleTransactionIdentityError):
        coord.coordinate_write(
            con,
            table="docs",
            key={"id": 20},
            column="content",
            lob_value=handle,
            constraint=constraint,
        )

    row = con.execute("SELECT * FROM docs WHERE id = 20").fetchone()
    assert row is None


def test_delayed_fetching_concurrent_mutation_detection():
    cap = probe_adapter_lob_capability("postgres")
    coord = LOBTransactionCoordinator(cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    identity = TransactionIdentity(
        tx_id="tx-mutating",
        snapshot_id="snap-live",
        scn_or_lsn=100,
        version_hash="v1",
        is_active=True,
    )

    def detect_mutation(ident: TransactionIdentity) -> bool:
        return True

    handle = DelayedFetchHandle(
        identity=identity,
        column_name="content",
        fetch_fn=lambda ident: "mutated payload",
        mutation_detector=detect_mutation,
    )

    with pytest.raises(ConcurrentLobMutationError):
        coord.coordinate_write(
            con,
            table="docs",
            key={"id": 30},
            column="content",
            lob_value=handle,
            constraint=constraint,
        )

    row = con.execute("SELECT * FROM docs WHERE id = 30").fetchone()
    assert row is None


def test_failure_between_row_insert_and_lob_streaming_atomic_compensation():
    cap = AdapterLobCapability(
        engine_name="sqlite",
        eager_materialize=False,
        max_inline_bytes=1024,
        supports_streaming=True,
        supports_savepoints=True,
        empty_string_is_null=False,
    )
    coord = LOBTransactionCoordinator(cap, chunk_size=512)
    constraint = ColumnConstraint(name="body", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE papers (id INT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL)")

    large_body = "x" * 4096

    class FaultyStream:
        def __init__(self, data: str):
            self.stream = io.StringIO(data)
            self.chunks_read = 0

        def read(self, n: int) -> str:
            self.chunks_read += 1
            if self.chunks_read >= 3:
                raise ConnectionResetError("Simulated network drop mid-stream")
            return self.stream.read(n)

    faulty = FaultyStream(large_body)

    with pytest.raises(LobStreamTransactionError) as exc_info:
        coord.coordinate_write(
            con,
            table="papers",
            key={"id": 100},
            column="body",
            lob_value=faulty,
            constraint=constraint,
            other_cols={"title": "Paper Title"},
        )

    err = exc_info.value
    assert err.compensation_record is not None
    assert err.compensation_record.status in (
        CompensationStatus.ROLLED_BACK,
        CompensationStatus.COMPENSATED,
    )
    assert err.compensation_record.recoverable is True
    assert err.compensation_record.table == "papers"

    row = con.execute("SELECT * FROM papers WHERE id = 100").fetchone()
    assert row is None


def test_successful_streamed_lob_atomic_commit():
    cap = AdapterLobCapability(
        engine_name="sqlite",
        eager_materialize=False,
        max_inline_bytes=1024,
        supports_streaming=True,
        supports_savepoints=True,
        empty_string_is_null=False,
    )
    coord = LOBTransactionCoordinator(cap, chunk_size=512)
    constraint = ColumnConstraint(name="body", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE papers (id INT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL)")

    large_body = "abc" * 1500  # 4500 chars, exceeds 1024 byte inline limit
    stream = io.StringIO(large_body)

    receipt = coord.coordinate_write(
        con,
        table="papers",
        key={"id": 200},
        column="body",
        lob_value=stream,
        constraint=constraint,
        other_cols={"title": "Atomic Stream Paper"},
    )

    assert receipt.committed is True
    assert receipt.path == ExecutionPath.STREAMED
    assert receipt.bytes_written == len(large_body)

    row = con.execute("SELECT title, body FROM papers WHERE id = 200").fetchone()
    assert row is not None
    assert row[0] == "Atomic Stream Paper"
    assert row[1] == large_body


def test_relax_not_null_prohibited():
    cap = probe_adapter_lob_capability("postgres")
    coord = LOBTransactionCoordinator(cap)
    constraint = ColumnConstraint(name="content", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT NOT NULL)")

    with pytest.raises(NotNullConstraintViolation) as exc_info:
        coord.coordinate_write(
            con,
            table="docs",
            key={"id": 1},
            column="content",
            lob_value="val",
            constraint=constraint,
            relax_not_null=True,
        )
    assert "prohibited" in str(exc_info.value).lower()


def test_compensation_fallback_via_delete_without_savepoints():
    cap = AdapterLobCapability(
        engine_name="nosavepoint",
        eager_materialize=False,
        max_inline_bytes=1024,
        supports_streaming=True,
        supports_savepoints=False,
        empty_string_is_null=False,
    )
    coord = LOBTransactionCoordinator(cap, chunk_size=512)
    constraint = ColumnConstraint(name="body", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE papers (id INT PRIMARY KEY, body TEXT NOT NULL)")

    class FaultyStream:
        def __init__(self, data: str):
            self.stream = io.StringIO(data)
            self.chunks = 0

        def read(self, n: int) -> str:
            self.chunks += 1
            if self.chunks >= 2:
                raise IOError("I/O error during chunk write")
            return self.stream.read(n)

    faulty = FaultyStream("abc" * 1000)

    with pytest.raises(LobStreamTransactionError) as exc_info:
        coord.coordinate_write(
            con,
            table="papers",
            key={"id": 300},
            column="body",
            lob_value=faulty,
            constraint=constraint,
        )

    err = exc_info.value
    assert err.compensation_record is not None
    assert err.compensation_record.status == CompensationStatus.COMPENSATED
    assert err.compensation_record.step_failed == "stream_chunk"

    row = con.execute("SELECT * FROM papers WHERE id = 300").fetchone()
    assert row is None


def test_binary_blob_streaming_and_digest():
    cap = probe_adapter_lob_capability("sqlite")
    coord = LOBTransactionCoordinator(cap, chunk_size=1024)
    constraint = ColumnConstraint(name="data", data_type="blob", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE binaries (id INT PRIMARY KEY, data BLOB NOT NULL)")

    payload = b"\x00\xff\xfe\x01\x02\x03" * 2000  # 12,000 bytes
    stream = io.BytesIO(payload)

    receipt = coord.coordinate_write(
        con,
        table="binaries",
        key={"id": 400},
        column="data",
        lob_value=stream,
        constraint=constraint,
    )

    assert receipt.committed is True
    assert receipt.bytes_written == len(payload)
    assert receipt.digest is not None

    row = con.execute("SELECT data FROM binaries WHERE id = 400").fetchone()
    assert row is not None
    assert row[0] == payload
    assert hashlib.md5(row[0]).hexdigest() == receipt.digest


def test_row_insert_failure_triggers_compensation():
    cap = probe_adapter_lob_capability("sqlite")
    coord = LOBTransactionCoordinator(cap, chunk_size=512)
    constraint = ColumnConstraint(name="body", data_type="text", nullable=False)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE papers (id INT PRIMARY KEY, body TEXT NOT NULL)")
    con.execute("INSERT INTO papers VALUES (500, 'initial')")

    stream = io.StringIO("x" * 2048)

    with pytest.raises(LobStreamTransactionError) as exc_info:
        coord.coordinate_write(
            con,
            table="papers",
            key={"id": 500},
            column="body",
            lob_value=stream,
            constraint=constraint,
        )

    err = exc_info.value
    assert err.compensation_record is not None
    assert err.compensation_record.step_failed == "row_insert"
    assert err.compensation_record.recoverable is True


def test_savepoint_failure_raises_unswallowed_error():
    cap = probe_adapter_lob_capability("sqlite")
    coord = LOBTransactionCoordinator(cap, chunk_size=512)
    constraint = ColumnConstraint(name="body", data_type="text", nullable=False)

    class BrokenConn:
        def execute(self, sql, *args):
            if "SAVEPOINT" in sql:
                raise RuntimeError("disk IO error while creating savepoint")
            return None

    broken_conn = BrokenConn()
    stream = io.StringIO("hello world")

    with pytest.raises(LobStreamTransactionError) as exc_info:
        coord.coordinate_write(
            broken_conn,
            table="papers",
            key={"id": 600},
            column="body",
            lob_value=stream,
            constraint=constraint,
        )

    err = exc_info.value
    assert err.compensation_record is not None
    assert err.compensation_record.step_failed == "savepoint_create"
    assert "Savepoint creation failed" in str(err)
    assert not err.compensation_record.recoverable


def test_lob_execution_rungs_under_watched():
    with watched():
        # 1. inline_lob_write holds
        f1 = {
            "lob_bytes": 1024,
            "max_inline_bytes": 65536,
            "memory_budget": 67108864,
            "not_null_satisfied": True,
            "supports_streaming": True,
            "supports_savepoints": True,
            "within_transaction_budget": True,
            "retains_snapshot_locators": True,
            "tx_identity_active": True,
            "concurrent_mutation": False,
            "spill_storage_available": True,
            "spill_permitted": True,
        }
        c1 = decide_lob_execution(f1)
        assert c1.rung.name == "inline_lob_write"

        # 2. inline_lob_write refuses, chunked_stream_write holds
        f2 = {
            "lob_bytes": 1000000,
            "max_inline_bytes": 65536,
            "memory_budget": 67108864,
            "not_null_satisfied": True,
            "supports_streaming": True,
            "supports_savepoints": True,
            "within_transaction_budget": True,
            "retains_snapshot_locators": True,
            "tx_identity_active": True,
            "concurrent_mutation": False,
            "spill_storage_available": True,
            "spill_permitted": True,
        }
        c2 = decide_lob_execution(f2)
        assert c2.rung.name == "chunked_stream_write"

        # 3. chunked_stream_write refuses, delayed_fetch_from_locator holds
        f3 = {
            "lob_bytes": 1000000,
            "max_inline_bytes": 65536,
            "memory_budget": 67108864,
            "not_null_satisfied": True,
            "supports_streaming": False,
            "supports_savepoints": False,
            "within_transaction_budget": False,
            "retains_snapshot_locators": True,
            "tx_identity_active": True,
            "concurrent_mutation": False,
            "spill_storage_available": True,
            "spill_permitted": True,
        }
        c3 = decide_lob_execution(f3)
        assert c3.rung.name == "delayed_fetch_from_locator"

        # 4. delayed_fetch_from_locator refuses, stage_to_spill_storage holds
        f4 = {
            "lob_bytes": 1000000,
            "max_inline_bytes": 65536,
            "memory_budget": 67108864,
            "not_null_satisfied": True,
            "supports_streaming": False,
            "supports_savepoints": False,
            "within_transaction_budget": False,
            "retains_snapshot_locators": False,
            "tx_identity_active": False,
            "concurrent_mutation": True,
            "spill_storage_available": True,
            "spill_permitted": True,
        }
        c4 = decide_lob_execution(f4)
        assert c4.rung.name == "stage_to_spill_storage"

        # 5. stage_to_spill_storage refuses, refuse_lob_write holds
        f5 = {
            "lob_bytes": 1000000,
            "max_inline_bytes": 65536,
            "memory_budget": 67108864,
            "not_null_satisfied": True,
            "supports_streaming": False,
            "supports_savepoints": False,
            "within_transaction_budget": False,
            "retains_snapshot_locators": False,
            "tx_identity_active": False,
            "concurrent_mutation": True,
            "spill_storage_available": False,
            "spill_permitted": False,
        }
        c5 = decide_lob_execution(f5)
        assert c5.rung.name == "refuse_lob_write"

