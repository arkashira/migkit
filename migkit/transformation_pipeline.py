import duckdb
import pyarrow as pa
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union


class TransformationPipeline:
    def __init__(self, sql: Optional[str] = None, lookups: Optional[Dict[str, Any]] = None):
        self.sql = sql
        self.lookups: Dict[str, Any] = dict(lookups or {})
        self.steps: List[str] = [sql] if sql else []

    def add_step(self, sql: str) -> "TransformationPipeline":
        self.steps.append(sql)
        return self

    def register_lookup(self, name: str, data: Any, columns: Optional[Sequence[str]] = None) -> "TransformationPipeline":
        if isinstance(data, (list, tuple)) and data and isinstance(data[0], (dict, tuple, list)):
            if isinstance(data[0], dict):
                tbl = pa.Table.from_pylist(list(data))
            else:
                cols = columns or [f"col_{i}" for i in range(len(data[0]))]
                tbl = pa.Table.from_arrays([pa.array([row[i] for row in data]) for i in range(len(cols))], names=cols)
            self.lookups[name] = tbl
        elif isinstance(data, pa.Table):
            self.lookups[name] = data
        else:
            self.lookups[name] = data
        return self

    def _rows_to_arrow(self, rows: Sequence[Any], columns: Optional[Sequence[str]] = None) -> pa.Table:
        if not rows:
            cols = columns or []
            return pa.table({c: pa.array([]) for c in cols})
        if isinstance(rows[0], dict):
            return pa.Table.from_pylist(list(rows))
        cols = list(columns or [f"col_{i}" for i in range(len(rows[0]))])
        arrays = [pa.array([r[i] for r in rows]) for i in range(len(cols))]
        return pa.Table.from_arrays(arrays, names=cols)

    def _execute_sql(self, con: duckdb.DuckDBPyConnection, sql: str) -> pa.Table:
        rel = con.sql(sql)
        if rel is None:
            return pa.table([])
        res = rel.arrow()
        if hasattr(res, "read_all"):
            return res.read_all()
        return res

    def transform_arrow(self, batch: pa.Table, sql: Optional[str] = None) -> pa.Table:
        queries = [sql] if sql else (self.steps or ["SELECT * FROM source_batch"])
        con = duckdb.connect(":memory:")
        try:
            for l_name, l_data in self.lookups.items():
                if isinstance(l_data, pa.Table):
                    con.register(l_name, l_data)
                elif hasattr(l_data, "to_arrow"):
                    con.register(l_name, l_data.to_arrow())
                else:
                    con.register(l_name, l_data)
            current = batch
            for q in queries:
                con.register("source_batch", current)
                con.register("batch", current)
                current = self._execute_sql(con, q)
            return current
        finally:
            con.close()

    def transform_batch(
        self,
        rows: Sequence[Any],
        columns: Optional[Sequence[str]] = None,
        sql: Optional[str] = None,
        as_dict: bool = True,
    ) -> Union[List[Dict[str, Any]], Tuple[List[Tuple[Any, ...]], List[str]]]:
        if not rows:
            return [] if as_dict else ([], list(columns or []))
        arrow_in = self._rows_to_arrow(rows, columns)
        arrow_out = self.transform_arrow(arrow_in, sql=sql)
        if as_dict:
            return arrow_out.to_pylist()
        out_cols = arrow_out.column_names
        pydict = arrow_out.to_pydict()
        num_rows = len(arrow_out)
        out_rows = [tuple(pydict[col][i] for col in out_cols) for i in range(num_rows)]
        return out_rows, out_cols

    def transform_cdc(
        self,
        changes: Sequence[Dict[str, Any]],
        sql: Optional[str] = None,
        row_key: str = "row",
    ) -> List[Dict[str, Any]]:
        if not changes:
            return []
        rows = [c.get(row_key, c) for c in changes]
        transformed_rows = self.transform_batch(rows, sql=sql, as_dict=True)
        out = []
        for orig, tr in zip(changes, transformed_rows):
            item = dict(orig)
            item[row_key] = tr
            out.append(item)
        return out

    def verify_transform(
        self,
        source_rows: Sequence[Any],
        target_rows: Sequence[Any],
        sql: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        sort_key: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        expected = self.transform_batch(source_rows, columns=columns, sql=sql, as_dict=True)
        actual = target_rows
        if actual and not isinstance(actual[0], dict) and columns:
            actual = [dict(zip(columns, r)) for r in actual]
        if sort_key:
            expected = sorted(expected, key=lambda r: str(r.get(sort_key, "")))
            actual = sorted(actual, key=lambda r: str(r.get(sort_key, "")))
        if len(expected) != len(actual):
            return False, f"Row count mismatch: expected {len(expected)}, got {len(actual)}"
        for idx, (exp, act) in enumerate(zip(expected, actual)):
            if exp != act:
                return False, f"Row {idx} mismatch: expected {exp}, got {act}"
        return True, None


def create_pipeline(sql: Optional[str] = None, lookups: Optional[Dict[str, Any]] = None) -> TransformationPipeline:
    return TransformationPipeline(sql=sql, lookups=lookups)
