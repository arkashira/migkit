from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple


@dataclass(frozen=True)
class KeylessSlice:
    index: int
    start: int
    end: Optional[int]
    predicate: str
    mode: str = "ctid"


@dataclass
class KeylessSliceResult:
    table: str
    total_rows: int
    slices_count: int
    matched_slices: int
    divergent_slices: int
    status: str
    detail: str
    certified_complete: bool = False
    slice_details: list = field(default_factory=list)


def ctid_slice_predicate(block_start: int, block_end: Optional[int] = None) -> str:
    if block_end is not None:
        return f"ctid >= '({block_start}, 0)' AND ctid < '({block_end}, 0)'"
    return f"ctid >= '({block_start}, 0)'"


def rowid_slice_predicate(start: int, end: Optional[int] = None) -> str:
    if end is not None:
        return f"_rowid >= {start} AND _rowid < {end}"
    return f"_rowid >= {start}"


def page_offset_slice_predicate(start: int, end: Optional[int] = None) -> str:
    if end is not None:
        return f"page_offset >= {start} AND page_offset < {end}"
    return f"page_offset >= {start}"


def postgres_ctid_slices(total_blocks: int, chunk_size: Optional[int] = None, *, open_ended: bool = False) -> List[KeylessSlice]:
    if total_blocks <= 0:
        return [KeylessSlice(0, 0, None, "1=1", mode="ctid")]
    step = chunk_size if chunk_size and chunk_size > 0 else max(1, total_blocks // 10 or 1)
    slices = []
    idx = 0
    cur = 0
    while cur < total_blocks:
        nxt = cur + step
        if nxt >= total_blocks:
            end = None if open_ended else total_blocks
            pred = ctid_slice_predicate(cur, end)
            slices.append(KeylessSlice(idx, cur, end, pred, mode="ctid"))
            break
        pred = ctid_slice_predicate(cur, nxt)
        slices.append(KeylessSlice(idx, cur, nxt, pred, mode="ctid"))
        idx += 1
        cur = nxt
    return slices


def mysql_rowid_slices(min_rowid: int, max_rowid: int, chunk_size: Optional[int] = None, *, open_ended: bool = False) -> List[KeylessSlice]:
    if max_rowid < min_rowid:
        return [KeylessSlice(0, min_rowid, None, "1=1", mode="rowid")]
    span = max_rowid - min_rowid + 1
    step = chunk_size if chunk_size and chunk_size > 0 else max(1, span // 10 or 1)
    slices = []
    idx = 0
    cur = min_rowid
    while cur <= max_rowid:
        nxt = cur + step
        if nxt > max_rowid:
            end = None if open_ended else max_rowid + 1
            pred = rowid_slice_predicate(cur, end)
            slices.append(KeylessSlice(idx, cur, end, pred, mode="rowid"))
            break
        pred = rowid_slice_predicate(cur, nxt)
        slices.append(KeylessSlice(idx, cur, nxt, pred, mode="rowid"))
        idx += 1
        cur = nxt
    return slices


def mysql_page_slices(total_pages: int, chunk_size: Optional[int] = None, *, open_ended: bool = False) -> List[KeylessSlice]:
    if total_pages <= 0:
        return [KeylessSlice(0, 0, None, "1=1", mode="page")]
    step = chunk_size if chunk_size and chunk_size > 0 else max(1, total_pages // 10 or 1)
    slices = []
    idx = 0
    cur = 0
    while cur < total_pages:
        nxt = cur + step
        if nxt >= total_pages:
            end = None if open_ended else total_pages
            pred = page_offset_slice_predicate(cur, end)
            slices.append(KeylessSlice(idx, cur, end, pred, mode="page"))
            break
        pred = page_offset_slice_predicate(cur, nxt)
        slices.append(KeylessSlice(idx, cur, nxt, pred, mode="page"))
        idx += 1
        cur = nxt
    return slices


def keyless_slice_predicate(engine: str, *,
                            block_start: Optional[int] = None,
                            block_end: Optional[int] = None,
                            rowid_start: Optional[int] = None,
                            rowid_end: Optional[int] = None,
                            page_start: Optional[int] = None,
                            page_end: Optional[int] = None,
                            mode: str = "auto") -> str:
    eng = str(engine).lower()
    if eng in ("postgres", "pg", "postgresql") or mode == "ctid" or (mode == "auto" and block_start is not None and rowid_start is None):
        bs = 0 if block_start is None else block_start
        return ctid_slice_predicate(bs, block_end)
    elif mode == "page" or page_start is not None:
        ps = 0 if page_start is None else page_start
        return page_offset_slice_predicate(ps, page_end)
    else:
        rs = 1 if rowid_start is None else rowid_start
        return rowid_slice_predicate(rs, rowid_end)


def keyless_slices(engine: str,
                   total_units: int,
                   chunk_size: Optional[int] = None,
                   *,
                   mode: str = "auto",
                   min_val: int = 0,
                   open_ended: bool = False) -> List[KeylessSlice]:
    eng = str(engine).lower()
    if mode == "ctid" or (mode == "auto" and eng in ("postgres", "pg", "postgresql")):
        return postgres_ctid_slices(total_units, chunk_size, open_ended=open_ended)
    elif mode == "page":
        return mysql_page_slices(total_units, chunk_size, open_ended=open_ended)
    else:
        start_val = 1 if min_val == 0 else min_val
        max_val = start_val + total_units - 1 if total_units > 0 else start_val
        return mysql_rowid_slices(start_val, max_val, chunk_size, open_ended=open_ended)


def keyless_slice_query(engine: str,
                        table: str,
                        slice_predicate: str,
                        *,
                        hash_expr: Optional[str] = None,
                        hash_algo: str = "xor",
                        hash_agg: Optional[str] = None,
                        charset_columns: tuple = (),
                        null_empty_conditions: tuple = (),
                        null_empty_columns: tuple = (),
                        where: str = "",
                        aliases: bool = False,
                        quote_identifiers: bool = True) -> str:
    from . import scanplan
    conds = []
    if slice_predicate:
        sp = slice_predicate.strip()
        if sp.lower().startswith("where "):
            sp = sp[6:].strip()
        if sp:
            conds.append(sp)
    if where:
        w = where.strip()
        if w.lower().startswith("where "):
            w = w[6:].strip()
        if w:
            conds.append(w)
    combined_where = " AND ".join(f"({c})" for c in conds) if conds else ""
    return scanplan.fused_pushdown_query(
        engine, table, hash_expr=hash_expr, hash_algo=hash_algo,
        hash_agg=hash_agg, charset_columns=charset_columns,
        null_empty_conditions=null_empty_conditions,
        null_empty_columns=null_empty_columns,
        where=combined_where, aliases=aliases,
        quote_identifiers=quote_identifiers)


def keyless_slice_verify(eng, db, table, *,
                         query: Callable[[str, str], Any],
                         source: Optional[Callable[[str, str], str]] = None,
                         sides: Tuple[str, ...] = ("src", "dst"),
                         total_blocks: Optional[int] = None,
                         total_rows: Optional[int] = None,
                         total_pages: Optional[int] = None,
                         chunk_size: Optional[int] = None,
                         mode: str = "auto",
                         hash_expr: Optional[str] = None,
                         hash_algo: str = "xor",
                         hash_agg: Optional[str] = None,
                         text_columns: tuple = (),
                         null_empty_conditions: tuple = (),
                         null_empty_columns: tuple = (),
                         quote_identifiers: bool = True) -> KeylessSliceResult:
    dialect = getattr(eng, "CANON_ENGINE", str(getattr(eng, "engine", eng))).lower()
    if dialect in ("postgres", "pg", "postgresql"):
        units = total_blocks if total_blocks is not None else (total_rows if total_rows is not None else 1000)
        mode = "ctid" if mode == "auto" else mode
    else:
        if mode == "page":
            units = total_pages if total_pages is not None else (total_rows if total_rows is not None else 1000)
        else:
            units = total_rows if total_rows is not None else 1000
            mode = "rowid" if mode == "auto" else mode

    slices = keyless_slices(dialect, units, chunk_size=chunk_size, mode=mode)

    if source is None:
        source = lambda side, t: t

    slice_details = []
    matched = 0
    divergent = 0
    tot_rows = 0

    for sl in slices:
        per_side = {}
        for side in sides:
            tbl = source(side, table)
            sql = keyless_slice_query(
                dialect, tbl, sl.predicate,
                hash_expr=hash_expr, hash_algo=hash_algo, hash_agg=hash_agg,
                charset_columns=text_columns,
                null_empty_conditions=null_empty_conditions,
                null_empty_columns=null_empty_columns,
                quote_identifiers=quote_identifiers)
            ans = query(side, sql)
            if not ans:
                raise RuntimeError(f"keyless query returned no rows for {table} on {side}")
            row = ans[0]
            if isinstance(row, str):
                row = row.split("|")
            r_cnt = int(row[0] or 0)
            r_hash = row[1] if len(row) > 1 and row[1] is not None else 0
            per_side[side] = (r_cnt, r_hash)

        if len(sides) >= 2:
            s_res = per_side[sides[0]]
            d_res = per_side[sides[1]]
            is_match = (s_res == d_res)
            if is_match:
                matched += 1
            else:
                divergent += 1
            slice_details.append({
                "index": sl.index,
                "predicate": sl.predicate,
                "match": is_match,
                "src": s_res,
                "dst": d_res
            })
            tot_rows += s_res[0]
        else:
            s_res = per_side[sides[0]]
            matched += 1
            tot_rows += s_res[0]
            slice_details.append({
                "index": sl.index,
                "predicate": sl.predicate,
                "match": True,
                "result": s_res
            })

    if divergent == 0:
        status = "ok"
        detail = f"{table}: OK rows={tot_rows} clean across {len(slices)} keyless slices ({mode})"
        cert = True
    else:
        status = "diff"
        detail = f"{table}: mismatch in {divergent} of {len(slices)} keyless slices ({mode})"
        cert = False

    return KeylessSliceResult(
        table=table,
        total_rows=tot_rows,
        slices_count=len(slices),
        matched_slices=matched,
        divergent_slices=divergent,
        status=status,
        detail=detail,
        certified_complete=cert,
        slice_details=slice_details
    )
