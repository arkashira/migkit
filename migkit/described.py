"""What a table migkit made on a store that keeps no column types is: its
columns' classes and its key, kept beside the store's endpoint on this
machine.

A Bigtable table holds bytes, a Firestore or Datastore document whatever
each field was given, a Timestream table whatever its records carry - none
of them says that a column was a decimal of two places or a date. A table
another engine's rows were copied into is read back through what it was
made from: each column by its class, its key from the key the store
files the row under. One made by anything else is described by the store
itself, as the engine reads it.
"""
import hashlib
import json
import os
from pathlib import Path


def _path(engine, where):
    from . import config
    d = Path(config.REPORTS) / f"_{engine}"
    d.mkdir(parents=True, exist_ok=True)
    return d / (hashlib.sha1(str(where).encode(), usedforsecurity=False)
                .hexdigest()[:16] + ".json")


def read(engine, where, name):
    """([(column, class)], [key column]) of a table migkit made, or None."""
    try:
        got = json.loads(_path(engine, where).read_text()).get(name)
    except (OSError, ValueError):
        got = None
    if not got:
        return None
    return [tuple(c) for c in got["columns"]], list(got["key"])


def keep(engine, where, name, columns, key):
    """Keep a table's description, the file replaced whole: a run stopped
    half way leaves the one before it, never half of one."""
    path = _path(engine, where)
    try:
        kept = json.loads(path.read_text())
    except (OSError, ValueError):
        kept = {}
    kept[name] = {"columns": [list(c) for c in columns], "key": list(key)}
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        f.write(json.dumps(kept, indent=1))
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def forget(engine, where, name):
    path = _path(engine, where)
    try:
        kept = json.loads(path.read_text())
    except (OSError, ValueError):
        return
    if kept.pop(name, None) is not None:
        path.write_text(json.dumps(kept, indent=1))


def key_text(classes, key, values):
    """The text a store files a row of a described table under: its key's
    value's canonical text, or several as one JSON array of them - which
    the store orders as it orders any text."""
    from . import canon
    texts = [canon.render_value(classes.get(k) or "text", values[k])
             for k in key]
    return texts[0] if len(texts) == 1 else json.dumps(texts)


def key_values(classes, key, text):
    """{key column: value} back from `key_text`."""
    from . import changetopic
    texts = [text] if len(key) == 1 else json.loads(text)
    return {k: value_of(classes.get(k) or "text", t)
            for k, t in zip(key, texts)}


def cell_of(cls, value):
    """A value as the bytes a store keeps for a described column: bytes as
    they are, text as UTF-8, JSON as its canonical text, anything else as
    its canonical text."""
    from . import canon
    if value is None:
        return None
    if cls == "bytes":
        return bytes(value)
    if cls in ("json", canon.JSON_ONE) and not isinstance(value, str):
        return canon.json_text(value).encode("utf-8")
    return str(canon.render_value(cls or "text", value)
               if cls not in ("text",) else value).encode("utf-8")


def value_of(cls, raw):
    """The value back out of `cell_of`'s bytes (or its text)."""
    from decimal import Decimal

    from . import changetopic
    if raw is None:
        return None
    if cls == "bytes":
        return bytes(raw)
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) \
        else str(raw)
    if cls == "json":
        return json.loads(text, parse_float=Decimal)
    if cls == "uuid":
        return text
    return changetopic.from_text(cls or "text", text)
