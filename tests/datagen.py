"""Faker-driven data generator covering every column shape migkit must
verify: integers, big text, jsonb, uuid, timestamps, bytea, nulls, unicode
and emoji, plus deliberately non-contiguous primary keys.
"""
import json

from faker import Faker

fake = Faker()
Faker.seed(42)

PG_DDL = """
drop table if exists people;
create table people (
    id          bigint primary key,
    name        text,
    email       varchar(120),
    age         int,
    balance     numeric(14,2),
    active      boolean,
    tags        jsonb,
    ref         uuid,
    payload     bytea,
    note        text,
    created_at  timestamptz
)
"""


def _row(i):
    unicode_note = fake.random_element([
        fake.text(80), "emoji 🚀🔥 test", "ไทย unicode ทดสอบ",
        "quote ' and \" and \\ backslash", None, "",
        "null-ish \x00 nope",
    ])
    if unicode_note == "null-ish \x00 nope":
        unicode_note = "control chars stripped"
    return {
        "id": i,
        "name": fake.name(),
        "email": fake.random_element([fake.email(), None]),
        "age": fake.random_int(0, 120),
        "balance": round(fake.pyfloat(min_value=-99999, max_value=999999), 2),
        "active": fake.boolean(),
        "tags": json.dumps({"a": fake.word(), "n": fake.random_int(),
                            "list": [fake.word() for _ in range(3)]}),
        "ref": str(fake.uuid4()),
        "payload": fake.binary(length=16).hex(),
        "note": unicode_note,
        "created_at": fake.date_time_this_decade().isoformat(),
    }


def rows(n, skip_ids=()):
    """Generate n rows with non-contiguous ids (some ids deleted)."""
    out = []
    i = 1
    while len(out) < n:
        if i not in skip_ids:
            out.append(_row(i))
        i += 1
    return out


def pg_copy_body(rows):
    """CSV body for \\copy, matching PG_DDL column order."""
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        w.writerow(["" if r[c] is None else r[c] for c in
                    ("id", "name", "email", "age", "balance", "active",
                     "tags", "ref", "payload", "note", "created_at")])
    return buf.getvalue()
