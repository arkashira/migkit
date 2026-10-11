"""BigQuery followed as a source: the rows that changed in a dataset, read
from its tables' change history (`CHANGES()`) and handed on as the net state
of each changed row.

BigQuery keeps no log a client reads. What it keeps, for a table whose
`enable_change_history` was on, is which rows were inserted, updated or
deleted in a span of time, and the table as it stood at any instant inside
its time travel window. A read of changes built from those two:

* a **window** `(S, E]` is read whole: the keys that changed in it
  (`CHANGES(TABLE t, S, E)`), and for each, the row as the table held it at
  `E` (`FOR SYSTEM_TIME AS OF E`), joined in one query. A key with a row at
  `E` is handed on as that row, a key with none as a delete. The net state
  at `E` is what the target should hold; what an update row of `CHANGES()`
  says of old and new images is never read, so nothing depends on it.
* every table of a window is read as of the same `E`, so the window is one
  snapshot of the dataset: a transaction across tables is never half in it.
* the position is the end of the last window delivered (`{"at": ...}`), or,
  while a window is being handed on in parts, the window and how far each
  table is through it. Reading from a position again gives the same
  changes: a window is a function of its two ends.
* a window ends at least ten minutes ago (BigQuery's own floor for
  `CHANGES()`) and spans at most a day, so the target is never closer than
  ten minutes to the source, and a fence or a drain waits that long.

A table to be followed has a declared key (a change is applied by its key)
and has had change history on since before the copy began; migkit does not
turn it on on a source, and a tail is refused, naming the statement, where a
table lacks either. Nothing here has run against BigQuery: the calls are
the documented ones, held by a stand-in client, and the cells they serve
are in `capabilities.UNPROVED`.
"""
import datetime
import itertools

from .. import canon

#: seconds an end of `CHANGES()` must be behind now, and one spare minute
CHANGES_FLOOR = 10 * 60 + 60
#: seconds a window spans at most (BigQuery allows a day), less an hour
CHANGES_SPAN = 23 * 3600

UTC = datetime.timezone.utc


def _stamp(at):
    """A timestamp as the position keeps it."""
    return at.astimezone(UTC).isoformat(timespec="microseconds")


def _read(text):
    return datetime.datetime.fromisoformat(str(text)).astimezone(UTC)


class BigQueryChanges:
    """The BigQuery engine's change reader (a mixin of
    `warehouse.BigQueryEngine`)."""

    #: reading from a position does not move it: a table that left a tail
    #: and returns is read again from where it left, and a verify reads
    #: the changes without consuming them
    REREADS_FROM_A_POSITION = True
    CHANGE_POINT_READS_ONLY = True
    #: a position says where to read on from
    READS_AHEAD = False

    # ---- where the changes are read from ---------------------------------------

    def _now(self, side):
        """BigQuery's own clock, as a datetime: positions are its time, never
        this machine's."""
        got = list(self._client(side).query(
            "select current_timestamp()").result())
        return list(got[0].values())[0].astimezone(UTC)

    def _history(self, side, db):
        """{table: whether change history is on}, from the dataset's table
        options."""
        out = {}
        for table, value in self._rows(
                side, db, "select table_name, option_value from"
                          f" {self._catalog(side, db, 'table_options')}"
                          " where option_name = 'enable_change_history'"):
            out[str(table)] = str(value).strip().lower() == "true"
        return out

    def tail_blockers(self, side, db):
        """[words] for every table the hop takes that cannot be followed:
        one with no declared key, and one whose change history is off - with
        the statement that turns it on, which is the source's owner's to
        run."""
        history = self._history(side, db)
        out = []
        for t in self.neutral_tables(side, db):
            key = self.neutral_key(side, db, t)
            if not key:
                out.append(f"{t} has no declared key, and a change is"
                           " applied by its key")
            else:
                rules = self.neutral_column_rules(side, db, t)
                nullable = [k for k in key if rules.get(k, {}).get("null")]
                if nullable:
                    out.append(f"{t}: its key column {', '.join(nullable)}"
                               " takes NULL, and a changed row is found by"
                               " the key's value (declare it REQUIRED)")
            if not history.get(t):
                out.append(
                    f"{t} keeps no change history: `alter table "
                    f"{self._qualified(side, db, t)} set options("
                    "enable_change_history = true)` turns it on, before the"
                    " copy a tail follows")
        return out

    def _refuse_unfollowable(self, side, db):
        blocked = self.tail_blockers(side, db)
        if blocked:
            raise SystemExit(
                f"{db} cannot be followed: " + "; ".join(blocked[:4])
                + (f"; and {len(blocked) - 4} more" if len(blocked) > 4
                   else ""))

    def change_point(self, side, db):
        """Now, by BigQuery's clock - every change after it is read once
        its window is ten minutes old. Refused for a dataset with a table
        that cannot be followed."""
        self._refuse_unfollowable(side, db)
        return {"at": _stamp(self._now(side))}

    def log_position(self, side, db):
        return {"at": _stamp(self._now(side))}

    @staticmethod
    def position_reached(have, want):
        """Whether the windows delivered end at or after where a read of
        "now" put the log."""
        try:
            return _read(have["at"]) >= _read(want["at"])
        except (KeyError, TypeError, ValueError):
            return None

    def stream_identity(self, side, db):
        return {"dataset": self.side_database(side, db)}

    #: seconds the dataset's time travel is trusted before it is asked again
    TRAVEL_ASKED_EVERY = 300

    def _travel(self, side, db):
        """Hours of change history the dataset keeps: its time travel."""
        import time
        kept = self.__dict__.get("_travel_of")
        if kept is None or kept[1] != (side, db) or \
                time.monotonic() - kept[0] > self.TRAVEL_ASKED_EVERY:
            hours = self._client(side).get_dataset(
                self.side_database(side, db)).max_time_travel_hours or 168
            kept = self.__dict__["_travel_of"] = (time.monotonic(),
                                                  (side, db), hours)
        return kept[2]

    def position_lost(self, side, db, token, now=None):
        """Why the dataset's history no longer holds a position: it is older
        than the time travel window, which is all the history there is."""
        try:
            at = _read((token or {}).get("at"))
        except (TypeError, ValueError):
            return None
        hours = self._travel(side, db)
        if (now or self._now(side)) - at > datetime.timedelta(hours=hours):
            return (f"the position is older than the dataset's time travel"
                    f" ({hours} hours), which is all the change history it"
                    " keeps")
        return None

    def stream_room(self, side, db, token):
        """Seconds left before the dataset's time travel forgets what a tail
        at `token` has not read."""
        try:
            behind = (self._now(side) - _read(token["at"])).total_seconds()
        except (KeyError, TypeError, ValueError):
            return None
        return {"seconds": max(0, int(self._travel(side, db) * 3600
                                      - behind))}

    def snapshot_mark(self, side, db):
        """What BigQuery has made visible by now: its clock. A change is the
        net state at the end of its window, which a read after the mark
        shows if the window ended before it."""
        return _stamp(self._now(side))

    def mark_covers(self, side, db, mark, change):
        at = change.get("txn")
        if not mark or not at:
            return None
        try:
            return _read(at) <= _read(mark)
        except ValueError:
            return None

    def table_oids(self, side, db):
        """{table: when it was created}: a name given to another table (a
        rename away and another renamed in) changes it, and the log says
        nothing of a rename."""
        return {str(t): str(created) for t, created in self._rows(
            side, db, "select table_name, creation_time from"
                      f" {self._catalog(side, db, 'tables')} where"
                      " table_type = 'BASE TABLE'")
            if not self._own(str(t)) and self.in_scope(db, str(t))}

    def release_changes(self):
        """The windows held open between two reads, let go."""
        self.__dict__.pop("_windows", None)

    # ---- one window of one table -------------------------------------------------

    def _window(self, side, db, table, key, start, end, skip):
        """An iterator over the rows of the window `(start, end]` of one
        table from its `skip`th on: for each key that changed, the key and
        the row as the table held it at `end` - `_migkit_here` null where
        it held none. One query a window, kept for the next read; asked
        again, and gone through to `skip`, after a stop."""
        held = self.__dict__.setdefault("_windows", {})
        at = (side, db, table, _stamp(start), _stamp(end))
        have = held.get(at)
        if have is None or have[1] != skip:
            tbl = self._qualified(side, db, table)
            keys = ", ".join(self._q(k) for k in key)
            # a key's columns take no NULL (`tail_blockers`), so an equality
            # is a join, and BigQuery runs it as one
            join = " and ".join(f"ch.{self._q(k)} = cur.{self._q(k)}"
                                for k in key)
            sql = (f"with cur as (select *, true as _migkit_here from {tbl}"
                   f" for system_time as of timestamp "
                   f"{self._lit(_stamp(end))}), ch as (select distinct "
                   f"{keys} from changes(table {tbl}, timestamp "
                   f"{self._lit(_stamp(start))}, timestamp "
                   f"{self._lit(_stamp(end))})) select "
                   + ", ".join(f"ch.{self._q(k)} as _migkit_k{i}"
                               for i, k in enumerate(key))
                   + ", cur.* from ch left join cur on " + join
                   + " order by " + ", ".join(f"ch.{self._q(k)}"
                                              for k in key))
            rows = iter(self._client(side).query(sql).result())
            have = [itertools.islice(rows, skip, None), skip]
            held[at] = have
        return have

    def _changes_of(self, side, db, table, key, classed, end, rows):
        """Change records for window rows: an update of the row as it stood
        (the target upserts by key), a delete of a key it had no row at."""
        names = [n for n, _ in classed]
        out = []
        for r in rows:
            ident = {k: r.get(f"_migkit_k{i}") for i, k in enumerate(key)}
            if r.get("_migkit_here"):
                values = self._fix_read(side, db, table, classed,
                                        [[r.get(n) for n in names]])[0]
                out.append(canon.change("update", table, ident,
                                        dict(zip(names, values)),
                                        txn=_stamp(end)))
            else:
                out.append(canon.change("delete", table, ident,
                                        txn=_stamp(end)))
        return out

    #: the tables followed, their keys and columns, kept this many seconds
    FOLLOWED_FOR = 60
    SHAPE_CACHES = ("_column_rules_of", "_ordered_of", "_followed_of",
                    "_travel_of")

    def _followed(self, side, db):
        """[(table, key, [(column, class)])] of the tables the hop takes,
        asked at most every `FOLLOWED_FOR` seconds - a poll is made every
        few - and again after a DDL (`forget_shapes`)."""
        import time
        kept = self.__dict__.get("_followed_of")
        if kept is not None and time.monotonic() - kept[0] < \
                self.FOLLOWED_FOR and kept[1] == (side, db):
            return kept[2]
        got = [(t, self.neutral_key(side, db, t),
                [(n, canon.type_class("bigquery", ty))
                 for n, ty in self.neutral_columns(side, db, t)])
               for t in self.neutral_tables(side, db)]
        self.__dict__["_followed_of"] = (time.monotonic(), (side, db), got)
        return got

    # ---- the changes ----------------------------------------------------------------

    def neutral_changes(self, side, db, token=None, limit=1000):
        """(changes, position): the net state, at the end of each window
        read, of every row that changed in it - windows following one
        another until `limit` changes are held or the newest window that
        may be read has been (`CHANGES_FLOOR`). A position inside a window
        names the window and how far each table is through it."""
        if token is None:
            return [], self.change_point(side, db)
        now = self._now(side)
        lost = self.position_lost(side, db, token, now)
        if lost:
            raise SystemExit(f"{db}: {lost}")
        floor = now - datetime.timedelta(seconds=CHANGES_FLOOR)
        tables = self._followed(side, db)
        start = _read(token["at"])
        end = _read(token["end"]) if token.get("end") else None
        done = dict(token.get("tables") or {})
        out = []
        while True:
            if end is None:
                end = min(start + datetime.timedelta(seconds=CHANGES_SPAN),
                          floor)
                if end <= start:
                    break
                done = {}
            for table, key, classed in tables:
                if done.get(table) == -1 or len(out) >= limit:
                    continue
                if not key:
                    raise SystemExit(f"{table} has no declared key: it "
                                     "cannot be followed")
                skip = done.get(table, 0)
                have = self._window(side, db, table, key, start, end, skip)
                rows = list(itertools.islice(have[0], limit - len(out)))
                have[1] += len(rows)
                out += self._changes_of(side, db, table, key, classed, end,
                                        rows)
                if len(out) < limit:
                    # the iterator ran out before the limit did: finished
                    done[table] = -1
                    self.__dict__.get("_windows", {}).pop(
                        (side, db, table, _stamp(start), _stamp(end)), None)
                else:
                    done[table] = have[1]
            if all(done.get(t) == -1 for t, _, _ in tables):
                start, end, done = end, None, {}
                if len(out) >= limit or start >= floor:
                    break
            else:
                return out, {"at": _stamp(start), "end": _stamp(end),
                             "tables": done}
        return out, {"at": _stamp(start)}

    # ---- the pair's tail, which reads these changes ----------------------------------

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_seed(self, db, token_path, point):
        return self._as_pair().tail_seed(db, token_path, point)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def _tail_items(self):
        """What `assess` says of following this dataset: how far behind the
        target is, and the tables that cannot be followed."""
        items = [{"level": "info", "scope": "source",
                  "item": "changes followed through change history",
                  "detail": "a change is read once its window is ten"
                            " minutes old, as the net state of its row at"
                            " the window's end: the target is never closer"
                            " than ten minutes behind, and a fence or a"
                            " drain waits that long"}]
        for db in self.databases():
            try:
                blocked = self.tail_blockers("src", db)
            except Exception as e:  # noqa: BLE001 - unknown, never clean
                items.append({
                    "level": "warn", "scope": db,
                    "item": "tables that can be followed",
                    "detail": "could not be read: "
                              f"{(str(e).splitlines() or [''])[-1][:90]}"})
                continue
            if blocked:
                items.append({"level": "warn", "scope": db,
                              "item": "tables that cannot be followed",
                              "detail": "; ".join(blocked[:4])})
        return items
