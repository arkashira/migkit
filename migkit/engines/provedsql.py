"""A digest worked out by a server that speaks PostgreSQL's or MySQL's SQL
without being either - GaussDB, OceanBase, PolarDB-X, TDSQL - trusted on a
table only once the table's own first rows are proved to render there as
they render in this process.

The renderings in `canon` were measured on PostgreSQL and MySQL. On a
server that only speaks their SQL, a function can differ - or be missing
in a way that answers NULL for every row, which sums to 0 on both sides
and would call any two tables equal. So before a table's digest is asked
of the server, its first rows (`PROOF_ROWS`, in key order) are read as the
engine reads them and rendered here, and the server is asked for its text
of the same rows: they must agree, row for row, and there must be rows. A
table they do not agree on, an empty one, one without a key to read the
same rows by, or one whose SQL the server refuses, is folded in this
process - the same number the slow way, never another one.
"""
from ..decide import Need, Rung


class ProvedDigest:
    #: the dialect of `canon`'s renderings the server is asked in
    PROVED_DIALECT = None
    PROOF_ROWS = 64
    #: where a table's digest is worked out
    PROVED_WAYS = (
        Rung("server", "the digest worked out by the server in its own SQL",
             needs=(Need("every column rendered in its SQL",
                         lambda f: f["rendered"],
                         "a column has no rendering in the server's SQL"),),
             prove=lambda f: None if f["proved"] else
             "the server does not render the table's first rows as migkit"
             " does"),
        Rung("here", "the rows read and folded here"))

    def _proved_rows(self, side, db, sql):
        """[[value]] the server answers `sql` with."""
        raise NotImplementedError

    def _proved_table(self, side, db, table):
        """The table as the server's SQL names it."""
        raise NotImplementedError

    def _proved_quote(self, name):
        raise NotImplementedError

    def _proved(self, side, db, table, columns, where=None):
        """Whether the server renders this table's first rows as this
        process does; asked once a table, side and set of columns."""
        from .. import canon, rowtext
        known = self.__dict__.setdefault("_proofs", {})
        at = (side, db, str(table), tuple(columns), where)
        if at in known:
            return known[at]
        key = self.neutral_key(side, db, table)
        if not key:
            known[at] = False
            return False
        names = [n for n, _ in columns]
        try:
            rows, _ = self.neutral_read(side, db, table, columns, None,
                                        self.PROOF_ROWS, where)
            # canon's renderings quote each name as their dialect does
            text = canon.row_expr(self.PROVED_DIALECT, list(columns))
            texts = self._proved_rows(
                side, db, f"select {text} from"
                          f" {self._proved_table(side, db, table)}"
                          + (f" where ({where})" if where else "")
                          + " order by "
                          + ", ".join(self._proved_quote(k) for k in key)
                          + f" limit {self.PROOF_ROWS}")
        except Exception:  # noqa: BLE001 - refused: folded here
            known[at] = False
            return False
        classes = [c for _, c in columns]
        ok = bool(rows) and len(rows) == len(texts) and all(
            rowtext.encode([canon.render_value(c, v) for c, v in
                            zip(classes, row)]) == got[0]
            for row, got in zip(rows, texts))
        if rows:
            # an empty table proves nothing, and is asked again once it
            # holds rows
            known[at] = ok
        return ok

    def neutral_digest(self, side, db, table, columns, where=None):
        """(rows, digest): the server's, where every column is rendered in
        its SQL and the rendering is proved on the table's first rows;
        folded here otherwise, over rows read in batches."""
        from .. import canon, decide
        rendered = not any(canon.folded_here(c) for _, c in columns)

        def run(choice):
            if choice.rung.name == "here":
                self.folded_last = True
                return canon.fold_batches(columns, self.neutral_batches(
                    side, db, table, columns, 5000, where))
            row = canon.row_expr(self.PROVED_DIALECT, list(columns))
            got = self._proved_rows(
                side, db, f"select count(*),"
                          f" {canon.digest_expr(self.PROVED_DIALECT, row)}"
                          f" from {self._proved_table(side, db, table)}"
                          + (f" where ({where})" if where else ""))
            if not got:
                raise decide.Blocked("the server answered no digest")
            self.folded_last = False
            return int(got[0][0]), str(got[0][1])
        return self._attempt(
            str(table), self.PROVED_WAYS, run, decide.Facts(
                {"rendered": rendered},
                proved=lambda: rendered and self._proved(
                    side, db, table, columns, where)))[1]
