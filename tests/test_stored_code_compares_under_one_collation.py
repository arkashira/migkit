"""Stored code that compares strings under two collations on the target
is named before the move (problems B4).

Pure: the server's answers are given in-process.
"""


def test_stored_code_that_compares_under_two_collations_is_named():
    """A routine recreated in the target database compares its variables
    and literals under that database's collation; a text column it names
    that keeps another, or a COLLATE clause in it naming another, is
    `Illegal mix of collations` when it runs (problems B4). Read from the
    body's own words - a name in a comment or a string is not one."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(Hop(name="h", engine="mysql", source=Endpoint(),
                          target=Endpoint()))

    def q(side, sql, args=None):
        if "information_schema.columns" in sql:
            return [("customers", "email", "utf8mb4_general_ci"),
                    ("customers", "city", "utf8mb4_0900_ai_ci"),
                    ("notes", "body", "utf8mb4_general_ci")]
        return [("PROCEDURE find_by_email",
                 "BEGIN SELECT id FROM customers WHERE email = p; END"),
                ("PROCEDURE by_city",
                 "BEGIN -- body of the note\n SELECT id FROM customers"
                 " WHERE city = p AND 'body' <> ''; END"),
                ("FUNCTION pinned",
                 "RETURN p COLLATE utf8mb4_bin")]
    eng._q = q
    got = eng._code_collation_mix("shop", "utf8mb4_0900_ai_ci")
    assert got[0]["level"] == "warn"
    said = got[0]["detail"]
    assert "find_by_email: customers.email (utf8mb4_general_ci)" in said
    assert "by_city" not in said
    assert "pinned: COLLATE utf8mb4_bin" in said
    assert "2 of 3" in said
