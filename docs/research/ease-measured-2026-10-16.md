# Address-only ease measurements

Run `python tools/ease_measure.py` while the local sandbox is running.

Each run starts with empty local configuration and state. Only two addresses and their passwords are supplied to init; no operator configuration is written. Generated configuration is not operator effort. Sandbox creation and seeding are outside the timer.

Profiles with distinct source and target roles are measured, including alternate server variants. Single-server and primary/standby profiles do not provide an independent pair. PostgreSQL/MySQL is also measured in both directions.

Time covers init, move and the complete check. Verified requires successful commands, a same verdict and exact fixture values at the target. Failed attempts have no verified time. Counts are transcript line counts: questions contain a question mark or prompt; action messages match commands, imperatives or requests for operator action. These are lexical counts, not a semantic claim. Raw output and credentials are never published.

| Pair / profile | Status | Verified seconds | Attempt seconds | Operator config lines | Questions | Action messages |
|---|---|---:|---:|---:|---:|---:|
| PostgreSQL (profile 0) to PostgreSQL (profile 0) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 5) to MySQL (profile 5) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 7) to MySQL (profile 7) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 9) to MySQL (profile 9) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 11) to MySQL (profile 11) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 14) to MySQL (profile 14) | not measured: local sandbox stopped | — | — | — | — | — |
| MongoDB (profile 16) to MongoDB (profile 16) | not measured: local sandbox stopped | — | — | — | — | — |
| redis (profile 18) to redis (profile 18) | not measured: local sandbox stopped | — | — | — | — | — |
| kafka (profile 21) to kafka (profile 21) | not measured: local sandbox stopped | — | — | — | — | — |
| kafka (profile 23) to kafka (profile 23) | not measured: local sandbox stopped | — | — | — | — | — |
| cassandra (profile 25) to cassandra (profile 25) | not measured: local sandbox stopped | — | — | — | — | — |
| clickhouse (profile 27) to clickhouse (profile 27) | not measured: local sandbox stopped | — | — | — | — | — |
| opensearch (profile 29) to opensearch (profile 29) | not measured: local sandbox stopped | — | — | — | — | — |
| MySQL (profile 5) to PostgreSQL (profile 0) | not measured: local sandbox stopped | — | — | — | — | — |
| PostgreSQL (profile 0) to MySQL (profile 5) | not measured: local sandbox stopped | — | — | — | — | — |
