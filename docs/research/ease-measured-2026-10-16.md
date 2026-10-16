# Address-only ease measurements

Run `python tools/ease_measure.py` while the local sandbox is running.

Each run starts with empty local configuration and state. Only two addresses and their passwords are supplied to init; no operator configuration is written. Generated configuration is not operator effort. Sandbox creation and seeding are outside the timer.

Profiles with distinct source and target roles are measured, including alternate server variants. Single-server and primary/standby profiles do not provide an independent pair. PostgreSQL/MySQL is also measured in both directions.

Time covers init, move and the complete check. Verified requires successful commands, a same verdict and exact fixture values at the target. Failed attempts have no verified time. Counts are transcript line counts: questions contain a question mark or prompt; action messages match commands, imperatives or requests for operator action. These are lexical counts, not a semantic claim. Raw output and credentials are never published.

| Pair / profile | Status | Verified seconds | Attempt seconds | Operator config lines | Questions | Action messages |
|---|---|---:|---:|---:|---:|---:|
| PostgreSQL to PostgreSQL, migrate from a minimal hop file | verified | 13.81 | 13.81 | — | 0 | — |
| MySQL to MySQL, migrate from a minimal hop file | verified | 24.18 | 24.18 | — | 0 | — |
