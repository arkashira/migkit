# Security policy

## Reporting a vulnerability

Please report security issues privately, not in a public issue. Use GitHub's
private vulnerability reporting on this repository (the **Security** tab ->
**Report a vulnerability**). You will get an acknowledgement and a fix timeline.

## Scope and design

migkit is built to be safe by default:

- **Read-only by default.** `check`, `assess`, `watch`, `report`, and `history`
  never write to either side. Anything that writes to the target (`sync`,
  `move`, `schema --convert`) is dry-run until `--apply` / `--go`, saves an undo
  first, and is recorded in a local changelog.
- **The source is never written**, and nothing of migkit's own is written into
  the target beyond the migrated data itself.
- **Credentials are never stored in plaintext in the repo.** `conf/hops.yaml` is
  gitignored; secrets resolve at load time from `env:`, `file:`, or `vault:`
  references. Prohibited credential operations are refused rather than handled.
- **Nothing leaves the machine unless configured, and then through one gate.**
  Notifications, traces, the model asked for help and the evidence's digests
  are cleaned of every secret and sent only where the environment's
  `MIGKIT_EGRESS` and the hop's `egress` allow; `MIGKIT_EGRESS=none` sends
  nothing and tries nothing. See `docs/threat-model.md`, "What leaves the
  machine".
- **The dashboard** listens on 127.0.0.1, opens once for the address it
  prints, believes a signing-in proxy only with its secret, and serves
  `/metrics` only for a bearer token or a signed-in person.

When reporting, please note the engine, the command run, and whether any target
or source data could have been affected.

## Supported versions

migkit is pre-1.0; fixes land on `master`. Please test against the latest
`master` before reporting.
