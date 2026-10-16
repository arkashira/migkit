# 0001 - Where product telemetry is kept and where it may go

Date: 2026-10-16. Status: decided (lead, owner decision 2 of wave 17),
built in `migkit/telemetry.py`. Backlog: P0-PRODUCT-TELEMETRY
(`docs/backlog.md`, the item's acceptance criteria: "Choose storage later
against these requirements", "Do not embed a shared storage write token in
the client", "reject unknown fields and free-form values at the producer
and receiver", "On disable, stop queued sends and dispose of queued
outbound events"); the sandbox store's open items in the same file
(the `objectstore` profile: health flag, posix directory, `IfNoneMatch`).

This record is for the people who build migkit. It is not published with
the manual (`tools/gen_docs.py` writes `docs/site/` from the code and reads
nothing under `docs/decisions/`), and the manual names no store: it says
"an S3-compatible bucket you run".

## Context

Telemetry was built as a typed allowlist with a bounded local spool and no
destination. The open requirements were a durable local record, a way out
that the operator controls, a receiver that checks what it is given, and a
store that the public repository can recommend.

migkit already reaches one kind of remote store, once: the S3 API, in
`anchor.py` (the audit record's checkpoints in an Object Lock bucket,
conditional put, the store's own credential chain, egress kind `anchor`)
and in the run state's bucket rung (`shared.py`). The test sandbox already
has one S3-compatible profile (`tests/servers.py` `objectstore`). Adding a
server protocol of telemetry's own would be a second seam to secure, audit
and test.

## Options

| Option | Licence (read from its LICENSE file, 2026-10-16) | Why not, or why |
|---|---|---|
| A collector service of migkit's own | - | a second protocol, a service to run and secure, a write token to hand out; nothing the S3 seam does not already give |
| MinIO | GNU AGPL-3.0 (`github.com/minio/minio`, LICENSE) | its images left Docker Hub on 2026-09-11, so neither the sandbox nor an operator can pull a maintained image; ruled out |
| Garage | GNU AGPL-3.0 (`git.deuxfleurs.fr/Deuxfleurs/garage`, LICENSE) | a licence the public repository would have to carry into its docs to recommend it; ruled out |
| RustFS | Apache-2.0 (`github.com/rustfs/rustfs`, LICENSE; repository created 2023-11-23 per its hosting API) | young, with no production track record to cite; ruled out. The brief called it "a 2025 project": its repository is from 2023-11, the reason stands |
| VersityGW | Apache-2.0 (`github.com/versity/versitygw`, LICENSE) | the sandbox's store: proves the calls; a gateway over a file system, not the recommendation for production |
| SeaweedFS | Apache-2.0 (`github.com/seaweedfs/seaweedfs`, LICENSE; repository since 2014-07) | recommended for production: a long record, the licence the repository already carries, the same S3 calls |

## Decision

Local first, append-only, sealed; out only by hand, to an S3-compatible
bucket the operator runs beside migkit.

- On this machine (`telemetry.Local`): an open head file, sealed when it
  fills (or when a push starts) into `segments/segment-<seq>.seg`: a
  manifest line (`segment`, `seq`, `schema`, `count`, `first` and `last`
  hour, `prev`, `chain`, `digest`) and the events as kept. Each event line
  is hash-chained from the one before, the first segment's chain from a
  random seed made with the store (no install, host or person in it; gone
  when the store is disposed), so two stores that kept the same events
  never seal the same segment. The digest covers the chain's start and the
  bytes. Every write is synced (`durable`); the head is renamed before it
  is sealed, so a seal cut anywhere seals each event once. Retention: the
  organisation's policy (`telemetry_keep` segments, `telemetry_days`),
  else the spool's own bound (`FILES` files of `FILE_BYTES`).
- Out (`telemetry.push`, `migkit telemetry push <bucket>`): manual only -
  there is no on-run-end switch (lead decision 6). Refused, before any
  client is made or anything listed, where telemetry is off, the policy
  says `telemetry_push: off`, or the egress gate's `telemetry` kind does not
  let it out (`MIGKIT_EGRESS`, `MIGKIT_EGRESS_TO`, the air-gapped
  `MIGKIT_EGRESS=none`). The client is the anchor's (`anchor._client`): the
  store's own credential chain, so no key is in the hop, the policy or
  migkit. Each sealed segment not yet pushed goes, oldest first, under
  `<prefix>/telemetry/<digest>.seg` with `IfNoneMatch: *`; a key already
  there is read back and must hold the same bytes. A segment is marked
  pushed only after the bucket holds it, so a push cut anywhere resumes to
  one object per segment. Retries are the anchor's (`anchor._retry`,
  transient failures only); the bytes one push sends are bounded by the
  store's own bound.
- Back (`telemetry.receive`, `migkit telemetry receive <bucket>`): lists
  and reads every segment, refuses a segment whose chain breaks, whose
  bytes its digest does not name or whose name is not its digest, runs the
  producer's own `_check` on every event again (`telemetry.checked`: an
  unknown kind, field or schema and a free string are refused), and counts
  what it took by kind, engine and way - the analysis loop's first table.

The S3 operations migkit needs of a store, so any store answering them
qualifies: `PutObject` with `If-None-Match: *`, `GetObject`,
`ListObjectsV2` (with its continuation token); Object Lock is optional and
not asked for by telemetry (the anchor asks for it on its own bucket).

## Consequences

- One S3 seam and one egress kind for every remote store migkit writes.
- The operator owns the bucket, its access control and its retention;
  migkit never holds a write token. Tamper evidence is the chain and the
  digest (an edit is found at the receiver); a writer with the bucket's
  own credentials could forge a whole new segment, which the receiver
  still checks event by event - Object Lock on the operator's bucket is the
  defence against replacement, and is the operator's to turn on.
- Turning telemetry off deletes the head, the sealed segments, their
  push marks and the seed: pending pushes are dropped with them.

## Proof

- `tests/test_telemetry_segments_are_sealed_pushed_once_and_received_checked.py`
  on the sandbox's `objectstore` profile (VersityGW): the health path
  (`/health`) answers 200; a second `PutObject` with `If-None-Match: *`
  answers `PreconditionFailed` and leaves the first object; a run full of
  canaries is sealed and pushed, pushed again and pushed with its marks
  lost, and the bucket holds one object per segment, with no canary in it
  or in what `show`, `push` and `receive` print; a push cut after its put
  resumes to one object; an edited segment fails its chain at `receive`
  and a forged one has its out-of-schema events refused there.
- `tests/test_telemetry_carries_no_customer_data.py`: the store's bound,
  sealing, a cut seal, retention by count and age, the push refused (egress
  none, a host the egress does not name, policy off, `telemetry_push: off`)
  with nothing connected, `telemetry off` disposing segments, the
  receiver's check, a blocked writer dropping and counting while the step
  never waits, and the writer's synced bytes a second.
- Not proved: Object Lock on the sandbox's store (a plain bucket answers
  `ObjectLockConfigurationNotFoundError`; the anchor's own tests keep
  their stand-in); SeaweedFS itself (the record recommends it by its S3
  calls, not by a run here); the analysis beyond the first table.
