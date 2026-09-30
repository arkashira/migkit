# Running migkit as a migration service on a VM

migkit can stand in for a managed migration service (DMS/DTS) on a trusted VM:
full load once, then continuously verify (and repair) the incremental stream.

`sync --mode`:
- `verify` - read-only, consistent-snapshot check (exit 1 on diff)
- `seed` - align schema -> bulk load (best mover) -> reconcile rows/sequences -> verify
- `stream` - start/continue CDC, then delta-verify each cycle (O(changes))
- `migrate` - full, then incremental (the DMS "full load + CDC")

Add `--go` to execute (dry-run otherwise), `--serve` to loop forever.

## docker
    cp conf/hops.example.yaml deploy/conf/hops.yaml   # fill endpoints, chmod 600
    docker compose -f deploy/docker-compose.yml up -d --build
    # dashboard: http://<vm>:8899

## systemd
    sudo cp -r . /opt/migkit && /opt/migkit/bootstrap.sh
    sudo install -Dm600 conf/hops.yaml /etc/migkit/hops.yaml
    sudo cp deploy/migkit-sync@.service /etc/systemd/system/
    sudo systemctl enable --now migkit-sync@my-hop

State (`/state` or `/var/lib/migkit`) holds resume checkpoints and reports, so
a restart continues where it stopped. The source is only ever read; the target
gets the migrated data and nothing else.

For continuous CDC, run `migkit move <hop> --mode cdc --go` and let migkit
verify the stream. Where an engine has no native change feed, migkit stands
up and supervises its own streaming pipeline behind the same command.

## Alerts and notifications

`/metrics` on the dashboard (`migkit report --serve`) carries each hop's last
verdict and, for a running change tail, how far behind it is, whether it is
still going round its loop, and whether it stopped on an error - and what
works inside migkit, under migkit's names: the copy's rows per table and
rows a second, each check's seconds, the verify's progress, every table's
rows on each side and whether they agree, the last cutover's outage and
phases, the change stream's parts and its reader's own readings.
`prometheus-alerts.yml` here has rules over those metrics; load it with
`rule_files:` and change the thresholds to fit. `grafana/migkit.json` is a
dashboard over the same metrics (import it and pick the Prometheus source).

With `OTEL_EXPORTER_OTLP_ENDPOINT` set (the standard OpenTelemetry
variables), every command is a trace: a span per command, per check of each
database, per table copied and per cutover phase, with the ways the engine
chose as events.

To be told without Prometheus, give the hop receivers, kept where its
passwords are kept:

    options:
      notify:
        to:
          - env:SLACK_HOOK          # Slack, Teams, Google Chat, Discord, LINE,
          - keyring:migkit-lark     # Lark/Feishu, Telegram, email, SNS, ...
          - pagerduty:env:PD_KEY    # one incident per hop, resolved when it clears
        events: [verdict, drift, lag, cutover, approval, failure, fallback, done]
        lag: 300                    # seconds a change tail may be behind

or set `MIGKIT_NOTIFY` (addresses separated by commas) for every hop. What
is sent names each finding's check and table, never the rows' values.

## In the team's pipelines

- GitHub Actions: `uses: arkashira/migkit@v1` with `hop:` (and `command:`,
  `args:`, `config:`); the job is gated on the verdict, and the run's
  evidence is its artefact and summary (`action.yml`).
- Argo Workflows: `argo/migkit-workflowtemplate.yaml`, the image above, the
  verdict an output parameter and the evidence an output artefact.
- Airflow: `from migkit.integrations import MigkitOperator`; Dagster:
  `migkit.integrations.migkit_op(name, command, hop)`.

Each runs the command a person would type, and nothing else.
