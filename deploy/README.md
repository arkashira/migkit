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
still going round its loop, and whether it stopped on an error.
`prometheus-alerts.yml` here has rules over those metrics; load it with
`rule_files:` and change the thresholds to fit.

To be told without Prometheus, give the hop receivers:

    options:
      notify:
        - https://hooks.slack.com/services/...    # or Discord, a Teams workflow, any JSON URL
        - pagerduty:<routing key>                 # one incident per hop, resolved when it clears

or set `MIGKIT_NOTIFY` (addresses separated by commas) for every hop. A check
sends when its verdict moves between same, different and error, and a tail
sends when it stops on an error and when it runs again. What is sent names
each finding's check and table, never the rows' values.
