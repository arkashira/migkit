#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
hop="${1:?usage: run-all.sh <hop> [db ...]}"
shift || true

overall=0
for step in check-schema check-counts check-sequences check-data-fast; do
  echo "== $step =="
  ./"$step".sh "$hop" "$@" || overall=1
done

echo "== result =="
if [ $overall = 0 ]; then
  echo "$hop: all checks passed"
else
  echo "$hop: DIFF found, see reports/$hop/"
  echo "fixes: fix-sequences.sh $hop | fix-data.sh $hop <db> <table> [--full]"
fi
exit $overall
