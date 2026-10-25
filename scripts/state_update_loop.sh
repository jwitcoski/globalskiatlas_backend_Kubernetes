#!/bin/sh
while true; do
  WRITE_STATUS=1 python scripts/run_one_resort.py >/dev/null 2>&1 || true
  sleep 60
done &
# Wipe scratch before every pass. run_one_resort.py holds resort-jobs/runner.flock
# for the whole pass, so the nightly add waits until the region sweep exits.
clear_region_work() {
  rm -rf /tmp/one_resort /app/cache/game_export_dems /app/cache/skadi
  mkdir -p /tmp/one_resort /app/cache/game_export_dems /app/cache/skadi
}
while true; do
  clear_region_work
  python scripts/run_one_resort.py || echo "state update failed"
done
