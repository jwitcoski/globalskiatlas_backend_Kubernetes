#!/bin/sh
while true; do
  WRITE_STATUS=1 python scripts/run_one_resort.py >/dev/null 2>&1 || true
  sleep 60
done &
while true; do
  start=$(date +%s)
  python scripts/run_one_resort.py || echo "state update failed"
  elapsed=$(( $(date +%s) - start ))
  if [ "$elapsed" -lt 1800 ]; then
    sleep $((1800 - elapsed))
  fi
done
