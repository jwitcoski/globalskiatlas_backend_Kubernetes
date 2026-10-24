#!/bin/sh
while true; do
  WRITE_STATUS=1 python scripts/run_one_resort.py >/dev/null 2>&1 || true
  sleep 60
done &
# One region per pass. run_one_resort.py deletes that region's scratch files
# before this loop starts the next region.
while true; do
  python scripts/run_one_resort.py || echo "state update failed"
done
