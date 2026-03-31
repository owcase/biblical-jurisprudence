#!/usr/bin/env bash
# Biblical Jurisprudence — Multi-state ingestion runner
#
# Usage:
#   ./run_states.sh              # seed courts then ingest all target courts
#   ./run_states.sh --skip-seed  # skip seeding (courts already in DB)
#   ./run_states.sh --dry-run    # search only, no writes
#
# Runs sequentially to respect CourtListener rate limits.
# Progress is logged to logs/ingest_YYYYMMDD_HHMMSS.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/.venv/bin/python"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/ingest_$(date +%Y%m%d_%H%M%S).log"
COURT_IDS_FILE="$SCRIPT_DIR/.court_ids.txt"

SKIP_SEED=false
DRY_RUN=""

for arg in "$@"; do
  case $arg in
    --skip-seed) SKIP_SEED=true ;;
    --dry-run)   DRY_RUN="--dry-run" ;;
  esac
done

mkdir -p "$LOG_DIR"

echo "=====================================" | tee -a "$LOG_FILE"
echo "Biblical Jurisprudence — State Ingest" | tee -a "$LOG_FILE"
echo "Started: $(date)"                       | tee -a "$LOG_FILE"
echo "=====================================" | tee -a "$LOG_FILE"

# Step 1: Seed courts
if [ "$SKIP_SEED" = false ]; then
  echo "" | tee -a "$LOG_FILE"
  echo "--- Seeding courts ---" | tee -a "$LOG_FILE"
  "$PYTHON" "$SCRIPT_DIR/seed_courts.py" 2>&1 | tee -a "$LOG_FILE"
fi

# Step 2: Read court IDs
if [ ! -f "$COURT_IDS_FILE" ]; then
  echo "Error: $COURT_IDS_FILE not found. Run seed_courts.py first." | tee -a "$LOG_FILE"
  exit 1
fi

COURTS=()
while IFS= read -r line || [ -n "$line" ]; do
  [ -n "$line" ] && COURTS+=("$line")
done < "$COURT_IDS_FILE"
TOTAL=${#COURTS[@]}
echo "" | tee -a "$LOG_FILE"
echo "--- Ingesting $TOTAL courts ---" | tee -a "$LOG_FILE"

# Step 3: Ingest each court
DONE=0
FAILED=0
for court in "${COURTS[@]}"; do
  DONE=$((DONE + 1))
  echo "" | tee -a "$LOG_FILE"
  echo "[$DONE/$TOTAL] Court: $court" | tee -a "$LOG_FILE"
  if "$PYTHON" "$SCRIPT_DIR/ingest.py" \
      --court "$court" \
      --after 1980-01-01 \
      $DRY_RUN \
      2>&1 | tee -a "$LOG_FILE"; then
    echo "[OK] $court" | tee -a "$LOG_FILE"
  else
    echo "[FAILED] $court" | tee -a "$LOG_FILE"
    FAILED=$((FAILED + 1))
  fi
done

echo "" | tee -a "$LOG_FILE"
echo "=====================================" | tee -a "$LOG_FILE"
echo "Completed: $(date)"                    | tee -a "$LOG_FILE"
echo "Courts processed: $DONE"               | tee -a "$LOG_FILE"
echo "Failures: $FAILED"                     | tee -a "$LOG_FILE"
echo "Log: $LOG_FILE"                        | tee -a "$LOG_FILE"
echo "=====================================" | tee -a "$LOG_FILE"
