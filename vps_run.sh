#!/bin/bash
# vps_run.sh - Wrapper for executing the pipeline on the VPS
# This ensures the environment is properly loaded and Python is executed correctly.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "[$(date)] vps_run.sh started in $DIR as user $(whoami)"
cd "$DIR"

# Determine the Python executable
if [ -d "$DIR/venv" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python3"
    echo "[$(date)] Using virtual environment: $PYTHON_EXEC"
else
    PYTHON_EXEC="/usr/bin/python3"
    echo "[$(date)] Using system python: $PYTHON_EXEC (Warning: Ensure requirements are installed)"
fi

# Ensure logs directory exists
mkdir -p "$DIR/logs"
LOG_FILE="$DIR/logs/alts-scraper_$(date +%Y%m%d).log"

# Cleanup logs older than 3 days
echo "[$(date)] 🧹 Cleaning up logs older than 3 days..."
find "$DIR/logs" -name "*.log" -mtime +3 -delete 2>/dev/null || true

# Define runner logic in a function to pipe all output
run_scraper() {
    # Capture Start Time
    START_TIME=$(date +%s)
    bash "$DIR/notify_vps.sh" START "alts-scraper.service"

    echo "[$(date)] Starting Alts Scraper Pipeline from $DIR"
    $PYTHON_EXEC -u run_pipeline.py
    LOCAL_EXIT_CODE=$?

    # Capture End Time and Calculate Duration
    END_TIME=$(date +%s)
    DURATION_SEC=$((END_TIME - START_TIME))
    DURATION_HUMAN=$(printf '%dh %dm %ds\n' $((DURATION_SEC/3600)) $((DURATION_SEC%3600/60)) $((DURATION_SEC%60)))

    echo "[$(date)] Pipeline Execution Finished (Exit Code: $LOCAL_EXIT_CODE, Duration: $DURATION_HUMAN)"

    if [ $LOCAL_EXIT_CODE -eq 0 ]; then
        bash "$DIR/notify_vps.sh" SUCCESS "alts-scraper.service" "$DURATION_HUMAN"
    fi
    
    return $LOCAL_EXIT_CODE
}

# Execute and pipe EVERYTHING to the log file AND the console/journal
run_scraper 2>&1 | tee -a "$LOG_FILE"

# The exit code should reflect the scraper's success
exit ${PIPESTATUS[0]}
