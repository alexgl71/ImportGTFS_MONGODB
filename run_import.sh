#!/bin/bash
# Importa tutti i feed GTFS in sequenza ogni mattina alle 7:00
# Lanciato da launchd — vedi com.importgtfs.daily.plist

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

NODE="$(which node)"
IMPORT="$SCRIPT_DIR/import.js"
ERROR_LOG="$LOG_DIR/import_error.log"

log_error() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$ERROR_LOG"
}

run_city() {
  local city=$1
  echo ""
  echo "=== $city — $(date '+%Y-%m-%d %H:%M:%S') ==="
  if caffeinate -i "$NODE" --max-old-space-size=8192 "$IMPORT" "$city" --sync; then
    echo "=== $city completata — $(date '+%Y-%m-%d %H:%M:%S') ==="
  else
    log_error "ERRORE: $city fallita con exit code $?"
  fi
}

cd "$SCRIPT_DIR"

echo "=============================="
echo "Import giornaliero avviato — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================="

# Verifica MongoDB locale raggiungibile
if ! "$NODE" -e "
  const { MongoClient } = require('mongodb');
  const c = new MongoClient('mongodb://localhost:27017');
  c.connect().then(() => c.close()).catch(e => { process.stderr.write('MongoDB non raggiungibile: ' + e.message + '\n'); process.exit(1); });
" 2>>"$ERROR_LOG"; then
  log_error "ABORT: MongoDB locale non raggiungibile — import annullato"
  exit 1
fi

run_city Torino
run_city Roma
run_city Firenze
run_city Bari

echo ""
echo "=============================="
echo "Import giornaliero completato — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================="
