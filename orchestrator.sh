#!/usr/bin/env bash
# =============================================================================
# orchestrator.sh — Liquidity Data Pipeline orchestrator
#
# Usage:
#   chmod +x orchestrator.sh
#   ./orchestrator.sh
#
# What it does:
#   1. Creates / activates a Python virtual environment
#   2. Installs dependencies from backend/requirements.txt
#   3. Starts the FastAPI server in the background
#   4. Waits for the server to be ready
#   5. Runs the full pipeline in the required order:
#        company → revenue → expense → fx_rate → financials/calculate
#   6. Runs all GET/audit read endpoints and prints results
#   7. Shuts down the server on exit
# =============================================================================

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8004
HOST="http://localhost:${PORT}"
VENV_DIR="${BASE_DIR}/.venv"
LOG_FILE="${BASE_DIR}/server.log"
SERVER_PID=""

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Colour

log()   { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()    { echo -e "${GREEN}[  OK  ]${NC} $*"; }
warn()  { echo -e "${YELLOW}[ WARN ]${NC} $*"; }
fail()  { echo -e "${RED}[ FAIL ]${NC} $*"; exit 1; }

separator() { echo -e "${CYAN}────────────────────────────────────────────────────────────${NC}"; }

# ── Cleanup on exit (only used when user confirms stop, or on error) ─────────
cleanup() {
    if [[ -n "${SERVER_PID}" ]]; then
        log "Shutting down server (PID ${SERVER_PID})..."
        kill "${SERVER_PID}" 2>/dev/null || true
        ok "Server stopped."
    fi
}
trap cleanup ERR INT TERM   # fire on error / Ctrl+C / termination signal
                             # NOT on normal EXIT — user gets to choose below

# ── Step 1: Virtual environment ───────────────────────────────────────────────
separator
log "Step 1 — Setting up Python virtual environment..."
if [[ ! -d "${VENV_DIR}" ]]; then
    python3 -m venv "${VENV_DIR}"
    ok "Virtual environment created at ${VENV_DIR}"
else
    ok "Virtual environment already exists at ${VENV_DIR}"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"
ok "Virtual environment activated."

# ── Step 2: Install dependencies ─────────────────────────────────────────────
separator
log "Step 2 — Installing dependencies from backend/requirements.txt..."
pip install --quiet --upgrade pip
pip install --quiet -r "${BASE_DIR}/backend/requirements.txt"
ok "Dependencies installed."

# ── Step 3: Start the FastAPI server ─────────────────────────────────────────
separator
log "Step 3 — Starting FastAPI server on port ${PORT}..."
cd "${BASE_DIR}"
python3 -m uvicorn backend.app.main:app \
    --host 0.0.0.0 \
    --port "${PORT}" \
    --log-level info \
    > "${LOG_FILE}" 2>&1 &
SERVER_PID=$!
ok "Server started (PID ${SERVER_PID}). Logs → ${LOG_FILE}"

# ── Step 4: Wait for server to be ready ──────────────────────────────────────
separator
log "Step 4 — Waiting for server to be ready..."
MAX_WAIT=60
WAITED=0
until curl --silent --fail "${HOST}/docs" > /dev/null 2>&1; do
    if [[ ${WAITED} -ge ${MAX_WAIT} ]]; then
        fail "Server did not start within ${MAX_WAIT}s. Check ${LOG_FILE}."
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done
ok "Server is ready (waited ${WAITED}s)."

# ── Helper: POST with response printing ──────────────────────────────────────
post() {
    local endpoint="$1"
    local label="$2"
    separator
    log "POST ${endpoint} — ${label}"
    RESPONSE=$(curl --silent --show-error --fail \
        -X POST "${HOST}${endpoint}" \
        -H "Content-Type: application/json" 2>&1) || {
        warn "POST ${endpoint} returned an error:"
        echo "${RESPONSE}"
        return 1
    }
    ok "Response:"
    echo "${RESPONSE}" | python3 -m json.tool 2>/dev/null || echo "${RESPONSE}"
}

get() {
    local endpoint="$1"
    local label="$2"
    separator
    log "GET ${endpoint} — ${label}"
    RESPONSE=$(curl --silent --show-error --fail \
        -X GET "${HOST}${endpoint}" 2>&1) || {
        warn "GET ${endpoint} returned an error:"
        echo "${RESPONSE}"
        return 1
    }
    ok "Response (first 500 chars):"
    echo "${RESPONSE}" | python3 -m json.tool 2>/dev/null | head -c 500
    echo ""
}

# ── Step 5: Run pipeline in order ────────────────────────────────────────────
separator
log "Step 5 — Running pipeline in required order..."
echo ""

post "/company"              "Load company master (dim_company)"
post "/revenue"              "Load monthly revenue"
post "/expense"              "Load monthly expenses"
post "/fx_rate"              "Load FX rates"
post "/financials/calculate" "Compute financial metrics"

# ── Step 6: Read endpoints ───────────────────────────────────────────────────
separator
log "Step 6 — Running read endpoints..."
echo ""

get "/company"   "Read dim_company"
get "/revenue"   "Read revenue records"
get "/expense"   "Read expense records"
get "/financials" "Read computed financial metrics"
get "/fx_rate"   "Read FX rates"

# ── Step 7: Audit endpoints ───────────────────────────────────────────────────
separator
log "Step 7 — Querying dataload_audit table..."
echo ""

get "/audit"                        "All audit records"
get "/audit?entity=company"         "Audit — company entity"
get "/audit?entity=revenue"         "Audit — revenue entity"
get "/audit?entity=expense"         "Audit — expense entity"
get "/audit?stage=GOLDEN&status=SUCCESS"  "Audit — GOLDEN SUCCESS"
get "/audit?stage=GOLDEN&status=FAILURE"  "Audit — GOLDEN FAILURE"

# ── Done — ask user whether to stop or keep the server running ───────────────
separator
ok "Pipeline orchestration complete."
separator
echo ""
echo -e "${YELLOW}  The FastAPI server is still running (PID ${SERVER_PID}).${NC}"
echo -e "${YELLOW}  Logs → ${LOG_FILE}${NC}"
echo ""

# Read from /dev/tty so the prompt works even when stdin is a pipe
while true; do
    printf "${CYAN}  Stop the server now? [y/N]:${NC} "
    read -r STOP_ANSWER </dev/tty || STOP_ANSWER="n"
    case "${STOP_ANSWER,,}" in          # lowercase the answer
        y|yes)
            separator
            cleanup
            trap - ERR INT TERM         # clear the trap — we handled it manually
            separator
            ok "Server stopped. Goodbye."
            separator
            exit 0
            ;;
        n|no|"")
            separator
            ok "Server left running in the background."
            log "  PID      : ${SERVER_PID}"
            log "  Logs     : ${LOG_FILE}"
            log "  API docs : http://localhost:${PORT}/docs"
            log "  Stop later with:  kill ${SERVER_PID}"
            separator
            trap - ERR INT TERM         # detach — do NOT kill on script exit
            exit 0
            ;;
        *)
            warn "Please answer y (yes) or n (no)."
            ;;
    esac
done

