# Data Pipeline

A FastAPI-based data pipeline that loads company, revenue, expense, and FX rate data from CSV files through a medallion architecture (Bronze → Silver → Quarantine → Golden) into a SQLite database, then computes financial metrics.

---

## Prerequisites

Make sure the following are installed on your machine before you start:

- **Python 3.10+** — `python3 --version`
- **pip** — comes bundled with Python
- **curl** — used by the orchestrator scripts to hit the API (pre-installed on macOS/Linux; on Windows install via [curl.se](https://curl.se/windows/))
- **Git** — to clone the repo

Docker is optional. It is only needed if you want to run the bundled MinIO / Spark services defined in `docker-compose.yml`. The core pipeline runs without them.

---

## Project Structure

```
DataPipeline/
├── backend/
│   ├── app/
│   │   ├── controllers/      # FastAPI route handlers
│   │   ├── services/         # Pipeline logic (load, cleanse, upsert)
│   │   └── main.py           # App entry point + logging setup
│   ├── db/
│   │   ├── models/           # SQLAlchemy ORM models
│   │   └── session.py        # Engine + session factory
│   └── resources/
│       ├── companies.csv
│       ├── fx_rate.csv
│       ├── monthly_expenses.csv
│       ├── monthly_revenue.csv
│       ├── BRONZE/           # Raw CSV snapshots per run
│       ├── SILVER/           # Cleansed CSV per run
│       └── QUARANTINE/       # Rejected rows per run
├── docker/
│   └── init_tables.sql       # DDL run at startup
├── docker-compose.yml        # Optional MinIO + Spark services
├── orchestrator.sh           # One-shot runner (macOS / Linux)
├── orchestrator.bat          # One-shot runner (Windows)
└── README.md
```

---

## Local Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd Liquidity_DataPipeline
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
```

### 3. Activate the virtual environment

**macOS / Linux**
```bash
source .venv/bin/activate
```

**Windows (Command Prompt)**
```bat
.venv\Scripts\activate.bat
```

**Windows (PowerShell)**
```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```bash
pip install --upgrade pip
pip install -r backend/requirements.txt
```

Dependencies installed:

| Package | Purpose |
|---|---|
| `fastapi` | API framework |
| `uvicorn[standard]` | ASGI server |
| `SQLAlchemy` | ORM + DB engine |
| `pandas` | CSV parsing and data cleansing |
| `python-multipart` | Form/file upload support |

---

## Running the Server

Start the FastAPI server from the **project root** (important — the import paths assume this):

```bash
python3 -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8004 --log-level info
```

Once running, the interactive API docs are available at:

- Swagger UI: [http://localhost:8004/docs](http://localhost:8004/docs)
- ReDoc: [http://localhost:8004/redoc](http://localhost:8004/redoc)

> The server uses an **in-memory SQLite database** by default. Data does not persist between restarts.

---

## Running the Full Pipeline

The pipeline must be executed in this order, because revenue/expense records are validated against `dim_company`:

```
company → revenue → expense → fx_rate → financials/calculate
```

### Option A — Orchestrator script (recommended)

The orchestrator handles everything: venv setup, dependency install, server start, pipeline run, read endpoints, and audit queries.

**macOS / Linux**
```bash
chmod +x orchestrator.sh
./orchestrator.sh
```

**Windows**
```bat
orchestrator.bat
```

At the end of the run, the script asks whether to stop the server or leave it running in the background.

---

### Option B — Manual curl calls

With the server already running on port 8004:

```bash
# 1. Load company master data
curl -s -X POST http://localhost:8004/company | python3 -m json.tool

# 2. Load monthly revenue
curl -s -X POST http://localhost:8004/revenue | python3 -m json.tool

# 3. Load monthly expenses
curl -s -X POST http://localhost:8004/expense | python3 -m json.tool

# 4. Load FX rates
curl -s -X POST http://localhost:8004/fx_rate | python3 -m json.tool

# 5. Compute financial metrics
curl -s -X POST http://localhost:8004/financials/calculate | python3 -m json.tool
```

---

## Reading Data Back

```bash
# All companies
curl -s http://localhost:8004/company | python3 -m json.tool

# Filter by country
curl -s "http://localhost:8004/company?country=USA" | python3 -m json.tool

# Revenue for a specific company and month
curl -s "http://localhost:8004/revenue?company_id=1&month=2024-01" | python3 -m json.tool

# Expenses for a month
curl -s "http://localhost:8004/expense?month=2024-03" | python3 -m json.tool

# Computed financials
curl -s http://localhost:8004/financials | python3 -m json.tool

# FX rates for a currency pair
curl -s "http://localhost:8004/fx_rate?local_currency=EUR&base_currency=USD" | python3 -m json.tool
```

---

## Audit Logs

Every stage of each pipeline run (Bronze, Silver, Quarantine, Golden) is recorded in the `dataload_audit` table.

```bash
# All audit records (newest first)
curl -s http://localhost:8004/audit | python3 -m json.tool

# Filter by entity
curl -s "http://localhost:8004/audit?entity=company" | python3 -m json.tool

# Filter by stage and status
curl -s "http://localhost:8004/audit?stage=GOLDEN&status=SUCCESS" | python3 -m json.tool

# All records for a specific pipeline run
curl -s "http://localhost:8004/audit?load_uuid=<uuid>" | python3 -m json.tool
```

---

## Pipeline Zones

| Zone | Location | Contents |
|---|---|---|
| **Bronze** | `backend/resources/BRONZE/` | Raw CSV snapshot taken at the start of each run |
| **Silver** | `backend/resources/SILVER/` | Rows that passed all validation checks |
| **Quarantine** | `backend/resources/QUARANTINE/` | Rows that failed validation, with a `quarantine_reason` column |
| **Golden** | SQLite (in-memory) | Final clean data upserted into the database |

Files inside each zone are named `<ENTITY>_<load_uuid>.csv` so every run is independently traceable.

---

## Data Quality Rules

| Entity | Rule | Quarantine reason |
|---|---|---|
| Company | Missing `company_id` | `NULL_COMPANY_ID` |
| Company | Duplicate `company_id` (older record) | `SUPERSEDED_BY_CORRECTION` |
| Revenue | `company_id` not in `dim_company` | `ORPHAN_COMPANY_ID` |
| Revenue | Null revenue | `NULL_REVENUE` |
| Revenue | Negative revenue | `NEGATIVE_REVENUE` |
| Expense | `company_id` not in `dim_company` | `ORPHAN_COMPANY_ID` |
| Expense | Null expenses | `NULL_EXPENSE` |
| Expense | Negative expenses | `NEGATIVE_EXPENSE` |
| FX Rate | Null or empty currency code | `INVALID_CURRENCY` |
| FX Rate | Null, zero, or negative rate | `INVALID_FX_RATE` |

---

## Optional: Docker Services

The `docker-compose.yml` defines MinIO (object storage) and Spark (distributed processing) services. These are **not required** to run the core pipeline — they are scaffolding for future extension.

```bash
docker-compose up -d
```

| Service | UI | Port |
|---|---|---|
| MinIO Console | http://localhost:9001 | 9000 (API), 9001 (UI) |
| Spark Master | http://localhost:8080 | 8080 |

Default MinIO credentials: `admin` / `password123`

---

## Stopping the Server

If you started the server manually:

```bash
# Find the PID
lsof -i :8004

# Kill it
kill <PID>
```

Or just hit `Ctrl+C` in the terminal where uvicorn is running.

