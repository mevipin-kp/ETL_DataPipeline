-- dim_company table (renamed from companies per data modelling spec)
-- Must be created before fact_company_monthly_financials so the FK reference resolves
CREATE TABLE IF NOT EXISTS dim_company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    industry VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    pipeline_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    pipeline_load_uuid VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_company_monthly_financials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id VARCHAR(50) NOT NULL,
    month VARCHAR(20) NOT NULL,
    revenue FLOAT ,
    expenses FLOAT ,
    currency VARCHAR(20) ,
    gross_profit FLOAT ,
    gross_margin_pct FLOAT ,
    mom_revenue_growth_pct FLOAT ,
    rolling_3m_revenue FLOAT ,
    revenue_updated_at DATE,
    expenses_updated_at DATE,
    revenue_pipeline_loaded_at DATETIME,
    expense_pipeline_loaded_at DATETIME,
    revenue_pipeline_load_uuid VARCHAR(100),
    expense_pipeline_load_uuid VARCHAR(100),
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, month),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id)
);

-- dataload_audit table — tracks every zone write (BRONZE/SILVER/QUARANTINE/GOLDEN) per pipeline run
-- Used for audit and observability, not incremental load control
CREATE TABLE IF NOT EXISTS dataload_audit (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity      VARCHAR(50)  NOT NULL,               -- 'company', 'revenue', 'expense', 'fx_rate'
    file_name   VARCHAR(255) NOT NULL,               -- source CSV filename
    load_uuid   VARCHAR(100) NOT NULL,               -- pipeline run UUID
    stage       VARCHAR(20)  NOT NULL,               -- 'BRONZE', 'SILVER', 'QUARANTINE', 'GOLDEN'
    status      VARCHAR(10)  NOT NULL,               -- 'SUCCESS' or 'FAILURE'
    row_count   INTEGER      DEFAULT 0,              -- rows written / processed in this stage
    error_msg   TEXT         DEFAULT NULL,           -- populated on FAILURE
    run_at      DATETIME     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_load_uuid ON dataload_audit(load_uuid);
CREATE INDEX IF NOT EXISTS idx_audit_entity    ON dataload_audit(entity);
CREATE INDEX IF NOT EXISTS idx_audit_run_at    ON dataload_audit(run_at);
-- FX Rate table
CREATE TABLE IF NOT EXISTS fx_rate (
    local_currency VARCHAR(10) NOT NULL,
    base_currency  VARCHAR(10) NOT NULL,
    date           VARCHAR(20) NOT NULL,
    fx_rate        FLOAT       NOT NULL,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(local_currency, base_currency, date)
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_fx_rate_currencies ON fx_rate(local_currency, base_currency);
CREATE INDEX IF NOT EXISTS idx_fx_rate_date ON fx_rate(date);
CREATE INDEX IF NOT EXISTS idx_dim_company_company_id ON dim_company(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_financials_company_id ON fact_company_monthly_financials(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_financials_month ON fact_company_monthly_financials(month);

