import logging
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from typing import Dict, Any, Optional

from backend.db.models import CompaniesFinancial

log = logging.getLogger(__name__)


class FinancialService:
    """Computes and persists derived financial metrics."""

    @staticmethod
    def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates gross_profit, gross_margin_pct, mom_revenue_growth_pct,
        and rolling_3m_revenue for rows that have both revenue and expenses.
        Rows missing either value are left untouched.
        """
        df = df.copy()

        # Only rows that have both revenue and expenses can get metrics
        eligible = df['revenue'].notna() & df['expenses'].notna()

        # --- Gross profit & margin ---
        df.loc[eligible, 'gross_profit'] = (
            df.loc[eligible, 'revenue'] - df.loc[eligible, 'expenses']
        )
        df.loc[eligible, 'gross_margin_pct'] = (
            df.loc[eligible, 'gross_profit'] / df.loc[eligible, 'revenue'] * 100
        ).where(df.loc[eligible, 'revenue'] != 0)

        # --- Sort for time-series metrics ---
        df['month_dt'] = pd.to_datetime(df['month'], format='%Y-%m', errors='coerce')
        df = df.sort_values(['company_id', 'month_dt'])

        # --- MoM revenue growth (per company) ---
        df['prev_revenue'] = df.groupby('company_id')['revenue'].shift(1)
        mom_eligible = eligible & df['prev_revenue'].notna() & (df['prev_revenue'] != 0)
        df.loc[mom_eligible, 'mom_revenue_growth_pct'] = (
            (df.loc[mom_eligible, 'revenue'] - df.loc[mom_eligible, 'prev_revenue'])
            / df.loc[mom_eligible, 'prev_revenue'] * 100
        )

        # --- Rolling 3-month revenue (per company, min 1 period, eligible rows only) ---
        df['rolling_3m_revenue'] = (
            df.groupby('company_id')['revenue']
              .transform(lambda x: x.rolling(window=3, min_periods=1).sum())
        )
        # Mask non-eligible rows — metrics require both revenue AND expenses
        df.loc[~eligible, 'rolling_3m_revenue'] = None

        # Clean up helper columns
        df = df.drop(columns=['month_dt', 'prev_revenue'], errors='ignore')

        log.info(f"  Metrics computed for {eligible.sum()} / {len(df)} rows")
        return df

    @staticmethod
    def save_metrics(db: Session, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Writes the computed metric columns back to fact_company_monthly_financials.
        Only the four metric fields are updated — revenue/expense source data
        is left untouched.
        """
        metric_cols = ['company_id', 'month', 'gross_profit', 'gross_margin_pct',
                       'mom_revenue_growth_pct', 'rolling_3m_revenue']

        records = df[metric_cols].copy()
        records['company_id'] = records['company_id'].astype(str)

        # Non-metric columns must be present to satisfy the upsert schema — leave them null
        records['revenue']                    = None
        records['expenses']                   = None
        records['currency']                   = None
        records['revenue_updated_at']         = None
        records['expenses_updated_at']        = None
        records['revenue_pipeline_loaded_at'] = None
        records['expense_pipeline_loaded_at'] = None
        records['revenue_pipeline_load_uuid'] = None
        records['expense_pipeline_load_uuid'] = None

        records = records.to_dict(orient='records')

        try:
            dialect = db.get_bind().dialect.name
        except Exception:
            dialect = 'sqlite'
        insert_fn = pg_insert if dialect == 'postgresql' else sqlite_insert

        # 15 columns per row → SQLite 999-variable cap → 66 rows per batch
        num_cols   = 15
        batch_size = 999 // num_cols

        try:
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                stmt = insert_fn(CompaniesFinancial).values(batch)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['company_id', 'month'],
                    set_={
                        'gross_profit':           stmt.excluded.gross_profit,
                        'gross_margin_pct':       stmt.excluded.gross_margin_pct,
                        'mom_revenue_growth_pct': stmt.excluded.mom_revenue_growth_pct,
                        'rolling_3m_revenue':     stmt.excluded.rolling_3m_revenue,
                    }
                )
                db.execute(upsert_stmt)
            db.commit()
        except IntegrityError as e:
            db.rollback()
            raise Exception(f"Database integrity error while saving metrics: {e}")

        return {'updated_rows': len(records)}

    @staticmethod
    def calculate_and_save(db: Session, company_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Pulls rows from fact_company_monthly_financials, runs compute_metrics,
        and saves the results back. Optionally scoped to a single company.
        """
        query = db.query(CompaniesFinancial)
        if company_id is not None:
            query = query.filter(CompaniesFinancial.company_id == str(company_id))

        rows = query.all()
        if not rows:
            return {
                'status': 'no_data',
                'message': 'No rows found in fact_company_monthly_financials to compute metrics for.',
                'total_rows': 0,
                'eligible_rows': 0,
                'updated_rows': 0,
            }

        df = pd.DataFrame([{
            'company_id':  r.company_id,
            'month':       r.month,
            'revenue':     r.revenue,
            'expenses':    r.expenses,
        } for r in rows])

        total_rows    = len(df)
        eligible_rows = int((df['revenue'].notna() & df['expenses'].notna()).sum())

        df = FinancialService.compute_metrics(df)
        save_result = FinancialService.save_metrics(db, df)

        return {
            'status':        'success',
            'message':       f'Metrics computed and saved for {eligible_rows} eligible rows.',
            'total_rows':    total_rows,
            'eligible_rows': eligible_rows,
            'updated_rows':  save_result['updated_rows'],
            'computed_at':   datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def get_financials(
        db: Session,
        company_id: Optional[str] = None,
        month: Optional[str] = None,
        currency: Optional[str] = None,
    ) -> list:
        """Queries fact_company_monthly_financials with optional company_id, month, currency filters."""
        try:
            query = db.query(CompaniesFinancial)
            if company_id is not None:
                query = query.filter(CompaniesFinancial.company_id == str(company_id))
            if month:
                query = query.filter(CompaniesFinancial.month == month)
            if currency:
                query = query.filter(CompaniesFinancial.currency == currency.upper())
            return [r._to_json() for r in query.all()]
        except Exception as e:
            raise Exception(f"Error retrieving financial data: {e}")


