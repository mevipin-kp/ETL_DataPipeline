import logging
import time
import uuid
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

from backend.db.models import CompaniesFinancial, Company
from backend.app.services.common_service import CommonService, PipelineLogFilter
from backend.app.services.audit_service import AuditService
from backend.app.services.data_loader_interface import DataLoaderInterface

log = logging.getLogger(__name__)

# SQLite caps bind variables at 999. Expense upsert uses 11 columns → 90 rows per batch.
_UPSERT_COLS = 11
_BATCH_SIZE  = 999 // _UPSERT_COLS   # 90 rows per batch


class ExpenseService(DataLoaderInterface):
    """Handles loading and querying monthly expense data."""

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list) -> list:
        """Returns a list of any required columns that are missing from df."""
        return [col for col in required_columns if col not in df.columns]

    @staticmethod
    def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
        """Normalises types and strips whitespace from raw expense data."""
        df['company_id'] = pd.to_numeric(df['company_id'], errors='coerce').astype('Int64').astype(str)
        df['company_id'] = df['company_id'].replace('<NA>', pd.NA)
        df['month'] = df['month'].astype(str).str.strip()
        df['expenses'] = pd.to_numeric(df['expenses'], errors='coerce')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        return df

    @staticmethod
    def cleanse_data(df: pd.DataFrame, db: Session = None) -> tuple:
        """
        Flags bad rows and splits the frame into clean vs quarantined.

        Priority order (highest wins when multiple issues apply):
        ORPHAN_COMPANY_ID > NULL_EXPENSE > NEGATIVE_EXPENSE

        After filtering, duplicates on (company_id, month) keep the row
        with the latest updated_at; older copies go to quarantine as
        SUPERSEDED_BY_CORRECTION.
        """
        df = df.copy()
        df['quarantine_reason'] = None

        null_company_mask     = df['company_id'].isna()
        null_expense_mask     = df['expenses'].isna()
        negative_expense_mask = df['expenses'].notna() & (df['expenses'] < 0)

        # Orphan check against known company_ids in dim_company
        if db is not None:
            known_ids = {
                str(row[0])
                for row in db.query(Company.company_id).all()
                if row[0] is not None
            }
            log.info(f"  Known company_ids in DB: {len(known_ids)}")
            orphan_company_mask = null_company_mask | ~df['company_id'].astype(str).isin(known_ids)
        else:
            orphan_company_mask = null_company_mask

        # Apply lowest priority first so higher-priority reasons overwrite
        df.loc[negative_expense_mask, 'quarantine_reason'] = 'NEGATIVE_EXPENSE'
        df.loc[null_expense_mask,     'quarantine_reason'] = 'NULL_EXPENSE'
        df.loc[orphan_company_mask,   'quarantine_reason'] = 'ORPHAN_COMPANY_ID'

        invalid_mask = df['quarantine_reason'].notna()
        df_orphan = df[invalid_mask].copy()
        df_clean  = df[~invalid_mask].copy()

        # Dedup — keep latest updated_at per (company_id, month); superseded rows quarantined
        df_clean = df_clean.sort_values('updated_at', ascending=True)
        superseded = df_clean[df_clean.duplicated(subset=['company_id', 'month'], keep='last')].copy()
        superseded['quarantine_reason'] = 'SUPERSEDED_BY_CORRECTION'
        df_clean = df_clean.drop_duplicates(subset=['company_id', 'month'], keep='last')

        df_orphan = pd.concat([df_orphan, superseded], ignore_index=True)
        log.info(f"  Expense orphan rows: {len(df_orphan)}")
        log.info(f"  Expense clean rows:  {len(df_clean)}")
        return df_clean, df_orphan

    @staticmethod
    def _build_records(df: pd.DataFrame) -> List[dict]:
        """Converts clean rows to upsert dicts, nulling out revenue-side columns."""
        records = df[['company_id', 'month', 'expenses', 'updated_at',
                       'pipeline_loaded_at', 'pipeline_load_uuid']].copy()
        records = records.rename(columns={
            'updated_at':         'expenses_updated_at',
            'pipeline_loaded_at': 'expense_pipeline_loaded_at',
            'pipeline_load_uuid': 'expense_pipeline_load_uuid',
        })
        records['company_id']               = records['company_id'].astype(str)
        # Revenue-side columns are unknown here — set to NULL so we don't clobber them on upsert
        records['revenue']                   = None
        records['currency']                  = None
        records['gross_profit']              = None
        records['gross_margin_pct']          = None
        records['mom_revenue_growth_pct']    = None
        records['rolling_3m_revenue']        = None
        records['revenue_updated_at']        = None
        records['revenue_pipeline_loaded_at']= None
        records['revenue_pipeline_load_uuid']= None
        return records.to_dict(orient='records')

    @staticmethod
    def _prepare_batch_stmt(batch: List[dict], insert_fn) -> Any:
        """Builds the upsert statement for a batch without hitting the database."""
        stmt = insert_fn(CompaniesFinancial).values(batch)
        return stmt.on_conflict_do_update(
            index_elements=['company_id', 'month'],
            set_={
                'expenses':                   stmt.excluded.expenses,
                'expenses_updated_at':        stmt.excluded.expenses_updated_at,
                'expense_pipeline_loaded_at': stmt.excluded.expense_pipeline_loaded_at,
                'expense_pipeline_load_uuid': stmt.excluded.expense_pipeline_load_uuid,
            }
        )

    @staticmethod
    def _upsert_batch(batch: List[dict], insert_fn, db: Session) -> None:
        """Builds and executes one upsert batch on the current thread."""
        stmt = ExpenseService._prepare_batch_stmt(batch, insert_fn)
        db.execute(stmt)

    @staticmethod
    def load_to_database(db: Session, df: pd.DataFrame, total_rows: int, csv_file_path: str) -> Dict[str, Any]:
        """
        Upserts expense rows into fact_company_monthly_financials (GOLDEN zone).
        Batch statements are prepared in parallel then executed sequentially
        to respect SQLite's single-writer constraint.
        """
        if df.empty:
            return {
                'status': 'success', 'total_rows': total_rows,
                'inserted_rows': 0, 'updated_rows': 0, 'skipped_rows': 0, 'errors': [],
                'message': f'No records to load from {Path(csv_file_path).name}'
            }

        t0 = time.perf_counter()
        records = ExpenseService._build_records(df)
        total_records = len(records)
        log.info(f"Built {total_records} upsert records in {time.perf_counter() - t0:.2f}s")

        try:
            dialect = db.get_bind().dialect.name
        except Exception:
            dialect = 'sqlite'
        insert_fn = pg_insert if dialect == 'postgresql' else sqlite_insert

        batches = [records[i: i + _BATCH_SIZE] for i in range(0, total_records, _BATCH_SIZE)]
        log.info(f"Executing {len(batches)} batches of up to {_BATCH_SIZE} rows each")

        t1 = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                prepared_stmts = list(executor.map(
                    lambda b: ExpenseService._prepare_batch_stmt(b, insert_fn), batches
                ))
            for stmt in prepared_stmts:
                db.execute(stmt)
            db.commit()
            log.info(f"DB upsert committed {total_records} rows in {time.perf_counter() - t1:.2f}s")
        except IntegrityError as e:
            db.rollback()
            log.exception("IntegrityError during expense upsert")
            raise Exception(f"Database integrity error: {str(e)}") from e
        except Exception as e:
            db.rollback()
            log.exception("Unexpected error during expense upsert")
            raise

        log.info(f"Total load_to_database time: {time.perf_counter() - t0:.2f}s")
        return {
            'status': 'success', 'total_rows': total_rows,
            'inserted_rows': total_records, 'updated_rows': 0,
            'skipped_rows': 0, 'errors': [],
            'message': f'Upserted {total_records} expense records from {Path(csv_file_path).name}'
        }

    @staticmethod
    def load_data(db: Session, csv_file_path: str) -> Dict[str, Any]:
        """Runs the full pipeline: read → BRONZE → preprocess → cleanse → SILVER/QUARANTINE → GOLDEN."""
        load_uuid = str(uuid.uuid4())
        pipeline_filter = PipelineLogFilter(load_uuid)
        log.addFilter(pipeline_filter)
        t_start = time.perf_counter()
        try:
            df = CommonService.read_csv_file(csv_file_path, load_uuid=load_uuid)
            required_columns = ['company_id', 'month', 'expenses']
            total_rows = len(df)
            log.info(f"Pipeline started — Read {total_rows} rows from {Path(csv_file_path).name}")

            missing_columns = ExpenseService.validate_required_columns(df, required_columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            bronze_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "BRONZE"     / "EXPENSE" / f"EXPENSE_{load_uuid}.csv"
            silver_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "SILVER"     / "EXPENSE" / f"EXPENSE_{load_uuid}.csv"
            quarantine_path = Path(__file__).resolve().parent.parent.parent / "resources" / "QUARANTINE" / "EXPENSE" / f"EXPENSE_{load_uuid}.csv"

            # BRONZE write and preprocessing run in parallel; aborts if BRONZE fails
            df, _ = CommonService.write_bronze_and_preprocess(
                df, str(bronze_path), ExpenseService.preprocess_data, log,
                db=db, entity="expense", file_name=csv_file_path, load_uuid=load_uuid
            )

            df_clean, df_orphan = ExpenseService.cleanse_data(df, db)
            log.info(f"Cleanse — clean: {len(df_clean)}, orphan: {len(df_orphan)}")

            # SILVER and QUARANTINE writes must both succeed before touching the DB
            CommonService.write_silver_and_quarantine(
                df_clean, df_orphan, str(silver_path), str(quarantine_path), db, log,
                entity="expense", file_name=csv_file_path, load_uuid=load_uuid
            )

            t_golden = time.perf_counter()
            log.info(f"GOLDEN zone: starting DB upsert for {len(df_clean)} rows")
            try:
                result = ExpenseService.load_to_database(db, df_clean, total_rows, csv_file_path)
                AuditService.record(db, "expense", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.SUCCESS,
                                    row_count=result.get('inserted_rows', 0))
                db.commit()
            except Exception as golden_err:
                AuditService.record(db, "expense", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.FAILURE,
                                    row_count=0, error=golden_err)
                try:
                    db.commit()
                except Exception:
                    pass
                raise
            log.info(f"GOLDEN zone: DB upsert completed in {time.perf_counter() - t_golden:.2f}s")


            result['load_uuid'] = load_uuid
            result['orphan_info'] = {'orphan_count': len(df_orphan)}
            log.info(f"Pipeline completed in {time.perf_counter() - t_start:.2f}s")
            return result

        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file is empty: {csv_file_path}")
        except Exception as e:
            log.exception("Unexpected error in ExpenseService.load_data")
            raise
        finally:
            log.removeFilter(pipeline_filter)

    @staticmethod
    def get_expense(db: Session, company_id: str = None, month: str = None) -> list:
        """Queries fact_company_monthly_financials for expense rows, with optional filters."""
        try:
            from backend.db.models import CompaniesFinancial
            query = db.query(CompaniesFinancial)
            if company_id is not None:
                query = query.filter(CompaniesFinancial.company_id == str(company_id))
            if month:
                query = query.filter(CompaniesFinancial.month == month)
            return query.all()
        except Exception as e:
            log.exception("Error retrieving expense data")
            raise Exception(f"Error retrieving expense data: {str(e)}") from e

