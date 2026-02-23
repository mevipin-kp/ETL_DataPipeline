import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from typing import Dict, Any, List

from backend.db.models import FxRate
from backend.app.services.common_service import CommonService, PipelineLogFilter
from backend.app.services.audit_service import AuditService
from backend.app.services.data_loader_interface import DataLoaderInterface

log = logging.getLogger(__name__)

# SQLite hard limit: 999 variables. FX rate upsert sends 4 columns → 999 // 4 = 249
_UPSERT_COLS = 4
_BATCH_SIZE  = 999 // _UPSERT_COLS   # 249 rows per batch


class FxRateService(DataLoaderInterface):
    """Handles loading and querying FX rate data."""

    @staticmethod
    def cleanse_data(df: pd.DataFrame, db: Session = None) -> tuple:
        """
        Flags bad rows and splits the frame into clean vs quarantined.

        INVALID_CURRENCY takes priority over INVALID_FX_RATE when both apply
        to the same row. After filtering out bad rows, duplicates on
        (local_currency, base_currency, date) are resolved by keeping the
        latest entry — older ones go to quarantine as SUPERSEDED_BY_CORRECTION.
        """
        df = df.copy()
        df['quarantine_reason'] = None

        null_currency_mask = (
            df['local_currency'].isna() | (df['local_currency'] == '') |
            df['base_currency'].isna()  | (df['base_currency'] == '')
        )
        invalid_rate_mask = df['fx_rate'].isna() | (pd.to_numeric(df['fx_rate'], errors='coerce') <= 0)

        # Apply lowest priority first so higher priority can overwrite
        df.loc[invalid_rate_mask,  'quarantine_reason'] = 'INVALID_FX_RATE'
        df.loc[null_currency_mask, 'quarantine_reason'] = 'INVALID_CURRENCY'

        invalid_mask = df['quarantine_reason'].notna()
        df_orphan = df[invalid_mask].copy()
        df_clean  = df[~invalid_mask].copy()

        # Dedup — keep latest row per (local_currency, base_currency, date)
        df_clean = df_clean.sort_values('date', ascending=True)
        superseded = df_clean[df_clean.duplicated(
            subset=['local_currency', 'base_currency', 'date'], keep='last'
        )].copy()
        superseded['quarantine_reason'] = 'SUPERSEDED_BY_CORRECTION'
        df_clean = df_clean.drop_duplicates(
            subset=['local_currency', 'base_currency', 'date'], keep='last'
        )

        df_orphan = pd.concat([df_orphan, superseded], ignore_index=True)
        if not df_orphan.empty:
            for reason, count in df_orphan['quarantine_reason'].value_counts().items():
                log.info(f"  FX rate quarantine [{reason}]: {count} rows")
        log.info(f"  FX rate orphan rows: {len(df_orphan)}")
        log.info(f"  FX rate clean rows:  {len(df_clean)}")
        return df_clean, df_orphan

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list) -> list:
        """Validate that required columns exist in the DataFrame."""
        return [col for col in required_columns if col not in df.columns]

    @staticmethod
    def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess and clean FX rate data."""
        df['local_currency'] = df['local_currency'].astype(str).str.strip().str.upper()
        df['base_currency']  = df['base_currency'].astype(str).str.strip().str.upper()
        df['date']           = df['date'].astype(str).str.strip()
        df['fx_rate']        = pd.to_numeric(df['fx_rate'], errors='coerce')
        return df

    @staticmethod
    def _build_records(df: pd.DataFrame) -> List[dict]:
        """Converts the clean DataFrame to a list of dicts ready for upsert."""
        records = df[['local_currency', 'base_currency', 'date', 'fx_rate']].copy()
        return records.to_dict(orient='records')

    @staticmethod
    def _prepare_batch_stmt(batch: List[dict], insert_fn) -> Any:
        """Builds the upsert statement for a batch without hitting the database."""
        stmt = insert_fn(FxRate).values(batch)
        return stmt.on_conflict_do_update(
            index_elements=['local_currency', 'base_currency', 'date'],
            set_={'fx_rate': stmt.excluded.fx_rate}
        )

    @staticmethod
    def load_to_database(db: Session, df: pd.DataFrame, total_rows: int, csv_file_path: str) -> Dict[str, Any]:
        """
        Upserts FX rate rows into the fx_rate table (GOLDEN zone).
        Batch statements are built in parallel threads, then executed
        sequentially to stay within SQLite's single-writer constraint.
        """
        if df.empty:
            return {
                'status': 'success', 'total_rows': total_rows,
                'inserted_rows': 0, 'updated_rows': 0, 'skipped_rows': 0, 'errors': [],
                'message': f'No records to load from {Path(csv_file_path).name}'
            }

        t0 = time.perf_counter()
        records = FxRateService._build_records(df)
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
                    lambda b: FxRateService._prepare_batch_stmt(b, insert_fn), batches
                ))
            for stmt in prepared_stmts:
                db.execute(stmt)
            db.commit()
            log.info(f"DB upsert committed {total_records} rows in {time.perf_counter() - t1:.2f}s")
        except IntegrityError as e:
            db.rollback()
            log.exception("IntegrityError during fx_rate upsert")
            raise Exception(f"Database integrity error: {str(e)}") from e
        except Exception:
            db.rollback()
            log.exception("Unexpected error during fx_rate upsert")
            raise

        log.info(f"Total load_to_database time: {time.perf_counter() - t0:.2f}s")
        return {
            'status': 'success', 'total_rows': total_rows,
            'inserted_rows': total_records, 'updated_rows': 0,
            'skipped_rows': 0, 'errors': [],
            'message': f'Upserted {total_records} FX rate records from {Path(csv_file_path).name}'
        }

    @staticmethod
    def load_data(db: Session, csv_file_path: str) -> Dict[str, Any]:
        """Runs the full pipeline: BRONZE → SILVER → QUARANTINE → GOLDEN."""
        load_uuid = str(uuid.uuid4())
        pipeline_filter = PipelineLogFilter(load_uuid)
        log.addFilter(pipeline_filter)
        t_start = time.perf_counter()
        try:
            df = CommonService.read_csv_file(csv_file_path, load_uuid=load_uuid)
            required_columns = ['local_currency', 'base_currency', 'date', 'fx_rate']
            total_rows = len(df)
            log.info(f"Pipeline started — Read {total_rows} rows from {Path(csv_file_path).name}")

            missing_columns = FxRateService.validate_required_columns(df, required_columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            bronze_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "BRONZE"     / "FXRATE" / f"FXRATE_{load_uuid}.csv"
            silver_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "SILVER"     / "FXRATE" / f"FXRATE_{load_uuid}.csv"
            quarantine_path = Path(__file__).resolve().parent.parent.parent / "resources" / "QUARANTINE" / "FXRATE" / f"FXRATE_{load_uuid}.csv"

            # BRONZE write and preprocessing run in parallel; aborts if BRONZE fails
            df, _ = CommonService.write_bronze_and_preprocess(
                df, str(bronze_path), FxRateService.preprocess_data, log,
                db=db, entity="fx_rate", file_name=csv_file_path, load_uuid=load_uuid
            )

            df_clean, df_orphan = FxRateService.cleanse_data(df)
            log.info(f"Cleanse — clean: {len(df_clean)}, orphan: {len(df_orphan)}")

            # SILVER and QUARANTINE writes must both succeed before touching the DB
            CommonService.write_silver_and_quarantine(
                df_clean, df_orphan, str(silver_path), str(quarantine_path), db, log,
                entity="fx_rate", file_name=csv_file_path, load_uuid=load_uuid
            )

            t_golden = time.perf_counter()
            log.info(f"GOLDEN zone: starting DB upsert for {len(df_clean)} rows")
            try:
                result = FxRateService.load_to_database(db, df_clean, total_rows, csv_file_path)
                AuditService.record(db, "fx_rate", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.SUCCESS,
                                    row_count=result.get('inserted_rows', 0))
                db.commit()
            except Exception as golden_err:
                AuditService.record(db, "fx_rate", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.FAILURE,
                                    row_count=0, error=golden_err)
                try:
                    db.commit()
                except Exception:
                    pass
                raise
            log.info(f"GOLDEN zone: DB upsert completed in {time.perf_counter() - t_golden:.2f}s")

            result['load_uuid']    = load_uuid
            result['orphan_info']  = {
                'orphan_count': len(df_orphan),
                'breakdown': df_orphan['quarantine_reason'].value_counts().to_dict() if not df_orphan.empty else {},
                'orphan_records': CommonService.build_orphan_records(
                    df_orphan,
                    key_cols=['local_currency', 'base_currency', 'date', 'fx_rate']
                ),
            }
            log.info(f"Pipeline completed in {time.perf_counter() - t_start:.2f}s")
            return result

        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file is empty: {csv_file_path}")
        except Exception:
            log.exception("Unexpected error in FxRateService.load_data")
            raise
        finally:
            log.removeFilter(pipeline_filter)

    @staticmethod
    def get_fx_rates(db: Session, local_currency: str = None, base_currency: str = None, date: str = None) -> list:
        """Retrieve FX rates from database with optional filters."""
        try:
            query = db.query(FxRate)
            if local_currency:
                query = query.filter(FxRate.local_currency == local_currency.upper())
            if base_currency:
                query = query.filter(FxRate.base_currency == base_currency.upper())
            if date:
                query = query.filter(FxRate.date == date)
            return [r._to_json() for r in query.all()]
        except Exception as e:
            log.exception("Error retrieving FX rates")
            raise Exception(f"Error retrieving FX rates: {str(e)}") from e
