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
from backend.db.models import Company
from backend.app.services.common_service import CommonService, PipelineLogFilter
from backend.app.services.audit_service import AuditService
from backend.app.services.data_loader_interface import DataLoaderInterface

log = logging.getLogger(__name__)

# SQLite caps bind variables at 999. Company upsert uses 6 columns → 166 rows per batch.
_UPSERT_COLS = 6
_BATCH_SIZE  = 999 // _UPSERT_COLS


class CompanyService(DataLoaderInterface):
    """Handles loading and querying company master data."""

    @staticmethod
    def cleanse_data(df: pd.DataFrame, db: Session = None) -> tuple:
        """
        Splits rows into clean vs quarantined. Rows missing company_id are
        immediately quarantined. For the rest, duplicates on company_id are
        resolved by keeping the record with the latest created_at — older
        copies are quarantined as SUPERSEDED_BY_CORRECTION.
        """
        column = 'company_id'
        df = df.copy()

        # --- Stamp NULL_COMPANY_ID before any sorting/splitting ---
        missing_id_mask = df[column].isna()
        df.loc[missing_id_mask, 'quarantine_reason'] = 'NULL_COMPANY_ID'

        df_with_id  = df[~missing_id_mask].copy()
        df_null_ids = df[missing_id_mask].copy()

        # Sort descending so keep='first' on duplicates retains the newest record
        df_with_id = df_with_id.sort_values(by=[column, 'created_at'], ascending=[True, False])

        # Rows that survive dedup → clean; discarded duplicates → SUPERSEDED_BY_CORRECTION
        df_unique = df_with_id.drop_duplicates(subset=[column], keep='first').copy()

        duplicates_mask = df_with_id.duplicated(subset=[column], keep='first')
        df_duplicates   = df_with_id[duplicates_mask].copy()
        df_duplicates['quarantine_reason'] = 'SUPERSEDED_BY_CORRECTION'

        df_orphan = pd.concat([df_null_ids, df_duplicates], ignore_index=True, sort=False)
        df_orphan = df_orphan.sort_values(by=[column, 'created_at'], ascending=[True, False])

        if not df_orphan.empty:
            for reason, count in df_orphan['quarantine_reason'].value_counts().items():
                log.info(f"  Company quarantine [{reason}]: {count} rows")
        log.info(f"Company orphan rows: {len(df_orphan)}")
        log.info(f"Company clean rows:  {len(df_unique)}")
        return df_unique, df_orphan

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list) -> list:
        """Returns any columns from required_columns that are missing in df."""
        return [col for col in required_columns if col not in df.columns]

    @staticmethod
    def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
        """Normalises types and strips whitespace from raw company data."""
        df['company_id'] = pd.to_numeric(df['company_id'], errors='coerce').astype('Int64').astype(str)
        df['company_id'] = df['company_id'].replace('<NA>', pd.NA)
        df['name'] = df['name'].astype(str).str.strip()
        df['industry'] = df['industry'].astype(str).str.strip()
        df['country'] = df['country'].astype(str).str.strip()
        return df

    @staticmethod
    def _build_records(df: pd.DataFrame) -> List[dict]:
        """Converts the clean DataFrame to a list of dicts ready for upsert."""
        records = df[['company_id', 'name', 'industry', 'country',
                       'pipeline_loaded_at', 'pipeline_load_uuid']].copy()
        records['company_id'] = records['company_id'].astype(str)
        return records.to_dict(orient='records')

    @staticmethod
    def _prepare_batch_stmt(batch: List[dict], insert_fn) -> Any:
        """Builds the upsert statement for a batch without hitting the database."""
        stmt = insert_fn(Company).values(batch)
        return stmt.on_conflict_do_update(
            index_elements=['company_id'],
            set_={
                'name':               stmt.excluded.name,
                'industry':           stmt.excluded.industry,
                'country':            stmt.excluded.country,
                'pipeline_loaded_at': stmt.excluded.pipeline_loaded_at,
                'pipeline_load_uuid': stmt.excluded.pipeline_load_uuid,
            }
        )

    @staticmethod
    def load_to_database(db: Session, df: pd.DataFrame, total_rows: int, csv_file_path: str, load_uuid: str = '') -> Dict[str, Any]:
        """
        Upserts company rows into dim_company (GOLDEN zone).
        Statements are prepared in parallel then executed sequentially
        to stay within SQLite's single-writer constraint.
        """
        if df.empty:
            return {
                'status': 'success', 'total_rows': total_rows,
                'inserted_rows': 0, 'updated_rows': 0, 'skipped_rows': 0, 'errors': [],
                'message': f'No records to load from {Path(csv_file_path).name}'
            }

        t0 = time.perf_counter()

        company_ids = [str(cid) for cid in df['company_id'].dropna().unique().tolist()]
        existing_ids = {
            str(row[0])
            for row in db.query(Company.company_id)
                         .filter(Company.company_id.in_(company_ids))
                         .all()
        }

        records = CompanyService._build_records(df)
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
                    lambda b: CompanyService._prepare_batch_stmt(b, insert_fn), batches
                ))
            for stmt in prepared_stmts:
                db.execute(stmt)
            db.commit()
            log.info(f"DB upsert committed {total_records} rows in {time.perf_counter() - t1:.2f}s")
        except IntegrityError as e:
            db.rollback()
            log.exception("IntegrityError during company upsert")
            raise Exception(f"Database integrity error: {str(e)}") from e
        except Exception as e:
            db.rollback()
            log.exception("Unexpected error during company upsert")
            raise

        log.info(f"Total load_to_database time: {time.perf_counter() - t0:.2f}s")

        inserted_rows = sum(1 for r in records if r['company_id'] not in existing_ids)
        updated_rows  = total_records - inserted_rows

        return {
            'status': 'success', 'total_rows': total_rows,
            'inserted_rows': inserted_rows, 'updated_rows': updated_rows,
            'skipped_rows': 0, 'errors': [],
            'message': f'Upserted {inserted_rows} new and {updated_rows} existing company records from {Path(csv_file_path).name}'
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
            required_columns = ['company_id', 'name', 'industry', 'country']
            total_rows = len(df)
            log.info(f"Pipeline started — Read {total_rows} rows from {Path(csv_file_path).name}")

            missing_columns = CompanyService.validate_required_columns(df, required_columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            bronze_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "BRONZE"     / "COMPANY" / f"COMPANY_{load_uuid}.csv"
            silver_path     = Path(__file__).resolve().parent.parent.parent / "resources" / "SILVER"     / "COMPANY" / f"COMPANY_{load_uuid}.csv"
            quarantine_path = Path(__file__).resolve().parent.parent.parent / "resources" / "QUARANTINE" / "COMPANY" / f"COMPANY_{load_uuid}.csv"

            # BRONZE write and preprocessing run in parallel; aborts if BRONZE fails
            df, _ = CommonService.write_bronze_and_preprocess(
                df, str(bronze_path), CompanyService.preprocess_data, log,
                db=db, entity="company", file_name=csv_file_path, load_uuid=load_uuid
            )

            df_clean, df_orphan = CompanyService.cleanse_data(df)
            log.info(f"Cleanse — clean: {len(df_clean)}, orphan: {len(df_orphan)}")

            # SILVER and QUARANTINE writes must both succeed before touching the DB
            CommonService.write_silver_and_quarantine(
                df_clean, df_orphan, str(silver_path), str(quarantine_path), db, log,
                entity="company", file_name=csv_file_path, load_uuid=load_uuid
            )

            t_golden = time.perf_counter()
            log.info(f"GOLDEN zone: starting DB upsert for {len(df_clean)} rows")
            try:
                result = CompanyService.load_to_database(db, df_clean, total_rows, csv_file_path, load_uuid)
                AuditService.record(db, "company", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.SUCCESS,
                                    row_count=result.get('inserted_rows', 0) + result.get('updated_rows', 0))
                db.commit()
            except Exception as golden_err:
                AuditService.record(db, "company", csv_file_path, load_uuid,
                                    AuditService.GOLDEN, AuditService.FAILURE,
                                    row_count=0, error=golden_err)
                try:
                    db.commit()
                except Exception:
                    pass
                raise
            log.info(f"GOLDEN zone: DB upsert completed in {time.perf_counter() - t_golden:.2f}s")


            result['load_uuid'] = load_uuid
            result['orphan_info'] = {
                'orphan_count': len(df_orphan),
                'breakdown': df_orphan['quarantine_reason'].value_counts().to_dict() if not df_orphan.empty else {},
                'orphan_records': CommonService.build_orphan_records(
                    df_orphan,
                    key_cols=['company_id', 'name', 'industry', 'country', 'created_at']
                ),
            }
            log.info(f"Pipeline completed in {time.perf_counter() - t_start:.2f}s")
            return result

        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file is empty: {csv_file_path}")
        except Exception as e:
            log.exception("Unexpected error in CompanyService.load_data")
            raise
        finally:
            log.removeFilter(pipeline_filter)

    # Kept around for backward compatibility — callers should use load_data() directly
    @staticmethod
    def load_company_data(db: Session, csv_file_path: str) -> Dict[str, Any]:
        """Deprecated: Use load_data() instead."""
        return CompanyService.load_data(db, csv_file_path)

    @staticmethod
    def get_company(db: Session, company_id: int = None, name: str = None, industry: str = None, country: str = None) -> list:
        """Looks up companies with optional filtering on id, name, industry, or country."""
        try:
            query = db.query(Company)
            if company_id is not None:
                query = query.filter(Company.company_id == str(company_id))
            if name:
                query = query.filter(Company.name.ilike(f"%{name}%"))
            if industry:
                query = query.filter(Company.industry.ilike(f"%{industry}%"))
            if country:
                query = query.filter(Company.country.ilike(f"%{country}%"))
            return query.all()
        except Exception as e:
            log.exception("Error retrieving company data")
            raise Exception(f"Error retrieving company data: {str(e)}") from e
