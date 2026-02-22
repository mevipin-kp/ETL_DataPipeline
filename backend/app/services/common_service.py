import math
import time
import logging
import uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Tuple, Optional


class PipelineLogFilter(logging.Filter):
    """
    Injects pipeline_load_uuid into every log record produced during a pipeline run.
    Add to a logger at the start of a run and remove in the finally block.
    """
    def __init__(self, load_uuid: str):
        super().__init__()
        self.load_uuid = load_uuid

    def filter(self, record: logging.LogRecord) -> bool:
        record.pipeline_load_uuid = self.load_uuid
        return True


class CommonService:
    """Shared utilities for CSV I/O and pipeline zone writes."""

    @staticmethod
    def read_csv_file(csv_file_path: str, load_uuid: str = None) -> pd.DataFrame:
        """Read CSV file and return a DataFrame enriched with audit metadata."""
        csv_path = Path(csv_file_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file is empty: {csv_file_path}")
        except pd.errors.ParserError as err:
            raise ValueError(f"CSV parsing failed for {csv_file_path}: {err}")
        if df.empty:
            raise ValueError(f"CSV file is empty: {csv_file_path}")
        ts = datetime.now(timezone.utc)
        if load_uuid is None:
            load_uuid = str(uuid.uuid4())
        return df.assign(
            pipeline_loaded_at=ts,
            pipeline_load_uuid=load_uuid,
        )

    @staticmethod
    def write_df(df: pd.DataFrame, csv_file_path: str):
        """Write DataFrame to CSV, ensuring the target directory exists."""
        if df is None:
            raise ValueError("DataFrame to write cannot be None")
        csv_path = Path(csv_file_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_csv(csv_path, index=False)
        except Exception as err:
            raise Exception(f"Error writing DataFrame to CSV {csv_file_path}: {err}")

    @staticmethod
    def write_bronze_and_preprocess(
        df: pd.DataFrame,
        bronze_path: str,
        preprocess_fn: Callable[[pd.DataFrame], pd.DataFrame],
        log_ref: logging.Logger,
        db=None,
        entity: str = "unknown",
        file_name: str = "",
        load_uuid: str = "",
    ) -> Tuple[pd.DataFrame, float]:
        """
        Writes the raw DataFrame to BRONZE and runs preprocess_fn at the same time.
        If the BRONZE write fails the pipeline aborts immediately — nothing is
        written to the DB. Returns the preprocessed DataFrame and elapsed time.
        """
        # Import here to avoid circular import
        from backend.app.services.audit_service import AuditService

        t = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                bronze_future     = executor.submit(CommonService.write_df, df, bronze_path)
                preprocess_future = executor.submit(preprocess_fn, df.copy())
                bronze_future.result()
                preprocessed = preprocess_future.result()
        except Exception as err:
            log_ref.exception("BRONZE write failed — aborting pipeline, no DB write will occur")
            if db is not None:
                AuditService.record(db, entity, file_name, load_uuid,
                                    AuditService.BRONZE, AuditService.FAILURE,
                                    row_count=0, error=err)
                try:
                    db.commit()
                except Exception:
                    pass
            raise RuntimeError(f"BRONZE write failed: {err}") from err

        elapsed = time.perf_counter() - t
        log_ref.info(f"BRONZE write + preprocess done in {elapsed:.2f}s")

        if db is not None:
            AuditService.record(db, entity, file_name, load_uuid,
                                AuditService.BRONZE, AuditService.SUCCESS,
                                row_count=len(df))
            try:
                db.commit()
            except Exception:
                pass

        return preprocessed, elapsed

    @staticmethod
    def write_silver_and_quarantine(
        df_clean: pd.DataFrame,
        df_orphan: pd.DataFrame,
        silver_path: str,
        quarantine_path: str,
        db,
        log_ref: logging.Logger,
        entity: str = "unknown",
        file_name: str = "",
        load_uuid: str = "",
    ) -> None:
        """
        Writes clean rows to SILVER and quarantined rows to QUARANTINE in parallel.
        Both writes must succeed before the GOLDEN DB upsert is allowed to proceed.
        If either fails the pipeline raises RuntimeError and the DB is left untouched.
        """
        from backend.app.services.audit_service import AuditService

        t = time.perf_counter()
        zone_executor = ThreadPoolExecutor(max_workers=2)
        silver_future     = zone_executor.submit(CommonService.write_df, df_clean.copy(),  silver_path)
        quarantine_future = zone_executor.submit(CommonService.write_df, df_orphan.copy(), quarantine_path)

        # --- SILVER ---
        try:
            silver_future.result()
            log_ref.info(f"SILVER write done in {time.perf_counter() - t:.2f}s")
            AuditService.record(db, entity, file_name, load_uuid,
                                AuditService.SILVER, AuditService.SUCCESS,
                                row_count=len(df_clean))
            db.commit()
        except Exception as err:
            zone_executor.shutdown(wait=False)
            log_ref.exception("SILVER write failed — aborting pipeline before DB write")
            AuditService.record(db, entity, file_name, load_uuid,
                                AuditService.SILVER, AuditService.FAILURE,
                                row_count=0, error=err)
            try:
                db.commit()
            except Exception:
                pass
            raise RuntimeError(f"SILVER write failed: {err}") from err

        # --- QUARANTINE ---
        try:
            quarantine_future.result()
            log_ref.info(f"QUARANTINE write done ({len(df_orphan)} rows) in {time.perf_counter() - t:.2f}s")
            AuditService.record(db, entity, file_name, load_uuid,
                                AuditService.QUARANTINE, AuditService.SUCCESS,
                                row_count=len(df_orphan))
            db.commit()
        except Exception as err:
            zone_executor.shutdown(wait=False)
            log_ref.exception("QUARANTINE write failed — aborting pipeline before DB write")
            AuditService.record(db, entity, file_name, load_uuid,
                                AuditService.QUARANTINE, AuditService.FAILURE,
                                row_count=0, error=err)
            try:
                db.commit()
            except Exception:
                pass
            raise RuntimeError(f"QUARANTINE write failed: {err}") from err

        zone_executor.shutdown(wait=False)

    @staticmethod
    def sanitise(obj):
        """Recursively replace NaN/Inf float values with None for JSON safety."""
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: CommonService.sanitise(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [CommonService.sanitise(i) for i in obj]
        return obj

    @staticmethod
    def get_currency_map():
        """Return a mapping of currency codes to their symbols."""
        return {
            "EUR": "EUR", "EURO": "EUR", "eur": "EUR",
            "USD": "USD", "usd": "USD", "USDD": "USD",
            "GBP": "GBP", "gbp": "GBP", "GBR": "GBP",
            "INR": "INR", "inr": "INR",
            "AED": "AED", "aed": "AED", "AEDX": "AED",
            "CAD": "CAD", "cad": "CAD",
            "SGD": "SGD", "sgd": "SGD",
            "CHF": "CHF", "chf": "CHF",
        }