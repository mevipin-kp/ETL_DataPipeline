import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy.orm import Session

from backend.db.models.dataload_audit import DataloadAudit

log = logging.getLogger(__name__)


class AuditService:
    """
    Writes one row to dataload_audit for each pipeline zone transition,
    regardless of whether the stage succeeded or failed.

    Failures capture a truncated error message and traceback.
    The record is flushed (not committed) so it participates in the
    caller's transaction for the GOLDEN zone; file-zone calls commit
    immediately since there is no surrounding transaction.
    Audit errors are swallowed — they must never abort the pipeline.
    """

    BRONZE     = "BRONZE"
    SILVER     = "SILVER"
    QUARANTINE = "QUARANTINE"
    GOLDEN     = "GOLDEN"

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

    @staticmethod
    def record(
        db: Session,
        entity: str,
        file_name: str,
        load_uuid: str,
        stage: str,
        status: str,
        row_count: int = 0,
        error: Exception = None,
    ) -> None:
        """Inserts one audit row. Any exception inside is caught and logged — never re-raised."""
        try:
            error_msg = None
            if error is not None:
                tb = traceback.format_exc()
                error_msg = f"{type(error).__name__}: {str(error)}\n{tb}"[:2000]

            audit_row = DataloadAudit(
                entity    = entity,
                file_name = Path(file_name).name,
                load_uuid = load_uuid,
                stage     = stage,
                status    = status,
                row_count = row_count,
                error_msg = error_msg,
                run_at    = datetime.now(timezone.utc),
            )
            db.add(audit_row)
            db.flush()
            log.debug(f"Audit recorded: {entity} | {stage} | {status} | {row_count} rows")
        except Exception as audit_err:
            log.warning(f"Failed to write audit record ({entity}/{stage}/{status}): {audit_err}")

