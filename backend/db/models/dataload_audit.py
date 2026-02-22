from sqlalchemy import Column, Integer, String, DateTime, Text
from ..session import Base
from datetime import datetime, timezone


class DataloadAudit(Base):
    """
    Tracks every zone transition in a pipeline run. One row is written per
    (load_uuid, stage), covering both successes and failures, so you can
    reconstruct exactly what happened for any given run.
    """
    __tablename__ = "dataload_audit"

    id         = Column(Integer, primary_key=True, index=True)
    entity     = Column(String(50),  nullable=False, index=True)   # company | revenue | expense | fx_rate
    file_name  = Column(String(255), nullable=False)               # source CSV basename
    load_uuid  = Column(String(100), nullable=False, index=True)   # unique ID for this pipeline run
    stage      = Column(String(20),  nullable=False)               # BRONZE | SILVER | QUARANTINE | GOLDEN
    status     = Column(String(10),  nullable=False)               # SUCCESS | FAILURE
    row_count  = Column(Integer,     default=0)
    error_msg  = Column(Text,        nullable=True)                # only set on failures
    run_at     = Column(DateTime,    default=lambda: datetime.now(timezone.utc))

    def _to_json(self):
        return {
            "id":        self.id,
            "entity":    self.entity,
            "file_name": self.file_name,
            "load_uuid": self.load_uuid,
            "stage":     self.stage,
            "status":    self.status,
            "row_count": self.row_count,
            "error_msg": self.error_msg,
            "run_at":    self.run_at.isoformat() if self.run_at else None,
        }

