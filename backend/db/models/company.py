from sqlalchemy import Column, Integer, String, DateTime
from ..session import Base
from datetime import datetime, timezone


class Company(Base):
    __tablename__ = "dim_company"
    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    industry = Column(String, nullable=False)
    country = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    pipeline_loaded_at= Column(DateTime,nullable=False)
    pipeline_load_uuid = Column(String, nullable=False)

    def _to_json(self):
        return {
            "id": self.id,
            "company_id": self.company_id,
            "name": self.name,
            "industry": self.industry,
            "country": self.country,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "pipeline_loaded_at": self.pipeline_loaded_at.isoformat() if self.pipeline_loaded_at else None,
            "pipeline_load_uuid": self.pipeline_load_uuid,
        }

