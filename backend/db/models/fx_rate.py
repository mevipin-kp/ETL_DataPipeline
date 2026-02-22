from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint
from ..session import Base
from datetime import datetime, timezone


class FxRate(Base):
    __tablename__ = "fx_rate"
    __table_args__ = (
        UniqueConstraint('local_currency', 'base_currency', 'date', name='uq_fx_rate_currency_date'),
    )

    id             = Column(Integer, primary_key=True, index=True)
    local_currency = Column(String, nullable=False, index=True)
    base_currency  = Column(String, nullable=False, index=True)
    date           = Column(String, nullable=False)   # stored as YYYY-MM-DD string
    fx_rate        = Column(Float,  nullable=False)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc))

    def _to_json(self):
        return {
            "id":             self.id,
            "local_currency": self.local_currency,
            "base_currency":  self.base_currency,
            "date":           self.date,
            "fx_rate":        self.fx_rate,
            "created_at":     self.created_at.isoformat() if self.created_at else None,
            "updated_at":     self.updated_at.isoformat() if self.updated_at else None,
        }



