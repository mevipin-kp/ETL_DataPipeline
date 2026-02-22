from sqlalchemy import Column, Integer, String, Float, Date, DateTime, UniqueConstraint
from ..session import Base
from datetime import datetime, timezone


class CompaniesFinancial(Base):
    __tablename__ = "fact_company_monthly_financials"
    __table_args__ = (
        UniqueConstraint('company_id', 'month', name='uq_fact_financials_company_month'),
    )

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(String(50), nullable=False, index=True)
    month = Column(String(20), nullable=False)
    revenue = Column(Float, nullable=True)
    expenses = Column(Float, nullable=True)
    currency = Column(String(20), nullable=True)
    gross_profit = Column(Float, nullable=True)
    gross_margin_pct = Column(Float, nullable=True)
    mom_revenue_growth_pct = Column(Float, nullable=True)
    rolling_3m_revenue = Column(Float, nullable=True)
    revenue_updated_at = Column(Date, nullable=True)
    expenses_updated_at = Column(Date, nullable=True)
    revenue_pipeline_loaded_at = Column(DateTime, nullable=True)
    expense_pipeline_loaded_at = Column(DateTime, nullable=True)
    revenue_pipeline_load_uuid = Column(String(100), nullable=True)
    expense_pipeline_load_uuid = Column(String(100), nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    def _to_json(self):
        return {
            "id": self.id,
            "company_id": self.company_id,
            "month": self.month,
            "revenue": self.revenue,
            "expenses": self.expenses,
            "currency": self.currency,
            "gross_profit": self.gross_profit,
            "gross_margin_pct": self.gross_margin_pct,
            "mom_revenue_growth_pct": self.mom_revenue_growth_pct,
            "rolling_3m_revenue": self.rolling_3m_revenue,
            "revenue_updated_at": self.revenue_updated_at.isoformat() if self.revenue_updated_at else None,
            "expenses_updated_at": self.expenses_updated_at.isoformat() if self.expenses_updated_at else None,
            "revenue_pipeline_loaded_at": self.revenue_pipeline_loaded_at.isoformat() if self.revenue_pipeline_loaded_at else None,
            "expense_pipeline_loaded_at": self.expense_pipeline_loaded_at.isoformat() if self.expense_pipeline_loaded_at else None,
            "revenue_pipeline_load_uuid": self.revenue_pipeline_load_uuid,
            "expense_pipeline_load_uuid": self.expense_pipeline_load_uuid,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

