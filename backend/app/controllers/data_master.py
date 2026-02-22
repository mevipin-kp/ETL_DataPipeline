import logging

from fastapi import APIRouter, HTTPException, Depends
from pathlib import Path
from sqlalchemy.orm import Session

from ...db.session import get_session
from backend.app.services.fx_rate_service import FxRateService
from backend.app.services.company_service import CompanyService
from backend.app.services.expense_service import ExpenseService
from backend.app.services.revenue_service import RevenueService
from backend.app.services.financial_service import FinancialService
from backend.app.services.common_service import CommonService
from backend.db.models import DataloadAudit

log = logging.getLogger(__name__)

router = APIRouter()


@router.post("/fx_rate")
def load_fx_rate(db: Session = Depends(get_session)):
    """Loads FX rate data from the local CSV into the database."""
    try:
        csv_file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "fx_rate.csv"
        result = FxRateService.load_data(db, str(csv_file_path))
        return result
    except FileNotFoundError as e:
        log.exception("fx_rate CSV not found")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.exception("Failed to load fx_rate data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fx_rate")
def get_fx_rates(db: Session = Depends(get_session), local_currency: str = None, base_currency: str = None, date: str = None):
    """Returns FX rates, optionally filtered by currency pair or date."""
    try:
        return FxRateService.get_fx_rates(db, local_currency, base_currency, date)
    except Exception as e:
        log.exception("Failed to retrieve fx_rate data")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/company")
def load_company(db: Session = Depends(get_session)):
    """Loads company master data from the local CSV into the database."""
    try:
        csv_file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "companies.csv"
        result = CompanyService.load_data(db, str(csv_file_path))
        return CommonService.sanitise(result)
    except Exception as e:
        log.exception("Failed to load company data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/company")
def get_company(db: Session = Depends(get_session), company_id: int = None, name: str = None, industry: str = None, country: str = None):
    """Returns companies, with optional filtering on id, name, industry, or country."""
    try:
        records = CompanyService.get_company(db, company_id, name, industry, country)
        log.info(f"Retrieved {len(records)} company records - company_id: {company_id}, name: {name}, industry: {industry}, country: {country}")
        return [record._to_json() for record in records]
    except Exception as e:
        log.exception("Failed to retrieve company data")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/expense")
def load_expense(db: Session = Depends(get_session)):
    """Loads monthly expense data from the local CSV into the database."""
    try:
        csv_file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "monthly_expenses.csv"
        result = ExpenseService.load_data(db, str(csv_file_path))
        return CommonService.sanitise(result)
    except Exception as e:
        log.exception("Failed to load expense data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/expense")
def get_expense(
    db: Session = Depends(get_session),
    company_id: str = None,
    month: str = None,
):
    """Returns expense rows, optionally filtered by company_id or month."""
    try:
        records = ExpenseService.get_expense(db, company_id, month)
        log.info(f"Retrieved {len(records)} expense records - company_id: {company_id}, month: {month}")
        return CommonService.sanitise([r._to_json() for r in records])
    except Exception as e:
        log.exception("Failed to retrieve expense data")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/revenue")
def load_revenue(db: Session = Depends(get_session)):
    """Loads monthly revenue data from the local CSV into the database."""
    try:
        csv_file_path = Path(__file__).resolve().parent.parent.parent / "resources" / "monthly_revenue.csv"
        result = RevenueService.load_data(db, str(csv_file_path))
        return CommonService.sanitise(result)
    except Exception as e:
        log.exception("Failed to load revenue data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/revenue")
def get_revenue(
    db: Session = Depends(get_session),
    company_id: str = None,
    month: str = None,
    currency: str = None,
):
    """Returns revenue rows, optionally filtered by company_id, month, or currency."""
    try:
        records = RevenueService.get_revenue(db, company_id, month, currency)
        log.info(f"Retrieved {len(records)} revenue records - company_id: {company_id}, month: {month}, currency: {currency}")
        return CommonService.sanitise([r._to_json() for r in records])
    except Exception as e:
        log.exception("Failed to retrieve revenue data")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/financials/calculate")
def calculate_financials(db: Session = Depends(get_session), company_id: str = None):
    """
    Runs the metrics calculation (gross_profit, gross_margin_pct,
    mom_revenue_growth_pct, rolling_3m_revenue) over rows that have both
    revenue and expenses, then writes the results back.

    Pass ?company_id=... to limit the run to a single company.
    """
    try:
        result = FinancialService.calculate_and_save(db, company_id)
        return CommonService.sanitise(result)
    except Exception as e:
        log.exception("Failed to calculate financials")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/financials")
def get_financials(
    db: Session = Depends(get_session),
    company_id: str = None,
    month: str = None,
    currency: str = None,
):
    """Fetch financial records. Filter by company_id, month (YYYY-MM), or currency."""
    try:
        records = FinancialService.get_financials(db, company_id, month, currency)
        log.info(f"Retrieved {len(records)} financial records - company_id: {company_id}, month: {month}, currency: {currency}")
        return CommonService.sanitise(records)
    except Exception as e:
        log.exception("Failed to retrieve financial data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/audit")
def get_audit(
    db: Session = Depends(get_session),
    entity: str = None,
    load_uuid: str = None,
    stage: str = None,
    status: str = None,
):
    """
    Pull audit records. All filters are optional:
    entity (company/revenue/expense/fx_rate), load_uuid, stage, status.
    Results are sorted newest-first.
    """
    try:
        query = db.query(DataloadAudit)
        if entity:
            query = query.filter(DataloadAudit.entity == entity)
        if load_uuid:
            query = query.filter(DataloadAudit.load_uuid == load_uuid)
        if stage:
            query = query.filter(DataloadAudit.stage == stage.upper())
        if status:
            query = query.filter(DataloadAudit.status == status.upper())
        records = query.order_by(DataloadAudit.run_at.desc()).all()
        log.info(f"Retrieved {len(records)} audit records")
        return [r._to_json() for r in records]
    except Exception as e:
        log.exception("Failed to retrieve audit data")
        raise HTTPException(status_code=500, detail=str(e))
