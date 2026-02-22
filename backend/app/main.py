import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from .controllers.data_master import router as data_master_router

from ..db.session import init_engine
from ..db.session import Base
from pathlib import Path


_LOG_FMT  = "%(asctime)s | %(levelname)-8s | %(pipeline_load_uuid)-36s | %(name)s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


class _PipelineFormatter(logging.Formatter):
    """
    Injects a fallback pipeline_load_uuid of 'N/A' into any log record
    that doesn't already carry one. Overrides formatMessage rather than
    format so the field is in place before the % substitution runs.
    """
    def formatMessage(self, record: logging.LogRecord) -> str:
        if not hasattr(record, 'pipeline_load_uuid'):
            record.pipeline_load_uuid = 'N/A'
        return super().formatMessage(record)


def _make_formatter() -> _PipelineFormatter:
    return _PipelineFormatter(fmt=_LOG_FMT, datefmt=_DATE_FMT)


def _apply_formatter_to_all_handlers() -> None:
    """
    Stamp our formatter onto every handler attached to the root logger
    and uvicorn's named loggers. Safe to call more than once.
    """
    fmt = _make_formatter()
    for name in (None, "uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"):
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            handler.setFormatter(fmt)


def _configure_logging() -> None:
    """
    Set up the root logger at import time. basicConfig adds a StreamHandler
    and we immediately swap in our formatter. Uvicorn registers its own
    handlers later, so we call _apply_formatter_to_all_handlers again
    inside lifespan to catch those too.
    """
    logging.basicConfig(level=logging.INFO)
    _apply_formatter_to_all_handlers()


_configure_logging()

# Store engine reference globally for access in shutdown
_db_engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles DB startup (table creation) and teardown (pool disposal)."""
    global _db_engine

    # Re-apply formatter now that uvicorn has added its own handlers
    _apply_formatter_to_all_handlers()

    base_dir = Path(__file__).parent.parent.parent
    ddl_script_path = base_dir / "docker" / "init_tables.sql"

    _db_engine = init_engine(ddl_script_path=str(ddl_script_path))
    Base.metadata.create_all(bind=_db_engine)

    print(f"Database initialized successfully with DDL from {ddl_script_path}")

    yield

    if _db_engine is not None:
        _db_engine.dispose()
        print("Database connection pool disposed")


app = FastAPI(title="Backend API", lifespan=lifespan)
app.include_router(data_master_router)





