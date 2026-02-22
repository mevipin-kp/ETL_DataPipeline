from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.pool import StaticPool, QueuePool
from typing import Generator
import os

Base = declarative_base()

_engine = None
_SessionLocal = None


def _set_sqlite_pragma(dbapi_conn, _connection_record):
    """
    Turns on foreign key enforcement for every SQLite connection.
    SQLite ignores FK constraints by default, so we hook this into
    the pool's connect event to make sure it fires on every new connection.
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()


def init_engine(url: str = "sqlite:///:memory:", ddl_script_path: str = None):
    """
    Sets up the SQLAlchemy engine and session factory.

    In-memory SQLite uses StaticPool with check_same_thread=False so the
    same DB is shared across all sessions for the lifetime of the process.
    Any other DB uses QueuePool with reasonable defaults.
    """
    global _engine, _SessionLocal
    if _engine is None:
        if "sqlite" in url:
            pool_config = {"poolclass": StaticPool}
        else:
            pool_config = {
                "poolclass": QueuePool,
                "pool_size": 10,
                "max_overflow": 20,
                "pool_pre_ping": True,
            }

        _engine = create_engine(
            url,
            echo=False,
            future=True,
            connect_args={"check_same_thread": False} if "sqlite" in url else {},
            **pool_config,
        )

        # Hook in the FK pragma for every new SQLite connection
        if "sqlite" in url:
            event.listen(_engine, "connect", _set_sqlite_pragma)

        _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, class_=Session)

        if ddl_script_path:
            execute_ddl_script(_engine, ddl_script_path)

    return _engine


def execute_ddl_script(engine, script_path: str):
    """Reads a SQL file and runs each semicolon-delimited statement against the engine."""
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"DDL script not found: {script_path}")

    with open(script_path, "r") as f:
        sql_script = f.read()

    # Split on semicolons to handle multi-statement scripts
    statements = [s.strip() for s in sql_script.split(";") if s.strip()]

    with engine.begin() as connection:
        for statement in statements:
            if statement:
                connection.execute(text(statement))


def get_session() -> Generator:
    """Yields a SQLAlchemy session. Used as a FastAPI dependency."""
    if _SessionLocal is None:
        raise RuntimeError("Engine not initialized. Call init_engine first.")
    db: Session = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
