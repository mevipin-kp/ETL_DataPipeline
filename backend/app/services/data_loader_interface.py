from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
import pandas as pd


class DataLoaderInterface(ABC):
    """Abstract base that all data loader services must implement."""

    @staticmethod
    @abstractmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list) -> list:
        """Returns any column names from required_columns that are absent in df."""
        pass

    @staticmethod
    @abstractmethod
    def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and normalises the raw DataFrame before validation."""
        pass

    @staticmethod
    @abstractmethod
    def cleanse_data(df: pd.DataFrame, db: Optional[Session] = None) -> tuple:
        """
        Splits the preprocessed DataFrame into valid and quarantined rows.
        Pass db when the cleanse logic needs to cross-reference other tables.
        Returns (valid_df, orphan_df).
        """
        pass

    @staticmethod
    @abstractmethod
    def load_to_database(db: Session, df: pd.DataFrame, total_rows: int, csv_file_path: str) -> Dict[str, Any]:
        """
        Upserts validated rows into the database.
        Returns a summary dict with inserted, updated, and skipped counts.
        """
        pass

    @staticmethod
    @abstractmethod
    def load_data(db: Session, csv_file_path: str) -> Dict[str, Any]:
        """
        Orchestrates the full load: read CSV → BRONZE → preprocess → cleanse
        → SILVER/QUARANTINE → GOLDEN DB write.
        Returns a summary dict with counts and the pipeline run UUID.
        """
        pass

