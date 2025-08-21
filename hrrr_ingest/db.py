"""
Database module for DuckDB integration.
"""

import duckdb
import pandas as pd
from typing import Optional, List
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class HrrrDatabase:
    """DuckDB database manager for HRRR forecast data."""
    
    def __init__(self, db_path: str = "data.db"):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB database: {self.db_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database {self.db_path}: {str(e)}") from e
    
    def create_forecast_table(self) -> None:
        """
        Create the hrrr_forecasts table if it doesn't exist.
        
        Table schema:
        - valid_time_utc: TIMESTAMP
        - run_time_utc: TIMESTAMP  
        - latitude: FLOAT
        - longitude: FLOAT
        - variable: VARCHAR
        - value: FLOAT
        - source_s3: VARCHAR
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS hrrr_forecasts (
            valid_time_utc TIMESTAMP,
            run_time_utc TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            variable VARCHAR,
            value FLOAT,
            source_s3 VARCHAR
        )
        """
        
        try:
            self.conn.execute(create_table_sql)
            logger.info("Created hrrr_forecasts table")
        except Exception as e:
            raise RuntimeError(f"Failed to create table: {str(e)}") from e
    
    def create_indexes(self) -> None:
        """
        Create indexes for better query performance.
        """
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_valid_time ON hrrr_forecasts(valid_time_utc)",
            "CREATE INDEX IF NOT EXISTS idx_location ON hrrr_forecasts(latitude, longitude)",
            "CREATE INDEX IF NOT EXISTS idx_variable ON hrrr_forecasts(variable)",
            "CREATE INDEX IF NOT EXISTS idx_run_time ON hrrr_forecasts(run_time_utc)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
                logger.info(f"Created index: {index_sql}")
            except Exception as e:
                logger.warning(f"Failed to create index: {str(e)}")
    
    def insert_forecast_data(self, df: pd.DataFrame) -> int:
        """
        Insert forecast data into the database.
        
        Args:
            df: DataFrame with forecast data in long format
            
        Returns:
            Number of rows inserted
        """
        if df.empty:
            logger.warning("No data to insert")
            return 0
        
        # Ensure table exists
        self.create_forecast_table()
        
        try:
            # Insert data using DuckDB's DataFrame support
            result = self.conn.execute("""
                INSERT INTO hrrr_forecasts 
                SELECT * FROM df
            """)
            
            rows_inserted = result.fetchone()[0]
            logger.info(f"Inserted {rows_inserted} rows into hrrr_forecasts")
            return rows_inserted
            
        except Exception as e:
            raise RuntimeError(f"Failed to insert data: {str(e)}") from e
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed database connection")


# Convenience functions for external use

def create_forecast_table(db_path: str = "data.db") -> None:
    """
    Convenience function to create the forecast table.
    
    Args:
        db_path: Path to the DuckDB database file
    """
    db = HrrrDatabase(db_path)
    try:
        db.create_forecast_table()
        db.create_indexes()
    finally:
        db.close()

def insert_forecast_data(df: pd.DataFrame, db_path: str = "data.db") -> int:
    """
    Convenience function to insert forecast data.
    
    Args:
        df: DataFrame with forecast data
        db_path: Path to the DuckDB database file
        
    Returns:
        Number of rows inserted
    """
    db = HrrrDatabase(db_path)
    try:
        return db.insert_forecast_data(df)
    finally:
        db.close()

