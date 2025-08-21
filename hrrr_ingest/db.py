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
        
        Unique constraint on (valid_time_utc, run_time_utc, latitude, longitude, variable, source_s3)
        to prevent duplicate insertions.
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS hrrr_forecasts (
            valid_time_utc TIMESTAMP,
            run_time_utc TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            variable VARCHAR,
            value FLOAT,
            source_s3 VARCHAR,
            UNIQUE(valid_time_utc, run_time_utc, latitude, longitude, variable, source_s3)
        )
        """
        
        try:
            self.conn.execute(create_table_sql)
            logger.info("Created hrrr_forecasts table with unique constraint")
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
        Insert forecast data into the database, ignoring duplicates.
        
        Args:
            df: DataFrame with forecast data in long format
            
        Returns:
            Number of rows inserted (duplicates are ignored)
        """
        if df.empty:
            logger.warning("No data to insert")
            return 0
        
        # Ensure table exists
        self.create_forecast_table()
        
        try:
            # Get count before insertion to detect duplicates
            result = self.conn.execute("SELECT COUNT(*) FROM hrrr_forecasts")
            count_before = result.fetchone()[0]
            
            # Insert data using DuckDB's DataFrame support with OR IGNORE
            self.conn.execute("""
                INSERT OR IGNORE INTO hrrr_forecasts 
                SELECT * FROM df
            """)
            
            # Get count after insertion
            result = self.conn.execute("SELECT COUNT(*) FROM hrrr_forecasts")
            count_after = result.fetchone()[0]
            
            # Calculate actual rows inserted and duplicates skipped
            actual_inserted = count_after - count_before
            duplicates_skipped = len(df) - actual_inserted
            
            if duplicates_skipped > 0:
                logger.info(f"Inserted {actual_inserted} new rows, skipped {duplicates_skipped} duplicates")
            else:
                logger.info(f"Inserted {actual_inserted} rows into hrrr_forecasts")
            
            return actual_inserted
            
        except Exception as e:
            raise RuntimeError(f"Failed to insert data: {str(e)}") from e
    
    def check_existing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check which rows in the DataFrame already exist in the database.
        
        Args:
            df: DataFrame with forecast data to check
            
        Returns:
            DataFrame containing only the rows that already exist in the database
        """
        if df.empty:
            return pd.DataFrame()
        
        # Ensure table exists
        self.create_forecast_table()
        
        try:
            # Use a more direct approach to check for existing data
            # Create a query that checks each row individually
            existing_rows = []
            
            for _, row in df.iterrows():
                # Check if this specific row exists, using approximate comparison for floats
                result = self.conn.execute("""
                    SELECT COUNT(*) FROM hrrr_forecasts 
                    WHERE valid_time_utc = ? 
                    AND run_time_utc = ? 
                    AND ABS(latitude - ?) < 0.0001
                    AND ABS(longitude - ?) < 0.0001
                    AND variable = ? 
                    AND source_s3 = ?
                """, [row['valid_time_utc'], row['run_time_utc'], row['latitude'], 
                      row['longitude'], row['variable'], row['source_s3']])
                
                count = result.fetchone()[0]
                if count > 0:
                    existing_rows.append(row.to_dict())
            
            return pd.DataFrame(existing_rows)
            
        except Exception as e:
            raise RuntimeError(f"Failed to check existing data: {str(e)}") from e
    
    def get_duplicate_count(self, df: pd.DataFrame) -> int:
        """
        Get the count of rows that would be duplicates if inserted.
        
        Args:
            df: DataFrame with forecast data to check
            
        Returns:
            Number of rows that already exist in the database
        """
        existing_data = self.check_existing_data(df)
        return len(existing_data)

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
    Convenience function to insert forecast data, ignoring duplicates.
    
    Args:
        df: DataFrame with forecast data
        db_path: Path to the DuckDB database file
        
    Returns:
        Number of rows inserted (duplicates are ignored)
    """
    db = HrrrDatabase(db_path)
    try:
        return db.insert_forecast_data(df)
    finally:
        db.close()

def check_existing_data(df: pd.DataFrame, db_path: str = "data.db") -> pd.DataFrame:
    """
    Convenience function to check which rows already exist in the database.
    
    Args:
        df: DataFrame with forecast data to check
        db_path: Path to the DuckDB database file
        
    Returns:
        DataFrame containing only the rows that already exist in the database
    """
    db = HrrrDatabase(db_path)
    try:
        return db.check_existing_data(df)
    finally:
        db.close()

def get_duplicate_count(df: pd.DataFrame, db_path: str = "data.db") -> int:
    """
    Convenience function to get the count of rows that would be duplicates.
    
    Args:
        df: DataFrame with forecast data to check
        db_path: Path to the DuckDB database file
        
    Returns:
        Number of rows that already exist in the database
    """
    db = HrrrDatabase(db_path)
    try:
        return db.get_duplicate_count(df)
    finally:
        db.close()

