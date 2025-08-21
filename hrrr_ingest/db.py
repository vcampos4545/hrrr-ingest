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
    
    def upsert_forecast_data(self, df: pd.DataFrame) -> int:
        """
        Upsert forecast data (insert or update based on unique constraints).
        
        Args:
            df: DataFrame with forecast data
            
        Returns:
            Number of rows affected
        """
        if df.empty:
            logger.warning("No data to upsert")
            return 0
        
        # Ensure table exists
        self.create_forecast_table()
        
        try:
            # For now, just insert the data since we don't have unique constraints
            # In a real implementation, you might want to add unique constraints
            # or implement a more sophisticated upsert logic
            result = self.conn.execute("""
                INSERT INTO hrrr_forecasts 
                SELECT * FROM df
            """)
            
            rows_affected = result.fetchone()[0]
            logger.info(f"Inserted {rows_affected} rows in hrrr_forecasts (upsert mode)")
            return rows_affected
            
        except Exception as e:
            raise RuntimeError(f"Failed to upsert data: {str(e)}") from e
    
    def query_forecast_data(
        self,
        variables: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        lat_min: Optional[float] = None,
        lat_max: Optional[float] = None,
        lon_min: Optional[float] = None,
        lon_max: Optional[float] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query forecast data with optional filters.
        
        Args:
            variables: List of variables to filter by
            start_time: Start time filter (ISO format)
            end_time: End time filter (ISO format)
            lat_min: Minimum latitude
            lat_max: Maximum latitude
            lon_min: Minimum longitude
            lon_max: Maximum longitude
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with filtered results
        """
        query = "SELECT * FROM hrrr_forecasts WHERE 1=1"
        params = []
        
        if variables:
            placeholders = ",".join(["?"] * len(variables))
            query += f" AND variable IN ({placeholders})"
            params.extend(variables)
        
        if start_time:
            query += " AND valid_time_utc >= ?"
            params.append(start_time)
        
        if end_time:
            query += " AND valid_time_utc <= ?"
            params.append(end_time)
        
        if lat_min is not None:
            query += " AND latitude >= ?"
            params.append(lat_min)
        
        if lat_max is not None:
            query += " AND latitude <= ?"
            params.append(lat_max)
        
        if lon_min is not None:
            query += " AND longitude >= ?"
            params.append(lon_min)
        
        if lon_max is not None:
            query += " AND longitude <= ?"
            params.append(lon_max)
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            result = self.conn.execute(query, params)
            df = result.df()
            logger.info(f"Query returned {len(df)} rows")
            return df
            
        except Exception as e:
            raise RuntimeError(f"Failed to query data: {str(e)}") from e
    
    def get_table_info(self) -> dict:
        """
        Get information about the hrrr_forecasts table.
        
        Returns:
            Dictionary with table statistics
        """
        try:
            # Get row count
            count_result = self.conn.execute("SELECT COUNT(*) FROM hrrr_forecasts")
            row_count = count_result.fetchone()[0]
            
            # Get unique variables
            var_result = self.conn.execute("SELECT DISTINCT variable FROM hrrr_forecasts")
            variables = [row[0] for row in var_result.fetchall()]
            
            # Get time range
            time_result = self.conn.execute("""
                SELECT MIN(valid_time_utc), MAX(valid_time_utc) 
                FROM hrrr_forecasts
            """)
            time_range = time_result.fetchone()
            
            return {
                'row_count': row_count,
                'variables': variables,
                'time_range': time_range,
                'db_path': self.db_path
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info: {str(e)}")
            return {}
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed database connection")

def create_forecast_table(db_path: str = "data.db") -> None:
    """
    Convenience function to create the forecast table.
    
    Args:
        db_path: Path to the database file
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
        db_path: Path to the database file
        
    Returns:
        Number of rows inserted
    """
    db = HrrrDatabase(db_path)
    try:
        return db.insert_forecast_data(df)
    finally:
        db.close()

