"""
Unit tests for database module.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime

from hrrr_ingest.db import HrrrDatabase, create_forecast_table, insert_forecast_data


class TestHrrrDatabase:
    """Test HrrrDatabase class functionality."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        # Create a temporary file and immediately close it to get the path
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        # Remove the file so DuckDB can create a fresh database
        Path(temp_path).unlink(missing_ok=True)
        yield temp_path
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        data = {
            'valid_time_utc': [
                datetime(2025, 1, 24, 12, 0, 0),
                datetime(2025, 1, 24, 13, 0, 0)
            ],
            'run_time_utc': [
                datetime(2025, 1, 24, 0, 0, 0),
                datetime(2025, 1, 24, 0, 0, 0)
            ],
            'latitude': [40.7128, 34.0522],
            'longitude': [-74.0060, -118.2437],
            'variable': ['temperature_2m', 'temperature_2m'],
            'value': [15.5, 22.3],
            'source_s3': [
                's3://test/file1.grib2',
                's3://test/file1.grib2'
            ]
        }
        return pd.DataFrame(data)
    
    def test_database_initialization(self, temp_db_path):
        """Test database initialization."""
        db = HrrrDatabase(temp_db_path)
        assert db.db_path == temp_db_path
        assert db.conn is not None
        db.close()
    
    def test_create_forecast_table(self, temp_db_path):
        """Test creating the forecast table."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            
            # Verify table exists by querying it
            result = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hrrr_forecasts'")
            table_exists = result.fetchone() is not None
            assert table_exists
        finally:
            db.close()
    
    def test_insert_forecast_data(self, temp_db_path, sample_dataframe):
        """Test inserting forecast data."""
        db = HrrrDatabase(temp_db_path)
        try:
            # Create table first
            db.create_forecast_table()
            
            # Insert data
            rows_inserted = db.insert_forecast_data(sample_dataframe)
            assert rows_inserted == 2
            
            # Verify data was inserted
            result = db.conn.execute("SELECT COUNT(*) FROM hrrr_forecasts")
            count = result.fetchone()[0]
            assert count == 2
        finally:
            db.close()
    
    def test_insert_empty_dataframe(self, temp_db_path):
        """Test inserting empty DataFrame."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            
            empty_df = pd.DataFrame()
            rows_inserted = db.insert_forecast_data(empty_df)
            assert rows_inserted == 0
        finally:
            db.close()
    

    



class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        # Create a temporary file and immediately close it to get the path
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        # Remove the file so DuckDB can create a fresh database
        Path(temp_path).unlink(missing_ok=True)
        yield temp_path
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    def test_create_forecast_table_function(self, temp_db_path):
        """Test create_forecast_table convenience function."""
        create_forecast_table(temp_db_path)
        
        # Verify table was created
        db = HrrrDatabase(temp_db_path)
        try:
            result = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hrrr_forecasts'")
            table_exists = result.fetchone() is not None
            assert table_exists
        finally:
            db.close()
    
    def test_insert_forecast_data_function(self, temp_db_path):
        """Test insert_forecast_data convenience function."""
        # Create sample data
        data = {
            'valid_time_utc': [datetime(2025, 1, 24, 12, 0, 0)],
            'run_time_utc': [datetime(2025, 1, 24, 0, 0, 0)],
            'latitude': [40.7128],
            'longitude': [-74.0060],
            'variable': ['temperature_2m'],
            'value': [15.5],
            'source_s3': ['s3://test/file.grib2']
        }
        df = pd.DataFrame(data)
        
        # Insert data
        rows_inserted = insert_forecast_data(df, temp_db_path)
        assert rows_inserted == 1
        
        # Verify data was inserted
        db = HrrrDatabase(temp_db_path)
        try:
            result = db.conn.execute("SELECT COUNT(*) FROM hrrr_forecasts")
            count = result.fetchone()[0]
            assert count == 1
        finally:
            db.close()

