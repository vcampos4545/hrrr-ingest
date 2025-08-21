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
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
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
    
    def test_upsert_forecast_data(self, temp_db_path, sample_dataframe):
        """Test upserting forecast data."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            
            # Insert data first
            db.insert_forecast_data(sample_dataframe)
            
            # Modify the DataFrame and upsert
            modified_df = sample_dataframe.copy()
            modified_df.loc[0, 'value'] = 20.0
            
            rows_affected = db.upsert_forecast_data(modified_df)
            assert rows_affected == 2
            
            # Verify the updated value
            result = db.conn.execute("SELECT value FROM hrrr_forecasts WHERE latitude = 40.7128")
            value = result.fetchone()[0]
            assert value == 20.0
        finally:
            db.close()
    
    def test_query_forecast_data(self, temp_db_path, sample_dataframe):
        """Test querying forecast data."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            db.insert_forecast_data(sample_dataframe)
            
            # Query with variable filter
            df = db.query_forecast_data(variables=['temperature_2m'])
            assert len(df) == 2
            assert all(df['variable'] == 'temperature_2m')
            
            # Query with location filter
            df = db.query_forecast_data(lat_min=40.0, lat_max=41.0)
            assert len(df) == 1
            assert df.iloc[0]['latitude'] == 40.7128
            
            # Query with time filter
            start_time = "2025-01-24T12:00:00"
            df = db.query_forecast_data(start_time=start_time)
            assert len(df) == 2
            
            # Query with limit
            df = db.query_forecast_data(limit=1)
            assert len(df) == 1
        finally:
            db.close()
    
    def test_get_table_info(self, temp_db_path, sample_dataframe):
        """Test getting table information."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            db.insert_forecast_data(sample_dataframe)
            
            info = db.get_table_info()
            
            assert info['row_count'] == 2
            assert 'temperature_2m' in info['variables']
            assert info['db_path'] == temp_db_path
            assert info['time_range'] is not None
        finally:
            db.close()
    
    def test_get_table_info_empty_table(self, temp_db_path):
        """Test getting table info for empty table."""
        db = HrrrDatabase(temp_db_path)
        try:
            db.create_forecast_table()
            
            info = db.get_table_info()
            
            assert info['row_count'] == 0
            assert info['variables'] == []
        finally:
            db.close()


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
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

