"""
Unit tests for CLI module.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

from hrrr_ingest.cli import (
    parse_arguments,
    validate_arguments,
    setup_logging,
    process_forecast_hour
)


class TestParseArguments:
    """Test command line argument parsing."""
    
    def test_parse_basic_arguments(self):
        """Test parsing basic required arguments."""
        with patch.object(sys, 'argv', [
            'hrrr-ingest',
            'points.txt',
            '--run-date', '2025-01-24',
            '--variables', 'temperature_2m'
        ]):
            args = parse_arguments()
            
            assert args.points_file == 'points.txt'
            assert args.run_date == '2025-01-24'
            assert args.variables == 'temperature_2m'
            assert args.num_hours == 1  # default
            assert args.db_path == 'data.db'  # default
            assert args.cache_dir == './cache'  # default
            assert args.upsert is False  # default
            assert args.verbose is False  # default
            assert args.dry_run is False  # default
    
    def test_parse_all_arguments(self):
        """Test parsing all available arguments."""
        with patch.object(sys, 'argv', [
            'hrrr-ingest',
            'locations.csv',
            '--run-date', '2025-01-24',
            '--variables', 'temperature_2m,surface_pressure',
            '--num-hours', '6',
            '--db-path', 'forecast.db',
            '--cache-dir', './hrrr_cache',
            '--base-path', 's3://custom-bucket/hrrr',
            '--level-types', 'surface,heightAboveGround',
            '--levels', '2,80',
            '--upsert',
            '--verbose',
            '--dry-run'
        ]):
            args = parse_arguments()
            
            assert args.points_file == 'locations.csv'
            assert args.run_date == '2025-01-24'
            assert args.variables == 'temperature_2m,surface_pressure'
            assert args.num_hours == 6
            assert args.db_path == 'forecast.db'
            assert args.cache_dir == './hrrr_cache'
            assert args.base_path == 's3://custom-bucket/hrrr'
            assert args.level_types == 'surface,heightAboveGround'
            assert args.levels == '2,80'
            assert args.upsert is True
            assert args.verbose is True
            assert args.dry_run is True


class TestValidateArguments:
    """Test argument validation."""
    
    def test_validate_valid_arguments(self):
        """Test validation of valid arguments."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            temp_file = f.name
        
        try:
            args = MagicMock()
            args.points_file = temp_file
            args.run_date = "2025-01-24"
            args.num_hours = 2
            args.variables = "temperature_2m"
            
            # Should not raise any exceptions
            validate_arguments(args)
        finally:
            Path(temp_file).unlink()
    
    def test_validate_nonexistent_points_file(self):
        """Test validation with nonexistent points file."""
        args = MagicMock()
        args.points_file = "nonexistent.txt"
        args.run_date = "2025-01-24"
        args.num_hours = 1
        args.variables = "temperature_2m"
        
        with pytest.raises(ValueError, match="Points file not found"):
            validate_arguments(args)
    
    def test_validate_invalid_run_date(self):
        """Test validation with invalid run date."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            temp_file = f.name
        
        try:
            args = MagicMock()
            args.points_file = temp_file
            args.run_date = "2025/01/24"  # Invalid format
            args.num_hours = 1
            args.variables = "temperature_2m"
            
            with pytest.raises(ValueError, match="Invalid run date"):
                validate_arguments(args)
        finally:
            Path(temp_file).unlink()
    
    def test_validate_invalid_num_hours(self):
        """Test validation with invalid num_hours."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            temp_file = f.name
        
        try:
            args = MagicMock()
            args.points_file = temp_file
            args.run_date = "2025-01-24"
            args.num_hours = 0  # Invalid
            args.variables = "temperature_2m"
            
            with pytest.raises(ValueError, match="num_hours must be between 1 and 48"):
                validate_arguments(args)
        finally:
            Path(temp_file).unlink()
    
    def test_validate_empty_variables(self):
        """Test validation with empty variables."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            temp_file = f.name
        
        try:
            args = MagicMock()
            args.points_file = temp_file
            args.run_date = "2025-01-24"
            args.num_hours = 1
            args.variables = ""  # Empty
            
            with pytest.raises(ValueError, match="Variables cannot be empty"):
                validate_arguments(args)
        finally:
            Path(temp_file).unlink()


class TestSetupLogging:
    """Test logging setup functionality."""
    
    def test_setup_logging_default(self):
        """Test setting up logging with default settings."""
        # This test mainly ensures the function doesn't crash
        setup_logging(verbose=False)
        # No assertions needed as we're just testing it doesn't raise exceptions
    
    def test_setup_logging_verbose(self):
        """Test setting up logging with verbose mode."""
        # This test mainly ensures the function doesn't crash
        setup_logging(verbose=True)
        # No assertions needed as we're just testing it doesn't raise exceptions


class TestProcessForecastHour:
    """Test forecast hour processing functionality."""
    
    @patch('hrrr_ingest.cli.download_grib')
    @patch('hrrr_ingest.cli.build_s3_url')
    @patch('hrrr_ingest.cli.parse_grib_file')
    def test_process_forecast_hour_success(
        self, 
        mock_parse_grib, 
        mock_build_url, 
        mock_download
    ):
        """Test successful forecast hour processing."""
        # Setup mocks
        mock_download.return_value = "/path/to/file.grib2"
        mock_build_url.return_value = "s3://test/file.grib2"
        mock_parse_grib.return_value = [{"variable_name": "temperature_2m"}]
        
        # Test parameters
        run_date = "2025-01-24"
        forecast_hour = 3
        points = [(40.7128, -74.0060)]
        variables = ["temperature_2m"]
        cache_dir = "./cache"
        base_path = "s3://test"
        
        # Call function
        grib_path, parsed_data, source_s3 = process_forecast_hour(
            run_date, forecast_hour, points, variables, cache_dir, base_path
        )
        
        # Verify results
        assert grib_path == "/path/to/file.grib2"
        assert parsed_data == [{"variable_name": "temperature_2m"}]
        assert source_s3 == "s3://test/file.grib2"
        
        # Verify mocks were called correctly
        mock_download.assert_called_once_with(run_date, forecast_hour, cache_dir, base_path)
        mock_build_url.assert_called_once_with(run_date, forecast_hour, base_path)
        mock_parse_grib.assert_called_once_with(
            "/path/to/file.grib2", variables, points, None, None
        )
    
    @patch('hrrr_ingest.cli.download_grib')
    @patch('hrrr_ingest.cli.build_s3_url')
    @patch('hrrr_ingest.cli.parse_grib_file')
    def test_process_forecast_hour_with_filters(
        self, 
        mock_parse_grib, 
        mock_build_url, 
        mock_download
    ):
        """Test forecast hour processing with level filters."""
        # Setup mocks
        mock_download.return_value = "/path/to/file.grib2"
        mock_build_url.return_value = "s3://test/file.grib2"
        mock_parse_grib.return_value = []
        
        # Test parameters with filters
        run_date = "2025-01-24"
        forecast_hour = 0
        points = [(40.7128, -74.0060)]
        variables = ["temperature_2m"]
        cache_dir = "./cache"
        base_path = "s3://test"
        level_types = ["surface"]
        levels = [2]
        
        # Call function
        grib_path, parsed_data, source_s3 = process_forecast_hour(
            run_date, forecast_hour, points, variables, cache_dir, base_path, level_types, levels
        )
        
        # Verify mocks were called with filters
        mock_parse_grib.assert_called_once_with(
            "/path/to/file.grib2", variables, points, level_types, levels
        )

