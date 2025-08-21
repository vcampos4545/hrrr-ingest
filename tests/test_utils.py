"""
Unit tests for utils module.
"""

import pytest
import numpy as np
from datetime import datetime
from pathlib import Path
import tempfile

from hrrr_ingest.utils import (
    find_nearest_grid_point,
    parse_run_date,
    calculate_valid_time,
    build_s3_url,
    validate_lat_lon,
    read_points_file
)


class TestFindNearestGridPoint:
    """Test nearest grid point finding functionality."""
    
    def test_find_nearest_grid_point(self):
        """Test finding nearest grid point to target coordinates."""
        # Create a simple 3x3 grid
        lats = np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]])
        lons = np.array([[0, 0, 0], [1, 1, 1], [2, 2, 2]])
        
        # Target point at (1.1, 1.1) should be closest to grid point (1, 1)
        row, col = find_nearest_grid_point(1.1, 1.1, lats, lons)
        assert row == 1
        assert col == 1
    
    def test_find_nearest_grid_point_edge_case(self):
        """Test edge case where target is exactly on a grid point."""
        lats = np.array([[0, 1], [0, 1]])
        lons = np.array([[0, 0], [1, 1]])
        
        row, col = find_nearest_grid_point(0, 0, lats, lons)
        assert row == 0
        assert col == 0


class TestParseRunDate:
    """Test run date parsing functionality."""
    
    def test_parse_valid_date(self):
        """Test parsing a valid date string."""
        date_str = "2025-01-24"
        result = parse_run_date(date_str)
        expected = datetime(2025, 1, 24)
        assert result == expected
    
    def test_parse_invalid_date(self):
        """Test parsing an invalid date string."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_run_date("2025/01/24")
    
    def test_parse_malformed_date(self):
        """Test parsing a malformed date string."""
        with pytest.raises(ValueError):
            parse_run_date("not-a-date")


class TestCalculateValidTime:
    """Test valid time calculation functionality."""
    
    def test_calculate_valid_time(self):
        """Test calculating valid time from run time and forecast hour."""
        run_time = datetime(2025, 1, 24, 12, 0, 0)
        forecast_hour = 3
        
        valid_time = calculate_valid_time(run_time, forecast_hour)
        expected = datetime(2025, 1, 24, 15, 0, 0)
        assert valid_time == expected


class TestBuildS3Url:
    """Test S3 URL building functionality."""
    
    def test_build_s3_url(self):
        """Test building S3 URL for HRRR data."""
        run_date = "2025-01-24"
        forecast_hour = 3
        
        url = build_s3_url(run_date, forecast_hour)
        expected = "s3://noaa-hrrr-bdp-pds/hrrr.20250124/conus/hrrr.t00z.wrfsfcf03.grib2"
        assert url == expected
    
    def test_build_s3_url_custom_base(self):
        """Test building S3 URL with custom base path."""
        run_date = "2025-01-24"
        forecast_hour = 0
        base_path = "s3://custom-bucket/hrrr"
        
        url = build_s3_url(run_date, forecast_hour, base_path)
        expected = "s3://custom-bucket/hrrr.20250124/conus/hrrr.t00z.wrfsfcf00.grib2"
        assert url == expected


class TestValidateLatLon:
    """Test latitude/longitude validation functionality."""
    
    def test_valid_coordinates(self):
        """Test valid coordinate pairs."""
        assert validate_lat_lon(0, 0) is True
        assert validate_lat_lon(90, 180) is True
        assert validate_lat_lon(-90, -180) is True
        assert validate_lat_lon(45.5, -120.3) is True
    
    def test_invalid_latitude(self):
        """Test invalid latitude values."""
        assert validate_lat_lon(91, 0) is False
        assert validate_lat_lon(-91, 0) is False
    
    def test_invalid_longitude(self):
        """Test invalid longitude values."""
        assert validate_lat_lon(0, 181) is False
        assert validate_lat_lon(0, -181) is False


class TestReadPointsFile:
    """Test points file reading functionality."""
    
    def test_read_valid_points_file(self):
        """Test reading a valid points file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            f.write("34.0522,-118.2437\n")
            f.write("41.8781,-87.6298\n")
            temp_file = f.name
        
        try:
            points = read_points_file(temp_file)
            expected = [
                (40.7128, -74.0060),
                (34.0522, -118.2437),
                (41.8781, -87.6298)
            ]
            assert points == expected
        finally:
            Path(temp_file).unlink()
    
    def test_read_points_file_with_comments(self):
        """Test reading points file with comments."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("# This is a comment\n")
            f.write("40.7128,-74.0060  # New York\n")
            f.write("\n")  # Empty line
            f.write("34.0522,-118.2437\n")
            temp_file = f.name
        
        try:
            points = read_points_file(temp_file)
            expected = [
                (40.7128, -74.0060),
                (34.0522, -118.2437)
            ]
            assert points == expected
        finally:
            Path(temp_file).unlink()
    
    def test_read_points_file_invalid_format(self):
        """Test reading points file with invalid format."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            f.write("invalid,format\n")
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Line 2:"):
                read_points_file(temp_file)
        finally:
            Path(temp_file).unlink()
    
    def test_read_points_file_invalid_coordinates(self):
        """Test reading points file with invalid coordinates."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("40.7128,-74.0060\n")
            f.write("91.0,0.0\n")  # Invalid latitude
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Line 2:"):
                read_points_file(temp_file)
        finally:
            Path(temp_file).unlink()
    
    def test_read_nonexistent_file(self):
        """Test reading a nonexistent file."""
        with pytest.raises(FileNotFoundError):
            read_points_file("nonexistent.txt")
    
    def test_read_empty_file(self):
        """Test reading an empty file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("")  # Empty file
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError, match="No valid points found"):
                read_points_file(temp_file)
        finally:
            Path(temp_file).unlink()

