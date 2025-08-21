"""
Tests for utility functions.
"""

import pytest
import numpy as np
from datetime import datetime
from hrrr_ingest.utils import (
    find_nearest_grid_point, 
    parse_run_date, 
    build_s3_url,
    read_points_file,
    get_allowed_variables,
    validate_variables
)

def test_find_nearest_grid_point():
    """Test finding nearest grid point."""
    # Create a simple 2x2 grid
    lats = np.array([[40.0, 40.1], [41.0, 41.1]])
    lons = np.array([[-74.0, -73.9], [-74.0, -73.9]])
    
    # Test point closest to (40.0, -74.0)
    row, col = find_nearest_grid_point(40.05, -74.05, lats, lons)
    assert row == 0
    assert col == 0
    
    # Test point closest to (41.1, -73.9)
    row, col = find_nearest_grid_point(41.05, -73.85, lats, lons)
    assert row == 1
    assert col == 1

def test_parse_run_date():
    """Test parsing run date."""
    # Valid date
    date = parse_run_date("2025-01-24")
    assert isinstance(date, datetime)
    assert date.year == 2025
    assert date.month == 1
    assert date.day == 24
    
    # Invalid date
    with pytest.raises(ValueError):
        parse_run_date("invalid-date")

def test_build_s3_url():
    """Test building S3 URL."""
    url = build_s3_url("2025-01-24", 3, "s3://test-bucket/hrrr")
    expected = "s3://test-bucket/hrrr.20250124/conus/hrrr.t06z.wrfsfcf03.grib2"
    assert url == expected

def test_read_points_file(tmp_path):
    """Test reading points file."""
    # Create a temporary points file
    points_file = tmp_path / "points.txt"
    points_file.write_text("40.7128,-74.0060\n34.0522,-118.2437\n")
    
    points = read_points_file(str(points_file))
    assert len(points) == 2
    assert points[0] == (40.7128, -74.0060)
    assert points[1] == (34.0522, -118.2437)

def test_get_allowed_variables():
    """Test getting allowed variables."""
    allowed_vars = get_allowed_variables()
    assert isinstance(allowed_vars, set)
    assert "temperature_2m" in allowed_vars
    assert "surface_pressure" in allowed_vars
    assert "u_component_wind_80m" in allowed_vars
    assert len(allowed_vars) == 11  # Total number of allowed variables

def test_validate_variables():
    """Test variable validation."""
    # Valid variables
    validate_variables(["temperature_2m", "surface_pressure"])
    
    # Invalid variables
    with pytest.raises(ValueError, match=r"Invalid variables: \['invalid_var'\]"):
        validate_variables(["temperature_2m", "invalid_var"])
    
    # Empty list
    validate_variables([])

def test_get_last_available_date():
    """Test getting last available date."""
    from hrrr_ingest.utils import get_last_available_date
    
    # Should return a valid date string in YYYY-MM-DD format
    date = get_last_available_date()
    assert isinstance(date, str)
    assert len(date) == 10  # YYYY-MM-DD format
    assert date[4] == '-' and date[7] == '-'
    
    # Should be a valid date
    from datetime import datetime
    parsed_date = datetime.strptime(date, "%Y-%m-%d")
    assert isinstance(parsed_date, datetime)

