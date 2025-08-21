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
    get_grib_variable_name,
    get_variable_level_config,
    validate_variables,
    map_variables_to_grib_names,
    get_variable_levels_for_filtering
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
    expected = "s3://test-bucket/hrrr.20250124/conus/hrrr.t00z.wrfsfcf03.grib2"
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

def test_get_grib_variable_name():
    """Test mapping argument names to grib names."""
    # Valid mappings
    assert get_grib_variable_name("temperature_2m") == "2 metre temperature"
    assert get_grib_variable_name("surface_pressure") == "Surface pressure"
    assert get_grib_variable_name("u_component_wind_80m") == "U component of wind"
    
    # Invalid variable
    with pytest.raises(ValueError, match="Variable 'invalid_var' is not allowed"):
        get_grib_variable_name("invalid_var")

def test_get_variable_level_config():
    """Test getting variable level configurations."""
    # Variables with level configs
    assert get_variable_level_config("u_component_wind_80m") == {"level": 80}
    assert get_variable_level_config("v_component_wind_80m") == {"level": 80}
    
    # Variables without level configs
    assert get_variable_level_config("temperature_2m") == {}
    assert get_variable_level_config("surface_pressure") == {}

def test_validate_variables():
    """Test variable validation."""
    # Valid variables
    validate_variables(["temperature_2m", "surface_pressure"])
    
    # Invalid variables
    with pytest.raises(ValueError, match=r"Invalid variables: \['invalid_var'\]"):
        validate_variables(["temperature_2m", "invalid_var"])
    
    # Empty list
    validate_variables([])

def test_map_variables_to_grib_names():
    """Test mapping variables to grib names."""
    # Valid mapping
    grib_names = map_variables_to_grib_names(["temperature_2m", "surface_pressure"])
    assert grib_names == ["2 metre temperature", "Surface pressure"]
    
    # Invalid variable should raise error
    with pytest.raises(ValueError):
        map_variables_to_grib_names(["temperature_2m", "invalid_var"])

def test_get_variable_levels_for_filtering():
    """Test getting level configurations for filtering."""
    # Variables with level configs
    level_types, levels = get_variable_levels_for_filtering(["u_component_wind_80m", "v_component_wind_80m"])
    assert levels == [80]
    assert level_types == []
    
    # Variables without level configs
    level_types, levels = get_variable_levels_for_filtering(["temperature_2m", "surface_pressure"])
    assert levels == []
    assert level_types == []
    
    # Mixed variables
    level_types, levels = get_variable_levels_for_filtering(["temperature_2m", "u_component_wind_80m"])
    assert levels == [80]
    assert level_types == []

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

