"""
Shared utilities for HRRR ingest operations.
"""

import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, List, Optional, Dict, Set
import logging
import requests

logger = logging.getLogger(__name__)

# Allowed variables that can be passed as arguments
ALLOWED_VARIABLES = {
    "surface_pressure": "Surface pressure",
    "surface_roughness": "Surface roughness", 
    "visible_beam_downward_solar_flux": "Visible beam downward solar flux",
    "visible_diffuse_downward_solar_flux": "Visible diffuse downward solar flux",
    "temperature_2m": "2 metre temperature",
    "dewpoint_2m": "2 metre dewpoint temperature",
    "relative_humidity_2m": "2 metre relative humidity",
    "u_component_wind_10m": "10 metre U wind component",
    "v_component_wind_10m": "10 metre V wind component",
    "u_component_wind_80m": "U component of wind",
    "v_component_wind_80m": "V component of wind"
}

# Variable level configurations for variables that require specific levels
VARIABLE_LEVELS = {
    "u_component_wind_80m": {"level": 80},
    "v_component_wind_80m": {"level": 80}
}

def get_allowed_variables() -> Set[str]:
    """
    Get the set of allowed variable names that can be passed as arguments.
    
    Returns:
        Set of allowed variable names
    """
    return set(ALLOWED_VARIABLES.keys())

def get_grib_variable_name(argument_name: str) -> str:
    """
    Get the actual grib variable name for a given argument name.
    
    Args:
        argument_name: The variable name passed as an argument
        
    Returns:
        The actual grib variable name
        
    Raises:
        ValueError: If the argument name is not allowed
    """
    if argument_name not in ALLOWED_VARIABLES:
        raise ValueError(f"Variable '{argument_name}' is not allowed. Allowed variables: {list(ALLOWED_VARIABLES.keys())}")
    
    return ALLOWED_VARIABLES[argument_name]

def get_variable_level_config(argument_name: str) -> Dict[str, any]:
    """
    Get the level configuration for a variable if it exists.
    
    Args:
        argument_name: The variable name passed as an argument
        
    Returns:
        Dictionary with level configuration or empty dict if no level config
    """
    return VARIABLE_LEVELS.get(argument_name, {})

def validate_variables(variables: List[str]) -> None:
    """
    Validate that all provided variables are in the allowed list.
    
    Args:
        variables: List of variable names to validate
        
    Raises:
        ValueError: If any variable is not allowed
    """
    allowed_vars = get_allowed_variables()
    invalid_vars = [var for var in variables if var not in allowed_vars]
    
    if invalid_vars:
        raise ValueError(
            f"Invalid variables: {invalid_vars}. "
            f"Allowed variables: {list(allowed_vars)}"
        )

def map_variables_to_grib_names(variables: List[str]) -> List[str]:
    """
    Map argument variable names to actual grib variable names.
    
    Args:
        variables: List of variable argument names
        
    Returns:
        List of actual grib variable names
    """
    validate_variables(variables)
    return [get_grib_variable_name(var) for var in variables]

def get_variable_levels_for_filtering(variables: List[str]) -> Tuple[List[str], List[int]]:
    """
    Get level types and levels for filtering based on variable configurations.
    
    Args:
        variables: List of variable argument names
        
    Returns:
        Tuple of (level_types, levels) for filtering
    """
    level_types = set()
    levels = set()
    
    for var in variables:
        level_config = get_variable_level_config(var)
        if level_config:
            if "level" in level_config:
                levels.add(level_config["level"])
            if "level_type" in level_config:
                level_types.add(level_config["level_type"])
    
    return list(level_types), list(levels)

def get_last_available_date(base_path: str = "s3://noaa-hrrr-bdp-pds/hrrr") -> str:
    """
    Find the last available date with complete HRRR data.
    
    Args:
        base_path: Base S3 path for HRRR data
        
    Returns:
        Date string in YYYY-MM-DD format
        
    Raises:
        RuntimeError: If unable to determine last available date
    """
    # Start from yesterday and work backwards to find complete data
    current_date = datetime.now() - timedelta(days=1)
    
    # Check up to 7 days back
    for days_back in range(7):
        check_date = current_date - timedelta(days=days_back)
        date_str = check_date.strftime("%Y-%m-%d")
        
        # Check if data exists for this date by trying to access a sample file
        # We'll check for the f00 file which should always be available if the run is complete
        test_url = build_s3_url(date_str, 0, base_path)
        
        # Convert S3 URL to HTTPS URL for checking
        if test_url.startswith("s3://"):
            https_url = test_url.replace("s3://", "https://")
            https_url = https_url.replace("noaa-hrrr-bdp-pds", "noaa-hrrr-bdp-pds.s3.amazonaws.com")
        else:
            https_url = test_url
            
        try:
            # Make a HEAD request to check if file exists
            response = requests.head(https_url, timeout=10)
            if response.status_code == 200:
                logger.info(f"Found complete data for date: {date_str}")
                return date_str
        except requests.RequestException:
            # If we can't check online, continue to next date
            pass
            
        logger.debug(f"Data not available for {date_str}, checking previous date")
    
    # If we can't find data online, default to 2 days ago
    fallback_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    logger.warning(f"Could not verify data availability, using fallback date: {fallback_date}")
    return fallback_date

def find_nearest_grid_point(
    target_lat: float, 
    target_lon: float, 
    grid_lats: np.ndarray, 
    grid_lons: np.ndarray
) -> Tuple[int, int]:
    """
    Find the nearest grid point to the target lat/lon coordinates.
    
    Args:
        target_lat: Target latitude
        target_lon: Target longitude  
        grid_lats: 2D array of grid latitudes
        grid_lons: 2D array of grid longitudes
        
    Returns:
        Tuple of (row_index, col_index) for the nearest grid point
    """
    # Calculate distances to all grid points
    distances = np.sqrt(
        (grid_lats - target_lat) ** 2 + (grid_lons - target_lon) ** 2
    )
    
    # Find the minimum distance index
    min_idx = np.unravel_index(np.argmin(distances), distances.shape)
    
    return min_idx

def parse_run_date(run_date: str) -> datetime:
    """
    Parse run date string into datetime object.
    
    Args:
        run_date: Date string in YYYY-MM-DD format
        
    Returns:
        datetime object for the run date
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {run_date}. Expected YYYY-MM-DD") from e

def calculate_valid_time(run_time: datetime, forecast_hour: int) -> datetime:
    """
    Calculate valid time for a given run time and forecast hour.
    
    Args:
        run_time: Run time datetime
        forecast_hour: Forecast hour (0-48)
        
    Returns:
        Valid time datetime
    """
    return run_time + timedelta(hours=forecast_hour)

def build_s3_url(
    run_date: str, 
    forecast_hour: int, 
    base_path: str = "s3://noaa-hrrr-bdp-pds/hrrr"
) -> str:
    """
    Build S3 URL for HRRR GRIB2 file.
    
    Args:
        run_date: Run date in YYYY-MM-DD format
        forecast_hour: Forecast hour (0-48)
        base_path: Base S3 path for HRRR data
        
    Returns:
        Complete S3 URL for the GRIB2 file
    """
    run_time = parse_run_date(run_date)
    date_str = run_time.strftime("%Y%m%d")
    hour_str = run_time.strftime("%H")
    forecast_str = f"f{forecast_hour:02d}"
    
    return f"{base_path}.{date_str}/conus/hrrr.t{hour_str}z.wrfsfc{forecast_str}.grib2"

def validate_lat_lon(lat: float, lon: float) -> bool:
    """
    Validate latitude and longitude coordinates.
    
    Args:
        lat: Latitude value
        lon: Longitude value
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    return -90 <= lat <= 90 and -180 <= lon <= 180

def read_points_file(file_path: str) -> List[Tuple[float, float]]:
    """
    Read lat/lon points from a text file.
    
    Args:
        file_path: Path to the points file
        
    Returns:
        List of (lat, lon) tuples
        
    Raises:
        FileNotFoundError: If points file doesn't exist
        ValueError: If file format is invalid
    """
    points = []
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                    
                try:
                    parts = line.split(',')
                    if len(parts) != 2:
                        raise ValueError(f"Line {line_num}: Expected 'lat,lon' format")
                    
                    lat = float(parts[0].strip())
                    lon = float(parts[1].strip())
                    
                    if not validate_lat_lon(lat, lon):
                        raise ValueError(f"Line {line_num}: Invalid coordinates {lat}, {lon}")
                    
                    points.append((lat, lon))
                    
                except ValueError as e:
                    raise ValueError(f"Line {line_num}: {str(e)}")
                    
    except FileNotFoundError:
        raise FileNotFoundError(f"Points file not found: {file_path}")
    
    if not points:
        raise ValueError("No valid points found in file")
    
    logger.info(f"Loaded {len(points)} points from {file_path}")
    return points

