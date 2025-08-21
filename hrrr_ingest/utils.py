"""
Shared utilities for HRRR ingest operations.
"""

import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, List, Optional
import logging

logger = logging.getLogger(__name__)

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
    
    return f"{base_path}.{date_str}/conus/hrrr.t{hour_str}z.wrfsfcf{forecast_str}.grib2"

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

