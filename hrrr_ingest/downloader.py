"""
Downloader module for HRRR GRIB2 files from S3.
"""

import requests
from pathlib import Path
from typing import Optional
import logging
from tqdm import tqdm

from .utils import build_s3_url

logger = logging.getLogger(__name__)

def download_grib(
    run_date: str, 
    forecast_hour: int, 
    cache_dir: str = "./cache",
    base_path: str = "s3://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr"
) -> str:
    """
    Download GRIB2 file from S3 with caching for idempotency.
    
    Args:
        run_date: Run date in YYYY-MM-DD format
        forecast_hour: Forecast hour (0-48)
        cache_dir: Directory to cache downloaded files
        base_path: Base S3 path for HRRR data
        
    Returns:
        Path to the local GRIB2 file
        
    Raises:
        requests.RequestException: If download fails
        ValueError: If parameters are invalid
    """
    if forecast_hour < 0 or forecast_hour > 48:
        raise ValueError("Forecast hour must be between 0 and 48")
    
    # Build S3 URL
    s3_url = build_s3_url(run_date, forecast_hour, base_path)
    
    # Create cache directory
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    
    # Generate local filename based on HTTPS URL structure
    run_time = run_date.replace("-", "")
    local_filename = f"noaa-hrrr-bdp-pds.s3.amazonaws.com_hrrr.{run_time}_conus_hrrr.t06z.wrfsfcf{forecast_hour:02d}.grib2"
    local_path = cache_path / local_filename
    
    # Check if file already exists
    if local_path.exists():
        logger.info(f"File already exists, skipping download: {local_path}")
        return str(local_path)
    
    # Convert S3 URL to HTTP URL for requests
    if s3_url.startswith("s3://"):
        http_url = s3_url.replace("s3://", "https://")
    else:
        http_url = s3_url
    
    logger.info(f"Downloading {http_url} to {local_path}")
    
    try:
        # Stream download with progress bar
        response = requests.get(http_url, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(local_path, 'wb') as f:
            with tqdm(
                total=total_size, 
                unit='B', 
                unit_scale=True, 
                desc=f"Downloading f{forecast_hour:02d}"
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        logger.info(f"Successfully downloaded: {local_path}")
        return str(local_path)
        
    except requests.RequestException as e:
        # Clean up partial download
        if local_path.exists():
            local_path.unlink()
        raise requests.RequestException(f"Failed to download {http_url}: {str(e)}") from e

