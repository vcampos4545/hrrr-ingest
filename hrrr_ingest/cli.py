"""
CLI module for HRRR ingest tool.
"""

import argparse
import sys
import logging
from typing import List
from pathlib import Path

from .downloader import download_grib
from .parser import parse_grib_file
from .transformer import transform_to_long_format, combine_forecast_data, validate_dataframe
from .db import HrrrDatabase
from .utils import read_points_file, build_s3_url

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False) -> None:
    """
    Setup logging configuration.
    
    Args:
        verbose: Enable verbose logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="HRRR Ingest - Download and process HRRR forecast data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  hrrr-ingest points.txt --run-date 2025-01-24 --variables temperature_2m --num-hours 2
  hrrr-ingest locations.csv --run-date 2025-01-24 --variables temperature_2m,surface_pressure --num-hours 6 --db-path forecast.db
        """
    )
    
    parser.add_argument(
        'points_file',
        help='Path to file containing lat,lon coordinates (one per line)'
    )
    
    parser.add_argument(
        '--run-date',
        required=True,
        help='Run date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--variables',
        required=True,
        help='Comma-separated list of variables to extract'
    )
    
    parser.add_argument(
        '--num-hours',
        type=int,
        default=1,
        help='Number of forecast hours to process (default: 1)'
    )
    
    parser.add_argument(
        '--db-path',
        default='data.db',
        help='Path to DuckDB database file (default: data.db)'
    )
    
    parser.add_argument(
        '--cache-dir',
        default='./cache',
        help='Directory to cache downloaded files (default: ./cache)'
    )
    
    parser.add_argument(
        '--base-path',
        default='s3://noaa-hrrr-bdp-pds/hrrr',
        help='Base S3 path for HRRR data'
    )
    
    parser.add_argument(
        '--level-types',
        help='Comma-separated list of level types to filter by'
    )
    
    parser.add_argument(
        '--levels',
        help='Comma-separated list of levels to filter by'
    )
    
    parser.add_argument(
        '--upsert',
        action='store_true',
        help='Use upsert instead of insert for database operations'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Download and parse data but do not insert into database'
    )
    
    return parser.parse_args()

def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate command line arguments.
    
    Args:
        args: Parsed arguments
        
    Raises:
        ValueError: If arguments are invalid
    """
    # Check points file exists
    if not Path(args.points_file).exists():
        raise ValueError(f"Points file not found: {args.points_file}")
    
    # Validate run date format
    try:
        from .utils import parse_run_date
        parse_run_date(args.run_date)
    except ValueError as e:
        raise ValueError(f"Invalid run date: {e}")
    
    # Validate num_hours
    if args.num_hours < 1 or args.num_hours > 48:
        raise ValueError("num_hours must be between 1 and 48")
    
    # Validate variables
    if not args.variables.strip():
        raise ValueError("Variables cannot be empty")

def process_forecast_hour(
    run_date: str,
    forecast_hour: int,
    points: List[tuple],
    variables: List[str],
    cache_dir: str,
    base_path: str,
    level_types: List[str] = None,
    levels: List[int] = None
) -> tuple:
    """
    Process a single forecast hour.
    
    Args:
        run_date: Run date string
        forecast_hour: Forecast hour to process
        points: List of (lat, lon) tuples
        variables: List of variable names
        cache_dir: Cache directory path
        base_path: Base S3 path
        level_types: Optional level types filter
        levels: Optional levels filter
        
    Returns:
        Tuple of (grib_file_path, parsed_data, source_s3)
    """
    logger.info(f"Processing forecast hour f{forecast_hour:02d}")
    
    # Download GRIB file
    grib_file_path = download_grib(run_date, forecast_hour, cache_dir, base_path)
    
    # Build source S3 URL
    source_s3 = build_s3_url(run_date, forecast_hour, base_path)
    
    # Parse GRIB file
    parsed_data = parse_grib_file(
        grib_file_path, 
        variables, 
        points, 
        level_types, 
        levels
    )
    
    return grib_file_path, parsed_data, source_s3

def main():
    """Main CLI entry point."""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Setup logging
        setup_logging(args.verbose)
        
        # Validate arguments
        validate_arguments(args)
        
        logger.info("Starting HRRR ingest process")
        
        # Parse variables list
        variables = [v.strip() for v in args.variables.split(',')]
        logger.info(f"Processing variables: {variables}")
        
        # Parse optional filters
        level_types = None
        if args.level_types:
            level_types = [lt.strip() for lt in args.level_types.split(',')]
        
        levels = None
        if args.levels:
            levels = [int(l.strip()) for l in args.levels.split(',')]
        
        # Read points file
        points = read_points_file(args.points_file)
        logger.info(f"Processing {len(points)} points")
        
        # Initialize database
        db = HrrrDatabase(args.db_path)
        
        try:
            # Process each forecast hour
            all_dataframes = []
            total_rows = 0
            
            for hour in range(args.num_hours):
                try:
                    grib_file_path, parsed_data, source_s3 = process_forecast_hour(
                        args.run_date,
                        hour,
                        points,
                        variables,
                        args.cache_dir,
                        args.base_path,
                        level_types,
                        levels
                    )
                    
                    if parsed_data:
                        # Transform to long format
                        df = transform_to_long_format(parsed_data, source_s3)
                        
                        if not df.empty:
                            # Validate DataFrame
                            if validate_dataframe(df):
                                all_dataframes.append(df)
                                logger.info(f"Processed {len(df)} rows for hour f{hour:02d}")
                            else:
                                logger.error(f"Data validation failed for hour f{hour:02d}")
                        else:
                            logger.warning(f"No data extracted for hour f{hour:02d}")
                    else:
                        logger.warning(f"No variables found for hour f{hour:02d}")
                        
                except Exception as e:
                    logger.error(f"Failed to process hour f{hour:02d}: {str(e)}")
                    continue
            
            # Combine all DataFrames
            if all_dataframes:
                combined_df = combine_forecast_data(all_dataframes)
                
                if not args.dry_run:
                    # Insert into database
                    if args.upsert:
                        total_rows = db.upsert_forecast_data(combined_df)
                    else:
                        total_rows = db.insert_forecast_data(combined_df)
                    
                    logger.info(f"Successfully processed {total_rows} total rows")
                else:
                    logger.info(f"Dry run: Would insert {len(combined_df)} rows")
            else:
                logger.warning("No data to process")
        
        finally:
            db.close()
        
        logger.info("HRRR ingest process completed")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

