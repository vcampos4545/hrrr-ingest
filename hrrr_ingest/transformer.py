"""
Transformer module for converting GRIB data to long format for database storage.
"""

import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

def transform_to_long_format(
    parsed_data: List[Dict[str, Any]], 
    source_s3: str
) -> pd.DataFrame:
    """
    Transform parsed GRIB data into long format DataFrame.
    
    Args:
        parsed_data: List of variable data dictionaries from parser
        source_s3: Source S3 URL for the GRIB file
        
    Returns:
        DataFrame in long format with columns:
        valid_time_utc, run_time_utc, latitude, longitude, variable, value, source_s3
    """
    if not parsed_data:
        logger.warning("No parsed data provided for transformation")
        return pd.DataFrame()
    
    # Collect all data points
    rows = []
    
    for var_data in parsed_data:
        variable_name = var_data['variable_name']
        valid_time = var_data['valid_time']
        run_time = var_data['run_time']
        
        for point_data in var_data['point_data']:
            # Use grid coordinates (actual lat/lon from data)
            lat = point_data['grid_lat']
            lon = point_data['grid_lon']
            value = point_data['value']
            
            # Create row for this variable/point combination
            row = {
                'valid_time_utc': valid_time,
                'run_time_utc': run_time,
                'latitude': lat,
                'longitude': lon,
                'variable': variable_name,
                'value': value,
                'source_s3': source_s3
            }
            rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    
    # Ensure proper data types
    df['valid_time_utc'] = pd.to_datetime(df['valid_time_utc'])
    df['run_time_utc'] = pd.to_datetime(df['run_time_utc'])
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['variable'] = df['variable'].astype(str)
    df['value'] = df['value'].astype(float)
    df['source_s3'] = df['source_s3'].astype(str)
    
    logger.info(f"Transformed {len(df)} rows to long format")
    return df

def combine_forecast_data(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple forecast DataFrames into a single DataFrame.
    
    Args:
        dataframes: List of DataFrames to combine
        
    Returns:
        Combined DataFrame
    """
    if not dataframes:
        return pd.DataFrame()
    
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in dataframes if not df.empty]
    
    if not non_empty_dfs:
        return pd.DataFrame()
    
    # Combine all DataFrames
    combined_df = pd.concat(non_empty_dfs, ignore_index=True)
    
    # Sort by time and location for consistency
    combined_df = combined_df.sort_values([
        'valid_time_utc', 'latitude', 'longitude', 'variable'
    ]).reset_index(drop=True)
    
    logger.info(f"Combined {len(combined_df)} total rows from {len(non_empty_dfs)} forecasts")
    return combined_df

def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate that DataFrame has the correct schema and data types.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_columns = [
        'valid_time_utc', 'run_time_utc', 'latitude', 'longitude', 
        'variable', 'value', 'source_s3'
    ]
    
    # Check required columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check data types
    expected_types = {
        'valid_time_utc': 'datetime64[ns]',
        'run_time_utc': 'datetime64[ns]',
        'latitude': 'float64',
        'longitude': 'float64',
        'variable': 'object',
        'value': 'float64',
        'source_s3': 'object'
    }
    
    for col, expected_type in expected_types.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if actual_type != expected_type:
                logger.warning(f"Column {col} has type {actual_type}, expected {expected_type}")
    
    # Check for missing values
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")
    
    # Check coordinate ranges
    if 'latitude' in df.columns:
        invalid_lats = df[(df['latitude'] < -90) | (df['latitude'] > 90)]
        if not invalid_lats.empty:
            logger.error(f"Found {len(invalid_lats)} invalid latitude values")
            return False
    
    if 'longitude' in df.columns:
        invalid_lons = df[(df['longitude'] < -180) | (df['longitude'] > 180)]
        if not invalid_lons.empty:
            logger.error(f"Found {len(invalid_lons)} invalid longitude values")
            return False
    
    return True

