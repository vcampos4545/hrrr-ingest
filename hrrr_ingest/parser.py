"""
Parser module for extracting data from GRIB2 files using pygrib.
"""

import pygrib
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional, Any
import logging
from datetime import datetime

from .utils import find_nearest_grid_point, calculate_valid_time, parse_run_date

logger = logging.getLogger(__name__)

class GribParser:
    """Parser for GRIB2 files with variable filtering and point extraction."""
    
    def __init__(self, grib_file_path: str):
        """
        Initialize parser with GRIB2 file.
        
        Args:
            grib_file_path: Path to the GRIB2 file
        """
        self.grib_file_path = grib_file_path
        self.grib = None
        self._open_grib()
    
    def _open_grib(self):
        """Open the GRIB2 file."""
        try:
            self.grib = pygrib.open(self.grib_file_path)
            logger.info(f"Opened GRIB2 file: {self.grib_file_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to open GRIB2 file {self.grib_file_path}: {str(e)}") from e
    
    def get_available_variables(self) -> List[Dict[str, Any]]:
        """
        Get list of available variables in the GRIB2 file.
        
        Returns:
            List of variable dictionaries with keys: name, levelType, level
        """
        variables = []
        self.grib.seek(0)
        
        for message in self.grib:
            var_info = {
                'name': message.name,
                'levelType': message.typeOfLevel,
                'level': message.level,
                'shortName': message.shortName
            }
            variables.append(var_info)
        
        return variables
    
    def find_variable_messages(
        self, 
        variable_names: List[str], 
        level_types: Optional[List[str]] = None,
        levels: Optional[List[int]] = None
    ) -> List[int]:
        """
        Find message indices for requested variables.
        
        Args:
            variable_names: List of variable names to find
            level_types: Optional list of level types to filter by
            levels: Optional list of levels to filter by
            
        Returns:
            List of message indices
        """
        message_indices = []
        self.grib.seek(0)
        
        for i, message in enumerate(self.grib):
            name_match = message.name in variable_names or message.shortName in variable_names
            
            level_type_match = True
            if level_types:
                level_type_match = message.typeOfLevel in level_types
            
            level_match = True
            if levels:
                level_match = message.level in levels
            
            if name_match and level_type_match and level_match:
                message_indices.append(i)
        
        return message_indices
    
    def extract_variable_data(
        self, 
        message_index: int, 
        target_points: List[Tuple[float, float]]
    ) -> Dict[str, Any]:
        """
        Extract variable data for specific lat/lon points.
        
        Args:
            message_index: Index of the GRIB message
            target_points: List of (lat, lon) tuples
            
        Returns:
            Dictionary containing variable data for each point
        """
        self.grib.seek(message_index)
        message = self.grib.read(1)[0]
        
        # Get grid information
        lats, lons = message.latlons()
        values = message.values
        
        # Get message metadata
        variable_name = message.name
        level_type = message.typeOfLevel
        level = message.level
        valid_time = message.validDate
        run_time = message.analDate
        
        # Extract data for each target point
        point_data = []
        for lat, lon in target_points:
            # Find nearest grid point
            row_idx, col_idx = find_nearest_grid_point(lat, lon, lats, lons)
            
            # Get value at nearest point
            value = values[row_idx, col_idx]
            grid_lat = lats[row_idx, col_idx]
            grid_lon = lons[row_idx, col_idx]
            
            point_data.append({
                'target_lat': lat,
                'target_lon': lon,
                'grid_lat': grid_lat,
                'grid_lon': grid_lon,
                'value': value,
                'variable': variable_name,
                'level_type': level_type,
                'level': level,
                'valid_time': valid_time,
                'run_time': run_time
            })
        
        return {
            'variable_name': variable_name,
            'level_type': level_type,
            'level': level,
            'valid_time': valid_time,
            'run_time': run_time,
            'point_data': point_data
        }
    
    def parse_variables_at_points(
        self, 
        variable_configs: Dict[str, Dict[str, Any]], 
        target_points: List[Tuple[float, float]]
    ) -> List[Dict[str, Any]]:
        """
        Parse multiple variables at multiple points.
        
        Args:
            variable_configs: Dictionary mapping GRIB variable names to their level configurations
            target_points: List of (lat, lon) tuples
            
        Returns:
            List of variable data dictionaries
        """
        results = []
        
        for grib_name, config in variable_configs.items():
            try:
                # Extract level config and argument name
                level_config = config['level_config']
                argument_name = config['argument_name']
                
                # Build selection arguments - only include level if it exists
                select_args = {"name": grib_name}
                select_args.update(level_config)  # Add level config if it exists
                
                # Select the variable
                grb = self.grib.select(**select_args)
                if not grb:
                    logger.warning(f"No variable found matching: {grib_name}")
                    continue
                
                message = grb[0]
                
                # Get grid information
                lats, lons = message.latlons()
                values = message.values
                
                # Get message metadata
                level_type = message.typeOfLevel
                level = message.level
                valid_time = message.validDate
                run_time = message.analDate
                

                
                # Extract data for each target point
                point_data = []
                for lat, lon in target_points:
                    # Find nearest grid point
                    row_idx, col_idx = find_nearest_grid_point(lat, lon, lats, lons)
                    
                    # Get value at nearest point
                    value = values[row_idx, col_idx]
                    grid_lat = lats[row_idx, col_idx]
                    grid_lon = lons[row_idx, col_idx]
                    
                    point_data.append({
                        'target_lat': lat,
                        'target_lon': lon,
                        'grid_lat': grid_lat,
                        'grid_lon': grid_lon,
                        'value': value,
                        'variable': argument_name,  # Store the argument name
                        'level_type': level_type,
                        'level': level,
                        'valid_time': valid_time,
                        'run_time': run_time
                    })
                
                var_data = {
                    'variable_name': argument_name,  # Store the argument name
                    'level_type': level_type,
                    'level': level,
                    'valid_time': valid_time,
                    'run_time': run_time,
                    'point_data': point_data
                }
                
                results.append(var_data)
                logger.info(f"Extracted {var_data['variable_name']} for {len(target_points)} points")
                
            except Exception as e:
                logger.error(f"Failed to extract variable {grib_name}: {str(e)}")
                continue
        
        return results
    
    def close(self):
        """Close the GRIB2 file."""
        if self.grib:
            self.grib.close()

def parse_grib_file(
    grib_file_path: str,
    variable_configs: Dict[str, Dict[str, Any]],
    target_points: List[Tuple[float, float]]
) -> List[Dict[str, Any]]:
    """
    Convenience function to parse a GRIB2 file.
    
    Args:
        grib_file_path: Path to the GRIB2 file
        variable_configs: Dictionary mapping variable names to their level configurations
        target_points: List of (lat, lon) tuples
        
    Returns:
        List of variable data dictionaries
    """
    parser = GribParser(grib_file_path)
    try:
        return parser.parse_variables_at_points(variable_configs, target_points)
    finally:
        parser.close()

