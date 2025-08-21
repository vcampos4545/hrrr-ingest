# HRRR Ingest

A command-line tool for downloading and processing HRRR (High-Resolution Rapid Refresh) forecast data from NOAA's S3 bucket and storing it in a DuckDB database.

## Features

- **Download HRRR GRIB2 files** from NOAA's S3 bucket with automatic caching (06z run)
- **Extract specific variables** at requested lat/lon coordinates using nearest neighbor interpolation
- **Store data in DuckDB** in a normalized long format for efficient querying
- **Idempotent operations** - safe to re-run without duplicating data
- **Modular design** - each component can be used independently
- **Comprehensive logging** with progress bars for large downloads

## Installation

### Prerequisites

- Python 3.8 or higher
- Access to NOAA's HRRR S3 bucket (public access)

### Install from source

```bash
git clone https://github.com/your-org/hrrr-ingest.git
cd hrrr-ingest
pip install -e .
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
# Basic usage with automatic date detection and all variables (48 hours by default)
hrrr-ingest points.txt

# Specify variables for testing with fewer hours
hrrr-ingest points.txt \
  --variables temperature_2m,surface_pressure \
  --num-hours 6

# Use specific date
hrrr-ingest points.txt \
  --run-date 2025-01-24 \
  --variables temperature_2m \
  --num-hours 2
```

### Advanced Usage

```bash
# Download multiple variables with specific level filters
hrrr-ingest locations.csv \
  --run-date 2025-01-24 \
  --variables temperature_2m,surface_pressure,wind_speed_80m \
  --num-hours 6 \

  --db-path forecast.db \
  --cache-dir ./hrrr_cache \

  --verbose
```

### Points File Format

Create a text file with one lat,lon coordinate pair per line:

```
# points.txt
# New York City
40.7128,-74.0060
# Los Angeles
34.0522,-118.2437
# Chicago
41.8781,-87.6298
```

### Command Line Options

| Option        | Description                                   | Default                                      |
| ------------- | --------------------------------------------- | -------------------------------------------- |
| `points_file` | Path to file containing lat,lon coordinates   | Required                                     |
| `--run-date`  | The forecast run date of the data to ingest   | Last available date                          |
| `--variables` | A comma separated list of variables to ingest | All supported variables                      |
| `--num-hours` | Number of hours of forecast data to ingest    | 48                                           |
| `--db-path`   | Path to DuckDB database file                  | data.db                                      |
| `--cache-dir` | Directory to cache downloaded files           | ./cache                                      |
| `--base-path` | Base S3 path for HRRR data                    | s3://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr |

| `--verbose` | Enable verbose logging | False |
| `--dry-run` | Download and parse data but do not insert into database | False |

## Database Schema

The tool creates a `hrrr_forecasts` table with the following schema:

| Column           | Type      | Description                                                                              |
| ---------------- | --------- | ---------------------------------------------------------------------------------------- |
| `valid_time_utc` | TIMESTAMP | UTC timestamp of when the forecast is valid (e.g., 2025-01-26 18:00:00 for f12 forecast) |
| `run_time_utc`   | TIMESTAMP | UTC timestamp of when the forecast was made (e.g., 2025-01-26 06:00:00 for 06z run)      |
| `latitude`       | FLOAT     | Actual grid latitude coordinate from the data                                            |
| `longitude`      | FLOAT     | Actual grid longitude coordinate from the data                                           |
| `variable`       | VARCHAR   | Variable name                                                                            |
| `value`          | FLOAT     | Variable value                                                                           |
| `source_s3`      | VARCHAR   | Source S3 URL                                                                            |

## Available Variables

The tool supports the following variables that can be passed as arguments:

| Argument Name                         | GRIB Variable Name                  | Description                              |
| ------------------------------------- | ----------------------------------- | ---------------------------------------- |
| `surface_pressure`                    | Surface pressure                    | Surface pressure                         |
| `surface_roughness`                   | Forecast surface roughness          | Surface roughness                        |
| `visible_beam_downward_solar_flux`    | Visible Beam Downward Solar Flux    | Visible beam downward solar flux         |
| `visible_diffuse_downward_solar_flux` | Visible Diffuse Downward Solar Flux | Visible diffuse downward solar flux      |
| `temperature_2m`                      | 2 metre temperature                 | Temperature at 2m above ground           |
| `dewpoint_2m`                         | 2 metre dewpoint temperature        | Dew point temperature at 2m above ground |
| `relative_humidity_2m`                | 2 metre relative humidity           | Relative humidity at 2m above ground     |
| `u_component_wind_10m`                | 10 metre U wind component           | U-Component of wind at 10m above ground  |
| `v_component_wind_10m`                | 10 metre V wind component           | V-Component of wind at 10m above ground  |
| `u_component_wind_80m`                | U component of wind                 | U-Component of wind at 80m above ground  |
| `v_component_wind_80m`                | V component of wind                 | V-Component of wind at 80m above ground  |

**Note:** Variables with specific levels (like 80m wind components) are automatically filtered to the correct level.

Use the `--verbose` flag to see all available variables in a GRIB file.

### Cache File Naming

Downloaded GRIB2 files are cached locally with names that closely match the HTTPS URLs they were downloaded from:

```
cache/noaa-hrrr-bdp-pds.s3.amazonaws.com_hrrr.20250124_conus_hrrr.t06z.wrfsfcf00.grib2
cache/noaa-hrrr-bdp-pds.s3.amazonaws.com_hrrr.20250124_conus_hrrr.t06z.wrfsfcf12.grib2
```

This naming convention preserves the URL structure and makes it easy to identify the source of each cached file.

## Examples

### Query the Database

```python
import duckdb
import pandas as pd

# Connect to the database
conn = duckdb.connect('data.db')

# Query temperature data for a specific location
df = conn.execute("""
    SELECT * FROM hrrr_forecasts
    WHERE variable = 'temperature_2m'
    AND latitude = 40.7128
    AND longitude = -74.0060
    ORDER BY valid_time_utc
""").df()

print(df)
```

### Programmatic Usage

```python
from hrrr_ingest import download_grib, parse_grib_file, transform_to_long_format
from hrrr_ingest.db import insert_forecast_data, check_existing_data, get_duplicate_count

# Download a GRIB file
grib_path = download_grib('2025-01-24', 0)

# Parse variables at specific points
points = [(40.7128, -74.0060), (34.0522, -118.2437)]
parsed_data = parse_grib_file(grib_path, ['temperature_2m'], points)

# Transform to long format
df = transform_to_long_format(parsed_data, 's3://source/file.grib2')

# Check for existing data before insertion
duplicate_count = get_duplicate_count(df, 'forecast.db')
if duplicate_count > 0:
    print(f"Found {duplicate_count} existing rows that would be duplicates")

# Insert into database (duplicates are automatically ignored)
rows_inserted = insert_forecast_data(df, 'forecast.db')
print(f"Inserted {rows_inserted} new rows")
```

## Development

### Running Tests

```bash
pytest tests/ -v
```
