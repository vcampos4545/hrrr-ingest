# HRRR Ingest

A command-line tool for downloading and processing HRRR (High-Resolution Rapid Refresh) forecast data from NOAA's S3 bucket and storing it in a DuckDB database.

## Features

- **Download HRRR GRIB2 files** from NOAA's S3 bucket with automatic caching
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
# Download temperature data for 2 forecast hours
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
  --level-types surface,heightAboveGround \
  --levels 2,80 \
  --db-path forecast.db \
  --cache-dir ./hrrr_cache \
  --upsert \
  --verbose
```

### Points File Format

Create a text file with one lat,lon coordinate pair per line:

```
# points.txt
40.7128,-74.0060  # New York City
34.0522,-118.2437 # Los Angeles
41.8781,-87.6298  # Chicago
```

### Command Line Options

| Option          | Description                                             | Default                     |
| --------------- | ------------------------------------------------------- | --------------------------- |
| `points_file`   | Path to file containing lat,lon coordinates             | Required                    |
| `--run-date`    | Run date in YYYY-MM-DD format                           | Required                    |
| `--variables`   | Comma-separated list of variables to extract            | Required                    |
| `--num-hours`   | Number of forecast hours to process                     | 1                           |
| `--db-path`     | Path to DuckDB database file                            | data.db                     |
| `--cache-dir`   | Directory to cache downloaded files                     | ./cache                     |
| `--base-path`   | Base S3 path for HRRR data                              | s3://noaa-hrrr-bdp-pds/hrrr |
| `--level-types` | Comma-separated list of level types to filter by        | None                        |
| `--levels`      | Comma-separated list of levels to filter by             | None                        |
| `--upsert`      | Use upsert instead of insert for database operations    | False                       |
| `--verbose`     | Enable verbose logging                                  | False                       |
| `--dry-run`     | Download and parse data but do not insert into database | False                       |

## Database Schema

The tool creates a `hrrr_forecasts` table with the following schema:

| Column           | Type      | Description                |
| ---------------- | --------- | -------------------------- |
| `valid_time_utc` | TIMESTAMP | Valid time of the forecast |
| `run_time_utc`   | TIMESTAMP | Model run time             |
| `latitude`       | FLOAT     | Latitude coordinate        |
| `longitude`      | FLOAT     | Longitude coordinate       |
| `variable`       | VARCHAR   | Variable name              |
| `value`          | FLOAT     | Variable value             |
| `source_s3`      | VARCHAR   | Source S3 URL              |

## Available Variables

Common HRRR variables include:

- `temperature_2m` - 2-meter temperature
- `surface_pressure` - Surface pressure
- `wind_speed_10m` - 10-meter wind speed
- `wind_direction_10m` - 10-meter wind direction
- `relative_humidity_2m` - 2-meter relative humidity
- `wind_speed_80m` - 80-meter wind speed (for wind energy)

Use the `--verbose` flag to see all available variables in a GRIB file.

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
from hrrr_ingest.db import insert_forecast_data

# Download a GRIB file
grib_path = download_grib('2025-01-24', 0)

# Parse variables at specific points
points = [(40.7128, -74.0060), (34.0522, -118.2437)]
parsed_data = parse_grib_file(grib_path, ['temperature_2m'], points)

# Transform to long format
df = transform_to_long_format(parsed_data, 's3://source/file.grib2')

# Insert into database
insert_forecast_data(df, 'forecast.db')
```

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Code Formatting

```bash
black hrrr_ingest/
flake8 hrrr_ingest/
```

### Type Checking

```bash
mypy hrrr_ingest/
```

## Project Structure

```
hrrr_ingest/
├── hrrr_ingest/
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # Command-line interface
│   ├── downloader.py        # GRIB file downloader
│   ├── parser.py            # GRIB file parser
│   ├── transformer.py       # Data transformation
│   ├── db.py                # Database operations
│   └── utils.py             # Shared utilities
├── tests/                   # Test files
├── requirements.txt         # Dependencies
├── setup.py                 # Package setup
└── README.md               # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- NOAA for providing HRRR forecast data
- The pygrib developers for GRIB file parsing capabilities
- DuckDB team for the embedded analytical database

## Support

For issues and questions:

1. Check the [documentation](https://github.com/your-org/hrrr-ingest/blob/main/README.md)
2. Search [existing issues](https://github.com/your-org/hrrr-ingest/issues)
3. Create a [new issue](https://github.com/your-org/hrrr-ingest/issues/new)

