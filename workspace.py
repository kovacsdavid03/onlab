"""
Main Dagster definitions combining both ETL and Incremental Load packages

Run from the onlab root directory with: dagster dev -f workspace.py
"""

from dagster import Definitions, load_assets_from_modules

# Import both packages
from movies_load_pkg01 import asset_extract
from movies_incremental_load_pkg import assets as incremental_assets

# Load all assets
etl_assets = load_assets_from_modules([asset_extract])
incremental_assets_loaded = load_assets_from_modules([incremental_assets])

# Combine all assets
all_assets = [*etl_assets, *incremental_assets_loaded]

# Create the main definitions
defs = Definitions(
    assets=all_assets,
    # Add any shared resources, schedules, sensors here
)