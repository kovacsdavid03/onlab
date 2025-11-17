"""
Movies Incremental Load Package

This package contains incremental loading and data processing assets that build on top of the
movies data warehouse created by the movies_load_pkg01 ETL pipeline.
"""

from dagster import Definitions, load_assets_from_modules
from . import assets

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    # Add any shared resources, schedules, sensors here
)