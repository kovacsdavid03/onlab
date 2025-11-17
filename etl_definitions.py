"""
ETL Pipeline Definitions

Run standalone with: dagster dev -f etl_definitions.py
"""

from dagster import Definitions, load_assets_from_modules
from movies_load_pkg01 import asset_extract

# Load ETL assets
etl_assets = load_assets_from_modules([asset_extract])

# Create definitions for ETL only
defs = Definitions(
    assets=etl_assets,
)