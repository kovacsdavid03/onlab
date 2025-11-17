"""
Standalone Incremental Load Package Definitions

Run this with: dagster dev -f incremental_definitions.py
"""

from dagster import Definitions, load_assets_from_modules
from movies_incremental_load_pkg import assets

# Load incremental load assets
incremental_assets = load_assets_from_modules([assets])

# Create definitions for incremental load only
defs = Definitions(
    assets=incremental_assets,
)