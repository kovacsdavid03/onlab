from dagster import Definitions, load_assets_from_modules

from . import asset_extract

all_assets = load_assets_from_modules([asset_extract])

defs = Definitions(
    assets=all_assets,
)
