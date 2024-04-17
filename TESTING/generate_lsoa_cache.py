from pathlib import Path

from caf.core.zoning import ZoningSystem, ZoningSystemMetaData
import pandas as pd

""" Testing using non-integer zone names in the zone system """

# Set up GIS folder and example cache directory
GIS_FOLDER = Path(r'INPUTS/LSOA (2021)')
CACHE_FOLDER = Path(r'CACHE')
CACHE_FOLDER.mkdir(exist_ok=True)

# Shapefile manually dumped to CSV - could be done with geopandas in "proper" version?
layer_info = pd.read_csv(GIS_FOLDER / 'LSOA_2021_EW_BFC_V8.csv')

# `reset_index` used here to get unique numeric IDs for LSOAs, could be smarter
# Other columns are hopefully as the process expects
reformatted = layer_info.reset_index(drop=True).rename(
    columns={
        'LSOA21CD': 'zone_id',
        'LSOA21NM': 'descriptions'
    }
).drop(columns=['BNG_E', 'BNG_N', 'LONG', 'LAT', 'GlobalID'])

# Not 100% sure on this. I *think* `shapefile_id_col` might need to be
# the "unique ID" column, which isn't in our shapefile. Hmm!
meta = ZoningSystemMetaData(
    name='lsoa2021', shapefile_id_col='LSOA21CD',
    shapefile_path=GIS_FOLDER / 'LSOA_2021_EW_BFC_V8.shp',
)

# Create and save the object
zs = ZoningSystem(name='lsoa2021', unique_zones=reformatted, metadata=meta)
zs.save(CACHE_FOLDER)

# Check it loads in okay, and that we get the same zone system
zs2 = ZoningSystem.get_zoning(name='lsoa2021', search_dir=CACHE_FOLDER)

assert zs2 == zs
