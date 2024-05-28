import logging
from os import PathLike
from pathlib import Path

from caf.core.zoning import ZoningSystem, ZoningSystemMetaData
# import geopandas as gpd
import pandas as pd

LOGGER = logging.getLogger(__name__)


def generate_zoning_system(
        name: str, 
        shapefile_path: PathLike, 
        id_col: str, desc_col: str
        ) -> ZoningSystem:
    """Creates a ZoningSystem object, reading from cache if already generated

    If a new ZoningSystem is created, it is also saved to the cache.

    Parameters
    ----------
    name : str
        the name of the zoning system (used to check the cache)
    shapefile_path : PathLike
        path to the shapefile of the zoning system
    id_col : str
        column containing the zone IDs
    desc_col : str
        zolumn containing the zone descriptions

    Returns
    -------
    ZoningSystem
        representation of the zoning system ready to be used by other caf
        functionality
    """
    # TODO: use of CACHE_FOLDER in this feels a bit clunky
    try:
        LOGGER.info(
            f'Reading ZoningSystem for {name} from cache'
        )
        zs = ZoningSystem.get_zoning(name, search_dir=CACHE_FOLDER)
        # TODO: could we put some checks on this to ensure no parameters have changed?
        return zs
    
    except FileNotFoundError:
        LOGGER.info(
            f'Could not find "{name}" in zone system cache folder '
            f'({CACHE_FOLDER.absolute()}), attempting to generate'
        )

    # TODO: should use this line, but something in caf.core seems to break geopandas. Using pandas *temporarily*, once this is fixed we should use the following and csv_path will no longer be needed
    # layer_info = gpd.read_file(str(shapefile_path))
    csv_path = Path(shapefile_path).with_suffix('.csv')
    layer_info = pd.read_csv(csv_path)

    # Rename columns and filter down
    reformatted = layer_info.rename(
        columns={
            id_col: 'zone_id',
            desc_col: 'descriptions'
        }
    )
    reformatted = reformatted[['zone_id', 'descriptions']]

    meta = ZoningSystemMetaData(
        name=name, shapefile_id_col=id_col, shapefile_path=Path(shapefile_path),
    )

    # Create and save the object
    zs = ZoningSystem(name=name, unique_zones=reformatted, metadata=meta)
    zs.save(CACHE_FOLDER)

    return zs


# Define location of zoning cache folder
CACHE_FOLDER = Path(r'.\CACHE')
CACHE_FOLDER.mkdir(exist_ok=True)
SHAPEFILE_DIRECTORY = Path(r'.\TEMP_SHAPEFILES')

# Define the name of the model zoning to be consistent with the cache file.
# The model zoning is the zone system the outputs of the population / employment
# models are intended to be used in at the end.
LSOA_NAME = 'LSOA2021'
LSOA_ZONING_SYSTEM = generate_zoning_system(
    name=LSOA_NAME, 
    shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2021)' / 'LSOA_2021_EW_BFC_V8.shp',
    id_col='LSOA21CD', desc_col='LSOA21NM'
)

# Define other zone systems which are defined in the cache which may be used
# as part of the modelling.
MSOA_NAME = 'MSOA2021'
MSOA_ZONING_SYSTEM = generate_zoning_system(
    name=MSOA_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'MSOA (2021)' / 'MSOA_2021_EW_BFC_V6.shp',
    id_col='MSOA21CD', desc_col='MSOA21NM'
)

# Define other zone systems which are defined in the cache which may be used
# as part of the modelling.
LAD_NAME = 'LAD2021'
LAD_ZONING_SYSTEM = generate_zoning_system(
    name=LAD_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'LAD (2021)' / 'LAD_2021_EW_BFC.shp',
    id_col='LAD21CD', desc_col='LAD21NM'
)
RGN_NAME = 'RGN2021'
RGN_ZONING_SYSTEM = generate_zoning_system(
    name=RGN_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'REGION (2021)' / 'RGN_2021_EW_BFC.shp',
    id_col='RGN21CD', desc_col='RGN21NM'
)

# TODO: think about a different way to implement generate_zoning_system possibly on the fly as needed?
try:
    LSOA_2011_NAME = 'LSOA2011'
    LSOA_2011_ZONING_SYSTEM = generate_zoning_system(
        name=LSOA_2011_NAME, 
        shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2011)' / 'infuse_lsoa_lyr_2011.shp',
        id_col='geo_code', desc_col='name'
    )
except FileNotFoundError:
    pass

# Dictionary of references for the yaml file
try:
    # TODO link with definitions above
    KNOWN_GEOGRAPHIES = {
        LSOA_NAME: LSOA_ZONING_SYSTEM,
        MSOA_NAME: MSOA_ZONING_SYSTEM,
        LAD_NAME: LAD_ZONING_SYSTEM,
        LSOA_2011_NAME: LSOA_2011_ZONING_SYSTEM,
    }
except NameError:
    KNOWN_GEOGRAPHIES = {
        LSOA_NAME: LSOA_ZONING_SYSTEM,
        MSOA_NAME: MSOA_ZONING_SYSTEM,
        LAD_NAME: LAD_ZONING_SYSTEM,
        RGN_NAME: RGN_ZONING_SYSTEM
    }
# TODO This generated zone translations on I drive no matter the cache_path specified. It works, just needs changing in caf.core.
# if __name__ == '__main__':
#     KNOWN_GEOGRAPHIES = {
#             LSOA_NAME: LSOA_ZONING_SYSTEM,
#             MSOA_NAME: MSOA_ZONING_SYSTEM,
#             LAD_NAME: LAD_ZONING_SYSTEM,
#             RGN_NAME: RGN_ZONING_SYSTEM
#         }
#     from itertools import combinations
#
#     for zone_name_1, zone_name_2 in combinations(KNOWN_GEOGRAPHIES.keys(), 2):
#         zone_system_1 = KNOWN_GEOGRAPHIES.get(zone_name_1)
#         zone_system_2 = KNOWN_GEOGRAPHIES.get(zone_name_2)
#         print(f'Generating zone translation for {zone_name_1} and {zone_name_2}')
#         zone_system_1.translate(zone_system_2, cache_path=CACHE_FOLDER, weighting='spatial')


