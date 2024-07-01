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
CACHE_FOLDER = Path(r'F:\Working\Land-Use\CACHE')
CACHE_FOLDER.mkdir(exist_ok=True)
SHAPEFILE_DIRECTORY = Path(r'F:\Working\Land-Use\SHAPEFILES')
GORS = ['EM', 'EoE', 'Lon', 'NE', 'NW', 'SE', 'SW', 'Wales', 'WM', 'YH']

# --- LSOA ZONE SYSTEMS (ENGLAND AND WALES ONLY) --- #
LSOA_NAME = 'LSOA2021'
LSOA_ZONING_SYSTEM = generate_zoning_system(
    name=LSOA_NAME, 
    shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2021)' / 'LSOA_2021_EnglandWales.shp',
    id_col='LSOA21CD', desc_col='LSOA21NM'
)
LSOA_2011_NAME = 'LSOA2011'
LSOA_2011_ZONING_SYSTEM = generate_zoning_system(
    name=LSOA_2011_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2011)' / 'LSOA_2011_EnglandWales.shp',
    id_col='LSOA11CD', desc_col='LSOA11NM'
)

# --- MSOA ZONE SYSTEMS (ENGLAND AND WALES ONLY) --- #
MSOA_NAME = 'MSOA2021'
MSOA_ZONING_SYSTEM = generate_zoning_system(
    name=MSOA_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'MSOA (2021)' / 'MSOA_2021_EnglandWales.shp',
    id_col='MSOA21CD', desc_col='MSOA21NM'
)
MSOA_2011_NAME = 'MSOA2011'
MSOA_2011_ZONING_SYSTEM = generate_zoning_system(
    name=MSOA_2011_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'MSOA (2011)' / 'MSOA_2011_EnglandWales.shp',
    id_col='MSOA11CD', desc_col='MSOA11NM'
)

# --- LAD ZONE SYSTEMS (ENGLAND AND WALES ONLY) --- #
LAD_NAME = 'LAD2021'
LAD_ZONING_SYSTEM = generate_zoning_system(
    name=LAD_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'LAD (2021)' / 'LAD_2021_EnglandWales.shp',
    id_col='LAD21CD', desc_col='LAD21NM'
)

# --- REGION ZONE SYSTEMS (ENGLAND AND WALES ONLY) --- #
RGN_NAME = 'RGN2021'
RGN_ZONING_SYSTEM = generate_zoning_system(
    name=RGN_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'GOR (2021)' / 'GOR_2021_EnglandWales.shp',
    id_col='RGN21CD', desc_col='RGN21NM'
)

# --- SCOTLAND SPECIFIC ZONE SYSTEMS --- #
SCOTLAND_NAME = 'DZ2011'
SCOTLAND_ZONING_SYSTEM = generate_zoning_system(
    name=SCOTLAND_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'DATA ZONES (2011)' / 'Scottish Data Zones 2011.shp',
    id_col='DZ2011CD', desc_col='tfn_at_nm'
)
NORTH_NAME = 'LSOA21-NORTH'
NORTH_ZONING_SYSTEM = generate_zoning_system(
    name=NORTH_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2021)' / f'LSOA_2021_EnglandWales_NORTH.shp',
    id_col='LSOA21CD', desc_col='LSOA21NM'
)
TFN_AT_AGG_NAME = 'TFN-AT-AGG'
TFN_AT_AGG_ZONING_SYSTEM = generate_zoning_system(
    name=TFN_AT_AGG_NAME,
    shapefile_path=SHAPEFILE_DIRECTORY / 'TFN AREA TYPES' / f'tfn_area_types_AGG.shp',
    id_col='TFN_AT_AGG', desc_col='AT_AGG_NM'
)

# --- Define zone systems by GOR to chunk the processing by GOR to save memory issues --- #
LSOAS_BY_GOR = dict()
MSOAS_BY_GOR = dict()
LADS_BY_GOR = dict()
RGNS_BY_GOR = dict()
for gor in GORS:
    # LSOA CORRESPONDENCES
    lsoa_zone_name = f'LSOA2021-{gor}'
    lsoa_zone_system = generate_zoning_system(
        name=lsoa_zone_name,
        shapefile_path=SHAPEFILE_DIRECTORY / 'LSOA (2021)' / f'LSOA_2021_EnglandWales_{gor}.shp',
        id_col='LSOA21CD', desc_col='LSOA21NM'
    )
    LSOAS_BY_GOR[lsoa_zone_name] = lsoa_zone_system

    # MSOA CORRESPONDENCES
    msoa_zone_name = f'MSOA2021-{gor}'
    msoa_zone_system = generate_zoning_system(
        name=msoa_zone_name,
        shapefile_path=SHAPEFILE_DIRECTORY / 'MSOA (2021)' / f'MSOA_2021_EnglandWales_{gor}.shp',
        id_col='MSOA21CD', desc_col='MSOA21NM'
    )
    MSOAS_BY_GOR[msoa_zone_name] = msoa_zone_system

    # LAD CORRESPONDENCES
    lad_zone_name = f'LAD2021-{gor}'
    lad_zone_system = generate_zoning_system(
        name=lad_zone_name,
        shapefile_path=SHAPEFILE_DIRECTORY / 'LAD (2021)' / f'LAD_2021_EnglandWales_{gor}.shp',
        id_col='LAD21CD', desc_col='LAD21NM'
    )
    LADS_BY_GOR[lad_zone_name] = lad_zone_system

    # REGION CORRESPONDENCES
    rgn_zone_name = f'RGN2021-{gor}'
    rgn_zone_system = generate_zoning_system(
        name=rgn_zone_name,
        shapefile_path=SHAPEFILE_DIRECTORY / 'GOR (2021)' / f'GOR_2021_EnglandWales_{gor}.shp',
        id_col='RGN21CD', desc_col='RGN21NM'
    )
    RGNS_BY_GOR[rgn_zone_name] = rgn_zone_system

# TODO: think about a different way to implement generate_zoning_system possibly on the fly as needed?

# --- GENERATE KNOWN_GEOGRAPHIES TO IMPORT INTO LAND USE PROCESSING --- #
# Dictionary of references for the yaml file
# TODO link with definitions above
KNOWN_GEOGRAPHIES = {
    LSOA_NAME: LSOA_ZONING_SYSTEM,
    LSOA_2011_NAME: LSOA_2011_ZONING_SYSTEM,
    MSOA_NAME: MSOA_ZONING_SYSTEM,
    MSOA_2011_NAME: MSOA_2011_ZONING_SYSTEM,
    LAD_NAME: LAD_ZONING_SYSTEM,
    RGN_NAME: RGN_ZONING_SYSTEM,
    SCOTLAND_NAME: SCOTLAND_ZONING_SYSTEM,
    NORTH_NAME: NORTH_ZONING_SYSTEM,
    TFN_AT_AGG_NAME: TFN_AT_AGG_ZONING_SYSTEM
}
KNOWN_GEOGRAPHIES = {
    **KNOWN_GEOGRAPHIES,
    **LSOAS_BY_GOR,
    **MSOAS_BY_GOR,
    **LADS_BY_GOR,
    **RGNS_BY_GOR
}

# --- GENERATE TRANSLATIONS FOR CACHE --- #

# if __name__ == '__main__':
#     # generate the zone systems required to translate the Scottish data
#     # correspondence between TfN area types and the aggregated versions can be found in
#     # "F:\Working\Land-Use\SHAPEFILES\CORRESPONDENCES\Scottish Data Zones 2011_TO_TFN_AREA_TYPES.csv"
#     # "F:\Working\Land-Use\SHAPEFILES\CORRESPONDENCES\LSOA_2021_EW_BFC_V8_NORTH_TO_TFN_AREA_TYPES.csv"
#     # this is basically to translate 'scottish urban' area type 20 to 'city / major' from the north
#     SCOTLAND_ZONING_SYSTEM.translate(
#         TFN_AT_AGG_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     NORTH_ZONING_SYSTEM.translate(
#         TFN_AT_AGG_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     LSOA_2011_ZONING_SYSTEM.translate(
#         LSOA_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     MSOA_2011_ZONING_SYSTEM.translate(
#         MSOA_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     MSOA_2011_ZONING_SYSTEM.translate(
#         LSOA_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     LSOA_ZONING_SYSTEM.translate(
#         MSOA_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     LSOA_ZONING_SYSTEM.translate(
#         LAD_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     LSOA_ZONING_SYSTEM.translate(
#         RGN_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     MSOA_ZONING_SYSTEM.translate(
#         LAD_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     MSOA_ZONING_SYSTEM.translate(
#         RGN_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#     LAD_ZONING_SYSTEM.translate(
#         RGN_ZONING_SYSTEM,
#         cache_path=CACHE_FOLDER
#     )
#
#     for i in range(0, len(GORS)):
#
#         lsoa = list(LSOAS_BY_GOR.values())[i]
#         msoa = list(MSOAS_BY_GOR.values())[i]
#         lad = list(LADS_BY_GOR.values())[i]
#         gor = list(RGNS_BY_GOR.values())[i]
#
#         lsoa.translate(
#             msoa,
#             cache_path=CACHE_FOLDER
#         )
#         lsoa.translate(
#             lad,
#             cache_path=CACHE_FOLDER
#         )
#         lsoa.translate(
#             gor,
#             cache_path=CACHE_FOLDER
#         )
#         msoa.translate(
#             lad,
#             cache_path=CACHE_FOLDER
#         )
#         msoa.translate(
#             gor,
#             cache_path=CACHE_FOLDER
#         )
#         lad.translate(
#             gor,
#             cache_path=CACHE_FOLDER
#         )
