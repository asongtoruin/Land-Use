from pathlib import Path

from caf.core.zoning import ZoningSystem

# Define location of zoning cache folder
CACHE_FOLDER = Path(r'.\CACHE')

# Define the name of the model zoning to be consistent with the cache file.
# The model zoning is the zone system the outputs of the population / employment
# models are intended to be used in at the end.
MODEL_ZONING = 'lsoa2021'
MODEL_ZONING_SYSTEM = ZoningSystem.get_zoning(MODEL_ZONING, search_dir=CACHE_FOLDER)

# Define other zone systems which are defined in the cache which may be used
# as part of the modelling.
MSOA = 'msoa2021'
MSOA_ZONING_SYSTEM = ZoningSystem.get_zoning(MSOA, search_dir=CACHE_FOLDER)

# Dictionary of references for the yaml file
# TODO link with definitions above
geographies = {
    'LSOA2021': MODEL_ZONING_SYSTEM,
    'MSOA2021': MSOA_ZONING_SYSTEM
}
