"""
Constants from Land Use
"""

import os

import pandas as pd

# SUFFIXES AND SEMI-STATIC CONFIG
COMPRESSION_SUFFIX = '.pbz2'
PROCESS_COUNT = -2

# PATHS
# Default land use folder
LU_FOLDER = 'I://NorMITs Land Use//'
BY_FOLDER = 'base_land_use'
FY_FOLDER = 'future_land_use'
DATA_FOLDER = 'Y://Data Strategy//Data//'

# Most recent Land Use Iteration
LU_MR_ITER = 'iter4f'
FYLU_MR_ITER = 'iter3c'
LU_IMPORTS = 'import'
LU_REFS = 'Lookups'

# Inputs
ZONE_NAME = 'MSOA'
ZONES_FOLDER = 'I:/NorMITs Synthesiser/Zone Translation/'
ZONE_TRANSLATION_PATH = ZONES_FOLDER + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv'
ADDRESSBASE_PATH_LIST = 'I:/Data/AddressBase/2018/List of ABP datasets.csv'
#LU_FOLDER + '/' + LU_IMPORTS + '/AddressBase/2018/List of ABP datasets.csv'
KS401_PATH = LU_FOLDER + '/' + LU_IMPORTS + '/' + 'Nomis Census 2011 Head & Household/KS401UK_LSOA.csv'
LU_AREA_TYPES = LU_FOLDER + '/area types/TfNAreaTypesLookup.csv'
ALL_RES_PROPERTY_PATH = 'I:/NorMITs Land Use/import/AddressBase/2018/processed'
CTripEnd_Database = 'I:/Data/NTEM/NTEM 7.2 outputs for TfN/'


# Path to a default land use build
RESI_LAND_USE_MSOA = os.path.join(
    LU_FOLDER,
    BY_FOLDER,
    LU_MR_ITER,
    'outputs',
    'land_use_output_safe_msoa.csv'
)

NON_RESI_LAND_USE_MSOA = os.path.join(
    LU_FOLDER,
    BY_FOLDER,
    LU_MR_ITER,
    'outputs',
    'land_use_2018_emp.csv')

E_CAT_DATA = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'HSL 2018',
    'non_freight_msoa_2018.csv'
)

UNM_DATA = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'SOC mix',
    'nomis_2021_10_12_165818.csv'
)

MSOA_REGION = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'msoa_region.csv'
)

# TODO: Doesn't exist yet
MSOA_SECTOR = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'msoa_sector.csv'
)

SCENARIO_FOLDERS = {'NTEM': 'SC00_NTEM',
                    'SC01_JAM': 'SC01_JAM',
                    'SC02_PP': 'SC02_PP',
                    'SC03_DD': 'SC03_DD',
                    'SC04_UZC': 'SC04_UZC'}

# TODO: Fill this in
SCENARIO_NUMBERS = {0: 'SC00_NTEM',
                    1: 'SC01_JAM'}

NTEM_POP_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'SC00_NTEM',
    'population',
    'future_population_growth.csv'
)

NTEM_EMP_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'SC00_NTEM',
    'employment',
    'future_workers_growth.csv'
)

# This is growth not values
NTEM_CA_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'SC00_NTEM',
    'car ownership',
    'ca_future_shares.csv'
)

NTEM_DEMOGRAPHICS_MSOA = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'SC00_NTEM',
    'demographics',
    'future_demographic_values.csv'
)

SOC_2DIGIT_SIC = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'SOC Mix',
    'soc_2_digit_sic_2018.csv'
)



# REFERENCES
# purposes to apply soc split to
SOC_P = [1, 2, 12]

# Property type dictionary: combines all flat types
PROPERTY_TYPE = {
    1: 1,  # detached
    2: 2,  # semi-detached
    3: 3,  # terrace
    4: 4,  # purpose-built flat
    5: 4,  # shared flat
    6: 4,  # flat in commercial
    7: 4,  # mobile home
    8: 8   # communal establishment
}

# Property type description to code
# TODO: lots of overlap with PROPERTY_TYPE, can it be a single object? (Used in main_build)
HOUSE_TYPE = {
    'Detached': 1,
    'Semi-detached': 2,
    'Terraced': 3,
    'Flat': 4
}

# NS-SeC category mapping
NS_SEC = {
    'NS-SeC 1-2': 1,
    'NS-SeC 3-5': 2,
    'NS-SeC 6-7': 3,
    'NS-SeC 8': 4,
    'NS-SeC L15': 5
}

# Car availabiity reference
CA_MODEL = pd.DataFrame({'cars': [0, 1, 2, 3],
                         'ca': [1, 2, 2, 2]})

REF_PATH = os.path.join(LU_FOLDER, LU_IMPORTS, LU_REFS)

AGE_REF = pd.read_csv(os.path.join(REF_PATH,
                                   'age_index.csv'))

GENDER_REF = pd.read_csv(os.path.join(REF_PATH,
                                      'gender_index.csv'))

HC_REF = pd.read_csv(os.path.join(REF_PATH,
                                  'household_composition_index.csv'))

# NTEM Traveller Type Reference
RAW_TT_INDEX = pd.read_csv(os.path.join(REF_PATH,
                           'ntem_traveller_types.csv'))

TT_INDEX = pd.read_csv(os.path.join(REF_PATH,
                                    'ntem_traveller_types_normalised.csv'))

# TfN Traveller Type Reference
TFN_TT_INDEX = pd.read_csv(os.path.join(REF_PATH,
                                        'tfn_traveller_types_normalised.csv'),
                           dtype=int)

TFN_TT_DESC = pd.read_csv(os.path.join(REF_PATH,
                                       'tfn_traveller_types_illustrated.csv'))