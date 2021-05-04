"""
Constants from Land Use
"""

import os

import pandas as pd

# PATHS
# Default land use folder
LU_FOLDER = 'Y://NorMITs Land Use'
DATA_FOLDER = 'Y://Data Strategy//Data//'

# Most recent Land Use Iteration
LU_MR_ITER = 'iter3b'
LU_IMPORTS = 'import'
LU_REFS = 'Lookups'

# Path to a default land use build
RESI_LAND_USE_MSOA = os.path.join(
    LU_FOLDER,
    LU_MR_ITER,
    'outputs',
    'land_use_output_safe_msoa.csv'
)

EMPLOYMENT_MSOA = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'HSL 2018',
    'non_freight_msoa_2018.csv'
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
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 4,
    6: 4,
    7: 4
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
