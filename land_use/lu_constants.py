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

# Path to a default land use build
RESI_LAND_USE_MSOA = os.path.join(
    LU_FOLDER,
    LU_MR_ITER,
    'outputs',
    'land_use_output_safe_msoa.csv'
)

EMPLOYMENT_MSOA = os.path.join(
    DATA_FOLDER,
    'HSL',
    '2018',
    'non_freight_msoa_2018.csv'
)

SCENARIO_FOLDERS = {'NTEM': 'SC00_NTEM',
                    'JAM': 'SC01_JAM',
                    'PP': 'SC02_PP',
                    'DD': 'SC03_DD',
                    'UZC': 'SC04_UZC'}

NTEM_POP_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'NTEM',
    'population',
    'future_population_growth.csv'
)

NTEM_EMP_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'NTEM',
    'employment',
    'future_workers_growth.csv'
)

# This is growth not values
NTEM_CA_GROWTH = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'scenarios',
    'NTEM',
    'car ownership',
    'ca_future_shares.csv'
)

SOC_2DIGIT_SIC = os.path.join(
    LU_FOLDER,
    LU_IMPORTS,
    'fy_soc',
    'soc_2_digit_sic_2018.csv'
)



# REFERENCES
# purposes to apply soc split to
SOC_P = [1, 2, 12]

# Car availabiity reference
# TODO: This is wrong just now
CA_MODEL = pd.DataFrame({'cars': [0, 1, 2, 3],
                         'ca': [1, 2, 2, 2]})