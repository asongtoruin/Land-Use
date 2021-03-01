"""
Constants from Land Use
"""

import os

import pandas as pd

# PATHS
# Default land use folder
LU_FOLDER = 'Y://NorMITs Land Use'

# Most recent Land Use Iteration
LU_MR_ITER = 'iter3b'
LU_IMPORTS = 'import'

# Path to a default land use build
LAND_USE_MSOA = os.path.join(
    LU_FOLDER,
    LU_MR_ITER,
    'outputs',
    'land_use_output_msoa.csv'
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