from pathlib import Path
import logging

import numpy as np
import pandas as pd
import yaml
from caf.core.zoning import TranslationWeighting

from land_use import constants
from land_use import data_processing


# set up logging
log_formatter = logging.Formatter(
    fmt="[%(asctime)-15s] %(levelname)s - [%(filename)s#%(lineno)d::%(funcName)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger("land_use")

# load configuration file
with open(
    r"scenario_configurations\iteration_5\base_employment_config.yml", "r"
) as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config["output_directory"])
OUTPUT_DIR.mkdir(exist_ok=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config["output_intermediate_outputs"])

# define logging path based on config file
logging.basicConfig(
    format=log_formatter._fmt,
    datefmt=log_formatter.datefmt,
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "employment.log", mode="w"),
    ],
)

generate_summary_outputs = bool(config["output_intermediate_outputs"])

# process bres 2022

# read in the data from the config file
LOGGER.info("Importing bres 2022 data from config file")
# note this data is only for England and Wales
bres_employment22_lad_4digit_sic = data_processing.read_dvector_using_config(
    config=config, key="bres_employment22_lad_4digit_sic"
)
bres_2022_employment_msoa_2011_2_digit_sic = data_processing.read_dvector_using_config(
    config=config, key="bres_2022_employment_msoa_2011_2_digit_sic"
)
bres_2022_employment_lsoa_2011_1_digit_sic = data_processing.read_dvector_using_config(
    config=config, key="bres_2022_employment_lsoa_2011_1_digit_sic"
)

LOGGER.info("Convert data held in LSOA 2011 and MSOA 2011 zoning to 2021")
LOGGER.info("LAD is already at LAD 2021 zoning so doesn't need translating")

bres_2022_employment_msoa_2021_2_digit_sic = (
    bres_2022_employment_msoa_2011_2_digit_sic.translate_zoning(
        new_zoning=constants.MSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True,
    )
)

bres_2022_employment_lsoa_2021_1_digit_sic = (
    bres_2022_employment_lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True,
    )
)

output_file_name = "Output E1.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_employment22_lad_4digit_sic, dimension="jobs"
)
bres_employment22_lad_4digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_employment22_lad_4digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX1",
        value_name="jobs",
    )


output_file_name = "Output E2.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_msoa_2021_2_digit_sic, dimension="jobs"
)
bres_2022_employment_msoa_2021_2_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_msoa_2021_2_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX2",
        value_name="jobs",
    )

output_file_name = "Output E3.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic, dimension="jobs"
)
bres_2022_employment_lsoa_2021_1_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX3",
        value_name="jobs",
    )

ons_sic_soc_lu = data_processing.read_dvector_using_config(
    config=config, key="ons_sic_soc_lu"
)

ons_sic_soc_lsoa = ons_sic_soc_lu.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False,
)

LOGGER.info("Checking SIC-SOC splits by region have been allocated to LSOAs correctly")
# take some representative lsoas to see if the values match
data = [
    ("E12000001", "E01008162"),
    ("E12000002", "E01004766"),
    ("E12000003", "E01007317"),
    ("E12000004", "E01013453"),
    ("E12000005", "E01008881"),
    ("E12000006", "E01015589"),
    ("E12000007", "E01000001"),
    ("E12000008", "E01016016"),
    ("E12000009", "E01014370"),
    ("WALES", "W01000003"),
]
data_df = pd.DataFrame.from_records(data, columns=["gor", "example_lsoa"])
rgn_list = list(data_df["gor"])
example_lsoa_list = list(data_df["example_lsoa"])

# convert to numpy arrays to avoid column mis-match issues
ons_with_col_in_order = ons_sic_soc_lu.data[rgn_list].to_numpy()
ons_sample_lsoa = ons_sic_soc_lsoa.data[example_lsoa_list].to_numpy()

if np.all(ons_with_col_in_order == ons_sample_lsoa):
    LOGGER.info("Split match as expected")
else:
    LOGGER.error(
        "Splits do not match suggesting an issue with how the values have been allocated to regions."
    )

LOGGER.info("Applying sic-soc splits to sic jobs")
employment_by_losa_soc_3 = bres_2022_employment_lsoa_2021_1_digit_sic * ons_sic_soc_lsoa

LOGGER.info(f"Convert to MSOA as that is required for output")
employment_by_mosa_soc_3 = employment_by_losa_soc_3.translate_zoning(
    new_zoning=constants.MSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.SPATIAL,
    check_totals=True,
)

output_file_name = "Output E4.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")
bres_2022_employment_lsoa_2021_1_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=employment_by_mosa_soc_3,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputE4",
        value_name="jobs",
    )
