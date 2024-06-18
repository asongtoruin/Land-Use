from pathlib import Path
import logging

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
