from pathlib import Path
import logging

import yaml
from caf.core.zoning import TranslationWeighting

from land_use import constants
from land_use import data_processing


def read_dvector(config: dict, key: str) -> data_processing.DVector:
    return data_processing.read_dvector_data(
        input_root_directory=config["input_root_directory"],
        **config[key],
    )


def translate_to_msoa_2021(dvec_in: data_processing.DVector) -> data_processing.DVector:
    return dvec_in.translate_zoning(
        new_zoning=constants.MSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True,
    )


def translate_to_lsoa_2021(dvec_in: data_processing.DVector) -> data_processing.DVector:
    return dvec_in.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True,
    )


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
bres_employment22_lad_4digit_sic = read_dvector(
    config=config, key="bres_employment22_lad_4digit_sic"
)
bres_2022_employment_msoa_2011_2_digit_sic = read_dvector(
    config=config, key="bres_2022_employment_msoa_2011_2_digit_sic"
)
bres_2022_employment_lsoa_2011_1_digit_sic = read_dvector(
    config=config, key="bres_2022_employment_lsoa_2011_1_digit_sic"
)

LOGGER.info("Convert data held in LSOA 2011 and MSOA 2011 zoning to 2021")
LOGGER.info("LAD is already at LAD 2021 zoning so doesn't need translating")

bres_2022_employment_msoa_2021_2_digit_sic = translate_to_msoa_2021(
    bres_2022_employment_msoa_2011_2_digit_sic
)

bres_2022_employment_lsoa_2021_1_digit_sic = translate_to_lsoa_2021(
    bres_2022_employment_lsoa_2011_1_digit_sic
)

# TODO clarify what names we want to give the outputs for now given generic X* names
LOGGER.info(rf"Writing to {OUTPUT_DIR}\Output X1.hdf")

data_processing.summary_reporting(
    dvector=bres_employment22_lad_4digit_sic, dimension="jobs"
)
bres_employment22_lad_4digit_sic.save(OUTPUT_DIR / "Output X1.hdf")
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_employment22_lad_4digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX1",
        value_name="jobs",
    )

LOGGER.info(rf"Writing to {OUTPUT_DIR}\Output X2.hdf")

data_processing.summary_reporting(
    dvector=bres_2022_employment_msoa_2021_2_digit_sic, dimension="jobs"
)
bres_2022_employment_msoa_2021_2_digit_sic.save(OUTPUT_DIR / "Output X2.hdf")
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_msoa_2021_2_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX2",
        value_name="jobs",
    )

LOGGER.info(rf"Writing to {OUTPUT_DIR}\Output X3.hdf")

data_processing.summary_reporting(
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic, dimension="jobs"
)
bres_2022_employment_lsoa_2021_1_digit_sic.save(OUTPUT_DIR / "Output X3.hdf")
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputX3",
        value_name="jobs",
    )
