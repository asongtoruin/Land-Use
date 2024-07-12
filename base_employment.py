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
    format=log_formatter._fmt,  # type: ignore # error in imported library which?
    datefmt=log_formatter.datefmt,
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "employment.log", mode="w"),
    ]
)

# --- Step 0 --- #
# read in the data from the config file
LOGGER.info("Importing BRES 2022 data from config file")
# note this data is only for England and Wales
bres_2022_employment_lad_4_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    key='bres_2022_employment_lad_4_digit_sic'
)

bres_2022_employment_msoa_2011_2_digit_sic_jobs = data_processing.read_dvector_from_config(
        config=config,
        key='bres_2022_employment_msoa_2011_2_digit_sic_jobs'
)

bres_2022_employment_lsoa_2011_1_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    key='bres_2022_employment_lsoa_2011_1_digit_sic'
)

bres_2022_employment_msoa_2011_2_digit_sic_1_splits = data_processing.read_dvector_from_config(
    config=config,
    key='bres_2022_employment_msoa_2011_2_digit_sic_1_splits'
)
ons_sic_soc_splits_lu = data_processing.read_dvector_from_config(
    config=config,
    key='ons_sic_soc_splits_lu'
)

# --- Step 1 --- #
LOGGER.info('--- Step 1 ---')
LOGGER.info('Exporting district-based 4 Digit SIC 2022 BRES data (Output E1)')
# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E1',
        dvector=bres_2022_employment_lad_4_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 2 --- #
LOGGER.info('--- Step 2 ---')
LOGGER.info('Convert 2 Digit SIC 2022 BRES data held in MSOA 2011 zoning to 2021 MSOA (Output E2)')
# LAD is already at LAD 2021 zoning so doesn't need translating
bres_2022_employment_msoa_2021_2_digit_sic_jobs = bres_2022_employment_msoa_2011_2_digit_sic_jobs.translate_zoning(
        new_zoning=constants.MSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E2',
        dvector=bres_2022_employment_msoa_2021_2_digit_sic_jobs,
        dvector_dimension='jobs'
)

# --- Step 3 --- #
LOGGER.info('--- Step 3 ---')
LOGGER.info('Convert 1 Digit SIC 2022 BRES data held in LSOA 2011 zoning to 2021 LSOA (Output E3)')
bres_2022_employment_lsoa_2021_1_digit_sic = bres_2022_employment_lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E3',
        dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 4 --- #
LOGGER.info('--- Step 4 ---')
LOGGER.info(f'Converting SIC SOC proportions from Region to LSOA 2021 level (Output E4)')
ons_sic_soc_splits_lsoa = ons_sic_soc_splits_lu.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

LOGGER.info(f'Applying SOC group proportions to BRES 1-digit SIC jobs')
jobs_by_lsoa_with_soc_group = (
    bres_2022_employment_lsoa_2021_1_digit_sic * ons_sic_soc_splits_lsoa
)

LOGGER.info('Converting proportions of SIC 2 digit by SIC 1 digit by SOC groups jobs to LSOA 2021')
bres_2022_employment_lsoa_2021_2_digit_sic_1_splits = bres_2022_employment_msoa_2011_2_digit_sic_1_splits.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
)

LOGGER.info(f'Applying SOC group proportions to BRES 2-digit SIC jobs')
jobs_by_sic_soc_lsoa = bres_2022_employment_lsoa_2021_2_digit_sic_1_splits * jobs_by_lsoa_with_soc_group

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E4',
        dvector=jobs_by_sic_soc_lsoa,
        dvector_dimension='jobs'
)
