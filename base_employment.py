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

LOGGER.info("Processing BRES 2022")

# read in the data from the config file
LOGGER.info("Importing bres 2022 data from config file")
# note this data is only for England and Wales
bres_2022_employment_lad_4_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    key="bres_2022_employment_lad_4_digit_sic"
)

bres_2022_employment_msoa_2011_2_digit_sic_jobs = data_processing.read_dvector_from_config(
        config=config,
        key="bres_2022_employment_msoa_2011_2_digit_sic_jobs"
)

bres_2022_employment_lsoa_2011_1_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    key="bres_2022_employment_lsoa_2011_1_digit_sic"
)

bres_2022_employment_msoa_2011_2_digit_sic_1_splits = data_processing.read_dvector_from_config(
    config=config,
    key="bres_2022_employment_msoa_2011_2_digit_sic_1_splits"
)

LOGGER.info("Convert data held in LSOA 2011 and MSOA 2011 zoning to 2021")
LOGGER.info("LAD is already at LAD 2021 zoning so doesn't need translating")

bres_2022_employment_msoa_2021_2_digit_sic_jobs = bres_2022_employment_msoa_2011_2_digit_sic_jobs.translate_zoning(
        new_zoning=constants.MSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

bres_2022_employment_lsoa_2021_1_digit_sic = bres_2022_employment_lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

output_file_name = "Output E1.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_lad_4_digit_sic,
    dimension="jobs"
)
bres_2022_employment_lad_4_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_lad_4_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputE1",
        value_name="jobs"
    )

data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=output_file_name,
    dvector=bres_2022_employment_lad_4_digit_sic,
    dvector_dimension="job"
)


output_file_name = "Output E2.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_msoa_2021_2_digit_sic_jobs,
    dimension="jobs"
)
bres_2022_employment_msoa_2021_2_digit_sic_jobs.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_msoa_2021_2_digit_sic_jobs,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputE2",
        value_name="jobs"
    )
data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=output_file_name,
    dvector=bres_2022_employment_msoa_2021_2_digit_sic_jobs,
    dvector_dimension="job"
)

output_file_name = "Output E3.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
    dimension="jobs"
)

bres_2022_employment_lsoa_2021_1_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputE3",
        value_name="jobs"
    )
data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=output_file_name,
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
    dvector_dimension="job"
)

LOGGER.info("Moving onto the steps required to create Output E4")

LOGGER.info("Calculate splits by industry and allocate to LSOA level")
ons_sic_soc_splits_lu = data_processing.read_dvector_from_config(
    config=config,
    key="ons_sic_soc_splits_lu"
)

ons_sic_soc_splits_lsoa = ons_sic_soc_splits_lu.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

LOGGER.info("calcualate jobs by lsoa with soc group")

jobs_by_lsoa_with_soc_group = (
    bres_2022_employment_lsoa_2021_1_digit_sic * ons_sic_soc_splits_lsoa
)

LOGGER.info("Convert proportion of sic 2 digit by sic 1 digit to apply to soc groups jobs by lsoa")
bres_2022_employment_lsoa_2021_2_digit_sic_1_splits = bres_2022_employment_msoa_2011_2_digit_sic_1_splits.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
)

LOGGER.info("create output E4")
output_e4 = bres_2022_employment_lsoa_2021_2_digit_sic_1_splits * jobs_by_lsoa_with_soc_group

output_file_name = "Output E4.hdf"
LOGGER.info(rf"Writing to {OUTPUT_DIR}\{output_file_name}")

data_processing.summary_reporting(
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic, dimension="jobs"
)
bres_2022_employment_lsoa_2021_1_digit_sic.save(OUTPUT_DIR / output_file_name)
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=output_e4,
        output_directory=OUTPUT_DIR,
        output_reference=f"OutputE4",
        value_name="jobs",
    )

data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=output_file_name,
    dvector=bres_2022_employment_lsoa_2021_1_digit_sic,
    dvector_dimension="job",
)
