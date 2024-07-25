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
lad_4_digit_sic = data_processing.read_dvector_from_config(config=config, key='lad_4_digit_sic')

msoa_2011_2_digit_sic = data_processing.read_dvector_from_config(
        config=config,
        key='msoa_2011_2_digit_sic'
)

lsoa_2011_1_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    key='lsoa_2011_1_digit_sic'
)

msoa_2011_2_digit_sic_1_digit_sic_splits = data_processing.read_dvector_from_config(
    config=config,
    key='msoa_2011_2_digit_sic_1_digit_sic_splits'
)
ons_sic_soc_splits_lu = data_processing.read_dvector_from_config(
    config=config,
    key='ons_sic_soc_splits_lu'
)

wfj = data_processing.read_dvector_from_config(config=config, key='wfj')

# --- Step 1 --- #
LOGGER.info('--- Step 1 ---')
LOGGER.info(
    'Balance the input datasets to each have the same totals at LAD level as the 4 Digit SIC 2022 BRES data'
)

lad_2011_2_digit_sic = (
    msoa_2011_2_digit_sic.translate_zoning(
        new_zoning=constants.LAD_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=False,
    )
)

lad_2011_1_digit_sic = (
    lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LAD_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=False,
    )
)

lad_total = lad_4_digit_sic.add_segment(
    constants.CUSTOM_SEGMENTS['total'], split_method='split'
).aggregate(['total'])

msoa_total_at_lad = lad_2011_2_digit_sic.add_segment(
    constants.CUSTOM_SEGMENTS['total'], split_method='split'
).aggregate(['total'])

lsoa_total_at_lad = lad_2011_1_digit_sic.add_segment(
    constants.CUSTOM_SEGMENTS['total'], split_method='split'
).aggregate(['total'])

msoa_adj_factors = lad_total / msoa_total_at_lad

lsoa_adj_factors = lad_total / lsoa_total_at_lad


# TODO: consider having a output/log of where the increases are outside expectations. Along the lines of
# sig_increases = adjustment_factors.data[adjustment_factors.data.ge(1.1)]
# sig_decreases = adjustment_factors.data[adjustment_factors.data.le(0.9)]

rehydrated_adj_factors_for_msoa = (
    msoa_adj_factors.add_segment(constants.CUSTOM_SEGMENTS['sic_2_digit'])
    .aggregate([constants.CUSTOM_SEGMENTS['sic_2_digit'].name])
    .translate_zoning(
        new_zoning=constants.MSOA_2011_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False,
    )
)

rehydrated_adj_factors_for_lsoa = (
    lsoa_adj_factors.add_segment(constants.CUSTOM_SEGMENTS['sic_1_digit'])
    .aggregate([constants.CUSTOM_SEGMENTS['sic_1_digit'].name])
    .translate_zoning(
        new_zoning=constants.LSOA_2011_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False,
    )
)

adj_msoa_2011_2_digit_sic = msoa_2011_2_digit_sic * rehydrated_adj_factors_for_msoa

adj_lsoa_2011_1_digit_sic = lsoa_2011_1_digit_sic * rehydrated_adj_factors_for_lsoa


# --- Step 2 --- #
LOGGER.info('--- Step 2 ---')
LOGGER.info('Exporting district-based 4 Digit SIC 2022 BRES data (Output E1)')
# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E1',
        dvector=lad_4_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 3 --- #
LOGGER.info('--- Step 3 ---')
LOGGER.info('Convert 2 Digit SIC 2022 BRES data held in MSOA 2011 zoning to 2021 MSOA (Output E2)')
# LAD is already at LAD 2021 zoning so doesn't need translating
msoa_2021_2_digit_sic = adj_msoa_2011_2_digit_sic.translate_zoning(
        new_zoning=constants.MSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E2',
        dvector=msoa_2021_2_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 4 --- #
LOGGER.info('--- Step 4 ---')
LOGGER.info('Convert 1 Digit SIC 2022 BRES data held in LSOA 2011 zoning to 2021 LSOA (Output E3)')
lsoa_2021_1_digit_sic = adj_lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True
)

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E3',
        dvector=lsoa_2021_1_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 5 --- #
LOGGER.info('--- Step 5 ---')
LOGGER.info(f'Converting SIC SOC proportions from Region to LSOA 2021 level (Output E4)')
ons_sic_soc_splits_lsoa = ons_sic_soc_splits_lu.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

LOGGER.info(f'Applying SOC group proportions to BRES 1-digit SIC jobs')
jobs_by_lsoa_with_soc_group = (
    lsoa_2021_1_digit_sic * ons_sic_soc_splits_lsoa
)

LOGGER.info('Converting proportions of SIC 2 digit by SIC 1 digit by SOC groups jobs to LSOA 2021')
lsoa_2021_2_digit_sic_1_splits = msoa_2011_2_digit_sic_1_digit_sic_splits.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
)

LOGGER.info(f'Applying SOC group proportions to BRES 2-digit SIC jobs')
jobs_by_sic_soc_lsoa = lsoa_2021_2_digit_sic_1_splits * jobs_by_lsoa_with_soc_group

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E4',
        dvector=jobs_by_sic_soc_lsoa,
        dvector_dimension='jobs'
)

# --- Step 6 --- #
LOGGER.info('--- Step 6 ---')
LOGGER.info(f'Uplifting Output E4 to Workforce jobs (WFJ) levels by region (Output E4_2)')

output_e4_by_rgn = jobs_by_sic_soc_lsoa.translate_zoning(
    new_zoning=constants.RGN_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.SPATIAL,
    check_totals=False
)

e4_total_by_rgn = output_e4_by_rgn.add_segment(
    constants.CUSTOM_SEGMENTS['total'], split_method='split'
).aggregate(['total'])

factors = wfj / e4_total_by_rgn

rehydrated_adj_factors_for_e4_2 = (
    factors.add_segment(constants.CUSTOM_SEGMENTS['sic_1_digit'])
    .add_segment(constants.CUSTOM_SEGMENTS['sic_2_digit'])
    .add_segment(constants.CUSTOM_SEGMENTS['soc_3'])
    .aggregate([
        constants.CUSTOM_SEGMENTS['sic_1_digit'].name,
        constants.CUSTOM_SEGMENTS['sic_2_digit'].name,
        constants.CUSTOM_SEGMENTS['soc_3'].name
        ])
    .translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )
)

output_e4_2 = jobs_by_sic_soc_lsoa * rehydrated_adj_factors_for_e4_2

data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E4_2',
        dvector=output_e4_2,
        dvector_dimension='jobs'
)
