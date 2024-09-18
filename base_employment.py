from pathlib import Path

import pandas as pd
import yaml

import caf.core as cc
from caf.core.data_structures import DVector
from caf.core.segments import SegmentsSuper
from caf.core.zoning import TranslationWeighting

from land_use import constants, data_processing
from land_use import logging as lu_logging


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

LOGGER = lu_logging.configure_logger(OUTPUT_DIR, log_name='employment')

# --- Step 0 --- #
# read in the data from the config file
block = 'base_data'
LOGGER.info("Importing BRES 2022 data from config file")
# note this data is only for England and Wales
lad_4_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='lad_4_digit_sic'
)

msoa_2011_2_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='msoa_2011_2_digit_sic'
)

lsoa_2011_1_digit_sic = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='lsoa_2011_1_digit_sic'
)

msoa_2011_splits_sic_1_digit_to_2_digit = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='msoa_2011_2_digit_sic_1_digit_sic_splits'
)

ons_sic_soc_jobs_lu = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='ons_sic_soc_jobs_lu'
)

wfj = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='wfj'
)

soc_4_factors = data_processing.read_dvector_from_config(
    config=config,
    data_block=block,
    key='soc_4_factors'
)

# --- Step 0 --- #
LOGGER.info('--- Step 0 ---')
LOGGER.info(
    'Balance the input datasets to each have the same totals at LAD level as the 4 Digit SIC 2022 BRES data'
)

lad_2011_2_digit_sic = (
    msoa_2011_2_digit_sic.translate_zoning(
        new_zoning=constants.LAD_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=False,
    )
)

lad_2011_1_digit_sic = (
    lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LAD_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=False,
    )
)

lad_total = lad_4_digit_sic.add_segments(
    [constants.CUSTOM_SEGMENTS['total']], split_method='split'
).aggregate(['total'])

msoa_total_at_lad = lad_2011_2_digit_sic.add_segments(
    [constants.CUSTOM_SEGMENTS['total']], split_method='split'
).aggregate(['total'])

lsoa_total_at_lad = lad_2011_1_digit_sic.add_segments(
    [constants.CUSTOM_SEGMENTS['total']], split_method='split'
).aggregate(['total'])

msoa_adj_factors = lad_total / msoa_total_at_lad

lsoa_adj_factors = lad_total / lsoa_total_at_lad


# TODO: consider having a output/log of where the increases are outside expectations. Along the lines of
# sig_increases = adjustment_factors.data[adjustment_factors.data.ge(1.1)]
# sig_decreases = adjustment_factors.data[adjustment_factors.data.le(0.9)]

rehydrated_adj_factors_for_msoa = (
    msoa_adj_factors.add_segments([SegmentsSuper('sic_2_digit').get_segment()])
    .aggregate([SegmentsSuper('sic_2_digit').get_segment().name])
    .translate_zoning(
        new_zoning=constants.MSOA_2011_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False,
    )
)

rehydrated_adj_factors_for_lsoa = (
    lsoa_adj_factors.add_segments([SegmentsSuper('sic_1_digit').get_segment()])
    .aggregate([SegmentsSuper('sic_1_digit').get_segment().name])
    .translate_zoning(
        new_zoning=constants.LSOA_2011_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False,
    )
)


# apply proportions and fill in nas with 0 (which will be for the unemployed rows)
adj_msoa_2011_2_digit_sic = rehydrated_adj_factors_for_msoa * msoa_2011_2_digit_sic
adj_msoa_2011_2_digit_sic.fillna(0)

adj_lsoa_2011_1_digit_sic = rehydrated_adj_factors_for_lsoa * lsoa_2011_1_digit_sic
adj_lsoa_2011_1_digit_sic.fillna(0)


# --- Step 1 --- #
LOGGER.info('--- Step 1 ---')
LOGGER.info('Exporting district-based 4 Digit SIC 2022 BRES data (Output E1)')
# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E1',
        dvector=lad_4_digit_sic,
        dvector_dimension='jobs'
)

# --- Step 2 --- #
LOGGER.info('--- Step 2 ---')
LOGGER.info('Convert 2 Digit SIC 2022 BRES data held in MSOA 2011 zoning to 2021 MSOA (Output E2)')
# LAD is already at LAD 2021 zoning so doesn't need translating
msoa_2021_2_digit_sic = adj_msoa_2011_2_digit_sic.translate_zoning(
        new_zoning=constants.MSOA_EWS_ZONING_SYSTEM,
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


# --- Step 3 --- #
LOGGER.info('--- Step 3 ---')
LOGGER.info('Convert 1 Digit SIC 2022 BRES data held in LSOA 2011 zoning to 2021 LSOA (Output E3)')
lsoa_2021_1_digit_sic = adj_lsoa_2011_1_digit_sic.translate_zoning(
        new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
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

# --- Step 4 --- #
LOGGER.info('--- Step 4 ---')
LOGGER.info(f'Converting SIC SOC jobs from Region to LSOA 2021 level (Output E4)')
ons_sic_soc_jobs_lsoa = ons_sic_soc_jobs_lu.translate_zoning(
    new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

jobs_by_lsoa_with_soc_group = data_processing.apply_proportions(
    source_dvector=ons_sic_soc_jobs_lsoa, 
    apply_to=lsoa_2021_1_digit_sic
)

# Note a warning is generated here about combinations with SOC as 4. We can ignore it.
LOGGER.info(f'Applying SOC group proportions to BRES 1-digit SIC jobs')
jobs_by_lsoa_with_soc_group = data_processing.apply_proportions(
    source_dvector=ons_sic_soc_jobs_lsoa,
    apply_to=lsoa_2021_1_digit_sic
)

LOGGER.info('Converting proportions of SIC 2 digit by SIC 1 digit by SOC groups jobs to LSOA 2021')
lsoa_2021_1_digit_2_digit_sic_splits = msoa_2011_splits_sic_1_digit_to_2_digit.translate_zoning(
        new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
)

# Note a warning is generated here about combinations with SOC as 4. We can ignore it.
LOGGER.info(f'Applying SOC group proportions to BRES 2-digit SIC jobs')
jobs_by_sic_soc_lsoa_no_soc_4 = data_processing.apply_proportions(
    source_dvector=lsoa_2021_1_digit_2_digit_sic_splits,
    apply_to=jobs_by_lsoa_with_soc_group
)

# now need to expand the data to include soc 4 (unemployed) based on factors
totals = jobs_by_sic_soc_lsoa_no_soc_4.add_segments(
    [constants.CUSTOM_SEGMENTS['total']]).aggregate(['total'])

totals_with_segs = totals.add_segments([
    SegmentsSuper('sic_1_digit').get_segment(),
        SegmentsSuper('sic_2_digit').get_segment(),
        SegmentsSuper('soc').get_segment()]
).aggregate([
    'sic_1_digit', 'sic_2_digit', 'soc'])

soc_1_3_totals = totals_with_segs.filter_segment_value('sic_1_digit', -1)

soc_1_3_totals = soc_1_3_totals.add_segments([SegmentsSuper('sic_1_digit').get_segment()])

# TODO: switch to using translate zoning directly, 
# however currently doesn't work with one row df, needs fix in caf.toolkit
# soc_4_factors_lsoa = soc_4_factors.translate_zoning(
#         new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
#         cache_path=constants.CACHE_FOLDER,
#         weighting=TranslationWeighting.NO_WEIGHT,
#         check_totals=True,
# )

#### workaround start ################################################################
soc_4_factors_with_soc = soc_4_factors.add_segments(["soc"])

soc_4_factors_with_soc_lsoa = soc_4_factors_with_soc.translate_zoning(
        new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False,
)

soc_4_factors_lsoa = soc_4_factors_with_soc_lsoa.aggregate(["total"])

soc_4_factors_lsoa.data = soc_4_factors_lsoa.data / len(soc_4_factors_with_soc_lsoa.data)

#### workaround end ##################################################################

# sort out the segmentations to match the full dataset
soc_4_factors_lsoa = (
    soc_4_factors_lsoa
    .add_segments(["sic_2_digit", "soc", "sic_1_digit"])
    .aggregate(["sic_2_digit", "soc", "sic_1_digit"])
    .filter_segment_value("sic_2_digit", -1)
    )

# apply factors to soc 1-3 totals to give the number of soc_4 people by lsoa
soc_4_row = soc_1_3_totals * soc_4_factors_lsoa

jobs_by_sic_soc_lsoa = jobs_by_sic_soc_lsoa_no_soc_4.concat(soc_4_row)

# save output to hdf and csvs for checking
data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E4',
        dvector=jobs_by_sic_soc_lsoa,
        dvector_dimension='jobs'
)

LOGGER.info(f'Uplifting Output E4 to Workforce jobs (WFJ) levels by region (Output E4_2)')

output_e4_by_rgn = jobs_by_sic_soc_lsoa_no_soc_4.translate_zoning(
    new_zoning=constants.RGN_EWS_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.SPATIAL,
    check_totals=False
)

e4_total_by_rgn = output_e4_by_rgn.add_segments(
    [constants.CUSTOM_SEGMENTS['total']], split_method='split'
).aggregate(['total'])

factors = wfj / e4_total_by_rgn

rehydrated_adj_factors_for_e4_2 = (
    factors.add_segments([
        SegmentsSuper('sic_1_digit').get_segment(),
        SegmentsSuper('sic_2_digit').get_segment(),
        SegmentsSuper('soc').get_segment()
    ])
    .aggregate([
        SegmentsSuper('sic_1_digit').get_segment().name,
        SegmentsSuper('sic_2_digit').get_segment().name,
        SegmentsSuper('soc').get_segment().name
        ])
    .translate_zoning(
        new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )
)

# Apply WFJ adjustment factors to *all* population - i.e. we assume Unemployed get the same uplift as employed.
output_e4_2 = jobs_by_sic_soc_lsoa * rehydrated_adj_factors_for_e4_2

data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E4_2',
        dvector=output_e4_2,
        dvector_dimension='jobs'
)

LOGGER.info('--- Step 5 ---')
LOGGER.info(f'Combining Output E1 (+soc 4) and E4 to give Jobs by LSOA SIC 4 digit and SOC group (1-4) (Output E5)')
# Output E5

# First need to create a Dummy DVector of 1's to concat to E1
columns = list(lad_4_digit_sic.data.columns)

# infill with 1's as we will be taking all the proportion for these rows from e4
e1_soc_4_row = pd.DataFrame(1.0, index=range(1), columns=columns)
e1_soc_4_row["sic_4_digit"] = -1
e1_soc_4_row = e1_soc_4_row.set_index("sic_4_digit")

seg_new = cc.Segmentation(
    cc.SegmentationInput(
        enum_segments=["sic_4_digit"],
        naming_order=["sic_4_digit"],
        subsets={"sic_4_digit": [-1]},
    )
)
soc_4_dummy = DVector(
    segmentation=seg_new,
    import_data=e1_soc_4_row,
    zoning_system=lad_4_digit_sic.zoning_system,
)
# Add on dummy row for soc 4
adjusted_e1 = lad_4_digit_sic.concat(soc_4_dummy)

# Now having got the adjusted e1 we can prepare it for merging with e4
e1_with_sic_2_lad = adjusted_e1.translate_segment(
    from_seg=SegmentsSuper('sic_4_digit').get_segment(),
    to_seg=SegmentsSuper('sic_2_digit').get_segment(),
    drop_from=False
)
e1_with_sic_2_lsoa = e1_with_sic_2_lad.translate_zoning(
    new_zoning=constants.LSOA_EWS_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.SPATIAL,
    check_totals=False
)

# apply proportions, including getting soc 4 values to match e4
jobs_by_sic_2_4_soc_lsoa = data_processing.apply_proportions(
    source_dvector=e1_with_sic_2_lsoa, 
    apply_to=jobs_by_sic_soc_lsoa
)

data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference='Output E5',
        dvector=jobs_by_sic_2_4_soc_lsoa,
        dvector_dimension='jobs'
)
