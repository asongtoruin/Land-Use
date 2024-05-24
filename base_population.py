from pathlib import Path
import logging

import yaml
from caf.core.zoning import TranslationWeighting
from caf.core.segmentation import SegmentsSuper

from land_use import constants
from land_use import data_processing


# set up logging
log_formatter = logging.Formatter(
    fmt='[%(asctime)-15s] %(levelname)s - [%(filename)s#%(lineno)d::%(funcName)s]: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)

LOGGER = logging.getLogger('land_use')

# load configuration file
with open(r'scenario_configurations\iteration_5\base_population_config.yml', 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])
OUTPUT_DIR.mkdir(exist_ok=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config['output_intermediate_outputs'])

# define logging path based on config file
logging.basicConfig(
    format=log_formatter._fmt,
    datefmt=log_formatter.datefmt,
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / 'population.log', mode='w')
    ],
)

# read in the data from the config file
LOGGER.info('Importing data from config file')
occupied_households = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['occupied_households'])
unoccupied_households = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['unoccupied_households'])
ons_table_1 = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_1'])
addressbase_dwellings = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['addressbase_dwellings'])
ons_table_2 = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_2'])
mype_2022 = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['mype_2022'])
ons_table_4 = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_4'])
hh_age_gender_2021 = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['hh_age_gender_2021'])
ons_table_3_econ = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_3_econ'])
ons_table_3_emp = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_3_emp'])
ons_table_3_soc = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_3_soc'])

# --- Step 1 --- #
LOGGER.info('--- Step 1 ---')
LOGGER.info(f'Calculating NS-SeC proportions by dwelling type')
# calculate NS-SeC splits of households by
# dwelling type by LSOA
total_hh_by_hh = ons_table_4.aggregate(segs=['accom_h'])
proportion_ns_sec = ons_table_4 / total_hh_by_hh

# fill missing proportions with 0 as they are where the total hh is zero in the census data
# TODO is this what we want to do? This drops some dwellings from the addressbase wherever the census total is zero.
proportion_ns_sec.data = proportion_ns_sec.data.fillna(0)

LOGGER.info(f'Applying NS-SeC proportions to AddressBase dwellings')
# apply proportional factors based on hh ns_sec to the addressbase dwellings
hh_by_nssec = addressbase_dwellings * proportion_ns_sec

# check against original addressbase data
# check = hh_by_nssec.aggregate(segs=['accom_h'])

LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output A.hdf')
data_processing.summary_reporting(
    dvector=hh_by_nssec,
    dimension='households',
    detailed_logs=True
)
hh_by_nssec.save(OUTPUT_DIR / 'Output A.hdf')
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=hh_by_nssec,
        output_directory=OUTPUT_DIR,
        output_reference='OutputA',
        value_name='households'
    )

# --- Step 2 --- #
LOGGER.info('--- Step 2 ---')
LOGGER.info(f'Calculating children, adults, and car availability proportions by dwelling type')
# calculate splits of households with or without children and by car availability
# and by number of adults by dwelling types by MSOA
total_hh_by_hh = ons_table_2.aggregate(segs=['accom_h'])
proportion_hhs_by_h_hc_ha_car = ons_table_2 / total_hh_by_hh

# fill missing proportions with 0 as they are where the total hh is zero in the census data
# TODO is this what we want to do? This drops some dwellings from the addressbase wherever the census total is zero.
proportion_hhs_by_h_hc_ha_car.data = proportion_hhs_by_h_hc_ha_car.data.fillna(0)

# check proportions sum to one
# tmp = proportion_hhs_by_h_hc_ha_car.aggregate(segs=['accom_h'])

LOGGER.info(f'Converting the proportions to LSOA level')
# expand these factors to LSOA level
proportion_hhs_by_h_hc_ha_car_lsoa = proportion_hhs_by_h_hc_ha_car.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT
)

# check proportions sum to one
# tmp = proportion_hhs_by_h_hc_ha_car_lsoa.aggregate(segs=['accom_h'])

LOGGER.info(f'Applying children, adult, and car availability proportions to households')
# apply proportional factors based on hh by adults / children / car availability to the hh by nssec
hh_by_nssec_hc_ha_car = hh_by_nssec * proportion_hhs_by_h_hc_ha_car_lsoa

# check against original addressbase data
# check = hh_by_nssec_hc_ha_car.aggregate(segs=['accom_h'])

# save output to hdf and csvs for checking
LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output B.hdf')
data_processing.summary_reporting(
    dvector=hh_by_nssec_hc_ha_car,
    dimension='households',
    detailed_logs=True
)
hh_by_nssec_hc_ha_car.save(OUTPUT_DIR / 'Output B.hdf')
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=hh_by_nssec_hc_ha_car,
        output_directory=OUTPUT_DIR,
        output_reference='OutputB',
        value_name='households'
    )

# --- Step 3 --- #
LOGGER.info('--- Step 3 ---')
LOGGER.info(f'Calculating average occupancy by dwelling type')
# Create a total dvec of total number of households based on occupied_properties + unoccupied_properties
all_properties = unoccupied_households + occupied_households

# Calculate adjustment factors by zone to get proportion of households occupied by dwelling type by zone
non_empty_proportion = occupied_households / all_properties
non_empty_proportion.data = non_empty_proportion.data.fillna(0)

# average occupancy for all dwellings
occupancy = (ons_table_1 / occupied_households) * non_empty_proportion
# occ_2 = population / all_properties

# infill missing occupancies with average value of other properties in the LSOA
# i.e. based on column
occupancy.data = occupancy.data.fillna(occupancy.data.mean(axis=0), axis=0)

# multiply occupancy by the addressbase dwellings to get total population by zone
addressbase_population = occupancy * addressbase_dwellings

# save output to hdf and csvs for checking
LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output C.hdf')
data_processing.summary_reporting(
    dvector=addressbase_population,
    dimension='population',
    detailed_logs=True
)
addressbase_population.save(OUTPUT_DIR / 'Output C.hdf')
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=addressbase_population,
        output_directory=OUTPUT_DIR,
        output_reference='OutputC',
        value_name='population'
    )

# --- Step 4 --- #
LOGGER.info('--- Step 4 ---')
LOGGER.info(f'Applying average occupancy to households')
# Apply average occupancy by dwelling type to the households by NS-SeC,
# car availability, number of adults and number of children
# TODO Do we want to do this in a "smarter" way? The occupancy of 1 adult households (for example) should not be more than 1
# TODO and households with 2+ children should be more than 3 - is this a place for IPF?
pop_by_nssec_hc_ha_car = hh_by_nssec_hc_ha_car * occupancy

# save output to hdf and csvs for checking
LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output D.hdf')
data_processing.summary_reporting(
    dvector=pop_by_nssec_hc_ha_car,
    dimension='population',
    detailed_logs=True
)
pop_by_nssec_hc_ha_car.save(OUTPUT_DIR / 'Output D.hdf')
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=pop_by_nssec_hc_ha_car,
        output_directory=OUTPUT_DIR,
        output_reference='OutputD',
        value_name='population'
    )

# --- Step 5 --- #
LOGGER.info('--- Step 5 ---')
LOGGER.info(f'Calculating age and gender proportions by dwelling type')
# Calculate splits by dwelling type, age, and gender
gender_age_splits = hh_age_gender_2021 / hh_age_gender_2021.aggregate(segs=['accom_h'])
# fill missing proportions with 0 as they are where the total hh is zero in the census data
gender_age_splits.data = gender_age_splits.data.fillna(0)

LOGGER.info(f'Converting proportions to LSOA')
# convert the factors back to LSOA
gender_age_splits_lsoa = gender_age_splits.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT
)

LOGGER.info(f'Applying age and gender splits by dwelling type')
# apply the splits at LSOA level to main population table
pop_by_nssec_hc_ha_car_gender_age = pop_by_nssec_hc_ha_car * gender_age_splits_lsoa

# save output to hdf and csvs for checking
# TODO Output E hdf is big!
LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output E.hdf')
data_processing.summary_reporting(
    dvector=pop_by_nssec_hc_ha_car_gender_age,
    dimension='population',
    detailed_logs=True
)
pop_by_nssec_hc_ha_car_gender_age.save(OUTPUT_DIR / 'Output E.hdf')
if generate_summary_outputs:
    data_processing.summarise_dvector(
        dvector=pop_by_nssec_hc_ha_car_gender_age,
        output_directory=OUTPUT_DIR,
        output_reference='OutputE',
    )

# --- Step 6 --- #
LOGGER.info('--- Step 6 ---')
LOGGER.info(f'Calculating economic status proportions')
# Calculate splits by dwelling type, econ, and NS-SeC of HRP
# TODO This is *officially* population over 16, somehow need to account for children
econ_splits = ons_table_3_econ / ons_table_3_econ.aggregate(segs=['accom_h', 'ns_sec'])
# fill missing proportions with 0 as they are where the total hh is zero in the census data
econ_splits.data = econ_splits.data.fillna(0)

LOGGER.info(f'Calculating employment status proportions')
# Calculate splits by dwelling type, employment, and NS-SeC of HRP
# TODO This is *officially* population over 16, somehow need to account for children
emp_splits = ons_table_3_emp / ons_table_3_emp.aggregate(segs=['accom_h', 'ns_sec'])
# fill missing proportions with 0 as they are where the total hh is zero in the census data
emp_splits.data = emp_splits.data.fillna(0)

LOGGER.info(f'Calculating SOC category proportions')
# Calculate splits by dwelling type, soc, and NS-SeC of HRP
# TODO This is *officially* population over 16, somehow need to account for children
soc_splits = ons_table_3_soc / ons_table_3_soc.aggregate(segs=['accom_h', 'ns_sec'])
# fill missing proportions with 0 as they are where the total hh is zero in the census data
soc_splits.data = soc_splits.data.fillna(0)

LOGGER.info(f'Converting economic status, employment status, and SOC category '
            f'splits to LSOA level')
# convert the factors back to LSOA
econ_splits_lsoa = econ_splits.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT
)
emp_splits_lsoa = emp_splits.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT
)
soc_splits_lsoa = soc_splits.translate_zoning(
    new_zoning=constants.LSOA_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT
)

LOGGER.info(f'Expanding segmentation to include age')
# expand the segmentation to include age (assuming the same weights for all age categories)
econ_splits_lsoa_age = data_processing.expand_segmentation(
    dvector=econ_splits_lsoa,
    segmentation_to_add=SegmentsSuper.get_segment(SegmentsSuper.AGE)
)
emp_splits_lsoa_age = data_processing.expand_segmentation(
    dvector=emp_splits_lsoa,
    segmentation_to_add=SegmentsSuper.get_segment(SegmentsSuper.AGE)
)
soc_splits_lsoa_age = data_processing.expand_segmentation(
    dvector=soc_splits_lsoa,
    segmentation_to_add=SegmentsSuper.get_segment(SegmentsSuper.AGE)
)

LOGGER.info(f'Setting child-specific employment / economic status / SOC values')
# set children to have economic status proportions to 1 for students
# only (stops under 16s being allocated working statuses)
econ_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=econ_splits_lsoa_age.data,
    segment_combination={'pop_econ': [1, 2, 3], 'age_9': [1]},
    value=0
)
econ_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=econ_splits_lsoa_age.data,
    segment_combination={'pop_econ': [4], 'age_9': [1]},
    value=1
)

# set children to have employment status proportions to 1 for non-working age
# only (stops under 16s being allocated employment statuses)
emp_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=emp_splits_lsoa_age.data,
    segment_combination={'pop_emp': [1, 2, 3, 4], 'age_9': [1]},
    value=0
)
emp_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=emp_splits_lsoa_age.data,
    segment_combination={'pop_emp': [5], 'age_9': [1]},
    value=1
)

# set children to have SOC grouping proportions to 1 for SOC4
# only (stops under 16s being allocated other SOC groupings)
soc_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=soc_splits_lsoa_age.data,
    segment_combination={'soc': [1, 2, 3], 'age_9': [1]},
    value=0
)
soc_splits_lsoa_age.data = data_processing.replace_segment_combination(
    data=soc_splits_lsoa_age.data,
    segment_combination={'soc': [4], 'age_9': [1]},
    value=1
)

# check proportions sum to one
# TODO some zeros in here that maybe shouldnt be? Need to check
# tmp = soc_splits_lsoa_age.aggregate(segs=['accom_h', 'age_9', 'ns_sec'])

# apply the splits at LSOA level to main population table
LOGGER.info(f'Applying economic status splits to population')
pop_by_nssec_hc_ha_car_gender_age_econ = econ_splits_lsoa_age * pop_by_nssec_hc_ha_car_gender_age
# TODO memory error here
LOGGER.info(f'Applying employment status splits to population')
pop_by_nssec_hc_ha_car_gender_age_econ_emp = emp_splits_lsoa_age * pop_by_nssec_hc_ha_car_gender_age_econ
LOGGER.info(f'Applying SOC category splits to population')
pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc = soc_splits_lsoa_age * pop_by_nssec_hc_ha_car_gender_age_econ_emp

# save output to hdf and csvs for checking
# TODO Output F hdf is big!
# LOGGER.info(fr'Writing to {OUTPUT_DIR}\Output F.hdf')
# pop_by_nssec_hc_ha_car_econ_emp_soc.save(OUTPUT_DIR / 'Output F.hdf')
# TODO Memory crashes when converting to long, ideally need to stick in wide format for summaries!
#   File "C:\Code\Land-Use\land_use\data_processing\outputs.py", line 33, in dvector_to_long
#     data = dvec.data.T.melt(ignore_index=False)
#   numpy.core._exceptions._ArrayMemoryError: Unable to allocate 9.57 GiB for an array with shape (1284192000,) and data type int64
# if generate_summary_outputs:
#     data_processing.summarise_dvector(
#         dvector=pop_by_nssec_hc_ha_car_econ_emp_soc,
#         output_directory=OUTPUT_DIR,
#         output_reference='OutputE',
#     )

