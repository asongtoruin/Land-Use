from argparse import ArgumentParser
from functools import reduce
from pathlib import Path

import pandas as pd
import yaml
from caf.core import DVector
from caf.core.segments import SegmentsSuper
from caf.core.zoning import TranslationWeighting
import numpy as np

from land_use import constants, data_processing
from land_use import logging as lu_logging


# parser = ArgumentParser('Land-Use command line runner')
# parser.add_argument('config_file', type=Path)
# args = parser.parse_args()
#
# # load configuration file
# with open(args.config_file, 'r') as text_file:
#     config = yaml.load(text_file, yaml.SafeLoader)


config_file = r'scenario_configurations\iteration_5\base_population_config.yml'
# load configuration file
with open(config_file, 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config['output_intermediate_outputs'])

# Set up logger
LOGGER = lu_logging.configure_logger(output_dir=OUTPUT_DIR, log_name='scotland_hh')

# SCOTLAND-SPECIFIC PROCESSING
LOGGER.info('Read in Scotland population data created in step 13 of population')
scot_pop = DVector.load(OUTPUT_DIR / f'Output P13_Scotland.hdf')

# collapse segmentation to household-specific segmentations (i.e. remove population segmentation)
aggregated_pop = scot_pop.aggregate(
    segs=['accom_h', 'ns_sec', 'adults', 'children', 'car_availability']
)

# Clear out the massive DVector for scotland (in case of memory issues)
data_processing.clear_dvectors(scot_pop)

# read in the occupied and unoccupied households from the donor regions
area_type_agg_ons = []
area_type_agg_occ = []
area_type_agg_unocc = []
for gor in config['scotland_donor_regions']:
    LOGGER.debug(f'Re-reading ONS table 1 for {gor}')
    ons_table_1 = data_processing.read_dvector_from_config(
        config=config,
        data_block='base_data',
        key='ons_table_1',
        geography_subset=gor
    )
    area_type_agg_ons.append(
        ons_table_1.translate_zoning(
            constants.TFN_AT_AGG_ZONING_SYSTEM,
            cache_path=constants.CACHE_FOLDER
        )
    )
    LOGGER.debug(f'Re-reading occupied households for {gor}')
    occupied_households = data_processing.read_dvector_from_config(
        config=config,
        data_block='base_data',
        key='occupied_households',
        geography_subset=gor
    )
    area_type_agg_occ.append(
        occupied_households.translate_zoning(
            constants.TFN_AT_AGG_ZONING_SYSTEM,
            cache_path=constants.CACHE_FOLDER
        )
    )
    LOGGER.debug(f'Re-reading unoccupied households for {gor}')
    unoccupied_households = data_processing.read_dvector_from_config(
        config=config,
        data_block='base_data',
        key='unoccupied_households',
        geography_subset=gor
    )
    area_type_agg_unocc.append(
        unoccupied_households.translate_zoning(
            constants.TFN_AT_AGG_ZONING_SYSTEM,
            cache_path=constants.CACHE_FOLDER
        )
    )

LOGGER.debug('Disaggregating area types to Scotland')
# Accumulate England totals at area type
england_ons_totals = reduce(lambda x, y: x+y, area_type_agg_ons)
england_occ_totals = reduce(lambda x, y: x+y, area_type_agg_occ)
england_unocc_totals = reduce(lambda x, y: x+y, area_type_agg_unocc)

# Clear out the individual DVectors for England (in case of memory issues)
data_processing.clear_dvectors(
    *area_type_agg_ons, *area_type_agg_occ, *area_type_agg_unocc
)

# calculate average occupancy, as in base_population.py
england_average_occupancy = (england_ons_totals / england_occ_totals)
# replace infinities with nans for infilling
# this is where the occupied_households value is zero for a dwelling type and zone,
# but the ons_table_1 has non-zero population. E.g. LSOA E01007423 in GOR = 'YH'
# caravans and mobile homes, the occupied households = 0 but ons_table_1 population = 4
england_average_occupancy._data = england_average_occupancy._data.replace(np.inf, np.nan)
# infill missing occupancies with average value of other properties in the zone
# i.e. based on column
england_average_occupancy._data = england_average_occupancy._data.fillna(
    england_average_occupancy._data.mean(axis=0), axis=0
)

# convert occupancy factors to scotland zoning
england_occupancy_scotland_zoning = england_average_occupancy.translate_zoning(
    constants.SCOTLAND_DZONE_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

# divide scottish population by average occupancy to get households
scotland_hydrated = aggregated_pop / england_occupancy_scotland_zoning
data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=f'Output P11.1_Scotland',
    dvector=scotland_hydrated,
    dvector_dimension='households',
    detailed_logs=True
)

# calculate occupied households
occupied_households = scotland_hydrated.aggregate(['accom_h'])

# calculate unoccupied households
empty_proportion = (england_unocc_totals / england_occ_totals)
empty_proportion._data = empty_proportion._data.replace(np.inf, np.nan)
# infill missing occupancies with average value of other properties in the zone
# i.e. based on column
empty_proportion._data = empty_proportion._data.fillna(
    empty_proportion._data.mean(axis=0), axis=0
)
unoccupied_households = occupied_households * empty_proportion.translate_zoning(
    constants.SCOTLAND_DZONE_ZONING_SYSTEM,
    cache_path=constants.CACHE_FOLDER,
    weighting=TranslationWeighting.NO_WEIGHT,
    check_totals=False
)

# save outputs
data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=f'Output P11.2_Scotland',
    dvector=occupied_households,
    dvector_dimension='households',
    detailed_logs=True
)
data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=f'Output P11.3_Scotland',
    dvector=unoccupied_households,
    dvector_dimension='households',
    detailed_logs=True
)

# checks on the output dvector
df = scotland_hydrated.data.copy()
zone_totals = df.sum().to_frame(name='households')
zone_totals.to_csv(OUTPUT_DIR / 'Output P11.1_Scotland.csv')
