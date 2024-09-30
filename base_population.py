from argparse import ArgumentParser
from functools import reduce
from pathlib import Path

import yaml
from caf.core import DVector
from caf.core.segments import SegmentsSuper
from caf.core.zoning import TranslationWeighting
import numpy as np

from land_use import constants, data_processing
from land_use import logging as lu_logging


# TODO: expand on the documentation here
parser = ArgumentParser('Land-Use base population command line runner')
parser.add_argument('config_file', type=Path)
args = parser.parse_args()

# load configuration file
with open(args.config_file, 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config['output_intermediate_outputs'])

# Set up logger
LOGGER = lu_logging.configure_logger(output_dir=OUTPUT_DIR, log_name='population')

# loop through GORs to save memory issues further down the line
for GOR in constants.GORS:

    # --- Step 0 --- #
    # read in the base data from the config file
    block = 'base_data'
    LOGGER.info(f'Importing base data from config file ({block} block)')
    occupied_households = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='occupied_households',
        geography_subset=GOR
    )
    unoccupied_households = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='unoccupied_households',
        geography_subset=GOR
    )
    ons_table_1 = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ons_table_1',
        geography_subset=GOR
    )
    addressbase_dwellings = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='addressbase_dwellings',
        geography_subset=GOR
    )
    ons_table_2 = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ons_table_2',
        geography_subset=GOR
    )
    ons_table_4 = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ons_table_4',
        geography_subset=GOR
    )
    hh_age_gender_2021 = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='hh_age_gender_2021',
        geography_subset=GOR
    )
    ons_table_3 = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ons_table_3',
        geography_subset=GOR
    )
    ce_uplift_factor = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ce_uplift_factor',
        geography_subset=GOR
    )
    ce_pop_by_type = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ce_pop_by_type',
        geography_subset=GOR
    )
    ce_pop_by_age_gender_soc = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ce_pop_by_age_gender_soc',
        geography_subset=GOR
    )
    ce_pop_by_age_gender_econ = data_processing.read_dvector_from_config(
        config=config,
        data_block=block,
        key='ce_pop_by_age_gender_econ',
        geography_subset=GOR
    )

    # read in the household validation data from the config file
    LOGGER.info(f'Importing household validation data from config file ({block} block)')
    household_adjustment = {
        key: data_processing.read_dvector_from_config(
            config=config,
            data_block='household_adjustment_data',
            key=key,
            geography_subset=GOR
        )
        for key in config['household_adjustment_data'].keys()
    }

    # read in the population adjustment data from the config file
    LOGGER.info(f'Importing population adjustment data from config file ({block} block)')
    population_adjustment = {
        key: data_processing.read_dvector_from_config(
            config=config,
            data_block='population_adjustment_data',
            key=key,
            geography_subset=GOR
        )
        for key in config['population_adjustment_data'].keys()
    }

    # --- Step 1 --- #
    LOGGER.info('--- Step 1 ---')
    LOGGER.info(f'Calculating average occupancy by dwelling type')
    # Create a total dvec of total number of households based on occupied_properties + unoccupied_properties
    all_properties = unoccupied_households + occupied_households

    # Calculate adjustment factors by zone to get proportion of households occupied by dwelling type by zone
    non_empty_proportion = occupied_households / all_properties

    # infill missing adjustment factors with average value of other properties
    # in the LSOA. Note this is where the total households in and LSOA of a given
    # type is 0
    # TODO do we want to do anything about 1/0 proportions??
    non_empty_proportion.data = non_empty_proportion.data.fillna(
        non_empty_proportion.data.mean(axis=0), axis=0
    )

    # average occupancy for occupied dwellings
    # TODO this average occupancy is now based on census households, not addressbase, is this what we want? Or do we want to use the adjusted addressbase?
    average_occupancy = (ons_table_1 / occupied_households)

    # replace infinities with nans for infilling
    # this is where the occupied_households value is zero for a dwelling type and LSOA,
    # but the ons_table_1 has non-zero population. E.g. LSOA E01007423 in GOR = 'YH'
    # caravans and mobile homes, the occupied households = 0 but ons_table_1 population = 4
    average_occupancy._data = average_occupancy._data.replace(np.inf, np.nan)

    # infill missing occupancies with average value of other properties in the LSOA
    # i.e. based on column
    average_occupancy._data = average_occupancy._data.fillna(
        average_occupancy._data.mean(axis=0), axis=0
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P1.1_{GOR}',
        dvector=occupied_households,
        dvector_dimension='households'
    )
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P1.2_{GOR}',
        dvector=unoccupied_households,
        dvector_dimension='households'
    )
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P1.3_{GOR}',
        dvector=average_occupancy,
        dvector_dimension='occupancy'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        occupied_households, unoccupied_households
    )

    # --- Step 2 --- #
    LOGGER.info('--- Step 2 ---')
    LOGGER.info(f'Adjusting addressbase buildings to reflect unoccupied dwellings')

    # apply factors of proportion of total households that are occupied by LSOA
    adjusted_addressbase_dwellings = addressbase_dwellings * non_empty_proportion

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P2_{GOR}',
        dvector=adjusted_addressbase_dwellings,
        dvector_dimension='households'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        addressbase_dwellings
    )

    # --- Step 3 --- #
    LOGGER.info('--- Step 3 ---')
    LOGGER.info(f'Applying NS-SeC proportions to Adjusted AddressBase dwellings')
    # apply proportional factors based on hh ns_sec to the addressbase dwellings
    hh_by_nssec = data_processing.apply_proportions(ons_table_4, adjusted_addressbase_dwellings)

    # check against original addressbase data
    # check = hh_by_nssec.aggregate(segs=['accom_h'])

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P3_{GOR}',
        dvector=hh_by_nssec,
        dvector_dimension='households'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        ons_table_4
    )

    # --- Step 4 --- #
    LOGGER.info('--- Step 4 ---')

    LOGGER.info('Converting ONS Table 2 to LSOA level (only to be used in proportions, totals will be wrong)')
    # expand these factors to LSOA level
    ons_table_2_lsoa = ons_table_2.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    # check proportions sum to one
    # tmp = proportion_hhs_by_h_hc_ha_car_lsoa.aggregate(segs=['accom_h'])

    LOGGER.info(f'Applying children, adult, and car availability proportions to households')
    # apply proportional factors based on hh by adults / children / car availability to the hh by nssec
    hh_by_nssec_hc_ha_car = data_processing.apply_proportions(ons_table_2_lsoa, hh_by_nssec)

    hh_by_nssec_hc_ha_car = hh_by_nssec_hc_ha_car.add_segments([SegmentsSuper.get_segment(SegmentsSuper.ADULT_NSSEC)])

    # check against original addressbase data
    # check = hh_by_nssec_hc_ha_car.aggregate(segs=['accom_h'])

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P4.1_{GOR}',
        dvector=hh_by_nssec_hc_ha_car,
        dvector_dimension='households'
    )

    # prepare ons_table_2 for ipf targets (drop accom_h segmentation)
    ons_table_2_target = ons_table_2.aggregate(
        segs=[seg for seg in ons_table_2.data.index.names if seg != 'accom_h']
    )

    # applying IPF
    LOGGER.info('Applying IPF for internal validation household targets')
    internal_rebalanced_hh, summary, differences = data_processing.apply_ipf(
        seed_data=hh_by_nssec_hc_ha_car,
        target_dvectors=[ons_table_2_target],
        cache_folder=constants.CACHE_FOLDER
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P4.2_{GOR}',
        dvector=internal_rebalanced_hh,
        dvector_dimension='households'
    )
    summary.to_csv(
        OUTPUT_DIR / f'Output P4.2_{GOR}_VALIDATION.csv',
        float_format='%.5f', index=False
    )
    data_processing.write_to_excel(
        output_folder=OUTPUT_DIR,
        file=f'Output P4.2_{GOR}_VALIDATION.xlsx',
        dfs=differences
    )

    # applying IPF
    LOGGER.info('Applying IPF for independent household targets')
    rebalanced_hh, summary, differences = data_processing.apply_ipf(
        seed_data=internal_rebalanced_hh,
        target_dvectors=list(list(household_adjustment['validation_data'])),
        cache_folder=constants.CACHE_FOLDER
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P4.3_{GOR}',
        dvector=rebalanced_hh,
        dvector_dimension='households'
    )
    summary.to_csv(
        OUTPUT_DIR / f'Output P4.3_{GOR}_VALIDATION.csv',
        float_format='%.5f', index=False
    )
    data_processing.write_to_excel(
        output_folder=OUTPUT_DIR,
        file=f'Output P4.3_{GOR}_VALIDATION.xlsx',
        dfs=differences
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        ons_table_2_target, ons_table_2, hh_by_nssec_hc_ha_car,
         internal_rebalanced_hh
    )

    # --- Step 5 --- #
    LOGGER.info('--- Step 5 ---')
    LOGGER.info(f'Applying average occupancy to households')
    # Apply average occupancy by dwelling type to the households by NS-SeC,
    # car availability, number of adults and number of children
    # TODO Do we want to do this in a "smarter" way? The occupancy of 1 adult households (for example) should not be more than 1
    # TODO and households with 2+ children should be more than 3 - is this a place for IPF?
    pop_by_nssec_hc_ha_car = rebalanced_hh * average_occupancy

    # calculate expected population based in the addressbase "occupied" dwellings
    addressbase_population = adjusted_addressbase_dwellings * average_occupancy

    # TODO: Review this. This step will correct the zone totals to match what's in our uplifted AddressBase. Is this going to give the correct number?
    # Rebalance the zone totals
    data_processing.rebalance_zone_totals(
        input_dvector=pop_by_nssec_hc_ha_car,
        desired_totals=addressbase_population
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P5_{GOR}',
        dvector=pop_by_nssec_hc_ha_car,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        addressbase_population, adjusted_addressbase_dwellings
    )

    # --- Step 6 --- #
    LOGGER.info('--- Step 6 ---')
    LOGGER.info(f'Converting household age and gender figures to LSOA level '
                f'(only to be used in proportions, totals will be wrong)')
    # convert to LSOA
    hh_age_gender_2021_lsoa = hh_age_gender_2021.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info(f'Applying age and gender splits by dwelling type')
    # apply the splits at LSOA level to main population table
    pop_by_nssec_hc_ha_car_gender_age = data_processing.apply_proportions(
        hh_age_gender_2021_lsoa, pop_by_nssec_hc_ha_car
    )

    # compare each step
    data_processing.compare_dvectors(
        dvec1=pop_by_nssec_hc_ha_car,
        dvec2=pop_by_nssec_hc_ha_car_gender_age
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P6_{GOR}',
        dvector=pop_by_nssec_hc_ha_car_gender_age,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        pop_by_nssec_hc_ha_car, hh_age_gender_2021_lsoa
    )

    # --- Step 7 --- #
    LOGGER.info('--- Step 7 ---')
    LOGGER.info('Converting ONS Table 3 to LSOA level '
                '(only to be used in proportions, totals will be wrong)')
    # convert the factors back to LSOA
    ons_table_3_lsoa = ons_table_3.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    # check proportions sum to one
    # TODO some zeros in here that maybe shouldnt be? Need to check
    # tmp = soc_splits_lsoa_age.aggregate(segs=['accom_h', 'age_9', 'ns_sec'])

    # apply the splits at LSOA level to main population table
    LOGGER.info('Applying economic status, employment status, and SOC category '
                'splits to population')
    pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc = data_processing.apply_proportions(
        ons_table_3_lsoa, pop_by_nssec_hc_ha_car_gender_age
    )

    # compare each step
    data_processing.compare_dvectors(
        dvec1=pop_by_nssec_hc_ha_car_gender_age,
        dvec2=pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P7_{GOR}',
        dvector=pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        ons_table_3, ons_table_3_lsoa, pop_by_nssec_hc_ha_car_gender_age
    )

    # --- Step 8 --- #
    LOGGER.info('--- Step 8 ---')
    LOGGER.info(f'Calculating adjustments for communal establishment residents')

    # calculate proportional increase by LSOA due to communal establishments
    # define a matrix of 1s, ce_uplift_factor - 1 doesnt work
    # TODO change dunder method to allow simple numeric operations?
    ones = ce_uplift_factor.copy()
    ones.data.loc[:] = 1
    ce_uplift = ce_uplift_factor - ones

    LOGGER.info(f'Calculating splits of CE type by MSOA')
    # calculate msoa-based splits of CE types
    ce_pop_by_type_total = ce_pop_by_type.add_segments(
        [constants.CUSTOM_SEGMENTS.get('total')]
    )
    ce_type_splits = ce_pop_by_type_total / ce_pop_by_type_total.aggregate(segs=['total'])
    # fill in nan values with 0 (this is where there are no CEs in a given MSOA)
    ce_type_splits.data = ce_type_splits.data.fillna(0)

    # translate the MSOA based CE-type splits to LSOA
    ce_type_splits_lsoa = ce_type_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info(f'Calculating splits of CE population by age, gender, and economic status')
    # calculate GOR-based splits of person types
    ce_pop_by_age_gender_econ_total = ce_pop_by_age_gender_econ.add_segments(
        [constants.CUSTOM_SEGMENTS.get('total')]
    )
    ce_econ_splits = ce_pop_by_age_gender_econ_total / ce_pop_by_age_gender_econ_total.aggregate(segs=['total'])
    # fill in nan values with 0 (this is where there are no CEs in a given REGION)
    ce_econ_splits.data = ce_econ_splits.data.fillna(0)

    # translate the GOR based splits to LSOA
    ce_econ_splits_lsoa = ce_econ_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info(f'Calculating splits of CE population by age, gender, and SOC')
    # calculate GOR-based splits of person types
    ce_pop_by_age_gender_soc_total = ce_pop_by_age_gender_soc.add_segments(
        [constants.CUSTOM_SEGMENTS.get('total')]
    )
    ce_soc_splits = ce_pop_by_age_gender_soc_total / ce_pop_by_age_gender_soc_total.aggregate(segs=['total'])
    # fill in nan values with 0 (this is where there are no CEs in a given REGION)
    ce_soc_splits.data = ce_soc_splits.data.fillna(0)

    # translate the GOR based splits to LSOA
    ce_soc_splits_lsoa = ce_soc_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info('Generating CE adjustment dataset at LSOA')
    # split the uplift factor to be by age, gender, soc, econ, and ce type
    LOGGER.info('Applying CE type splits to zonal uplift')
    ce_uplift_by_ce = ce_uplift * ce_type_splits_lsoa
    LOGGER.info('Applying economic status splits to zonal uplift')
    ce_uplift_by_ce_age_gender_econ = ce_uplift_by_ce * ce_econ_splits_lsoa
    LOGGER.info('Applying SOC category splits to zonal uplift')
    ce_uplift_by_ce_age_gender_econ_soc = ce_uplift_by_ce_age_gender_econ * ce_soc_splits_lsoa

    # drop the 'total' segmentation
    ce_uplift_by_ce_age_gender_econ_soc = ce_uplift_by_ce_age_gender_econ_soc.aggregate(
        segs=['ce', 'age_9', 'g', 'economic_status', 'soc']
    )

    # define a matrix of 1s, ce_uplift_by_ce_age_gender_econ_soc + 1 doesnt work
    # TODO change dunder method to allow simple numeric operations?
    ones = ce_uplift_by_ce_age_gender_econ_soc.copy()
    ones.data.loc[:] = 1

    LOGGER.info('Calculating zonal adjustment factors by CE type, age, gender, '
                'economic status, and SOC')
    # calculate adjustment factors by ce type, age, gender, economic status, and SOC
    ce_uplift_factor_by_ce_age_gender_econ_soc = ce_uplift_by_ce_age_gender_econ_soc + ones
    # TODO some level of output is needed here? Confirm with Matteo

    # drop communal establishment type to apply back to main population
    ce_uplift_factor = ce_uplift_by_ce_age_gender_econ_soc.aggregate(
        segs=['age_9', 'g', 'economic_status', 'soc']
    )
    ones = ce_uplift_factor.copy()
    ones.data.loc[:] = 1
    ce_uplift_factor = ce_uplift_factor + ones

    LOGGER.info('Uplifting population to account for CEs')
    # calculate population in CEs by ce type, age, gender, econ status, and soc
    # TODO: This only works *this* way round. Why?
    adjusted_pop = pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc * ce_uplift_factor

    LOGGER.debug('Checks on impact of Communal Establishments uplift')
    ce_change = adjusted_pop - pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc
    data_processing.summary_reporting(
        ce_change,
        dimension='Change derived from Communal Establishments',
    )

    data_processing.compare_dvectors(
        dvec1=pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc,
        dvec2=adjusted_pop
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P8_{GOR}',
        dvector=adjusted_pop,
        dvector_dimension='population',
        detailed_logs=True
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        ce_pop_by_age_gender_econ_total, ce_pop_by_age_gender_econ,
         ce_econ_splits, ce_econ_splits_lsoa, ce_soc_splits, ce_soc_splits_lsoa,
         ce_uplift_by_ce_age_gender_econ_soc, ones,
        ce_uplift_factor_by_ce_age_gender_econ_soc,
         pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc,
        ce_uplift_factor, ce_change
    )

    # --- Step 9 --- #
    LOGGER.info('--- Step 9 ---')

    # prepare ipf targets (drop accom_h segmentation)
    hh_age_gender_2021_target = hh_age_gender_2021.aggregate(
        segs=[seg for seg in hh_age_gender_2021.data.index.names if seg != 'accom_h']
    )

    # applying IPF (adjusting totals to match P9 outputs)
    LOGGER.info('Applying IPF for internal validation population targets')
    rebalanced_pop, summary, differences = data_processing.apply_ipf(
        seed_data=adjusted_pop,
        target_dvectors=[hh_age_gender_2021_target],
        cache_folder=constants.CACHE_FOLDER,
        target_dvector=adjusted_pop
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P9_{GOR}',
        dvector=rebalanced_pop,
        dvector_dimension='population',
        detailed_logs=True
    )
    summary.to_csv(
        OUTPUT_DIR / f'Output P9_{GOR}_VALIDATION.csv',
        float_format='%.5f', index=False
    )
    data_processing.write_to_excel(
        output_folder=OUTPUT_DIR,
        file=f'Output P9_{GOR}_VALIDATION.xlsx',
        dfs=differences
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        hh_age_gender_2021_target, adjusted_pop
    )

    # --- Step 10 --- #
    LOGGER.info('--- Step 10 ---')

    # applying IPF (adjusting totals to match P9 outputs)
    LOGGER.info('Applying IPF for independent population targets')
    ipfed_pop, summary, differences = data_processing.apply_ipf(
        seed_data=rebalanced_pop,
        target_dvectors=list(population_adjustment['validation_data']),
        cache_folder=constants.CACHE_FOLDER,
        target_dvector=rebalanced_pop
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P10_{GOR}',
        dvector=ipfed_pop,
        dvector_dimension='population',
        detailed_logs=True
    )
    summary.to_csv(
        OUTPUT_DIR / f'Output P10_{GOR}_VALIDATION.csv',
        float_format='%.5f', index=False
    )
    data_processing.write_to_excel(
        output_folder=OUTPUT_DIR,
        file=f'Output P10_{GOR}_VALIDATION.xlsx',
        dfs=differences
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        rebalanced_pop, *population_adjustment['validation_data']
    )

    # --- Step 11 --- #
    LOGGER.info('--- Step 11 ---')
    LOGGER.info('Rebasing households to 2023')

    # get the 2023 addresses by dwelling type
    # TODO: the way the config is set up means this will be a list of one DVector so for now am just popping the first one out, although maybe we should be more explicit about this
    dwellings_rebase = household_adjustment['rebase_data'][0]

    LOGGER.info(f'Adjusting addressbase buildings to reflect unoccupied dwellings')
    # apply factors of proportion of total households that are occupied by LSOA
    adjusted_hh_rebase = dwellings_rebase * non_empty_proportion

    # get proportions of households by segment and zone from the output of the
    # 2021 IPFed households
    hh_rebase = data_processing.apply_proportions(
        source_dvector=rebalanced_hh,
        apply_to=adjusted_hh_rebase
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P11_{GOR}',
        dvector=hh_rebase,
        dvector_dimension='households'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        dwellings_rebase, non_empty_proportion
    )

    # --- Step 12 --- #
    LOGGER.info('--- Step 12 ---')
    LOGGER.info('Rebasing population to 2023')
    LOGGER.info(f'Applying average occupancy to households')
    # apply average occupancy by dwelling type
    pop_rebase = hh_rebase * average_occupancy

    # calculate expected population based in the addressbase "occupied" dwellings
    addressbase_rebase = adjusted_hh_rebase * average_occupancy

    # TODO: Review this. This step will correct the zone totals to match what's in our uplifted AddressBase. Is this going to give the correct number?
    # Rebalance the zone totals
    data_processing.rebalance_zone_totals(
        input_dvector=pop_rebase,
        desired_totals=addressbase_rebase
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P12.1_{GOR}',
        dvector=pop_rebase,
        dvector_dimension='population'
    )

    LOGGER.info(f'Applying population proportional splits to average occupancy')
    # apply average occupancy by dwelling type
    segmented_pop_rebase = data_processing.apply_proportions(
        source_dvector=ipfed_pop,
        apply_to=pop_rebase
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P12.2_{GOR}',
        dvector=segmented_pop_rebase,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        average_occupancy, pop_rebase, ipfed_pop, addressbase_rebase
    )

    # --- Step 13 --- #
    LOGGER.info('--- Step 13 ---')

    # applying IPF (adjusting totals to match P9 outputs)
    LOGGER.info('Applying IPF for population rebase targets')
    rebased_pop, summary, differences = data_processing.apply_ipf(
        seed_data=segmented_pop_rebase,
        target_dvectors=list(population_adjustment['rebase_data']),
        cache_folder=constants.CACHE_FOLDER,
        target_dvector=list(population_adjustment['rebase_data'])[2]
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Output P13_{GOR}',
        dvector=rebased_pop,
        dvector_dimension='population',
        detailed_logs=True
    )
    summary.to_csv(
        OUTPUT_DIR / f'Output P13_{GOR}_VALIDATION.csv',
        float_format='%.5f', index=False
    )
    data_processing.write_to_excel(
        output_folder=OUTPUT_DIR,
        file=f'Output P13_{GOR}_VALIDATION.xlsx',
        dfs=differences
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        segmented_pop_rebase, *population_adjustment['rebase_data']
    )

    LOGGER.info(f'*****COMPLETED PROCESSING FOR {GOR}*****')


# SCOTLAND-SPECIFIC PROCESSING
LOGGER.info('Applying regional profiles to Scotland population data')
area_type_agg = []
for gor in config['scotland_donor_regions']:
    LOGGER.debug(f'Re-reading P13 for {gor}')
    final_pop = DVector.load(OUTPUT_DIR / f'Output P13_{gor}.hdf')
    area_type_agg.append(
        final_pop.translate_zoning(constants.TFN_AT_AGG_ZONING_SYSTEM, cache_path=constants.CACHE_FOLDER)
    )

LOGGER.debug('Disaggregating area types to Scotland')
# Accumulate England totals at area type, then disaggregate to Scotland zoning
england_totals = reduce(lambda x, y: x+y, area_type_agg)

# Clear out the individual DVectors for England (in case of memory issues)
data_processing.clear_dvectors(*area_type_agg)

england_totals_scotland_zoning = england_totals.translate_zoning(
    constants.SCOTLAND_DZONE_ZONING_SYSTEM, cache_path=constants.CACHE_FOLDER
)

# Read in the Scotland data, and then apply proportions
scotland_population = data_processing.read_dvector_from_config(
    config=config,
    data_block='base_data',
    key='scotland_population'
)

scotland_hydrated = data_processing.apply_proportions(
    source_dvector=england_totals_scotland_zoning, apply_to=scotland_population
)

LOGGER.debug('Removing any superfluous segments from Scotland data')
scotland_hydrated = scotland_hydrated.aggregate(
    england_totals.segmentation
)

data_processing.save_output(
    output_folder=OUTPUT_DIR,
    output_reference=f'Output P13_Scotland',
    dvector=scotland_hydrated,
    dvector_dimension='population',
    detailed_logs=True
)
