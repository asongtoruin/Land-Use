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
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

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

# loop through GORs to save memory issues further down the line
for GOR in constants.GORS:

    # --- Step 0 --- #
    # read in the data from the config file
    LOGGER.info('Importing data from config file')
    occupied_households = data_processing.read_dvector_from_config(
        config=config,
        key='occupied_households',
        geography_subset=GOR
    )
    unoccupied_households = data_processing.read_dvector_from_config(
        config=config,
        key='unoccupied_households',
        geography_subset=GOR
    )
    ons_table_1 = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_1',
        geography_subset=GOR
    )
    addressbase_dwellings = data_processing.read_dvector_from_config(
        config=config,
        key='addressbase_dwellings',
        geography_subset=GOR
    )
    ons_table_2 = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_2',
        geography_subset=GOR
    )
    mype_2022 = data_processing.read_dvector_from_config(
        config=config,
        key='mype_2022',
        geography_subset=GOR
    )
    ons_table_4 = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_4',
        geography_subset=GOR
    )
    hh_age_gender_2021 = data_processing.read_dvector_from_config(
        config=config,
        key='hh_age_gender_2021',
        geography_subset=GOR
    )
    ons_table_3_econ = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_3_econ',
        geography_subset=GOR
    )
    ons_table_3_emp = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_3_emp',
        geography_subset=GOR
    )
    ons_table_3_soc = data_processing.read_dvector_from_config(
        config=config,
        key='ons_table_3_soc',
        geography_subset=GOR
    )
    ce_uplift_factor = data_processing.read_dvector_from_config(
        config=config,
        key='ce_uplift_factor',
        geography_subset=GOR
    )
    ce_pop_by_type = data_processing.read_dvector_from_config(
        config=config,
        key='ce_pop_by_type',
        geography_subset=GOR
    )
    ce_pop_by_age_gender_soc = data_processing.read_dvector_from_config(
        config=config,
        key='ce_pop_by_age_gender_soc',
        geography_subset=GOR
    )
    ce_pop_by_age_gender_econ = data_processing.read_dvector_from_config(
        config=config,
        key='ce_pop_by_age_gender_econ',
        geography_subset=GOR
    )

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

    # infill missing occupancies with average value of other properties in the LSOA
    # i.e. based on column
    average_occupancy.data = average_occupancy.data.fillna(
        average_occupancy.data.mean(axis=0), axis=0
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step1_Occupied Households_{GOR}',
        dvector=occupied_households,
        dvector_dimension='households'
    )
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step1_Unoccupied Households_{GOR}',
        dvector=unoccupied_households,
        dvector_dimension='households'
    )
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step1_Average Occupancy_{GOR}',
        dvector=average_occupancy,
        dvector_dimension='occupancy'
    )

    # --- Step 2 --- #
    LOGGER.info('--- Step 2 ---')
    LOGGER.info(f'Adjusting addressbase buildings to reflect unoccupied dwellings')

    # apply factors of proportion of total households that are occupied by LSOA
    adjusted_addressbase_dwellings = addressbase_dwellings * non_empty_proportion

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step2_Adjusted Addressbase Dwellings_{GOR}',
        dvector=adjusted_addressbase_dwellings,
        dvector_dimension='households'
    )

    # --- Step 3 --- #
    LOGGER.info('--- Step 3 ---')
    LOGGER.info(f'Calculating NS-SeC household proportions by dwelling type')
    # calculate NS-SeC splits of households by
    # dwelling type by LSOA
    total_hh_by_hh = ons_table_4.aggregate(segs=['accom_h'])
    proportion_ns_sec = ons_table_4 / total_hh_by_hh

    # fill missing proportions with 0 as they are where the total hh is zero in the census data
    # TODO is this what we want to do? This drops some dwellings from the addressbase wherever the census total is zero.
    proportion_ns_sec.data = proportion_ns_sec.data.fillna(0)

    LOGGER.info(f'Applying NS-SeC proportions to Adjusted AddressBase dwellings')
    # apply proportional factors based on hh ns_sec to the addressbase dwellings
    hh_by_nssec = adjusted_addressbase_dwellings * proportion_ns_sec

    # check against original addressbase data
    # check = hh_by_nssec.aggregate(segs=['accom_h'])

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step3_Households_{GOR}',
        dvector=hh_by_nssec,
        dvector_dimension='households'
    )

    # --- Step 4 --- #
    LOGGER.info('--- Step 4 ---')
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
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    # check proportions sum to one
    # tmp = proportion_hhs_by_h_hc_ha_car_lsoa.aggregate(segs=['accom_h'])

    LOGGER.info(f'Applying children, adult, and car availability proportions to households')
    # apply proportional factors based on hh by adults / children / car availability to the hh by nssec
    hh_by_nssec_hc_ha_car = hh_by_nssec * proportion_hhs_by_h_hc_ha_car_lsoa

    # check against original addressbase data
    # check = hh_by_nssec_hc_ha_car.aggregate(segs=['accom_h'])

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step4_Households_{GOR}',
        dvector=hh_by_nssec_hc_ha_car,
        dvector_dimension='households'
    )

    # --- Step 5 --- #
    LOGGER.info('--- Step 5 ---')
    LOGGER.info(f'Applying average occupancy to households')
    # Apply average occupancy by dwelling type to the households by NS-SeC,
    # car availability, number of adults and number of children
    # TODO Do we want to do this in a "smarter" way? The occupancy of 1 adult households (for example) should not be more than 1
    # TODO and households with 2+ children should be more than 3 - is this a place for IPF?
    pop_by_nssec_hc_ha_car = hh_by_nssec_hc_ha_car * average_occupancy

    # calculate expected population based in the addressbase "occupied" dwellings
    addressbase_population = adjusted_addressbase_dwellings * average_occupancy

    # TODO: Review this. This step will correct the zone totals to match what's in our uplifted AddressBase. Is this going to give the correct number?
    # Rebalance the zone totals
    data_processing.processing.rebalance_zone_totals(
        input_dvector=pop_by_nssec_hc_ha_car,
        desired_totals=addressbase_population
    )

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step5_Population_{GOR}',
        dvector=pop_by_nssec_hc_ha_car,
        dvector_dimension='population'
    )

    # --- Step 6 --- #
    LOGGER.info('--- Step 6 ---')
    LOGGER.info(f'Calculating age and gender proportions by dwelling type')
    # Calculate splits by dwelling type, age, and gender
    gender_age_splits = hh_age_gender_2021 / hh_age_gender_2021.aggregate(segs=['accom_h'])
    # fill missing proportions with 0 as they are where the total hh is zero in the census data
    gender_age_splits.data = gender_age_splits.data.fillna(0)

    LOGGER.info(f'Converting proportions to LSOA')
    # convert the factors back to LSOA
    gender_age_splits_lsoa = gender_age_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info(f'Applying age and gender splits by dwelling type')
    # apply the splits at LSOA level to main population table
    pop_by_nssec_hc_ha_car_gender_age = pop_by_nssec_hc_ha_car * gender_age_splits_lsoa

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step6_Population_{GOR}',
        dvector=pop_by_nssec_hc_ha_car_gender_age,
        dvector_dimension='population'
    )

    # --- Step 7 --- #
    LOGGER.info('--- Step 7 ---')
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
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )
    emp_splits_lsoa = emp_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )
    soc_splits_lsoa = soc_splits.translate_zoning(
        new_zoning=constants.KNOWN_GEOGRAPHIES.get(f'LSOA2021-{GOR}'),
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.NO_WEIGHT,
        check_totals=False
    )

    LOGGER.info(f'Expanding segmentation to include age')
    # expand the segmentation to include age (assuming the same weights for all age categories)
    econ_splits_lsoa_age = econ_splits_lsoa.add_segment(
        SegmentsSuper.get_segment(SegmentsSuper.AGE)
    )
    emp_splits_lsoa_age = emp_splits_lsoa.add_segment(
        SegmentsSuper.get_segment(SegmentsSuper.AGE)
    )
    soc_splits_lsoa_age = soc_splits_lsoa.add_segment(
        SegmentsSuper.get_segment(SegmentsSuper.AGE)
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
    LOGGER.info(f'Applying employment status splits to population')
    pop_by_nssec_hc_ha_car_gender_age_econ_emp = emp_splits_lsoa_age * pop_by_nssec_hc_ha_car_gender_age_econ
    LOGGER.info(f'Applying SOC category splits to population')
    pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc = soc_splits_lsoa_age * pop_by_nssec_hc_ha_car_gender_age_econ_emp

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step7_Population_{GOR}',
        dvector=pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.processing.clear_dvectors(
        econ_splits, emp_splits, soc_splits,
        econ_splits_lsoa, emp_splits_lsoa, soc_splits_lsoa,
        econ_splits_lsoa_age, emp_splits_lsoa_age, soc_splits_lsoa_age,
        pop_by_nssec_hc_ha_car_gender_age_econ,
        pop_by_nssec_hc_ha_car_gender_age_econ_emp
    )

    # --- Step 8 --- #
    LOGGER.info('--- Step 8 ---')
    LOGGER.info(f'Calculating adjustments for communal establishment residents')

    # calculate proportional increase by LSOA due to communal establishments
    # define a matrix of 1s, ce_uplift_factor - 1 doesnt work
    # TODO change dunder method to allow simple numeric operations?
    ones = ce_uplift_factor.copy()
    for col in ones.data.columns:
        ones.data[col] = 1
    ce_uplift = ce_uplift_factor - ones

    LOGGER.info(f'Calculating splits of CE type by MSOA')
    # calculate msoa-based splits of CE types
    ce_pop_by_type_total = ce_pop_by_type.add_segment(
        constants.CUSTOM_SEGMENTS.get('total')
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
    ce_pop_by_age_gender_econ_total = ce_pop_by_age_gender_econ.add_segment(
        constants.CUSTOM_SEGMENTS.get('total')
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
    ce_pop_by_age_gender_soc_total = ce_pop_by_age_gender_soc.add_segment(
        constants.CUSTOM_SEGMENTS.get('total')
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
        segs=['ce', 'age_9', 'g', 'pop_econ', 'soc']
    )

    # define a matrix of 1s, ce_uplift_by_ce_age_gender_econ_soc + 1 doesnt work
    # TODO change dunder method to allow simple numeric operations?
    ones = ce_uplift_by_ce_age_gender_econ_soc.copy()
    for col in ones.data.columns:
        ones.data[col] = 1

    LOGGER.info('Calculating zonal adjustment factors by CE type, age, gender, economic status, and SOC')
    # calculate adjustment factors by ce type, age, gender, economic status, and SOC
    ce_uplift_factor_by_ce_age_gender_econ_soc = ce_uplift_by_ce_age_gender_econ_soc + ones
    # TODO some level of output is needed here? Confirm with Matteo

    # drop communal establishment type to apply back to main population
    ce_uplift_factor = ce_uplift_by_ce_age_gender_econ_soc.aggregate(
        segs=['age_9', 'g', 'pop_econ', 'soc']
    )
    ones = ce_uplift_factor.copy()
    for col in ones.data.columns:
        ones.data[col] = 1
    ce_uplift_factor = ce_uplift_factor + ones

    LOGGER.info('Uplifting population to account for CEs')
    # calculate population in CEs by ce type, age, gender, econ status, and soc
    adjusted_pop = ce_uplift_factor * pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc

    # save output to hdf and csvs for checking
    data_processing.save_output(
        output_folder=OUTPUT_DIR,
        output_reference=f'Step8_Population_{GOR}',
        dvector=adjusted_pop,
        dvector_dimension='population'
    )

    # clear data at the end of the loop
    data_processing.clear_dvectors(
        pop_by_nssec_hc_ha_car_gender_age_econ_emp_soc,
        ce_uplift_factor,
        ce_uplift, ce_pop_by_type_total, ce_type_splits,
        ce_type_splits_lsoa, ce_pop_by_age_gender_econ_total,
        ce_econ_splits, ce_econ_splits_lsoa,
        ce_pop_by_age_gender_soc_total, ce_soc_splits,
        ce_soc_splits_lsoa, ce_uplift_by_ce, ce_uplift_by_ce_age_gender_econ,
        ce_uplift_by_ce_age_gender_econ_soc, ones,
        ce_uplift_factor_by_ce_age_gender_econ_soc, adjusted_pop
    )
