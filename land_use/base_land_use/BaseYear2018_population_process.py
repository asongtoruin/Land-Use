# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28

@author: adamtebbs
Version number:

Written using: Python 3.7.1

Module versions used for writing:
    pandas v0.25.3
    numpy v1.17.3

Includes functions that were first defined in main_build.py:
    Created on Tue May 26 09:05:40 2020

    @author: mags15
    Version number:

    Written using: Python 3.7.3

    Module versions used for writing:
        pandas v0.24.2

    Main Build:
        - imports addressbase
        - applies household occupancy
        - applies 2011 census segmentation
        - and ns-sec and soc segmentation
        - distinguishes communal establishments

Updates here relative to main_build.py are:
    - Reads in f and P from 2011 Census year outputs
    - Revised processes for uplifting 2018 population based on 20128 MYPE
    - Revised expansion of NTEM population to full dimensions
"""

import pandas as pd
import numpy as np
import os
from ipfn import ipfn
import datetime
import pyodbc
import geopandas as gpd
from land_use.utils import file_ops as utils
from land_use.utils import compress
import land_use.lu_constants as consts
import logging

# Shapefile locations
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_lsoaRef = _default_zone_ref_folder + 'UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_ladRef = os.path.join(
    _default_zone_ref_folder,
    'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp')
_default_msoaRef = _default_zone_ref_folder + 'UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'

# Other paths
_census_f_value_path = '2011 Census Furness/04 Post processing/Outputs'
_Zone_2021LA_path = 'Lookups/MSOA_1991LA_2011LA_2021LA_LAgroups.csv'
_MYE_folder = 'MYE 2018 ONS/2018_MYE_WP3'

# Set Model Year
ModelYear = '2018'

# Directory and file paths for the MYPE section

# Directory Paths
scottish_data_directory = r'I:\NorMITs Land Use\import\MYE 2018 ONS\2018_MidyearMSOA'
la_to_msoa_directory = r'I:\NorMITs Synthesiser\Zone Translation\Export\lad_to_msoa'
geography_directory = r'I:\NorMITs Land Use\import\2018 Population Processing lookups'
lookups_directory = r'I:\NorMITs Land Use\import\2011 Census Micro lookups'  # Uses the same lookups as 2011
inputs_directory_mye = r'I:\NorMITs Land Use\import\MYE 2018 ONS\2018_pop_process_inputs'
inputs_directory_census = r'I:\NorMITs Land Use\import\Nomis Census 2011 Head & Household'
inputs_directory_aps = r'I:\NorMITs Land Use\import\NOMIS APS'

# File names
nomis_mype_msoa_age_gender_path = r'NOMIS_2021_10_21_MYPE_MSOA_Age_Gender.csv'
uk_2011_and_2021_la_path = r'UK_2011_and_2021_LA_IDs.csv'
scottish_2011_z2la_path = r'2011_Scottish_Zones_to_LA.csv'
lookup_geography_2011_path = r'geography.csv'
qs101_uk_path = r'211022_QS101UK_ResidenstType_MSOA.csv'
scottish_2018_males_path = r'Males_Scotland_2018.csv'
scottish_2018_females_path = r'Females_Scotland_2018.csv'
la_to_msoa_path = r'lad_to_msoa_normalised.csv'  # Path with manual corrections to make proportions equal 1
# la_to_msoa_path_og = r'lad_to_msoa.csv'  # Original path, proportions don't quite add to 1
scottish_la_changes_post_2011_path = r'ca11_ca19.csv'
aps_ftpt_gender_2018_path = r'nomis_2021_08_24_APS_FTPT_Gender_2018_only.csv'
nomis_2018_mye_pop_by_la_path = r'nomis_2021_10_25_MYE_2018_LA_withareacodes_total_gender.csv'
aps_soc_path = r'nomis_2021_08_24_APS_SOC.csv'

# Output directory names
copy_address_database_output_dir = '3.2.1_read_in_core_property_data'
filled_properties_output_dir = '3.2.2_filled_property_adjustment'
apply_household_occupancy_output_dir = '3.2.3_apply_household_occupancy'
land_use_formatting_output_dir = '3.2.4_land_use_formatting'
mye_pop_compiled_output_dir = '3.2.5_uplifting_2018_pop_2018_MYPE'
pop_with_full_dims_output_dir = '3.2.6_expand_NTEM_pop'
pop_with_full_dims_second_output_dir = '3.2.7_verify_population_profile_by_dwelling_type'
subsets_worker_nonworker_output_dir = '3.2.8_subsets_of_workers+nonworkers'
la_level_adjustment_output_dir = '3.2.9_verify_district_level_worker_and_nonworker'
further_adjustments_output_dir = '3.2.10_adjust_zonal_pop_with_full_dimensions'
cer_output_dir = '3.2.11_process_CER_data'


# This function doesn't seem to actually do anything?
def copy_addressbase_files(census_and_by_lu_obj):
    """
    Copy the relevant ABP files from import drive to census_and_by_lu_obj.home_folder for use in later functions.
    census_and_by_lu_obj: base year land use object
    """
    logging.info('Running Step 3.2.1')
    print('Running Step 3.2.1')

    # dest = census_and_by_lu_obj.home_folder
    # files = pd.read_csv(census_and_by_lu_obj.addressbase_path_list)
    print('no longer copying into default iter folder')

    # for file in files.FilePath:
    #    try:
    #        shutil.copy(file, dest)
    #        print("Copied over file into default iter folder: " + file)
    #    except IOError:
    #        print("File not found")

    audit_3_2_1_header = 'Audit for Step 3.2.1\nCreated ' + str(datetime.datetime.now())
    audit_3_2_1_text = 'Step 3.2.1 currently does nothing, so there is nothing to audit'
    audit_3_2_1_content = '\n'.join([audit_3_2_1_header, audit_3_2_1_text])
    audit_3_2_1_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    copy_address_database_output_dir,
                                    'Audits',
                                    'Audit_3.2.1.txt')
    with open(audit_3_2_1_path, 'w') as text_file:
        text_file.write(audit_3_2_1_content)

    census_and_by_lu_obj.state['3.2.1 read in core property data'] = 1
    logging.info('Step 3.2.1 completed')
    print('Step 3.2.1 completed')


def filled_properties(census_and_by_lu_obj):
    """
    This is a rough account for unoccupied properties using KS401UK at LSOA level to infer whether the properties
    have any occupants.
        census_and_by_lu_obj: base year land use object, which includes the following paths:
            zone_translation_path: correspondence between LSOAs and the zoning system (default MSOA)
            KS401path: csv file path for the census KS401 table
    """
    logging.info('Running Step 3.2.2')
    print('Running Step 3.2.2')
    # Define folder name for outputs

    # Read in the census filled property data
    filled_properties_df = pd.read_csv(census_and_by_lu_obj.KS401path)
    filled_properties_df = filled_properties_df.rename(columns={
        'Dwelling Type: All categories: Household spaces; measures: Value': 'Total_Dwells',
        'Dwelling Type: Household spaces with at least one usual resident; measures: Value': 'Filled_Dwells',
        'geography code': 'geography_code'
    })
    filled_properties_df = filled_properties_df[['geography_code', 'Total_Dwells', 'Filled_Dwells']]

    # Read in the zone translation (default LSOA to MSOA)
    zone_translation = pd.read_csv(census_and_by_lu_obj.zone_translation_path)
    zone_translation = zone_translation.rename(columns={'lsoa_zone_id': 'lsoaZoneID',
                                                        'msoa_zone_id': 'msoaZoneID'})
    zone_translation = zone_translation[['lsoaZoneID', 'msoaZoneID']]

    # Merge and apply the zone translation onto the census data
    filled_properties_df = filled_properties_df.rename(columns={'geography_code': 'lsoaZoneID'})
    filled_properties_df = filled_properties_df.merge(zone_translation, on='lsoaZoneID')
    filled_properties_df = filled_properties_df.drop(columns={'lsoaZoneID'})
    filled_properties_df = filled_properties_df.groupby(['msoaZoneID']).sum().reset_index()

    # Calculate the probability that a property is filled
    filled_properties_df['Prob_DwellsFilled'] = (filled_properties_df['Filled_Dwells'] /
                                                 filled_properties_df['Total_Dwells'])
    filled_properties_df = filled_properties_df.drop(columns={'Filled_Dwells', 'Total_Dwells'})

    # The above filled properties probability is based on E+W so need to join back to Scottish MSOAs
    uk_msoa = gpd.read_file(_default_msoaRef)[['msoa11cd']].rename(columns={'msoa11cd': 'msoaZoneID'})
    filled_properties_df = uk_msoa.merge(filled_properties_df, on='msoaZoneID', how='outer')
    filled_properties_df = filled_properties_df.fillna(1)  # default to all Scottish properties being occupied
    # Adam - DONE, we need to think how to organise the structure of outputs files per step
    filled_properties_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                          filled_properties_output_dir,
                                          'ProbabilityDwellfilled.csv')
    filled_properties_df.to_csv(filled_properties_path, index=False)

    audit_3_2_2_header = 'Audit for Step 3.2.2\nCreated ' + str(datetime.datetime.now())
    audit_3_2_2_text = 'Step 3.2.2 currently has no audits listed, so there is nothing to audit'
    audit_3_2_2_content = '\n'.join([audit_3_2_2_header, audit_3_2_2_text])
    audit_3_2_2_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    filled_properties_output_dir,
                                    'Audits',
                                    'Audit_3.2.2.txt')
    with open(audit_3_2_2_path, 'w') as text_file:
        text_file.write(audit_3_2_2_content)

    census_and_by_lu_obj.state['3.2.2 filled property adjustment'] = 1  # record that this process has been run
    logging.info('Step 3.2.2 completed')
    print('Step 3.2.2 completed')
    return filled_properties_df


# Sub-function used by apply_household_occupancy. Not called directly by census_and_by_lu.py
def lsoa_census_data_prep(dat_path, population_tables, property_tables, geography=_default_lsoaRef):
    """
    This function prepares the census data by picking fields out of the census csvs.
    Computes the ratio between the population and number of properties to return the household occupancy.
    dat_path: location of the census data
    census_tables: list of the csv file names to import
    geography: file path for geometry, defaults to LSOA
    """
    logging.info('Running lsoa_census_data_prep function')
    print('Running lsoa_census_data_prep function')

    def _read_census_table(census_tables, table_type):
        logging.info('Running _read_census_table sub-function')
        print('Running _read_census_table sub-function')
        imports = []
        for census_table in census_tables:
            table = pd.read_csv(dat_path + '/' + census_table).iloc[:, [2, 6, 7, 8, 10, 11, 12, 13]]
            table.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
            imports.append(table)

        imports = pd.concat(imports, sort=True)  # combine into a single DataFrame
        imports = pd.wide_to_long(imports,
                                  stubnames='cpt',
                                  i='geography_code',
                                  j='census_property_type').reset_index().rename(columns={"cpt": table_type})
        logging.info('_read_census_table sub-function completed')
        print('_read_census_table sub-function completed')
        return imports

    # Read in the population and property data
    population_imports = _read_census_table(population_tables, "population")
    property_imports = _read_census_table(property_tables, "properties")

    # Read in the geometry
    geography = gpd.read_file(geography)
    geography = geography.iloc[:, 0:3]

    # Merge the population data, property data and geometry into a single DataFrame: household_occupancy
    household_occupancy = population_imports.merge(property_imports,
                                                   how='left',
                                                   on=['geography_code', 'census_property_type'])
    household_occupancy = geography.merge(household_occupancy,
                                          how='left',
                                          left_on='lsoa11cd',
                                          right_on='geography_code')

    # Calculate the household occupancy ratio
    household_occupancy['household_occupancy'] = household_occupancy['population'] / household_occupancy['properties']
    logging.info('lsoa_census_data_prep function completed')
    print('lsoa_census_data_prep function completed')
    return household_occupancy


# Sub-function used by apply_household_occupancy. Not called directly by census_and_by_lu.py
# TODO: improve the docstring here

# Sub-sub-function used by apply_household_occupancy, called by balance_missing_hops.
# Not called directly by census_and_by_lu.py
def zone_up(census_and_by_lu_obj, cpt_data, grouping_col='msoaZoneID'):
    """
    Function to raise up a level of spatial aggregation & aggregate at that level, then bring new factors back down
    # TODO: Might be nice to have this zone up any level of zonal aggregation
    Raise LSOA to MSOA for spatial aggregation
    """
    logging.info('Running zone_up function')
    print('Running zone_up function')
    zone_translation_path = census_and_by_lu_obj.zones_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv'
    zone_translation = pd.read_csv(zone_translation_path)
    zone_translation = zone_translation.rename(columns={'lsoa_zone_id': 'lsoaZoneID',
                                                        'msoa_zone_id': 'msoaZoneID'})
    zone_translation = zone_translation[['lsoaZoneID', grouping_col]]

    # Audit any missing objectids
    datLSOAs = len(cpt_data['objectid'].unique())
    ztLSOAs = len(zone_translation['lsoaZoneID'].unique())

    if datLSOAs == ztLSOAs:
        print('zones match 1:1 - zoning up should be smooth')
    else:
        print('some zones missing for LSOA-MSOA zone translation:', datLSOAs - ztLSOAs)

    cpt_data = cpt_data.rename(columns={'lsoa11cd': 'lsoaZoneID'})
    cpt_data = cpt_data.merge(zone_translation, how='left', on='lsoaZoneID').reset_index()
    cpt_data = aggregate_cpt(cpt_data, grouping_col=grouping_col)

    logging.info('zone_up function completed')
    print('zone_up function completed')

    return cpt_data


# Sub-sub-(sub)-function used by apply_household_occupancy, called by balance_missing_hops (and zone_up).
# Not called directly by census_and_by_lu.py
def aggregate_cpt(cpt_data, grouping_col=None):
    """
    Take some census property type data and return hops totals
    """
    logging.info('Running function aggregate_cpt')
    print('Running function aggregate_cpt')
    if not grouping_col:
        cpt_data = cpt_data.loc[:, ['census_property_type', 'population', 'properties']]
        agg_data = cpt_data.groupby('census_property_type').sum().reset_index()
        agg_data['household_occupancy'] = agg_data['population'] / agg_data['properties']
    else:
        cpt_data = cpt_data.loc[:, ['census_property_type', 'population', 'properties', grouping_col]]
        agg_data = cpt_data.groupby(['census_property_type', grouping_col]).sum().reset_index()
        agg_data['household_occupancy'] = agg_data['population'] / agg_data['properties']
    logging.info('function aggregate_cpt complete')
    print('function aggregate_cpt complete')
    return agg_data


def balance_missing_hops(census_and_by_lu_obj, cpt_data, grouping_col='msoaZoneID'):
    """
    # TODO: Replace global with LAD or Country - likely to be marginal improvements. Currently UK-wide
    This resolves the  msoa/lad household occupancy
    """
    logging.info('Running function balanced_missing_hops')
    print('Running function balanced_missing_hops')
    msoa_agg = zone_up(census_and_by_lu_obj, cpt_data, grouping_col=grouping_col)
    msoa_agg = msoa_agg.loc[:, [grouping_col, 'census_property_type',
                                'household_occupancy']].rename(columns={'household_occupancy': 'msoa_ho'})

    global_agg = zone_up(census_and_by_lu_obj, cpt_data, grouping_col=grouping_col)
    global_agg = aggregate_cpt(global_agg, grouping_col=None)
    global_agg = global_agg.loc[:, ['census_property_type',
                                    'household_occupancy']].rename(columns={'household_occupancy': 'global_ho'})

    cpt_data = msoa_agg.merge(global_agg, how='left', on='census_property_type')

    print('Resolving ambiguous household occupancies')

    # Use the global household occupancy where the MSOA household occupancy is unavailable
    cpt_data['household_occupancy'] = cpt_data['msoa_ho'].fillna(cpt_data['global_ho'])
    cpt_data['ho_type'] = np.where(np.isnan(cpt_data['msoa_ho']), 'global', 'msoa')  # record where global has been used
    cpt_data = cpt_data.drop(['msoa_ho', 'global_ho'], axis=1)

    logging.info('Function balanced_missing_hops completed')
    print('Function balanced_missing_hops completed')

    return cpt_data


def apply_household_occupancy(census_and_by_lu_obj, do_import=False, write_out=True):
    """
    Import household occupancy data and apply to property data.
    TODO: want to be able to run at LSOA level when point correspondence is done.
    TODO: Folders for outputs to separate this process from the household classification
    """
    logging.info('Running Step 3.2.3')
    print('Running Step 3.2.3')

    if do_import:
        balanced_cpt_data = pd.read_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                     apply_household_occupancy_output_dir,
                                                     'UKHouseHoldOccupancy2011.csv'))
    else:
        # TODO: put in constants?
        EWQS401 = 'QS401UK_LSOA.csv'
        SQS401 = 'QS_401UK_DZ_2011.csv'
        EWQS402 = 'QS402UK_LSOA.csv'
        SQS402 = 'QS402UK_DZ_2011.csv'

        census_dat = census_and_by_lu_obj.import_folder + 'Nomis Census 2011 Head & Household'
        cpt_data = lsoa_census_data_prep(census_dat, [EWQS401, SQS401], [EWQS402, SQS402],
                                         geography=_default_lsoaRef)

        # Zone up here to MSOA aggregations
        balanced_cpt_data = balance_missing_hops(census_and_by_lu_obj, cpt_data, grouping_col='msoaZoneID')
        balanced_cpt_data = balanced_cpt_data.fillna(0)

        # Read the filled property adjustment back in and apply it to household occupancy
        probability_filled = pd.read_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                      filled_properties_output_dir,
                                                      'ProbabilityDwellfilled.csv'))
        balanced_cpt_data = balanced_cpt_data.merge(probability_filled, how='outer', on='msoaZoneID')
        balanced_cpt_data['household_occupancy'] = (balanced_cpt_data['household_occupancy'] *
                                                    balanced_cpt_data['Prob_DwellsFilled'])
        balanced_cpt_data = balanced_cpt_data.drop(columns={'Prob_DwellsFilled'})
        balanced_cpt_data.to_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                              apply_household_occupancy_output_dir,
                                              'UKHouseHoldOccupancy2011.csv'), index=False)

    # Visual spot checks - count zones, check cpt
    audit = balanced_cpt_data.groupby(['msoaZoneID']).count().reset_index()
    uk_msoa = gpd.read_file(_default_msoaRef)[['objectid', 'msoa11cd']]
    print('census hops zones =', audit['msoaZoneID'].drop_duplicates().count(), 'should be', len(uk_msoa))
    print('counts of property type by zone', audit['census_property_type'].drop_duplicates())

    # Join MSOA ids to balanced cptdata
    uk_msoa = uk_msoa.rename(columns={'msoa11cd': 'msoaZoneID'})
    balanced_cpt_data = balanced_cpt_data.merge(uk_msoa, how='left', on='msoaZoneID').drop('objectid', axis=1)

    # Join MSOA to lad translation
    lad_translation = pd.read_csv(census_and_by_lu_obj.zones_folder + 'Export/lad_to_msoa/lad_to_msoa.csv')
    lad_translation = lad_translation.rename(columns={'lad_zone_id': 'ladZoneID', 'msoa_zone_id': 'msoaZoneID'})
    lad_translation = lad_translation[['ladZoneID', 'msoaZoneID']]
    balanced_cpt_data = balanced_cpt_data.merge(lad_translation, how='left', on='msoaZoneID')

    # Join LAD code
    uk_lad = gpd.read_file(_default_ladRef)[['objectid', 'lad17cd']]
    balanced_cpt_data = balanced_cpt_data.merge(uk_lad, how='left', left_on='ladZoneID', right_on='objectid')

    # Check the join
    if len(balanced_cpt_data['ladZoneID'].unique()) == len(lad_translation['ladZoneID'].unique()):
        print('All LADs joined properly')
        logging.info('All LADs joined properly')
    else:
        logging.info('Some LAD zones not accounted for')
        print('Some LAD zones not accounted for')

    # Read in HOPS growth data
    balanced_cpt_data = balanced_cpt_data.drop(['ladZoneID', 'objectid'], axis=1)
    hops_path = census_and_by_lu_obj.import_folder + 'HOPs/hops_growth_factors.csv'
    hops_growth = pd.read_csv(hops_path)[['Area code', '11_to_18']]

    # Uplift the figures to 2018
    balanced_cpt_data = balanced_cpt_data.merge(hops_growth,
                                                how='left', left_on='lad17cd',
                                                right_on='Area code').drop('Area code', axis=1).reset_index(drop=True)

    balanced_cpt_data['household_occupancy_18'] = (balanced_cpt_data['household_occupancy'] *
                                                   (1 + balanced_cpt_data['11_to_18']))
    trim_cols = ['msoaZoneID', 'census_property_type', 'household_occupancy_18', 'ho_type']
    balanced_cpt_data = balanced_cpt_data[trim_cols]

    # Read in all res property for the level of aggregation
    print('Reading in AddressBase extract')
    addressbase_extract_path = (consts.ALL_RES_PROPERTY_PATH + '/allResProperty' +
                                census_and_by_lu_obj.model_zoning + 'Classified.csv')
    all_res_property = pd.read_csv(addressbase_extract_path)[['ZoneID', 'census_property_type', 'UPRN']]
    all_res_property = all_res_property.groupby(['ZoneID', 'census_property_type']).count().reset_index()

    if census_and_by_lu_obj.model_zoning == 'MSOA':
        all_res_property = all_res_property.merge(balanced_cpt_data,
                                                  how='inner',
                                                  left_on=['ZoneID', 'census_property_type'],
                                                  right_on=['msoaZoneID', 'census_property_type'])
        all_res_property = all_res_property.drop('msoaZoneID', axis=1)

        # Audit join - ensure all zones accounted for
        if all_res_property['ZoneID'].drop_duplicates().count() != uk_msoa[
            'msoaZoneID'
        ].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')

        # allResPropertyZonal.merge(filled_properties, on = 'ZoneID')
        all_res_property['population'] = all_res_property['UPRN'] * all_res_property[
            'household_occupancy_18']

        # Create folder for exports (audits)
        arp_msoa_audit = all_res_property.groupby('ZoneID')['population'].sum().reset_index()
        hpa_folder = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                  apply_household_occupancy_output_dir,
                                  'Hops Population Audits')
        utils.create_folder(hpa_folder)
        arp_msoa_audit_path = os.path.join(hpa_folder,
                                           census_and_by_lu_obj.model_zoning + '_population_from_2018_hops.csv')
        arp_msoa_audit.to_csv(arp_msoa_audit_path, index=False)

        arp_msoa_audit_total = arp_msoa_audit['population'].sum()
        audit_3_2_3_header = 'Audit for Step 3.2.3\nCreated ' + str(datetime.datetime.now())
        audit_3_2_3_text = 'The total arp population is currently: ' + str(arp_msoa_audit_total)
        audit_3_2_3_text2 = 'A zonal breakdown of the arp population has been created here:'
        audit_3_2_3_content = '\n'.join([audit_3_2_3_header, audit_3_2_3_text, audit_3_2_3_text2, arp_msoa_audit_path])
        audit_3_2_3_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                        apply_household_occupancy_output_dir,
                                        'Audits',
                                        'Audit_3.2.3.txt')
        with open(audit_3_2_3_path, 'w') as text_file:
            text_file.write(audit_3_2_3_content)

        # Adam - DONE, we need to think how to organise the structure of outputs files per step
        apply_household_occupancy_filename = 'classifiedResProperty' + census_and_by_lu_obj.model_zoning + '.csv'
        apply_household_occupancy_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                      apply_household_occupancy_output_dir,
                                                      apply_household_occupancy_filename)
        if write_out:
            all_res_property.to_csv(apply_household_occupancy_path, index=False)

        census_and_by_lu_obj.state['3.2.3 household occupancy adjustment'] = 1  # record that this process has been run
        logging.info('Step 3.2.3 completed')
        print('Step 3.2.3 completed')
        return all_res_property

    else:
        logging.info("No support for this zoning system")
        print("No support for this zoning system")  # only the MSOA zoning system is supported at the moment


def land_use_formatting(census_and_by_lu_obj):
    """
    Combines all flats into one category, i.e. property types = 4,5,6 and 7.
    """
    logging.info('Running Step 3.2.4')
    print('Running Step 3.2.4')

    classified_res_property_import_filename = 'classifiedResProperty' + census_and_by_lu_obj.model_zoning + '.csv'
    classified_res_property_import_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                       apply_household_occupancy_output_dir,
                                                       classified_res_property_import_filename)

    crp_cols = ['ZoneID', 'census_property_type', 'UPRN', 'household_occupancy_18', 'population']
    crp = pd.read_csv(classified_res_property_import_path)[crp_cols]
    crp_for_audit = crp.copy()
    # crp = crp.rename(columns={'census_property_type': 'property_type'})
    # Combine all flat types (4,5,6) and type 7.
    # Combine 4,5,6 and 7 dwelling types to 4.
    crp['census_property_type'] = crp['census_property_type'].map(consts.PROPERTY_TYPE)
    crp['popXocc'] = crp['population'] * crp['household_occupancy_18']
    crp = crp.groupby(['ZoneID', 'census_property_type']).sum().reset_index()
    crp['household_occupancy_18'] = crp['popXocc'] / crp['population']  # compute the weighted average occupancy
    crp = crp.drop('popXocc', axis=1)
    logging.info('Population currently {}'.format(crp.population.sum()))
    processed_crp_for_audit = crp.copy()

    # Adam - DONE, we need to think how to organise the structure of outputs files per step
    land_use_formatting_filename = 'classifiedResProperty_Flatscombined' + census_and_by_lu_obj.model_zoning + '.csv'
    land_use_formatting_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                            land_use_formatting_output_dir,
                                            land_use_formatting_filename)
    crp.to_csv(land_use_formatting_path, index=False)

    crp_for_audit = crp_for_audit.groupby(['ZoneID'])[['UPRN', 'population']].sum()
    crp_for_audit = crp_for_audit.rename(columns={'UPRN': 'Properties_from_3.2.3',
                                                  'population': 'Population_from_3.2.3'})
    processed_crp_for_audit = processed_crp_for_audit.groupby(['ZoneID'])[['UPRN', 'population']].sum()
    processed_crp_for_audit = processed_crp_for_audit.rename(columns={'UPRN': 'Properties_from_3.2.4',
                                                                      'population': 'Population_from_3.2.4'})
    crp_for_audit = pd.merge(crp_for_audit, processed_crp_for_audit, how='left', on='ZoneID')
    crp_for_audit['Check_Properties'] = (crp_for_audit['Properties_from_3.2.4'] - crp_for_audit['Properties_from_3.2.3']
                                         ) / crp_for_audit['Properties_from_3.2.3']
    crp_for_audit['Check_Population'] = (crp_for_audit['Population_from_3.2.4'] - crp_for_audit['Population_from_3.2.3']
                                         ) / crp_for_audit['Population_from_3.2.3']
    crp_params_to_check = ['Properties', 'Population']

    audit_land_use_formatting_filename = 'Audit_' + census_and_by_lu_obj.model_zoning + '_Zonal_Properties+Pop.csv'
    audit_land_use_formatting_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                  land_use_formatting_output_dir,
                                                  audit_land_use_formatting_filename)
    crp_for_audit.to_csv(audit_land_use_formatting_path, index=False)

    audit_3_2_4_header = 'Audit for Step 3.2.4\nCreated ' + str(datetime.datetime.now())
    audit_3_2_4_text_pop = 'The total population at the end of this step is: ' + str(crp.population.sum())
    audit_3_2_4_text_properties = 'The total number of properties at the end of this step is: ' + str(crp.UPRN.sum())
    audit_3_2_4_text_zonal_min_max_means = \
        'The Step 3.2.4 zonal breakdown of properties and population\n has been checked against Step 3.2.3.'
    for p in crp_params_to_check:
        full_param = 'Check_' + p
        audit_3_2_4_text_zonal_min_max_means = audit_3_2_4_text_zonal_min_max_means + '\nFor ' + p + ':'
        audit_3_2_4_text_zonal_min_max_means = ''.join([audit_3_2_4_text_zonal_min_max_means,
                                                        '\n\tMin %age variation is: ',
                                                        str(crp_for_audit[full_param].min() * 100), '%'])
        audit_3_2_4_text_zonal_min_max_means = ''.join([audit_3_2_4_text_zonal_min_max_means,
                                                        '\n\tMax %age variation is: ',
                                                        str(crp_for_audit[full_param].max() * 100), '%'])
        audit_3_2_4_text_zonal_min_max_means = ''.join([audit_3_2_4_text_zonal_min_max_means,
                                                        '\n\tMean %age variation is: ',
                                                        str(crp_for_audit[full_param].mean() * 100), '%'])
    audit_3_2_4_text_zonal = 'A full listing of the zonal breakdown of population and properties has been created here:'
    audit_3_2_4_text_final = 'These should match those found in the output of Step 3.2.3.'
    audit_3_2_4_content = '\n'.join([audit_3_2_4_header, audit_3_2_4_text_properties, audit_3_2_4_text_pop,
                                     audit_3_2_4_text_zonal_min_max_means, audit_3_2_4_text_zonal,
                                     audit_land_use_formatting_path, audit_3_2_4_text_final])
    audit_3_2_4_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    land_use_formatting_output_dir,
                                    'Audits',
                                    'Audit_3.2.4.txt')
    with open(audit_3_2_4_path, 'w') as text_file:
        text_file.write(audit_3_2_4_content)

    census_and_by_lu_obj.state['3.2.4 property type mapping'] = 1
    logging.info('Step 3.2.4 completed')
    print('Step 3.2.4 completed')
    return crp


def MYE_APS_process(census_and_by_lu_obj, function_that_called_me, mye_aps_process_dir):
    logging.info('Running MYE_APS process function')
    logging.info('This has been called by ' + function_that_called_me)
    print('Running MYE_APS process function')
    print('This has been called by ' + function_that_called_me)

    # Read in files
    nomis_mype_msoa_age_gender = pd.read_csv(os.path.join(inputs_directory_mye, nomis_mype_msoa_age_gender_path),
                                             skiprows=6, skip_blank_lines=True)
    uk_2011_and_2021_la = pd.read_csv(os.path.join(geography_directory, uk_2011_and_2021_la_path))
    scottish_2011_z2la = pd.read_csv(os.path.join(geography_directory, scottish_2011_z2la_path))
    lookup_geography_2011 = pd.read_csv(os.path.join(lookups_directory, lookup_geography_2011_path))
    qs101_uk = pd.read_csv(os.path.join(inputs_directory_census, qs101_uk_path), skiprows=7).dropna()
    scottish_2018_males = pd.read_csv(os.path.join(scottish_data_directory, scottish_2018_males_path))
    scottish_2018_females = pd.read_csv(os.path.join(scottish_data_directory, scottish_2018_females_path))
    # la_to_msoa_uk = pd.read_csv(
    #     os.path.join(la_to_msoa_directory, la_to_msoa_path_og))  # Original path, proportions don't quite add to 1
    la_to_msoa_uk = pd.read_csv(
        os.path.join(la_to_msoa_directory, la_to_msoa_path))  # Path with manual corrections to make proportions equal 1
    scottish_la_changes_post_2011 = pd.read_csv(os.path.join(inputs_directory_mye, scottish_la_changes_post_2011_path))
    aps_ftpt_gender_2018 = pd.read_csv(os.path.join(inputs_directory_aps, aps_ftpt_gender_2018_path), skiprows=17,
                                       skip_blank_lines=True)
    nomis_2018_mye_pop_by_la = pd.read_csv(os.path.join(inputs_directory_mye, nomis_2018_mye_pop_by_la_path),
                                           skiprows=9, skip_blank_lines=True)
    aps_soc = pd.read_csv(os.path.join(inputs_directory_aps, aps_soc_path), skiprows=12, skip_blank_lines=True)

    # Processing to fix data types or missing headers
    lookup_geography_2011_ew_only = lookup_geography_2011.dropna().copy()  # Removes Scotland and the nans
    lookup_geography_2011_ew_only['Grouped LA'] = lookup_geography_2011_ew_only['Grouped LA'].astype(int)
    qs101_uk.rename(columns={'Unnamed: 1': 'MSOA'}, inplace=True)
    nomis_2018_mye_pop_by_la.rename(columns={'Unnamed: 1': '2021_LA'}, inplace=True)

    def process_age_based_pop(padp_df):
        padp_df['16-74'] = (padp_df['Aged 16 to 64'].astype(int) +
                            padp_df['Aged 65-69'].astype(int) +
                            padp_df['Aged 70-74'].astype(int))
        padp_df['under_16'] = padp_df['Aged 0 to 15'].astype(int)
        padp_df['75_and_over'] = (padp_df['All Ages'].astype(int) -
                                  (padp_df['16-74'] + padp_df['under_16'].astype(int)))
        padp_df = padp_df[['mnemonic', 'under_16', '16-74', '75_and_over']]
        return padp_df

    # Create the zone to LA lookup for the all_residents df
    all_residents_geography = lookup_geography_2011_ew_only.copy()
    all_residents_geography = all_residents_geography[['NorMITs Zone', '2011 LA', 'MSOA']]
    all_residents_geography.rename(columns={'NorMITs Zone': 'Zone', '2011 LA': '2011_LA'}, inplace=True)

    # Format the scottish zones geography for easy read from qs101
    scottish_2011_z2la_for_qs101 = scottish_2011_z2la.copy()
    scottish_2011_z2la_for_qs101 = scottish_2011_z2la_for_qs101[['MSOA', 'NorMITs Zone', 'LA Code']]
    scottish_2011_z2la_for_qs101.rename(columns={'NorMITs Zone': 'Zone', 'LA Code': '2011_LA'}, inplace=True)

    # Now need to format E&W 2011 geography in the same way...
    englandwales_2011_z2la_for_qs101 = all_residents_geography.copy()
    englandwales_2011_z2la_for_qs101 = englandwales_2011_z2la_for_qs101[['MSOA', 'Zone', '2011_LA']]

    # Append rows from scotland onto the end of the E&W df
    # Also check max zone = max index + 1 (+1 due to 0 indexing)
    uk_2011_z2la_for_qs101 = englandwales_2011_z2la_for_qs101.append(scottish_2011_z2la_for_qs101).reset_index()
    uk_2011_z2la_for_qs101.drop(columns=['index'], inplace=True)
    max_z_uk_2011_z2la_for_qs101 = uk_2011_z2la_for_qs101['Zone'].max()
    max_i_uk_2011_z2la_for_qs101 = uk_2011_z2la_for_qs101.shape
    max_i_uk_2011_z2la_for_qs101 = max_i_uk_2011_z2la_for_qs101[0]
    if max_z_uk_2011_z2la_for_qs101 == max_i_uk_2011_z2la_for_qs101:
        logging.info('All ' + str(max_z_uk_2011_z2la_for_qs101) + ' zones accounted for in the UK')
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('Something is wrong with the UK zonal data')
        logging.info('Expected ' + str(max_z_uk_2011_z2la_for_qs101) + ' zones')
        logging.info('Got ' + str(max_i_uk_2011_z2la_for_qs101) + ' zones')
        print('!!!!! WARNING !!!!!')
        print('Something is wrong with the UK zonal data')
        print('Expected', max_z_uk_2011_z2la_for_qs101, 'zones')
        print('Got', max_i_uk_2011_z2la_for_qs101, 'zones')

    # Process UK 2021 geography into a format for QS101 processing
    qs101_uk_2011_and_2021_la = uk_2011_and_2021_la.copy()
    qs101_uk_2011_and_2021_la = qs101_uk_2011_and_2021_la[['2011 LA Code', '2021 LA Code']]
    qs101_uk_2011_and_2021_la.rename(columns={'2011 LA Code': '2011_LA', '2021 LA Code': '2021_LA'}, inplace=True)

    # Cut scottish_la_changes_post_2011 down to a useful lookup
    recoded_scottish_lads_post_2011 = scottish_la_changes_post_2011.copy()
    recoded_scottish_lads_post_2011 = recoded_scottish_lads_post_2011[['CA', 'CAName', 'CADateEnacted']]
    recoded_scottish_lads_post_2011 = recoded_scottish_lads_post_2011.loc[
        recoded_scottish_lads_post_2011['CADateEnacted'] >= 20111231]  # New codes post 12th Dec 2011
    recoded_scottish_lads_post_2011.reset_index(inplace=True)
    recoded_scottish_lads_post_2011.drop(columns=['index', 'CADateEnacted'], inplace=True)
    recoded_scottish_lads_post_2011.rename(columns={'CA': 'New area code', 'CAName': 'Area1'}, inplace=True)

    # Process Scottish male and female data into a format for all_residents processing
    lad_scottish_2018_males = scottish_2018_males.copy()
    lad_scottish_2018_males = pd.merge(lad_scottish_2018_males,
                                       recoded_scottish_lads_post_2011,
                                       on='Area1',
                                       how='left')
    lad_scottish_2018_males['New area code'] = lad_scottish_2018_males['New area code'].fillna(0)
    lad_scottish_2018_males['Area code'] = np.where(lad_scottish_2018_males['New area code'] == 0,
                                                    lad_scottish_2018_males['Area code'],
                                                    lad_scottish_2018_males['New area code'])
    lad_scottish_2018_males.drop(columns=['New area code'], inplace=True)
    lad_scottish_2018_males['M_Total'] = lad_scottish_2018_males.iloc[:, 3:].sum(axis=1)
    lad_scottish_2018_males = lad_scottish_2018_males[['Area code', 'M_Total', 'under 16', '16-74', '75 or over']]
    lad_scottish_2018_males.rename(columns={'Area code': '2011_LA',
                                            'under 16': 'M_under_16',
                                            '16-74': 'M_16-74',
                                            '75 or over': 'M_75_and_over'}, inplace=True)
    lad_scottish_2018_females = scottish_2018_females.copy()
    lad_scottish_2018_females = pd.merge(lad_scottish_2018_females,
                                         recoded_scottish_lads_post_2011,
                                         on='Area1',
                                         how='left')
    lad_scottish_2018_females['New area code'] = lad_scottish_2018_females['New area code'].fillna(0)
    lad_scottish_2018_females['Area code'] = np.where(lad_scottish_2018_females['New area code'] == 0,
                                                      lad_scottish_2018_females['Area code'],
                                                      lad_scottish_2018_females['New area code'])
    lad_scottish_2018_females.drop(columns=['New area code'], inplace=True)
    lad_scottish_2018_females['F_Total'] = lad_scottish_2018_females.iloc[:, 3:].sum(axis=1)
    lad_scottish_2018_females = lad_scottish_2018_females[['Area code', 'F_Total', 'under 16', '16-74', '75 or over']]
    lad_scottish_2018_females.rename(columns={'Area code': '2011_LA',
                                              'under 16': 'F_under_16',
                                              '16-74': 'F_16-74',
                                              '75 or over': 'F_75_and_over'}, inplace=True)
    lad_scottish_2018_all = pd.merge(lad_scottish_2018_males,
                                     lad_scottish_2018_females,
                                     how='outer',
                                     on='2011_LA')
    lad_scottish_2018_all['M_Total'] = lad_scottish_2018_all['M_Total'] + lad_scottish_2018_all['F_Total']
    lad_scottish_2018_all.rename(columns={'M_Total': 'Total'}, inplace=True)
    lad_scottish_2018_all.drop(columns=['F_Total'], inplace=True)
    # Now check that the total is still valid
    scottish_male_total = scottish_2018_males.iloc[:, 3:].sum(axis=1)
    scottish_female_total = scottish_2018_females.iloc[:, 3:].sum(axis=1)
    scottish_pop_total = scottish_male_total.sum() + scottish_female_total.sum()
    if scottish_pop_total == lad_scottish_2018_all['Total'].sum():
        logging.info('All ' + str(scottish_pop_total) + ' people accounted for in Scotland')
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('Something is wrong with the Scottish population data')
        logging.info('Expected population of ' + scottish_pop_total)
        logging.info('Got a population of ' + lad_scottish_2018_all['Total'].sum())
        print('!!!!! WARNING !!!!!')
        print('Something is wrong with the Scottish population data')
        print('Expected population of', scottish_pop_total)
        print('Got a population of', lad_scottish_2018_all['Total'].sum())

    # Format lookup for LA to MSOA
    la_to_msoa_uk_lookup = la_to_msoa_uk.copy()
    la_to_msoa_uk_lookup = la_to_msoa_uk_lookup[['msoa_zone_id', 'lad_to_msoa']]
    la_to_msoa_uk_lookup.rename(columns={'msoa_zone_id': 'MSOA'}, inplace=True)
    la_to_msoa_uk_lookup['lad_to_msoa'].sum()

    # Strip Northern Ireland from the Nomis 2018 MYPE
    nomis_2018_mye_pop_by_la_uk = nomis_2018_mye_pop_by_la.copy()
    nomis_2018_mye_pop_by_la_uk = nomis_2018_mye_pop_by_la_uk[~nomis_2018_mye_pop_by_la_uk['2021_LA'].str.contains('N')]

    # Process APS data

    # Remove the Isles of Scilly with UK average data (as it's missing in APS data)
    # Also stip the totals row
    aps_ftpt_gender_2018_to_use = aps_ftpt_gender_2018.copy()
    aps_ftpt_gender_2018_to_use.dropna(inplace=True)  # Drops total column
    # Following lines not required unless you wish to interrogate some of the checking dfs in more detail
    # aps_ftpt_gender_2018_to_use_la_list = aps_ftpt_gender_2018_to_use.copy()
    # aps_ftpt_gender_2018_to_use_la_list = aps_ftpt_gender_2018_to_use_la_list[['LAD']]

    # Deal with Scilly
    aps_ftpt_gender_2018_to_use = aps_ftpt_gender_2018_to_use.set_index(['LAD'])
    aps_ftpt_gender_2018_scilly_pulled = aps_ftpt_gender_2018_to_use.loc[['Isles of Scilly']].reset_index()
    aps_ftpt_gender_2018_to_use.drop(['Isles of Scilly'], inplace=True)
    aps_ftpt_gender_2018_to_use.reset_index(inplace=True)

    aps_ftpt_gender_2018_uk_ave_cols = list(aps_ftpt_gender_2018_to_use.columns)
    aps_ftpt_gender_2018_uk_ave_cols = aps_ftpt_gender_2018_uk_ave_cols[2:]
    aps_ftpt_gender_2018_to_use.replace(',', '', regex=True, inplace=True)
    # ! indicate a small (0-2) sample size. Setting values to 0 where this occurs
    aps_ftpt_gender_2018_to_use.replace('!', '0', regex=True, inplace=True)
    # Ditto for *, but sample size is in range 3-9. Setting values to 0 here too...
    aps_ftpt_gender_2018_to_use.replace('*', '0', inplace=True)
    # - indicates missing data. Only appears in confidence intervals columns
    # and the (removed) Isles of Scilly row, so replacing with 0 for simplicity
    # Also, all numbers in this dataset should be +ve, so no risk of removing
    # -ve values!
    aps_ftpt_gender_2018_to_use.replace('-', '0', regex=True, inplace=True)
    aps_ftpt_gender_2018_to_use[aps_ftpt_gender_2018_uk_ave_cols] = aps_ftpt_gender_2018_to_use[
        aps_ftpt_gender_2018_uk_ave_cols].astype(float)
    # Re-instate all the examples of '-' LAD in names
    aps_ftpt_gender_2018_to_use['LAD'].replace('0', '-', regex=True, inplace=True)

    # Process APS workers data
    aps_ftpt_gender_2018_summary = aps_ftpt_gender_2018_to_use.copy()
    aps_ftpt_gender_2018_summary_cols = [
        col for col in aps_ftpt_gender_2018_summary.columns if 'numerator' in col]
    aps_ftpt_gender_2018_summary = aps_ftpt_gender_2018_summary[aps_ftpt_gender_2018_summary_cols]
    aps_ftpt_gender_2018_summary_cols2 = [
        col for col in aps_ftpt_gender_2018_summary.columns if 'male' in col]
    aps_ftpt_gender_2018_summary = aps_ftpt_gender_2018_summary[aps_ftpt_gender_2018_summary_cols2]
    aps_ftpt_gender_2018_summary_cols3 = [s.replace(' - aged 16-64 numerator', '') for s in
                                          aps_ftpt_gender_2018_summary_cols2]
    aps_ftpt_gender_2018_summary_cols3 = [s.replace('s in employment working', '') for s in
                                          aps_ftpt_gender_2018_summary_cols3]
    aps_ftpt_gender_2018_summary_cols3 = [s.replace('% of ', '') for s in aps_ftpt_gender_2018_summary_cols3]
    aps_ftpt_gender_2018_summary_cols3 = [s.replace('full-time', 'fte') for s in aps_ftpt_gender_2018_summary_cols3]
    aps_ftpt_gender_2018_summary_cols3 = [s.replace('part-time', 'pte') for s in aps_ftpt_gender_2018_summary_cols3]
    aps_ftpt_gender_2018_summary.columns = aps_ftpt_gender_2018_summary_cols3
    aps_ftpt_gender_2018_summary['Total Worker 16-64'] = aps_ftpt_gender_2018_summary.sum(axis=1)

    aps_ftpt_gender_2018_rows = aps_ftpt_gender_2018_to_use.copy()
    aps_ftpt_gender_2018_rows = aps_ftpt_gender_2018_rows.iloc[:, :2]
    aps_ftpt_gender_2018_summary = aps_ftpt_gender_2018_rows.join(aps_ftpt_gender_2018_summary)

    aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary.copy()
    aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary_percent[
        aps_ftpt_gender_2018_summary_cols3].divide(
        aps_ftpt_gender_2018_summary_percent['Total Worker 16-64'], axis='index')

    # Actually might want to do this Scilly bit right at the very end once merged with the rest of the data?
    aps_ftpt_gender_2018_scilly = aps_ftpt_gender_2018_summary_percent.mean()
    aps_ftpt_gender_2018_scilly = pd.DataFrame(aps_ftpt_gender_2018_scilly)
    aps_ftpt_gender_2018_scilly = aps_ftpt_gender_2018_scilly.transpose()
    aps_ftpt_gender_2018_scilly['Checksum'] = aps_ftpt_gender_2018_scilly.sum(axis=1)
    aps_ftpt_gender_2018_scilly['Checksum'] = aps_ftpt_gender_2018_scilly['Checksum'] - 1
    scilly_rows = aps_ftpt_gender_2018_scilly_pulled.copy()
    scilly_rows = scilly_rows.iloc[:, :2]
    scilly_rows = scilly_rows.join(aps_ftpt_gender_2018_scilly)

    aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_rows.join(aps_ftpt_gender_2018_summary_percent)
    aps_ftpt_gender_2018_summary_percent['Checksum'] = aps_ftpt_gender_2018_summary_percent.iloc[:, -4:].sum(axis=1)
    aps_ftpt_gender_2018_summary_percent['Checksum'] = aps_ftpt_gender_2018_summary_percent['Checksum'] - 1
    aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary_percent.append(scilly_rows)
    if abs(aps_ftpt_gender_2018_summary_percent['Checksum'].sum()) < 0.000000001:
        logging.info('Sum of gender %ages across categories is close enough to 1 for all rows')
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('Summing across fte/pte and gender caused an error')
        logging.info('All rows did not sum to 1')
        print('!!!!! WARNING !!!!!')
        print('Summing across fte/pte and gender caused an error')
        print('All rows did not sum to 1')

    # Following lines would tidy up aps_ftpt_gender_2018_summary_percent if you ever want to look at it
    # Also remember to un-comment the small section above the gives one of the variables
    # aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary_percent.set_index('LAD')
    # aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary_percent.reindex(
    #     index=aps_ftpt_gender_2018_to_use_la_list['LAD'])
    # aps_ftpt_gender_2018_summary_percent = aps_ftpt_gender_2018_summary_percent.reset_index()

    # Repeat process for SOC data
    aps_soc_to_use = aps_soc.copy()
    aps_soc_to_use = aps_soc_to_use.set_index(['LAD'])
    aps_soc_to_use.drop(['Isles of Scilly', 'Column Total'], inplace=True)
    aps_soc_to_use.reset_index(inplace=True)
    aps_soc_to_use = aps_soc_to_use[
        aps_soc_to_use.columns.drop(list(aps_soc_to_use.filter(regex='Unemployment rate')))]
    aps_soc_to_use = aps_soc_to_use[
        aps_soc_to_use.columns.drop(list(aps_soc_to_use.filter(regex='percent')))]
    aps_soc_to_use = aps_soc_to_use[
        aps_soc_to_use.columns.drop(list(aps_soc_to_use.filter(regex='conf')))]
    aps_soc_to_use.replace(',', '', regex=True, inplace=True)
    # ! indicate a small (0-2) sample size. Setting values to 0 where this occurs
    aps_soc_to_use.replace('!', '0', regex=True, inplace=True)
    # Ditto for *, but sample size is in range 3-9. Setting values to 0 here too...
    aps_soc_to_use.replace('*', '0', inplace=True)
    # ~ indicates an absolute value <500. It occurs only in the 'Sales
    # customer service' column for the Orkney Islands. As the Orkney's
    # are not a haven for the call centre industry, setting this to 0
    # too.
    aps_soc_to_use.replace('~', '0', inplace=True)
    # # indicates data that is deemed 'statistically unreliable' by ONS.
    # It occurs in only 4 locations, mostly in the south in the columns
    # we are interested in here. Whilst a more robust approach might be
    # to estimate a figure to replace it, it is being set to 0 here for
    # simplicity, as any other approach would require bespoke solutions
    # for each instance.
    aps_soc_to_use.replace('#', '0', regex=True, inplace=True)
    aps_soc_to_use_ave_cols = list(aps_soc_to_use.columns)
    aps_soc_to_use_ave_cols = aps_soc_to_use_ave_cols[1:]
    aps_soc_to_use[aps_soc_to_use_ave_cols] = aps_soc_to_use[
        aps_soc_to_use_ave_cols].astype(float)

    aps_soc_to_use = aps_soc_to_use.rename(columns={'% of all people aged 16+ who are male denominator': 'Aged_16+'})
    aps_soc_to_use = aps_soc_to_use[
        aps_soc_to_use.columns.drop(list(aps_soc_to_use.filter(regex='denominator')))]
    aps_soc_to_use = aps_soc_to_use[
        aps_soc_to_use.columns.drop(list(aps_soc_to_use.filter(regex='male numerator')))]
    aps_soc_to_use_cols = list(aps_soc_to_use.columns)
    aps_soc_to_use_cols = [s.replace('% all in employment who are - ', '') for s in aps_soc_to_use_cols]
    aps_soc_to_use_cols_new = []
    aps_soc_to_use_cols_soc = []
    for s in aps_soc_to_use_cols:
        split_s = s.split(':', 1)[0]
        if len(s) > len(split_s):
            string_s = ['SOC', s.split(':', 1)[0]]
            aps_soc_to_use_cols_new.append(''.join(string_s))
            aps_soc_to_use_cols_soc.append(''.join(string_s))
        else:
            aps_soc_to_use_cols_new.append(s)
    aps_soc_to_use.set_axis(aps_soc_to_use_cols_new, axis=1, inplace=True)
    aps_soc_to_use['Total_Workers'] = aps_soc_to_use[aps_soc_to_use_cols_soc].sum(axis=1)

    # Turn SOC data into proportions by 2021 LA
    aps_soc_props = aps_soc_to_use.copy()
    aps_soc_props['higher'] = aps_soc_props['SOC1'] + aps_soc_props['SOC2'] + aps_soc_props['SOC3']
    aps_soc_props['medium'] = aps_soc_props['SOC4'] + aps_soc_props['SOC5'] + aps_soc_props['SOC6'] + aps_soc_props[
        'SOC7']
    aps_soc_props['skilled'] = aps_soc_props['SOC8'] + aps_soc_props['SOC9']
    aps_soc_props = aps_soc_props[
        aps_soc_props.columns.drop(list(aps_soc_props.filter(regex='SOC')))]
    aps_soc_props.drop(columns=['Aged_16+'], inplace=True)
    aps_soc_props = aps_soc_props.append(
        aps_soc_props.sum(numeric_only=True), ignore_index=True)
    aps_soc_props['LAD'].fillna("UK wide total", inplace=True)
    aps_soc_props['higher'] = aps_soc_props['higher'] / aps_soc_props['Total_Workers']
    aps_soc_props['medium'] = aps_soc_props['medium'] / aps_soc_props['Total_Workers']
    aps_soc_props['skilled'] = aps_soc_props['skilled'] / aps_soc_props['Total_Workers']
    aps_soc_props['Checksum'] = aps_soc_props['higher'] + aps_soc_props['medium'] + aps_soc_props['skilled'] - 1
    if abs(max(aps_soc_props['Checksum'])) < 0.000001 and abs(min(aps_soc_props['Checksum'])) < 0.000001:
        logging.info('All SOC proportions summed to 1')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(aps_soc_props['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(aps_soc_props['Checksum'])))
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('SOC proportions did not sum to 1')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(aps_soc_props['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(aps_soc_props['Checksum'])))
        print('!!!!! WARNING !!!!!')
        print('SOC proportions did not sum to 1')
        print('(within reasonable deviation)')
        print('Max deviation value was:', max(aps_soc_props['Checksum']))
        print('Min deviation value was:', min(aps_soc_props['Checksum']))
    aps_soc_props_to_add = aps_soc_props['LAD'] == 'UK wide total'
    aps_soc_props_to_add = aps_soc_props[aps_soc_props_to_add]
    aps_soc_props_to_add = aps_soc_props_to_add.replace('UK wide total', 'Isles of Scilly')
    aps_soc_props = aps_soc_props.append([aps_soc_props_to_add], ignore_index=True)
    aps_soc_props_to_merge = aps_soc_props.copy()
    aps_soc_props_to_merge = aps_soc_props_to_merge.drop(columns=['Total_Workers', 'Checksum'])

    # Turn gender/ftpt employment data into proportions by 2021 LA
    aps_ftpt_gender_2018_props = aps_ftpt_gender_2018_summary.copy()
    aps_ftpt_gender_2018_props = aps_ftpt_gender_2018_props.append(
        aps_ftpt_gender_2018_props.sum(numeric_only=True), ignore_index=True)
    aps_ftpt_gender_2018_props['LAD'].fillna("UK wide total", inplace=True)
    aps_ftpt_gender_2018_props['2021_LA'].fillna("All_UK001", inplace=True)
    aps_ftpt_gender_2018_props['male fte'] = (aps_ftpt_gender_2018_props['male fte'] /
                                              aps_ftpt_gender_2018_props['Total Worker 16-64'])
    aps_ftpt_gender_2018_props['male pte'] = (aps_ftpt_gender_2018_props['male pte'] /
                                              aps_ftpt_gender_2018_props['Total Worker 16-64'])
    aps_ftpt_gender_2018_props['female fte'] = (aps_ftpt_gender_2018_props['female fte'] /
                                                aps_ftpt_gender_2018_props['Total Worker 16-64'])
    aps_ftpt_gender_2018_props['female pte'] = (aps_ftpt_gender_2018_props['female pte'] /
                                                aps_ftpt_gender_2018_props['Total Worker 16-64'])
    aps_ftpt_gender_2018_props['Checksum'] = (aps_ftpt_gender_2018_props['male fte']
                                              + aps_ftpt_gender_2018_props['male pte']
                                              + aps_ftpt_gender_2018_props['female fte']
                                              + aps_ftpt_gender_2018_props['female pte']
                                              - 1)
    if (abs(max(aps_ftpt_gender_2018_props['Checksum'])) < 0.000001 and
            abs(min(aps_ftpt_gender_2018_props['Checksum'])) < 0.000001):
        logging.info('All ft/pt gender proportions summed to 1')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was:' + str(max(aps_ftpt_gender_2018_props['Checksum'])))
        logging.info('Min deviation value was:' + str(min(aps_ftpt_gender_2018_props['Checksum'])))
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('ft/pt gender proportions did not sum to 1')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was:' + str(max(aps_ftpt_gender_2018_props['Checksum'])))
        logging.info('Min deviation value was:' + str(min(aps_ftpt_gender_2018_props['Checksum'])))
        print('!!!!! WARNING !!!!!')
        print('ft/pt gender proportions did not sum to 1')
        print('(within reasonable deviation)')
        print('Max deviation value was:', max(aps_ftpt_gender_2018_props['Checksum']))
        print('Min deviation value was:', min(aps_ftpt_gender_2018_props['Checksum']))
    aps_ftpt_gender_to_add = aps_ftpt_gender_2018_props['LAD'] == 'UK wide total'
    aps_ftpt_gender_to_add = aps_ftpt_gender_2018_props[aps_ftpt_gender_to_add]
    aps_ftpt_gender_to_add = aps_ftpt_gender_to_add.replace('UK wide total', 'Isles of Scilly')
    aps_ftpt_gender_to_add = aps_ftpt_gender_to_add.replace('All_UK001', 'E06000053')
    aps_ftpt_gender_2018_props = aps_ftpt_gender_2018_props.append([aps_ftpt_gender_to_add], ignore_index=True)
    aps_ftpt_gender_2018_props_to_merge = aps_ftpt_gender_2018_props.copy()
    aps_ftpt_gender_2018_props_to_merge.drop(columns=['Total Worker 16-64', 'Checksum'], inplace=True)

    # Merge SOC and ft/pt gender tables into a single APS props table
    aps_props_to_merge = pd.merge(aps_soc_props_to_merge,
                                  aps_ftpt_gender_2018_props_to_merge,
                                  how='outer',
                                  on='LAD')
    aps_props_to_merge.drop(columns=['LAD'], inplace=True)

    # Create HHR from QS101UK data
    qs101_uk_by_z = qs101_uk.copy()
    qs101_uk_by_z = pd.merge(qs101_uk_by_z, uk_2011_z2la_for_qs101, how='outer', on='MSOA')
    qs101_uk_by_z = pd.merge(qs101_uk_by_z, qs101_uk_2011_and_2021_la, how='left', on='2011_LA')
    qs101_uk_by_z['All categories: Residence type'] = qs101_uk_by_z['All categories: Residence type'].str.replace(',',
                                                                                                                  '')
    qs101_uk_by_z['Lives in a household'] = qs101_uk_by_z['Lives in a household'].str.replace(',', '')
    qs101_uk_by_z['%_of_HHR'] = (qs101_uk_by_z['Lives in a household'].astype(int) /
                                 qs101_uk_by_z['All categories: Residence type'].astype(int))

    # Perform initial processing on the NOMIS data.
    # Manipulates the (rather badly) formatted csv to present male and female data in same df.
    # And formats the age ranges to match those used in NorMITs.
    nomis_all_residents = nomis_mype_msoa_age_gender.copy()
    header_rows = nomis_all_residents.loc[
        nomis_all_residents['2011 super output area - middle layer'] == '2011 super output area - middle layer']
    header_rows = header_rows.index.tolist()
    nomis_all_residents_total_genders = nomis_all_residents.iloc[:header_rows[0], :].reset_index()
    nomis_all_residents_male = nomis_all_residents.iloc[header_rows[0] + 1:header_rows[1], :].reset_index()
    nomis_all_residents_female = nomis_all_residents.iloc[header_rows[1] + 1:, :].reset_index()
    nomis_gender_indices = pd.DataFrame([nomis_all_residents_total_genders.index.max(),
                                         nomis_all_residents_male.index.max(),
                                         nomis_all_residents_female.index.max()])
    nomis_gender_indices_min = nomis_gender_indices[0].min() + 1
    nomis_all_residents_total_genders = nomis_all_residents_total_genders.iloc[:nomis_gender_indices_min, :].drop(
        columns=['index'])
    nomis_all_residents_total_check = nomis_all_residents_total_genders[['mnemonic', 'All Ages']]
    nomis_all_residents_male = nomis_all_residents_male.iloc[:nomis_gender_indices_min, :].drop(columns=['index'])
    nomis_all_residents_female = nomis_all_residents_female.iloc[:nomis_gender_indices_min, :].drop(columns=['index'])

    nomis_all_residents_male = process_age_based_pop(nomis_all_residents_male)
    nomis_all_residents_female = process_age_based_pop(nomis_all_residents_female)
    all_residents_headers = list(nomis_all_residents_male.columns)
    all_residents_male_headers = ['M_' + s for s in all_residents_headers]
    all_residents_female_headers = ['F_' + s for s in all_residents_headers]
    nomis_all_residents_male.columns = nomis_all_residents_male.columns[:-3].tolist() + all_residents_male_headers[1:]
    nomis_all_residents_female.columns = nomis_all_residents_female.columns[
                                         :-3].tolist() + all_residents_female_headers[1:]

    all_residents = pd.merge(nomis_all_residents_male, nomis_all_residents_female, how='outer', on='mnemonic')
    all_residents = pd.merge(nomis_all_residents_total_check, all_residents, how='outer', on='mnemonic')

    # Check the male and female data sum to the total from source by MSOA.
    all_residents['All_Ages'] = all_residents.iloc[:, 2:].sum(axis=1)
    all_residents['All_Ages_Check'] = np.where(all_residents['All_Ages'] == all_residents['All Ages'].astype(int), 0, 1)
    if all_residents['All_Ages_Check'].sum() == 0:
        logging.info('Male and female totals summed across age bands')
        logging.info('Result successfully matched the input totals from NOMIS')
    else:
        logging.info('!!!!! WARNING !!!!!!')
        logging.info('Something went wrong when I tried to sum male and female')
        logging.info('population totals across all age bands')
        logging.info('I expected that \'All Ages\' should match\'All_Ages\',')
        logging.info('but it did not in ' + str(all_residents['All_Ages_Check'].sum()) + ' cases')
        logging.info('The erronious lines are:')
        logging.info(all_residents.loc[all_residents['All_Ages_Check'] == 1])
        print('!!!!! WARNING !!!!!!')
        print('Something went wrong when I tried to sum male and female')
        print('population totals across all age bands')
        print('I expected that \'All Ages\' should match\'All_Ages\',')
        print('but it did not in', all_residents['All_Ages_Check'].sum(), 'cases')
        print('The erronious lines are:')
        print(all_residents.loc[all_residents['All_Ages_Check'] == 1])
    all_residents.drop(columns=['All_Ages', 'All_Ages_Check'], inplace=True)
    all_residents.rename(columns={'All Ages': 'Total', 'mnemonic': 'MSOA'}, inplace=True)

    all_residents = pd.merge(all_residents_geography, all_residents, how='outer', on='MSOA')

    # Create estimates of all_residents by zone in Scotland
    # As data is not available by zone in Scotland, the LA level data needs breaking down
    # An LA to MSOA conversion factor table is available
    all_residents_scotland = scottish_2011_z2la_for_qs101.copy()
    all_residents_scotland = all_residents_scotland[['Zone', '2011_LA', 'MSOA']]
    all_residents_scotland = pd.merge(all_residents_scotland,
                                      lad_scottish_2018_all,
                                      how='left',
                                      on='2011_LA')
    all_residents_scotland = pd.merge(all_residents_scotland,
                                      la_to_msoa_uk_lookup,
                                      how='left',
                                      on='MSOA')
    all_res_scot_cols_to_multiply = list(all_residents_scotland.columns)
    all_res_scot_cols_to_multiply = all_res_scot_cols_to_multiply[3:-1]
    all_residents_scotland[all_res_scot_cols_to_multiply] = all_residents_scotland[
        all_res_scot_cols_to_multiply].multiply(all_residents_scotland['lad_to_msoa'], axis='index')
    scot_pop_pcent_error = ((all_residents_scotland['Total'].sum() - scottish_pop_total) / scottish_pop_total) * 100
    logging.info('Summing the LAD to MSOA factors is: ' + str(all_residents_scotland['lad_to_msoa'].sum()))
    logging.info('It should be the sum of the number of Scottish districts (32).')
    if abs(scot_pop_pcent_error) < 0.0001:
        logging.info('After scaling the Scottish population to MSOA level,')
        logging.info('it has an error of ' + str(scot_pop_pcent_error) + '%.')
        logging.info('This is less than 0.0001% and is deemed an acceptable level of variation.')
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('After scaling the Scottish population data to MSOA level')
        logging.info('an error of ' + str(scot_pop_pcent_error) + '% relative to the')
        logging.info('input population was calculated. This is greater than')
        logging.info('0.0001% and is deemed unacceptably large!')
        print('!!!!! WARNING !!!!!')
        print('After scaling the Scottish population data to MSOA level')
        print('an error of', scot_pop_pcent_error, '% relative to the')
        print('input population was calculated. This is greater than')
        print('0.0001% and is deemed unacceptably large!')
    all_residents_scotland.drop(columns=['lad_to_msoa'], inplace=True)

    # Make MSOA (zonal) HHR table
    hhr_by_z = qs101_uk_by_z.copy()
    hhr_by_z = hhr_by_z[['%_of_HHR', 'MSOA']]

    # prepare Engalnd and Wales data for merging with Scotland and use in hhr
    ew_data_for_hhr = all_residents.copy()
    data_for_hhr_to_multiply = list(ew_data_for_hhr.columns)
    data_for_hhr_to_multiply = data_for_hhr_to_multiply[3:]
    ew_data_for_hhr[data_for_hhr_to_multiply] = ew_data_for_hhr[data_for_hhr_to_multiply].astype(float)

    # Need to add Scotland to all_residents before adding it to hhr
    scotland_data_for_hhr = all_residents_scotland.copy()
    uk_data_for_hhr = ew_data_for_hhr.append(scotland_data_for_hhr)

    # Merge the dta for hhr with the initial hhr setup and apply the % of HHR factor
    hhr_by_z = pd.merge(hhr_by_z, uk_data_for_hhr, how='left', on='MSOA')
    hhr_by_z[data_for_hhr_to_multiply] = hhr_by_z[data_for_hhr_to_multiply].multiply(hhr_by_z['%_of_HHR'], axis='index')

    # Getting workers/SOC

    # Need to have LA level over 16s as a proportion of total pop
    # Later on can multiply this by APS proportion of workers who are over 16
    working_age_pop_by_la_uk = nomis_2018_mye_pop_by_la_uk.copy()
    working_age_pop_by_la_uk = working_age_pop_by_la_uk[['2021_LA', 'Aged 16+', 'All Ages']]
    working_age_pop_by_la_uk['Aged 16+'] = working_age_pop_by_la_uk['Aged 16+'].str.replace(',', '').astype(float)
    working_age_pop_by_la_uk['All Ages'] = working_age_pop_by_la_uk['All Ages'].str.replace(',', '').astype(float)
    # Just create this column to hold the place for now
    working_age_pop_by_la_uk['Over_16_prop'] = 0

    aps_lad_lookup = aps_ftpt_gender_2018_summary.copy()
    aps_lad_lookup = aps_lad_lookup[['LAD', '2021_LA']]
    working_age_pop_by_la_uk = pd.merge(aps_lad_lookup,
                                        working_age_pop_by_la_uk,
                                        how='right',
                                        on='2021_LA')
    working_age_pop_by_la_uk['LAD'].fillna("Isles of Scilly", inplace=True)

    aps_soc_to_merge = aps_soc_to_use.copy()
    aps_soc_to_merge = aps_soc_to_merge[['LAD', 'Aged_16+', 'Total_Workers']]
    working_age_pop_by_la_uk = pd.merge(working_age_pop_by_la_uk,
                                        aps_soc_to_merge,
                                        how='left',
                                        on='LAD')

    # Get totals to help solve Scilly
    working_age_pop_by_la_uk = working_age_pop_by_la_uk.append(
        working_age_pop_by_la_uk.sum(numeric_only=True), ignore_index=True)
    working_age_pop_by_la_uk['LAD'].fillna("UK wide total", inplace=True)
    working_age_pop_by_la_uk['2021_LA'].fillna("All_UK001", inplace=True)
    # Now the total column is added, populate this column
    working_age_pop_by_la_uk['Over_16_prop'] = (
            working_age_pop_by_la_uk['Aged 16+'] / working_age_pop_by_la_uk['All Ages'])

    working_age_pop_by_la_uk['APS_working_prop'] = (
            working_age_pop_by_la_uk['Total_Workers'] / working_age_pop_by_la_uk['Aged_16+'])
    working_age_pop_by_la_uk['worker/total_pop'] = (
            working_age_pop_by_la_uk['Over_16_prop'] * working_age_pop_by_la_uk['APS_working_prop'])

    working_age_pop_by_la_uk.set_index('LAD', inplace=True)
    working_age_pop_mask_nans = working_age_pop_by_la_uk.loc['Isles of Scilly', :].isnull()
    working_age_pop_by_la_uk.loc[
        'Isles of Scilly', working_age_pop_mask_nans] = working_age_pop_by_la_uk.loc[
        'UK wide total', working_age_pop_mask_nans]
    working_age_pop_by_la_uk.reset_index(inplace=True)

    pop_props_by_2021_la = working_age_pop_by_la_uk.copy()
    pop_props_by_2021_la = pop_props_by_2021_la[['LAD', '2021_LA', 'worker/total_pop']]
    pop_props_by_2021_la = pd.merge(pop_props_by_2021_la,
                                    aps_props_to_merge,
                                    how='outer',
                                    on='2021_LA')

    # Summarise HHR data by district
    hhr_by_d = hhr_by_z.copy()
    hhr_by_d = hhr_by_d.groupby(['2011_LA']).sum().reset_index()
    hhr_by_d.drop(columns=['%_of_HHR', 'Zone'], inplace=True)

    # Switch to 2021_LA
    la2011_to_la2021 = uk_2011_and_2021_la.copy()
    la2011_to_la2021 = la2011_to_la2021[['2011 LA Code', '2021 LA Code']]
    la2011_to_la2021.columns = la2011_to_la2021.columns.str.replace(' LA Code', '_LA', regex=True)
    hhr_by_d = pd.merge(la2011_to_la2021, hhr_by_d, how='right', on='2011_LA')
    hhr_by_d.drop(columns=['2011_LA'], inplace=True)
    hhr_by_d = hhr_by_d.groupby(['2021_LA']).sum().reset_index()

    # Produce worker table
    hhr_worker_by_d = hhr_by_d.copy()
    hhr_worker_by_d = hhr_worker_by_d[['2021_LA', 'Total']]
    hhr_worker_by_d['Worker'] = 0  # Create column in correct place. We'll fill it later
    hhr_worker_by_d = pd.merge(hhr_worker_by_d, pop_props_by_2021_la, how='left', on='2021_LA')
    hhr_worker_by_d.drop(columns=['LAD'], inplace=True)
    hhr_worker_by_d['Worker'] = hhr_worker_by_d['Total'] * hhr_worker_by_d['worker/total_pop']
    hhr_worker_by_d.drop(columns=['Total', 'worker/total_pop'], inplace=True)
    hhr_worker_type = hhr_worker_by_d.columns
    hhr_worker_type = hhr_worker_type[2:]
    hhr_worker_by_d[hhr_worker_type] = hhr_worker_by_d[hhr_worker_type].multiply(hhr_worker_by_d['Worker'],
                                                                                 axis='index')
    hhr_worker_by_d['Checksum'] = ((hhr_worker_by_d['Worker'] * 2) -
                                   hhr_worker_by_d[hhr_worker_type].sum(axis=1))
    if (abs(max(hhr_worker_by_d['Checksum'])) < 0.000001 and
            abs(min(hhr_worker_by_d['Checksum'])) < 0.000001):
        logging.info('Worker proportions summed to total')
        logging.info('across both ft/pt by gender and SOC')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(hhr_worker_by_d['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(hhr_worker_by_d['Checksum'])))
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('Worker proportions did not sum to total')
        logging.info('across both ft/pt by gender and SOC')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(hhr_worker_by_d['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(hhr_worker_by_d['Checksum'])))
        print('!!!!! WARNING !!!!!')
        print('Worker proportions did not sum to total')
        print('across both ft/pt by gender and SOC')
        print('(within reasonable deviation)')
        print('Max deviation value was:', max(hhr_worker_by_d['Checksum']))
        print('Min deviation value was:', min(hhr_worker_by_d['Checksum']))
    hhr_worker_by_d_for_export = hhr_worker_by_d.copy()
    hhr_worker_by_d_for_export = hhr_worker_by_d_for_export[['2021_LA',
                                                             'Worker',
                                                             'male fte',
                                                             'male pte',
                                                             'female fte',
                                                             'female pte',
                                                             'higher',
                                                             'medium',
                                                             'skilled']]
    hhr_worker_by_d_for_export.columns = hhr_worker_by_d_for_export.columns.str.replace('male', 'Male', regex=True)
    hhr_worker_by_d_for_export.columns = hhr_worker_by_d_for_export.columns.str.replace('feMale', 'Female', regex=True)
    hhr_worker_by_d_for_export.columns = hhr_worker_by_d_for_export.columns.str.replace('_LA', '_LA_code', regex=True)

    hhr_worker_by_d_row_info = nomis_2018_mye_pop_by_la_uk.copy()
    hhr_worker_by_d_row_info.rename(
        columns={'local authority: district / unitary (as of April 2021)': '2021_LA_name',
                 '2021_LA': '2021_LA_code'},
        inplace=True)
    hhr_worker_by_d_row_info = hhr_worker_by_d_row_info[['2021_LA_name', '2021_LA_code']]
    hhr_worker_by_d_row_info['LA'] = hhr_worker_by_d_row_info.index + 1

    hhr_worker_by_d_for_export = pd.merge(hhr_worker_by_d_row_info,
                                          hhr_worker_by_d_for_export,
                                          how='left',
                                          on='2021_LA_code')

    # Produce Non-worker table
    hhr_nonworker_by_d = hhr_by_d.copy()
    hhr_nonworker_by_d = pd.merge(hhr_nonworker_by_d, hhr_worker_by_d, how='left', on='2021_LA')
    hhr_nonworker_by_d_cols_to_rem = hhr_nonworker_by_d.columns
    hhr_nonworker_by_d_cols_to_rem = hhr_nonworker_by_d_cols_to_rem[1:]
    hhr_nonworker_by_d['Non worker'] = hhr_nonworker_by_d['Total'] - hhr_nonworker_by_d['Worker']
    hhr_nonworker_by_d['Children'] = hhr_nonworker_by_d['M_under_16'] + hhr_nonworker_by_d['F_under_16']
    hhr_nonworker_by_d['M_75 and over'] = hhr_nonworker_by_d['M_75_and_over']
    hhr_nonworker_by_d['F_75 and over'] = hhr_nonworker_by_d['F_75_and_over']
    hhr_nonworker_by_d['M_16-74_out'] = (hhr_nonworker_by_d['M_16-74'] -
                                         (hhr_nonworker_by_d['male fte'] +
                                          hhr_nonworker_by_d['male pte']))
    hhr_nonworker_by_d['F_16-74_out'] = (hhr_nonworker_by_d['F_16-74'] -
                                         (hhr_nonworker_by_d['female fte'] +
                                          hhr_nonworker_by_d['female pte']))
    pe_dag = hhr_nonworker_by_d.copy()  # Copy the nonworker df here - it has all pop cols needed in Pe_dag audit later
    hhr_nonworker_by_d = hhr_nonworker_by_d.drop(columns=hhr_nonworker_by_d_cols_to_rem)
    hhr_nonworker_by_d.rename(columns={'2021_LA': '2021_LA_code'}, inplace=True)
    hhr_nonworker_by_d.columns = hhr_nonworker_by_d.columns.str.rstrip('_out')
    hhr_nonworker_types_by_d = hhr_nonworker_by_d.columns
    hhr_nonworker_types_by_d = hhr_nonworker_types_by_d[2:]
    hhr_nonworker_by_d['Checksum'] = (hhr_nonworker_by_d['Non worker'] -
                                      hhr_nonworker_by_d[hhr_nonworker_types_by_d].sum(axis=1))
    if (abs(max(hhr_nonworker_by_d['Checksum'])) < 0.000001 and
            abs(min(hhr_nonworker_by_d['Checksum'])) < 0.000001):
        logging.info('Non-worker proportions summed to total across both all non-worker types')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(hhr_nonworker_by_d['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(hhr_nonworker_by_d['Checksum'])))
    else:
        logging.info('!!!!! WARNING !!!!!')
        logging.info('Non-worker proportions did not sum to total')
        logging.info('across both all non-worker types')
        logging.info('(within reasonable deviation)')
        logging.info('Max deviation value was: ' + str(max(hhr_nonworker_by_d['Checksum'])))
        logging.info('Min deviation value was: ' + str(min(hhr_nonworker_by_d['Checksum'])))
        print('!!!!! WARNING !!!!!')
        print('Non-worker proportions did not sum to total')
        print('across both all non-worker types')
        print('(within reasonable deviation)')
        print('Max deviation value was:', max(hhr_nonworker_by_d['Checksum']))
        print('Min deviation value was:', min(hhr_nonworker_by_d['Checksum']))
    hhr_nonworker_by_d.drop(columns=['Checksum'], inplace=True)

    hhr_nonworker_by_d_for_export = pd.merge(hhr_worker_by_d_row_info,
                                             hhr_nonworker_by_d,
                                             how='left',
                                             on='2021_LA_code')

    # Produce a Pe_(d, a, g) df for use in auditing Step 3.2.9 later
    pe_dag = pe_dag.rename(columns={'2021_LA': '2021_LA_code'})
    pe_dag = pd.melt(pe_dag, id_vars=['2021_LA_code'], value_vars=[
        'Children', 'M_16-74', 'F_16-74', 'M_75 and over', 'F_75 and over']).rename(
        columns={'variable': 'ag', 'value': 'HHR_pop'})
    a = {
        'Children': 1,
        'M_16-74': 2,
        'F_16-74': 2,
        'M_75 and over': 3,
        'F_75 and over': 3
    }

    g = {
        'Children': 1,
        'M_16-74': 2,
        'F_16-74': 3,
        'M_75 and over': 2,
        'F_75 and over': 3
    }
    pe_dag['a'] = pe_dag['ag'].map(a)
    pe_dag['g'] = pe_dag['ag'].map(g)
    pe_dag = pe_dag[['2021_LA_code', 'a', 'g', 'HHR_pop']]
    pe_dag.to_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                               mye_pop_compiled_output_dir,
                               ModelYear + '_HHR_pop_by_dag.csv'))

    # Printing outputs
    # Adam - DONE, we need to think how to organise the structure of outputs files per step
    full_mye_aps_process_dir = os.path.join(census_and_by_lu_obj.out_paths['write_folder'], mye_aps_process_dir)
    hhr_vs_all_pop_name = ModelYear + '_total_pop_vs_HHR_total.csv'
    # all_residents_scotland_name = r'Output\2018_all_residents_scotland.csv' # Not used, but kept for QA purposes
    hhr_worker_by_d_for_export_name = r'MYE_APS_LA_Worker_scriptoutput.csv'
    hhr_nonworker_by_d_for_export_name = r'MYE_APS_LA_NonWorker_scriptoutput.csv'
    la_info_for_2021_name = r'2021LAID_LAcode_LAname.csv'

    # Export only the requested outputs
    mye_aps_logging_string = 'The MYE_APS_process completed after being called by'
    if function_that_called_me == 'MYE_pop_compiled':
        hhr_vs_all_pop = hhr_by_z.copy()
        hhr_vs_all_pop = hhr_vs_all_pop[['Zone', 'MSOA', 'Total']]
        hhr_vs_all_pop.rename(columns={'Total': 'Total_HHR'}, inplace=True)
        hhr_vs_all_pop = pd.merge(hhr_vs_all_pop, uk_data_for_hhr[['MSOA', 'Total']], how='left', on='MSOA')
        hhr_vs_all_pop.rename(columns={'Total': 'Total_Pop'}, inplace=True)
        # Now write out MYE_MSOA_pop as step 3.2.10 (and 3.2.11?) need it.
        # As it is only 8480 lines long, it should be quick to write/read
        # It saves both of these steps from having to call step 3.2.5 to recalculate it.
        # Unlike this step (step 3.2.5), steps 3.2.10/3.2.11 will be called
        # after this function (and 3.2.5) have run, so will not need to skip ahead
        # to call it, as was the case here.
        hhr_vs_all_pop.to_csv(os.path.join(full_mye_aps_process_dir, hhr_vs_all_pop_name), index=False)
        mye_aps_process_output = hhr_vs_all_pop
    elif function_that_called_me == 'LA_level_adjustment':
        # Dump the APS worker and non-worker files
        hhr_worker_by_d_for_export.to_csv(
            os.path.join(full_mye_aps_process_dir, hhr_worker_by_d_for_export_name), index=False)
        hhr_nonworker_by_d_for_export.to_csv(
            os.path.join(full_mye_aps_process_dir, hhr_nonworker_by_d_for_export_name), index=False)
        la_info_for_2021 = hhr_nonworker_by_d_for_export.copy()
        la_info_for_2021 = la_info_for_2021[['2021_LA_name', '2021_LA_code', 'LA']]
        la_info_for_2021.to_csv(os.path.join(full_mye_aps_process_dir, la_info_for_2021_name), index=False)
        # Create a list of outputs that can be picked up by the calling function
        mye_aps_process_output = [hhr_worker_by_d_for_export,
                                  hhr_nonworker_by_d_for_export,
                                  la_info_for_2021,
                                  pe_dag]
    else:
        logging.info('WARNING - The function that called the MYE_APS_process it not recognised!')
        logging.info('Why was the MYE_APS_process called by an unrecognised function?')
        # Dump all residents table for Scotland - Not used but kept for QA purposes
        # all_residents_scotland.to_csv(
        #     os.path.join(full_mye_aps_process_dir, all_residents_scotland_name), index = False)
        mye_aps_process_output = ['No data to export', 'See warning for MYE_APS_Function']
    mye_aps_logging_string = ' '.join([mye_aps_logging_string, function_that_called_me])
    logging.info(mye_aps_logging_string)
    logging.info('Note that only the outputs requested by the function that called it have been generated')
    print('The MYE_APS_process function completed')
    print('Note that only the outputs requested by the function that called it have been generated')

    return mye_aps_process_output


def NTEM_Pop_Interpolation(census_and_by_lu_obj, calling_functions_output_dir):
    """
    Process population data from NTEM CTripEnd database:
    Interpolate population to the target year, in this case, it is for base year 2018 as databases
    are available in 5 year interval;
    Translate NTEM zones in Scotland into NorNITs zones; for England and Wales, NTEM zones = NorMITs zones (MSOAs)
    """

    # The year of data is set to define the upper and lower NTEM run years and interpolate as necessary between them.
    # The base year for NTEM is 2011 and it is run in 5-year increments from 2011 to 2051.
    # The year selected below must be between 2011 and 2051 (inclusive).
    Year = 2018

    logging.info('Running NTEM_Pop_Interpolation function for Year ' + str(Year))
    print('Running NTEM_Pop_Interpolation function for Year ' + str(Year))

    if Year < 2011 | Year > 2051:
        raise ValueError("Please select a valid year of data.")
    else:
        pass

    Output_Folder = census_and_by_lu_obj.out_paths['write_folder']
    logging.info('NTEM_Pop_Interpolation output being written in:')
    logging.info(Output_Folder)
    LogFile = os.path.join(Output_Folder, calling_functions_output_dir, 'NTEM_Pop_Interpolation_LogFile.txt')
    # 'I:/NorMITs Synthesiser/Zone Translation/'
    Zone_path = census_and_by_lu_obj.zones_folder + 'Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv'
    Pop_Segmentation_path = census_and_by_lu_obj.import_folder + 'CTripEnd/Pop_Segmentations.csv'
    with open(LogFile, 'w') as o:
        o.write("Notebook run on - " + str(datetime.datetime.now()) + "\n")
        o.write("\n")
        o.write("Data Year - " + str(Year) + "\n")
        o.write("\n")
        o.write("Correspondence Lists:\n")
        o.write(Zone_path + "\n")
        o.write(Pop_Segmentation_path + "\n")
        o.write("\n")

    # Data years
    # NTEM is run in 5-year increments with a base of 2011.
    # This section calculates the upper and lower years of data that are required
    InterpolationYears = Year % 5
    LowerYear = Year - ((InterpolationYears - 1) % 5)
    UpperYear = Year + ((1 - InterpolationYears) % 5)

    logging.info("Lower Interpolation Year - " + str(LowerYear))
    logging.info("Upper Interpolation Year - " + str(UpperYear))
    print("Lower Interpolation Year - " + str(LowerYear))
    print("Upper Interpolation Year - " + str(UpperYear))

    # Import Upper and Lower Year Tables
    # 'I:/Data/NTEM/NTEM 7.2 outputs for TfN/'
    LowerNTEMDatabase = census_and_by_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(LowerYear) + '.accdb'
    UpperNTEMDatabase = census_and_by_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(UpperYear) + '.accdb'
    # UpperNTEMDatabase = by_lu_obj.CTripEnd_Database_path + r"\CTripEnd7_" + str(UpperYear) + r".accdb"
    cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' +
                          '{};'.format(UpperNTEMDatabase))

    query = r"SELECT * FROM ZoneData"
    UZoneData = pd.read_sql(query, cnxn)
    cnxn.close()

    cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' +
                          '{};'.format(LowerNTEMDatabase))

    query = r"SELECT * FROM ZoneData"
    LZoneData = pd.read_sql(query, cnxn)
    cnxn.close()

    # Re-format Tables
    LZonePop = LZoneData.copy()
    UZonePop = UZoneData.copy()
    LZonePop.drop(
        ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07', 'E08', 'E09', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'K01',
         'K02', 'K03', 'K04', 'K05', 'K06', 'K07', 'K08', 'K09', 'K10', 'K11', 'K12', 'K13', 'K14', 'K15'], axis=1,
        inplace=True)
    UZonePop.drop(
        ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07', 'E08', 'E09', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'K01',
         'K02', 'K03', 'K04', 'K05', 'K06', 'K07', 'K08', 'K09', 'K10', 'K11', 'K12', 'K13', 'K14', 'K15'], axis=1,
        inplace=True)
    LZonePop_long = pd.melt(LZonePop, id_vars=["I", "R", "B", "Borough", "ZoneID", "ZoneName"],
                            var_name="LTravellerType", value_name="LPopulation")
    UZonePop_long = pd.melt(UZonePop, id_vars=["I", "R", "B", "Borough", "ZoneID", "ZoneName"],
                            var_name="UTravellerType", value_name="UPopulation")

    LZonePop_long.rename(columns={"I": "LZoneID", "B": "LBorough", "R": "LAreaType"}, inplace=True)
    UZonePop_long.rename(columns={"I": "UZoneID", "B": "UBorough", "R": "UAreaType"}, inplace=True)

    LZonePop_long['LIndivID'] = LZonePop_long.LZoneID.map(str) + "_" + LZonePop_long.LAreaType.map(
        str) + "_" + LZonePop_long.LBorough.map(str) + "_" + LZonePop_long.LTravellerType.map(str)
    UZonePop_long['UIndivID'] = UZonePop_long.UZoneID.map(str) + "_" + UZonePop_long.UAreaType.map(
        str) + "_" + UZonePop_long.UBorough.map(str) + "_" + UZonePop_long.UTravellerType.map(str)

    # Join Upper and Lower Tables
    TZonePop_DataYear = LZonePop_long.join(UZonePop_long.set_index('UIndivID'), on='LIndivID', how='right',
                                           lsuffix='_left', rsuffix='_right')
    TZonePop_DataYear.drop(['UZoneID', 'UBorough', 'UAreaType', 'UTravellerType'], axis=1, inplace=True)

    # Interpolate Between Upper and Lower Years
    TZonePop_DataYear['GrowthinPeriod'] = TZonePop_DataYear.eval('UPopulation - LPopulation')
    TZonePop_DataYear['GrowthperYear'] = TZonePop_DataYear.eval('GrowthinPeriod / 5')
    TZonePop_DataYear = TZonePop_DataYear.assign(GrowthtoYear=TZonePop_DataYear['GrowthperYear'] * (Year - LowerYear))
    TZonePop_DataYear['Population'] = TZonePop_DataYear.eval('LPopulation + GrowthtoYear')

    # Tidy up
    TZonePop_DataYear.rename(
        columns={"LZoneID": "ZoneID", "LBorough": "Borough", "LAreaType": "AreaType", "LTravellerType": "TravellerType",
                 "LIndivID": "IndivID"}, inplace=True)
    TZonePop_DataYear.drop(
        ['GrowthinPeriod', 'GrowthperYear', 'GrowthtoYear', 'LPopulation', 'UPopulation', 'ZoneID_left', 'ZoneID_right',
         'ZoneName_right', 'ZoneName_left', 'Borough_left', 'Borough_right', 'IndivID'], axis=1, inplace=True)
    print(TZonePop_DataYear.Population.sum())

    # Translating zones for those in Scotland
    Zone_List = pd.read_csv(Zone_path)
    TZonePop_DataYear = TZonePop_DataYear.join(Zone_List.set_index('ntemZoneID'), on='ZoneID', how='right')
    # TZonePop_DataYear.rename(columns={'msoaZoneID': 'ModelZone'}, inplace=True)
    TZonePop_DataYear[
        'Population_RePropped'] = TZonePop_DataYear['Population'] * TZonePop_DataYear['overlap_ntem_pop_split_factor']

    Segmentation_List = pd.read_csv(Pop_Segmentation_path)
    TZonePop_DataYear = TZonePop_DataYear.join(Segmentation_List.set_index('NTEM_Traveller_Type'), on='TravellerType',
                                               how='right')
    TZonePop_DataYear.drop(
        ['Population', 'ZoneID', 'overlap_population', 'ntem_population', 'msoa_population',
         'overlap_msoa_pop_split_factor', 'overlap_type'], axis=1, inplace=True)
    TZonePop_DataYear.rename(columns={"Population_RePropped": "Population"}, inplace=True)
    print(TZonePop_DataYear.Population.sum())
    TZonePop_DataYear = TZonePop_DataYear.groupby(['msoaZoneID', 'AreaType', 'Borough', 'TravellerType',
                                                   'NTEM_TT_Name', 'Age_code', 'Age',
                                                   'Gender_code', 'Gender', 'Household_composition_code',
                                                   'Household_size', 'Household_car', 'Employment_type_code',
                                                   'Employment_type'])[
        ['Population']].sum().reset_index()
    NTEM_HHpop = TZonePop_DataYear
    # Export
    Export_SummaryPop = TZonePop_DataYear.groupby(['TravellerType', 'NTEM_TT_Name']).sum()
    print(Export_SummaryPop.Population.sum())
    # Export_SummaryPop.drop(['msoaZoneID'], inplace=True, axis=1)
    PopOutput = "NTEM_{}_Population.csv".format(Year)

    with open(os.path.join(Output_Folder, calling_functions_output_dir, PopOutput), "w", newline='') as f:
        TZonePop_DataYear.to_csv(f, header=True, sep=",")
    f.close()

    with open(LogFile, "a") as o:
        o.write("Total Population: \n")
        Export_SummaryPop.to_csv(o, header=False, sep="-")
        o.write("\n")
        o.write("\n")
    print("Export complete.")
    print(NTEM_HHpop.head(5))
    logging.info('NTEM_Pop_Interpolation function complete')
    print('NTEM_Pop_Interpolation function complete')
    return NTEM_HHpop


def MYE_pop_compiled(census_and_by_lu_obj):
    logging.info('Running Step 3.2.5')
    print('Running Step 3.2.5')
    mye_pop_compiled_name = 'MYE_pop_compiled'

    # Adam - DONE: the following inputs from MYE should be called in from your outcome of function MYE_APS_process
    # Note that this should work regardless of the position of the function that is being called being later in the doc
    # string as the function gets called directly from this script.
    # It's a bit inefficient as every function that requires MYE_APS_process outputs runs it!
    # TODO - Control in MYE_APS_process which tells it which function has called it and thus which part of
    #  the function to run and export and then alters the various calls and readings of outputs to work with this
    #  altered format. Some work has been done on this to prevent wasting time exporting the outputs several times,
    #  but it could be made much more efficient!
    # Added a manual control to allow read in from file (rather than internal memory) in the event of a partial run
    # False means "read in from memory" and is the default.
    _MYE_2018Pop_MSOA_path = mye_pop_compiled_output_dir
    read_2018pop_msoa_path_file = False
    if read_2018pop_msoa_path_file:
        MYE_MSOA_pop_name = r'2018_total_pop_vs_HHR_total.csv'
        MYE_MSOA_pop = pd.read_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                _MYE_2018Pop_MSOA_path,
                                                MYE_MSOA_pop_name))
        logging.info('Step 3.2.5 read in data processed by step the APS compiling function through an existing csv')
        logging.info('WARNING - This is not the default way of reading this data!')
        logging.info('Did you mean to do that?')
    else:
        logging.info('Step 3.2.5 is calling step the APS compiling function in order to obtain 2018 population data')
        MYE_MSOA_pop = MYE_APS_process(census_and_by_lu_obj, mye_pop_compiled_name, _MYE_2018Pop_MSOA_path)
        logging.info('Step 3.2.5 successfully read in data processed by the MYE_APS_prcoess function')
        logging.info('from internal memory')

    audit_MYE_MSOA_pop = MYE_MSOA_pop.copy()

    # Call Step 3.2.4 to get crp_pop
    logging.info('Step 3.2.5 is calling Step 3.2.4 to get crp_pop')
    print('Step 3.2.5 is calling Step 3.2.4 to get crp_pop')
    crp_pop = land_use_formatting(census_and_by_lu_obj)
    logging.info('Step 3.2.5 has called Step 3.2.4 and has obtained crp_pop')
    print('Step 3.2.5 has called Step 3.2.4 and has obtained crp_pop')

    # crp_pop =  pd.read_csv(Output_Folder + 'classifiedResProperty_Flatscombined_2018.csv')
    crp_MSOA_pop = crp_pop.groupby(['ZoneID'])['population'].sum().reset_index()
    crp_MSOA_pop = crp_MSOA_pop.merge(MYE_MSOA_pop, how='left', left_on='ZoneID', right_on='MSOA').drop(
        columns={'MSOA'})
    # print(crp_MSOA_pop.head(5))
    crp_MSOA_pop['pop_aj_factor'] = crp_MSOA_pop['Total_HHR'] / crp_MSOA_pop['population']
    crp_MSOA_pop = crp_MSOA_pop.drop(columns={'Total_HHR', 'Total_Pop', 'population'})
    # print(crp_MSOA_pop.head(5))
    aj_crp = crp_pop.merge(crp_MSOA_pop, how='left', on='ZoneID')
    aj_crp['aj_population'] = aj_crp['population'] * aj_crp['pop_aj_factor']
    aj_crp = aj_crp.drop(columns={'population'})
    aj_crp = aj_crp.rename(columns={'aj_population': 'population'})
    audit_aj_crp = aj_crp.copy()
    # print(aj_crp.head(5))

    # Start of block moved from Step 3.2.6 due to need to audit in Step 3.2.5

    # Car availability from NTEM
    # Read NTEM hh pop at NorMITs Zone level and make sure the zonal total is consistent to crp
    NTEM_HHpop = NTEM_Pop_Interpolation(census_and_by_lu_obj, mye_pop_compiled_output_dir)

    uk_msoa = gpd.read_file(_default_msoaRef)[['objectid', 'msoa11cd']]
    NTEM_HHpop = NTEM_HHpop.merge(uk_msoa, how='left', left_on='msoaZoneID', right_on='objectid')
    NTEM_HHpop_cols = ['msoaZoneID', 'msoa11cd', 'Borough', 'TravellerType', 'NTEM_TT_Name', 'Age_code',
                       'Age', 'Gender_code', 'Gender', 'Household_composition_code', 'Household_size', 'Household_car',
                       'Employment_type_code', 'Employment_type', 'Population']

    NTEM_HHpop_cols_to_groupby = NTEM_HHpop_cols[:-1]
    NTEM_HHpop = NTEM_HHpop.groupby(NTEM_HHpop_cols_to_groupby)['Population'].sum().reset_index()

    # Testing with Manchester
    # NTEM_HHpop_E02001045 = NTEM_HHpop[NTEM_HHpop['msoa11cd'] == 'E02001045']
    # NTEM_HHpop_E02001045.to_csv('NTEM_HHpop_E02001045.csv', index=False)

    NTEM_HHpop = NTEM_HHpop[NTEM_HHpop_cols]
    NTEM_HHpop_Total = NTEM_HHpop.groupby(['msoaZoneID'])['Population'].sum().reset_index()
    NTEM_HHpop_Total = NTEM_HHpop_Total.rename(columns={'Population': 'ZoneNTEMPop'})
    # print('Headings of NTEM_HHpop_Total')
    # print(NTEM_HHpop_Total.head(5))
    NTEM_HHpop_Total.to_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                         mye_pop_compiled_output_dir,
                                         'HHpop_NTEM_Total.csv'), index=False)

    Hhpop_Dt_Total = aj_crp.groupby(['ZoneID'])['population'].sum().reset_index()
    Hhpop_Dt_Total = Hhpop_Dt_Total.rename(columns={'population': 'ZonePop'})
    # print('Headings of Hhpop_Dt_Total')
    # print(Hhpop_Dt_Total.head(5))
    Hhpop_Dt_Total.to_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                       mye_pop_compiled_output_dir,
                                       'HHpop_Dt_Total.csv'), index=False)

    NTEM_HHpop = NTEM_HHpop.merge(NTEM_HHpop_Total, how='left', on=['msoaZoneID'])
    NTEM_HHpop = NTEM_HHpop.merge(Hhpop_Dt_Total, how='left', left_on=['msoa11cd'],
                                  right_on=['ZoneID']).drop(columns={'ZoneID'})
    # print('Headings of NTEM_HHpop')
    # print(NTEM_HHpop.head(5))
    NTEM_HHpop['pop_aj_factor'] = NTEM_HHpop['ZonePop'] / NTEM_HHpop['ZoneNTEMPop']

    NTEM_HHpop['pop_aj'] = NTEM_HHpop['Population'] * NTEM_HHpop['pop_aj_factor']
    audit_NTEM_HHpop = NTEM_HHpop.copy()
    # print(NTEM_HHpop.pop_aj.sum())
    # print(aj_crp.population.sum())

    # End of block moved from Step 3.2.6 due to need to audit in Step 3.2.5

    logging.info('Total population from MYE: ')
    logging.info(MYE_MSOA_pop.Total_Pop.sum())
    logging.info('Total household residents from MYE: ')
    logging.info(MYE_MSOA_pop.Total_HHR.sum())
    logging.info('Total household residents from aj_crp: ')
    logging.info(aj_crp.population.sum())
    logging.info('Population currently {}'.format(aj_crp.population.sum()))
    # Adam - DONE, we need to think how to organise the structure of outputs files per step
    mye_pop_compiled_filename = 'classifiedResProperty_MYEcompiled_' + ModelYear + '.csv'
    mye_pop_compiled_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                         mye_pop_compiled_output_dir,
                                         mye_pop_compiled_filename)
    aj_crp.to_csv(mye_pop_compiled_path, index=False)

    audit_aj_crp = audit_aj_crp[['ZoneID', 'population']]
    audit_aj_crp = audit_aj_crp.groupby(['ZoneID'])['population'].sum().reset_index()
    audit_aj_crp = audit_aj_crp.rename(columns={'population': 'crp_pop', 'ZoneID': 'MSOA'})
    audit_NTEM_HHpop = audit_NTEM_HHpop[['msoa11cd', 'pop_aj']]
    audit_NTEM_HHpop = audit_NTEM_HHpop.groupby(['msoa11cd'])['pop_aj'].sum().reset_index()
    audit_NTEM_HHpop = audit_NTEM_HHpop.rename(columns={'pop_aj': 'NTEM_pop', 'msoa11cd': 'MSOA'})
    audit_MYE_MSOA_pop = audit_MYE_MSOA_pop[['MSOA', 'Total_HHR']]
    audit_MYE_MSOA_pop = audit_MYE_MSOA_pop.rename(columns={'Total_HHR': 'MYE_pop'})
    audit_3_2_5_csv = pd.merge(audit_MYE_MSOA_pop, audit_NTEM_HHpop, how='left', on='MSOA')
    audit_3_2_5_csv = pd.merge(audit_3_2_5_csv, audit_aj_crp, how='left', on='MSOA')
    audit_3_2_5_csv['MYE_vs_NTEM'] = (audit_3_2_5_csv['MYE_pop'] -
                                      audit_3_2_5_csv['NTEM_pop']) / audit_3_2_5_csv['NTEM_pop']
    audit_3_2_5_csv['NTEM_vs_crp'] = (audit_3_2_5_csv['NTEM_pop'] -
                                      audit_3_2_5_csv['crp_pop']) / audit_3_2_5_csv['crp_pop']
    audit_3_2_5_csv['crp_vs_MYE'] = (audit_3_2_5_csv['crp_pop'] -
                                     audit_3_2_5_csv['MYE_pop']) / audit_3_2_5_csv['MYE_pop']
    audit_3_2_5_csv_max = max(audit_3_2_5_csv['MYE_vs_NTEM'].max(),
                              audit_3_2_5_csv['NTEM_vs_crp'].max(),
                              audit_3_2_5_csv['crp_vs_MYE'].max())
    audit_3_2_5_csv_min = min(audit_3_2_5_csv['MYE_vs_NTEM'].min(),
                              audit_3_2_5_csv['NTEM_vs_crp'].min(),
                              audit_3_2_5_csv['crp_vs_MYE'].min())
    audit_3_2_5_csv_mean = np.mean([audit_3_2_5_csv['MYE_vs_NTEM'].mean(),
                                    audit_3_2_5_csv['NTEM_vs_crp'].mean(),
                                    audit_3_2_5_csv['crp_vs_MYE'].mean()])
    audit_3_2_5_csv_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                        mye_pop_compiled_output_dir,
                                        'Audits',
                                        'Audit_3.2.5_full_zonal_breakdown.csv')
    audit_3_2_5_csv.to_csv(audit_3_2_5_csv_path, index=False)

    audit_3_2_5_header = 'Audit for Step 3.2.5\nCreated ' + str(datetime.datetime.now())
    audit_3_2_5_text = '\n'.join(['The total 2018 population from MYPE is: ' + str(MYE_MSOA_pop.Total_Pop.sum()),
                                  'The total 2018 household population from MYPE is: ' + str(
                                      MYE_MSOA_pop.Total_HHR.sum()),
                                  'The total 2018 household population output from Step 3.2.5 is: ',
                                  '\tBy zone, age, gender, HH composition and employment status (from NTEM): ' + str(
                                      audit_NTEM_HHpop['NTEM_pop'].sum()),
                                  '\tBy zone and dwelling type: ' + str(audit_aj_crp['crp_pop'].sum()),
                                  'Comparing zonal HH population. All %age difference values should be very close to 0',
                                  'The max, min and mean of the three possible comparisons are presented here:',
                                  '\tMax percentage difference: ' + str(audit_3_2_5_csv_max * 100) + '%',
                                  '\tMin percentage difference: ' + str(audit_3_2_5_csv_min * 100) + '%',
                                  '\tMean percentage difference: ' + str(audit_3_2_5_csv_mean * 100) + '%',
                                  'A full zonal breakdown of these metrics is presented in:',
                                  audit_3_2_5_csv_path])
    audit_3_2_5_content = '\n'.join([audit_3_2_5_header, audit_3_2_5_text])
    audit_3_2_5_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    mye_pop_compiled_output_dir,
                                    'Audits',
                                    'Audit_3.2.5.txt')
    with open(audit_3_2_5_path, 'w') as text_file:
        text_file.write(audit_3_2_5_content)

    census_and_by_lu_obj.state['3.2.5 Uplifting 2018 population according to 2018 MYPE'] = 1
    logging.info('Step 3.2.5 completed')
    print('Step 3.2.5 completed')
    return [aj_crp, NTEM_HHpop, audit_MYE_MSOA_pop]


def pop_with_full_dimensions(census_and_by_lu_obj):
    """
    Function to join the bespoke census query to the classified residential property data.
    Problem here is that there are segments with attributed population coming in from the bespoke census query that
    don't have properties to join on. So we need classified properties by MSOA for this to work atm
    TODO: make it work for zones other than MSOA
    ART, 12/11/2021 model_year moved defined at the head of this file. Can't find model_name!
    """
    logging.info('Running Step 3.2.6/3.2.7 function')
    print('Running Step 3.2.6/3.2.7 function')

    # TODO - Sort out these filepaths a bit more permanently
    output_NTEM_HHpop_filepath = 'NTEM_HHpop_Aj.csv'
    input_NTEM_HHpop_filepath = r'I:\NorMITs Land Use\import\NorCOM outputs\NorCOM_TT_R20.csv'

    # TODO - Make this switching more robust
    # Comment out one or the other of the following statements
    # These select whether you are running NorMITs to MAKE NorCOM inputs
    # or to UTILISE NorCOM inputs
    # how_to_run = 'export to NorCOM'
    how_to_run = 'import from NorCOM'

    logging_how_to_run = ' '.join(['Running with section 3.2.6 set to', how_to_run])
    logging.info(logging_how_to_run)

    # TODO - These variable names could probably do with tidying up at bit to avoid confusion with
    #  their Step 3_2_5 counterparts. Only still like this as a block of code was moved from Step
    #  3_2_6 to Step 3_2_5 at the last minute!
    logging.info('Step 3.2.6 is calling Step 3.2.5')
    print('Step 3.2.6 is calling Step 3.2.5')
    call_3_2_5 = MYE_pop_compiled(census_and_by_lu_obj)
    aj_crp = call_3_2_5[0]
    NTEM_HHpop = call_3_2_5[1]
    audit_original_hhpop = call_3_2_5[2]
    logging.info('Step 3.2.6 has completed its call of Step 3.2.5, Step 3.2.6 is continuing...')
    print('Step 3.2.6 has completed its call of Step 3.2.5, Step 3.2.6 is continuing...')

    if how_to_run == 'export to NorCOM':
        NTEM_HHpop.to_csv(output_NTEM_HHpop_filepath, index=False)
        # Testing for just Manchester
        # NTEM_HHpop_Aj_E02001045 = NTEM_HHpop[NTEM_HHpop['msoa11cd'] == 'E02001045']
        # NTEM_HHpop_Aj_E02001045.to_csv('NTEM_HHpop_Aj_E02001045.csv', index=False)
        logging.info('Step 3.2.6 completed')
        logging.info('Dumped file "NTEM_HHpop_Aj.csv" for NorCOM')
        logging.info('!!!!!! SERIOUS WARNING !!!!!!')
        logging.info('Any further functions that are called from this Land Use process are highly likely to be wrong!')
        # TODO - Call NorCOM script directly here? Then could remove if/else statement?
        #  Waiting for NorCOM to be refined enough that it doesn't take 3 days to run...
    else:
        logging.info('Reading in file from NorCOM')
        # Make sure this read in and is picking up an actual file and actual columns
        NorCOM_NTEM_HHpop = pd.read_csv(input_NTEM_HHpop_filepath)
        NorCOM_NTEM_HHpop = NorCOM_NTEM_HHpop[['msoa11cd', 'lu_TravellerType', 'NorCOM_result']]

        # Sort out df prior to merger
        NTEM_HHpop_trim = NTEM_HHpop[['msoaZoneID',
                                      'msoa11cd',
                                      'TravellerType',
                                      'Age_code',
                                      'Gender_code',
                                      'Household_composition_code',
                                      'Household_size',
                                      'Household_car',
                                      'Employment_type_code']]
        NTEM_HHpop_trim.groupby(['msoaZoneID',
                                 'msoa11cd',
                                 'TravellerType',
                                 'Age_code',
                                 'Gender_code',
                                 'Household_composition_code',
                                 'Household_size',
                                 'Household_car',
                                 'Employment_type_code'])

        NTEM_HHpop_trim = pd.merge(NTEM_HHpop_trim,
                                   NorCOM_NTEM_HHpop,
                                   how='right',
                                   left_on=['msoa11cd', 'TravellerType'],
                                   right_on=['msoa11cd', 'lu_TravellerType'])

        NTEM_HHpop_trim = NTEM_HHpop_trim.rename(
            columns={'msoaZoneID': 'z',
                     'TravellerType': 'ntem_tt',
                     'Age_code': 'a',
                     'Gender_code': 'g',
                     'Household_composition_code': 'h',
                     'Employment_type_code': 'e',
                     'NorCOM_result': 'P_NTEM'})
        NTEM_HHpop_trim['z'] = NTEM_HHpop_trim['z'].astype(int)
        NTEM_HHpop_trim_iterator = zip(NTEM_HHpop_trim['z'],
                                       NTEM_HHpop_trim['a'],
                                       NTEM_HHpop_trim['g'],
                                       NTEM_HHpop_trim['h'],
                                       NTEM_HHpop_trim['e'])

        NTEM_HHpop_trim['aghe_Key'] = ['_'.join([str(z), str(a), str(g), str(h), str(e)])
                                       for z, a, g, h, e in NTEM_HHpop_trim_iterator]
        # Read in f (f_tns|zaghe) to expand adjustedNTEM hh pop with addtional dimension of t(dwelling type),
        # n(HRP NS-SEC) and s (SOC)
        # Replace this block with new process from 2011 output f.
        # ['z', 'a', 'g', 'h', 'e', 't', 'n', 's', 'f_tns|zaghe']
        census_f_value = pd.read_csv(os.path.join(
            census_and_by_lu_obj.import_folder, _census_f_value_path, 'NorMITs_2011_post_ipfn_f_values.csv'))
        # census_f_value['z'] = census_f_value['z'].astype(int)
        census_f_value_iterator = zip(census_f_value['z'],
                                      census_f_value['a'],
                                      census_f_value['g'],
                                      census_f_value['h'],
                                      census_f_value['e'])
        census_f_value['aghe_Key'] = ['_'.join([str(z), str(a), str(g), str(h), str(e)])
                                      for z, a, g, h, e in census_f_value_iterator]
        NTEM_HHpop_trim = pd.merge(NTEM_HHpop_trim,
                                   census_f_value,
                                   on='aghe_Key')
        NTEM_HHpop_trim = NTEM_HHpop_trim.drop(
            columns=['z_x', 'a_x', 'g_x', 'h_x', 'e_x', 'lu_TravellerType'])
        NTEM_HHpop_trim = NTEM_HHpop_trim.rename(
            columns={'z_y': 'z', 'a_y': 'a', 'g_y': 'g', 'h_y': 'h', 'e_y': 'e'})
        NTEM_HHpop_trim['P_aghetns'] = (NTEM_HHpop_trim['f_tns|zaghe']
                                        * NTEM_HHpop_trim['P_NTEM'])

        # Testing with Manchester
        # NTEM_HH_P_aghetns_E02001045 = NTEM_HHpop_trim[NTEM_HHpop_trim['msoa11cd'] == 'E02001045']
        # NTEM_HH_P_aghetns_E02001045.to_csv('NTEM_HH_P_aghetns_E02001045.csv', index=False)
        logging.info('NTEM HH pop scaled by f is currently:')
        logging.info(NTEM_HHpop_trim.P_aghetns.sum())

        audit_NTEM_HHpop_trim = NTEM_HHpop_trim.copy()
        audit_NTEM_HHpop_trim = audit_NTEM_HHpop_trim[['msoa11cd', 'P_aghetns']]
        audit_3_2_6_csv = pd.merge(audit_original_hhpop, audit_NTEM_HHpop_trim,
                                   how='left', left_on='MSOA', right_on='msoa11cd')
        audit_3_2_6_csv['HH_pop_%age_diff'] = (audit_3_2_6_csv['P_aghetns'] -
                                               audit_3_2_6_csv['MYE_pop']) / audit_3_2_6_csv['MYE_pop']
        audit_3_2_6_csv_max = audit_3_2_6_csv['HH_pop_%age_diff'].max()
        audit_3_2_6_csv_min = audit_3_2_6_csv['HH_pop_%age_diff'].min()
        audit_3_2_6_csv_mean = audit_3_2_6_csv['HH_pop_%age_diff'].mean()
        audit_3_2_6_csv_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                            pop_with_full_dims_output_dir,
                                            'Audits',
                                            'Audit_3.2.6_full_zonal_breakdown.csv')
        audit_3_2_6_csv.to_csv(audit_3_2_6_csv_path, index=False)

        audit_3_2_6_header = 'Audit for Step 3.2.6\nCreated ' + str(datetime.datetime.now())
        audit_3_2_6_text = '\n'.join(['The total 2018 population from MYPE is: ' + str(NTEM_HHpop_trim.P_aghetns.sum()),
                                      'Comparing zonal HH population original to present:',
                                      '\tMax percentage difference: ' + str(audit_3_2_6_csv_max * 100) + '%',
                                      '\tMin percentage difference: ' + str(audit_3_2_6_csv_min * 100) + '%',
                                      '\tMean percentage difference: ' + str(audit_3_2_6_csv_mean * 100) + '%',
                                      'All of the above should be equal (or very close) to 0.',
                                      'A full zonal breakdown of these metrics is presented in:',
                                      audit_3_2_6_csv_path])
        audit_3_2_6_content = '\n'.join([audit_3_2_6_header, audit_3_2_6_text])
        audit_3_2_6_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                        pop_with_full_dims_output_dir,
                                        'Audits',
                                        'Audit_3.2.6.txt')
        with open(audit_3_2_6_path, 'w') as text_file:
            text_file.write(audit_3_2_6_content)

        logging.info('Step 3.2.6 completed. Continuing running this function as Step 3.2.7.')

        # Further adjust detailed dimensional population according to zonal dwelling type from crp
        # This is Section 3.2.7 (to the end of this function)
        # Title of section 3.2.7 is "Verify population profile by dwelling type"
        # TODO ART, update script around here to reflect Jupyter - Done? It seems to work anyway...
        #  still need to add audits for 3.2.6 above this though
        NorMITs_HHpop_byDt = aj_crp.rename(columns={'ZoneID': 'MSOA',
                                                    'population': 'crp_P_t',
                                                    'UPRN': 'properties',
                                                    'census_property_type': 't'})
        NTEM_HHpop_byDt = NTEM_HHpop_trim.groupby(['z', 't'])['P_aghetns'].sum().reset_index()
        NTEM_HHpop_byDt = NTEM_HHpop_byDt.rename(columns={'P_aghetns': 'P_t'})
        NTEM_HHpop_byDt.to_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                            pop_with_full_dims_output_dir,
                                            'NTEM_HHpop_byDt.csv'), index=False)

        # Testing with Manchester
        # NTEM_HHpop_byDt_total_E02001045 = NTEM_HHpop_byDt[NTEM_HHpop_byDt['z'] == '1013']
        # NTEM_HHpop_byDt_total_E02001045.to_csv('NTEM_HHpop_byDt_total_E02001045.csv', index=False)

        uk_ave_hh_occ = NorMITs_HHpop_byDt.copy()
        uk_ave_hh_occ['pop_pre_aj'] = uk_ave_hh_occ['crp_P_t'] / uk_ave_hh_occ['pop_aj_factor']
        uk_ave_hh_occ = uk_ave_hh_occ.groupby(['t'])[['properties', 'pop_pre_aj']].sum()
        uk_ave_hh_occ['UK_average_hhocc'] = uk_ave_hh_occ['pop_pre_aj'] / uk_ave_hh_occ['properties']

        HHpop = NTEM_HHpop_trim.merge(NTEM_HHpop_byDt, how='left', on=['z', 't'])
        # Where the problem occur: Does it still occur?
        HHpop = HHpop.merge(NorMITs_HHpop_byDt,
                            how='left',
                            left_on=['msoa11cd', 't'],
                            right_on=['MSOA', 't']).drop(
            columns={'MSOA', 'pop_aj_factor', 'Zone'}).rename(
            columns={'msoa11cd': 'MSOA'})
        HHpop.loc[HHpop['household_occupancy_18'].isnull(),
                  'household_occupancy_18'] = HHpop['t'].map(uk_ave_hh_occ.UK_average_hhocc)
        HHpop.fillna({'properties': 0, 'crp_P_t': 0}, inplace=True)

        HHpop['P_aghetns_aj_factor'] = HHpop['crp_P_t'] / HHpop['P_t']
        HHpop['P_aghetns_aj'] = HHpop['P_aghetns'] * HHpop['P_aghetns_aj_factor']

        # Testing with Manchester
        # HH_P_aghetns_aj_E02001045 = HHpop[HHpop['MSOA'] == 'E02001045']
        # HH_P_aghetns_aj_E02001045.to_csv('HH_P_aghetns_aj_E02001045.csv', index=False)

        HHpop = HHpop.rename(columns={'P_aghetns': 'NTEM_HH_pop', 'P_aghetns_aj': 'people'})

        logging.info('total of hh pop from aj_ntem: ')
        logging.info(HHpop.people.sum())
        logging.info('total of hh pop from aj_crp: ')
        logging.info(aj_crp.population.sum())

        # Check the outcome compare NTEM aj pop (NTEM_HH_pop) against NorMITs pop (people)
        # adjusted according to pop by dwelling type
        # Create and save an audit
        current_running_dir = os.getcwd()
        pop_with_full_dims_second_dir_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                          pop_with_full_dims_second_output_dir)
        os.chdir(os.path.join(pop_with_full_dims_second_dir_path, 'Audits'))
        seg_folder = 'NTEM Segmentation Audits'
        utils.create_folder(seg_folder)

        ZonalTot = HHpop.groupby(['z', 'MSOA'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        ZonalTot = ZonalTot.rename(columns={'people': 'NorMITs_Zonal', 'NTEM_HH_pop': 'NTEM_Zonal'})
        audit_ZonalTot = pd.merge(audit_original_hhpop, ZonalTot, how='right', on='MSOA')
        audit_ZonalTot['HH_pop_%age_diff'] = (audit_ZonalTot['NorMITs_Zonal'] -
                                              audit_ZonalTot['MYE_pop']) / audit_ZonalTot['MYE_pop']
        audit_ZonalTot_max = audit_ZonalTot['HH_pop_%age_diff'].max()
        audit_ZonalTot_min = audit_ZonalTot['HH_pop_%age_diff'].min()
        audit_ZonalTot_mean = audit_ZonalTot['HH_pop_%age_diff'].mean()
        audit_ZonalTot.to_csv('Audit_3_2_7_HHpop_by_zone.csv', index=False)

        DT = HHpop.groupby(['z', 'MSOA', 't'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        DT_check = DT.merge(ZonalTot, how='left', on=['z'])
        DT_check['Ab_Perdiff'] = DT_check['people'] / DT_check['NTEM_HH_pop'] - 1
        DT_check['NorMITs_profile'] = DT_check['people'] / DT_check['NorMITs_Zonal']
        DT_check['NTEM_profile'] = DT_check['NTEM_HH_pop'] / DT_check['NTEM_Zonal']
        DT_check['Profile_Perdiff'] = DT_check['NorMITs_profile'] / DT_check['NTEM_profile'] - 1
        DT_check.to_csv(seg_folder + '/Zone_check_byDT.csv', index=False)

        Cars = HHpop.groupby(['z', 'MSOA', 'Household_car'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        Cars_check = Cars.merge(ZonalTot, how='left', on=['z'])
        Cars_check['Ab_Perdiff'] = Cars_check['people'] / Cars_check['NTEM_HH_pop'] - 1
        Cars_check['NorMITs_profile'] = Cars_check['people'] / Cars_check['NorMITs_Zonal']
        Cars_check['NTEM_profile'] = Cars_check['NTEM_HH_pop'] / Cars_check['NTEM_Zonal']
        Cars_check['Profile_Perdiff'] = Cars_check['NorMITs_profile'] / Cars_check['NTEM_profile'] - 1
        Cars_check.to_csv(seg_folder + '/Zone_check_byCars.csv', index=False)

        HHsize = HHpop.groupby(['z', 'MSOA', 'Household_size'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        HHsize_check = HHsize.merge(ZonalTot, how='left', on=['z'])
        HHsize_check['Ab_Perdiff'] = HHsize_check['people'] / HHsize_check['NTEM_HH_pop'] - 1
        HHsize_check['NorMITs_profile'] = HHsize_check['people'] / HHsize_check['NorMITs_Zonal']
        HHsize_check['NTEM_profile'] = HHsize_check['NTEM_HH_pop'] / HHsize_check['NTEM_Zonal']
        HHsize_check['Profile_Perdiff'] = HHsize_check['NorMITs_profile'] / HHsize_check['NTEM_profile'] - 1
        HHsize_check.to_csv(seg_folder + '/Zone_check_byHHsize.csv', index=False)

        HH_composition = HHpop.groupby(['z', 'MSOA', 'h'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        HH_composition_check = HH_composition.merge(ZonalTot, how='left', on=['z'])
        HH_composition_check['Ab_Perdiff'] = HH_composition_check['people'] / HH_composition_check['NTEM_HH_pop'] - 1
        HH_composition_check['NorMITs_profile'] = HH_composition_check['people'] / HH_composition_check['NorMITs_Zonal']
        HH_composition_check['NTEM_profile'] = HH_composition_check['NTEM_HH_pop'] / HH_composition_check['NTEM_Zonal']
        HH_composition_check['Profile_Perdiff'] = (HH_composition_check['NorMITs_profile'] /
                                                   HH_composition_check['NTEM_profile']) - 1
        HH_composition_check.to_csv(seg_folder + '/Zone_check_byHH_composition.csv', index=False)

        Age = HHpop.groupby(['z', 'MSOA', 'a'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        Age_check = Age.merge(ZonalTot, how='left', on=['z'])
        Age_check['Ab_Perdiff'] = Age_check['people'] / Age_check['NTEM_HH_pop'] - 1
        Age_check['NorMITs_profile'] = Age_check['people'] / Age_check['NorMITs_Zonal']
        Age_check['NTEM_profile'] = Age_check['NTEM_HH_pop'] / Age_check['NTEM_Zonal']
        Age_check['Profile_Perdiff'] = Age_check['NorMITs_profile'] / Age_check['NTEM_profile'] - 1
        Age_check.to_csv(seg_folder + '/Zone_check_byAge.csv', index=False)

        Gender = HHpop.groupby(['z', 'MSOA', 'g'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        Gender_check = Gender.merge(ZonalTot, how='left', on=['z'])
        Gender_check['Ab_Perdiff'] = Gender_check['people'] / Gender_check['NTEM_HH_pop'] - 1
        Gender_check['NorMITs_profile'] = Gender_check['people'] / Gender_check['NorMITs_Zonal']
        Gender_check['NTEM_profile'] = Gender_check['NTEM_HH_pop'] / Gender_check['NTEM_Zonal']
        Gender_check['Profile_Perdiff'] = Gender_check['NorMITs_profile'] / Gender_check['NTEM_profile'] - 1
        Gender_check.to_csv(seg_folder + '/Zone_check_byGender.csv', index=False)

        Employment = HHpop.groupby(['z', 'MSOA', 'e'])[['people', 'NTEM_HH_pop']].sum().reset_index()
        Employment_check = Employment.merge(ZonalTot, how='left', on=['z'])
        Employment_check['Ab_Perdiff'] = Employment_check['people'] / Employment_check['NTEM_HH_pop'] - 1
        Employment_check['NorMITs_profile'] = Employment_check['people'] / Employment_check['NorMITs_Zonal']
        Employment_check['NTEM_profile'] = Employment_check['NTEM_HH_pop'] / Employment_check['NTEM_Zonal']
        Employment_check['Profile_Perdiff'] = Employment_check['NorMITs_profile'] / Employment_check['NTEM_profile'] - 1
        Employment_check.to_csv(seg_folder + '/Zone_check_byEmployment.csv', index=False)

        audit_3_2_7_header = 'Audit for Step 3.2.7\nCreated ' + str(datetime.datetime.now())
        audit_3_2_7_text = '\n'.join(['>>> IMPORTANT NOTE <<<',
                                      '\tIf you can\'t find the output you are looking for, which should have been',
                                      '\texported to Step 3.2.7, try looking in the Step 3.2.6 directory, as both',
                                      '\tsteps run using the same function, so separating the outputs can be tricky!',
                                      'High level summaries:',
                                      '\tThe total HH pop from aj_ntem is: ' + str(HHpop.people.sum()),
                                      '\tThe total HH pop from aj_crp is: ' + str(aj_crp.population.sum()),
                                      'The zonal variation in HH pop against the original MYPE derived HH pop has:',
                                      '\tMax percentage diff: ' + str(audit_ZonalTot_max * 100) + '%',
                                      '\tMin percentage diff: ' + str(audit_ZonalTot_min * 100) + '%',
                                      '\tMean percentage diff: ' + str(audit_ZonalTot_mean * 100) + '%',
                                      'These percentage differences should be equal (or close to) 0.',
                                      'A full zonal breakdown of these differences can be found here:',
                                      os.path.join(os.getcwd(), 'Audit_3_2_7_HHpop_by_zone.csv'),
                                      'Additionally, number of segmentation audits have also been produced.',
                                      'These can be found in:',
                                      os.path.join(os.getcwd(), seg_folder),
                                      'Again, the differences are expected to be small.'])
        audit_3_2_7_content = '\n'.join([audit_3_2_7_header, audit_3_2_7_text])
        audit_3_2_7_path = 'Audit_3.2.7.txt'  # Long filepath not needed as we are in the directory we want to dump to.
        with open(audit_3_2_7_path, 'w') as text_file:
            text_file.write(audit_3_2_7_content)

        os.chdir(current_running_dir)

        # get 2021 LA in
        Zone_2021LA = pd.read_csv(
            os.path.join(
                census_and_by_lu_obj.import_folder, _Zone_2021LA_path))[['NorMITs Zone', '2021 LA', '2021 LA Name']]
        HHpop = HHpop.merge(Zone_2021LA, how='left',
                            left_on=['z'],
                            right_on=['NorMITs Zone']).drop(columns={'NorMITs Zone'})
        HHpop = HHpop.rename(columns={'2021 LA': '2021_LA_code', '2021 LA Name': '2021_LA_Name'})

        output_cols = ['2021_LA_code', '2021_LA_Name', 'z', 'MSOA', 'a', 'g', 'h', 'e', 't', 'n', 's', 'properties',
                       'people']
        HHpop = HHpop[output_cols]
        logging.info('Population currently {}'.format(HHpop.people.sum()))

        # Adam - DONE, we need to think about how to organise the output files per step
        pop_with_full_dims_filename = '_'.join(['HhPop_byfullNorMITsSegs_initial',
                                                ModelYear,
                                                census_and_by_lu_obj.model_zoning])
        uk_ave_hh_occ_filename = 'uk_average_hh_occupancy_by_t.csv'

        pop_with_full_dims_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                               pop_with_full_dims_output_dir,
                                               pop_with_full_dims_filename)
        uk_ave_hh_occ_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                          pop_with_full_dims_output_dir,
                                          uk_ave_hh_occ_filename)

        compress.write_out(HHpop, pop_with_full_dims_path)
        uk_ave_hh_occ.to_csv(uk_ave_hh_occ_path, index=False)

        census_and_by_lu_obj.state[
            '3.2.6 and 3.2.7 expand NTEM population to full dimensions and verify pop profile'] = 1
        logging.info('Step 3.2.7 completed (along with some file dumping for Step 3.2.6)')
        logging.info('Step 3.2.6/Step 3.2.7 function has completed')
        print('Step 3.2.7 completed (along with some file dumping for Step 3.2.6)')
        print('Step 3.2.6/Step 3.2.7 function has completed')


def subsets_worker_nonworker(census_and_by_lu_obj, function_that_called_me):
    logging.info('Running Step 3.2.8, which has been called by ' + function_that_called_me)
    print('Running Step 3.2.8, which has been called by ' + function_that_called_me)

    # Read in output of Step 3.2.6/3.2.7 rather than calling the function and taking the output directly.
    # This prevents chain calling from 3.2.10 all the way back to 3.2.4!
    hhpop_dir_path = pop_with_full_dims_output_dir
    hhpop_filename = '_'.join(['HhPop_byfullNorMITsSegs_initial',
                               ModelYear,
                               census_and_by_lu_obj.model_zoning])
    hhpop_filepath = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                  hhpop_dir_path,
                                  hhpop_filename)
    HHpop = compress.read_in(hhpop_filepath)

    audit_3_2_8_data = HHpop.copy()
    audit_3_2_8_data = audit_3_2_8_data.groupby(['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])[
        ['people']].sum().reset_index()
    audit_3_2_8_data = audit_3_2_8_data.rename(columns={'people': 'HHpop'})

    HHpop_workers = HHpop.loc[
        (HHpop['e'] <= 2)]
    HHpop_non_workers = HHpop.loc[
        (HHpop['e'] > 2)]

    HHpop_workers_LA = HHpop_workers.groupby(['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])[
        ['people']].sum().reset_index()
    audit_HHpop_workers_LA = HHpop_workers_LA.copy()
    HHpop_non_workers_LA = HHpop_non_workers.groupby(['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])[
        ['people']].sum().reset_index()
    audit_HHpop_non_workers_LA = HHpop_non_workers_LA.copy()

    # check totals
    # print(HHpop.people.sum())
    # print(HHpop_workers.people.sum())
    # print(HHpop_non_workers.people.sum())
    # print(HHpop_workers_LA.people.sum() + HHpop_non_workers_LA.people.sum())

    # gender and employment status combined for workers to prepare for furness on LA level
    ge_combination = [(HHpop_workers_LA['g'] == 2) & (HHpop_workers_LA['e'] == 1),
                      (HHpop_workers_LA['g'] == 2) & (HHpop_workers_LA['e'] == 2),
                      (HHpop_workers_LA['g'] == 3) & (HHpop_workers_LA['e'] == 1),
                      (HHpop_workers_LA['g'] == 3) & (HHpop_workers_LA['e'] == 2)]
    ge_combination_values = ['1', '2', '3', '4']
    HHpop_workers_LA['ge'] = np.select(ge_combination, ge_combination_values)
    seed_worker = HHpop_workers_LA[['2021_LA_code', 'ge', 's', 'a', 'h', 't', 'n', 'people']]
    # print(HHpop_workers_LA.head(5))
    # print(HHpop_workers_LA.tail(5))

    HHpop_nwkrs_ag_LA = HHpop_non_workers_LA.groupby(['2021_LA_code', 'a', 'g'])[['people']].sum().reset_index()
    # the following outputs are just for checking purpose
    HHpop_wkrs_ge_LA = HHpop_workers_LA.groupby(['2021_LA_code', 'ge'])[['people']].sum().reset_index()
    HHpop_wkrs_s_LA = HHpop_workers_LA.groupby(['2021_LA_code', 's'])[['people']].sum().reset_index()

    logging.info('Worker currently {}'.format(HHpop_workers.people.sum()))
    logging.info('Non_worker currently {}'.format(HHpop_non_workers.people.sum()))
    logging.info('Population currently {}'.format(HHpop_workers.people.sum() + HHpop_non_workers.people.sum()))
    census_and_by_lu_obj.state['3.2.8 get subsets of worker and non-worker'] = 1

    audit_3_2_8_data = pd.merge(audit_3_2_8_data, audit_HHpop_workers_LA,
                                how='left', on=['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])
    audit_3_2_8_data = audit_3_2_8_data.rename(columns={'people': 'worker_pop'})
    audit_3_2_8_data = pd.merge(audit_3_2_8_data, audit_HHpop_non_workers_LA,
                                how='left', on=['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])
    audit_3_2_8_data = audit_3_2_8_data.rename(columns={'people': 'non_worker_pop'})
    audit_3_2_8_data['worker+non_worker_pop'] = audit_3_2_8_data['worker_pop'] + audit_3_2_8_data['non_worker_pop']
    audit_3_2_8_data['Check_pop_tots'] = (audit_3_2_8_data['worker+non_worker_pop'] -
                                          audit_3_2_8_data['HHpop']) / audit_3_2_8_data['HHpop']
    audit_3_2_8_data_max = audit_3_2_8_data['Check_pop_tots'].max()
    audit_3_2_8_data_min = audit_3_2_8_data['Check_pop_tots'].min()
    audit_3_2_8_data_mean = audit_3_2_8_data['Check_pop_tots'].mean()
    audit_3_2_8_data_export_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                subsets_worker_nonworker_output_dir,
                                                'Audits',
                                                'Audit_3.2.8_pop_vs_workers+non-workers_df_dump')
    compress.write_out(audit_3_2_8_data, audit_3_2_8_data_export_path)

    audit_3_2_8_header = 'Audit for Step 3.2.8\nCreated ' + str(datetime.datetime.now())
    audit_3_2_8_text = '\n'.join(['Totals at the end of Step 3.2.8:',
                                  '\tWorkers: {}'.format(HHpop_workers.people.sum()),
                                  '\tNon_workers: {}'.format(HHpop_non_workers.people.sum()),
                                  '\tPopulation (worker + non-worker): {}'.format(
                                      HHpop_workers.people.sum() + HHpop_non_workers.people.sum()),
                                  'Also check variations at the LA level (by a, g, h, e, t, n, s), where:',
                                  '\tMax %age difference is:' + str(audit_3_2_8_data_max * 100) + '%',
                                  '\tMin %age difference is:' + str(audit_3_2_8_data_min * 100) + '%',
                                  '\tMean %age difference is:' + str(audit_3_2_8_data_mean * 100) + '%',
                                  'These differences should be 0 by definition.',
                                  'A compressed full d, a, g, h, e,t, n, s breakdown is included for completeness.',
                                  'It is expected that a csv dump would have been too big.',
                                  'The compressed file is dumped here (plus its file extension):',
                                  audit_3_2_8_data_export_path])
    audit_3_2_8_content = '\n'.join([audit_3_2_8_header, audit_3_2_8_text])
    audit_3_2_8_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    subsets_worker_nonworker_output_dir,
                                    'Audits',
                                    'Audit_3.2.8.txt')
    with open(audit_3_2_8_path, 'w') as text_file:
        text_file.write(audit_3_2_8_content)

    print('Step 3.2.8 completed (it is just returning outputs now...)')
    # Return variables based on function calling this step
    if function_that_called_me == 'LA_level_adjustment':
        la_2021_to_z_lookup = HHpop_workers[['2021_LA_code', 'MSOA']]
        logging.info('Step 3.2.8 completed - Called by step 3.2.9')
        logging.info('Returning variable "seed_worker" in internal memory')
        logging.info('Returning variable "HHpop_workers_LA" in internal memory')
        logging.info('Returning variable "HHpop_nwkrs_ag_LA" in internal memory')
        logging.info('Returning variable "HHpop_non_workers_LA" in internal memory')
        logging.info('Returning variable "la_2021_to_z_lookup" in internal memory')
        logging.info('Note that no files have been written out from this call')
        print('Returned variables:')
        print('seed_worker, HHpop_workers_LA, HHpop_nwrkrs_LA, HHpop_non_workers_LA, la_2021_to_z_lookup')
        return [seed_worker, HHpop_workers_LA, HHpop_nwkrs_ag_LA, HHpop_non_workers_LA, la_2021_to_z_lookup]
    elif function_that_called_me == 'adjust_zonal_workers_nonworkers':
        logging.info('Step 3.2.8 completed - Called by step 3.2.10')
        logging.info('Returning variable "HHpop_workers_LA" in internal memory')
        print('Returned variable adjust_zonal_workers_nonworkers')
        return [HHpop_workers, HHpop_non_workers]
    else:
        # Adam - DONE, we need to think how to organise the structure of outputs files per step
        # Saving files out only when called by functions outside of this .py file
        subsets_worker_nonworker_dir_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                         subsets_worker_nonworker_output_dir)
        HHpop_nwkrs_ag_LA.to_csv(
            os.path.join(subsets_worker_nonworker_dir_path, 'Initial_HHpop_nwkrs_ag_LA.csv'), index=False)
        HHpop_wkrs_ge_LA.to_csv(
            os.path.join(subsets_worker_nonworker_dir_path, 'Initial_HHpop_wkrs_ge_LA.csv'), index=False)
        HHpop_wkrs_s_LA.to_csv(
            os.path.join(subsets_worker_nonworker_dir_path, 'Initial_HHpop_wkrs_s_LA.csv'), index=False)
        logging.info('Step 3.2.8 completed - not called by a 3.2.x function')
        logging.info('Returning only a short list stating no output was requested')
        logging.info('HHpop data has been saved to file though')
        print('Was not called by a valid 3.2.x function, no data returned, but saved HHpop to file')
        return pd.DataFrame([], columns=['No data requested', 'but data saved to file'])


def LA_level_adjustment(census_and_by_lu_obj):
    logging.info('Running Step 3.2.9')
    print('Running Step 3.2.9')
    la_level_adjustment_name = 'LA_level_adjustment'
    # Adam - DONE, the inputs here should be called in from the outcome of your scripts from function MYE_APS_process
    # Added a manual control to allow read in from file (rather than internal memory) in the event of a partial run
    # False means "read in from memory" and is the default.
    read_2018pop_msoa_path_file = False

    if read_2018pop_msoa_path_file:
        _MYE_2018Pop_MSOA_path = la_level_adjustment_output_dir
        _LA_worker_control_path = r'MYE_APS_LA_Worker.csv'
        _LA_nonworker_control_path = r'MYE_APS_LA_NonWorker.csv'
        _2021LAID_path = r'2021LAID_LAcode_LAname.csv'

        _LA_worker_control_path = os.path.join(
            census_and_by_lu_obj.out_paths['write_folder'], _MYE_2018Pop_MSOA_path, _LA_worker_control_path)
        _LA_nonworker_control_path = os.path.join(
            census_and_by_lu_obj.out_paths['write_folder'], _MYE_2018Pop_MSOA_path, _LA_nonworker_control_path)
        _2021LAID_path = os.path.join(
            census_and_by_lu_obj.out_paths['write_folder'], _MYE_2018Pop_MSOA_path, _2021LAID_path)

        LA_ID = pd.read_csv(_2021LAID_path)
        LA_worker_control = pd.read_csv(_LA_worker_control_path)[['2021_LA_name', '2021_LA_code', 'LA', 'Worker']]
        LA_worker_ge_control = pd.read_csv(_LA_worker_control_path)[
            ['2021_LA_name', '2021_LA_code', 'LA', 'Male fte', 'Male pte', 'Female fte', 'Female pte']]
        LA_worker_s_control = pd.read_csv(_LA_worker_control_path)[
            ['2021_LA_name', '2021_LA_code', 'LA', 'higher', 'medium', 'skilled']]
        LA_nonworker_control = pd.read_csv(_LA_nonworker_control_path)[
            ['2021_LA_name', '2021_LA_code', 'LA', 'Non worker']]
        LA_nonworker_ag_control = pd.read_csv(_LA_nonworker_control_path)[
            ['2021_LA_name', '2021_LA_code', 'LA', 'Children', 'M_16-74', 'F_16-74', 'M_75 and over', 'F_75 and over']]

        pe_dag_for_audit = pd.read_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                    mye_pop_compiled_output_dir,
                                                    ModelYear + '_HHR_pop_by_dag.csv'))

        logging.info('Step 3.2.9 read in data processed by the MYE_APS_process through an existing csv')
        logging.info('WARNING - This is not the default way of reading this data!')
        logging.info('Did you mean to do that?')
    else:
        logging.info('Step 3.2.9 is calling the MYE_APS_process function in order to obtain 2018 population data')
        MYE_MSOA_pop = MYE_APS_process(census_and_by_lu_obj, la_level_adjustment_name, la_level_adjustment_output_dir)
        logging.info('Step 3.2.9 read in data processed by the MYE_APS_process function from internal memory')

        la_worker_df_import = MYE_MSOA_pop[0]
        la_nonworker_df_import = MYE_MSOA_pop[1]
        LA_ID = MYE_MSOA_pop[2]
        pe_dag_for_audit = MYE_MSOA_pop[3]

        LA_worker_control = la_worker_df_import[['2021_LA_name', '2021_LA_code', 'LA', 'Worker']]
        LA_worker_ge_control = la_worker_df_import[[
            '2021_LA_name', '2021_LA_code', 'LA', 'Male fte', 'Male pte', 'Female fte', 'Female pte']]
        LA_worker_s_control = la_worker_df_import[[
            '2021_LA_name', '2021_LA_code', 'LA', 'higher', 'medium', 'skilled']]
        LA_nonworker_control = la_nonworker_df_import[[
            '2021_LA_name', '2021_LA_code', 'LA', 'Non worker']]
        LA_nonworker_ag_control = la_nonworker_df_import[[
            '2021_LA_name', '2021_LA_code', 'LA', 'Children', 'M_16-74', 'F_16-74', 'M_75 and over', 'F_75 and over']]

    # Call the file containing Pe from step 3.2.5's outputs
    pe_df = pd.read_csv(os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                     mye_pop_compiled_output_dir,
                                     ModelYear + '_total_pop_vs_HHR_total.csv'))

    # seed_worker not exported as a file, so wll always need to read directly from a function 3.2.8 call
    # Whilst reading it in, can also read in HHpop variables
    call_3_2_8 = subsets_worker_nonworker(census_and_by_lu_obj, la_level_adjustment_name)
    seed_worker = call_3_2_8[0]
    HHpop_workers_LA = call_3_2_8[1]
    HHpop_nwkrs_ag_LA = call_3_2_8[2]
    hhpop_non_workers_d = call_3_2_8[3]
    z_2_la = call_3_2_8[4]

    pe_df = pe_df[['MSOA', 'Total_HHR']]
    pe_df = pd.merge(pe_df, z_2_la, how='left', on='MSOA')
    pe_df = pe_df.groupby('2021_LA_code')['Total_HHR'].sum().reset_index()

    LA_nonworker_control = LA_nonworker_control.rename(columns={'Non worker': 'nonworker'})
    LA_worker_ge_control = pd.melt(LA_worker_ge_control,
                                   id_vars=['2021_LA_name', '2021_LA_code', 'LA'],
                                   value_vars=['Male fte', 'Male pte', 'Female fte',
                                               'Female pte']).rename(columns={'variable': 'ge', 'value': 'worker'})
    # print(LA_worker_ge_control.head(5))
    LA_worker_s_control = pd.melt(LA_worker_s_control,
                                  id_vars=['2021_LA_name', '2021_LA_code', 'LA'],
                                  value_vars=['higher', 'medium',
                                              'skilled']).rename(columns={'variable': 's', 'value': 'worker'})
    # print(LA_worker_s_control.head(5))
    LA_nonworker_ag_control = pd.melt(LA_nonworker_ag_control,
                                      id_vars=['2021_LA_name', '2021_LA_code', 'LA'],
                                      value_vars=['Children', 'M_16-74', 'F_16-74', 'M_75 and over',
                                                  'F_75 and over']).rename(
        columns={'variable': 'ag', 'value': 'nonworker'})
    # print(LA_nonworker_ag_control.head(5))
    # print('\n')
    logging.info('number of worker (by ge): ')
    logging.info(LA_worker_ge_control.worker.sum())
    logging.info('number of worker (by s): ')
    logging.info(LA_worker_s_control.worker.sum())
    logging.info('number of nonworker: ')
    logging.info(LA_nonworker_ag_control.nonworker.sum())
    logging.info('number of hh pop: ')
    logging.info(LA_worker_control.Worker.sum() + LA_nonworker_control.nonworker.sum())

    ge = {
        'Male fte': 1,
        'Male pte': 2,
        'Female fte': 3,
        'Female pte': 4
    }

    s = {
        'higher': 1,
        'medium': 2,
        'skilled': 3
    }

    a = {
        'Children': 1,
        'M_16-74': 2,
        'F_16-74': 2,
        'M_75 and over': 3,
        'F_75 and over': 3
    }

    g = {
        'Children': 1,
        'M_16-74': 2,
        'F_16-74': 3,
        'M_75 and over': 2,
        'F_75 and over': 3
    }
    LA_worker_ge_control['ge'] = LA_worker_ge_control['ge'].map(ge)
    LA_worker_s_control['s'] = LA_worker_s_control['s'].map(s)
    LA_nonworker_ag_control['a'] = LA_nonworker_ag_control['ag'].map(a)
    LA_nonworker_ag_control['g'] = LA_nonworker_ag_control['ag'].map(g)

    seed_worker = seed_worker.merge(
        LA_ID, how='left', on='2021_LA_code').drop(columns={'2021_LA_name', '2021_LA_code'})

    seed_worker = seed_worker.rename(columns={"LA": "d", "people": "total"})
    seed_worker = seed_worker[['d', 'ge', 's', 'a', 'h', 't', 'n', 'total']]

    ctrl_ge = LA_worker_ge_control.copy()
    ctrl_ge = ctrl_ge.rename(
        columns={"LA": "d", "worker": "total"}).drop(columns={'2021_LA_name', '2021_LA_code'})

    ctrl_s = LA_worker_s_control.copy()
    ctrl_s = ctrl_s.rename(
        columns={"LA": "d", "worker": "total"}).drop(columns={'2021_LA_name', '2021_LA_code'})

    # Export seed_worker, then read it back in again. Failure to do so will result in the script crashing in ipfn
    # It is unclear why this occurs.
    seed_worker_name = 'seed_worker.csv'
    seed_worker_full_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                         la_level_adjustment_output_dir,
                                         seed_worker_name)
    seed_worker.to_csv(seed_worker_full_path, index=False)
    seed = pd.read_csv(seed_worker_full_path)

    ctrl_ge = ctrl_ge.groupby(['d', 'ge'])['total'].sum()
    ctrl_s = ctrl_s.groupby(['d', 's'])['total'].sum()

    aggregates = [ctrl_ge, ctrl_s]
    dimensions = [['d', 'ge'], ['d', 's']]

    print('IPFN process started')
    IPF = ipfn.ipfn(seed, aggregates, dimensions)
    seed = IPF.iteration()
    print('IPFN process complete')

    # Following 2 lines not used, but keeping so they can be dumped for QA later if required
    # Wk_ge = seed.groupby(['d', 'ge'])['total'].sum()
    # Wk_s = seed.groupby(['d', 's'])['total'].sum()

    HHpop_workers_LA = HHpop_workers_LA.merge(LA_ID, how='left', on='2021_LA_code').drop(columns={'2021_LA_name'})
    HHpop_workers_LA = HHpop_workers_LA.rename(columns={'LA': 'd'})
    furnessed_worker_LA = seed.copy()
    furnessed_worker_LA_iterator = zip(furnessed_worker_LA['d'],
                                       furnessed_worker_LA['ge'],
                                       furnessed_worker_LA['s'],
                                       furnessed_worker_LA['a'],
                                       furnessed_worker_LA['h'],
                                       furnessed_worker_LA['t'],
                                       furnessed_worker_LA['n'])
    furnessed_worker_LA['key'] = ['_'.join([str(d), str(ge), str(s), str(a), str(h), str(t), str(n)])
                                  for d, ge, s, a, h, t, n in furnessed_worker_LA_iterator]
    furnessed_worker_LA = furnessed_worker_LA[['key', 'total']]
    HHpop_workers_LA_iterator = zip(HHpop_workers_LA['d'],
                                    HHpop_workers_LA['ge'],
                                    HHpop_workers_LA['s'],
                                    HHpop_workers_LA['a'],
                                    HHpop_workers_LA['h'],
                                    HHpop_workers_LA['t'],
                                    HHpop_workers_LA['n'])
    HHpop_workers_LA['key'] = ['_'.join([str(d), str(ge), str(s), str(a), str(h), str(t), str(n)])
                               for d, ge, s, a, h, t, n in HHpop_workers_LA_iterator]
    HHpop_workers_LA = HHpop_workers_LA.merge(furnessed_worker_LA, how='left', on=['key'])

    HHpop_workers_LA['wkr_aj_factor'] = HHpop_workers_LA['total'] / HHpop_workers_LA['people']
    HHpop_workers_LA['wkr_aj_factor'] = HHpop_workers_LA['wkr_aj_factor'].fillna(1)

    # New df called aj_HHpop_workers_LA copy of
    #  HHpop_workers_LA[['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's', 'total']]
    aj_HHpop_workers_LA = HHpop_workers_LA.copy()
    aj_HHpop_workers_LA = aj_HHpop_workers_LA[['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's', 'total']]

    wkrs_aj_factor_LA = HHpop_workers_LA[['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's', 'wkr_aj_factor']]
    logging.info('worker currently {}'.format(HHpop_workers_LA.total.sum()))

    HHpop_nwkrs_ag_LA = HHpop_nwkrs_ag_LA.merge(LA_nonworker_ag_control, how='left', on=['2021_LA_code', 'a', 'g'])
    HHpop_nwkrs_ag_LA['nwkr_aj_factor'] = HHpop_nwkrs_ag_LA['nonworker'] / HHpop_nwkrs_ag_LA['people']

    nwkrs_aj_factor_LA = HHpop_nwkrs_ag_LA[['2021_LA_code', 'a', 'g', 'nwkr_aj_factor']]
    logging.info('non_worker currently {}'.format(HHpop_nwkrs_ag_LA.nonworker.sum()))

    # Fetches HHpop_non_workers_LA from Step 3.2.8. Merge on LA, a ,g with nwkrs_aj_factor_LA to get
    #  'nwkr_aj_factor' in. Resulting df is aj_HHpop_non_workers_LA with heading 'total' in it. This heading is
    #  the hhpop for nonworkers (i.e. column 'people') x 'nwkr_aj_factor'.
    aj_HHpop_non_workers_LA = pd.merge(hhpop_non_workers_d, nwkrs_aj_factor_LA,
                                       how='left', on=['2021_LA_code', 'a', 'g'])
    aj_HHpop_non_workers_LA['total'] = aj_HHpop_non_workers_LA['people'] * aj_HHpop_non_workers_LA['nwkr_aj_factor']

    # Append aj_HHpop_non_workers_LA to aj_HHpop_workers_LA to get full district level household pop with full
    #  dimensions. Groupby on d (for 'total'). Compare to MYPE on household population. Dump file as audit. First line
    #  of final Step 3.2.9 Audit bullet point. Groupby d, a ,g  (for 'total'). Compare to MYPE household population.
    #  Dump as audit. Second line of final Step 3.2.9 bullet point. All comparisons should be very near 0.
    audit_hhpop_by_d = aj_HHpop_workers_LA.copy()
    audit_hhpop_by_d = audit_hhpop_by_d.append(aj_HHpop_non_workers_LA)
    audit_hhpop_by_dag = audit_hhpop_by_d.copy()
    audit_hhpop_by_d = audit_hhpop_by_d.groupby('2021_LA_code')['total'].sum().reset_index()
    audit_hhpop_by_d = audit_hhpop_by_d.rename(columns={'total': 'Step_3.2.9_total_pop'})
    audit_hhpop_by_d = pd.merge(audit_hhpop_by_d, pe_df, how='left', on='2021_LA_code')
    audit_hhpop_by_d['%age_diff_in_pop'] = (audit_hhpop_by_d['Step_3.2.9_total_pop'] -
                                            audit_hhpop_by_d['Total_HHR']) / audit_hhpop_by_d['Total_HHR']
    audit_hhpop_by_d_max = audit_hhpop_by_d['%age_diff_in_pop'].max()
    audit_hhpop_by_d_min = audit_hhpop_by_d['%age_diff_in_pop'].min()
    audit_hhpop_by_d_mean = audit_hhpop_by_d['%age_diff_in_pop'].mean()
    audit_hhpop_by_d_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                         la_level_adjustment_output_dir,
                                         'Audits',
                                         'audit_' + ModelYear + '_HHpop_by_d.csv')
    audit_hhpop_by_d.to_csv(audit_hhpop_by_d_path, index=False)

    audit_hhpop_by_dag = audit_hhpop_by_dag.groupby(['2021_LA_code', 'a', 'g'])['total'].sum().reset_index()
    audit_hhpop_by_dag = audit_hhpop_by_dag.rename(columns={'total': 'Step_3.2.9_total_pop'})
    audit_hhpop_by_dag = pd.merge(audit_hhpop_by_dag, pe_dag_for_audit, how='left', on=['2021_LA_code', 'a', 'g'])
    audit_hhpop_by_dag['%age_diff_in_pop'] = (audit_hhpop_by_dag['Step_3.2.9_total_pop'] -
                                              audit_hhpop_by_dag['HHR_pop']) / audit_hhpop_by_dag['HHR_pop']
    audit_hhpop_by_dag_max = audit_hhpop_by_dag['%age_diff_in_pop'].max()
    audit_hhpop_by_dag_min = audit_hhpop_by_dag['%age_diff_in_pop'].min()
    audit_hhpop_by_dag_mean = audit_hhpop_by_dag['%age_diff_in_pop'].mean()
    audit_hhpop_by_dag_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                           la_level_adjustment_output_dir,
                                           'Audits',
                                           'audit_' + ModelYear + '_HHpop_by_d.csv')
    audit_hhpop_by_d.to_csv(audit_hhpop_by_d_path, index=False)

    audit_3_2_9_header = 'Audit for Step 3.2.9\nCreated ' + str(datetime.datetime.now())
    audit_3_2_9_text = '\n'.join(['The total 2018 population is currently: ' + str(HHpop_workers_LA.total.sum() +
                                                                                   HHpop_nwkrs_ag_LA.nonworker.sum()),
                                  'Total HHR workers currently {}'.format(HHpop_workers_LA.total.sum()),
                                  'Total non_workers currently {}'.format(HHpop_nwkrs_ag_LA.nonworker.sum()),
                                  'Comparing LA level HH population original to present (worker + non-worker):',
                                  '\tBy LA only:',
                                  '\t\tMax percentage difference: ' + str(audit_hhpop_by_d_max * 100) + '%',
                                  '\t\tMin percentage difference: ' + str(audit_hhpop_by_d_min * 100) + '%',
                                  '\t\tMean percentage difference: ' + str(audit_hhpop_by_d_mean * 100) + '%',
                                  '\tBy LA, age and gender:',
                                  '\t\tMax percentage difference: ' + str(audit_hhpop_by_dag_max * 100) + '%',
                                  '\t\tMin percentage difference: ' + str(audit_hhpop_by_dag_min * 100) + '%',
                                  '\t\tMean percentage difference: ' + str(audit_hhpop_by_dag_mean * 100) + '%',
                                  'All of the above should be equal (or very close) to 0.',
                                  'A full breakdown of the LA only data is presented in:',
                                  audit_hhpop_by_d_path,
                                  'A full breakdown of the LA, age and gender data is presented in:',
                                  audit_hhpop_by_dag_path])
    audit_3_2_9_content = '\n'.join([audit_3_2_9_header, audit_3_2_9_text])
    audit_3_2_9_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                    pop_with_full_dims_output_dir,
                                    'Audits',
                                    'Audit_3.2.9.txt')
    with open(audit_3_2_9_path, 'w') as text_file:
        text_file.write(audit_3_2_9_content)

    la_level_adjustment_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                            la_level_adjustment_output_dir)
    furnessed_data_filename = 'furnessed_2018Worker.csv'
    nwkrs_aj_factor_LA_filename = 'nwkrs_aj_factor_LA_' + ModelYear + '.csv'
    wkrs_aj_factor_LA_filename = 'wkrs_aj_factor_LA_' + ModelYear + '.csv'

    # Export files
    seed.to_csv(os.path.join(la_level_adjustment_path, furnessed_data_filename), index=False)
    nwkrs_aj_factor_LA.to_csv(os.path.join(la_level_adjustment_path, nwkrs_aj_factor_LA_filename), index=False)
    wkrs_aj_factor_LA.to_csv(os.path.join(la_level_adjustment_path, wkrs_aj_factor_LA_filename), index=False)
    census_and_by_lu_obj.state['3.2.9 verify district level worker and non-worker'] = 1
    logging.info('Step 3.2.9 completed')
    print('Step 3.2.9 completed')


def adjust_zonal_workers_nonworkers(census_and_by_lu_obj):
    logging.info('Running Step 3.2.10')
    print('Running Step 3.2.10')
    adjust_zonal_workers_nonworkers_name = 'adjust_zonal_workers_nonworkers'
    nomis_mye_2018_path = os.path.join(
        census_and_by_lu_obj.import_folder,
        'MYE 2018 ONS/2018_pop_process_inputs/nomis_2021_08_24_MYE_2018_sheet_2018MYE_only.csv')
    # Had to save a copy of the xlsx as a csv as read_excel is annoying and doesn't support xlsx files anymore!
    nomis_mye_2018 = pd.read_csv(nomis_mye_2018_path, skiprows=6)
    nomis_mye_2018 = nomis_mye_2018[['local authority: district / unitary (as of April 2021)', 'All Ages']]
    nomis_mye_2018 = nomis_mye_2018.rename(
        columns={'local authority: district / unitary (as of April 2021)': '2021_LA_Name',
                 'All Ages': 'MYE_pop'})

    # Call function for 3.2.8 to get HHpop_workers
    # (because 3.2.8 is a quick to run function vs the file read/write time)
    call_3_2_8 = subsets_worker_nonworker(census_and_by_lu_obj, adjust_zonal_workers_nonworkers_name)
    HHpop_workers = call_3_2_8[0]
    HHpop_non_workers = call_3_2_8[1]

    # Read wkrs_aj_factor_LA from csv dumped by 3.2.9 (because IPFN can take a long time to run)
    read_la_level_adjustment_output_dir = la_level_adjustment_output_dir
    wkrs_aj_factor_LA_filename = 'wkrs_aj_factor_LA_' + ModelYear + '.csv'
    nwkrs_aj_factor_LA_filename = 'nwkrs_aj_factor_LA_' + ModelYear + '.csv'
    wkrs_aj_factor_LA_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                          read_la_level_adjustment_output_dir,
                                          wkrs_aj_factor_LA_filename)
    nwkrs_aj_factor_LA_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                           read_la_level_adjustment_output_dir,
                                           nwkrs_aj_factor_LA_filename)

    wkrs_aj_factor_LA = pd.read_csv(wkrs_aj_factor_LA_path)
    nwkrs_aj_factor_LA = pd.read_csv(nwkrs_aj_factor_LA_path)

    # Read average uk hh occupancy from step 3.2.6
    uk_ave_hh_occ_lookup_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                             pop_with_full_dims_output_dir,
                                             'uk_average_hh_occupancy_by_t.csv')
    uk_ave_hh_occ_lookup = pd.read_csv(uk_ave_hh_occ_lookup_path)

    HHpop_workers = HHpop_workers.merge(wkrs_aj_factor_LA,
                                        how='left',
                                        on=['2021_LA_code', 'a', 'g', 'h', 'e', 't', 'n', 's'])
    HHpop_workers['aj_worker'] = HHpop_workers['people'] * HHpop_workers['wkr_aj_factor']
    # print(HHpop_workers.head(5))
    # print(HHpop_workers.people.sum())
    # print(HHpop_workers.aj_worker.sum())
    HHpop_workers = HHpop_workers.drop(columns={'people'})
    HHpop_workers = HHpop_workers.rename(columns={'aj_worker': 'people', 'wkr_aj_factor': 'scaling_factor'})

    HHpop_non_workers = HHpop_non_workers.merge(nwkrs_aj_factor_LA, how='left', on=['2021_LA_code', 'a', 'g'])
    HHpop_non_workers['aj_nonworker'] = HHpop_non_workers['people'] * HHpop_non_workers['nwkr_aj_factor']
    # print(HHpop_non_workers.head(5))
    # print(HHpop_non_workers.people.sum())
    # print(HHpop_non_workers.aj_nonworker.sum())
    HHpop_non_workers = HHpop_non_workers.drop(columns={'people'})
    HHpop_non_workers = HHpop_non_workers.rename(columns={'aj_nonworker': 'people', 'nwkr_aj_factor': 'scaling_factor'})

    hhpop_combined = HHpop_non_workers.copy()
    hhpop_combined = hhpop_combined.append(HHpop_workers, ignore_index=True)
    hhpop_combined = hhpop_combined.drop(columns=['properties', 'scaling_factor'])

    final_zonal_hh_pop_by_t = hhpop_combined.groupby(['MSOA', 'z', 't'])['people'].sum()
    final_zonal_hh_pop_by_t = final_zonal_hh_pop_by_t.reset_index()
    final_zonal_hh_pop_by_t_iterator = zip(final_zonal_hh_pop_by_t['z'], final_zonal_hh_pop_by_t['t'])
    final_zonal_hh_pop_by_t['z_t'] = ['_'.join([str(z), str(t)]) for z, t in final_zonal_hh_pop_by_t_iterator]

    zonal_properties_by_t = pd.read_csv(
        r"I:\NorMITs Land Use\test\Test_2018pop\2021-11-19 Notebook\Output\classifiedResProperty_MYEcompiled_2018.csv")
    zonal_properties_by_t_iterator = zip(zonal_properties_by_t['Zone'], zonal_properties_by_t['census_property_type'])
    zonal_properties_by_t['z_t'] = ['_'.join([str(z), str(t)]) for z, t in zonal_properties_by_t_iterator]

    final_zonal_hh_pop_by_t = pd.merge(final_zonal_hh_pop_by_t,
                                       zonal_properties_by_t,
                                       how='left',
                                       on='z_t')
    final_zonal_hh_pop_by_t['final_hh_occ'] = final_zonal_hh_pop_by_t['people'] / final_zonal_hh_pop_by_t['UPRN']
    final_zonal_hh_pop_by_t.loc[
        final_zonal_hh_pop_by_t['final_hh_occ'].isnull(),
        'final_hh_occ'] = final_zonal_hh_pop_by_t['t'].map(uk_ave_hh_occ_lookup.UK_average_hhocc)
    final_zonal_hh_pop_by_t['UPRN'].fillna(0, inplace=True)
    final_zonal_hh_pop_by_t.rename(columns={'UPRN': 'Properties'}, inplace=True)
    final_zonal_hh_pop_by_t = final_zonal_hh_pop_by_t[['MSOA', 'z', 't', 'people', 'Properties', 'final_hh_occ']]

    # Check zonal total vs MYE HHpop at zonal level min, max, mean
    logging.info('Total hhpop is now:')
    logging.info(hhpop_combined.people.sum())

    mye_folder = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                              mye_pop_compiled_output_dir)
    _MYE_2018Pop_MSOA_path = os.path.join(mye_folder, '2018_total_pop_vs_HHR_total.csv')
    mye_msoa_pop = pd.read_csv(_MYE_2018Pop_MSOA_path)
    mye_hhr_pop = mye_msoa_pop[['MSOA', 'Total_HHR']]
    logging.info('Original hhpop is:')
    logging.info(mye_hhr_pop.Total_HHR.sum())

    hhpop_combined_check_z = hhpop_combined.copy()
    hhpop_combined_check_z = hhpop_combined_check_z[['MSOA', 'people']]
    hhpop_combined_check_z = hhpop_combined_check_z.groupby(['MSOA']).sum()
    hhpop_combined_check_z = hhpop_combined_check_z.merge(mye_hhr_pop, how='outer', on='MSOA')

    hhpop_combined_check_la = hhpop_combined_check_z.copy()

    hhpop_combined_check_z['percentage_diff'] = (hhpop_combined_check_z['people'] / hhpop_combined_check_z[
        'Total_HHR']) - 1
    logging.info('Check zonal level totals:')
    logging.info('The min %age diff is ' + str(hhpop_combined_check_z['percentage_diff'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(hhpop_combined_check_z['percentage_diff'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(hhpop_combined_check_z['percentage_diff'].mean() * 100) + '%')

    hhpop_combined_pdiff_min = hhpop_combined_check_z.loc[
        hhpop_combined_check_z.percentage_diff.idxmin()]
    hhpop_combined_pdiff_max = hhpop_combined_check_z.loc[
        hhpop_combined_check_z.percentage_diff.idxmax()]
    hhpop_combined_pdiff_min = pd.DataFrame(hhpop_combined_pdiff_min).transpose()
    hhpop_combined_pdiff_max = pd.DataFrame(hhpop_combined_pdiff_max).transpose()
    hhpop_combined_pdiff_extremes = hhpop_combined_pdiff_min.append(hhpop_combined_pdiff_max)
    logging.info(hhpop_combined_pdiff_extremes)

    # Check LA total vs MYE HHpop - there should be 0% variance
    la_2_z = hhpop_combined.copy()
    la_2_z = la_2_z[['2021_LA_Name', 'MSOA']].drop_duplicates().reset_index().drop(columns=['index'])
    hhpop_combined_check_la = pd.merge(hhpop_combined_check_la, la_2_z, how='left', on='MSOA')
    hhpop_combined_check_la = hhpop_combined_check_la.drop(columns=['MSOA'])
    hhpop_combined_check_la = hhpop_combined_check_la.groupby(['2021_LA_Name']).sum()
    hhpop_combined_check_la['percentage_diff'] = (hhpop_combined_check_la['people'] / hhpop_combined_check_la[
        'Total_HHR']) - 1
    logging.info('Check district level totals:')
    logging.info('The min %age diff is ' + str(hhpop_combined_check_la['percentage_diff'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(hhpop_combined_check_la['percentage_diff'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(hhpop_combined_check_la['percentage_diff'].mean() * 100) + '%')

    hhpop_combined_check_z_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                               further_adjustments_output_dir,
                                               'Audits',
                                               ModelYear + '_hhpop_combined-check_by_z.csv')
    hhpop_combined_check_la_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                further_adjustments_output_dir,
                                                'Audits',
                                                ModelYear + '_hhpop_combined-check_by_d.csv')
    hhpop_combined_check_z.to_csv(hhpop_combined_check_z_path)
    hhpop_combined_check_la.to_csv(hhpop_combined_check_la_path)

    audit_3_2_10_header = 'Audit for Step 3.2.10\nCreated ' + str(datetime.datetime.now())
    audit_3_2_10_text = '\n'.join(['The total 2018 population is currently: ' + str(hhpop_combined.people.sum()),
                                   'Comparing z and d level HH population original to present:',
                                   '\tBy zone (z) - values will vary from 0. See Tech Note for expected values:',
                                   '\t\tMax percentage difference: ' + str(
                                       hhpop_combined_check_z['percentage_diff'].max() * 100) + '%',
                                   '\t\tMin percentage difference: ' + str(
                                       hhpop_combined_check_z['percentage_diff'].min() * 100) + '%',
                                   '\t\tMean percentage difference: ' + str(
                                       hhpop_combined_check_z['percentage_diff'].mean() * 100) + '%',
                                   '\tBy LA (d) - values should not vary significantly from 0:',
                                   '\t\tMax percentage difference: ' + str(
                                       hhpop_combined_check_la['percentage_diff'].max() * 100) + '%',
                                   '\t\tMin percentage difference: ' + str(
                                       hhpop_combined_check_la['percentage_diff'].min() * 100) + '%',
                                   '\t\tMean percentage difference: ' + str(
                                       hhpop_combined_check_la['percentage_diff'].mean() * 100) + '%',
                                   'A full zonal breakdown of the data is presented in:',
                                   hhpop_combined_check_z_path,
                                   'A full district breakdown of the data is presented in:',
                                   hhpop_combined_check_la_path])
    audit_3_2_10_content = '\n'.join([audit_3_2_10_header, audit_3_2_10_text])

    running_dir_3_2_10 = os.getcwd()
    os.chdir(os.path.join(census_and_by_lu_obj.out_paths['write_folder'], further_adjustments_output_dir))
    hhpop_combined_pdiff_extremes.to_csv('zonal_hhpop_extreme_percentage_diffs.csv')
    final_zonal_hh_pop_by_t.to_csv(ModelYear + '_final_zonal_hh_pop_by_t.csv')
    audit_3_2_10_path = os.path.join('Audits', 'Audit_3.2.10.txt')
    with open(audit_3_2_10_path, 'w') as text_file:
        text_file.write(audit_3_2_10_content)
    os.chdir(running_dir_3_2_10)

    # Now call 3.2.11 directly from 3.2.10.
    # This allows 3.2.10 to pass 3.2.11 to big main df directly and
    # read the output back in and merge it to create the final output.
    logging.info('Step 3.2.10 is calling step 3.2.11 to generate CER pop data')
    print('Step 3.2.10 is calling Step 3.2.11')
    expanded_cer_pop = process_cer_data(census_and_by_lu_obj, hhpop_combined, la_2_z)
    logging.info('Step 3.2.11 completed, returning to step 3.2.10')
    print('Step 3.2.10 has completed its call of Step 3.2.11')

    # Append CER to the end of the HHpop table (with CER t=8)
    # So you have zaghetns population (i.e. no A - already dropped)
    # Dump this to compressed file in the pycharm script
    all_pop = hhpop_combined.copy()
    all_pop = all_pop.drop(columns=['2021_LA_code'])
    cer_pop_expanded = expanded_cer_pop.rename(columns={'Zone': 'z', 'zaghetns_CER': 'people'})
    all_pop = all_pop.append(cer_pop_expanded)

    all_pop_by_d_groupby_cols = ['2021_LA_Name']
    all_pop_by_d = all_pop.groupby(all_pop_by_d_groupby_cols)['people'].sum().reset_index()
    logging.info('Total pop (including CER) now:')
    logging.info(all_pop_by_d.people.sum())

    # Check LA level pop against MYE
    check_all_pop_by_d = all_pop_by_d.copy()
    check_all_pop_by_d = pd.merge(check_all_pop_by_d, nomis_mye_2018, on='2021_LA_Name', how='left')
    # Remove all the pesky 1000 separators from the string interpreted numerical columns!
    check_all_pop_by_d.replace(',', '', regex=True, inplace=True)
    check_all_pop_by_d['MYE_pop'] = check_all_pop_by_d['MYE_pop'].astype(int)
    check_all_pop_by_d['pop_deviation'] = (check_all_pop_by_d['people'] / check_all_pop_by_d['MYE_pop']) - 1
    logging.info('The min %age diff is ' + str(check_all_pop_by_d['pop_deviation'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(check_all_pop_by_d['pop_deviation'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(check_all_pop_by_d['pop_deviation'].mean() * 100) + '%')
    logging.info('The overall deviation is ' + str(
        check_all_pop_by_d['people'].sum() - check_all_pop_by_d['MYE_pop'].sum()) + ' people')
    check_all_pop_by_d_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                           further_adjustments_output_dir,
                                           'Audits',
                                           'check_final_' + ModelYear + '_pop_by_d.csv')
    check_all_pop_by_d.to_csv(check_all_pop_by_d_path, index=False)

    # Also groupby this output by removing t to get zaghens population.
    # Dump this to compressed file in the pycharm script
    all_pop_by_t_groupby_cols = all_pop.columns.values.tolist()
    all_pop_by_t_groupby_cols = [x for x in all_pop_by_t_groupby_cols if x != 't' and x != 'people']
    all_pop_by_t = all_pop.groupby(all_pop_by_t_groupby_cols)['people'].sum().reset_index()

    # Auditing the bit of Step 3.2.11 that is carried out directly in the Step 3.2.10 function
    audit_3_2_11_header = '\n'.join(['Audit for the parts of Step 3.2.11 carried out directly by Step 3.2.10',
                                     'Created ' + str(datetime.datetime.now())])
    audit_3_2_11_text = '\n'.join(['Note that the audit of CER pop is carried out in the Step 3.2.11 Audit directory.',
                                   'The total ' + ModelYear + ' population at the end of the running process is:',
                                   '\t' + str(all_pop_by_d['people'].sum()),
                                   'Checking final district total population against MYE district population:',
                                   '\tThe min %age diff is ' + str(
                                       check_all_pop_by_d['pop_deviation'].min() * 100) + '%',
                                   '\tThe max %age diff is ' + str(
                                       check_all_pop_by_d['pop_deviation'].max() * 100) + '%',
                                   '\tThe mean %age diff is ' + str(
                                       check_all_pop_by_d['pop_deviation'].mean() * 100) + '%',
                                   'The overall deviation is ' + str(
                                       check_all_pop_by_d['people'].sum() - check_all_pop_by_d[
                                           'MYE_pop'].sum()) + ' people',
                                   'All of the above values should be equal (or close) to 0.',
                                   'A full breakdown of the ' + ModelYear + 'population by d can be found at:',
                                   check_all_pop_by_d_path,
                                   'The Step 3.2.11 Audits directory is located here:',
                                   os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                cer_output_dir),
                                   'The Step 3.2.11 main audit file should be obvious in it.'])
    audit_3_2_11_content = '\n'.join([audit_3_2_11_header, audit_3_2_11_text])
    audit_3_2_11_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                     further_adjustments_output_dir,
                                     'Audits',
                                     'Audit_3.2.11.txt')
    with open(audit_3_2_11_path, 'w') as text_file:
        text_file.write(audit_3_2_11_content)


    # Dump outputs
    pre_3_2_10_dir = os.getcwd()
    output_3_2_10_dir = os.path.join(census_and_by_lu_obj.out_paths['write_folder'], further_adjustments_output_dir)
    os.chdir(output_3_2_10_dir)
    logging.info('Dumping final outputs...')
    compress.write_out(hhpop_combined, ModelYear + '_household_population_processed')
    logging.info('HH pop dumped')
    compress.write_out(all_pop, ModelYear + '_total_population')
    logging.info('Total pop (by z, a, g, h, e, t, n, s) dumped')
    compress.write_out(all_pop_by_t, ModelYear + '_total_population_independent_of_property_type')
    logging.info('Total pop (by z, a, g, h, e, n, s) dumped')
    os.chdir(pre_3_2_10_dir)

    census_and_by_lu_obj.state['3.2.10 adjust zonal pop with full dimensions'] = 1
    logging.info('Step 3.2.10 completed')
    logging.info('If undertaking a full run through of the 2018 Base Year LU process,')
    logging.info('then this should have been the last function to run.')
    logging.info('So the 2018 Base Year is DONE!')
    print('Step 3.2.10 completed')
    print('So, in theory, all steps have been run (Step 3.2.11 via Step 3.2.10), so we are')
    print('DONE!')


def process_cer_data(census_and_by_lu_obj, hhpop_combined_from_3_2_10, la_2_z_from_3_2_10):
    # This function should ONLY be called by 3.2.10
    logging.info('Running Step 3.2.11')
    print('Running Step 3.2.11')

    # Read the total pop data direct from 3.2.10 as it is ~26 million lines
    # In which case, call la_2_z from it too
    # Read MYE_MSOA_pop from file as it is small (produced by MYE_APS_process sub-function of 3.2.5)

    # Subtract HHpop from MYE total to get CER
    mye_folder = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                              mye_pop_compiled_output_dir)
    _MYE_2018Pop_MSOA_path = os.path.join(mye_folder, '2018_total_pop_vs_HHR_total.csv')
    mye_pop = pd.read_csv(_MYE_2018Pop_MSOA_path)
    cer_pop = hhpop_combined_from_3_2_10.copy()
    cer_pop = cer_pop[['MSOA', 'people']]
    cer_pop = cer_pop.groupby(['MSOA']).sum()
    cer_pop = pd.merge(cer_pop, mye_pop, how='left', on='MSOA')
    cer_pop['CER_pop'] = cer_pop['Total_Pop'] - cer_pop['people']

    # Suppress negative CER values to 0
    cer_pop['CER_pop'] = np.where(cer_pop['CER_pop'] < 0, 0, cer_pop['CER_pop'])

    # Use CER as weights to distribute LA level CER
    cer_pop = pd.merge(cer_pop, la_2_z_from_3_2_10, how='left', on='MSOA')
    cer_pop_la = cer_pop.copy()
    cer_pop_la = cer_pop_la.drop(columns=['MSOA'])
    cer_pop_la = cer_pop_la.groupby(['2021_LA_Name']).sum()
    cer_pop_la['Total_CER'] = cer_pop_la['Total_Pop'] - cer_pop_la['Total_HHR']
    cer_pop_la = cer_pop_la.reset_index()
    cer_pop_la = cer_pop_la.rename(columns={'CER_pop': 'CER_weight_denom'})
    cer_pop_la = cer_pop_la[['2021_LA_Name', 'CER_weight_denom', 'Total_CER']]
    cer_pop = pd.merge(cer_pop, cer_pop_la, how='left', on='2021_LA_Name')
    cer_pop['CER_weight'] = cer_pop['CER_pop'] / cer_pop['CER_weight_denom']
    cer_pop['Zonal_CER'] = cer_pop['Total_CER'] * cer_pop['CER_weight']

    uk_ave_pop_df = hhpop_combined_from_3_2_10.copy()
    # Technically this should be done after the groupby,
    # but setting t = 8 here is still produces the desired output
    # and saves having to get a 'new' 't' column in the right place afterwards
    uk_ave_pop_df['t'] = 8
    uk_ave_pop_df = uk_ave_pop_df.groupby(['a', 'g', 'h', 'e', 't', 'n', 's'])['people'].sum()
    uk_ave_pop_df = pd.DataFrame(uk_ave_pop_df).reset_index()
    uk_total_pop = uk_ave_pop_df.people.sum()
    uk_ave_pop_df['people'] = uk_ave_pop_df['people'] / uk_total_pop
    uk_ave_pop_df.rename(columns={'people': 'aghetns_pop_prop'}, inplace=True)
    logging.info('a, g, h, e, t, n, s population proportions for t=8 (CER pop) should sum to 1.')
    if uk_ave_pop_df['aghetns_pop_prop'].sum() == 1:
        logging.info('Here they sum to 1 as expected.')
    else:
        logging.info('!!!! WARNING !!!!')
        logging.info('Proportions do not sum to 1! Instead they sum to:')
        logging.info(uk_ave_pop_df['aghetns_pop_prop'].sum())

    # Expand cer_pop by uk_ave_pop_df
    cer_pop_expanded = cer_pop.copy()
    uk_ave_pop_df_expander = uk_ave_pop_df.copy()
    cer_pop_expanded['key'] = 0
    uk_ave_pop_df_expander['key'] = 0
    cer_pop_expanded = pd.merge(cer_pop_expanded, uk_ave_pop_df_expander, on='key').drop(columns=['key'])
    cer_pop_expanded['zaghetns_CER'] = cer_pop_expanded['Zonal_CER'] * cer_pop_expanded['aghetns_pop_prop']
    logging.info('Check expanded CER pop matches zonal CER pop.')
    logging.info('Expanded CER pop is:')
    logging.info(cer_pop_expanded['zaghetns_CER'].sum())
    logging.info('Zonal CER pop is:')
    logging.info(cer_pop['Zonal_CER'].sum())
    cer_pop_expanded = cer_pop_expanded[
        ['MSOA', 'Zone', 'a', 'g', 'h', 'e', 't', 'n', 's', '2021_LA_Name', 'zaghetns_CER']]

    # Dump cer_pop_expanded to compressed file for QA purposes
    cer_output_dir_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'], cer_output_dir)
    cer_output_full_path = os.path.join(cer_output_dir_path, ModelYear + '_CER_population_expanded')
    compress.write_out(cer_pop_expanded, cer_output_full_path)

    # Auditing
    audit_3_2_11_header = 'Audit for Step 3.2.11\nCreated ' + str(datetime.datetime.now())
    audit_3_2_11_text = '\n'.join(['The total CER population is: ' + str(cer_pop_expanded.zaghetns_CER.sum()),
                                   'All other logging for this step is carried out in the Step 3.2.10 audit directory.',
                                   'This is because the script for Step 3.2.10 carries out all of the processing that',
                                   'is assigned to Step 3.2.11 in the Technical Note beyond the creation of CER pop.',
                                   'The Step 3.2.10 Audits directory is located here:',
                                   os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                                further_adjustments_output_dir),
                                   'The Step 3.2.11 (in Step 3.2.10) main audit file should be obvious in it.'])
    audit_3_2_11_content = '\n'.join([audit_3_2_11_header, audit_3_2_11_text])
    audit_3_2_11_path = os.path.join(census_and_by_lu_obj.out_paths['write_folder'],
                                     cer_output_dir,
                                     'Audits',
                                     'Audit_3.2.11.txt')
    with open(audit_3_2_11_path, 'w') as text_file:
        text_file.write(audit_3_2_11_content)

    # Flagging and logging
    census_and_by_lu_obj.state['3.2.11 process CER data'] = 1
    logging.info('Step 3.2.11 completed')
    print('Step 3.2.11 completed')

    return cer_pop_expanded
