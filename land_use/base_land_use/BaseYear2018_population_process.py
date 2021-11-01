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
import geopandas as gpd
from land_use.utils import file_ops as utils
from land_use.utils import compress
import land_use.lu_constants as consts
import logging

# Shapefile locations
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_lsoaRef = _default_zone_ref_folder + 'UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_ladRef = _default_zone_ref_folder + 'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
_default_msoaRef = _default_zone_ref_folder + 'UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'

# This function doesn't seem to actually do anything?
def copy_addressbase_files(census_and_by_lu_obj):
    """
    Copy the relevant ABP files from import drive to census_and_by_lu_obj.home_folder for use in later functions.
    census_and_by_lu_obj: base year land use object
    """
    #dest = census_and_by_lu_obj.home_folder
    #files = pd.read_csv(census_and_by_lu_obj.addressbase_path_list)
    print('no longer copying into default iter folder')

    #for file in files.FilePath:
    #    try:
    #        shutil.copy(file, dest)
    #        print("Copied over file into default iter folder: " + file)
    #    except IOError:
    #        print("File not found")

    census_and_by_lu_obj.state['3.2.1 read in core property data'] = 1
    logging.info('Step 3.2.1 completed')

def land_use_formatting(census_and_by_lu_obj):
    """
    Combines all flats into one category, i.e. property types = 4,5,6.
    """
    # 1.Combine all flat types. Sort out flats on the landuse side; actually there's no 7
    land_use = compress.read_in(census_and_by_lu_obj.home_folder + '/landUseOutput' +
                                census_and_by_lu_obj.model_zoning + '_withCommunal')
    land_use['property_type'] = land_use['property_type'].map(consts.PROPERTY_TYPE)
    logging.info('Population currently {}'.format(land_use.people.sum()))
    compress.write_out(land_use,
                       census_and_by_lu_obj.home_folder + '/landUseOutput' +
                       census_and_by_lu_obj.model_zoning + '_flats_combined')

    census_and_by_lu_obj.state['3.2.2 property type mapping'] = 1
    logging.info('Step 3.2.2 completed')

    return land_use

def filled_properties(census_and_by_lu_obj):
    """
    This is a rough account for unoccupied properties using KS401UK at LSOA level to infer whether the properties
    have any occupants.
        census_and_by_lu_obj: base year land use object, which includes the following paths:
            zone_translation_path: correspondence between LSOAs and the zoning system (default MSOA)
            KS401path: csv file path for the census KS401 table
    """
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
    filled_properties_df['Prob_DwellsFilled'] = filled_properties_df['Filled_Dwells'] / \
                                                filled_properties_df['Total_Dwells']
    filled_properties_df = filled_properties_df.drop(columns={'Filled_Dwells', 'Total_Dwells'})

    # The above filled properties probability is based on E+W so need to join back to Scottish MSOAs
    uk_msoa = gpd.read_file(_default_msoaRef)[['msoa11cd']].rename(columns={'msoa11cd': 'msoaZoneID'})
    filled_properties_df = uk_msoa.merge(filled_properties_df, on='msoaZoneID', how='outer')
    filled_properties_df = filled_properties_df.fillna(1)  # default to all Scottish properties being occupied
    filled_properties_df.to_csv('ProbabilityDwellfilled.csv', index=False)

    census_and_by_lu_obj.state['3.2.3 filled property adjustment'] = 1  # record that this process has been run
    logging.info('Step 3.2.3 completed')
    return filled_properties_df

def apply_household_occupancy(census_and_by_lu_obj, do_import=False, write_out=True):
    """
    Import household occupancy data and apply to property data.
    TODO: want to be able to run at LSOA level when point correspondence is done.
    TODO: Folders for outputs to separate this process from the household classification
    """
    if do_import:
        balanced_cpt_data = pd.read_csv('UKHouseHoldOccupancy2011.csv')
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
        probability_filled = pd.read_csv(census_and_by_lu_obj.home_folder + '/ProbabilityDwellfilled.csv')
        balanced_cpt_data = balanced_cpt_data.merge(probability_filled, how='outer', on='msoaZoneID')
        balanced_cpt_data['household_occupancy'] = balanced_cpt_data['household_occupancy'] * \
                                                   balanced_cpt_data['Prob_DwellsFilled']
        balanced_cpt_data = balanced_cpt_data.drop(columns={'Prob_DwellsFilled'})
        balanced_cpt_data.to_csv('UKHouseHoldOccupancy2011.csv', index=False)

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
    else:
        print('Some LAD zones not accounted for')

    # Read in HOPS growth data
    balanced_cpt_data = balanced_cpt_data.drop(['ladZoneID', 'objectid'], axis=1)
    hops_path = census_and_by_lu_obj.import_folder + 'HOPs/hops_growth_factors.csv'
    hops_growth = pd.read_csv(hops_path)[['Area code', '11_to_18']]

    # Uplift the figures to 2018
    balanced_cpt_data = balanced_cpt_data.merge(hops_growth,
                                                how='left', left_on='lad17cd',
                                                right_on='Area code').drop('Area code', axis=1).reset_index(drop=True)

    balanced_cpt_data['household_occupancy_18'] = balanced_cpt_data['household_occupancy'] * \
                                                  (1 + balanced_cpt_data['11_to_18'])
    trim_cols = ['msoaZoneID', 'census_property_type', 'household_occupancy_18', 'ho_type']
    balanced_cpt_data = balanced_cpt_data[trim_cols]

    # Read in all res property for the level of aggregation
    print('Reading in AddressBase extract')
    # addressbase_extract_path = \
    #     census_and_by_lu_obj.home_folder + '/allResProperty' + census_and_by_lu_obj.model_zoning + 'Classified.csv'
    addressbase_extract_path = consts.ALL_RES_PROPERTY_PATH + '/allResProperty' + \
                               census_and_by_lu_obj.model_zoning + 'Classified.csv'
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

        # Create folder for exports
        arp_msoa_audit = all_res_property.groupby('ZoneID')['population'].sum().reset_index()
        hpa_folder = 'Hops Population Audits'
        utils.create_folder(hpa_folder)
        arp_msoa_audit.to_csv(hpa_folder + '/' + census_and_by_lu_obj.model_zoning +
                              '_population_from_2018_hops.csv', index=False)
        if write_out:
            all_res_property.to_csv('classifiedResProperty' + census_and_by_lu_obj.model_zoning + '.csv', index=False)

        census_and_by_lu_obj.state['3,2,4 household occupancy adjustment'] = 1  # record that this process has been run
        logging.info('Step 3.2.4 completed')
        return all_res_property

    else:
        print("No support for this zoning system")  # only the MSOA zoning system is supported at the moment

# Sub-function used by apply_household_occupancy. Not called directly by census_and_by_lu.py
def lsoa_census_data_prep(dat_path, population_tables, property_tables, geography=_default_lsoaRef):
    """
    This function prepares the census data by picking fields out of the census csvs.
    Computes the ratio between the population and number of properties to return the household occupancy.

    dat_path: location of the census data
    census_tables: list of the csv file names to import
    geography: file path for geometry, defaults to LSOA
    """

    def _read_census_table(census_tables, table_type):
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
    return household_occupancy

# Sub-function used by apply_household_occupancy. Not called directly by census_and_by_lu.py
# TODO: improve the docstring here
def balance_missing_hops(census_and_by_lu_obj, cpt_data, grouping_col='msoaZoneID'):
    """
    # TODO: Replace global with LAD or Country - likely to be marginal improvements. Currently UK-wide
    This resolves the  msoa/lad household occupancy
    """
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

    return cpt_data

# Sub-sub-function used by apply_household_occupancy, called by balance_missing_hops.
# Not called directly by census_and_by_lu.py
def zone_up(census_and_by_lu_obj, cpt_data, grouping_col='msoaZoneID'):
    """
    Function to raise up a level of spatial aggregation & aggregate at that level, then bring new factors back down
    # TODO: Might be nice to have this zone up any level of zonal aggregation
    Raise LSOA to MSOA for spatial aggregation
    """
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

    return cpt_data

# Sub-sub-(sub)-function used by apply_household_occupancy, called by balance_missing_hops (and zone_up).
# Not called directly by census_and_by_lu.py
def aggregate_cpt(cpt_data, grouping_col=None):
    """
    Take some census property type data and return hops totals
    """
    if not grouping_col:
        cpt_data = cpt_data.loc[:, ['census_property_type', 'population', 'properties']]
        agg_data = cpt_data.groupby('census_property_type').sum().reset_index()
        agg_data['household_occupancy'] = agg_data['population'] / agg_data['properties']
    else:
        cpt_data = cpt_data.loc[:, ['census_property_type', 'population', 'properties', grouping_col]]
        agg_data = cpt_data.groupby(['census_property_type', grouping_col]).sum().reset_index()
        agg_data['household_occupancy'] = agg_data['population'] / agg_data['properties']

    return agg_data
