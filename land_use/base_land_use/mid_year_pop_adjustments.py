# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:58:57 2021

@author: ESRIAdmin

Mid-year population uplift
- get MYPE

#TODO: change to lowercase for functions
once run the source data should be ready for other functions
# TODO: change the RD06 to category 4 = 'flats' in next iter
# TODO: change 'ONS reports 27.4M in 2017' to be VOA based - think this is done
# TODO: take into account classifications for communal establishments
# TODO: change values/characters to enumerations 
 1. Work out communal %s per MSOA for males and females in each MSOA
 2. Uplift everything to 2018 using ONS Mid-year population estimates
 3. Adjust employment using GB control
 4. Adjust SOC lad level then gb level
 5. Control to LAD Employment
 6. Control to NS-SEC 2018 
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import gc
import logging
from land_use.utils import compress

# Outputs from previous steps
_default_communal_2011 = '/CommunalEstablishments/MSOACommunalEstablishments2011.csv'
_default_landuse_2011 = '/landUseOutputMSOA_withCommunal'
_landuse_segments = '/landUseOutputMSOA_NS_SEC_SOC'

# Zones and shapefiles
_default_lad_translation = 'Export/lad_to_msoa/lad_to_msoa.csv'
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_ladRef = _default_zone_ref_folder + 'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'

# Imports
_mype_females = '/MYE 2018 ONS/2018_MidyearMSOA/MYEfemales_2018.csv'
_mype_males = 'MYE 2018 ONS/2018_MidyearMSOA/MYEmales_2018.csv'
_mypeScot_females = 'MYE 2018 ONS/2018_MidyearMSOA/Females_Scotland_2018.csv'
_mypeScot_males = 'MYE 2018 ONS/2018_MidyearMSOA/Males_Scotland_2018.csv'
_country_control = 'NPR Segmentation/processed data/Country Control 2018/nomis_CountryControl.csv'
_gb_soc_totals = 'NPR Segmentation/raw data and lookups/LAD labour market data/nomis_SOCGBControl.csv'
_emp_controls = 'NPR Segmentation/raw data and lookups/LAD labour market data/Nomis_lad_EconomicActivity3.csv'
_ladsoc_control = 'NPR Segmentation/raw data and lookups/LAD labour market data/nomis_lad_SOC2018_constraints.csv'


def format_scottish_mype(by_lu_obj):
    """
    getting Scottish MYPE into the right format - 'melt' to get columns as rows, then rename them
    This should be a standard from any MYPE in the future segmented into females and males.
    Relies on the LAD to MSOA translation output from NorMITs Synthesiser (_default_lad_translation)
    
    Parameters
    ----------

    Returns
    ----------
    Scot_adjust- one formatted DataFrame of new Scottish MYPE including population
    split by age and gender by MSOA
    """
    land_use_segments = compress.read_in(by_lu_obj.home_folder + _landuse_segments)

    # Translation from LAD to MSOAs
    lad_translation = pd.read_csv(by_lu_obj.zones_folder + _default_lad_translation)
    lad_translation = lad_translation.rename(columns={'lad_zone_id': 'ladZoneID'})
    lad_cols = ['objectid', 'lad17cd']
    uk_lad = gpd.read_file(_default_ladRef)
    uk_lad = uk_lad.loc[:, lad_cols]

    scot_females = pd.read_csv(by_lu_obj.import_folder + _mypeScot_females)
    scot_males = pd.read_csv(by_lu_obj.import_folder + _mypeScot_males)
    scot_mype = scot_males.append(scot_females)
    scot_mype = scot_mype.rename(columns={'Area code': 'lad17cd'})
    scot_mype = pd.melt(scot_mype, id_vars=['lad17cd', 'Gender'], value_vars=['under 16', '16-74', '75 or over'])
    scot_mype = scot_mype.rename(columns={'variable': 'Age', 'value': '2018pop'})
    scot_mype.loc[scot_mype['Age'] == 'under 16', 'Gender'] = 'Children'
    scot_mype['2018pop'].sum()

    scot_lad = scot_mype.merge(uk_lad, on='lad17cd')
    scot_lad = scot_lad.rename(columns={'objectid': 'ladZoneID'})
    scot_msoa = scot_lad.merge(lad_translation, on='ladZoneID')

    # final stage of the translation from LAD to MSOA for Scotland
    scot_msoa['people2018'] = scot_msoa['2018pop'] * scot_msoa['lad_to_msoa']
    scot_msoa = scot_msoa.drop(columns={'overlap_type', 'lad_to_msoa', 'msoa_to_lad',
                                        '2018pop', 'lad17cd', 'ladZoneID'}).rename(columns={'msoa_zone_id': 'ZoneID'})
    # TODO: TO get back log and find where has gone wrong for all the null values
    # Temp solution to get rid of `null records- need to dig where has gone wrong to have all these null values though
    # total is not affected
    land_use_segments = land_use_segments.dropna()
    scotland_use = land_use_segments[land_use_segments.ZoneID.str.startswith('S')]

    scotland_use_grouped = scotland_use.groupby(by=['ZoneID', 'Age', 'Gender'],
                                                as_index=False).sum().drop(columns={'area_type',
                                                                                    'household_composition',
                                                                                    'property_type'})

    scot = scotland_use_grouped.merge(scot_msoa, how='outer', on=['ZoneID', 'Gender', 'Age'])
    scot['pop_factor'] = scot['people2018'] / scot['people']
    scot['newpop'] = scot['people'] * scot['pop_factor']
    scot = scot.drop(columns={'people'})
    scottish_mype = scotland_use.merge(scot, on=['ZoneID', 'Gender', 'Age'])
    scottish_mype['newpop'] = scottish_mype['people'] * scottish_mype['pop_factor']
    scottish_mype = scottish_mype.drop(columns={'people', 'people2018', 'pop_factor'}).rename(columns={'newpop': 'pop'})
    scottish_mype = scottish_mype[['ZoneID', 'Gender', 'Age', 'pop']]

    print('The adjusted MYPE/future year population for Scotland is', scottish_mype['pop'].sum() / 1000000, 'M')

    return scottish_mype


def get_ew_population(by_lu_obj):
    """
    Could be MYPE or future years population, function checks the format
    Change the path for mype if the population is for future years
    
    Parameters
    ----------

    Returns
    ----------
    :
        DataFrame containing formatted population ready to be joined to Census segmentation and 
        into sort_communal_output function
    """
    print('Reading in new EW population data')
    mype_males = pd.read_csv(by_lu_obj.import_folder + _mype_males)
    mype_females = pd.read_csv(by_lu_obj.import_folder + _mype_females)

    mype = mype_males.append(mype_females)
    mype = mype.rename(columns={'Area Codes': 'ZoneID'})
    mype = pd.melt(mype, id_vars=['ZoneID', 'gender'], value_vars=['under_16', '16-74', '75 or over'])
    mype = mype.rename(columns={'variable': 'Age', 'value': '2018pop'})
    mype = mype.replace({'Age': {'under_16': 'under 16'}})
    mype = mype.replace({'gender': {'male': 'Male', 'female': 'Females'}})

    # children are a 'gender' in NTEM, so need to sum the two rows
    mype.loc[mype['Age'] == 'under 16', 'gender'] = 'Children'
    mype = mype.groupby(['ZoneID', 'Age', 'gender']).sum().reset_index()
    mype = mype.rename(columns={'gender': 'Gender', '2018pop': 'pop'})

    return mype


def get_fy_population():
    """
    Imports fy population
    placeholder, potentially to import code developed by Chris/Liz
    """


# TODO: include block commenting
def sort_communal_uplift(by_lu_obj, midyear=True):
    """
    Imports a csv of Communal Establishments 2011 and uses MYPE to uplift to MYPE (2018 for now)
    First this function takes the communal establishments and adjust for the people living
    in those property types.
    It is calculated using the splits for people by type of establishment, age and
    gender using LAD data available. MSOA totals by type are then used to work 
    out the gender and age of the people per each MSOA.
    This is then compared to the total population living in that MSOA in 2011 to
    work out the %.
    The percentage is then used to calculate the new communal establishment people
    from 2018 MYE.
    Midyear = True uses the MYPE population, else can use total population supplied,
    e.g. for future years
    Parameters
    ----------
    midyear
    by_lu_obj:
        Base year land use object.
    ----------
    Returns
    ----------
    Uplifted Communal:
        DataFrame containing Communal Establishments according to the MYPE (2018).
    """
    communal = pd.read_csv(by_lu_obj.home_folder + _default_communal_2011).rename(columns={'people': 'communal'})
    census_output = compress.read_in(by_lu_obj.home_folder + _default_landuse_2011)

    # split land use data into 2 pots: Scotland and E+W
    zones = census_output["ZoneID"].drop_duplicates().dropna()
    scott = zones[zones.str.startswith('S')]
    ew_land_use = census_output[~census_output.ZoneID.isin(scott)]

    if midyear:
        # group to ZoneID, Gender, Age to match info from MYPE
        ew_land_use_group = ew_land_use.groupby(by=['ZoneID', 'Gender', 'Age'],
                                                as_index=False).sum()[['ZoneID', 'Gender', 'Age', 'people']]
        # get a communal factor calculated
        communal_group = communal.groupby(by=['ZoneID', 'Gender', 'Age'],
                                          as_index=False).sum()[['ZoneID', 'Gender', 'Age', 'communal']]
        com2011 = ew_land_use_group.merge(communal_group, on=['ZoneID', 'Gender', 'Age'])
        com2011['CommunalFactor'] = com2011['communal'] / com2011['people']
        com2011 = com2011.rename(columns={'people': 'Census'})

        # uplift communal to MYPE
        mype = get_ew_population(by_lu_obj)
        mype_adjust = mype.merge(com2011, on=['ZoneID', 'Gender', 'Age'], how='outer')
        mype_adjust['communal_mype'] = mype_adjust['pop'].values * mype_adjust['CommunalFactor'].values
        print('Communal establishments total for new MYPE is ', mype_adjust['communal_mype'].sum())
        mype_communal = mype_adjust[['ZoneID', 'Gender', 'Age', 'communal_mype']]
        return mype_communal

    else:
        pass
        # TODO: set up for future year
        '''
        ew_land_use_group = ew_land_use.groupby(by=['ZoneID'], as_index=False).sum()[['ZoneID', 'people']]
        communal_group = communal.groupby(by=['ZoneID'], as_index=False).sum()[['ZoneID', 'communal']]
        com2011 = ew_land_use_group.merge(communal_group, on=['ZoneID'])
        com2011['CommunalFactor'] = com2011['communal'] / com2011['people']
        com2011 = com2011.rename(columns={'people': 'Census'})
        fy = get_fy_population()
        fype_adjust = fy.merge(com2011, on=['ZoneID'], how='left')
        fype_adjust['communal_fype'] = fype_adjust['pop'].values * fype_adjust['CommunalFactor'].values

        print('Communal establishments total for fy is ', fype_adjust['communal_mype'].sum())
        '''


# TODO: include block commenting
def adjust_landuse_to_specific_yr(by_lu_obj, writeOut=True):
    """
    Takes adjusted landuse (after splitting out communal establishments)
    Parameters
    ----------
    land use output
        Path to csv of landuseoutput 2011 with all the segmentation (emp type, soc, ns_sec, gender, hc, prop_type), 
        to get the splits

    Returns
    ----------
    
    """
    if writeOut:
        landuse_segments = compress.read_in(by_lu_obj.home_folder + _landuse_segments)
        landuse_segments = landuse_segments[['ZoneID', 'area_type', 'property_type', 'Age',
                                             'Gender', 'employment_type', 'ns_sec',
                                             'household_composition',
                                             'SOC_category', 'people']].drop_duplicates()

        # TODO: put these normalisation dictionaries in lu_constants
        gender_nt = {'Male': 2, 'Females': 3, 'Children': 1}
        age_nt = {'under 16': 1, '16-74': 2, '75 or over': 3}
        emp_nt = {'fte': 1, 'pte': 2, 'unm': 3, 'stu': 4, 'non_wa': 5}

        # Set inactive SOC category to 0 and normalise the data
        landuse_segments['SOC_category'] = landuse_segments['SOC_category'].fillna(0)
        landuse_segments['gender'] = landuse_segments['Gender'].map(gender_nt)
        landuse_segments['age_code'] = landuse_segments['Age'].map(age_nt)
        landuse_segments['emp'] = landuse_segments['employment_type'].map(emp_nt)
        landuse_segments = landuse_segments.drop(columns={'Age', 'Gender', 'employment_type'})
        landuse_segments = landuse_segments.groupby(by=['ZoneID', 'age_code', 'emp', 'gender', 'SOC_category',
                                                        'ns_sec', 'area_type', 'property_type',
                                                        'household_composition'],
                                                    as_index=False).sum()

        # change to int8 to reduce table size
        landuse_segments['age_code'] = landuse_segments['age_code'].astype(np.int8)
        landuse_segments['emp'] = landuse_segments['emp'].astype(np.int8)
        landuse_segments['gender'] = landuse_segments['gender'].astype(np.int8)
        landuse_segments['ns_sec'] = landuse_segments['ns_sec'].astype(np.int8)
        landuse_segments['SOC_category'] = landuse_segments['SOC_category'].astype(np.int8)
        landuse_segments['area_type'] = landuse_segments['area_type'].astype(np.int8)
        landuse_segments['household_composition'] = landuse_segments['household_composition'].astype(np.int8)
        landuse_segments['property_type'] = landuse_segments['property_type'].astype(np.int8)

        # Get the communal establishments removed
        landuse_no_com = landuse_segments[landuse_segments.property_type != 8]
        # group by age and gender columns and sum people
        pop_pc_totals = landuse_no_com.groupby(by=['ZoneID', 'age_code', 'gender'],
                                               as_index=False).sum()[['ZoneID', 'age_code', 'gender', 'people']]

        # LU SIMPLIFICATION
        # Build simplified land use for building adjustment factors
        len_before = len(landuse_no_com)
        lu_index = list(landuse_no_com)
        lu_groups = lu_index.copy()
        lu_groups.remove('people')
        landuse_no_com = landuse_no_com[lu_index].groupby(lu_groups).sum().reset_index()
        len_after = len(landuse_no_com)
        # TODO: logging to file rather than console
        print('LU length %d before %d after' % (len_before, len_after))

        # Get Scottish Population
        scot_mype = format_scottish_mype(by_lu_obj)
        scot_mype = scot_mype[['ZoneID', 'Gender', 'Age', 'pop']]
        print('Reading in new Scot population data')

        mype_communal = sort_communal_uplift(by_lu_obj)
        ewmype = get_ew_population(by_lu_obj)

        # adjust mype in EW to get rid of communal

        ewmype = ewmype.merge(mype_communal, on=['ZoneID', 'Gender', 'Age'])
        ewmype['newpop'] = ewmype['pop'] - ewmype['communal_mype']
        ewmype = ewmype[['ZoneID', 'Gender', 'Age', 'newpop']].rename(columns={'newpop': 'pop'})

        mype_gb = ewmype.append(scot_mype)
        mype_gb['gender'] = mype_gb['Gender'].map(gender_nt).drop(columns={'Gender'})
        mype_gb['age_code'] = mype_gb['Age'].map(age_nt).drop(columns={'Age'})

        mype_pops = pop_pc_totals.merge(mype_gb, on=['ZoneID', 'gender', 'age_code'])
        del scot_mype, ewmype
        mype_pops['pop_factor'] = mype_pops['pop'] / mype_pops['people']

        # mype simplification
        mype_before = len(mype_pops)

        mype_index = ['ZoneID', 'gender', 'age_code', 'pop_factor']
        mype_groups = ['ZoneID', 'gender', 'age_code']
        mype_pops = mype_pops[mype_index].groupby(mype_groups).sum()
        mype_pops = mype_pops.reset_index()

        mype_after = len(mype_pops)

        print('MYPE length %d before %d after' % (mype_before, mype_after))

        # 1. select relevant categories only - group by categories, sum
        landuse_simple_cols = ['ZoneID', 'gender', 'age_code', 'people']
        # TODO: this is v similar to mype_pops minus the factors, so is it needed? Also gets overwritten!
        landuse_simple = landuse_no_com[landuse_simple_cols].groupby(mype_groups).sum().reset_index()

        landuse = pd.merge(landuse_simple, mype_pops, how='inner', on=['ZoneID', 'gender', 'age_code'])
        landuse['adj_pop'] = landuse['people'] * landuse['pop_factor']  # adjusted 2018 population

        # Merge adj factors onto main land use build
        landuse = pd.merge(landuse_no_com, mype_pops, how='inner', on=['ZoneID', 'gender', 'age_code'])

        landuse['people'] = landuse['people'] * landuse['pop_factor']
        landuse = landuse.drop(columns={'pop_factor'})
        landuse_cols = ['ZoneID', 'gender', 'age_code', 'emp', 'SOC_category', 'ns_sec',
                        'area_type', 'property_type', 'household_composition', 'people']
        landuse = landuse[landuse_cols]

        # COMMUNAL ESTABLISHMENTS
        # Get the communal establishments 
        landuse_com = landuse_segments[landuse_segments.property_type == 8]

        com = sort_communal_uplift(by_lu_obj)
        com['gender'] = com['Gender'].map(gender_nt)
        com['age_code'] = com['Age'].map(age_nt)
        com = com.drop(columns={'Age', 'Gender'})

        pop_pc_comms = landuse_com.groupby(by=['ZoneID', 'age_code', 'gender'],
                                           as_index=False).sum()[['ZoneID', 'age_code', 'gender', 'people']]

        mye_pops = pop_pc_comms.merge(com, on=['ZoneID', 'gender', 'age_code'])
        mye_pops['pop_factor'] = mye_pops['communal_mype'] / mye_pops['people']
        mye_pops = mye_pops.drop(columns={'communal_mype', 'people'})

        communal_pop = landuse_com.merge(mye_pops, on=['ZoneID', 'gender', 'age_code'])
        communal_pop['newpop'] = communal_pop['people'] * communal_pop['pop_factor']
        communal_pop['newpop'].sum()

        communal_pop = communal_pop.drop(columns={'people', 'pop_factor'}).rename(columns={'newpop': 'people'})
        communal_pop = communal_pop[landuse_cols]
        # need to retain the missing MSOAs for both population landuse outputs and HOPs  
        gb_adjusted = landuse.append(communal_pop)

        # checks:
        # TODO: put these checks into logging file rather than console
        print('checking for null values:', gb_adjusted.isnull().any())
        print('Full population for 2018 is now =', gb_adjusted['people'].sum())
        print('check all MSOAs are present, should be 8480:', gb_adjusted['ZoneID'].drop_duplicates().count())
        gb_adjusted = gb_adjusted.groupby(by=['ZoneID', 'gender', 'age_code', 'emp', 'SOC_category', 'ns_sec',
                                              'area_type', 'property_type', 'household_composition']
                                          , as_index=False).sum()
        logging.info('Population currently {}'.format(gb_adjusted.people.sum()))
        compress.write_out(gb_adjusted, by_lu_obj.home_folder + '/landUseOutputMSOA_2018')
        print('full GB adjusted dataset should be now saved in default iter folder')

        # reclaim memory
        del (communal_pop, pop_pc_comms, mye_pops, landuse_com, com, landuse_segments, len_before, len_after)
        gc.collect()
    else:
        print('FY not set up yet')


# TODO: rename this function
# TODO: include block commenting
def sort_out_hops_uplift(by_lu_obj):
    """    
    This provides the new household occupancy figures for each property type 
    following MYPE adjustment.
    Get the filledproperties percentage.
    Parameters
    ----------
    allResPropertyZonal calculated from main build
    MYPE population

    Returns
    ----------
    Adjusted HOPs
    
    """
    all_res_property_zonal = pd.read_csv(by_lu_obj.home_folder + '/classifiedResPropertyMSOA.csv')
    all_res_property_zonal['new_prop_type'] = all_res_property_zonal['census_property_type']
    # TODO: how does this relate to the landuse_formatting in main_build? Map a dictionary instead
    all_res_property_zonal.loc[all_res_property_zonal['census_property_type'] == 5, 'new_prop_type'] = 4
    all_res_property_zonal.loc[all_res_property_zonal['census_property_type'] == 6, 'new_prop_type'] = 4
    all_res_property_zonal.loc[all_res_property_zonal['census_property_type'] == 7, 'new_prop_type'] = 4
    all_res_property_zonal = all_res_property_zonal.drop(columns='census_property_type')
    all_res_property_zonal = all_res_property_zonal.rename(columns={'new_prop_type': 'property_type'})
    all_res_property_zonal = all_res_property_zonal.groupby(by=['ZoneID', 'property_type'], as_index=False).sum()
    all_res_property_zonal['household_occupancy_18'] = all_res_property_zonal['population'] / \
                                                       all_res_property_zonal['UPRN']

    mype_pop = compress.read_in(by_lu_obj.home_folder + '/landUseOutputMSOA_2018')
    mype_pop = mype_pop.groupby(by=['ZoneID', 'property_type'], as_index=False).sum()
    mype_pop = mype_pop[['ZoneID', 'property_type', 'people']]

    hops = all_res_property_zonal.merge(mype_pop, on=['ZoneID', 'property_type'])
    hops['household_occupancy_2018_mype'] = hops['people'] / hops['UPRN']

    hops.to_csv(by_lu_obj.home_folder + '/Hops Population Audits/household_occupation_comparison.csv', index=False)
    hops = hops.drop(columns={'UPRN', 'household_occupancy_18', 'population', 'people'})
    hops = hops.rename(columns={'household_occupancy_2018_mype': 'household_occupancy'})

    # Check all msoas are included:
    print('Check all MSOAs are present, should be 8480:', hops['ZoneID'].drop_duplicates().count())
    hops.to_csv(by_lu_obj.home_folder + '/Hops Population Audits/2018_household_occupancy.csv', index=False)


# TODO: improve block commenting
def adjust_car_availability(by_lu_obj):
    """
    applies nts extract to landuse
    Parameters
    ----------
    Returns
    ----------
    """
    _nts_import_path = by_lu_obj.home_folder + '/nts_splits.csv'
    land_use = compress.read_in(by_lu_obj.home_folder + _landuse_segments)
    cars_adjust = pd.read_csv(_nts_import_path)

    segments = land_use.groupby(by=['area_type', 'employment_type', 'household_composition'],
                                as_index=False)['people'].sum()

    # Calculate splits between each hh comp category across each area type and employment type
    segments['totals2'] = segments.groupby(['area_type', 'employment_type'])['people'].transform('sum')
    segments['splits2'] = segments['people'] / segments['totals2']
    segments['lu_total'] = segments.groupby('employment_type')['people'].transform('sum')

    # derive the new totals using the NTS splits    
    join = segments.merge(cars_adjust, on=['employment_type', 'area_type', 'household_composition'])
    join['newhc'] = join['splits'] * join['totals2']
    join = join[['area_type', 'employment_type', 'household_composition', 'newhc']]

    # TODO: SOC category includes nulls (also hh comp and Gender), which get thrown out by the groupby
    land = land_use.groupby(by=['ZoneID', 'area_type', 'ns_sec', 'Age', 'SOC_category',
                                'property_type', 'Gender', 'employment_type'],
                            as_index=False)['people'].sum()
    land['total'] = land.groupby(['area_type', 'employment_type'])['people'].transform('sum')
    land['factor'] = land['people'] / land['total']

    all_combined2 = land.merge(join, on=['area_type', 'employment_type'])
    all_combined2['new'] = all_combined2['newhc'] * all_combined2['factor']
    all_combined2.to_csv(by_lu_obj.home_folder + '/landuse_caradj.csv', index=False)

    car_available = all_combined2.groupby(by=['household_composition'], as_index=False).sum()
    car_available.to_csv(by_lu_obj.home_folder + '/caravailable.csv')

    by_lu_obj.state['5.2.11 car availability'] = 1
    logging.info('Step 5.2.11 completed')


# TODO: revise the print statements in this function. Good points to add logging maybe
# TODO: include block commenting
def adjust_soc_gb(by_lu_obj):
    """
    To apply before the MYPE
    adjusts SOC values to gb levels for 2018
    """
    gb_soc_totals = pd.read_csv(by_lu_obj.import_folder + _gb_soc_totals)

    lad_translation = pd.read_csv(by_lu_obj.zones_folder + _default_lad_translation)
    lad_translation = lad_translation.drop(columns={'overlap_type', 'lad_to_msoa', 'msoa_to_lad'}).rename(
        columns={'msoa_zone_id': 'ZoneID', 'lad_zone_id': 'objectid'}
    )

    gb_soc_totals = gb_soc_totals.rename(columns={
        'T12a:1 (1 Managers, Directors and Senior Officials (SOC2010) : All people )': 'SOC1',
        'T12a:4 (2 Professional Occupations (SOC2010) : All people )': 'SOC2',
        'T12a:7 (3 Associate Prof & Tech Occupations (SOC2010) : All people )': 'SOC3',
        'T12a:10 (4 Administrative and Secretarial Occupations (SOC2010) : All people )': 'SOC4',
        'T12a:13 (5 Skilled Trades Occupations (SOC2010) : All people )': 'SOC5',
        'T12a:16 (6 Caring, Leisure and Other Service Occupations (SOC2010) : All people )': 'SOC6',
        'T12a:19 (7 Sales and Customer Service Occupations (SOC2010) : All people )': 'SOC7',
        'T12a:22 (8 Process, Plant and Machine Operatives (SOC2010) : All people )': 'SOC8',
        'T12a:25 (9 Elementary occupations (SOC2010) : All people )': 'SOC9'})

    gb_soc_totals = gb_soc_totals[['Country', 'SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5',
                                   'SOC6', 'SOC7', 'SOC8', 'SOC9']]
    gb_soc_totals = gb_soc_totals[gb_soc_totals.Country.isin(['England and Wales number', 'Scotland number'])]

    gb_soc_totals = pd.melt(gb_soc_totals,
                            id_vars='Country',
                            value_vars=['SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 'SOC7', 'SOC8', 'SOC9'])
    gb_soc_totals['value'] = pd.to_numeric(gb_soc_totals['value'])

    gb_soc_totals = gb_soc_totals.replace({'variable': {'SOC1': 1, 'SOC2': 1, 'SOC3': 1,
                                                        'SOC4': 2, 'SOC5': 2, 'SOC6': 2,
                                                        'SOC7': 2, 'SOC8': 3, 'SOC9': 3}})
    gb_soc_totals = gb_soc_totals.rename(columns={'variable': 'SOC_category'})
    gb_soc_totals = gb_soc_totals.groupby(by=['Country', 'SOC_category'], as_index=False).sum()

    gb_soc_totals['total'] = gb_soc_totals.groupby(['Country'])['value'].transform('sum')
    gb_soc_totals['splits'] = gb_soc_totals['value'] / gb_soc_totals['total']

    land_use_segments = compress.read_in(by_lu_obj.home_folder + '/AdjustedGBlanduse_emp')
    employed = land_use_segments[land_use_segments.emp.isin([1, 2])]  # fte and pte
    employed = employed.merge(lad_translation, on='ZoneID')
    employed['Country'] = 'England and Wales number'
    employed.loc[employed['ZoneID'].str.startswith('S'), 'Country'] = 'Scotland number'

    emp_soc_total = employed.groupby(by=['Country', 'SOC_category'],
                                     as_index=False).sum()[['Country', 'SOC_category', 'people']]
    emp_soc_total['total_land'] = emp_soc_total.groupby(['Country'])['people'].transform('sum')

    # for audit
    emp_soc_total['splits_land'] = emp_soc_total['people'] / emp_soc_total['total_land']
    emp_soc_total['SOC_category'] = pd.to_numeric(emp_soc_total['SOC_category'])

    # save for comparison
    emp_compare = emp_soc_total.merge(gb_soc_totals,
                                      on=['Country', 'SOC_category'],
                                      how='left').drop(columns={'people', 'splits_land', 'value', 'total'})

    emp_compare.to_csv(by_lu_obj.home_folder + '/SOCsplitsComparison.csv')

    emp_compare['pop'] = emp_compare['splits'] * emp_compare['total_land']
    print(emp_compare['pop'].sum())
    emp_compare = emp_compare.drop(columns={'total_land', 'splits'})

    land_use_grouped = employed.groupby(by=['Country', 'SOC_category'], as_index=False).sum()
    land_use_grouped = land_use_grouped[['Country', 'SOC_category', 'people']]
    land_use_grouped['total'] = land_use_grouped.groupby(['Country'])['people'].transform('sum')
    land_use_grouped['factor'] = land_use_grouped['people'] / land_use_grouped['total']

    soc_revised = emp_compare.merge(land_use_grouped, on=['Country', 'SOC_category'], how='left')
    soc_revised['factor'] = soc_revised['pop'] / soc_revised['people']
    soc_revised = soc_revised[['Country', 'SOC_category', 'factor']]
    soc_revised = employed.merge(soc_revised, on=['Country', 'SOC_category'])

    soc_revised['newpop'] = soc_revised['factor'] * soc_revised['people']
    print(soc_revised['people'].sum())
    soc_revised = soc_revised.drop(columns={'factor'})

    npr_segments = ['ZoneID', 'area_type', 'property_type', 'emp',
                    'age_code', 'gender', 'household_composition',
                    'ns_sec', 'SOC_category', 'newpop']
    soc_revised = soc_revised[npr_segments]
    print(soc_revised['newpop'].sum())
    soc_revised = soc_revised.rename(columns={'newpop': 'people'})

    # join to the rest
    not_employed = land_use_segments[~land_use_segments.emp.isin([1, 2])]  # neither fte nor pte
    npr_segmentation = not_employed.append(soc_revised)

    logging.info('Population currently {}'.format(npr_segmentation.people.sum()))
    compress.write_out(npr_segmentation, by_lu_obj.home_folder + '/landuse_adjustedSOCs')


def adjust_soc_lad(by_lu_obj):
    """
    TODO: lad translation path has changed here - needs updating
    """
    # Read in the LAD controls data and pick out the totals columns
    lad_soc_control = pd.read_csv(by_lu_obj.import_folder + _ladsoc_control)
    lad_soc_control = lad_soc_control.rename(columns={
        '% all in employment who are - 1: managers, directors and senior officials (SOC2010) numerator': 'SOC1',
        '% all in employment who are - 2: professional occupations (SOC2010) numerator': 'SOC2',
        '% all in employment who are - 3: associate prof & tech occupations (SOC2010) numerator': 'SOC3',
        '% all in employment who are - 4: administrative and secretarial occupations (SOC2010) numerator': 'SOC4',
        '% all in employment who are - 5: skilled trades occupations (SOC2010) numerator': 'SOC5',
        '% all in employment who are - 6: caring, leisure and other service occupations (SOC2010) numerator': 'SOC6',
        '% all in employment who are - 7: sales and customer service occupations (SOC2010) numerator': 'SOC7',
        '% all in employment who are - 8: process, plant and machine operatives (SOC2010) numerator': 'SOC8',
        '% all in employment who are - 9: elementary occupations (SOC2010) numerator': 'SOC9'
    })
    lad_cols = ['lad17cd', 'SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 'SOC7', 'SOC8', 'SOC9']
    lad_soc_control = lad_soc_control[lad_cols]
    lad_soc_control = lad_soc_control.replace({'!': 0, '~': 0, '-': np.nan, '#': 0})  # these are data quality markers

    # Aggregate to just three SOC categories and compute the splits for each LAD between these three
    lad_soc = pd.melt(lad_soc_control,
                      id_vars='lad17cd',
                      value_vars=['SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 'SOC7', 'SOC8', 'SOC9'])
    lad_soc['value'] = pd.to_numeric(lad_soc['value'])
    lad_soc = lad_soc.replace({'variable': {'SOC1': 1, 'SOC2': 1, 'SOC3': 1,
                                            'SOC4': 2, 'SOC5': 2, 'SOC6': 2,
                                            'SOC7': 2, 'SOC8': 3, 'SOC9': 3}})
    lad_soc = lad_soc.rename(columns={'variable': 'SOC_category'})
    lad_soc['total'] = lad_soc.groupby('lad17cd')['value'].transform('sum')
    lad_soc['splits'] = lad_soc['value'] / lad_soc['total']
    lad_soc = lad_soc.groupby(by=['lad17cd', 'SOC_category'])[['value', 'total', 'splits']].apply(
        lambda x: x.sum(min_count=1, skipna=False)).reset_index()  # sum where np.nan is retained

    # Read in the MSOA-LAD correspondence and perform a cross join such that every MSOA pair within an LAD is included
    lad_ref = pd.read_csv(by_lu_obj.zones_folder + _default_lad_translation).iloc[:, 0:2]
    lad_ref = lad_ref.rename(columns={'msoa_zone_id': 'ZoneID'})

    # Read in the population to adjust
    land_use = compress.read_in(by_lu_obj.home_folder + '/landuse_adjustedSOCs')

    # First handle employed
    employed = land_use[land_use.emp.isin([1, 2])]  # fte and pte
    print('Employed people in landuse: ', employed['people'].sum())

    ladref = gpd.read_file(_default_ladRef).iloc[:, 0:2].rename(columns={'objectid': 'lad_zone_id'})

    # Compute existing totals by LAD
    soc_totals_msoa = employed.groupby(by=['ZoneID', 'SOC_category'], as_index=False).sum()[
        ['ZoneID', 'SOC_category', 'people']]
    soc_totals_msoa = soc_totals_msoa.merge(lad_ref, on='ZoneID')
    soc_totals_lad = soc_totals_msoa.groupby(by=['lad_zone_id'], as_index=False).sum()
    soc_totals_lad = soc_totals_lad[['lad_zone_id', 'people']]
    soc_totals_lad = soc_totals_lad.merge(ladref, on='lad_zone_id')
    soc_totals_before = soc_totals_msoa.groupby(by=['lad_zone_id', 'SOC_category'], as_index=False).sum()

    # Compute the re-constrained totals by LAD
    soc_totals_after = soc_totals_lad.merge(lad_soc, on=['lad17cd'])
    soc_totals_after['newpop'] = soc_totals_after['people'] * soc_totals_after['splits']
    soc_totals_after = soc_totals_after[['lad17cd', 'lad_zone_id', 'SOC_category', 'newpop']]

    # Work out scaling factors to apply to each MSOA and SOC category
    compare = soc_totals_before.merge(soc_totals_after, on=['lad_zone_id', 'SOC_category'])
    compare['factor'] = compare['newpop'] / compare['people']
    compare = compare.drop(columns={'lad17cd', 'people', 'newpop'})

    # Apply the scaling factors to the main land use DataFrame
    land_use = land_use[~(land_use.emp.isin([1, 2]) & (land_use.SOC_category == 0))]  # exclude employed rows with SOC 0
    land_use = land_use.merge(lad_ref, on='ZoneID')
    land_use = land_use.merge(compare, how='left', on=['lad_zone_id', 'SOC_category']).fillna(1).replace([np.inf], 0)

    print('Population before LAD SOC control:', land_use.people.sum())
    land_use['people'] *= land_use['factor']
    print('Population after LAD SOC control:', land_use.people.sum())

    # Write it out as an output
    land_use = land_use.drop(columns={'lad_zone_id', 'factor'})
    logging.info('Population currently {}'.format(land_use.people.sum()))
    compress.write_out(land_use, by_lu_obj.home_folder + '/final_land_use')
    by_lu_obj.state['5.2.10 SEC/SOC'] = 1  # record as done, this is the final SEC/SOC adjustment
    logging.info('Step 5.2.10 completed')

    
def control_to_lad_employment_ag(by_lu_obj):
    """
    control to employment at LAD level for age, gender and fte/pte employment; 
    adjusts inactive people in work accordingly
    
    Parameters
    ----------
    by_lu_obj: base year land use object
    
    Returns
    ----------
    gb_land_use_controlled: number of employed people is controlled to 2018 age, gender and
    fte/pte patterns in employment
    """

    land_use = compress.read_in(by_lu_obj.home_folder + '/landUseOutputMSOA_2018')

    lad_translation = pd.read_csv(by_lu_obj.zones_folder + _default_lad_translation)
    lad_translation = lad_translation.drop(columns={'lad_to_msoa', 'msoa_to_lad', 'overlap_type'})
    lad_translation = lad_translation.rename(columns={'msoa_zone_id': 'ZoneID', 'lad_zone_id': 'objectid'})
    lad_ref = gpd.read_file(_default_ladRef).iloc[:, 0:2]
    land_use = land_use.merge(lad_translation, on='ZoneID', how='left')
    land_use = land_use.merge(lad_ref, on='objectid')

    emp_controls = pd.read_csv(by_lu_obj.import_folder + _emp_controls)
    emp_controls = emp_controls.rename(columns={
        'T08:29 (Males - Aged 16 - 64 : Full-time ) number': 'Male FTE',
        'T08:30 (Males - Aged 16 - 64 : Part-time ) number': 'Male PTE',
        'T08:44 (Females - Aged 16 - 64 : Full-time ) number': 'Females FTE',
        'T08:45 (Females - Aged 16 - 64 : Part-time ) number': 'Females PTE',
        'T01:8 (All aged 16 & over - In employment : Males ) number': 'Male Emp',
        'T01:9 (All aged 16 & over - In employment : Females ) number': 'Females Emp',
    })

    emp_controls = emp_controls.replace({'!': 0, '~': 0, '-': 0, '#': 0, ',': ''})
    lad_emp_gen_controls = emp_controls[['lad17cd', 'Male Emp', 'Females Emp']]

    lad_controlled = pd.melt(lad_emp_gen_controls, id_vars='lad17cd', value_vars=['Male Emp', 'Females Emp'])
    lad_controlled['gender'] = 2
    lad_controlled['employment_cat'] = 'emp'

    lad_controlled.loc[lad_controlled['variable'] == 'Male Emp', 'employment_cat'] = 'emp'
    lad_controlled.loc[lad_controlled['variable'] == 'Male Emp', 'gender'] = 2
    lad_controlled.loc[lad_controlled['variable'] == 'Females Emp', 'employment_cat'] = 'emp'
    lad_controlled.loc[lad_controlled['variable'] == 'Females Emp', 'gender'] = 3
    lad_controlled['value'] = pd.to_numeric(lad_controlled['value'])

    lad_controlled = lad_controlled.groupby(by=['lad17cd', 'employment_cat', 'gender'], as_index=False).sum()

    # Compute the total for the 16-74 working age population
    wa_all = land_use[land_use.emp != 5]
    total_wa_pop = wa_all.groupby(by=['lad17cd', 'gender'], as_index=False).sum()
    total_wa_pop = total_wa_pop.drop(columns={'household_composition', 'area_type',
                                              'property_type', 'objectid', 'SOC_category',
                                              'ns_sec', 'age_code'})
    lad_controlled_wa = lad_controlled.merge(total_wa_pop, on=['lad17cd', 'gender'], how='left')
    lad_controlled_wa['splits'] = lad_controlled_wa['value'] / lad_controlled_wa['people']

    # For Isles of Scilly E06000053:
    LADcontrolledAverage = lad_controlled_wa.groupby(by=['employment_cat'], as_index=False).sum()
    LADcontrolledAverage['av_splits'] = LADcontrolledAverage['value'] / LADcontrolledAverage['people']
    LADcontrolledAverage = LADcontrolledAverage.drop(columns={'value', 'people', 'splits', 'gender', 'emp'})
    lad_controlled_wa = lad_controlled_wa.drop(columns={'value', 'people'})

    # TODO: check why they are missing
    # Filling in splits where they're missing
    # TODO: is the '2' suffix necessary?
    lad_controlled2 = lad_controlled_wa.merge(LADcontrolledAverage, on='employment_cat', how='right')
    lad_controlled2.loc[lad_controlled2.lad17cd == 'E06000053', 'splits'] = lad_controlled2['av_splits']
    lad_controlled2['splits'] = lad_controlled2['splits'].fillna(lad_controlled2['av_splits'])
    lad_controlled2 = lad_controlled2.drop(columns={'av_splits'})
    lad_controlled2.loc[lad_controlled2['lad17cd'] == 'E09000001', 'splits'] = 1
    lad_controlled2['inactivesplits'] = 1 - lad_controlled2['splits']
    lad_controlled2 = lad_controlled2.drop(columns={'emp'})

    # FTE/PTE splits
    lad_fte_pte_controls = emp_controls[['lad17cd', 'Male FTE', 'Females FTE', 'Male PTE', 'Females PTE']]
    lad_fte_pte_controls = pd.melt(lad_fte_pte_controls,
                                   id_vars='lad17cd',
                                   value_vars=['Male FTE', 'Females FTE', 'Male PTE', 'Females PTE'])

    lad_fte_pte_controls['gender'] = 2
    lad_fte_pte_controls['emp'] = 3

    # TODO: implement this as a dictionary mapping
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Male FTE', 'emp'] = 1
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Male FTE', 'gender'] = 2
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Male PTE', 'emp'] = 2
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Male PTE', 'gender'] = 2
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Females FTE', 'emp'] = 1
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Females FTE', 'gender'] = 3
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Females PTE', 'emp'] = 2
    lad_fte_pte_controls.loc[lad_fte_pte_controls['variable'] == 'Females PTE', 'gender'] = 3
    lad_fte_pte_controls = lad_fte_pte_controls.drop(columns={'variable'})
    lad_fte_pte_controls['value'] = pd.to_numeric(lad_fte_pte_controls['value'])

    lad_fte_pte_controls = lad_fte_pte_controls.groupby(by=['lad17cd', 'emp', 'gender'], as_index=False).sum()

    lad_fte_pte_controls['totals'] = lad_fte_pte_controls.groupby(['lad17cd', 'gender'])['value'].transform('sum')
    lad_fte_pte_controls['splits'] = lad_fte_pte_controls['value'] / lad_fte_pte_controls['totals']

    lad_fte_pte_controls_average = lad_fte_pte_controls.groupby(by=['emp'], as_index=False).sum().drop(
        columns={'gender'})
    lad_fte_pte_controls_average['av_splits'] = lad_fte_pte_controls_average['value'] / lad_fte_pte_controls_average[
        'totals']
    lad_fte_pte_controls_average = lad_fte_pte_controls_average.drop(columns={'value', 'totals', 'splits'})

    lad_fte_pte_controls = lad_fte_pte_controls.merge(lad_fte_pte_controls_average, on='emp', how='right')
    lad_fte_pte_controls['splits'] = lad_fte_pte_controls['splits'].fillna(lad_fte_pte_controls['av_splits'])

    lad_fte_pte_controls = lad_fte_pte_controls.drop(columns={'av_splits', 'value', 'totals'})
    lad_fte_pte_controls['employment_cat'] = 'emp'

    # Calculate the total population by LAD and apply the splits between inactive and active
    wa_all = land_use[land_use.emp != 5]
    land_use_lad = wa_all.groupby(by=['ZoneID', 'lad17cd', 'gender'],
                                  as_index=False).sum()[['ZoneID', 'lad17cd', 'gender', 'people']]
    factored_emp = land_use_lad.merge(lad_controlled2, on=['lad17cd', 'gender'], how='left')
    factored_emp['newpop'] = factored_emp['people'] * factored_emp['splits']
    factored_emp = factored_emp.drop(columns={'people', 'splits', 'inactivesplits'})

    # Emp Active Split by fte/pte 
    employed = factored_emp.merge(lad_fte_pte_controls, on=['lad17cd', 'gender',
                                                            'employment_cat'], how='left')
    employed['pop'] = employed['newpop'] * employed['splits']
    employed = employed.drop(columns={'newpop', 'splits', 'employment_cat'})

    active_land_use = wa_all[wa_all.emp.isin([1, 2])]  # fte and pte
    active_land_use_grouped = active_land_use.groupby(by=['ZoneID', 'lad17cd', 'gender', 'emp'],
                                                      as_index=False).sum()
    active_land_use_grouped = active_land_use_grouped.drop(columns={'household_composition',
                                                                    'area_type',
                                                                    'objectid',
                                                                    'property_type',
                                                                    'ns_sec',
                                                                    'SOC_category'})
    # Work out fte/pte scaling factors
    active_comp = active_land_use_grouped.merge(employed, on=['ZoneID', 'lad17cd', 'gender', 'emp'], how='left')
    active_comp['factor'] = active_comp['pop'] / active_comp['people']
    active_comp = active_comp.drop(columns={'people', 'pop', 'age_code'})

    # Join back to fte/pte land use level for scaling
    active_land_use2 = active_land_use.merge(active_comp,
                                             on=['ZoneID', 'lad17cd', 'gender', 'emp'],
                                             how='left')
    active_land_use2['newpop'] = active_land_use2['people'] * active_land_use2['factor']
    active_land_use2 = active_land_use2.drop(columns={'factor'})
    active_land_use2['newpop'].sum()
    # TODO: is this check needed?
    # check_active = active_land_use2.groupby('emp', as_index=False).sum()

    # Inactive and unemployed people adjustment
    land_use_lad = wa_all.groupby(by=['ZoneID', 'lad17cd', 'gender'], as_index=False).sum()
    land_use_lad = land_use_lad.drop(columns={'household_composition', 'area_type',
                                              'property_type', 'objectid', 'ns_sec',
                                              'SOC_category'})
    factored_inc = land_use_lad.merge(lad_controlled2, on=['lad17cd', 'gender'], how='left')
    factored_inc['newpop'] = factored_inc['people'] * factored_inc['inactivesplits']
    factored_inc = factored_inc.drop(columns={'people', 'inactivesplits', 'splits'})

    # Compare the new and old inactive and unemployed people to get a scaling factor
    inactive_land_use = wa_all[wa_all.emp.isin([3, 4])]  # inactive and unemployed
    inactive_land_use_grouped = inactive_land_use.groupby(by=['ZoneID', 'lad17cd', 'gender'], as_index=False).sum()
    inactive_land_use_grouped = inactive_land_use_grouped.drop(columns={'household_composition', 'area_type',
                                                                        'objectid', 'property_type', 'age_code',
                                                                        'emp', 'SOC_category', 'ns_sec'})
    inactive_comp = inactive_land_use_grouped.merge(factored_inc, on=['ZoneID', 'lad17cd', 'gender'], how='left')
    inactive_comp = inactive_comp.drop(columns={'age_code', 'emp'})
    inactive_comp['factor'] = inactive_comp['newpop'] / inactive_comp['people']
    inactive_comp = inactive_comp.drop(columns={'people', 'newpop'})

    inactive_land_use2 = inactive_land_use.merge(inactive_comp, on=['ZoneID', 'lad17cd', 'gender'], how='left')
    inactive_land_use2['newpop'] = inactive_land_use2['people'] * inactive_land_use2['factor']

    gb_cols = ['ZoneID', 'age_code', 'emp', 'area_type', 'property_type',
               'household_composition', 'gender',
               'SOC_category', 'ns_sec', 'newpop']
    # get all dfs into the same format for appending
    inactive_land_use2 = inactive_land_use2[gb_cols]
    inactive_land_use2['newpop'].sum()
    active_land_use2 = active_land_use2[gb_cols]
    gb_land_use_controlled = inactive_land_use2.append(active_land_use2)

    # bring back the children to make the full GB population again
    nowa_all = land_use[land_use.emp == 5]

    nowa_all = nowa_all.rename(columns={'people': 'newpop'})
    nowa_all = nowa_all[gb_cols]
    # check the total of children - should be about 17.6m in 2018
    print('Bring back the children, should be ~17.6m in 2018', nowa_all['newpop'].sum())

    gb_land_use_controlled = gb_land_use_controlled.append(nowa_all)
    gb_land_use_controlled = gb_land_use_controlled.rename(columns={'newpop': 'people'})
    print('People in jobs are now adjusted. Total population should be back to ~64.5m, and is',
          gb_land_use_controlled['people'].sum(), 'Now saving the new landuse dataset.')

    # audit to check population per msoa
    check_msoa_totals(by_lu_obj, gb_land_use_controlled, function_name='control_to_lad')

    gb_land_use_controlled.groupby(by=['ZoneID', 'age_code', 'emp', 'area_type', 'property_type',
                                       'household_composition', 'gender',
                                       'SOC_category', 'ns_sec'])
    logging.info('Population currently {}'.format(gb_land_use_controlled.people.sum()))
    compress.write_out(gb_land_use_controlled, by_lu_obj.home_folder + '/GBlanduseControlled')
    gc.collect()

    by_lu_obj.state['5.2.8 MYPE adjustment'] = 1  # record that the mid year adjustment is complete
    logging.info('Step 5.2.8 completed')

    return gb_land_use_controlled


def check_msoa_totals(by_lu_obj, df, function_name):
    """
    check how the outputs compare to MYPE for audits
    """
    df_msoa = df.groupby(by=['ZoneID'], as_index=False).sum().reindex(columns={'ZoneID', 'people'})
    # read in mype msoa totals
    msoa_totals = get_ew_population(by_lu_obj)
    msoa_totals = msoa_totals.groupby(by=['ZoneID'], as_index=False).sum().rename(columns={'pop': 'mype'}).reindex(
        columns=['ZoneID', 'mype'])

    msoa_comparison = msoa_totals.merge(df_msoa, on=['ZoneID'])
    msoa_comparison.to_csv(by_lu_obj.home_folder + '/msoa_check' + function_name + '.csv')
    print(msoa_comparison)


def country_emp_control(by_lu_obj):
    """
    this function is to make sure we have the right amount of people in work 
    Based on APS extract (as of 2018)
    
    Parameters
    ----------
    by_lu_obj: base year land use object
    
    Returns
    ----------
    : number of employed people is controlled to 2018
    
    
    """

    # Country employment control for total numbers of people in work in EW and Scotland
    country_emp = pd.read_csv(by_lu_obj.import_folder + _country_control)
    country_emp = country_emp.rename(columns={'T01:7 (All aged 16 & over - In employment : All People )': 'Emp'})
    country_emp = country_emp[['Country', 'Emp']]
    country_emp = country_emp[country_emp.Country.isin(['England and Wales number', 'Scotland number'])]
    # read in landuse with some employment controls already
    land_use = compress.read_in(by_lu_obj.home_folder + '/GBlanduseControlled')
    zones = land_use['ZoneID'].drop_duplicates()
    scott = zones[zones.str.startswith('S')]
    # work out how many people are employed in Scotland (from land use)
    active = land_use[land_use.emp.isin([1, 2])]  # fte and pte (employed)
    scott_active = active[active.ZoneID.isin(scott)]
    # add this column for matching to control
    scott_active['Country'] = 'Scotland number'
    scott_active['people'].sum()
    scott_active_total = scott_active.groupby(by=['Country'], as_index=False).sum()[['Country', 'people']]
    # ' match the control and landuse for Scotland to work out scaling factor and apply it
    scott_active_total = scott_active_total.merge(country_emp, on='Country')
    scott_active_total['factor'] = scott_active_total['Emp'] / scott_active_total['people']
    scott_active_total = scott_active_total.drop(columns={'people', 'Emp'})
    scott_active = scott_active.merge(scott_active_total, on='Country')
    scott_active['newpop'] = scott_active['people'] * scott_active['factor']
    scott_active = scott_active.drop(columns={'people', 'factor', 'Country'})

    # work out EW employed people, match to control and work out scaling factor
    eng_active = active[~active.ZoneID.isin(scott)]
    eng_active['Country'] = 'England and Wales number'
    eng_active_total = eng_active.groupby(by=['Country'], as_index=False).sum()[['Country', 'people']]
    eng_active_total = eng_active_total.merge(country_emp, on='Country')
    eng_active_total['factor'] = eng_active_total['Emp'] / eng_active_total['people']
    eng_active_total = eng_active_total.drop(columns={'people', 'Emp'})
    eng_active = eng_active.merge(eng_active_total, on='Country')
    eng_active['newpop'] = eng_active['people'] * eng_active['factor']
    eng_active = eng_active.drop(columns={'people', 'factor', 'Country'})
    # check population before
    print('England and Wales employed population after the adjustment', eng_active['newpop'])

    gb_cols = ['ZoneID', 'age_code', 'emp', 'area_type', 'property_type',
               'household_composition', 'gender', 'ns_sec', 'SOC_category', 'people']
    # append the new employed population adjusted for Scotland, England and Wales
    active_adj = eng_active.append(scott_active)
    active_adj = active_adj.rename(columns={'newpop': 'people'})
    active_adj = active_adj[gb_cols]
    active_new_total = active_adj.groupby('ZoneID', as_index=False).sum()[['ZoneID', 'people']]
    active_new_total = active_new_total.rename(columns={'people': 'employed'})

    # get the population per MSOA for employed and inactive (but not children as they're in separate category age wise)
    gb_totals = land_use[land_use.emp != 5]
    gb_totals = gb_totals.groupby('ZoneID', as_index=False).sum()[['ZoneID', 'people']]
    gb_totals = gb_totals.merge(active_new_total, on='ZoneID')
    gb_totals['inactive'] = gb_totals['people'] - gb_totals['employed']
    gb_totals = gb_totals.drop(columns={'people', 'employed'})

    inactive = land_use[land_use.emp.isin([3, 4])]  # inactive employment categories
    inactive2 = inactive.groupby(by=['ZoneID'], as_index=False).sum()[['ZoneID', 'people']]
    inactive2 = inactive2.merge(gb_totals, on='ZoneID')
    inactive2['factor'] = inactive2['inactive'] / inactive2['people']
    # TODO: there are a couple of examples of replace([np.inf], 0), is it really np.inf or is it np.nan?
    inactive2 = inactive2.replace([np.inf], 0)
    inactive2 = inactive2[['ZoneID', 'factor']]
    # TODO: stop this enumeration of variables ('3' in this case)
    inactive3 = inactive.merge(inactive2, on='ZoneID')
    inactive3['newpop'] = inactive3['factor'] * inactive3['people']
    inactive3 = inactive3.drop(columns={'people'})
    inactive3 = inactive3.rename(columns={'newpop': 'people'})
    inactive3 = inactive3[gb_cols]
    inactive3['people'].sum()

    # inactive plus active people appending
    adjusted_gb_land_use = inactive3.append(active_adj)
    adjusted_gb_land_use['people'].sum()
    # get the children by MSOA too
    children = land_use[land_use.emp == 5]
    children = children[gb_cols]
    # children['people'].sum() # should be ~17m
    # append children
    adjusted_gb_land_use = adjusted_gb_land_use.append(children)
    # adjusted_gb_land_use['people'].sum() # should be 64.5m
    # audit the msoa population totals
    check_msoa_totals(by_lu_obj, adjusted_gb_land_use, function_name='country_control')
    print('Saving to default folder...')
    logging.info('Population currently {}'.format(adjusted_gb_land_use.people.sum()))
    compress.write_out(adjusted_gb_land_use, by_lu_obj.home_folder + '/AdjustedGBlanduse_emp')

    by_lu_obj.state['5.2.9 employment adjustment'] = 1  # record step as complete in the base year land use object
    logging.info('Step 5.2.9 completed')


def run_mype(by_lu_obj, midyear=True):
    # normalise_landuse()
    adjust_landuse_to_specific_yr(by_lu_obj)
    control_to_lad_employment_ag(by_lu_obj)
    country_emp_control(by_lu_obj)
    adjust_soc_gb(by_lu_obj)
    adjust_soc_lad(by_lu_obj)
    sort_out_hops_uplift(by_lu_obj)  # audit
    adjust_car_availability(by_lu_obj)  # TODO: replace with NTEM
