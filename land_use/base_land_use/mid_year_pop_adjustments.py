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
import os # File operations
import sys 
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Demand Tool/Python/ZoneTranslation')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/TAME shared resources/Python/')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Utilities/Python')
sys.path.append('C:/Users/ESRIAdmin/Desktop/')

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import *
import gc 
from functools import reduce
import nu_project as nup

# Default file paths

_default_iter = 'iter4'
_default_home = 'E:/NorMITs_Export/'
_default_home_dir = (_default_home + _default_iter)
_import_folder = 'Y:/NorMITs Land Use/import/'
_import_file_drive = 'Y:/'
_default_zone_folder = ('I:/NorMITs Synthesiser/Zone Translation/Export/')
# Default zone names
_default_zone_names = ['LSOA','MSOA']
_default_zone_name = 'MSOA' #MSOA or LSOA

_default_communal_2011 = (_default_home_dir+'/CommunalEstablishments/'+_default_zone_name+'CommunalEstablishments2011.csv')
_default_landuse_2011 = (_default_home_dir+'/landuseOutput'+_default_zone_name+'_withCommunal.csv')
_default_property_count = (_default_home_dir+'/landuseOutput'+_default_zone_name+'.csv')
_default_lad_translation = (_default_zone_folder+'lad_to_msoa/lad_to_msoa.csv')    
_default_census_dat = (_import_folder+'Nomis Census 2011 Head & Household')
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_area_types = ('Y:/NorMITs Land Use/area types/TfNAreaTypesLookup.csv')
    
_default_lsoaRef = _default_zone_ref_folder+'UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_msoaRef = _default_zone_ref_folder+'UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
_default_ladRef = _default_zone_ref_folder+'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
_default_mladRef = _default_zone_ref_folder+'Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'
_mype_females = _import_folder+'/MYE 2018 ONS/2018_MidyearMSOA/MYEfemales_2018.csv'
_mype_males = _import_folder+'MYE 2018 ONS/2018_MidyearMSOA/MYEmales_2018.csv'
_hops2011 = _default_home_dir+'/UKHouseHoldOccupancy2011.csv'
_mypeScot_females = _import_folder+'MYE 2018 ONS/2018_MidyearMSOA/Females_Scotland_2018.csv'
_mypeScot_males = _import_folder+'MYE 2018 ONS/2018_MidyearMSOA/Males_Scotland_2018.csv'
_landuse_segments = _default_home_dir+'/landuseOutput'+_default_zone_name+'_stage4.csv'
_ward_to_msoa = _default_zone_folder + 'uk_ward_msoa_pop_weighted_lookup.csv'
_nts_path = 'Y:/NTS/import/tfn_unclassified_build.csv'
_country_control = _import_folder + 'NPR Segmentation/processed data/Country Control 2018/nomis_CountryControl.csv'
_gb_soc_totals = _import_folder +'NPR Segmentation/raw data and lookups/LAD labour market data/nomis_SOCGBControl.csv'
_soc_lookup = 'Y:/NTS/lookups/soc_cat---XSOC2000_B02ID.csv'
_LADSOCControlPath = _import_folder + 'NPR Segmentation/raw data and lookups/LAD labour market data/nomis_lad_SOC2018_constraints.csv'
_emp_controls = _import_folder + 'NPR Segmentation/raw data and lookups/LAD labour market data/Nomis_lad_EconomicActivity3.csv'
_hc_lookup = _import_folder+'Car availability/household_composition.csv'
_emp_lookup = _import_folder+'Car availability/emp_type.csv'
_adults_lookup = _import_folder +'Car availability/adults_lookup.csv'
_lad2017 = _import_folder +'Documentation/LAD_2017.csv'
_ladsoc_control = _import_folder + 'NPR Segmentation/raw data and lookups/LAD labour market data/nomis_lad_SOC2018_constraints.csv'
ntsimportPath = _default_home_dir+'/nts_splits.csv'

def isnull_any(df):
    return df.isnull().any()

def format_english_mype(mype_m = _mype_males,
                        mype_f = _mype_females):
    """
    getting MYPE into the right format - 'melt' to get columns as rows, then rename them
    This should be a standard from any MYPE in the future segmented by gender and age
    Parameters
    ----------
    mye_males
    mye_females
    
    Returns
    ----------
    mye_2018 - one formatted DataFrame of new MYPE including population split 
    by age and gender by MSOA
    """
    mype_males = pd.read_csv(mype_m)
    mype_females = pd.read_csv(mype_f)
    
    mype = mype_males.append(mype_females)
    mype = mype.rename(columns = {'Area Codes':'ZoneID'})
    mype = pd.melt(mype, id_vars = ['ZoneID','gender'], value_vars = 
                        ['under_16', '16-74', '75 or over'])
    mype = mype.rename(columns= {'variable':'Age', 'value':'2018pop'})
    mype = mype.replace({'Age':{'under_16': 'under 16'}})
    mype = mype.replace({'gender':{'male': 'Male', 'female':'Females'}})
    
    # children are a 'gender' in NTEM, so need to sum the two rows
    mype.loc[mype['Age'] == 'under 16', 'gender'] = 'Children'
    mype['pop'] = mype.groupby(['ZoneID', 'Age', 'gender'])['2018pop'].transform('sum') 
    mype = mype.drop_duplicates()
    mype = mype.drop(columns={'2018pop'}).drop_duplicates().rename(columns={
                            'gender':'Gender'})
    cols = ['ZoneID', 'Gender', 'Age', 'pop']
    mype = mype.reindex(columns=cols)

    return(mype)
    del(mype_females, mype_males)
    print('ONS population MYE for E+W is:', mype['pop'].sum())

def format_scottish_mype( # might need changing
                         scot_f = _mypeScot_females,
                         scot_m = _mypeScot_males):
    """
    getting Scottish MYPE into the right format - 'melt' to get columns as rows, then rename them
    This should be a standard from any MYPE in the future segmented into females and males.
    Relies on the LAD to MSOA translation output from NorMITs Synthesiser (_default_lad_translation)
    
    Parameters
    ----------
    Scot_mypemales
    Scot_mypefemales
    
    Returns
    ----------
    Scot_adjust- one formatted DataFrame of new Scottish MYPE including population
    split by age and gender by MSOA
    """
    landusesegments = pd.read_csv(_landuse_segments)
    # this has the translation from LAD to MSOAs
    LadTranslation = pd.read_csv(_default_lad_translation).rename(columns={'lad_zone_id':'ladZoneID'})
    ladCols = ['objectid','lad17cd']
    ukLAD = gpd.read_file(_default_ladRef)
    ukLAD = ukLAD.loc[:,ladCols]

    Scot_females = pd.read_csv(_mypeScot_females)
    Scot_males = pd.read_csv(_mypeScot_males)
    Scot_mype = Scot_males.append(Scot_females)
    Scot_mype = Scot_mype.rename(columns = {'Area code':'lad17cd'})
    Scot_mype = pd.melt(Scot_mype, id_vars = ['lad17cd','Gender'], value_vars = 
                        ['under 16', '16-74', '75 or over'])
    Scot_mype = Scot_mype.rename(columns= {'variable':'Age', 'value':'2018pop'})
    Scot_mype.loc[Scot_mype['Age'] == 'under 16', 'Gender'] = 'Children'
    Scot_mype['2018pop'].sum()
    
    ScotLad = Scot_mype.merge(ukLAD, on = 'lad17cd')
    ScotLad = ScotLad.rename(columns={'objectid':'ladZoneID'})
       
    ScotMSOA = ScotLad.merge(LadTranslation, on = 'ladZoneID')
    # final stage of the translation from LAD to MSOA for Scotland
    ScotMSOA['people2018'] = ScotMSOA['2018pop']*ScotMSOA['lad_to_msoa']
    ScotMSOA = ScotMSOA.drop(columns={'overlap_type', 'lad_to_msoa', 'msoa_to_lad',
                                     '2018pop', 'lad17cd', 'ladZoneID'}).rename(columns={'msoa_zone_id':'ZoneID'})
   
    Scotlanduse = landusesegments[landusesegments.ZoneID.str.startswith('S')]

    Scotlandusegrouped  = Scotlanduse.groupby(
            by=['ZoneID', 'Age', 'Gender'],
            as_index=False
            ).sum().drop(columns = {'area_type', 
                 'household_composition', 'property_type'})
    
    Scot = Scotlandusegrouped.merge(ScotMSOA, how = 'outer', on = ['ZoneID', 'Gender', 'Age'])
    Scot['pop_factor'] = Scot['people2018']/Scot['people']
    Scot['newpop'] = Scot['people']*Scot['pop_factor']
    Scot = Scot.drop(columns={'people'})
    scottish_mype = Scotlanduse.merge(Scot, on =['ZoneID','Gender', 'Age'])
    scottish_mype['newpop'] = scottish_mype['people']*scottish_mype['pop_factor']
    scottish_mype = scottish_mype.drop(columns={'people', 'people2018', 
                                            'pop_factor'}).rename(
                                            columns= {'newpop':'pop'})
    cols = ['ZoneID', 'Gender', 'Age', 'pop']
    scottish_mype = scottish_mype.reindex(columns=cols)

    print('The adjusted MYPE/future year population for Scotland is', 
         scottish_mype['pop'].sum()/1000000, 'M')

    return(scottish_mype)
    
def get_ewpopulation():
    
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
    mype = format_english_mype()
    cols = ['ZoneID', 'Gender', 'Age', 'pop']
    mype = mype.reindex(columns=cols)

    print('Reading in new EW population data')
    
    return(mype)

def get_scotpopulation():
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
    scot_mype = format_scottish_mype()
    cols = ['ZoneID', 'Gender', 'Age', 'pop']
    scot_mype = scot_mype.reindex(columns=cols)

    print('Reading in new Scot population data')
    
    return(scot_mype)

def get_fy_population():
    """
    Imports fy population
    placeholder, potentially to import code developed by Chris/Liz
    """


def sort_communal_uplift(midyear = True):
    
    """
    Imports a csv of Communal Establishments 2011 and uses MYPE to uplift to MYPE (2018 for now)
    First this function takes the communal establishments and adjust for the people living
    in those property types.
    It is calulated using the splits for people by type of establishment, age and 
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
    communal:
        Path to csv of Communal Establishments 2011 sorted accordingly to age and gender and by zone.

    Returns
    ----------
    Uplifted Communal:
        DataFrame containing Communal Establishments according to the MYPE (2018).
    """
    communal = pd.read_csv(_default_communal_2011).rename(columns={'people':'communal'})
    censusoutput = pd.read_csv(_default_landuse_2011)
          
        # split landuse data into 2 pots: Scotland and E+W
    zones = censusoutput["ZoneID"].drop_duplicates().dropna()
    Scott = zones[zones.str.startswith('S')]
    EWlanduse = censusoutput[~censusoutput.ZoneID.isin(Scott)]
        
    if midyear:

        # group to ZoneID, Gender, Age to match info from MYPE
        EWlandusegroup = EWlanduse.groupby(by=['ZoneID', 'Gender', 'Age'], 
                                       as_index = False).sum().reindex(columns=
                                                            {'ZoneID', 'Gender', 
                                                             'Age', 'people'})
        # get a communal factor calculated
        communalgroup = communal.groupby(by=['ZoneID', 'Gender', 'Age'], 
                                     as_index = False).sum().reindex(
                                     columns = {'ZoneID', 'Gender', 'Age', 
                                                'communal'})
        com2011 = EWlandusegroup.merge(communalgroup, on = ['ZoneID', 'Gender', 'Age'])
        com2011['CommunalFactor'] = com2011['communal']/com2011['people']
        com2011 = com2011.rename(columns={'people':'Census'})
    
        # uplift communal to MYPE
        mype = get_ewpopulation()  
        mype_adjust = mype.merge(com2011, on = ['ZoneID', 'Gender', 'Age'], how = 'outer')
        mype_adjust['communal_mype'] = mype_adjust['pop'].values * mype_adjust['CommunalFactor'].values
        print('Communal establishments total for new MYPE is ', 
          mype_adjust['communal_mype'].sum())
    
        mype_communal = mype_adjust[['ZoneID', 'Gender', 'Age', 'communal_mype']]
        return(mype_communal)
    
    else:
    
        EWlandusegroup = EWlanduse.groupby(by=['ZoneID'], as_index = False).sum().reindex(columns={'ZoneID', 'people'})
        communalgroup = communal.groupby(by=['ZoneID'], 
                                     as_index = False).sum().reindex(
                                     columns = {'ZoneID', 'communal'})
        com2011 = EWlandusegroup.merge(communalgroup, on = ['ZoneID'])
        com2011['CommunalFactor'] = com2011['communal']/com2011['people']
        com2011 = com2011.rename(columns={'people':'Census'})
        fy = get_fy_population()
        fype_adjust = fy.merge(com2011, on = ['ZoneID'], how = 'left')
        fype_adjust['communal_fype'] = fype_adjust['pop'].values * fype_adjust['CommunalFactor'].values
        print('Communal establishments total for fy is ', 
          fype_adjust['communal_mype'].sum())

def adjust_mype():
    """
    adjust mype in EW to get rid of communal
    """
    mype_communal = sort_communal_uplift()
    ewmype = get_ewpopulation()
        
    ewmype = ewmype.merge(mype_communal, on = ['ZoneID', 'Gender', 'Age'])
    ewmype['newpop'] = ewmype['pop'] - ewmype['communal_mype'] 
    ewmype = ewmype[['ZoneID', 'Gender', 'Age', 'newpop']].rename(columns={'newpop':'pop'})
    return(ewmype)
    
        
def adjust_landuse_to_specific_yr(writeOut = True, verbose: bool = True):
    """    
    Takes adjusted landuse (after splitting out communal establishments)
    Parameters
    ----------
    landuseoutput:
        Path to csv of landuseoutput 2011 with all the segmentation (emp type, soc, ns_sec, gender, hc, prop_type), 
        to get the splits

    Returns
    ----------
    
    """
    if writeOut:
        landusesegments = pd.read_csv(_landuse_segments, usecols = ['ZoneID', 'area_type',
                                             'property_type', 'Age',
                                             'Gender', 'employment_type',
                                             'ns_sec', 'household_composition',
                                             'SOC_category', 'people']).drop_duplicates()

        # TODO: put these normalisation dictionaries in lu_constants
        gender_nt = {'Male': 2,
                     'Females': 3,
                     'Children': 1}
        age_nt = {'under 16': 1,
                  '16-74': 2,
                  '75 or over': 3}
        emp_nt = {'fte': 1,
                  'pte': 2,
                  'unm': 3,
                  'stu': 4,
                  'non_wa': 5}

        # Set inactive SOC category to 0 and normalise the data
        landusesegments['SOC_category'] = landusesegments['SOC_category'].fillna(0)
        landusesegments['gender'] = landusesegments['Gender'].map(gender_nt)
        landusesegments['age_code'] = landusesegments['Age'].map(age_nt)
        landusesegments['emp'] = landusesegments['employment_type'].map(emp_nt)
        landusesegments = landusesegments.drop(columns={'Age','Gender', 'employment_type'})
        landusesegments = landusesegments.groupby(by=['ZoneID', 'age_code', 'emp', 'gender', 'SOC_category', 'ns_sec', 'area_type', 
                                          'property_type', 'household_composition'], as_index = False).sum()
        # change to int8 to reduce table size
        
        landusesegments['age_code'] = landusesegments['age_code'].astype(np.int8)
        landusesegments['emp'] = landusesegments['emp'].astype(np.int8)
        landusesegments['gender'] = landusesegments['gender'].astype(np.int8)
        landusesegments['ns_sec'] = landusesegments['ns_sec'].astype(np.int8)
        landusesegments['SOC_category'] = landusesegments['SOC_category'].astype(np.int8)
        landusesegments['area_type'] = landusesegments['area_type'].astype(np.int8)
        landusesegments['household_composition'] = landusesegments['household_composition'].astype(np.int8)
        landusesegments['property_type'] = landusesegments['property_type'].astype(np.int8)
        
        # Get the communal establishments removed
        landusese_nocom = landusesegments[landusesegments.property_type != 8]
        
        pop_pc_totals = landusese_nocom.groupby(
                by = ['ZoneID', 'age_code', 'gender'],as_index=False
                ).sum().reindex(columns={'ZoneID', 'age_code', 'gender', 'people'})

        # LU SIMPLIFICATION
        # landusesegments == final pop for adjustment
        len_before = len(landusese_nocom)
        
        lu_index = list(landusese_nocom)
        lu_groups = lu_index.copy()
        lu_groups.remove('people')
        landusese_nocom = landusese_nocom.reindex(lu_index, axis=1).groupby(lu_groups).sum()
        landusese_nocom = landusese_nocom.reset_index()
        
        len_after = len(landusese_nocom)
        
        print('LU length %d before %d after' % (len_before, len_after))
        
        # Build simplified land use for building adjustment factors
        Scot_adjust = get_scotpopulation()
        ewmype = adjust_mype()
        
        mype_gb = ewmype.append(Scot_adjust)
        mype_gb['gender'] = mype_gb['Gender'].map(gender_nt).drop(columns={'Gender'})
        mype_gb['age_code'] = mype_gb['Age'].map(age_nt).drop(columns = {'Age'})
        
        mypepops = pop_pc_totals.merge(mype_gb, on = ['ZoneID', 'gender', 'age_code'])
        del Scot_adjust, ewmype
        mypepops['pop_factor'] = mypepops['pop']/mypepops['people']
        
        # mype simplification
        mype_before = len(mypepops)
        
        mype_index = ['ZoneID', 'gender', 'age_code', 'pop_factor']
        mype_groups = ['ZoneID', 'gender', 'age_code']
        mypepops = mypepops.reindex(mype_index, axis=1).groupby(mype_groups).sum()
        mypepops = mypepops.reset_index()
        
        mype_after = len(mypepops)
        print('MYPE length %d before %d after' % (mype_before, mype_after))
        mype_cols = list(mypepops)

        # 1. select relevant categories only - group by categories, sum
        landuse_simplecols = ['ZoneID', 'gender', 'age_code', 'people']
        landuse_simple = landusese_nocom.reindex(landuse_simplecols, axis=1).groupby(mype_groups).sum().reset_index()
        
        landuse = pd.merge(landuse_simple, mypepops, how = 'inner', on = ['ZoneID', 'gender', 'age_code'])
        landuse['adj_pop'] = landuse['people']*landuse['pop_factor'] # adjusted 2018 population
        
        # Merge adj factors onto main land use build
        landuse = pd.merge(landusese_nocom, mypepops, how = 'inner', on = ['ZoneID', 'gender', 'age_code'])
        
        landuse['newpop'] = landuse['people']*landuse['pop_factor']
        landuse = landuse.drop(columns={'people', 'pop_factor'}).rename(columns={'newpop':'people'})
        landuse_cols = ['ZoneID', 'gender', 'age_code','emp', 'SOC_category', 'ns_sec', 
                        'area_type', 'property_type', 'household_composition', 'people']
        landuse = landuse.reindex(columns = landuse_cols)
        # Check new total == mype total
        landuse['people'].sum()
        mype_gb['pop'].sum()
                
        # COMMUNAL ESTABLISHMENTS
        # Get the communal establishments 
        landuse_com = landusesegments[landusesegments.property_type == 8]

        com = sort_communal_uplift()
        com['gender'] = com['Gender'].map(gender_nt)
        com['age_code'] = com['Age'].map(age_nt)
        com = com.drop(columns={'Age', 'Gender'})
        
        pop_pc_comms = landuse_com.groupby(by=['ZoneID', 'age_code', 'gender'], 
                                              as_index = False).sum().reindex(
                                                      columns={'ZoneID', 'age_code', 
                                                               'gender', 'people'})
    
        myepops = pop_pc_comms.merge(com, on = ['ZoneID', 'gender', 'age_code'])
        myepops['pop_factor'] = myepops['communal_mype']/myepops['people']
        myepops = myepops.drop(columns={'communal_mype', 'people'})
                
        communal_pop = landuse_com.merge(myepops, on = ['ZoneID', 'gender', 'age_code'])
        communal_pop['newpop'] = communal_pop['people']*communal_pop['pop_factor']
        communal_pop['newpop'].sum()
        
        communal_pop= communal_pop.drop(columns={'people', 'pop_factor'}).rename(columns={'newpop':'people'}) 
        communal_pop = communal_pop.reindex(columns=landuse_cols)
        # need to retain the missing MSOAs for both population landuse outputs and HOPs  
        GB_adjusted = landuse.append(communal_pop)
        
        # a few checks: 
    
        print('checking for null values:', isnull_any(GB_adjusted))

        print('Full population for 2018 is now =', 
              GB_adjusted['people'].sum())
        print('check all MSOAs are present, should be 8480:', 
              GB_adjusted['ZoneID'].drop_duplicates().count())
        GB_adjusted = GB_adjusted.groupby(by = ['ZoneID', 'gender', 
                                                               'age_code','emp', 
                                                               'SOC_category', 
                                                               'ns_sec', 
                                                               'area_type', 
                                                               'property_type', 
                                                               'household_composition'
                                                               ], as_index = False).sum()
        GB_adjusted.to_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv', index = False)
        print('full GB adjusted dataset should be now saved in default iter folder')
        del(communal_pop, pop_pc_comms, myepops, landuse_com, com, landusesegments, len_before, len_after)
        gc.collect()
    else:
        print ('FY not set up yet')
              
    
def sort_out_hops_uplift():
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
    allResPropertyZonal = pd.read_csv(_default_home_dir+'/classifiedResPropertyMSOA.csv')    
    allResPropertyZonal['new_prop_type'] = allResPropertyZonal['census_property_type']
    allResPropertyZonal.loc[allResPropertyZonal['census_property_type']==5, 'new_prop_type'] = 4
    allResPropertyZonal.loc[allResPropertyZonal['census_property_type']==6, 'new_prop_type'] = 4
    allResPropertyZonal.loc[allResPropertyZonal['census_property_type']==7, 'new_prop_type'] = 4
    allResPropertyZonal = allResPropertyZonal.drop(columns = 'census_property_type').rename(columns = {'new_prop_type':'property_type'})
    allResPropertyZonal = allResPropertyZonal.groupby(by=['ZoneID', 'property_type'], as_index = False).sum().drop(columns={'msoa11cd'})
    allResPropertyZonal['household_occupancy_18'] = allResPropertyZonal['population']/allResPropertyZonal['UPRN']
        
    MYPEpop = pd.read_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv').groupby(
                by=['ZoneID', 'property_type'],as_index = False).sum()
    MYPEpop = MYPEpop.reindex(columns={'ZoneID', 'property_type', 'people'})
    
    hops = allResPropertyZonal.merge(MYPEpop, on = ['ZoneID', 'property_type'])
    hops['household_occupancy_2018_mype']= hops['people']/hops['UPRN']
    
    hops.to_csv(_default_home_dir+'/Hops Population Audits/household_occupation_comparison.csv', index = False)
    hops = hops.drop(columns = {'UPRN', 'household_occupancy_18', 'population', 'people'
                                     })
    hops= hops.rename(columns = {'household_occupancy_2018_mype': 'household_occupancy'})
    
    # check all msoas are included:
    print('check all MSOAs are present, should be 8480:', 
              hops['ZoneID'].drop_duplicates().count())
    hops.to_csv(_default_home_dir+'/Hops Population Audits/2018_household_occupancy.csv', index = False)
    
def adjust_car_availability(
                     ntsimportPath = _default_home_dir+'/nts_splits.csv',
                     midyear = True
                     ):
    """
    applies nts extract to landuse
    Parameters
    ----------
    Returns
    ----------
    """
    ntsimportPath = _default_home_dir+'/nts_splits.csv'
    if midyear:
        landuse = pd.read_csv(_landuse_segments)
    elif fy:
        landuse = pd.read_csv(fyPath)

    cars_adjust = pd.read_csv(ntsimportPath) 
        
    segments = landuse.groupby(by=['area_type', 'employment_type', 'household_composition'], as_index = False).sum().reindex(
                                        columns={'area_type', 'employment_type', 'household_composition', 'people'})
        
    segments['totals2'] = segments.groupby(['area_type','employment_type'])['people'].transform('sum')
    segments['splits2']= segments['people']/segments['totals2']
    segments['lu_total'] = segments.groupby(['employment_type'])['people'].transform('sum')
        
    # derive the new totals using the NTS splits    
    join = segments.merge(cars_adjust, on =['employment_type', 'area_type', 'household_composition'])
    join['newhc'] = join['splits']*join['totals2']
    join = join.reindex(columns=['area_type', 'employment_type', 'household_composition', 'newhc'])
    ### the below might need to be re-written to simplify
    land2 = landuse.groupby(by=['ZoneID','area_type', 
                               'employment_type'],as_index = False).sum().reindex(columns=['msoa_zone_id', 'area_type', 
                                                'household_composition', 'employment_type', 'people'])
    land2 = land2.drop(columns= {'household_composition'})
    land2['total'] = land2.groupby(['area_type', 'employment_type'])['people'].transform('sum')
    land2['factor'] = land2['people']/land2['total']
    
    allcombined2 = land2.merge(join, on = ['area_type', 'employment_type'])
    allcombined2['new'] = allcombined2['newhc']*allcombined2['factor']
    allcombined2['new'].sum()
    check = allcombined2.groupby(by=['household_composition'], as_index = False).sum()
    #check3 = allcombined2.groupby(by=[], as_index = False).sum()
    check3_l = landuse.groupby(by=['employment_type'], as_index = False).sum()
    # check2 = allcombined.groupby(by=['household_composition'],as_index = False).sum()
    
    london = allcombined2[allcombined2.msoa_zone_id == 'E02000001']
    london['new'].sum() # should be ~8706
    
    ### trying to combine all
    land2 = landuse.groupby(by=['ZoneID','area_type', 'ns_sec', 'Age', 'SOC_category', 'property_type', 'Gender',
                               'employment_type'],as_index = False).sum().reindex(columns=['ZoneID','area_type', 'ns_sec', 'Age', 'SOC_category', 'property_type', 'Gender',
                               'employment_type', 'people'])
    land2['total'] = land2.groupby(['area_type', 'employment_type'])['people'].transform('sum')
    land2['factor'] = land2['people']/land2['total']
    
    allcombined2 = land2.merge(join, on = ['area_type', 'employment_type'])
    allcombined2['new'] = allcombined2['newhc']*allcombined2['factor']
    allcombined2['new'].sum()
    allcombined2.to_csv(_default_home_dir + 'landuse_caradj.csv', index = False)

    check = allcombined2.groupby(by=['household_composition'], as_index = False).sum()
    check3 = allcombined2.groupby(by=['SOC_category'], as_index = False).sum()
    check3_l = landuse.groupby(by=['employment_type'], as_index = False).sum()
     
        
      ########  
        land = landuse.groupby(by=['msoa_zone_id','ns', 'soc', 'property_type', 'gender',
                                   'area_type', 'age', 'household_composition', 
                                   'employment_type'],as_index = False).sum()
        land = land.reindex(columns=['msoa_zone_id','ns', 'soc', 'property_type', 'gender',
                                     'area_type', 'age', 'household_composition', 
                                     'employment_type', 'people'])
        land['total']= land.groupby(['area_type', 'age', 
                                   'employment_type', 'household_composition'])['people'].transform('sum')
        land['pop_factor'] = land['people']/land['total']
        land = land.drop(columns={'people', 'total'})

        car_available = allcombined2.groupby(by=['household_composition'],as_index = False).sum()
        car_available.to_csv(_default_home_dir + '/caravailable.csv')
        
        
def adjust_soc_gb():
                   # might need changing):
    """
    To apply before the MYPE
    adjusts SOC values to gb levels for 2018
    
    """
    
    ladref = gpd.read_file(_default_ladRef).iloc[:,0:2]
    gb_soc_totals = pd.read_csv(_gb_soc_totals)

    LadTranslation = pd.read_csv(_default_lad_translation).drop(columns={'overlap_type', 
                                        'lad_to_msoa','msoa_to_lad'}).rename(columns={
                                        'msoa_zone_id':'ZoneID', 'lad_zone_id':'objectid'}
                                        )

    gb_soc_totals = gb_soc_totals.rename(columns={
            'T12a:1 (1 Managers, Directors and Senior Officials (SOC2010) : All people )':'SOC1',
            'T12a:4 (2 Professional Occupations (SOC2010) : All people )': 'SOC2',
            'T12a:7 (3 Associate Prof & Tech Occupations (SOC2010) : All people )':'SOC3',
            'T12a:10 (4 Administrative and Secretarial Occupations (SOC2010) : All people )':'SOC4',
            'T12a:13 (5 Skilled Trades Occupations (SOC2010) : All people )':'SOC5',
            'T12a:16 (6 Caring, Leisure and Other Service Occupations (SOC2010) : All people )':'SOC6',
            'T12a:19 (7 Sales and Customer Service Occupations (SOC2010) : All people )':'SOC7',
            'T12a:22 (8 Process, Plant and Machine Operatives (SOC2010) : All people )':'SOC8',
            'T12a:25 (9 Elementary occupations (SOC2010) : All people )':'SOC9'})

    SOCcols = ['Country','SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5',
                        'SOC6', 'SOC7', 'SOC8', 'SOC9']
    gb_soc_totals = gb_soc_totals.reindex(columns= SOCcols)

    Rows = ['England and Wales number', 'Scotland number']
    gb_soc_totals = gb_soc_totals[gb_soc_totals.Country.isin(Rows)]
    
    gb_soc_totals = pd.melt(gb_soc_totals, id_vars = 'Country', value_vars = ['SOC1', 
                                            'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 
                                            'SOC7', 'SOC8', 'SOC9'])
    gb_soc_totals['value'] = pd.to_numeric(gb_soc_totals['value'])
 
    gb_soc_totals =gb_soc_totals.replace({'variable':{'SOC1':1, 'SOC2':1,'SOC3':1, 
                                                'SOC4':2, 'SOC5':2, 'SOC6':2,'SOC7':2,
                                                'SOC8':3, 'SOC9':3}})
    gb_soc_totals = gb_soc_totals.rename(columns={'variable':'SOC_category'})
    gb_soc_totals = gb_soc_totals.groupby(by=['Country', 'SOC_category'], as_index = False).sum()
    
    gb_soc_totals['total']= gb_soc_totals.groupby(['Country'])['value'].transform('sum')
    gb_soc_totals['splits'] = gb_soc_totals['value']/gb_soc_totals['total']
    
    ########################## CALCULATE COUNTRY SPLITS FROM LANDUSE ########
    landusesegments = pd.read_csv(_default_home_dir+ '/GBlanduseControlled.csv')
    Emp = [1, 2]
    Employed = landusesegments[landusesegments.emp.isin(Emp)]
    Employed = Employed.merge(LadTranslation, on = 'ZoneID')
    zones = landusesegments["ZoneID"].drop_duplicates().dropna()
    Employed['Country']= 'England and Wales number'
    Employed.loc[Employed['ZoneID'].str.startswith('S'), 'Country']= 'Scotland number'
    
    EmpSOCTotal = Employed.groupby(by=['Country', 'SOC_category'],
                                   as_index = False).sum().reindex(
                                           columns=['Country', 'SOC_category', 'people'])
    EmpSOCTotal['total_land'] = EmpSOCTotal.groupby(['Country'])['people'].transform('sum')
    
    # for audit
    EmpSOCTotal['splits_land']= EmpSOCTotal['people']/EmpSOCTotal['total_land']
    # EmpSOCTotal = EmpSOCTotal.drop(columns={'newpop', 'total_land'})
    EmpSOCTotal['SOC_category'] = pd.to_numeric(EmpSOCTotal['SOC_category'])
    
    #save for comparison
    EmpCompare = EmpSOCTotal.merge(gb_soc_totals, on = ['Country', 'SOC_category'], 
                                   how = 'left').drop(columns={'people', 'splits_land', 'value', 'total'})

    EmpCompare.to_csv(_default_home_dir + '/SOCsplitsComparison.csv')
    
    EmpCompare['pop'] = EmpCompare['splits']*EmpCompare['total_land']
    print(EmpCompare['pop'].sum())
    EmpCompare = EmpCompare.drop(columns={'total_land', 'splits'})
    
    LanduseGrouped = Employed.groupby(by=['Country', 'SOC_category'],as_index = False).sum().reindex(
                                                        columns={'Country', 'SOC_category', 'people'})
    LanduseGrouped['total'] = LanduseGrouped.groupby(['Country'])['people'].transform('sum')
    LanduseGrouped['factor']= LanduseGrouped['people']/LanduseGrouped['total']
    
    SOCrevised = EmpCompare.merge(LanduseGrouped, on = ['Country', 'SOC_category'], how= 'left')
    SOCrevised['factor'] = SOCrevised['pop']/SOCrevised['people']
    SOCrevised = SOCrevised.reindex(columns={'Country', 'SOC_category', 'factor'})
    Soc_adjusted = Employed.merge(SOCrevised, on = ['Country', 'SOC_category'])
    
    Soc_adjusted['newpop']= Soc_adjusted['factor']*Soc_adjusted['people']
    print(Soc_adjusted['people'].sum())
    Soc_adjusted = Soc_adjusted.drop(columns={'factor'})
                    
    NPRSegments = ['ZoneID', 'area_type', 'property_type', 'emp', 
                   'age_code', 'gender', 'household_composition',
                   'ns_sec', 'SOC_category', 'newpop'] 

    Soc_adjusted = Soc_adjusted.reindex(columns=NPRSegments)
    print(Soc_adjusted['newpop'].sum())
    Soc_adjusted = Soc_adjusted.rename(columns={'newpop':'people'})
    # join to the rest
    
    NotEmployed = landusesegments[~landusesegments.emp.isin(Emp)]
    NPRSegmentation = NotEmployed.append(Soc_adjusted)
    NPRSegmentation['people'].sum()
    
    NPRSegmentation.to_csv(_default_home_dir+'/landuse_adjustedSOCs.csv')

def adjust_soc_lad ():
    """
    lad translation path has changed here - needs updating

    """           
    ladref = pd.read_csv(_default_lad_translation).iloc[:,0:2]

    # format the LAD controls data
    LADSOCcontrol = pd.read_csv(_ladsoc_control)
    LADSOCcontrol = LADSOCcontrol.rename(columns={
            '% all in employment who are - 1: managers, directors and senior officials (SOC2010) numerator':'SOC1',
            '% all in employment who are - 2: professional occupations (SOC2010) numerator': 'SOC2',
            '% all in employment who are - 3: associate prof & tech occupations (SOC2010) numerator':'SOC3',
            '% all in employment who are - 4: administrative and secretarial occupations (SOC2010) numerator':'SOC4',
            '% all in employment who are - 5: skilled trades occupations (SOC2010) numerator':'SOC5',
            '% all in employment who are - 6: caring, leisure and other service occupations (SOC2010) numerator':'SOC6',
            '% all in employment who are - 7: sales and customer service occupations (SOC2010) numerator':'SOC7',
            '% all in employment who are - 8: process, plant and machine operatives (SOC2010) numerator':'SOC8',
            '% all in employment who are - 9: elementary occupations (SOC2010) numerator':'SOC9'})
    LADcols = ['lad17cd','SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5',
                        'SOC6', 'SOC7', 'SOC8', 'SOC9']
    LADSOCcontrol = LADSOCcontrol.reindex(columns= LADcols)
    LADSOCcontrol= LADSOCcontrol.replace({'!':0, '~':0, '-':0, '#':0})

    LADSoc = pd.melt(LADSOCcontrol, id_vars = 'lad17cd', 
                     value_vars = ['SOC1', 'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 
                                'SOC7', 'SOC8', 'SOC9'])
    LADSoc['value'] = pd.to_numeric(LADSoc['value'])
    LADSoc =LADSoc.replace({'variable':{'SOC1':1, 'SOC2':1,'SOC3':1, 
                                                'SOC4':2, 'SOC5':2, 'SOC6':2,'SOC7':2,
                                                'SOC8':3, 'SOC9':3}})
    LADSoc = LADSoc.rename(columns={'variable':'SOC_category'})
    LADSoc['total'] = LADSoc.groupby(['lad17cd'])['value'].transform('sum')
    LADSoc['splits'] = LADSoc['value']/LADSoc['total']
    
    LADSoc = LADSoc.groupby(by=['lad17cd','SOC_category'], as_index = False).sum()
   
    ####################### READ IN POPULATION TO ADJUST ####################
    LadTranslation = pd.read_csv(_default_lad_translation).drop(columns={'overlap_type', 
                                        'lad_to_msoa','msoa_to_lad'}).rename(columns={
                                        'msoa_zone_id':'ZoneID'}
                                        ).merge(ladref, on = 'lad_zone_id')

    landuse = pd.read_csv(_default_home_dir+'/landuse_adjustedSOCs.csv')
    Emp = ['fte', 'pte']
    Employed = landuse[landuse.employment_type.isin(Emp)]

    print('Employed people in landuse: ', Employed['people'].sum())

    SOCtotals2LAD = Employed.groupby(by=['ZoneID', 'SOC_category'], as_index = False).sum().drop(
            columns={'area_type'})
    SOCtotals2LAD = SOCtotals2LAD.merge(LadTranslation, on = 'ZoneID')
    SOCtotals2LAD = SOCtotals2LAD.groupby(by=['lad_zone_id', 'SOC_category'], as_index = False).sum().reindex(columns=['lad_zone_id', 'SOC_category', 'people'])
    
    SOCtotalsMSOA = Employed.groupby(by=['ZoneID', 'SOC_category'], as_index = False).sum()
    SOCtotalsMSOA = SOCtotalsMSOA.merge(LadTranslation, on = 'ZoneID')
    SOCtotalsMSOA = SOCtotalsMSOA.groupby(by=['ZoneID', 'lad_zone_id'], as_index = False).sum().reindex(columns=['ZoneID','lad_zone_id', 'people'])
"""
    ladref = gpd.read_file(_default_ladRef).iloc[:,0:2]
    landuse = landuse.merge(LadTranslation, on = 'ZoneID', how='left')
    LADSoc = LADSoc.merge(ladref, on = 'objectid')
"""    
    Compare = SOCtotalsMSOA.merge(LADSoc, on = ['lad17cd'])
    Compare['people'] = Compare['newpop']*Compare['splits']

    Compare = SOCtotalsLAD.merge(LADSoc, on = ['lad17cd'])
    Compare['newvalue']= Compare['newpop']*Compare['splits']      
    Compare = Compare.drop(columns={'newop', 'value', 't})

    Grouped = Employed.groupby(by=['ZoneID', 'property_type', 'Age', 'Gender', 
                                  'household_composition', 'area_type','ns_sec'],as_index = False).sum()
    
    Grouped = Grouped.merge(Compare, on =['lad17cd'],how= 'left')
    LADgrouped = Employed.groupby(by=['ZoneID', 'lad17cd'], as_index = False).sum()
            
    Compare['factor'] = Compare['value']/Compare['newpop']
    Compare = Compare.drop(columns={'newpop', 'value'})
    
    ######################### JOIN BACK TO MSOA LEVEL ###############################
    #Socs['SOC_category']=pd.to_numeric(Socs['SOC_category'])
   # SOCs =[1,2,3]
   # landuseSOC = AllSOCs[AllSOCs.SOC_category.isin(SOCs)]
   # landuseNoSOC = AllSOCs[~AllSOCs.SOC_category.isin(SOCs)]       
    landuseSOC = Employed.merge(LadTranslation, on = 'ZoneID')
    landuseSOC2 = landuseSOC.groupby(by=['ZoneID', 'lad17cd','SOC_category'], as_index = False).sum()
    landuseSOC2['SOC_category']= pd.to_numeric(landuseSOC2['SOC_category'])
    landuseSOC2 = landuseSOC2.merge(Compare, on = ['lad17cd', 'SOC_category'], how = 'left')
    landuseSOC2['pop']= landuseSOC2['newpop']*landuseSOC2['factor']
    landuseSOC2['pop'].sum()
    landuseSOC2 = landuseSOC2.drop(columns={'property_type', 'household_composition',
                                            'ns_sec'})
    landuseSOC2 =landuseSOC2.drop(columns={'area_type', 'objectid_x', 'objectid_y', 'factor'})
    landuseSOC2 = landuseSOC2.drop(columns={'newpop'})
    AdjustedPopulation = landuseSOC.groupby(by=['ZoneID', 'lad17cd', 'property_type', 'household_composition',
                                                'Age', 'Gender', 'employment_type', 'ns_sec', 'SOC_category'], as_index = False).sum()
    AdjustedPopulation = AdjustedPopulation.drop(columns={'SOC_category'})    
    
    AdjustedPopulation = AdjustedPopulation.merge(landuseSOC2, on = ['lad17cd'], how = 'left')
    AdjustedPopulation['pop2']=AdjustedPopulation['pop']*AdjustedPopulation['newpop']
    AdjustedPopulation['pop2'].sum()
    AdjustedPopulation[AdjustedPopulation.lad17cd == 'E06000005']
    
    Socs = SOCtotals.merge(Compare, on = ['lad17cd', 'SOC_category'])   
    Socs['newpop2'] = Socs['newpop']*Socs['factor']
    Socs['newpop2'].sum()
    Socs = Socs.drop(columns={'newpop'})
######### JOIN TO THE REST ##############
    Rest = Socs.groupby(by=['ZoneID', 'area_type', 'Gender', 'Age', 'household_composition', 
                    'ns_sec'], as_index = False).sum().drop(columns={'SOC_category'})
    Join = Socs2.merge(Rest, on =['ZoneID', 'area_type', 'objectid'], how = 'left')
    Join['pop']=Join['newpop2']*Join['newpop']
    Join = Join.replace(np.inf, 0)
    Join['pop'].sum() 
    
     
    Zonefactor = Socs.groupby(by=['ZoneID', 'Gender', 'Age', ''])
    SOCtotals['newpop2'] = SOCtotals['newpop']*SOCtotals['factor']
    check = SOCtotals['newpop2'].sum()
    
    GBlanduse = pd.read_csv(_default_home_dir + 'landuseGBMYE_flatcombined.csv')
    
    All = pd.read_csv(_default_home_dir + 'NPRSegments_stage1.csv')
    All2 = All.groupby(by=['ZoneID', 'property_type', 'Age', 'employment_type',
                           'ns_sec', 'SOC_category'],as_index = False).sum()
    All = All.drop_duplicates()
    SOCcheck =  All.groupby(['ns_sec', 'SOC_category'], as_index = False).sum()
    nonwa = ['non_wa']
    Nonwa = All[All.employment_type.isin(nonwa)]
    All['SOC_category'] = All['SOC_category'].fillna('NA')

    return All
    
def control_to_lad_employment_ag():
    """
    control to employment at LAD level for age, gender and fte/pte employment
    
    """
    landuse = pd.read_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv')

    LadTranslation = pd.read_csv(_default_lad_translation).drop(columns={'lad_to_msoa', 'msoa_to_lad', 
                                'overlap_type'}).rename(columns = {'msoa_zone_id':'ZoneID', 
                                              'lad_zone_id':'objectid'})                                   
    ladref = gpd.read_file(_default_ladRef).iloc[:,0:2]
    landuse = landuse.merge(LadTranslation, on = 'ZoneID', how='left')
    landuse = landuse.merge(ladref, on = 'objectid')
    
    EmpControls = pd.read_csv(_emp_controls)
    EmpControls = EmpControls.rename(columns={
            'T08:29 (Males - Aged 16 - 64 : Full-time ) number':'Male FTE',
            'T08:30 (Males - Aged 16 - 64 : Part-time ) number':'Male PTE',
            'T08:44 (Females - Aged 16 - 64 : Full-time ) number':'Females FTE',
            'T08:45 (Females - Aged 16 - 64 : Part-time ) number':'Females PTE',   
            'T01:8 (All aged 16 & over - In employment : Males ) number':'Male Emp',
            'T01:9 (All aged 16 & over - In employment : Females ) number':'Females Emp',
            })

    EmpControls = EmpControls.replace({'!':0, '~':0, '-':0, '#':0, ',':''})                                              
    LADEmpGenControls  = EmpControls[['lad17cd', 
                                     'Male Emp', 'Females Emp']]
                                                
    LADcontrolled = pd.melt(LADEmpGenControls , id_vars = 'lad17cd',
                            value_vars = ['Male Emp', 'Females Emp'
                                          ])
    #, 'Male Inc', 'Females Inc'])
    LADcontrolled['gender'] = 2
    LADcontrolled['employment_cat']= 'emp'
    # format the dataframe
    LADcontrolled.loc[LADcontrolled['variable']=='Male Emp', 'employment_cat']='emp'
    LADcontrolled.loc[LADcontrolled['variable']=='Male Emp', 'gender'] = 2
    LADcontrolled.loc[LADcontrolled['variable']=='Females Emp', 'employment_cat']='emp'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Emp', 'gender']= 3    
    LADcontrolled['value'] = pd.to_numeric(LADcontrolled['value'])
    
    LADcontrolled = LADcontrolled.groupby(by=['lad17cd', 'employment_cat', 'gender'], as_index = False).sum()
    
    #WORK OUT THE TOTAL FOR 16-74 working age population
    WAAll = landuse[landuse.emp != 5]
    TotalWAPop = WAAll.groupby(by =['lad17cd','gender'
                                       ],as_index = False).sum().drop(columns={
                                                 'household_composition', 'area_type', 
                                                 'property_type', 'objectid', 'SOC_category',
                                                 'ns_sec', 'age_code'})
    LADcontrolledWA = LADcontrolled.merge(TotalWAPop, on = ['lad17cd','gender'], how='left')
    LADcontrolledWA['splits']= LADcontrolledWA['value']/LADcontrolledWA['people']
    
    # for Isles of Scilly E06000053:
    LADcontrolledAverage = LADcontrolledWA.groupby(by=['employment_cat'], as_index = False).sum()
    LADcontrolledAverage['av_splits'] = LADcontrolledAverage['value']/LADcontrolledAverage['people']
    LADcontrolledAverage = LADcontrolledAverage.drop(columns={'value', 'people', 'splits', 'gender', 'emp'})
    LADcontrolledWA = LADcontrolledWA.drop(columns={'value', 'people'})

    # filling in splits where they're missing
    LADcontrolled2 = LADcontrolledWA.merge(LADcontrolledAverage, on = 'employment_cat',
                                          how = 'right')
    LADcontrolled2.loc[LADcontrolled2.lad17cd == 'E06000053','splits']=LADcontrolled2['av_splits']
    LADcontrolled2['splits'] = LADcontrolled2['splits'].fillna(LADcontrolled2['av_splits'])
    LADcontrolled2 = LADcontrolled2.drop(columns={'av_splits'})
    LADcontrolled2.loc[LADcontrolled2['lad17cd']=='E09000001', 'splits']=1
    LADcontrolled2['inactivesplits']= 1 - LADcontrolled2['splits']    
    LADcontrolled2 = LADcontrolled2.drop(columns={'emp'})
    
    # FTE/PTE splits
    LADftepteControls = EmpControls[['lad17cd', 'Male FTE', 'Females FTE',
                                           'Male PTE', 'Females PTE']]      
    LADftepteControls = pd.melt(LADftepteControls, id_vars = 'lad17cd',
                            value_vars = ['Male FTE', 'Females FTE',
                                          'Male PTE','Females PTE'])

    LADftepteControls['gender'] = 2
    LADftepteControls['emp']= 3

    LADftepteControls.loc[LADftepteControls['variable']=='Male FTE', 'emp']=1
    LADftepteControls.loc[LADftepteControls['variable']=='Male FTE', 'gender']= 2
    LADftepteControls.loc[LADftepteControls['variable']=='Male PTE', 'emp']=2
    LADftepteControls.loc[LADftepteControls['variable']=='Male PTE', 'gender']=2
    LADftepteControls.loc[LADftepteControls['variable']=='Females FTE', 'emp']=1
    LADftepteControls.loc[LADftepteControls['variable']=='Females FTE', 'gender']=3
    LADftepteControls.loc[LADftepteControls['variable']=='Females PTE', 'emp']=2
    LADftepteControls.loc[LADftepteControls['variable']=='Females PTE', 'gender']=3
    LADftepteControls = LADftepteControls.drop(columns={'variable'})
    LADftepteControls['value'] = pd.to_numeric(LADftepteControls['value'])

    LADftepteControls = LADftepteControls.groupby(by=['lad17cd', 'emp', 'gender'], as_index = False).sum()
    
    LADftepteControls['totals'] = LADftepteControls.groupby(['lad17cd','gender'])['value'].transform('sum')
    LADftepteControls['splits']= LADftepteControls['value']/LADftepteControls['totals']
    
    LADftepteControlsAverage = LADftepteControls.groupby(by=['emp'], as_index = False).sum().drop(columns={'gender'})
    LADftepteControlsAverage['av_splits'] = LADftepteControlsAverage['value']/LADftepteControlsAverage['totals']
    LADftepteControlsAverage = LADftepteControlsAverage.drop(columns={'value', 'totals', 'splits'})

    LADftepteControls = LADftepteControls.merge(LADftepteControlsAverage, on = 'emp', how = 'right')
    LADftepteControls['splits'] = LADftepteControls['splits'].fillna(LADftepteControls['av_splits'])

    LADftepteControls = LADftepteControls.drop(columns={'av_splits','value', 'totals'})
    LADftepteControls['employment_cat'] = 'emp'

    #SUM UP The Population by LAD and APPLY the splits between inactive and active 
    
    WAAll = landuse[landuse.emp != 5]
    LanduseLAD = WAAll.groupby(by =['ZoneID', 'lad17cd','gender'
                                       ],as_index = False).sum().reindex(
                                        columns={'ZoneID', 'lad17cd', 'gender', 'people'})
    FactoredEmp = LanduseLAD.merge(LADcontrolled2, on = ['lad17cd', 'gender'], 
                                         how = 'left')
    FactoredEmp['newpop'] = FactoredEmp['people']*FactoredEmp['splits']
    
    FactoredEmp['newpop'].sum()
    FactoredEmp = FactoredEmp.drop(columns={'people', 'splits', 'inactivesplits'})
            
    # Emp Active Split by fte/pte 
    Employed = FactoredEmp.merge(LADftepteControls, on = ['lad17cd', 'gender', 
                                                          'employment_cat'], how = 'left')
    Employed['pop']= Employed['newpop']*Employed['splits']
    Employed = Employed.drop(columns={'newpop', 'splits', 'employment_cat'})
    Employed['pop'].sum()
    
    InEmployment = [1,2]
    ActiveLanduse = WAAll[WAAll.emp.isin(InEmployment)]
    ActiveLanduseGrouped = ActiveLanduse.groupby(by=['ZoneID', 'lad17cd', 'gender', 'emp'],
                             as_index = False).sum().drop(columns={'household_composition', 
                                                  'area_type', 'objectid', 'property_type',
                                                  'ns_sec', 'SOC_category'})
    # to work out fte/pte 
    ActiveComp = ActiveLanduseGrouped.merge(Employed, on = ['ZoneID', 'lad17cd', 'gender', 'emp'], how = 'left')
    ActiveComp['factor']= ActiveComp['pop']/ActiveComp['people']
    ActiveComp = ActiveComp.drop(columns={'people', 'pop'})
    
    # JOIN BACK TO FTE/PTE LANDUSE LEVEL 
    ActiveLanduse2 = ActiveLanduse.merge(ActiveComp, on = ['ZoneID', 'lad17cd', 
                                                           'gender', 'emp'
                                                           ], how = 'left')

    ActiveLanduse2['newpop'] = ActiveLanduse2['people']*ActiveLanduse2['factor']
    ActiveLanduse2['newpop'].sum()
   
    ActiveLanduse2 = ActiveLanduse2.drop(columns={'factor'})

    checkActive = ActiveLanduse2.groupby('emp', as_index = False).sum()  

    
    # Inactive and Unemployed
    Inact = [3, 4]
    LanduseLAD = WAAll.groupby(by =['ZoneID', 'lad17cd','gender'
                                       ],as_index = False).sum().drop(columns={
                                                 'household_composition', 'area_type', 
                                                 'property_type', 'objectid', 'ns_sec',
                                                 'SOC_category'})
    FactoredInc = LanduseLAD.merge(LADcontrolled2, on = ['lad17cd', 'gender'], 
                                         how = 'left')
    FactoredInc['newpop'] = FactoredInc['people']*FactoredInc['inactivesplits']
    
    FactoredInc['newpop'].sum()
    FactoredInc = FactoredInc.drop(columns={'people', 'inactivesplits', 'splits'})

    # compare      
    InactiveLanduse = WAAll[WAAll.emp.isin(Inact)]
    InactiveLanduseGrouped = InactiveLanduse.groupby(by=['ZoneID', 'lad17cd', 'gender'],
                             as_index = False).sum().drop(columns={'household_composition', 
                                                  'area_type', 'objectid', 'property_type', 
                                                  'age_code', 'emp', 'SOC_category', 'ns_sec'})
    InactiveComp = InactiveLanduseGrouped.merge(FactoredInc, on = ['ZoneID', 'lad17cd', 
                                                                   'gender'], how = 'left').drop(columns={'age_code', 'emp'})
    InactiveComp['factor']= InactiveComp['newpop']/InactiveComp['people']
    InactiveComp = InactiveComp.drop(columns={'people', 'newpop'})
    
    InactiveLanduse2 = InactiveLanduse.merge(InactiveComp, on = ['ZoneID', 'lad17cd', 'gender'], how = 'left')
    InactiveLanduse2['newpop'] = InactiveLanduse2['people']*InactiveLanduse2['factor']
    InactiveLanduse2['newpop'].sum()
    
    #checkInactive = InactiveLanduse2.groupby('employment_type', as_index = False).sum()
    
    GBcols = ['ZoneID', 'age_code', 'emp', 'area_type', 'property_type',
              'household_composition', 'Gender', 'objectid', 'lad17cd', 'SOC_category', 
              'ns_sec', 'newpop']
    InactiveLanduse2 = InactiveLanduse2.reindex(columns = GBcols)
    ActiveLanduse2 = ActiveLanduse2.reindex(columns = GBcols)
    GBlanduseControlled = InactiveLanduse2.append(ActiveLanduse2)

    GBlanduseControlled['newpop'].sum()
    NowaAll = landuse[landuse.emp == 5]

    NowaAll = NowaAll.rename(columns={'people':'newpop'})
    NowaAll = NowaAll.reindex(columns=GBcols)
    NowaAll['newpop'].sum()
    GBlanduseControlled = GBlanduseControlled.append(NowaAll)
    GBlanduseControlled['newpop'].sum()
    GBlanduseControlled = GBlanduseControlled.rename(columns={'newpop':'people'})
    GBlanduseControlled.to_csv(_default_home_dir + 'GBlanduseControlled.csv')
    
    return GBlanduseControlled
    
def country_emp_control():
    """
    this function is to make sure we have the right amount of people in work 
    Based on APS extract (as of 2018)
    
    """
    # Country employment control
    CountryControl = pd.read_csv(_country_control)
    CountryEmp = CountryControl.rename(columns={
            'T01:7 (All aged 16 & over - In employment : All People )':'Emp'
            })
    CountryEmp = CountryEmp[['Country','Emp']]
    
    Rows = ['England and Wales number', 'Scotland number']
    CountryEmpControl = CountryEmp[CountryEmp.Country.isin(Rows)]

    landuse = pd.read_csv(_default_home_dir + 'AdjustedGBlanduse.csv') 
    Employed = [1,2]
    zones = landuse['ZoneID'].drop_duplicates()
    Scott = zones[zones.str.startswith('S')]

    Active = landuse[landuse.emp.isin(Employed)]
    ScottActive = Active[Active.ZoneID.isin(Scott)]
    ScottActive['Country'] = 'Scotland number'
    ScottActiveTotal = ScottActive.groupby(by=['Country'], as_index = False).sum().reindex(columns=['Country', 'people'])

    ScottActiveTotal= ScottActiveTotal.merge(CountryEmpControl,on='Country')
    ScottActiveTotal['factor'] = ScottActiveTotal['Emp']/ScottActiveTotal['people']
    ScottActiveTotal =ScottActiveTotal.drop(columns={'people', 'Emp'})
    ScottActive = ScottActive.merge(ScottActiveTotal, on = 'Country')
    ScottActive['newpop']= ScottActive['people']*ScottActive['factor']
    ScottActive = ScottActive.drop(columns={'people', 'factor', 'Country'})
    
    EngActive = Active[~Active.ZoneID.isin(Scott)]
    EngActive['Country']='England and Wales number'
    EngActiveTotal = EngActive.groupby(by=['Country'], as_index = False).sum().reindex(columns=['Country', 'people'])
    EngActiveTotal= EngActiveTotal.merge(CountryEmpControl,on='Country')
    EngActiveTotal['factor'] = EngActiveTotal['Emp']/EngActiveTotal['people']
    EngActiveTotal =EngActiveTotal.drop(columns={'people', 'Emp'})
    EngActive = EngActive.merge(EngActiveTotal, on = 'Country')
    EngActive['newpop']= EngActive['people']*EngActive['factor']
    EngActive = EngActive.drop(columns={'people', 'factor', 'Country'})
    EngActive['newpop'].sum()
    GBcols2 = ['ZoneID', 'Age', 'employment_type', 'area_type', 'property_type',
              'household_composition', 'Gender', 'objectid', 'lad17cd', 'people']

    ActiveAdj = EngActive.append(ScottActive)
    ActiveAdj = ActiveAdj.rename(columns={'newpop':'people'})
    ActiveAdj['people'].sum()
    ActiveAdj = ActiveAdj.reindex(columns=GBcols2)
    ActiveNewTotal = ActiveAdj.groupby('ZoneID',as_index = False).sum().reindex(columns={'ZoneID', 'people'})
    ActiveNewTotal = ActiveNewTotal.rename(columns={'people':'employed'})
        
    GBTotals = landuse[landuse.emp != 5]
    GBTotals['people'].sum()
    GBTotals = GBTotals.groupby('ZoneID', as_index = False).sum().reindex(columns={'ZoneID', 'people'})
    GBTotals = GBTotals.merge(ActiveNewTotal, on = 'ZoneID')
    GBTotals['inactive'] = GBTotals['people']- GBTotals['employed']
    GBTotals = GBTotals.drop(columns={'people', 'employed'})
    
    Inact = [3,4]
    Inactive = landuse[landuse.emp.isin(Inact)]
    Inactive2 = Inactive.groupby(by=['ZoneID'],as_index = False).sum().reindex(columns=['ZoneID', 'people'])
    Inactive2 = Inactive2.merge(GBTotals, on = 'ZoneID')
    Inactive2['factor']= Inactive2['inactive']/Inactive2['people']
    Inactive2 = Inactive2.replace([np.inf],0)
    Inactive2 =Inactive2.reindex(columns=['ZoneID', 'factor'])
    Inactive3 = Inactive.merge(Inactive2, on = 'ZoneID')
    Inactive3['newpop']= Inactive3['factor']*Inactive3['people']
    Inactive3['newpop'].sum()        
    Inactive3 = Inactive3.drop(columns={'people'})
    Inactive3 = Inactive3.rename(columns={'newpop':'people'})
    Inactive3 = Inactive3.reindex(columns=GBcols2)
    
    AdjustedGBlanduse = Inactive3.append(NowaAll).append(ActiveAdj)
    AdjustedGBlanduse.to_csv(_default_home_dir + 'AdjustedGBlanduse_emp.csv') 
    AdjustedGBlandsue['people'].sum()
    #final check 
    check = AdjustedGBlanduse.groupby(by=['ZoneID'], as_index = False).sum()
    
def run_mype(midyear = True):
    # normalise_landuse()
    adjust_landuse_to_specific_year()
    control_to_lad_employment_ag()
    country_emp_control()
    adjust_soc_gb()
    adjust_soc_lad()
    sort_out_hops_uplift() # order doesn't matter for this one
    get_ca()
    adjust_car_availability()
