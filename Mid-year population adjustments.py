# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:58:57 2021

@author: ESRIAdmin

Mid-year population uplift
- get MYPE

#TODO: change to lowercase for functions
once run the source data should be ready for other functions
# TODO: change the RD06 to categiry 4 = 'flats' in next iter
# TODO: change 'ONS reports 27.4M in 2017' to be VOA based
# TODO: take into account classifications for communal establishments
# TODO: change values/characters to enumerations 
 1. Work out communal %s per MSOA for males and females in each MSOA
 2. Uplift everything to 2018 using ONS Mid-year population estimates
 3. Adjust employment using GB control
 4. Adjust SOC
 5. Control to LAD Employment
 6. Control to NS-SEC 2018 
"""
import os # File operations
import sys 
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Demand Tool/Python/ZoneTranslation')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/TAME shared resources/Python/')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Utilities/Python')

import numpy as np # Vector operations
import pandas as pd # main module
import geopandas as gpd
from shapely.geometry import *

import nu_project as nup

# Default file paths

_default_iter = 'iter4'
_default_home = 'D:/NorMITs_Export/'
_default_home_dir = ('D:/NorMITs_Export/' + _default_iter)
_import_folder = 'Y:/NorMITs Land Use/import/'
_import_file_drive = 'Y:/'
_default_zone_folder = ('I:/NorMITs Synthesiser/Zone Translation/')
# Default zone names
_default_zone_names = ['LSOA','MSOA']
_default_zone_name = 'MSOA' #MSOA or LSOA

_default_communal_2011 = (_default_home_dir+'/CommunalEstablishments/'+_default_zone_name+'CommunalEstablishments2011.csv')
_default_landuse_2011 = (_default_home_dir+'/landuseOutput'+_default_zone_name+'_withCommunal.csv')
_default_property_count = (_default_home_dir+'/landuseOutput'+_default_zone_name+'.csv')
_default_lad_translation = (_default_zone_folder+'Export/lad_to_msoa/lad_to_msoa.csv')    
_default_census_dat = (_import_folder+'Nomis Census 2011 Head & Household')
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_area_types = ('Y:/NorMITs Land Use/import/area_types_msoa.csv')
    
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
_ward_to_msoa = 'Y:/NTS/new area type lookup/uk_ward_msoa_pop_weighted_lookup.csv'
_nts_path = 'Y:/NTS/import/tfn_unclassified_build.csv'

def format_english_mype():
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
    mype_males = pd.read_csv(_mype_males)
    mype_females = pd.read_csv(_mype_females)
    
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
    cols = ['ZoneID', 'Gender', 'Age', 'Gender', 'pop']
    mype = mype.reindex(columns=cols)

    return(mype)
    del(mype_females, mype_males)
    print('ONS population MYE for E+W is:', mype['pop'].sum())

def format_scottish_mype():
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
    #Scot = landuseoutput.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
    Scot['newpop'] = Scot['people']*Scot['pop_factor']
    Scot = Scot.drop(columns={'people'})
    scot_mype = Scotlanduse.merge(Scot, on =['ZoneID','Gender', 'Age'])
    scot_mype['newpop'] = Scot_mype['people']*Scot_mype['pop_factor']
    Scot_mype = Scot_mype.drop(columns={'people', 'people2018', 
                                            'pop_factor'}).rename(
                                            columns= {'newpop':'people'})
    cols = ['ZoneID', 'Gender', 'Age', 'people']
    Scot_mype = Scot_mype.reindex(columns=cols)

    print('The adjusted MYPE/future year population for Scotland is', 
         Scot_mype['people'].sum()/1000000, 'M')

    return(Scot_mype)
    
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

    print('Reading in new population data')
    
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

    print('Reading in new population data')
    
    return(scot_mype)

def get_fy_population():
    """
    Imports fy population
    placeholder, need to import code developed by Liz
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
    if midyear:
        communal = pd.read_csv(_default_communal_2011).rename(columns={'people':'communal'})
        censusoutput = pd.read_csv(_default_landuse_2011)
         
        # split landuse data into 2 pots: Scotland and E+W
        zones = censusoutput["ZoneID"].drop_duplicates().dropna()
        Scott = zones[zones.str.startswith('S')]
        EWlanduse = censusoutput[~censusoutput.ZoneID.isin(Scott)]
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
        mye_adjust = mype.merge(com2011, on = ['ZoneID', 'Gender', 'Age'], how = 'outer')
        mye_adjust['communal_mype'] = mye_adjust['pop'].values * mye_adjust['CommunalFactor'].values
        print('Communal establishments total for new MYPE is ', 
          mye_adjust['communal_mype'].sum())
    
        mye_adjust['mype_notcommunal'] = mye_adjust['pop'].values - mye_adjust['communal_mype'].values
        mye_adjust = mye_adjust.drop(columns={ 'pop', 'CommunalFactor', 'Census', 'communal'})
        return (mye_adjust)
        print('Population total for England & Wales after adjusting for communal establishments is:', 
          mye_adjust['mype_notcommunal'].sum())
    else:
    
        print('FY not set up yet')
        
def adjust_landuse_to_MYPE(midyear = True):
    """    
    Takes adjusted landuse (after splitting out communal establishments)
    Parameters
    ----------
    landuseoutput:
        Path to csv of landuseoutput 2011 with all the segmentaion (emp type, soc, ns_sec, gender, hc, prop_type), 
        to get the splits

    Returns
    ----------
    
    """
    # summarise land use output
    landusesegments = pd.read_csv(_landuse_segments)
    landusesegments['SOC_category'] = landusesegments['SOC_category'].fillna(0)
    
    if midyear:

        pop_pc_totals = landusesegments.groupby(
            by = ['ZoneID', 'Age', 'Gender'],
            as_index=False
            ).sum().reindex(columns={'ZoneID', 'Age', 'Gender', 'people'})
    
        mye_adjust = sort_communal_uplift()
        myepops = pop_pc_totals.merge(mye_adjust, on = ['ZoneID', 'Gender', 'Age'])
    
        myepops['pop_factor'] = myepops['mype_notcommunal']/myepops['people']
        myepops = myepops.drop(columns={'people'})
        popadj = landusesegments.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
        popadj['newpop'] = popadj['people']*popadj['pop_factor']
        print('The adjusted 2018 population for England and Wales is', 
          popadj['newpop'].sum()/1000000, 'M')

        # need to retain the missing MSOAs for both population landuse outputs and HOPs
        popadj = popadj.drop(columns = {'pop_factor','people'}).rename(columns={'newpop':'people'})
    
        Scot_adjust = get_scotpopulation()
    
        cols = ['ZoneID', 'area_type', 'property_type', 'Gender', 'Age', 'employment_type', 
            'SOC_category', 'ns_sec', 'household_composition', 'people']
        popadj = popadj.reindex(columns=cols)
        GB_adjusted = popadj.append(Scot_adjust)
    # ensure communal pop or popadj columns is the same as Scottish
    
        # this might not be needed but there were some zones that weren't behaving properly before
        check = GB_adjusted['ZoneID'].drop_duplicates()   
        missingMSOAs = landusesegments[~landusesegments.ZoneID.isin(check)]
        fullGBadjustment = GB_adjusted.append(missingMSOAs)
    
    
        print('Full population for 2018 is now =', 
          fullGBadjustment['people'].sum())
        print('check all MSOAs are present, should be 8480:', 
          fullGBadjustment['ZoneID'].drop_duplicates().count())
        fullGBadjustment = fullGBadjustment.reindex(columns = {'ZoneID', 'property_type', 
                                                           'household_composition', 
                                                           'employment_type', 'SOC_category', 
                                                           'ns_sec', 
                                                           })
        fullGBadjustment.to_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv', index = False)
        return(fullGBadjustment)
        del(otherrestofUK, check, Scot_Adjust, GB_adjusted, popadj)
 
    else:
        print ('FY not set up yet')
        
    
def sort_out_hops_uplift():
    """    
    Parameters
    ----------
    landuseoutput:
        Path to csv of landuseoutput 2011 to get the splits

    Returns
    ----------
    
    """ 
    property_count = pd.read_csv(_default_property_count).groupby(by=['ZoneID', 
                                'property_type'], as_index = False).sum().reindex(
                                columns={'ZoneID', 'property_type', 'properties'})
    hops2011 = pd.read_csv(_hops2011).rename(columns={'msoaZoneID':'ZoneID', 
                          'census_property_type':'property_type'})
               
    MYPEpop = sort_communal_uplift()
    
    msoaShp = gpd.read_file(_default_msoaRef).reindex(['objectid','msoa11cd'],axis=1)
    hops2011 = hops2011.rename(columns={'msoaZoneID':'ZoneID', 'census_property_type':'property_type'})
    msoaShp = msoaShp.rename(columns={
                'msoa11cd':'ZoneID'})
    hops2011 = hops2011.merge(msoaShp, on = 'ZoneID')

    hopsadj = hops2011.merge(property_count, on = ['ZoneID', 'property_type'])
    hopsadj = hopsadj.merge(MYPEpop, on = ['ZoneID', 'property_type'])
    hopsadj['household_occupancy_2018']=hopsadj['people']/hopsadj['properties']      
    hopsadj.to_csv('household_occupation_Comparison.csv')
    # TODO: need a new folder for 2018 adjustments?
    hopsadj = hopsadj.drop(columns = {'properties', 'household_occupancy', 
                                      'ho_type', 'people'})
    hopsadj = hopsadj.rename(columns = {'household_occupancy_2018': 'household_occupancy'})
    adjhopsMSOA2018 = hopsadj['ZoneID'].drop_duplicates()
    hops2011= hops2011.drop(columns={'ho_type'})
    restofUKhops = hops2011[~hops2011.ZoneID.isin(adjhopsMSOA2018)]
    fulladjHops = hopsadj.append(restofUKhops)
    print('check all MSOAs are present, should be 8480:', 
          fulladjHops['ZoneID'].drop_duplicates().count())
    fulladjHops.to_csv('2018_household_occupancy.csv')
    
    
def control_to_lad():
    ladactive = pd.read_csv().rename(columns={'Unnamed: 15':'Region' })
    employmentfigures = NPRSegments.groupby(by =['ZoneID', 'employment_type'], 
                                         as_index = False).sum().reindex(['ZoneID',
                                                               'employment_type',
                                                               'people'],axis=1)
    ladtranslation = pd.read_csv(ladPath).rename(columns={'lad_zone_id':'objectid',
                                             'msoa_zone_id':'MSOA'})
    lad_controls = pd.read_csv(LADControlPAth)
                
    NPRSegments = pd.read_csv('C:/NorMITs_Export/iter3/NPRSegments.csv')
    emp = NRSegments.groupby('employment_type').sum()
    NRSegments['ns_sec'].dtype
    NRSegments['ns_sec']= NRSegments['ns_sec'].astype('category')
    emp = NRSegments.groupby(by=['employment_type', 'ns_sec','SOC_category'], as_index = False).sum()
    NPRSegments2 = pd.read_csv('Y:/NorMITs Land Use/iter3/NPR_Segments.csv')
    emp2 = NPRSegments2.groupby(by=['employment_type', 'ns_sec','SOC_category'], as_index = False).sum()
  
####################      
# employment adjustment
# car availability adjustment
def ntsimport (year = ['2017']):
    """
    imports car availability from nts extract
    
    """
    ntsextract = pd.read_csv(_nts_path)
    ntsextract = ntsextract[ntsextract.SurveyYear.isin(year)]

    # need to change to MSOAs to add new area types
    ntsextract = ntsextract[['HHoldOSWard_B01ID', 'HHoldAreaType1_B01ID', 'HHoldNumAdults', 'CarAccess_B01ID', 'W1', 'W2', 
                 'XSOC2000_B02ID']]
    ###NTS weightings ###
    # household: weight = w1
    # individual: weight = w1 *w2
    ntsextract['people'] = ntsextract['W1']*ntsextract['W2']
    ntsextract = ntsextract.rename(columns = {'HHoldOSWard_B01ID': 'uk_ward_zone_id'})
    # change household number of adults into household composition
    # change SOC to standard 1,2,3
    
    cars = ntsextract.groupby(by=['uk_ward_zone_id', 'household_composition',  
                                'employment'], 
                                as_index = False).sum().drop(columns = {'W1', 'W2'})
    cars = cars[cars.area_type != -8]
    cars = cars.replace({-9:0})
    cars['people'].sum()

    wardToMSOA = pd.read_csv(wardToMSOAPath).drop(columns={'overlap_var', 
                          'uk_ward_var', 'msoa_var', 'overlap_type', 'overlap_msoa_split_factor'})

    ####### lots of zeros for MSOAs totals and other segments so not able to use it ###############
    cars_msoa = cars.merge(wardToMSOA, on = 'uk_ward_zone_id')
    cars_msoa['population'] = cars_msoa['people'] * cars_msoa['overlap_uk_ward_split_factor']
    cars_msoa['population'].sum()
    cars_msoa = cars_msoa.groupby(by=['msoa_zone_id', 'household_composition',
                                       'employment'],
                                        as_index = False).sum().drop(columns=
                                                             {'overlap_uk_ward_split_factor', 
                                                              'people', 'area_type'})
    cars_msoa['population'].sum()
    
    # read in the new areatypes
    areatypes = pd.read_csv(areatypesPath).rename(columns={'msoa_area_code':'msoa_zone_id', 
                           'tfn_area_type_id':'area_type'})
    cars_msoa = cars_msoa.merge(areatypes, on = 'msoa_zone_id') 
    
    # per area type - MSOA had too small sample sizes
    cars_at = cars_msoa.groupby(by=['area_type', 'household_composition', 
                                'employment'], 
                                as_index = False).sum()
    # -9 in soc is NA
    # but -8 looks odd so getting rid
    cars_at = cars_at[cars_at.area_type != -8]
    # cars = cars[cars.soc_cat != -8]
    cars_at['totals'] = cars_at.groupby(['area_type','employment'])['population'].transform('sum')
    
    cars_at['splits'] = cars_at['population']/cars_at['totals']
    cars_at = cars_at.rename(columns={'ns_sec':'ns', 
                                'employment':'employment_type'})
    cars_at['splits'].sum()
    cars_at = cars_at.replace({-9:0})
    cars_n = cars.groupby(by=['area_type'], as_index = False).sum()
    
    del(cars_msoa, cars, ntsextract)
   
def car_availability(midyear = True, year = '2017'):
    """
    
    """
    landuse = pd.read_csv(landusePath)

    nts2017 = nts[nts.SurveyYear.isin(year)]
    

    if midyear:
        # placeholder
        
    else: 
        #placeholder, read in random csv?
        
def AdjustSOCGB(
        GBSOCTotalsPath = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/LAD labour market data/nomis_SOCGBControl.csv'
        LadPath= 'Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv',
        Lad = 'Y:/NorMITs Land Use/import/Documentation/LAD_2017.csv')
 
    
    LADref = pd.read_csv(Lad).iloc[:,0:2]
        

    LadTranslation = pd.read_csv(LadPath).drop(columns={'overlap_type', 
                                        'lad_to_msoa','msoa_to_lad'}).rename(columns={
                                        'msoa_zone_id':'ZoneID', 'lad_zone_id':'objectid'}
                                        )

    SOCcontrol = pd.read_csv(GBSOCTotalsPath)
    SOCcontrol = SOCcontrol.rename(columns={
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
    SOCcontrol = SOCcontrol.reindex(columns= SOCcols)

    Rows = ['England and Wales number', 'Scotland number']
    SOCcontrol = SOCcontrol[SOCcontrol.Country.isin(Rows)]
    
    SOCcontrol = pd.melt(SOCcontrol, id_vars = 'Country', value_vars = ['SOC1', 
                                            'SOC2', 'SOC3', 'SOC4', 'SOC5', 'SOC6', 
                                            'SOC7', 'SOC8', 'SOC9'])
    SOCcontrol['value'] = pd.to_numeric(SOCcontrol['value'])
 
    SOCcontrol =SOCcontrol.replace({'variable':{'SOC1':1, 'SOC2':1,'SOC3':1, 
                                                'SOC4':2, 'SOC5':2, 'SOC6':2,'SOC7':2,
                                                'SOC8':3, 'SOC9':3}})
    SOCcontrol = SOCcontrol.rename(columns={'variable':'SOC_category'})
    SOCcontrol = SOCcontrol.groupby(by=['Country', 'SOC_category'], as_index = False).sum()
    
    SOCcontrol['total']= SOCcontrol.groupby(['Country'])['value'].transform('sum')
    SOCcontrol['splits'] = SOCcontrol['value']/SOCcontrol['total']
    
    ########################## CALCULATE COUNTRY SPLITS FROM LANDUSE ########
    Emp = ['fte', 'pte']
    Employed = AllSOCs[AllSOCs.employment_type.isin(Emp)]
    Employed = Employed.merge(LadTranslation, on = 'ZoneID')
    zones = AllSOCs["ZoneID"].drop_duplicates().dropna()
    Employed['Country']= 'England and Wales number'
    Employed.loc[Employed['ZoneID'].str.startswith('S'), 'Country']= 'Scotland number'
    
    EmpSOCTotal = Employed.groupby(by=['Country', 'SOC_category'],
                                   as_index = False).sum().reindex(
                                           columns=['Country', 'SOC_category', 'newpop'])
    EmpSOCTotal['total_land'] = EmpSOCTotal.groupby(['Country'])['newpop'].transform('sum')
    
    # for audit
    EmpSOCTotal['splits_land']= EmpSOCTotal['newpop']/EmpSOCTotal['total_land']
    # EmpSOCTotal = EmpSOCTotal.drop(columns={'newpop', 'total_land'})
    EmpSOCTotal['SOC_category'] = pd.to_numeric(EmpSOCTotal['SOC_category'])
    
    #save for comparison
    EmpCompare = EmpSOCTotal.merge(SOCcontrol, on = ['Country', 'SOC_category'], 
                                   how = 'left').drop(columns={'newpop', 'splits_land', 'value', 'total'})
    .drop(columns={'newpop', 'total_land', 'value', 'total'})
    EmpCompare = EmpCompare

    EmpCompare.to_csv('C:/NorMITs_Export/iter3_2/SOCsplitsComparison.csv')
    
    EmpCompare['pop'] = EmpCompare['splits']*EmpCompare['total_land']
    EmpCompare['pop'].sum()
    EmpCompare = EmpCompare.drop(columns={'total_land', 'splits'})
    
    LanduseGrouped = Employed.groupby(by=['ZoneID', 'Country', 'ns_sec', 'employment_type', 
                                         'household_composition', 'Gender', 'Age', 
                                         'property_type',],as_index = False).sum()
    LanduseGrouped['total'] = LanduseGrouped.groupby(['Country'])['newpop'].transform('sum')
    LanduseGrouped['factor']= LanduseGrouped['newpop']/LanduseGrouped['total']
    
    SOCrevised = EmpCompare.merge(LanduseGrouped, on = 'Country', how= 'left')
    SOCrevised['people']= SOCrevised['factor']*SOCrevised['pop']
    SOCrevised['people'].sum()
    SOCrevised = SOCrevised.drop(columns={'pop', 'factor', 'newpop', 'objectid'}
                    ).rename(columns={'people':'newpop'})
    NPRSegments = ['ZoneID', 'area_type', 'property_type', 'employment_type', 
                   'Age', 'Gender', 'household_composition',
                   'ns_sec', 'SOC_category', 'newpop'] 

    SOCrevised = SOCrevised.reindex(columns=NPRSegments)
    SOCrevised['newpop'].sum()
    # join to the rest
    
    NotEmployed = AllSOCs[~AllSOCs.employment_type.isin(Emp)]
    NPRSegmentation = NotEmployed.append(SOCrevised)
    NPRSegmentation['newpop'].sum()
    
    NPRSegmentation.to_csv('C:/NorMITs_Export/iter3_2/NPRSegments.csv')
    

################################################################
    


def AdjustSOCs (LADSOCControlPath = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/LAD labour market data/nomis_lad_SOC2018_constraints.csv',
                LadPath= 'Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv',
                Lad = 'Y:/NorMITs Land Use/import/Documentation/LAD_2017.csv',
                #AllPath = 'C:/NorMITs_Export/NPRSegments.csv'):
    
    LADref = pd.read_csv(Lad).iloc[:,0:2]

    # format the LAD controls data
    LADSOCcontrol = pd.read_csv(LADSOCControlPath)
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
    # getting rid of City of London as there're a lot of 
   # LADSOCcontrol = LADSOCcontrol[LADSOCcontrol['lad17cd'] != 'E09000001']
    #LADSOCcontrol = LADSOCcontrol.copy()
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
  #  LADSoc['SOC_category']=LADSoc['SOC_category']
  #  LADSoc = LADSoc.merge(LADref, on = 'lad17cd')
    

   # LADSoc = LADSoc.rename(columns={'lad17cd':'LA'})
    # LADSoc = LADSoc.drop(columns={'objectid_x', 'objectid_y'})
#    LADSoc['SOC_category']= LADSoc['SOC_category'].astype('category')
   # LADSoc['SOC_category'] = LADSoc['SOC_category'].astype(float)
   
    ####################### READ IN POPULATION TO ADJUST ####################
    LadTranslation = pd.read_csv(LadPath).drop(columns={'overlap_type', 
                                        'lad_to_msoa','msoa_to_lad'}).rename(columns={
                                        'msoa_zone_id':'ZoneID', 'lad_zone_id':'objectid'}
                                        ).merge(LADref, on = 'objectid')
    #AllSOCs = pd.read_csv(AllPath, dtype={'property_type': object,
     #                'household_composition':object, 
      #               'ns_sec':object, 'SOC_category':object}).drop(columns={'Unnamed: 0'})
    EmployedPop = AllSOCs[AllSOCs.SOC_category != 'NA']
    EmployedPop['newpop'].sum()

    EmployedPop = EmployedPop.merge(LadTranslation, on = 'ZoneID')
    SOCtotals2LAD = EmployedPop.groupby(by=['lad17cd', 'SOC_category'], as_index = False).sum().drop(
            columns={'area_type'})
    
    SOCtotals = SOCtotals.drop(columns={'property_type', 'ns_sec',
                                        'household_composition', 'ns_sec'})
    SOCtotalsMSOA = EmployedPop.groupby(by=['ZoneID', 'lad17cd'], as_index = False).sum().reindex(columns=['ZoneID','lad17cd', 'newpop'])

    # SOCtotals['SOC_category']=pd.to_numeric(SOCtotals['SOC_category'])
    SOCtotalsMSOA = EmployedPop.merge(LadTranslation, on = 'ZoneID')
    SOCtotalsLAD = SOCtotalsLAD.groupby(by=['lad17cd'], as_index = False).sum().reindex(columns=['lad17cd','newpop'])

    Compare = SOCtotalsMSOA.merge(LADSoc, on = ['lad17cd'])
    Compare['people'] = Compare['newpop']*Compare['splits']

    Compare = SOCtotalsLAD.merge(LADSoc, on = ['lad17cd'])
    Compare['newvalue']= Compare['newpop']*Compare['splits']      
    Compare = Compare.drop(columns={'newop', 'value', 't})
    Grouped = EmployedPop.groupby(by=['ZoneID', 'property_type', 'Age', 'Gender', 
                                  'household_composition', 'area_type','ns_sec'],as_index = False).sum()
    
    Grouped = Grouped.merge(Compare, on =['lad17cd'],how= 'left')
    LADgrouped = EmployedPop.groupby(by=['ZoneID', 'lad17cd'], as_index = False).sum()
    
    
            
    Compare['factor'] = Compare['value']/Compare['newpop']
    Compare = Compare.drop(columns={'newpop', 'value'})
    
    ######
"""       
    SOC_Join = area_code_lookup.merge(SOCtotals, on =['ZoneID'])
    # group SOCs 
    SOC_Join_la = SOC_Join.groupby(['LAD18CD', 'SOC_category'], as_index = False).sum().rename(
            columns={'newpop':'SOC_total', 'LAD18CD':'LA'})
    SOC_Join_county = SOC_Join.groupby(['CTY18CD', 'SOC_category'], as_index = False).sum().rename(
            columns={'newpop':'SOC_total', 'CTY18CD':'LA'})
    ##### 
    SOC_Join_All = SOC_Join_la.append(SOC_Join_county)
    SOC_Join_All = SOC_Join_All.groupby(by=['LA', 'SOC_category'], as_index = False).sum()
    SOC_Join_All['SOC_category']=pd.to_numeric(SOC_Join_All['SOC_category'])
    
    SOC_compare = SOC_Join_All.merge(LADSoc, on = ['LA', 'SOC_category'])

    SOC_compare['factor'] = SOC_compare['SOC_total']/SOC_compare['value']
    SOC_compare = SOC_compare.drop(columns={'SOC_total', 'value'})
    """
################### JOIN BACK TO MSOA LEVEL ###############################
    #Socs['SOC_category']=pd.to_numeric(Socs['SOC_category'])
   # SOCs =[1,2,3]
   # landuseSOC = AllSOCs[AllSOCs.SOC_category.isin(SOCs)]
   # landuseNoSOC = AllSOCs[~AllSOCs.SOC_category.isin(SOCs)]       
    landuseSOC = EmployedPop.merge(LadTranslation, on = 'ZoneID')
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
    
    GBlanduse = pd.read_csv('C:/NorMITs_Export/iter3_2/landuseGBMYE_flatcombined.csv')
    
    All = pd.read_csv('C:/NorMITs_Export/iter3_2/NPRSegments_stage1.csv')
    All2 = All.groupby(by=['ZoneID', 'property_type', 'Age', 'employment_type',
                           'ns_sec', 'SOC_category'],as_index = False).sum()
    All = All.drop_duplicates()
    SOCcheck =  All.groupby(['ns_sec', 'SOC_category'], as_index = False).sum()
    nonwa = ['non_wa']
    Nonwa = All[All.employment_type.isin(nonwa)]
    All['SOC_category'] = All['SOC_category'].fillna('NA')
    
def ControltoLADEmploymentGender(#AllPath = 'C:/NorMITs_Export/iter3_2/NPRSegments_stage1.csv'
                                 , areatypesPath,
                                 EmpControls = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/LAD labour market data/Nomis_lad_EconomicActivity3.csv'),
                                 LadPath= 'Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv',
                                 Lad = 'Y:/NorMITs Land Use/import/Documentation/LAD_2017.csv',
                                 GBlandusePath = 'C:/NorMITs_Export/iter3_2/landuseGBMYE_flatcombined.csv' 
                                 ):
    
    # could use this to control the splits - age/gender
    All = pd.read_csv(GBlandusePath).drop(columns={'Unnamed: 0'})
   #  Lad = pd.read_csv(LadPath)
    LadTranslation = pd.read_csv(LadPath).drop(columns={'lad_to_msoa', 'msoa_to_lad', 
                                'overlap_type'}).rename(columns = {'msoa_zone_id':'ZoneID', 
                                              'lad_zone_id':'objectid'})                                   
    LADref = gpd.read_file(ladRef).iloc[:,0:2]
    All = All.merge(LadTranslation, on = 'ZoneID', how='left')
    All = All.merge(LADref, on = 'objectid')
    
    ################### WORK OUT SPLITS FROM CONTROL ###############
  
    # format the input table - includes both males and females in fte/pte
    EmpControls = EmpControls.rename(columns={
            'T08:29 (Males - Aged 16 - 64 : Full-time ) number':'Male FTE',
            'T08:30 (Males - Aged 16 - 64 : Part-time ) number':'Male PTE',
            'T08:44 (Females - Aged 16 - 64 : Full-time ) number':'Females FTE',
            'T08:45 (Females - Aged 16 - 64 : Part-time ) number':'Females PTE',   
            'T01:8 (All aged 16 & over - In employment : Males ) number':'Male Emp',
            'T01:9 (All aged 16 & over - In employment : Females ) number':'Females Emp',
            })

    EmpControls= EmpControls.replace({'!':0, '~':0, '-':0, '#':0, ',':''})
                                                  
    LADEmpGenControls = EmpControls[['lad17cd', 
                                     'Male Emp', 'Females Emp'
                                                           ]]
                                                
    LADcontrolled = pd.melt(LADEmpGenControls, id_vars = 'lad17cd',
                            value_vars = ['Male Emp', 'Females Emp'
                                          ])
    #, 'Male Inc', 'Females Inc'])
    LADcontrolled['Gender'] = 'Male'
    LADcontrolled['employment_cat']= 'emp'
           
    LADcontrolled.loc[LADcontrolled['variable']=='Male Emp', 'employment_cat']='emp'
    LADcontrolled.loc[LADcontrolled['variable']=='Male Emp', 'Gender']='Male'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Emp', 'employment_cat']='emp'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Emp', 'Gender']='Females'
    """
    LADcontrolled.loc[LADcontrolled['variable']=='Male Unm', 'employment_cat']='unm'
    LADcontrolled.loc[LADcontrolled['variable']=='Male Unm', 'Gender']='Male'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Unm', 'employment_cat']='unm'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Unm', 'Gender']='Females'
    LADcontrolled.loc[LADcontrolled['variable']=='Male Inc', 'employment_cat']='unm'
    LADcontrolled.loc[LADcontrolled['variable']=='Male Inc', 'Gender']='Male'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Inc', 'employment_cat']='unm'
    LADcontrolled.loc[LADcontrolled['variable']=='Females Inc', 'Gender']='Females'        
    """
     
    LADcontrolled['value'] = pd.to_numeric(LADcontrolled['value'])
    
    LADcontrolled = LADcontrolled.groupby(by=['lad17cd', 'employment_cat', 'Gender'], as_index = False).sum()
    
    #LADcontrolled['totals'] = LADcontrolled.groupby(['lad17cd','Gender'])['value'].transform('sum')
    #LADcontrolled = LADcontrolled.rename(columns={'value':'LADcontrol_total',
          #                                        'variable':'Gender'})
    ####################### WORK OUT THE TOTAL FOR 16-74 ##################
    WAAll = All[All.employment_type != 'non_wa']
    # LanduseStu = WAAll[WAAll.employment_type == 'stu']
    # LanduseStu = LanduseStu.groupby(by=['lad17cd', 'Gender'],as_index = False).sum().drop(
      #      columns={'household_composition', 'area_type', 
       #                                          'property_type', 'objectid'})
    TotalWAPop = WAAll.groupby(by =['lad17cd','Gender'
                                       ],as_index = False).sum().drop(columns={
                                                 'household_composition', 'area_type', 
                                                 'property_type', 'objectid'})
    LADcontrolled3 = LADcontrolled.merge(TotalWAPop, on = ['lad17cd','Gender'], how='left')
    # LADcontrolled2['value'].sum()
    LADcontrolled3['splits']= LADcontrolled3['value']/LADcontrolled3['people']
    
    # for Isles of Scilly E06000053:
    LADcontrolledAverage = LADcontrolled3.groupby(by=['employment_cat'], as_index = False).sum()
    LADcontrolledAverage['splits2'] = LADcontrolledAverage['value']/LADcontrolledAverage['people']
    LADcontrolledAverage = LADcontrolledAverage.drop(columns={'value', 'people', 'splits'})
    LADcontrolled3 = LADcontrolled3.drop(columns={'value', 'people'})
    
    LADcontrolled = LADcontrolled3.merge(LADcontrolledAverage, on = 'employment_cat', how = 'right')
    LADcontrolled.loc[LADcontrolled.lad17cd == 'E06000053','splits']=LADcontrolled['splits2']
    LADcontrolled['splits'] = LADcontrolled['splits'].fillna(LADcontrolled['splits2'])
    LADcontrolled = LADcontrolled.drop(columns={'splits2'})
    LADcontrolled.loc[LADcontrolled['lad17cd']=='E09000001', 'splits']=1
    LADcontrolled['inactivesplits']= 1 - LADcontrolled['splits']
    #LADcontrolled['splits'] = LADcontrolled['splits']>1,
    
    
    #### FTE/PTE splits ##############
    LADftepteControls = EmpControls[['lad17cd', 'Male FTE', 'Females FTE',
                                           'Male PTE', 'Females PTE']]      
    LADftepteControls = pd.melt(LADftepteControls, id_vars = 'lad17cd',
                            value_vars = ['Male FTE', 'Females FTE',
                                          'Male PTE','Females PTE'])
    LADftepteControls['Gender'] = 'Male'
    LADftepteControls['employment_type']= 'unm'

    LADftepteControls.loc[LADftepteControls['variable']=='Male FTE', 'employment_type']='fte'
    LADftepteControls.loc[LADftepteControls['variable']=='Male FTE', 'Gender']='Male'
    LADftepteControls.loc[LADftepteControls['variable']=='Male PTE', 'employment_type']='pte'
    LADftepteControls.loc[LADftepteControls['variable']=='Male PTE', 'Gender']='Male'
    LADftepteControls.loc[LADftepteControls['variable']=='Females FTE', 'employment_type']='fte'
    LADftepteControls.loc[LADftepteControls['variable']=='Females FTE', 'Gender']='Females'
    LADftepteControls.loc[LADftepteControls['variable']=='Females PTE', 'employment_type']='pte'
    LADftepteControls.loc[LADftepteControls['variable']=='Females PTE', 'Gender']='Females'
    LADftepteControls = LADftepteControls.drop(columns={'variable'})
    LADftepteControls['value'] = pd.to_numeric(LADftepteControls['value'])

    LADftepteControls = LADftepteControls.groupby(by=['lad17cd', 'employment_type', 'Gender'], as_index = False).sum()
    
    LADftepteControls['totals'] = LADftepteControls.groupby(['lad17cd','Gender'])['value'].transform('sum')
    LADftepteControls['splits']= LADftepteControls['value']/LADftepteControls['totals']
    
    LADftepteControlsAverage = LADftepteControls.groupby(by=['employment_type'], as_index = False).sum()
    LADftepteControlsAverage['splits2'] = LADftepteControlsAverage['value']/LADftepteControlsAverage['totals']
    LADftepteControlsAverage = LADftepteControlsAverage.drop(columns={'value', 'totals', 'splits'})

    LADftepteControls = LADftepteControls.merge(LADftepteControlsAverage, on = 'employment_type', how = 'right')
    LADftepteControls['splits'] = LADftepteControls['splits'].fillna(LADftepteControls['splits2'])

    LADftepteControls = LADftepteControls.drop(columns={'splits2','value', 'totals'})

    ################# SUM UP The Population by LAD and APPLY the splits between inactive and active ########################
    
    # All = All.drop(columns={'objectid'})
    WAAll = All[All.employment_type != 'non_wa']
    LanduseLAD = WAAll.groupby(by =['ZoneID', 'lad17cd','Gender'
                                       ],as_index = False).sum().drop(columns={
                                                 'household_composition', 'area_type', 
                                                 'property_type', 'objectid'})
    FactoredEmp = LanduseLAD.merge(LADcontrolled, on = ['lad17cd', 'Gender'], 
                                         how = 'left')
    FactoredEmp['newpop'] = FactoredEmp['people']*FactoredEmp['splits']
    
    FactoredEmp['newpop'].sum()
    FactoredEmp = FactoredEmp.drop(columns={'people', 'splits', 'inactivesplits'})
            
    ############# Emp Active Split by fte/pte #############################
    # Employed = FactoredEmp[FactoredEmp.employment_cat == 'emp']
    
    Employed = FactoredEmp.merge(LADftepteControls, on = ['lad17cd', 'Gender'], how = 'left')
    Employed['pop']= Employed['newpop']*Employed['splits']
    Employed = Employed.drop(columns={'newpop', 'splits', 'employment_cat'})
    Employed['pop'].sum()
    
    InEmployment = ['fte', 'pte']
    #WAAllActive = 
    ActiveLanduse = WAAll[WAAll.employment_type.isin(InEmployment)]
    ActiveLanduseGrouped = ActiveLanduse.groupby(by=['ZoneID', 'lad17cd', 'Gender', 'employment_type'],
                             as_index = False).sum().drop(columns={'household_composition', 
                                                  'area_type', 'objectid', 'property_type'})
    ##### to work out fte/pte #############################################
    ActiveComp = ActiveLanduseGrouped.merge(Employed, on = ['ZoneID', 'lad17cd', 'Gender', 'employment_type'], how = 'left')
    ActiveComp['factor']= ActiveComp['pop']/ActiveComp['people']
    ActiveComp = ActiveComp.drop(columns={'people', 'pop'})
    # ActiveComp = ActiveComp.replace([np.inf],0)
    
    ############ JOIN BACK TO FTE/PTE LANDUSE LEVEL #######################      
    ActiveLanduse2 = ActiveLanduse.merge(ActiveComp, on = ['ZoneID', 'lad17cd', 
                                                           'Gender', 'employment_type'
                                                           ], how = 'left')

    ActiveLanduse2['newpop'] = ActiveLanduse2['people']*ActiveLanduse2['factor']
    ActiveLanduse2['newpop'].sum()
   
    ActiveLanduse2 = ActiveLanduse2.drop(columns={'factor'})
    checkActive = ActiveLanduse2.groupby('employment_type', as_index = False).sum()  
    #Communal = ActiveLanduse2[ActiveLanduse2.property_type ==8]
    #Communal['newpop'].sum()
    ################## WORK OUT ####################################
    
    ############# Inactive and Unemployed #################################
    Inact = ['unm', 'stu']
    LanduseLAD = WAAll.groupby(by =['ZoneID', 'lad17cd','Gender'
                                       ],as_index = False).sum().drop(columns={
                                                 'household_composition', 'area_type', 
                                                 'property_type', 'objectid'})
    FactoredInc = LanduseLAD.merge(LADcontrolled, on = ['lad17cd', 'Gender'], 
                                         how = 'left')
    FactoredInc['newpop'] = FactoredInc['people']*FactoredInc['inactivesplits']
    
    FactoredInc['newpop'].sum()
    FactoredInc = FactoredInc.drop(columns={'people', 'inactivesplits', 'splits'})

    ###############compare#############################       
    InactiveLanduse = WAAll[WAAll.employment_type.isin(Inact)]
    InactiveLanduseGrouped = InactiveLanduse.groupby(by=['ZoneID', 'lad17cd', 'Gender'],
                             as_index = False).sum().drop(columns={'household_composition', 
                                                  'area_type', 'objectid', 'property_type'})
    InactiveComp = InactiveLanduseGrouped.merge(FactoredInc, on = ['ZoneID', 'lad17cd', 'Gender'], how = 'left')
    InactiveComp['factor']= InactiveComp['newpop']/InactiveComp['people']
    InactiveComp = InactiveComp.drop(columns={'people', 'newpop'})
    
    InactiveLanduse2 = InactiveLanduse.merge(InactiveComp, on = ['ZoneID', 'lad17cd', 'Gender'], how = 'left')
    InactiveLanduse2['newpop'] = InactiveLanduse2['people']*InactiveLanduse2['factor']
    InactiveLanduse2['newpop'].sum()
    
    checkInactive = InactiveLanduse2.groupby('employment_type', as_index = False).sum()
    
    GBcols = ['ZoneID', 'Age', 'employment_type', 'area_type', 'property_type',
              'household_composition', 'Gender', 'objectid', 'lad17cd',
              'newpop']

    InactiveLanduse2 = InactiveLanduse2.reindex(columns = GBcols)
    ActiveLanduse2 = ActiveLanduse2.reindex(columns = GBcols)
    GBlanduseControlled = InactiveLanduse2.append(ActiveLanduse2)

    GBlanduseControlled['newpop'].sum()
    NowaAll = All[All.employment_type == 'non_wa']
    NowaAll = NowaAll.rename(columns={'people':'newpop'})
    NowaAll = NowaAll.reindex(columns=GBcols)
    NowaAll['newpop'].sum()
    GBlanduseControlled = GBlanduseControlled.append(NowaAll)
    GBlanduseControlled['newpop'].sum()
    GBlanduseControlled = GBlanduseControlled.rename(columns={'newpop':'people'})
    GBlanduseControlled.to_csv('C:/NorMITs_Export/iter3_2/GBlanduseControlled.csv')
    
    
    def CountryControl():
    ############## Cuontry control #################
    CountryControl = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/processed data/Country Control 2018/nomis_CountryControl.csv')
    CountryEmp = CountryControl.rename(columns={
            'T01:7 (All aged 16 & over - In employment : All People )':'Emp'
            })
    CountryEmp = CountryEmp[['Country','Emp']]
    
    Rows = ['England and Wales number', 'Scotland number']
    CountryEmpControl = CountryEmp[CountryEmp.Country.isin(Rows)]
    
    emp = ['fte', 'pte']
    zones = GBlanduseControlled["ZoneID"].drop_duplicates().dropna()
    Scott = zones[zones.str.startswith('S')]

    Active = GBlanduseControlled[GBlanduseControlled.employment_type.isin(emp)]
    ScottActive = Active[Active.ZoneID.isin(Scott)]
    ScottActive['Country'] = 'Scotland number'
    ScottActiveTotal = ScottActive.groupby(by=['Country'], as_index = False).sum().reindex(columns=['Country', 'people'])

    ScottActiveTotal= ScottActiveTotal.merge(CountryEmpControl,on='Country')
    ScottActiveTotal['factor'] = ScottActiveTotal['Emp']/ScottActiveTotal['people']
    ScottActiveTotal =ScottActiveTotal.drop(columns={'people', 'Emp'})
    ScottActive = ScottActive.merge(ScottActiveTotal, on = 'Country')
    ScottActive['newpop']= ScottActive['people']*ScottActive['factor']
    ScottActive = ScottActive.drop(columns={'people', 'factor', 'Country'})

    ScottActive['newpop'].sum()
    
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

    ActiveAdj = EngActive.append(ScottActive)
    ActiveAdj = ActiveAdj.rename(columns={'newpop':'people'})
    ActiveAdj['people'].sum()
    ActiveAdj = ActiveAdj.reindex(columns=GBcols2)
    ActiveNewTotal = ActiveAdj.groupby('ZoneID',as_index = False).sum().reindex(columns={'ZoneID', 'people'})
    ActiveNewTotal = ActiveNewTotal.rename(columns={'people':'employed'})
    
    # GBlanduseControlled.zone sum - new zone sum
    
    GBTotals = GBlanduseControlled[GBlanduseControlled.employment_type != 'non_wa']
    GBTotals['people'].sum()
    GBTotals = GBTotals.groupby('ZoneID', as_index = False).sum().reindex(columns={'ZoneID', 'people'})
    GBTotals = GBTotals.merge(ActiveNewTotal, on = 'ZoneID')
    GBTotals['inactive'] = GBTotals['people']- GBTotals['employed']
    GBTotals = GBTotals.drop(columns={'people', 'employed'})
    
    Inact = ['unm', 'stu']
    Inactive = WAAll[WAAll.employment_type.isin(Inact)]
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
    
    GBcols2 = ['ZoneID', 'Age', 'employment_type', 'area_type', 'property_type',
              'household_composition', 'Gender', 'objectid', 'lad17cd',
              'people']
    NowaAll = NowaAll.rename(columns={'newpop':'people'})
    NowaAll = NowaAll.reindex(columns=GBcols2)
    NowaAll['people'].sum()


    AdjustedGBlanduse = Inactive3.append(NowaAll).append(ActiveAdj)
    AdjustedGBlanduse.to_csv('C:/NorMITs_Export/AdjustedGBlanduse.csv') 
    AdjustedGBlandsue['people'].sum()
    check = AdjustedGBlanduse.groupby(by=['ZoneID'], as_index = False).sum()
            
    check = GBlanduseControlled.groupby(by=['ZoneID', 'employment_type'], as_index = False).sum()
    check.to_csv('C:/NorMITs_Export/check.csv')
    Employmenttypes = Adjusted.groupby(by=['employment_type'],as_index = False).sum()
    Employmenttypes.to_csv('C:/NorMITs_Export/employment2.csv')
    
    

   # CommunalEstablishments = GBlanduseControlled[GBlanduseControlled.property_type ==8]
# Check = GBlanduseControlled.groupby(by=['ZoneID'], as_index = False).sum()
    
    


    ############################# JOIN BACK TO MSOA ################################
    
    Activeatwork = ['fte', 'pte']
    ActivePopAtWork = ActivePop[ActivePop.employment_type.isin(Activeatwork)]
    ###
    ActivePopGender = ActivePop.groupby(by=['ZoneID', 'Gender'], as_index = False).sum()
    ActivePopGender = ActivePopGender.drop(columns={'household_composition','SOC_category','ns_sec'})
    
    FactoredEmployees = ActivePop.merge(compare, on = ['lad17cd', 'Gender'], how = 'left')
    FactoredEmployees = 
    FactoredEmployees['newpop2'] = FactoredEmployees['newpop']/FactoredEmployees['factor']
    CityofLondon = FactoredEmployees[FactoredEmployees.ZoneID=='E02000001']
    
    FactoredEmployees = FactoredEmployees.replace([np.inf, -np.inf], np.nan)
    FactoredEmployees['newpop2'] = FactoredEmployees['newpop2'].fillna(FactoredEmployees['newpop'])
    FactoredEmployees['newpop2'].sum()
    FactoredEmployees = FactoredEmployees.replace([np.inf, -np.inf], np.nan)
    FactoredEmployees.loc[FactoredEmployees['factor'] == 0, 'newpop2']=FactoredEmployees['newpop']    
    FactoredEmployees['newpop2'].sum()

    ########################### WORK OUT UNEMPLOYED ######################################
    # if Employees['newpop']= 0 don't readjust & for Scotland's few LAs
    Unemployed = FactoredEmployees.groupby(by=['lad17cd', 'Gender'],as_index = False).sum().drop(columns=
                                          {'landuse_total', 'SOC_category', 'household_composition','LADcontrol_total', 'property_type',
                                           'area_type','ns_sec', 'objectid', 'Unnamed: 0'})
    Unemployed['unm_people']= Unemployed['newpop']- Unemployed['newpop2']
    # this is only for City of London:
    Unemployed.loc[Unemployed['unm_people'] <= 0,'unm_people'] = 0
    Unemployed = Unemployed.drop(columns={'newpop', 'newpop2', 'factor'})
    Unemployed['employment_type'] = 'unm'
           
    ########################## JOIN TO LANDUSE TOTALS TO GET A FACTOR #############################
    Unemployedtype=['unm', 'stu']
    LanduseLADunm = LanduseLAD[LanduseLAD.employment_type.isin(Unemployedtype)]
    compareunm = LanduseLADunm.merge(Unemployed, on = ['lad17cd', 'Gender', 'employment_type'], how = 'left')
    compareunm['factorunm']= compareunm['unm_people']/compareunm['landuse_total']

    
    ######################### JOIN BACK TO LANDUSE TO WORK OUT NEW UNM PEOPLE #####################
    #compare = compare.drop(columns={'landuse_total', 'LADcontrol_total'})
    Unemployedtype=['unm']
    ActiveUnemployed = ActivePop[ActivePop.employment_type.isin(Unemployedtype)]

    # LanduseLADunm = LanduseLAD[LanduseLAD.employment_type.isin(Unemployedtype)]
    FactoredUnemployment = ActiveUnemployed.merge(compareunm, on = ['lad17cd', 
                                                                    'Gender', 
                                                                    'employment_type'], how = 'outer')
    FactoredUnemployment['newpop2'] = FactoredUnemployment['factorunm']*FactoredUnemployment['newpop']
    
    
    FactoredUnemployment['newpop2'].sum()
    FactoredUnemployment = FactoredUnemployment.drop(columns={'newpop'}).rename(columns={'newpop2':'people'})
    ####################### JOIN THE TWO TOGETHER ###########################################
    FactoredEmployees= FactoredEmployees.drop(columns={'people'})
    FactoredEmployees = FactoredEmployees.rename(columns={'newpop2':'people'})

    FactoredEmployees = FactoredEmployees.reindex(columns=['ZoneID', 'area_type',
                                                   'property_type','Age', 'employment_type',
                                                   'household_composition', 'Gender', 'ns_sec', 
                                                   'SOC_category','people'])
    
    FactoredUnemployment = FactoredUnemployment.reindex(columns= ['ZoneID', 'area_type',
                                                   'property_type','Age', 'employment_type',
                                                   'household_composition', 'Gender', 'ns_sec',
                                                   'SOC_category','people'])
    Unm = FactoredUnemployment['people'].sum()
    Emp = FactoredEmployees['people'].sum()

    FactoredActive = FactoredUnemployment.append(FactoredEmployees)
    check = FactoredActive['people'].sum()
    FactoredActive = FactoredActive.reindex(columns=['ZoneID', 'Age', 'Gender','employment_type', 'area_type', 
                                    'property_type', 'household_composition',
                                    'people'])
    
    InactivePot = InactivePot.reindex(columns=['ZoneID', 'Age', 'Gender','employment_type', 'area_type', 
                                    'property_type', 'household_composition',
                                    'people'])
    NEWpopulation = FactoredActive.append(InactivePot)
    NEWpopulation.to_csv('C:/NorMITs_Export/cotnrolledpopulation.csv')
    
    del(compareunm, compare, LADcontrolled)
    del(FactoredActive, FactoredEmployees, FactoredUnemployment, ActiveUnemployed)
    del( LADControlPath, Activeatwork)
    del(Unemployed, Unemployedtype)
    NEWpopulation.to_csv('C:/NorMITs_Export/iter3/NewAdjustedpopulation.csv')

    
def run_mype(midyear = True):
    adjust_landuse_to_MYPE()
    sort_out_hops_uplift()
    control_to_lad()
    
    
