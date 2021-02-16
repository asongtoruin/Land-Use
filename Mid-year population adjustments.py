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
_default_zone_folder = ('Y:/NorMITs Synthesiser/Zone Translation/')
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

    ladCols = ['objectid','lad17cd']
    ukLAD = gpd.read_file(_default_ladRef)
    ukLAD = ukLAD.loc[:,ladCols]

def format_english_mype():
    """
    getting MYPE into the right format - 'melt' to get columns as rows, then rename them
    This should be a standard from any MYPE in the future segmented into females and males
    Parameters
    ----------
    mye_males
    mye_females
    
    Returns
    ----------
    mye_2018 - one formatted DataFrame of new MYPE including population split by age and gender by MSOA
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
    
    # children are a 'gender' in NTEM, then need to sum the two rows

    mype.loc[mype['Age'] == 'under 16', 'gender'] = 'Children'
    mype['pop'] = mype.groupby(['ZoneID', 'Age', 'gender'])['2018pop'].transform('sum') 
    mype = mype.drop_duplicates()
    mype = mype.drop(columns={'2018pop'}).drop_duplicates().rename(columns={
                            'gender':'Gender'})
    return(mype)
    del(mye_females, mye_males)
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
   
    Scotlanduse = landuseoutput[landuseoutput.ZoneID.str.startswith('S')]

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
    Scot_adjust = Scotlanduse.merge(Scot, on =['ZoneID','Gender', 'Age'])
    Scot_adjust['newpop'] = Scot_adjust['people']*Scot_adjust['pop_factor']
    Scot_adjust = Scot_adjust.drop(columns={'people', 'people2018', 
                                            'pop_factor'}).rename(
                                            columns= {'newpop':'people'})
    print('The adjusted MYPE population for Scotland is', 
         Scot_adjust['people'].sum()/1000000, 'M')

    return(Scot_adjust)

def sort_communal_uplift():
    
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
    landuseoutput = pd.read_csv(_default_landuse_2011)
         
    # split landuse data into 2 pots: Scotland and E+W
    zones = landuseoutput["ZoneID"].drop_duplicates().dropna()
    Scott = zones[zones.str.startswith('S')]
    EWlanduse = landuseoutput[~landuseoutput.ZoneID.isin(Scott)]
    # group to ZoneID, Gender, Age to match info from MYPE
    EWlandusegroup = EWlanduse.groupby(by=['ZoneID', 'Gender', 'Age'], as_index = False).sum().reindex(
            columns={'ZoneID', 'Gender', 'Age', 'people'})
    # get a communal factor calculated
    communalgroup = communal.groupby(by=['ZoneID', 'Gender', 'Age'], as_index = False).sum().reindex(
            columns = {'ZoneID', 'Gender', 'Age', 'communal'})
    com2011 = EWlandusegroup.merge(communalgroup, on = ['ZoneID', 'Gender', 'Age'])
    com2011['CommunalFactor'] = com2011['communal']/com2011['people']
    com2011 = com2011.rename(columns={'people':'Census'})
    
    # uplift communal to MYPE
    mype = format_english_mype()  
    mye_adjust = mype.merge(com2011, on = ['ZoneID', 'Gender', 'Age'], how = 'outer')
    mye_adjust['communal_mype'] = mye_adjust['pop'].values * mye_adjust['CommunalFactor'].values
    print('Communal establishments total for new MYPE is ', 
          mye_adjust['communal_mype'].sum())
    
    mye_adjust['mype_notcommunal'] = mye_adjust['pop'].values - mye_adjust['communal_mype'].values
    mye_adjust = mye_adjust.drop(columns={ 'pop', 'CommunalFactor', 'Census', 'communal'})
    return (mye_adjust)
    print('ONS population MYPE totals for England & Wales after adjusting for communal establishments is:', 
          mye_adjust['mype_notcommunal'].sum())
    return(mye_adjust)
    
    
def adjust_landuse_to_MYPE():
    """    
    Parameters
    ----------
    landuseoutput:
        Path to csv of landuseoutput 2011 with all the segmentaion (emp type, soc, ns_sec, gender, hc, prop_type), 
        to get the splits

    Returns
    ----------
    
    """
    # summarise land use output
    landusesegmented = pd.read_csv(_default_home_dir+'/landuseOutputMSOA_stage4.csv')
    
    pop_pc_totals = landusesegmented.groupby(
            by = ['ZoneID', 'Age', 'Gender'],
            as_index=False
            ).sum().drop(columns ={ 'area_type', 
                 'household_composition', 'property_type'})
    mye_adjust = sort_communal_uplift()
    myepops = pop_pc_totals.merge(mye_adjust, on = ['ZoneID', 'Gender', 'Age'])
    
    myepops['pop_factor'] = myepops['mype_notcommunal']/myepops['people']
    myepops = myepops.drop(columns={'people'})
    popadj = landussegmented.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
    popadj['newpop'] = popadj['people']*popadj['pop_factor']
    print('The adjusted 2018 population for England and Wales is', 
          popadj['newpop'].sum()/1000000, 'M')

    # need to retain the missing MSOAs for both population landuse outputs and HOPs
    popadj = popadj.drop(columns = {'myeadj', 'pop_factor','people'})
    popadj = popadj.rename(columns={'newpop':'people'})
    adjMSOA2018 = popadj['ZoneID'].drop_duplicates()
    

    Scot_adjust = format_scottish_mype()
    GB_adjusted = popadj.append(Scot_adjust)
    # this might not be needed but there were some zones that weren't behaving properly before
    check = GB_adjusted['ZoneID'].drop_duplicates()   
    otherrestofUK = landuseoutput[~landuseoutput.ZoneID.isin(check)]
    fullGBadjustment = GB_adjusted.append(otherrestofUK)
    
    print('Full population for 2018 is now =', 
          fullGBadjustment['people'].sum())
    print('check all MSOAs are present, should be 8480:', 
          fullGBadjustment['ZoneID'].drop_duplicates().count())
    fullGBadjustment = fullGBadjustment.reindex(columns = {'ZoneID', 'property_type', 
                                                           'household_composition', 
                                                           'employment_type'
                                                           })
    fullGBadjustment.to_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv', index = False)
    return(fullGBadjustment)
    del(otherrestofUK, check, Scot_Adjust, GB_adjusted, popadj)

    
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
               
    MYPEpop = fullGBadjustment.groupby(by=['ZoneID', 'property_type'], as_index = False).sum()
    
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
    
    
def ControlToLAD(LAdTranslation):
    LADActive = pd.read_csv().rename(columns={'Unnamed: 15':'Region' })
    EmploymentFigures = NPRSegments.groupby(by =['ZoneID', 'employment_type'], 
                                         as_index = False).sum().reindex(['ZoneID',
                                                               'employment_type',
                                                               'people'],axis=1)
    LadTranslation = pd.read_csv(ladPath).rename(columns={'lad_zone_id':'objectid',
                                             'msoa_zone_id':'MSOA'})
    LAD_controls = pd.read_csv(LADControlPAth)
                
    NRSegments = pd.read_csv('C:/NorMITs_Export/iter3/NPRSegments.csv')
    emp = NRSegments.groupby('employment_type').sum()
    NRSegments['ns_sec'].dtype
    NRSegments['ns_sec']= NRSegments['ns_sec'].astype('category')
    emp = NRSegments.groupby(by=['employment_type', 'ns_sec','SOC_category'], as_index = False).sum()
    NPRSegments2 = pd.read_csv('Y:/NorMITs Land Use/iter3/NPR_Segments.csv')
    emp2 = NPRSegments2.groupby(by=['employment_type', 'ns_sec','SOC_category'], as_index = False).sum()
        
            
        
