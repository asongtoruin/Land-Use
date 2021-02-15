# -*- coding: utf-8 -*-
"""
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
## TODO: ApplyNtEM Segments: Scottish MSOAs show population of 0s. Need to double check what's gone wrong here
# Audit that splits out population by England/Wales/Scotland using startswith and matching to actual population
# Need to check it reads everything in - if error then needs reporting
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


_default_addressbase_extract_path = (_default_home_dir+'/allResProperty'+ _default_zone_name +'Classified.csv')
_default_census_property_types_path = (_import_folder+'Census_Property_Type_Maps.xlsx')
_default_communal_types_path = (_import_folder+'Communal Establishments 2011 QS421EW/communal_msoaoutput.csv')
_default_communal_employment_path = (_import_folder+'Communal Establishments 2011 QS421EW/DC6103EW_sums.csv')
_default_census_population = (_import_folder+'Communal Establishments 2011 QS421EW/DC11004_formatted.csv')
_default_lad_translation = (_default_zone_folder+'Export/lad_to_msoa/lad_to_msoa.csv')    
_default_census_dat = (_import_folder+'Nomis Census 2011 Head & Household')
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_area_types = ('Y:/NorMITs Land Use/import/area_types_msoa.csv')
    
_default_lsoaRef = _default_zone_ref_folder+'UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_msoaRef = _default_zone_ref_folder+'UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
_default_ladRef = _default_zone_ref_folder+'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
_default_mladRef = _default_zone_ref_folder+'Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'
_nssecPath = _import_folder+'NPR Segmentation/processed data/TfN_households_export.csv'


# 1. Functions to read in the source data 
# todo: function to check addressbase_prep was run?

def get_addressbase_extract(path = _default_addressbase_extract_path):

    """
    Import a csv of AddressBase extract (2018) already filtered out and classified.

    This might point to pre-processing function or modelling folders in the future.

    Parameters
    ----------
    census_segmentation_path:
        Path to csv of AddressBase extract.

    Returns
    ----------
    AddressBase extract:
        DataFrame containing AddressBase extract.
    """
    ABPFile = pd.read_csv(path)

    print('Reading in AddressBase extract')
    
    return(ABPFile)
    

def get_communal_types(path = _default_communal_types_path):
    """
     Import a csv of Communal Establishments types.

    This might point to pre-processing function or modelling folders in the future

    Parameters
    ----------
    _default_communal_types_path:
        Path to csv of Communal Establishments by type 2011 totals. 
        This is Census data.

    Returns
    ----------
    UPRN lookup:
        DataFrame containing Communal Establishments by type 2011 totals
    """
    print('Reading in Communal Establishments by type 2011 totals')

    communal_types = pd.read_csv(path)
    return(communal_types)
     
def get_communal_employment(path = _default_communal_employment_path):
    """
    Import a csv of Communal Establishments by employment (fte/pte/stu).

    Parameters
    ----------
    _default_communal_employment_path:
        Path to csv of Communal Establishments employment data.

    Returns
    ----------
    Communal Employment Employment:
        DataFrame containing Communal Establishments by employment
    """
    print('Reading in Communal Establishments by employment 2011 totals')

    communal_employment = pd.read_csv(path)
    return(communal_employment)
    
def get_census_population(path = _default_census_population):
    """
    Import a csv of Communal Establishments by age categories.

    Parameters
    ----------
    _default_census_population:
        Path to csv of census population

    Returns
    ----------
    Communal Employment Employment:
        DataFrame containing population in 2011 by gender and age and zone
    """
    print('Reading in census population by age and gender 2011 totals')

    census_population = pd.read_csv(path)
    return(census_population)
     
# 2. Main analysis functions - everything related to census and segmentation
def PathConfig(datPath=_default_census_dat):
    
    dat = datPath
    files = os.listdir(dat)
     
    return(dat, files)
    
def set_wd(homeDir = _default_home, iteration = _default_iter):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()
    
def LSOACensusDataPrep(datPath, EWQS401, SQS401, EWQS402, SQS402, geography = _default_lsoaRef):
    """
    This function prepares the census data by picking fields out of the csvs:
        
    """
    # 401 = people 402 = properties
    # Takes LSOA shapefile from 'Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
    # However, join is on code so if you have different IDs, just change the path and it'll be fine.
    # TODO: need to add percentage filled by msoa to account for seasonal households
    # FilledProperties()
    geography = gpd.read_file(_default_lsoaRef)
    geography = geography.iloc[:, 0:3]
        
    EWQS401import = pd.read_csv(datPath + '/' + EWQS401)
    EWQS401numbers = EWQS401import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(EWQS401import)
    EWQS401numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']

    ## Something
    SQS401import = pd.read_csv(datPath + '/' + SQS401)
    SQS401numbers = SQS401import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(SQS401import)
    SQS401numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
    
    UKQS401 = pd.concat([EWQS401numbers, SQS401numbers]).copy()
    del(EWQS401numbers, SQS401numbers)
    UKQS401 = pd.wide_to_long(UKQS401, stubnames='cpt', i='geography_code', j='census_property_type').reset_index().rename(columns={"cpt": "population"})
    
    EWQS402import = pd.read_csv(datPath + '/' + EWQS402)
    EWQS402numbers = EWQS402import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(EWQS402import)
    EWQS402numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
    
    SQS402import = pd.read_csv(datPath + '/' + SQS402)
    SQS402numbers = SQS402import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(SQS402import)
    SQS402numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
    
    UKQS402 = pd.concat([EWQS402numbers, SQS402numbers]).copy()
    del(EWQS402numbers, SQS402numbers)
    UKQS402 = pd.wide_to_long(UKQS402, stubnames='cpt', i='geography_code', j='census_property_type').reset_index().rename(columns={"cpt": "properties"})
    
    UKHouseholdOccupancy = UKQS401.merge(UKQS402, how='left', on=['geography_code', 'census_property_type'])
    UKHouseholdOccupancyGeo = geography.merge(UKHouseholdOccupancy, how = 'left', left_on ='lsoa11cd', right_on='geography_code')
    del(UKHouseholdOccupancy)

    UKHouseholdOccupancyGeo['household_occupancy'] = UKHouseholdOccupancyGeo['population']/UKHouseholdOccupancyGeo['properties']
    return(UKHouseholdOccupancyGeo)
    
def AggregateCpt(cptData, groupingCol=None, writeOut=True):
    """
    # Take some census property type data and return hops totals
    """
    if not groupingCol:
        cptData = cptData.loc[:,['census_property_type','population', 'properties']]
        aggData = cptData.groupby('census_property_type').sum().reset_index()
        aggData['household_occupancy'] = aggData['population']/aggData['properties']
    else:
        cptData = cptData.loc[:,['census_property_type','population', 'properties', groupingCol]]
        aggData = cptData.groupby(['census_property_type', groupingCol]).sum().reset_index()
        aggData['household_occupancy'] = aggData['population']/aggData['properties']

   # if writeOut:
       # aggData.to_csv('cptlsoa2011.csv')
        
    return(aggData)

def ZoneUp(cptData, hlsaName = 'MSOA', zoneTranslationPath = _default_zone_folder+'Export/msoa_to_lsoa/msoa_to_lsoa.csv', 
           groupingCol='msoaZoneID'):
    """

    Function to raise up a level of spatial aggregation & aggregate at that level, then bring new factors back down
    # TODO: Might be nice to have this zone up any level of zonal aggregation
    Raise LSOA to MSOA for spatial aggregation


    """
    zoneTranslation = pd.read_csv(zoneTranslationPath)
    zoneTranslation = zoneTranslation.rename(columns={'lsoa_zone_id':'lsoaZoneID',
                                                      'msoa_zone_id':'msoaZoneID'})
    zoneTranslation = zoneTranslation.loc[:,['lsoaZoneID', groupingCol]]
    # Audit any missing objectids
    datLSOAs = len(cptData.loc[:,'objectid'].drop_duplicates())
    ztLSOAs = len(zoneTranslation.loc[:,'lsoaZoneID'])

    if datLSOAs == ztLSOAs:
        print('zones match 1:1 - zoning up should be smooth')
    else:
        print('some zones missing')
        # TODO: Be more specific with your criticism - could say which or how many, export missing?
    cptData = cptData.rename(columns={'lsoa11cd':'lsoaZoneID'})    
    cptData = cptData.merge(zoneTranslation, how='left', on='lsoaZoneID').reset_index()
    print(cptData)
    cptData = AggregateCpt(cptData, groupingCol=groupingCol)
        
    return(cptData)
    
def FinalHoCaseWhen(row):
    """
    Useful function when resolving household occupancies
    """
    if np.isnan(row['msoa_ho']):
        return(row['global_ho'])
    else:
        return(row['msoa_ho'])
        
def HoTypeCaseWhen(row):
    """
    Useful function when resolving household occupancies
    """
    if np.isnan(row['msoa_ho']):
        return('global')
    else:
        return('msoa')
        
def BalanceMissingHOPs(cptData, groupingCol='msoaZoneID', hlsaName = _default_zone_name, 
                       zoneTranslationPa0th = (_default_zone_folder+'Export/msoa_to_lsoa/msoa_to_lsoa.csv')):
    """
    # TODO: Replace global with LAD or Country - likely to be marginal improvements
    This resolves the  msoa/lad household occupancy
    """
    msoaAgg = ZoneUp(cptData, hlsaName = 'MSOA', 
                     zoneTranslationPath = _default_zone_folder+'Export/msoa_to_lsoa/msoa_to_lsoa.csv', 
                     groupingCol=groupingCol)
    msoaAgg = msoaAgg.loc[:,[groupingCol, 'census_property_type', 
                             'household_occupancy']].rename(columns={'household_occupancy': 'msoa_ho'})

    globalAgg = ZoneUp(cptData, hlsaName = 'MSOA', 
                       zoneTranslationPath = _default_zone_folder+'Export/msoa_to_lsoa/msoa_to_lsoa.csv', groupingCol=groupingCol)
    globalAgg = AggregateCpt(globalAgg, groupingCol=None)
    globalAgg = globalAgg.loc[:,['census_property_type', 
                                 'household_occupancy']].rename(columns={'household_occupancy': 'global_ho'})

    cptData = msoaAgg.merge(globalAgg, how='left', on='census_property_type')

    print('Resolving ambiguous household occupancies')
                
    cptData['final_ho'] = cptData.apply(FinalHoCaseWhen, axis=1)
    cptData['ho_type'] = cptData.apply(HoTypeCaseWhen, axis=1)
    cptData = cptData.drop(['msoa_ho', 'global_ho'], axis=1).rename(columns={'final_ho':'household_occupancy'})
    
    return(cptData)

def AggWapFactor(ksSub, newSeg):
    """
    Function to combine working age population factors to create NTEM 
    employment categories
    """
    ksSub = ksSub.groupby(['msoaZoneID','Gender']).sum().reset_index()
    ksSub['employment_type']=newSeg
    return(ksSub)

def CreateEmploymentSegmentation(bsq, 
    ksEmpImportPath=_import_folder+'/KS601-3UK/uk_msoa_ks601equ_w_gender.csv'):
    """
# Synthesise in employment segmentation using 2011 data
# TODO: Growth 2011 employment segments
# TODO employment category should probably be conscious of property type
# Get to segments:
# full time employment
# part time employment
# students
# not employed/students
    """
# Shapes
    msoaShp = gpd.read_file(_default_msoaRef).reindex(['objectid','msoa11cd'],axis=1)
    
    nonWa = ['under 16', '75 or over']
    workingAgePot = bsq[~bsq.Age.isin(nonWa)]
    nonWorkingAgePot = bsq[bsq.Age.isin(nonWa)]
# Add non working age placeholder
    placeholderValue = 'non_wa'
    nonWorkingAgePot['employment_type'] = placeholderValue
    del(placeholderValue)

# Import UK MSOA Employment - tranformed to long in R - most segments left in for aggregation here
# Factors are already built in R - will aggregate to 2 per msoa 1 for Males 1 for Females
    ksEmp = pd.read_csv(ksEmpImportPath).reindex(['msoaZoneID','Gender','employment_type','wap_factor'],axis=1)

# Change MSOA codes to objectids
    ksEmp = ksEmp.merge(msoaShp, how ='left', left_on='msoaZoneID', 
                        right_on='msoa11cd').drop(['msoa11cd','msoaZoneID'],
                                           axis=1).rename(columns=
                                                 {'objectid':'msoaZoneID'})
   
# full time employment =  sum(emp_ft, emp_se)
    ksFte = ksEmp[ksEmp.employment_type.isin(['emp_ft','emp_se'])]
    ksFte = AggWapFactor(ksFte, newSeg='fte')
# part time employment = sum(emp_pt)
    ksPte = ksEmp[ksEmp.employment_type.isin(['emp_pt'])] 
    ksPte = AggWapFactor(ksPte, newSeg='pte')
# students = sum(emp_stu)
    ksStu = ksEmp[ksEmp.employment_type.isin(['emp_stu'])]
    ksStu = AggWapFactor(ksStu, newSeg='stu')
# not employed/students = sum(unemp, unemp_ret, unemp_stu, unemp_care, 
# unemp_lts, unemp_other)
    ksUnm = ksEmp[ksEmp.employment_type.isin(['unemp','unemp_ret','unemp_stu',
                                              'unemp_care','unemp_lts',
                                              'unemp_other'])]
    ksUnm = AggWapFactor(ksUnm, newSeg='unm')

    ksEmp = ksFte.append(ksPte).append(ksStu).append(ksUnm).reset_index(drop=True)

    workingAgePot = workingAgePot.merge(ksEmp, how='left', on=['msoaZoneID', 'Gender'])
    workingAgePot['w_pop_factor'] = workingAgePot['pop_factor'] * workingAgePot['wap_factor']
    workingAgePot = workingAgePot.drop(['pop_factor','wap_factor'],axis=1).rename(
            columns={'w_pop_factor':'pop_factor'})

    bsq = workingAgePot.append(nonWorkingAgePot, sort=True)
    bsq = bsq.reindex(['msoaZoneID','Age','Gender','employment_type',
                       'household_composition','property_type','B','R',
                       'Zone_Desc','pop_factor'],axis=1)
# Pull out one msoa worth of segements to audit
    # testSegments = bsq[bsq.msoaZoneID == 1500]
    # testSegments.to_csv('testSegmentsMSOA.csv')
    
    return(bsq)
    
def CreateNtemSegmentation(bsqImportPath=_import_folder+'Bespoke Census Query/formatted_long_bsq.csv',
                    areaTypeImportPath = _import_folder+'/CTripEnd/ntem_zone_area_type.csv',
                    ksEmpImportPath=_import_folder+'/KS601-3UK/uk_msoa_ks601equ_w_gender.csv'):
    bsq = CreateNtemAreas(bsqImportPath)
    bsq = CreateEmploymentSegmentation(bsq)
    #bsq.to_csv('bsq.csv')
    return(bsq)

def CountMsoa(bsq):
    unqMSOA = bsq.reindex(['msoaZoneID'],axis=1).drop_duplicates()
    return(len(unqMSOA))

def CreateNtemAreas(bsqImportPath=_import_folder+'/Bespoke Census Query/formatted_long_bsq.csv',
                    areaTypeImportPath = _import_folder+'/CTripEnd/ntem_zone_area_type.csv'):
# Import Bespoke Census Query - already transformed to long format in R
    print('Importing bespoke census query')
    bsq = pd.read_csv(bsqImportPath)
# Import area types
    areaTypes = pd.read_csv(areaTypeImportPath)

# Shapes
    mlaShp = gpd.read_file(_default_mladRef).reindex(['objectid','cmlad11cd'],axis=1)
    msoaShp = gpd.read_file(_default_msoaRef).reindex(['objectid','msoa11cd'],axis=1)
# Lookups
# Bespoke census query types
    pType = pd.read_csv(_import_folder+'/Bespoke Census Query/bsq_ptypemap.csv')
    hType = pd.read_csv(_import_folder+'/Bespoke Census Query/bsq_htypemap.csv')
# Zonal conversions
    mlaLookup = pd.read_csv(_default_zone_folder+'Export/merged_la_to_msoa/merged_la_to_msoa.csv').reindex(['msoaZoneID', 'merged_laZoneID'],axis=1)
    ntemToMsoa = pd.read_csv(_default_zone_folder+'Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv').reindex(['ntemZoneID', 'msoaZoneID', 'overlap_ntem_pop_split_factor'],axis=1)
# Reduce age & gender categories down to NTEM requirements
    def SegmentTweaks(bsq, asisSegments, groupingCol, aggCols, targetCol, newSegment):
    # Take a bsq set, segments to leave untouched and a target column.
    # Sum and reclassify all values not in the untouched segments
        asisPot = bsq[bsq[groupingCol].isin(asisSegments)]
        changePot = bsq[~bsq[groupingCol].isin(asisSegments)]
        changePot = changePot.groupby(aggCols).sum().reset_index()
        changePot[targetCol] = newSegment
        changePot = changePot.reindex(['LAD_code','LAD_Desc','Gender','Age','Dwelltype','household_type','population'],axis=1)
        bsq = asisPot.append(changePot).reset_index(drop=True)
        return(bsq)

# All working age population comes in one monolithic block - 16-74
    bsq = SegmentTweaks(bsq, asisSegments = ['under 16', '75 or over'], 
                        groupingCol='Age', aggCols=[
                                'LAD_code','LAD_Desc','Gender','Dwelltype',
                                'household_type'], targetCol='Age',newSegment='16-74')
# Children have no gender in NTEM - Aggregate & replace gender with 'Children'
    bsq = SegmentTweaks(bsq, asisSegments = ['16-74','75 or over'], 
                        groupingCol='Age', aggCols=['LAD_code','LAD_Desc',
                                                    'Age','Dwelltype','household_type'],
                                                    targetCol='Gender', 
                                                    newSegment='Children')

    bsq = bsq.merge(pType, how='left', left_on='Dwelltype' , 
                    right_on='c_type').drop(['Dwelltype', 'c_type'], axis=1)
    bsq = bsq.merge(hType, how='left', on='household_type').drop('household_type',axis=1)
    bsqTotal = bsq.reindex(['LAD_code', 'LAD_Desc', 'population'],axis=1).groupby(
            ['LAD_code', 'LAD_Desc']).sum().reset_index().rename(columns = 
            {'population':'lad_pop'})
    bsq = bsq.merge(bsqTotal, how='left', on=['LAD_code','LAD_Desc'])
    del(bsqTotal)
    bsq['pop_factor'] = bsq['population']/bsq['lad_pop']
    bsq = bsq.reindex(['LAD_code','LAD_Desc','Gender', 'Age', 'property_type', 
                       'household_composition', 'pop_factor'],axis=1)

    # Append MSOA to Merged LAD lookup - will only derive English & Welsh MSOAs
    bsq = bsq.merge(mlaShp, how='left', left_on='LAD_code', 
                    right_on='cmlad11cd').drop('cmlad11cd',axis=1)
    bsq = bsq.merge(mlaLookup, how='left', left_on='objectid', 
                    right_on='merged_laZoneID').drop('objectid',axis=1)
    # Pull out one lad worth of segements to audit
    # testSegments = bsq[bsq.LAD_code == 'E41000001']
    # testSegments.to_csv('testSegments.csv')

    # TODO: This would be the place to tweak the 2011 population compositions to 2018
    # Define a basic function to count the MSOAs in the bsq - so I don't have 
    # to write it again later.

    print(CountMsoa(bsq),'should be 8480')
# Add area types (the story of how I ultimately fixed Scotland)
# Get an NTEM Zone for every MSOA - use the population lookup - ie. get the one
# with the most people, not a big field
    msoaToNtemOverlaps = ntemToMsoa.groupby(['msoaZoneID']).max(
            level='overlap_ntem_pop_split_factor').reset_index()
# Add area types to these sketchy MSOAs (in Scotland they're sketchy, 
# they're 1:1 in England and Wales)
    areaTypesMsoa = msoaToNtemOverlaps.merge(areaTypes, how='left', on='ntemZoneID')
    areaTypesMsoa = areaTypesMsoa.reindex(['msoaZoneID','R','B','Zone_Desc'],axis=1)
    # TODO: This is crucial for some later stuff - retain
    areaTypesMsoa.to_csv('areaTypesMSOA.csv', index = False)
# Fasten area types onto bsq
    bsq = bsq.merge(areaTypesMsoa, how='left', on='msoaZoneID')
# Derive North East and North West bsq data by area type
    unqMergedLad = bsq.reindex(['LAD_code','LAD_Desc'],axis=1).drop_duplicates().reset_index(drop=True)
    northUnqMergedLad = unqMergedLad.iloc[0:72]
    del(unqMergedLad)
    northMsoaBsq = bsq[bsq.LAD_code.isin(northUnqMergedLad.LAD_code)]
    genericNorthTypeBsq = northMsoaBsq.drop(['msoaZoneID',
                                             'merged_laZoneID', 
                                             'B'],axis=1).groupby(['R',
                                                'Gender','Age',
                                                'property_type',
                                                'household_composition']).mean().reset_index()
    del(northMsoaBsq)
# TODO: Spot check that these balance to 1
    audit = genericNorthTypeBsq.groupby('R').sum()
# Fix missing msoas in bsq
# Filter the list of msoas by area types to msoas not in the bsq list
    missingMsoa = areaTypesMsoa[~areaTypesMsoa.msoaZoneID.isin(
            bsq.msoaZoneID)]
    missingMsoa = missingMsoa.merge(genericNorthTypeBsq, 
                                    how='left', on='R')
# reindex bsq to match the generic zones (drop reference to mLAD)
    bsq = bsq.reindex(list(missingMsoa),axis=1)
# stack bsq - full msoa bsq
    bsq = bsq.append(missingMsoa).reset_index(drop=True)
    print(CountMsoa(bsq),'should be 8480')
# Create and export pop_factor audit
    audit = bsq.groupby(['msoaZoneID']).sum().reindex(['pop_factor'],
                       axis=1)
    audit.to_csv('msoa_pop_factor_audit.csv',index=False)
    landAudit = bsq.reindex(['msoaZoneID','Zone_Desc'],
                            axis=1).drop_duplicates().merge(
                                    msoaShp, how='inner', 
                                    left_on='msoaZoneID', 
                                    right_on='objectid').drop(
                                            'objectid',axis=1)
    landAudit.to_csv('landAudit.csv',index=False)
    
    return(bsq)
    
def FilledProperties():
    """
    this is a rough account for unoccupied properties
    using KS401UK LSOA level to infer whether the properties have any occupants
    """
   
    KS401 = pd.read_csv(_import_folder+'Nomis Census 2011 Head & Household/KS401UK_LSOA.csv')
  
    KS401permhops = KS401.reindex(columns = ['geography code', 
                                     'Dwelling Type: All categories: Household spaces; measures: Value', 
                                     'Dwelling Type: Household spaces with at least one usual resident; measures: Value'
                                     ])
    KS401permhops = KS401permhops.rename(columns =
                                             {'Dwelling Type: All categories: Household spaces; measures: Value':
                                                 'Total_Dwells', 
                                                 'Dwelling Type: Household spaces with at least one usual resident; measures: Value':
                                                 'Filled_Dwells', 
                                                 'geography code': 'geography_code'})                                    
                     
    #KS401UKpermhops.columns = ['geography_code', 'totalhops', 'filledhops']
    KS401permhops['Prob_DwellsFilled'] = KS401permhops['Filled_Dwells']/KS401permhops['Total_Dwells']
    
    KS401permhops = KS401permhops.drop(columns = {'Filled_Dwells', 'Total_Dwells'})
    zoneTranslationPath = _default_zone_folder+'Export/msoa_to_lsoa/msoa_to_lsoa.csv'
    zoneTranslation = pd.read_csv(zoneTranslationPath)
    zoneTranslation = zoneTranslation.rename(columns={'lsoa_zone_id':'lsoaZoneID',
                                                      'msoa_zone_id':'msoaZoneID'})

    zoneTranslation = zoneTranslation.loc[:,['lsoaZoneID', 'msoaZoneID']]
    KS401permhops = KS401permhops.rename(columns={'geography_code':'lsoaZoneID'})
    FilledProperties = KS401permhops.merge(zoneTranslation, on = 'lsoaZoneID')
    FilledProperties=FilledProperties.drop(columns={'lsoaZoneID'}).groupby(['msoaZoneID']).mean().reset_index()
   
    # the above filleddwellings probability is based on E+W so need to join back to Scottish MSOAs
    # needed for later when multiplying by this factor to adjust the household occupancies
    ukMSOA = gpd.read_file(_default_msoaRef).reindex(columns={'msoa11cd'}).rename(columns={'msoa11cd':'msoaZoneID'})
    FilledProperties2 = ukMSOA.merge(FilledProperties, on = 'msoaZoneID', how = 'outer')   
    FilledProperties = FilledProperties2.fillna(1)
    FilledProperties.to_csv('ProbabilityDwellfilled.csv', index = False)
    
    return(FilledProperties)
    
   
def ApplyHouseholdOccupancy(doImport=False, writeOut=True, \
        level=_default_zone_name, hopsPath=_import_folder+'/HOPs/hops_growth_factors.csv'):
    """
    # Import household occupancy data and apply to property data
    # TODO: toggles for unused 'level' parameter. Want to be able to run
    # at LSOA level when point correspondence is done.
    # TODO: Folders for outputs to seperate this process from the household 
    # classification
    """
    
    if doImport:
        balancedCptData = pd.read_csv('UKHouseHoldOccupancy2011.csv')
       # lsoacpt = pd.read_csv('cptlsoa2011.csv')
    else:
        censusDat = PathConfig(_import_folder+'/Nomis Census 2011 Head & Household')
        print(censusDat[1])
        
        # TODO: - This was a patch to get it to work fast and it did. 
        # Make these a bit cleverer to reduce risk of importing the wrong thing
        EWQS401 = 'QS401UK_LSOA.csv'
        SQS401 = 'QS_401UK_DZ_2011.csv'
        EWQS402 = 'QS402UK_LSOA.csv'
        SQS402 = 'QS402UK_DZ_2011.csv'
        # KS401 = 'KS401_UK_LSOA.csv'
        
        #cptData = LSOACensusDataPrep(censusDat[0], EWQS401, SQS401, EWQS402, SQS402, KS40EW, geography = lsoaRef))    
        cptData = LSOACensusDataPrep(censusDat[0], EWQS401, SQS401, EWQS402, SQS402, geography = _default_lsoaRef)
        
        # ZoneUp here to MSOA aggregations
        balancedCptData = BalanceMissingHOPs(cptData, groupingCol='msoaZoneID')
        balancedCptData = balancedCptData.fillna(0)
        FilledProperties = pd.read_csv(_default_home_dir+'/ProbabilityDwellfilled.csv')
        balancedCptData = balancedCptData.merge(FilledProperties, how = 'outer', on = 'msoaZoneID')
        balancedCptData['household_occupancy'] = balancedCptData['household_occupancy']*balancedCptData['Prob_DwellsFilled']
        balancedCptData = balancedCptData.drop(columns={'Prob_DwellsFilled'})
        balancedCptData.to_csv('UKHouseHoldOccupancy2011.csv', index=False)

    msoaCols = ['objectid', 'msoa11cd']
    ladCols = ['ladZoneID','msoaZoneID']
    ukMSOA = gpd.read_file(_default_msoaRef)
    ukMSOA = ukMSOA.loc[:,msoaCols]
    
    # Visual spot checks - count zones, check cpt
    audit = balancedCptData.groupby(['msoaZoneID']).count().reset_index()
    print('census hops zones =', audit['msoaZoneID'].drop_duplicates().count(),'should be', len(ukMSOA))
    print('counts of property type by zone', audit['census_property_type'].drop_duplicates())
    
    # Join MSOA ids to balanced cptdata
    ukMSOA = ukMSOA.rename(columns={'msoa11cd':'msoaZoneID'})
    balancedCptData = balancedCptData.merge(ukMSOA, how='left', 
                        on='msoaZoneID').drop('objectid', axis=1)
    # Import msoa to lad translation
    ladTranslation = pd.read_csv(_default_zone_folder+'Export/lad_to_msoa/lad_to_msoa.csv')
    ladTranslation = ladTranslation.rename(columns={'lad_zone_id':'ladZoneID', 
                                                    'msoa_zone_id':'msoaZoneID'}).loc[:,ladCols]
    unqLad = ladTranslation['ladZoneID'].unique()
    balancedCptData = balancedCptData.merge(ladTranslation, how='left', 
                        on='msoaZoneID')

    joinLad = balancedCptData['ladZoneID'].unique()

    ladCols = ['objectid','lad17cd']
    ukLAD = gpd.read_file(_default_ladRef)
    ukLAD = ukLAD.loc[:,ladCols]

    balancedCptData = balancedCptData.merge(ukLAD, how='left', 
                        left_on='ladZoneID', right_on='objectid').drop(['ladZoneID', 'objectid'],axis=1)

    if len(joinLad) == len(unqLad):
        print('All LADs joined properly')
    else:
        print('Some LAD zones not accounted for')
    del(unqLad, joinLad)

    hgCols = ['Area code','11_to_18']
    hopsGrowth = pd.read_csv(hopsPath).loc[:,hgCols]
    
    # using HOPS growth data to uplift the figures to 2018

    balancedCptData = balancedCptData.merge(hopsGrowth,
                        how='left', left_on='lad17cd', 
                        right_on='Area code').drop('Area code',axis=1).reset_index(drop=True)
    balancedCptData['household_occupancy_18'] = balancedCptData['household_occupancy']*1+balancedCptData['11_to_18']

    trimCols = ['msoaZoneID', 'msoa11cd', 'census_property_type', 
                'household_occupancy_18', 'ho_type']
    balancedCptData = balancedCptData.reindex(trimCols,axis=1)
    #balancedCptData = balancedCptData.drop(columns = {'msoa11cd'})

    # Read in all res property for the level of aggregation
    allResProperty = get_addressbase_extract()
    
    allResProperty = allResProperty.reindex(['ZoneID','census_property_type',
                                             'UPRN'],axis=1)
    allResPropertyZonal = allResProperty.groupby(['ZoneID', 
                            'census_property_type']).count().reset_index()
    del(allResProperty)
    
    
    if level == 'MSOA':
        
        allResPropertyZonal = allResPropertyZonal.merge(balancedCptData, 
                            how='inner', 
                            left_on=['ZoneID', 'census_property_type'], 
                            right_on=['msoaZoneID','census_property_type']).drop('msoaZoneID',axis=1)
    
    # Audit join - ensure all zones accounted for
        if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukMSOA['msoaZoneID'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')
                
       # allResPropertyZonal.merge(FilledProperties, on = 'ZoneID')
        allResPropertyZonal['population'] = allResPropertyZonal['UPRN'] * allResPropertyZonal['household_occupancy_18'] 
   
    # Create folder for exports
        arpMsoaAudit = allResPropertyZonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'],axis=1)
        hpaFolder = 'Hops Population Audits'
        nup.CreateFolder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        if writeOut:
            allResPropertyZonal.to_csv('classifiedResProperty' + level + '.csv', index = False)
    
        return(allResPropertyZonal)
        
    if level == 'LSOA':
        #here change msoa that is expected in the function above to lsoa Zone ID.
        #To do that we need the zone translations and lsoa table with zoneID objectid and also msoa lookup for the objectid!
        lsoaCols = ['objectid', 'lsoa11cd']
        #TODO: change the path for translations
        lsoa_lookup = pd.read_csv('Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz_cleaned_dbf.csv')
        LSOAzonetranslation = pd.read_csv(_default_zone_folder+ 'Export/msoa_to_lsoa/msoa_to_lsoa_pop_weighted.csv')  
        msoa_lookup = pd.read_csv('Y:/Data Strategy/GIS Shapefiles/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz_dbf.csv')
        
        lsoa_lookup = lsoa_lookup.rename(
            columns={
                    "objectid":"lsoaZoneID"
                    })
    
        msoa_lookup = msoa_lookup.rename(
            columns={
                    "objectid":"msoaZoneID"})
    
        zonetrans = pd.merge(LSOAzonetranslation, lsoa_lookup, how= "outer", on ='lsoaZoneID')
        zonetrans = zonetrans.merge(msoa_lookup, how = "outer", on='msoaZoneID')
        
        zonetrans = zonetrans.drop(columns={'lsoa_var', 'msoa_var', 
                                            'overlap_lsoa_split_factor', 
                                            'overlap_type', 'overlap_var', 
                                            'lsoa11nm', 'lsoa11nmw', 'st_areasha_x',
                                            'st_lengths_x', 'msoa11nm', 'msoa11nmw',
                                            'st_areasha_y', 'st_lengths_y'})

        zonetrans = zonetrans.rename(columns={
                "lsoa11cd":"ZoneID"})
        allResPropertyZonal = pd.merge(allResPropertyZonal, zonetrans, on = 'ZoneID')
    
        allResPropertyZonal = pd.merge(allResPropertyZonal, balancedCptData, \
                            how='inner', \
                            left_on=['msoaZoneID', 'census_property_type'], \
                            right_on=['msoaZoneID','census_property_type'])
        allResPropertyZonal = allResPropertyZonal.drop(columns=
                                                       {'lsoaZoneID', 'msoaZoneID', 
                                                        'overlap_msoa_split_factor'}
                                                        )
     #TODO need to introduce lsoa cols for this below to work                      
                                 
    # TODO: Insert some logic here to make it work for LSOA and OA level#
    # Going to look something like assigning areas their MSOA, 
    # joining on correspondence and dropping thee MSOA again.
        # Audit join - ensure all zones accounted for
        ukLSOA = gpd.read_file(_default_lsoaRef)
        ukLSOA = ukLSOA.loc[:,lsoaCols]
        
        if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukLSOA['lsoa11cd'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')
            
        allResPropertyZonal['population'] = allResPropertyZonal['UPRN'] * allResPropertyZonal['household_occupancy_18']
    # Create folder for exports
        arpMsoaAudit = allResPropertyZonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'],axis=1)
        hpaFolder = 'Hops Population Audits'
        nup.CreateFolder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        arpMsoaAudit = arpMsoaAudit['population'].sum()
        print(arpMsoaAudit)
        
        if writeOut:
            allResPropertyZonal.to_csv('classifiedResPropertyLSOA.csv', index = False)
            
            #it has 1900 too many people
       
    else:
            print ("no support for this zone")
            
def ApplyNtemSegments(classifiedResPropertyImportPath = 'classifiedResPropertyMSOA.csv',
                      bsqImportPath=_import_folder+'Bespoke Census Query/formatted_long_bsq.csv',
                      areaTypeImportPath = _import_folder+'CTripEnd/ntem_zone_area_type.csv',
                      ksEmpImportPath=_import_folder+'KS601-3UK/uk_msoa_ks601equ_w_gender.csv',
                      level = _default_zone_name, writeSteps = False):
                      
        """
        Function to join the bespoke census query to the classified residential 
        property data
        Problem here is that there are segments with attributed population 
        coming in from the bespoke census query that don't have properties to join on.
        so we need classified properties by msoa for this to work atm
        Import bespoke census query - this function creates it
        At the moment only works for MSOA
        """
        crp = pd.read_csv(classifiedResPropertyImportPath)
        crpCols = ['ZoneID', 'census_property_type', 'UPRN', 'household_occupancy_18', 'population']
        crp = crp.reindex(crpCols, axis=1)
        unqMSOA = crp.reindex(['ZoneID'],axis=1).drop_duplicates()
        len(unqMSOA)
        crp['population'].sum()

        bsq = CreateNtemSegmentation()
        
    #if level == 'MSOA':
        # Split bsq pop factors out to represent property type as well as zone
        
        factorPropertyType = bsq.reindex(['msoaZoneID','property_type',\
                        'pop_factor'],axis=1).groupby(['msoaZoneID','property_type']).sum().reset_index()
        factorPropertyType = factorPropertyType.rename(columns={'pop_factor':'pt_pop_factor'})
        bsq = bsq.merge(factorPropertyType, how='left', \
                    on=['msoaZoneID','property_type'])
    
        bsq['pop_factor'] = bsq['pop_factor']/bsq['pt_pop_factor']
        bsq = bsq.drop('pt_pop_factor',axis=1)
    
        testSegment = bsq[bsq['msoaZoneID']==1]
        testSegment = testSegment[testSegment['property_type'] == 4]
        testSegment['pop_factor'].sum() 
        # This is a good audit - all population factors by zoneID &
        # property type before the join
        segFolder = 'NTEM Segmentation Audits'
        nup.CreateFolder(segFolder)
        audit = bsq.reindex(['msoaZoneID','property_type', 'pop_factor'],\
                        axis=1).groupby(['msoaZoneID','property_type']).\
                        sum().reset_index()
        audit.to_csv(segFolder + '/Zone_PT_Factor_Pre_Join_Audit.csv',index=False)
        
        print('Should be near to zones x property types - ie. 8480 x 6 = 50880 :', bsq['pop_factor'].sum())
  
        msoaCols = ['objectid', 'msoa11cd']
        ukMSOA = gpd.read_file(_default_msoaRef)
        ukMSOA = ukMSOA.loc[:,msoaCols]

        # Join in MSOA names and format matrix for further steps
        # TODO: Some handling required here for other zoning systems
        bsq = bsq.merge(ukMSOA, how='left', left_on='msoaZoneID', right_on='objectid')
        bsqCols = ['msoa11cd', 'Age', 'Gender', 'employment_type', 'household_composition', 'property_type', 'R', 'pop_factor']
        bsq = bsq.reindex(bsqCols,axis=1)
        bsq = bsq.rename(columns={'R':'area_type'})
        unqMSOA = bsq.reindex(['msoa11cd'],axis=1).drop_duplicates()
        len(unqMSOA)
        
        audit = bsq.reindex(['msoa11cd','property_type', 'pop_factor'],axis=1).groupby(['msoa11cd','property_type']).sum().reset_index()
        audit.to_csv(segFolder + '/Zone_PT_Factor_Audit_Inter_Join.csv',index=False)

        bsqAudit = bsq.groupby(['msoa11cd', 'property_type']).count().reset_index()
        print(bsqAudit['pop_factor'].drop_duplicates()) 
        crpAudit = crp['population'].sum()
        print(crpAudit)
        # So far so good

        # TODO: Fix join issue.
        # inner join crp - will lose land use bits on non-classified & communal establishments
        crp = crp.merge(bsq, how='outer', left_on=['ZoneID','census_property_type'],right_on=['msoa11cd','property_type'])
        print('pop factor needs to be same as no of zones - 8480')
        print('population needs to resolve back to 60+ million once duplicates are removed')
        crp['pop_factor'].sum()
        crpAudit = crp['population'].drop_duplicates().sum()
        print(crpAudit)
    # Still fine
    
        crp['pop_factor'].sum()
        crpAudit = crp['population'].drop_duplicates().sum()
        print(crpAudit)
        # Still fine...

        # Audit bank
        unqMSOA = crp.reindex(['ZoneID'],axis=1).drop_duplicates()
        len(unqMSOA)
        print(crp['population'].sum())
        print(crp['pop_factor'].sum())
    
        # This is where it used to fall to bits
        # Apply population factor to populations to get people by property type
        crp['people'] =  crp['population'] * crp['pop_factor']
    
        outputCols = ['ZoneID', 'area_type', 'census_property_type', 'property_type', 'UPRN',
                        'household_composition', 'Age', 'Gender', 'employment_type', 'people']
        crp = crp.reindex(outputCols,axis=1)
        crp = crp.rename(columns={'UPRN':'properties'})
    
        pop = crp['people'].sum()
        print('Final population', pop)
        crp = crp.dropna()
        print('Final population after removing nans', pop)
       
        crp.to_csv(_default_home_dir+ '/landUseOutput' + level + '.csv', index = False)
    
    # Total MSOA Pop Audit
        msoaAudit = crp.reindex(['ZoneID', 'people'], axis=1).groupby('ZoneID').sum()
        msoaAudit.to_csv(segFolder +'/2018MSOAPopulation_OutputEnd.csv',index=False)
    
        return(crp, bsq)
                    
def communal_establishments_splits():
    """
    Function to establish the proportions of communal establishment population 
    across zones and gender and age
    
    """
    
    communal_types= get_communal_types().reindex(columns=['msoa11cd', 'Age', 'gender', 
                                              'Total_people']).rename(
                                               columns={'msoa11cd':'msoacd', 
                                              'gender':'Gender'})
    
    communal_types = communal_types.replace({'Gender':{'male':'Male', 'female':'Female'}})
    
    communal_types.loc[communal_types['Age'] == 'under 16', 'Gender'] = 'Children'
    communal_types['communal_total'] = communal_types.groupby(['msoacd', 'Age', 'Gender']
                                            )['Total_people'].transform('sum')

    communal_types = communal_types.drop(columns={'Total_people'}).drop_duplicates()
    # merge with 2011 census totals per Zone per gender and age 
    census_population = get_census_population().replace({'Gender':{'male':'Male', 'female':'Female'}})
    census_populationB = pd.melt(census_population, id_vars = ['msoacd', 'Gender'], 
                                    value_vars = ['under 16', '16-74', '75 or over'
                                     ]).rename(columns={'variable':'Age', 
                                     'value':'2011pop'})
    
    census_populationB.loc[census_populationB['Age']=='under 16', 'Gender']= 'Children'
    census_populationB['2011pop2'] = census_populationB.groupby(['msoacd', 'Age', 'Gender']
                                            )['2011pop'].transform('sum')
    
    census_population = census_populationB.drop(columns={'2011pop'}).drop_duplicates()
    
    print('Population in 2011 was', census_population['2011pop2'].sum())
    communal_establishments = communal_types.merge(census_population, on = ['msoacd', 'Age', 'Gender'], 
                               how = 'outer')
    print('Working out the communal establishment population splits per zone, age and gender for 2011')
    
    communal_establishments['CommunalFactor']= (
            communal_establishments['communal_total']/communal_establishments['2011pop2'])
    
    communal_establishments = communal_establishments.drop(columns={'2011pop2'})
    print('Communal establishments average split for 2011 was', communal_establishments['CommunalFactor'].mean())
    communal_establishments = communal_establishments.replace({'Gender':{'Female':'Females'}})    

    return(communal_establishments)
    
    
def communal_establishments_employment():
    communal_emp = get_communal_employment()
    communal_emp = pd.melt(communal_emp, id_vars = ['Gender', 'Age'], value_vars = 
                     ['fte', 'pte', 'unm', 'stu']).rename(columns={'variable':'employment_type',
                     'value':'splits'})
    communal_emp['total'] = communal_emp.groupby(['Age', 'Gender']
                                            )['splits'].transform('sum')
    communal_emp['emp_factor']= communal_emp['splits']/communal_emp['total']
    communal_emp = communal_emp.drop(columns ={'splits', 'total'})
    
    print('Communal establishments employment splits done')
    return(communal_emp)
    
def join_establishments(level = _default_zone_name):
    areatypes = pd.read_csv(_default_area_types)
    areatypes = areatypes.drop(columns={'zone_desc'})
    areatypes = areatypes.rename(columns = {'msoa_zone_id':'ZoneID'})
    # add areatypes
    communal_establishments = communal_establishments_splits()
    communal_emp = communal_establishments_employment()
    
    # need to split out the NonWork Age to be able to sort out employment
    NonWorkAge = ['under 16', '75 or over']
    CommunalEstActive = communal_establishments[~communal_establishments.Age.isin(NonWorkAge)].copy()
    CommunalNonWork = communal_establishments[communal_establishments.Age.isin(NonWorkAge)].copy()
    # non_wa for employment_type for under 16 and 75 or over people
    CommunalNonWork.loc[CommunalNonWork['Age']=='under 16','employment_type']= 'non_wa'
    CommunalNonWork.loc[CommunalNonWork['Age']=='75 or over','employment_type']= 'non_wa'
    CommunalNonWork = CommunalNonWork.rename(columns={'communal_total':'people'}
                                                        ).drop(columns={'CommunalFactor'})
    
    # apply employment segmentation to the active age people
    CommunalEstActive = CommunalEstActive.merge(communal_emp, on = ['Age', 'Gender'],
                                                how = 'outer')
    CommunalEstActive['comm_people'] = CommunalEstActive['communal_total']*CommunalEstActive['emp_factor']
    CommunalEstActive['comm_people'].sum()
    CommunalEstActive = CommunalEstActive.drop(columns={'communal_total', 'CommunalFactor','emp_factor'}
                                        ).rename(columns={'comm_people':'people'})
    communal_establishments = CommunalEstActive.append(CommunalNonWork, sort = True)
    print('Communal Establishment total for 2018 should be ~1.1m and is ',
          communal_establishments['people'].sum()/1000000)

    communal_establishments['property_type'] = 8
    communal_establishments = communal_establishments.rename(columns={'msoacd':'ZoneID'})
    communal_establishments = communal_establishments.merge(areatypes, on = 'ZoneID')
    
    #### bring in landuse for hc ####
    landuse = pd.read_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'.csv')
    zones = landuse["ZoneID"].drop_duplicates().dropna()
    Ezones = zones[zones.str.startswith('E')]
    Elanduse = landuse[landuse.ZoneID.isin(Ezones)]
    Restlanduse = landuse[~landuse.ZoneID.isin(Ezones)].drop(columns={'properties', 'census_property_type'})
    
# work out household composition - arguably not needed - see IW's comments 09/06/20        
    HouseComp = Elanduse.groupby(by = ['area_type', 'Age', 'Gender', 
                                        'employment_type', 'household_composition'], 
                                        as_index = False).sum()

    HouseComp['total'] = HouseComp.groupby(by=['area_type', 'Age', 'Gender', 
                                     'employment_type'])['people'].transform('sum')
    HouseComp['hc'] = HouseComp['people']/HouseComp['total'] 
    HouseComp = HouseComp.drop(columns={'total', 'people', 'property_type'})
    
    CommunalEstablishments = communal_establishments.merge(HouseComp, 
                                              on=['area_type', 'Age', 'Gender',
                                                  'employment_type'], how = 'outer')
    CommunalEstablishments['newpop'] = CommunalEstablishments['people']*CommunalEstablishments['hc']
    CommunalEstablishments = CommunalEstablishments.drop(
                columns = {'people', 'hc', 'properties','census_property_type'}).rename(columns= {'newpop': 'people'})
    CommunalEstablishments['people'].sum()
    CommunalEstFolder = 'CommunalEstablishments'
    nup.CreateFolder(CommunalEstFolder)
    CommunalEstablishments.to_csv(CommunalEstFolder + '/' + _default_zone_name + 
                                      'CommunalEstablishments2011.csv', index=False)
    CommunalEstablishments['people'].sum()
    cols = ['ZoneID', 'area_type', 'property_type', 'Age', 'Gender', 'employment_type', 
                       'household_composition', 'people'] 
    CommunalEstablishments = CommunalEstablishments.reindex(columns = cols)
    landuse = landuse.reindex(columns=cols)
    landusewComm = landuse.append(CommunalEstablishments)
    print('Joined communal communitiies. Total pop for GB is now', landusewComm['people'].sum())
    landusewComm.to_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_withCommunal.csv')   

def LanduseFormatting(landusePath = _default_home_dir+'/landuseOutput'+_default_zone_name+'_withCommunal.csv'):
    """
    Combines all flats into one category, i.e. property types = 4,5,6

    Parameters
    ----------
    landusePath:
        Path to Census segmented property linked landuse from Main build hh script
    Returns
    ----------
    formattedLanduse:
        DataFrame containing landuse with combined flats.
    """
    
        # 1.Combine all flat types. Sort out flats on the landuse side; actually there's no 7
    landuse = pd.read_csv(landusePath)
    landuse['new_prop_type'] = landuse['property_type']
    landuse.loc[landuse['property_type']==5, 'new_prop_type'] = 4
    landuse.loc[landuse['property_type']==6, 'new_prop_type'] = 4
    
    landuse = landuse.drop(columns = 'property_type').rename(columns = {'new_prop_type':'property_type'})
    landuse['people'].sum()
    landuse = landuse.to_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_stage3.csv')

def ApplyNSSECSOCsplits():
        """
        Parameters
        ----------
        nssecPath:
            Path to Census NS-SEC table
        Returns
        ----------
            NS-SEC from Census       
            house type definition - change NS-SEC house types detached etc to 1/2/3/4
        """

        nssec = pd.read_csv(_nssecPath)
        nssec.loc[nssec['house_type'] == 'Detached', 'property_type'] = 1
        nssec.loc[nssec['house_type'] == 'Semi-detached', 'property_type'] = 2
        nssec.loc[nssec['house_type'] == 'Terraced', 'property_type'] = 3
        nssec.loc[nssec['house_type'] == 'Flat', 'property_type'] = 4
        nssec = nssec.drop(columns={'house_type'})

        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 1-2', 'ns_sec'] = 1
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 3-5', 'ns_sec'] = 2
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 6-7', 'ns_sec'] = 3
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 8', 'ns_sec'] = 4
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC L15', 'ns_sec'] = 5
        nssec = nssec.drop(columns={'NS_SeC'}).rename(columns = {'MSOA name': 'ZoneID'})

        # all economically active in one group 
        nssec = nssec.rename(columns = {'Economically active FT 1-3':'FT higher',
                                'Economically active FT 4-7': 'FT medium',
                                'Economically active FT 8-9':'FT skilled',
                                'Economically active PT 1-3':'PT higher',
                                'Economically active PT 4-7':'PT medium',
                                'Economically active PT 8-9':'PT skilled',
                                'Economically active unemployed':'unm',
                                'Economically inactive':'children',
                                'Economically inactive retired':'75 or over',
                                'Full-time students':'stu'}).drop(columns={
                                        'TfN area type'})
        nssec2 = nssec.copy()
        # rename columns and melt it down
        nssec = nssec.rename(columns = {'msoa_name':'ZoneID'})
        nssec_melt = pd.melt(nssec, id_vars = ['ZoneID', 'property_type', 'ns_sec'], 
                     value_vars = ['FT higher', 'FT medium', 'FT skilled', 
                                   'PT medium', 'PT skilled',
                                   'PT higher', 'unm', 'children', '75 or over', 'stu'])
        nssec_melt = nssec_melt.rename(columns = {'variable':'employment_type', 
                                      'value':'numbers'})
        # map out categories to match the landuse format
        nssec_melt['SOC_category'] = nssec_melt['employment_type']  
        nssec_melt['Age'] = '16-74'
        nssec_melt.loc[nssec_melt['employment_type'] == 'children', 'Age']= 'under 16'
        nssec_melt.loc[nssec_melt['employment_type'] == '75 or over', 'Age']= '75 or over'
        nssec_melt = nssec_melt.replace({'employment_type':{ 'FT higher':'fte', 
                                         'FT medium':'fte',
                                         'FT skilled':'fte',
                                         'PT higher':'pte',
                                         'PT skilled':'pte',
                                         'PT medium':'pte',
                                         'children':'non_wa',
                                         '75 or over':'non_wa'
                                         }})
        nssec_melt = nssec_melt.replace({'SOC_category':{'FT higher':'1',
                                             'FT medium':'2',
                                             'FT skilled':'3',
                                             'PT higher':'1',
                                             'PT medium':'2',
                                             'PT skilled':'3',
                                             'unm':'NA',
                                             '75 or over':'NA',
                                             'children':'NA'}})
        # split the nssec into inactive and active
        nssec_formatted = nssec_melt.to_csv(_default_home_dir+'/NSSECformatted'+_default_zone_name +'.csv')
        inactive = ['stu', 'non_wa']
        InactiveNSSECPot = nssec_melt[nssec_melt.employment_type.isin(inactive)].copy()
        ActiveNSSECPot = nssec_melt[~nssec_melt.employment_type.isin(inactive)].copy()

        del(nssec_melt, nssec)

        
#### Active Splits ####     
        areatypes = pd.read_csv(_default_area_types).rename(columns={
                         'msoa_zone_id':'ZoneID'}).drop(columns={'zone_desc'})
        ActiveNSSECPot = ActiveNSSECPot.merge(areatypes, on = 'ZoneID')
        # MSOAActiveNSSECSplits for Zones
        MSOAActiveNSSECSplits = ActiveNSSECPot.copy()
        MSOAActiveNSSECSplits['totals'] = MSOAActiveNSSECSplits.groupby(['ZoneID', 'property_type',
                      'employment_type'])['numbers'].transform('sum')
        MSOAActiveNSSECSplits['empsplits'] = MSOAActiveNSSECSplits['numbers']/MSOAActiveNSSECSplits['totals']
        # For Scotland
        GlobalActiveNSSECSplits = ActiveNSSECPot.copy()
        GlobalActiveNSSECSplits = ActiveNSSECPot.groupby(['area_type', 'property_type',
                                                   'employment_type', 'Age', 
                                                   'ns_sec', 'SOC_category'], 
                                                    as_index = False).sum()
        GlobalActiveNSSECSplits['totals'] = GlobalActiveNSSECSplits.groupby(['area_type', 'property_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        GlobalActiveNSSECSplits['global_splits'] = GlobalActiveNSSECSplits['numbers']/GlobalActiveNSSECSplits['totals']
                
        GlobalActiveNSSECSplits = GlobalActiveNSSECSplits.drop(columns = {'numbers', 'totals'})
        # for communal establishments
        AverageActiveNSSECSplits = ActiveNSSECPot.copy()

        AverageActiveNSSECSplits = AverageActiveNSSECSplits.groupby(by=['area_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        AverageActiveNSSECSplits['totals2'] = AverageActiveNSSECSplits.groupby(['area_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        AverageActiveNSSECSplits['average_splits']= AverageActiveNSSECSplits['numbers']/AverageActiveNSSECSplits['totals2']
        AverageActiveNSSECSplits['SOC_category']= 'NA'
        AverageActiveNSSECSplits = AverageActiveNSSECSplits.drop(columns={'totals2', 'numbers', 'property_type'})

#### InactiveSplits ####
        """
        There is 17m in this category
        Sorts out the Inactive splits   
        CommunalEstablishment segments won't have splits so use Area types for 'globalsplits'
        
        """
        InactiveNSSECPot = InactiveNSSECPot.merge(areatypes, on='ZoneID')
        # Zone splits
        MSOAInactiveNSSECSplits = InactiveNSSECPot.copy()
        MSOAInactiveNSSECSplits['totals'] = MSOAInactiveNSSECSplits.groupby(['ZoneID', 'property_type',
                        'Age','employment_type'])['numbers'].transform('sum')
        MSOAInactiveNSSECSplits['msoa_splits'] = MSOAInactiveNSSECSplits['numbers']/MSOAInactiveNSSECSplits['totals']
        MSOAInactiveNSSECSplits['SOC_category'] ='NA' 
        MSOAInactiveNSSECSplits = MSOAInactiveNSSECSplits.drop(columns={'totals', 'numbers'})
        MSOAInactiveNSSECSplits['SOC_category'] ='NA' 
        # For Scotland
        GlobalInactiveNSSECSplits = InactiveNSSECPot.copy()
        GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.groupby(by=['area_type', 
                                                                          'property_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        GlobalInactiveNSSECSplits['totals2'] = GlobalInactiveNSSECSplits.groupby(['area_type', 'property_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        GlobalInactiveNSSECSplits['global_splits'] = GlobalInactiveNSSECSplits['numbers']/GlobalInactiveNSSECSplits['totals2']
        GlobalInactiveNSSECSplits['SOC_category'] ='NA' 
        GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.drop(columns={'totals2', 'numbers'})
        # for communal establishments
        AverageInactiveNSSECSplits = InactiveNSSECPot.copy()
        AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.groupby(by=['area_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        AverageInactiveNSSECSplits['totals2'] = AverageInactiveNSSECSplits.groupby(['area_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        AverageInactiveNSSECSplits['average_splits']= AverageInactiveNSSECSplits['numbers']/AverageInactiveNSSECSplits['totals2']
        AverageInactiveNSSECSplits['SOC_category']= 'NA'
        AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.drop(columns={'totals2', 'numbers', 'property_type'})
        
        InactiveSplits = MSOAInactiveNSSECSplits.merge(GlobalInactiveNSSECSplits, 
                                                       on = ['area_type', 'property_type',
                                                             'employment_type', 'Age',
                                                             'SOC_category', 'ns_sec'], how = 'right')
                                                                
        InactiveSplits = InactiveSplits.merge(AverageInactiveNSSECSplits, 
                                              on = ['area_type',
                                                'employment_type', 'SOC_category','ns_sec','Age'
                                                ], how = 'right')
        # check where there's no splitting factors, use the zone average for age?
        InactiveNSSECPot['SOC_category'] ='NA' 
        InactiveSplits['splits2'] = InactiveSplits['msoa_splits']  
        InactiveSplits['splits2']= InactiveSplits['splits2'].fillna(InactiveSplits['global_splits'])          
        InactiveSplits['splits2']= InactiveSplits['splits2'].fillna(InactiveSplits['average_splits'])
        InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['msoa_splits'], 'type'] = 'msoa_splits' 
        InactiveSplits.loc[(InactiveSplits['splits2'] == InactiveSplits['global_splits']), 'type'] = 'global_splits' 
        InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['average_splits'], 'type'] = 'average_splits' 
        # InactiveSplits = InactiveSplits.drop(columns={'numbers_x','numbers_y', 'totals2_x', 'totals2_y', 'msoa_splits'})
        #InactiveSplits = InactiveSplits.drop(columns={'average_splits', 'global_splits'})
        InactiveSplits = InactiveSplits.drop(columns={'msoa_splits', 'global_splits'})
        InactiveSplitsAudit = InactiveSplits.groupby(by = ['ZoneID', 'property_type', 'Age' ]).sum()
          
###### APPLICATION OF SPLITS Apply EW splits ####
        #England and Wales - applies splits for inactive people
        # apply splits
        landuse = pd.read_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_stage3.csv')
        Inactive_categs = ['stu', 'non_wa']
        #landuse = pd.read_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_stage3.csv')
        ActivePot = landuse[~landuse.employment_type.isin(Inactive_categs)].copy()
        InactivePot = landuse[landuse.employment_type.isin(Inactive_categs)].copy()

        #InactivePot = InactivePot.groupby(by=['ZoneID', 'area_type', 'property_type',
               #                               'Age', 'employment_type'], as_index = False).sum()
            
        # take out Scottish MSOAs from this pot - nssec/soc data is only for E+W
        # Scotland will be calculated based on area type
        areatypes = pd.read_csv(_default_area_types).drop(columns={'zone_desc'}
                                    ).rename(columns={'msoa_zone_id':'ZoneID'})
        ZoneIDs = ActivePot['ZoneID'].drop_duplicates().dropna()
        Scott = ZoneIDs[ZoneIDs.str.startswith('S')]
        ActiveScot = ActivePot[ActivePot.ZoneID.isin(Scott)].copy()
        ActiveScot = ActiveScot.drop(columns={'area_type'})
        ActiveScot = ActiveScot.merge(areatypes, on = 'ZoneID')
        #ActiveEng['people'].sum()
        ActiveEng = ActivePot[~ActivePot.ZoneID.isin(Scott)].copy()
        InactiveScot = InactivePot[InactivePot.ZoneID.isin(Scott)].copy()
        InactiveScot = InactiveScot.drop(columns={'area_type'})
        InactiveScot = InactiveScot.merge(areatypes, on = 'ZoneID')
        InactiveEng = InactivePot[~InactivePot.ZoneID.isin(Scott)].copy()
        print("total number of economically active people in the E+W landuse"+
              "pot should be around 41m and is ", ActiveEng['people'].sum()/1000000)
       
        CommunalEstablishments = [8]
        CommunalInactive = InactiveEng[InactiveEng.property_type.isin(CommunalEstablishments)].copy()
        CommunalInactive['people'].sum()
        InactiveNotCommunal = InactiveEng[~InactiveEng.property_type.isin(CommunalEstablishments)].copy()
        Inactive_Eng = InactiveSplits.merge(InactiveNotCommunal, on = ['ZoneID', 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        Inactive_Eng['newpop']= Inactive_Eng['people'].values*Inactive_Eng['splits2'].values
              
        CommunalInactive = CommunalInactive.merge(AverageInactiveNSSECSplits, on =[ 'area_type',
                                                'employment_type','Age'
                                                ], how = 'left')
        CommunalInactive['newpop'] = CommunalInactive['people']*CommunalInactive['average_splits']
        print("Communal Inactive should be about 600k and is ", CommunalInactive['newpop'].sum())
              
#### Apply Scottish Inactive ####
    
        #Scotland - applies splits for inactive people
        InactiveScot['people'].sum()
        InactiveScotland = GlobalInactiveNSSECSplits.merge(InactiveScot, on = [ 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        InactiveScotland['newpop']= InactiveScotland['people'].values*InactiveScotland['global_splits'].values
        InactiveScotland['newpop'].sum()
              
#### Apply EW active splits ####       
        # England and Wales - applies splits for active people
        CommunalEstablishments = [8]
        CommunalActive = ActiveEng[ActiveEng.property_type.isin(CommunalEstablishments)].copy()
        ActiveNotCommunal = ActiveEng[~ActiveEng.property_type.isin(CommunalEstablishments)].copy()
        Active_emp = ActiveNotCommunal.merge(MSOAActiveNSSECSplits, on = ['ZoneID', 'Age', 
                                                                          'property_type', 'employment_type'
                                                                          ],how = 'outer')
        # apply the employment splits for ActivePot to work out population
        Active_emp.groupby('employment_type')
        Active_emp['newpop'] = Active_emp['people']*Active_emp['empsplits']
 
        Active_emp = Active_emp.drop(columns = {'area_type_x', 'area_type_y'})
        Active_emp = Active_emp.merge(areatypes, on = 'ZoneID')
        if (Active_emp['people'].sum() < (40.6*0.01)):
            print('something has gone wrong with splits')
        else: print('EWsplits has worked fine')
        CommunalActive = CommunalActive.merge(AverageActiveNSSECSplits, on =[ 'area_type',
                                                'employment_type','Age'
                                                ], how = 'left')
        CommunalActive['newpop'] = CommunalActive['people']*CommunalActive['average_splits']

        # apply error catcher here if it's within 10% then accept  

#### Apply Scottish Active splits ####
        ActiveScotland = GlobalActiveNSSECSplits.merge(ActiveScot, on = [ 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        ActiveScotland['newpop']= ActiveScotland['people'].values*ActiveScotland['global_splits'].values
        ActiveScotland['newpop'].sum()
              
#### AppendAllGroups ####

        NPRSegments = ['ZoneID', 'area_type', 'property_type', 'Age', 'employment_type', 
                       'ns_sec', 'SOC_category', 'newpop'] 
        CommunalInactive = CommunalInactive.reindex(columns= NPRSegments)
        Inactive_Eng = Inactive_Eng.reindex(columns=NPRSegments)
        ActiveScotland = ActiveScotland.reindex(columns = NPRSegments)
        CommunalActive = CommunalActive.reindex(columns=NPRSegments)
        Active_emp = Active_emp.reindex(columns= NPRSegments)
        InactiveScotland= InactiveScotland.reindex(columns= NPRSegments)
        
        All = CommunalInactive.append(Inactive_Eng).append(CommunalActive).append(Active_emp)
        All = All.append(InactiveScotland).append(ActiveScotland)
        All = All.rename(columns={'newpop':'people'})
        All.to_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_stage4.csv')
        print(All['people'].sum())
            
def run_main_build():
    set_wd()
    FilledProperties()
    ApplyHouseholdOccupancy()
    ApplyNtemSegments()
    join_establishments()
    LanduseFormatting()
    ApplyNSSECSOCsplits()

    





    
    
    
    
 
