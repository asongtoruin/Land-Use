# -*- coding: utf-8 -*-
"""
Created on: 25/02/2018

File purpose:

Python wrapper for queries to land use data for NoRMITs land use.
Queries OS Addressbase premium and Master Map data to return land use 
by area types.
Zoning system translation queries built in for model conversion purposes.
"""
# TODO: Path to universally findable repo
# TODO change so that any zone can be fed in
# path to locally cloned git repo

import gc
import os
import sys
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Demand Tool/Python/Zone Translation')
sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Utilities/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Land Use/Python/GB Property Database')

import nest_functions_v95 as nf
import nu_project as nup
import sql_connect as sc
import numpy as np
import pandas as pd
import geopandas as gpd

#import nlu_census_data_prep as cdp and import nlu_ntem_segment_prep as nsp 
# are now integrated within this script 
# This works fine & has no business flagging a warning
from shapely.geometry import *
###

# Set shapefile references
lsoaRef = 'Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
msoaRef = 'Y:/Data Strategy/GIS Shapefiles/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
ladRef = 'Y:/Data Strategy/GIS Shapefiles/LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
mladRef = 'Y:/Data Strategy/GIS Shapefiles/Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'

defaultZoningPath = msoaRef 
#(choose between the 4 listed above, though only lsoa and msoa works for now)

defaultZoneNames = ['LSOA','MSOA']
defaultZoneName = 'MSOA' #MSOA or LSOA

defaultHomeDir = 'C/NorMITs Land Use'
defaultIter = 'iter2_MSOA'

# Set root wd
# needs to be one from ['OA', 'LSOA', 'MSOA']
# TODO: Audit up front and bail out early if not


def SetWd(homeDir = defaultHomeDir, iteration=defaultIter):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()

# Setup functions

def CountListShp(shp=lsoaRef, idCol=None):
    # Go and fetch a shape and return a count and a list of unq values
    # TODO: Make this properly shape agnostic
    shp = gpd.read_file(shp)
    if idCol is None:
        idCol=list(shp)[0]
    shp = shp.loc[:,idCol]
    return(len(shp),shp)
    
def BuildCorePropertyData(writeOut=True):
    
    # TODO: Check if the data has been imported already - if so skip the calls
    importString = (os.getcwd() + '/')
    writtenFiles = os.listdir(importString)
    
    if writeOut:
        print('Writing project files to project folder as csv - \
              this may take a while')
    # This can potentially take a geographic limiter off the bat 
    # - needs the ABP to be spatially enabled ie. PostGIS
    # Bring in data from SQL server, could be parametised to work with 
    # any AB or ABP data. 
    conn = sc.ConnectSQL()
    print("SQL now connected")

    # Query to get count of records by all classification codes'
    allCodes = ('SELECT [CLASSIFICATION_CODE], COUNT(*) AS \'RECORD_COUNT\'' +
                'FROM [psma].[abp_class_records]' + 
                'GROUP BY [CLASSIFICATION_CODE]')

    allResCodes = ('SELECT [CLASSIFICATION_CODE],' +
                   'COUNT(*) AS \'RECORD_COUNT\'' +
                   'FROM [psma].[abp_class_records]' +
                   'WHERE [CLASSIFICATION_CODE] LIKE \'R%\'' +
                   'GROUP BY [CLASSIFICATION_CODE]')
    
    allProperties = ('SELECT psma.abp_class_records.UPRN, '+
                    'psma.abp_class_records.CLASSIFICATION_CODE, '+ 
                    'psma.abp_class_records.LAST_UPDATE_DATE, '+
                    'psma.abp_blpu_records.BLPU_STATE, '+
                    'psma.abp_blpu_records.LOGICAL_STATUS, '+
                    'psma.abp_blpu_records.ADDRESSBASE_POSTAL, '+
                    'psma.abp_blpu_records.X_COORDINATE, '+
                    'psma.abp_blpu_records.Y_COORDINATE, '+
                    'psma.abp_blpu_records.POSTCODE_LOCATOR '+
                    'FROM psma.abp_class_records '+
                    'INNER JOIN psma.abp_blpu_records '+ 
                    'ON psma.abp_class_records.UPRN = psma.abp_blpu_records.UPRN '+
                    'WHERE psma.abp_class_records.CLASS_SCHEME = '
                    '\'AddressBase Premium Classification Scheme\'')

    allAddresses = ('SELECT [UPRN], [ORGANISATION_NAME], [DEPARTMENT_NAME], '+
                    '[SUB_BUILDING_NAME], [BUILDING_NAME], [BUILDING_NUMBER] '+
                    'FROM [psma].[abp_dpa_records]')
    
    uprnLookup = 'SELECT * FROM [ogl].[uprn_lookup_jul18]'
    
    if writeOut:
        if not 'allCodeCount.csv' in writtenFiles:
            allCodeCount = pd.read_sql(allCodes, conn)
            print('Writing all property code audit counts')
            allCodeCount.to_csv('allCodeCount.csv', index=False)
        else:
            print('Property code audit counts already written')
            
        if not 'allResCodeCount.csv' in writtenFiles:
            allResCodeCount = pd.read_sql(allResCodes, conn)
            print('Writing all residential code audit counts')
            allResCodeCount.to_csv('allResCodeCount.csv', index=False)
        else:
            print('Residential code audits counts already written')
        
        if not 'allProperties.csv' in writtenFiles:
            allProperties = pd.read_sql(allProperties, conn)
            print('Writing full property dataset')
            allProperties.to_csv('allProperties.csv', index=False)
        else:
            print('Full property data set already written')
            
        if not 'allAddresses.csv' in writtenFiles:
            # TODO - progress bar for this because it's a big output ~2.25GB
            allAddresses = pd.read_sql(allAddresses, conn)
            print('Writing all address data set')
            allAddresses.to_csv('allAddresses.csv', index=False)
        else:
            print('All address data set already written')
            
        if not 'uprnLookup.csv' in writtenFiles:
            uprnLookup = pd.read_sql(uprnLookup, conn)
            print('Writing UPRN lookup data set')
            uprnLookup.to_csv('uprnLookup.csv', index=False)
        else:
            print('UPRN Lookup data set already written')
        
        print('Core property data created')

    else:
        print('SQL imports set to off')
        if not 'allProperties.csv' in writtenFiles:
            ValueError('No data in project file')
            
    # TODO: Being a bit naughty not explicitly turning 
    # the SQL connection off again

    return(print('Property database built'))

def GeoEnable(ABPFile, XYcols = ['X_COORDINATE', 'Y_COORDINATE']): 
    # Take an addressbase premium file and write coords as WKT
    
    # Function to take the X and Y coordinates in ABP data and turn them 
    # into OS1936 points for spatial work
    # Great to have - no longer needed for this as we have the ONS lookup - 
    # may move to utils
    
    ABPFile['Coordinates'] = list(zip(ABPFile[XYcols[0]], ABPFile[XYcols[1]]))
    # if successful? don't want it doing this otherwise
    ABPFile['Coordinates'] = ABPFile['Coordinates'].apply(Point)
    ABPFile = gpd.GeoDataFrame(ABPFile, geometry='Coordinates')
    ABPFile.drop(['X_COORDINATE', 'Y_COORDINATE'], axis=1)
    ABPFile.crs = nf.osgbCrs
    
    return(ABPFile)

def SubsetBuild(ABPFile, subsetShape):

    # Function to subset a zonal dataframe by a given shapefile
    # gpd.sjoin is super inefficient and this may need rewriting to go by line
    # This could also just live in utils

    subsetShape = gpd.read_file(subsetShape)
    ABPFile = GeoEnable(ABPFile)
    # TODO - build a way to make this work using polygon exclusion - 
    # may work already, but check.
    subsetABP = gpd.sjoin(ABPFile, subsetShape)
    return(subsetABP)

def BuildZoneCorrespondence(ABPFile, writeOut = False, subsetShape = None, \
                            zoningShpName=defaultZoneName):
    # Function to take classified property data and apply a spatial category
    # Reads classified property data direct from the current iteration folder
    # Takes a subset shape on which it applies the subset build function

    # TODO: Limit zoning shp name to OA,LSOA&MSOA only

    # Process uprns
    uprnLookup = pd.read_csv('uprnLookup.csv')
    uprnDict = {'OA':'oa11', 'LSOA':'lsoa11', 'MSOA':'msoa11'}
    lookupCol = uprnDict[zoningShpName]
    uprnLookup = uprnLookup.reindex(['uprn', lookupCol], axis=1)
    
    # Join on uprns
    ABPFile = ABPFile.merge(uprnLookup, how='left', left_on=['UPRN'], right_on=['uprn'])
    ABPFile = ABPFile.rename(columns={lookupCol:'ZoneID'}).drop('uprn', axis=1)
    
    # Audit uprns
    ABPFileAudit = ABPFile[ABPFile.loc[:,'ZoneID'].isnull()]
    audit = ClassificationCount(ABPFileAudit)
    print(audit[0])
    print(audit[1])
    
    if subsetShape is not None:
        ABPFile = SubsetBuild(ABPFile, subsetShape)
        
    print('Zone correspondence built')
    
    return(ABPFile)

# Audit functions

def UprnAudit(defaultZoneName):
    # Check the MSOA zoning system we're all in on matches across types
    # Needed because of everyone using different scottish Geographies
    # TODO: Get this to work with any zoning system in the UPRN lookup
           
   #doing both so it can be used later
        uprnLookup = pd.read_csv('uprnLookup.csv')
        unqUprnLsoa = uprnLookup.loc[:,'lsoa11'].drop_duplicates().reset_index()
      
        lsoaCols = ['objectid', 'lsoa11cd']
        ukLSOA = gpd.read_file(lsoaRef)
        ukLSOA = ukLSOA.loc[:,lsoaCols]
        auditSet = ukLSOA.merge(unqUprnLsoa, how='outer', left_on='lsoa11cd', right_on='lsoa11')

        if len(unqUprnLsoa) == len(ukLSOA) & len(unqUprnLsoa) == len(auditSet):
                return(print('Uprn lookup and spatial index match'))
        else:
                return(print('Uprn lookup and spatial index zones don\'t match, \
                     check ONS UPRN lookup and match zones to that'))
        
        uprnLookup = pd.read_csv('uprnLookup.csv')  
        unqUprnMsoa = uprnLookup.loc[:,'msoa11'].drop_duplicates().reset_index()

        msoaCols = ['objectid', 'msoa11cd']
        ukMSOA = gpd.read_file(msoaRef)
        ukMSOA = ukMSOA.loc[:,msoaCols]
        auditSet = ukMSOA.merge(unqUprnMsoa, how='outer', left_on='msoa11cd', right_on='msoa11')

        if len(unqUprnMsoa) == len(ukMSOA) & len(unqUprnMsoa) == len(auditSet):
               return(print('Uprn lookup and spatial index match'))
        else:       return(print('Uprn lookup and spatial index zones don\'t match, \
                                check ONS UPRN lookup and match zones to that'))
      
def ClassificationCount(cRD):

    # Standard classification audit report
    # Takes classified residential data and counts the classifications

    def ResClassCaseWhen(row):
        if row['census_property_type'] == 0:
            return 'ignored'
        elif row['census_property_type'] >= 1 \
        and row['census_property_type'] <= 9:
            return 'classified'
        elif row['census_property_type'] == 99:
            return 'more work required'
        else:
            return 'other'
    
    reportCols = ['CLASSIFICATION_CODE', 'census_property_type', 'UPRN']
    # classifiedResidentialData
    compCatCount = cRD.groupby(
            ['CLASSIFICATION_CODE', 'census_property_type'], as_index=False).count().reindex(reportCols,axis=1)
    #compCatCount = compCatCount.iloc[:, 0:3]
    
    allResCount = pd.read_csv('allResCodeCount.csv')
    compCatCount = allResCount.merge(compCatCount,
                                     how='left',
                                     left_on='CLASSIFICATION_CODE',
                                     right_on='CLASSIFICATION_CODE')
    
    compCatCount.rename(columns={'UPRN':'n'}, inplace=True)

    compCatCount['pc'] = \
        compCatCount.loc[:,'n']/np.nansum(compCatCount.loc[:,'n'])
        
    compCatCount['classification_status'] = \
        compCatCount.apply(ResClassCaseWhen, axis=1)
    
    print(compCatCount.n.sum(), 'properties')
    
    abpTypeAudit = compCatCount.groupby(['CLASSIFICATION_CODE', 'census_property_type']).sum().drop(columns = 'pc').reset_index()
    censusTypeAudit = compCatCount.groupby('classification_status').sum().drop(columns = 'census_property_type').reset_index()
    
    return(abpTypeAudit, censusTypeAudit)
    
def ZonalPropertyCount(RD, 
                       groupingCol=None,
                       targetLen=CountListShp(shp=defaultZoningPath)[0],
                       targetZones=CountListShp(shp=defaultZoningPath)[1],
                       writeOut=False,
                       reportName=''):
    
    # Another standard report but outputs at zonal grouping
    # Uses the ZoneID cols by default
    # Takes grouping variable as a column name
    # TODO: Make properly zone agnostic - this will probably break with LSOA
    
    if groupingCol is None:
        pByZone = RD.groupby(['ZoneID']).count().reindex(['UPRN'],axis=1).reset_index()
    else:
        pByZone = RD.groupby(['ZoneID', groupingCol]).count().reindex(['UPRN',groupingCol],axis=1)
    
    print('Most properties by Zone:')
    print(pByZone.UPRN.nlargest(n=10))
    print('Least properties by Zone:')
    print(pByZone.UPRN.nsmallest(n=10))
    print('Total Mainland GB properties')
    print(pByZone.UPRN.sum(), ', ', round(pByZone.UPRN.sum()/1000000,2), 'M')
    print('ONS reports 27.4M in 2017')
    
    zoneCount = len(pByZone)
    
    if zoneCount < targetLen:
        print('Some zones missing, writing missing zones to audit folder')
        mz = targetZones[~targetZones.isin(pByZone.ZoneID)]
        mz = mz.to_frame()
        # Placeholder here for vis (can't filter on nothing)
        mz['exists'] = 'No'
        mz.to_csv('Land Use Audits/' + reportName + '_missing_zones.csv', \
                  index=False)
    else:
        print('All zones accounted for')
    
    if writeOut:
        pByZone.to_csv('Land Use Audits/' + reportName + '_property_by_zone.csv', index=True)
    
    return(pByZone)

def LandUseAudits(zonalResProperty,
                  auditDatImportPath = "Y:/NorMITs Land Use/import/ONS Audits/2011_household_type_audit_format_msoa.csv",
                  writeOut=False, 
                  reportName='', level = 'MSOA'):
    if level == 'MSOA':
    # Function to count classified zonal property outputs and compare to 
    # and audit dataset
    # TODO: ATM it's just eitehr LSOA or MSOA
    # Need a loop to convert the property types from the audit into other
    # zoning systems as required.

    # TODO: Have this error handle if the lookup doesn't work as it's non-crucial
     
        landUseAudit = pd.read_csv(auditDatImportPath)
        landUseAudit = landUseAudit.rename(columns={'property_count': 'msoa_property_count'})
    
        zonalResProperty = zonalResProperty.reindex(['ZoneID', \
                            'census_property_type', 'UPRN'], axis=1)
    
        zoneResPropertyCount = zonalResProperty.groupby(['ZoneID', \
                            'census_property_type'], as_index=False).count()
    
        zoneResPropertyCount = zoneResPropertyCount.rename(columns={'UPRN': 'gbpd_property_count'})
                
        zoneResPropertyCount = zoneResPropertyCount.merge(landUseAudit, \
               how='inner', left_on=['ZoneID', 'census_property_type'], \
                right_on=['MSOA_code','property_type']).reset_index(drop=True)
    
        zoneResPropertyCount['difference'] = zoneResPropertyCount['gbpd_property_count'] - zoneResPropertyCount['msoa_property_count']
    
        unqPropertyTypes = zoneResPropertyCount['census_property_type'].unique()
    
        summary = zoneResPropertyCount.groupby(['census_property_type']).sum()
        summary['perc_diff'] = summary['difference']/summary['msoa_property_count']
        summary.to_csv('msoa_Property_Type_Summary.csv')
    
        for pt in unqPropertyTypes: 
            temp = zoneResPropertyCount[zoneResPropertyCount['census_property_type'] == pt]
            temp.to_csv('census_property_type_' + repr(pt) + '_audit.csv') 
        del(temp)
    
        if writeOut:
                zoneResPropertyCount.to_csv('Land Use Audits/' + reportName + \
                                '_msoa_ons_comparison.csv', index=False)
    # Import and check against MSOA 2011 counts by property type
    # Will need to call ZonalCount function with an MSOA breakdown 
    # for England and Wales only.
        return(zoneResPropertyCount)
        
    if level == 'LSOA':
        auditDatImportPath = "Y:/NorMITs Land Use/import/ONS Audits/2011_household_type_audit_format_lsoa.csv"
        landUseAudit = pd.read_csv(auditDatImportPath)
        landUseAudit = landUseAudit.rename(columns={'property_count': level+'_property_count'})
    
        zonalResProperty = zonalResProperty.reindex(['ZoneID', \
                            'census_property_type', 'UPRN'], axis=1)
    
        zoneResPropertyCount = zonalResProperty.groupby(['ZoneID', \
                            'census_property_type'], as_index=False).count()
    
        zoneResPropertyCount = zoneResPropertyCount.rename(columns={'UPRN': 'gbpd_property_count'})
                
        zoneResPropertyCount = zoneResPropertyCount.merge(landUseAudit, \
               how='inner', left_on=['ZoneID', 'census_property_type'], \
                right_on=['LSOA_code','property_type']).reset_index(drop=True)
    
        zoneResPropertyCount['difference'] = zoneResPropertyCount['gbpd_property_count'] - zoneResPropertyCount['lsoa_property_count']
    
        unqPropertyTypes = zoneResPropertyCount['census_property_type'].unique()
    
        summary = zoneResPropertyCount.groupby(['census_property_type']).sum()
        summary['perc_diff'] = summary['difference']/summary['lsoa_property_count']
        summary.to_csv('lsoa_Property_Type_Summary.csv')
    
        for pt in unqPropertyTypes: 
            temp = zoneResPropertyCount[zoneResPropertyCount['census_property_type'] == pt]
            temp.to_csv('census_property_type_' + repr(pt) + '_audit.csv') 
        del(temp)
    
        if writeOut:
                zoneResPropertyCount.to_csv('Land Use Audits/' + reportName + \
                                '_lsoa_ons_comparison.csv', index=False)
    # Import and check against MSOA 2011 counts by property type
    # Will need to call ZonalCount function with an MSOA breakdown 
    # for England and Wales only.
        return(zoneResPropertyCount)
   
def PathConfig(datPath):
    
    dat = datPath
    files = os.listdir(dat)
     
    return(dat, files)
    
def FilledProperties():
    #this is a rough account for unoccupied properties
    #using KS401UK LSOA level to infer whether the properties have any occupants
   
    KS401 = pd.read_csv('Y:/NorMITs Land Use/import/Nomis Census 2011 Head & Household/KS401UK_LSOA.csv')
    
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
    zoneTranslationPath = 'Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa.csv'
    zoneTranslation = pd.read_csv(zoneTranslationPath)
    zoneTranslation = zoneTranslation.rename(columns={'lsoa_zone_id':'lsoaZoneID',
                                                      'msoa_zone_id':'msoaZoneID'})

    zoneTranslation = zoneTranslation.loc[:,['lsoaZoneID', 'msoaZoneID']]
    KS401permhops = KS401permhops.rename(columns={'geography_code':'lsoaZoneID'})
    FilledProperties = KS401permhops.merge(zoneTranslation, on = 'lsoaZoneID')
    FilledProperties=FilledProperties.drop(columns={'lsoaZoneID'}).groupby(['msoaZoneID']).mean().reset_index()

    FilledProperties.to_csv('ProbabilityDwellfilled.csv')
    return(FilledProperties)
    # KS401permhops['geography code'].drop_duplicates()
    # check whether the same thing exists for Scotland?
    #SQS402permhops = SQS401import.iloc[:[3,4]]

def LSOACensusDataPrep(path, EWQS401, SQS401, EWQS402, SQS402, geography = lsoaRef):
    # 401 = people 402 = properties
    # Takes LSOA shapefile from 'Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
    # However, join is on code so if you have different IDs, just change the path and it'll be fine.
    # TODO: need to add percentage filled by msoa to account for seasonal households
    # FilledProperties()
    geography = gpd.read_file(lsoaRef)
    geography = geography.iloc[:, 0:3]
        
    EWQS401import = pd.read_csv(path + '/' + EWQS401)
    EWQS401numbers = EWQS401import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(EWQS401import)
    EWQS401numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']

    ## Something
    SQS401import = pd.read_csv(path + '/' + SQS401)
    SQS401numbers = SQS401import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(SQS401import)
    SQS401numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
    
    UKQS401 = pd.concat([EWQS401numbers, SQS401numbers]).copy()
    del(EWQS401numbers, SQS401numbers)
    UKQS401 = pd.wide_to_long(UKQS401, stubnames='cpt', i='geography_code', j='census_property_type').reset_index().rename(columns={"cpt": "population"})
    
    EWQS402import = pd.read_csv(path + '/' + EWQS402)
    EWQS402numbers = EWQS402import.iloc[:,[2,6,7,8,10,11,12,13]]
    del(EWQS402import)
    EWQS402numbers.columns = ['geography_code', 'cpt1', 'cpt2', 'cpt3', 'cpt4', 'cpt5', 'cpt6', 'cpt7']
    
    SQS402import = pd.read_csv(path + '/' + SQS402)
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
    # Take some census property type data and return hops totals
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

def ZoneUp(cptData, hlsaName = 'MSOA', zoneTranslationPath = 'Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa.csv', 
           groupingCol='msoaZoneID'):
    # Function to raise up a level of spatial aggregation & aggregate at that level, then bring new factors back down
    # TODO: Might be nice to have this zone up any level of zonal aggregation
    # Raise LSOA to MSOA for spatial aggregation
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

def BalanceMissingHOPs(cptData, groupingCol='msoaZoneID', hlsaName = 'MSOA', zoneTranslationPa0th = 'Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa.csv'):
    
    # TODO: Replace global with LAD or Country - likely to be marginal improvements
    
    msoaAgg = ZoneUp(cptData, hlsaName = 'MSOA', zoneTranslationPath = 'Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa.csv', groupingCol=groupingCol)
    msoaAgg = msoaAgg.loc[:,[groupingCol, 'census_property_type', 'household_occupancy']].rename(columns={'household_occupancy': 'msoa_ho'})

    globalAgg = ZoneUp(cptData, hlsaName = 'MSOA', zoneTranslationPath = 'Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa.csv', groupingCol=groupingCol)
    globalAgg = AggregateCpt(globalAgg, groupingCol=None)
    globalAgg = globalAgg.loc[:,['census_property_type', 'household_occupancy']].rename(columns={'household_occupancy': 'global_ho'})

    cptData = msoaAgg.merge(globalAgg, how='left', on='census_property_type')

    print('Resolving ambiguous household occupancies')
    
    def FinalHoCaseWhen(row):
        if np.isnan(row['msoa_ho']):
            return(row['global_ho'])
        else:
            return(row['msoa_ho'])
    
    def HoTypeCaseWhen(row):
        if np.isnan(row['msoa_ho']):
            return('global')
        else:
            return('msoa')
            
    cptData['final_ho'] = cptData.apply(FinalHoCaseWhen, axis=1)
    cptData['ho_type'] = cptData.apply(HoTypeCaseWhen, axis=1)
    cptData = cptData.drop(['msoa_ho', 'global_ho'], axis=1).rename(columns={'final_ho':'household_occupancy'})
    
    return(cptData)

def CreateNtemAreas(bsqImportPath='Y:/NorMITs Land Use/import/Bespoke Census Query/formatted_long_bsq.csv',
                    areaTypeImportPath = 'Y:/NorMITs Land Use/import/CTripEnd/ntem_zone_area_type.csv'):
# Import Bespoke Census Query - already transformed to long format in R
    print('Importing bespoke census query')
    bsq = pd.read_csv(bsqImportPath)
# Import area types
    areaTypes = pd.read_csv(areaTypeImportPath)

# Shapes
    mlaShp = gpd.read_file(mladRef).reindex(['objectid','cmlad11cd'],axis=1)
    msoaShp = gpd.read_file(msoaRef).reindex(['objectid','msoa11cd'],axis=1)
# Lookups
# Bespoke census query types
    pType = pd.read_csv('Y:/NorMITs Land Use/import/Bespoke Census Query/bsq_ptypemap.csv')
    hType = pd.read_csv('Y:/NorMITs Land Use/import/Bespoke Census Query/bsq_htypemap.csv')
# Zonal conversions
    mlaLookup = pd.read_csv('Y:/NorMITs Synthesiser/Zone Translation/Export/merged_la_to_msoa/merged_la_to_msoa.csv').reindex(['msoaZoneID', 'merged_laZoneID'],axis=1)
    ntemToMsoa = pd.read_csv('Y:/NorMITs Synthesiser/Zone Translation/Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv').reindex(['ntemZoneID', 'msoaZoneID', 'overlap_ntem_pop_split_factor'],axis=1)
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
    def CountMsoa(bsq):
        unqMSOA = bsq.reindex(['msoaZoneID'],axis=1).drop_duplicates()
        return(len(unqMSOA))

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
    areaTypesMsoa.to_csv('areaTypesMSOA.csv')
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
    
def Myeadjustment(
        mye_females = pd.read_csv('Y:/NorMITs Land Use/import/Population estimates/2018_MidyearMSOA/MYEfemales_2018.csv'),
        mye_males = pd.read_csv('Y:/NorMITs Land Use/import/Population estimates/2018_MidyearMSOA/MYEmales_2018.csv'),
        comm_sum = pd.read_csv('Y:/NorMITs Land Use/iter2_MSOA/Communal Establishments/communal_msoaoutput.csv'),
        landuseoutput = pd.read_csv('C:/NorMITs_Export/' + defaultIter +'/landUseOutputMSOA.csv'),
        hops2011 = pd.read_csv('C:/NorMITs_Export/' + defaultIter +'/UKHouseHoldOccupancy2011.csv'),
        Scot_females = pd.read_csv('Y:/NorMITs Land Use/import/Population estimates/2018_MidyearMSOA/Females_Scotland_2018.csv'),
        Scot_males = pd.read_csv('Y:/NorMITs Land Use/import/Population estimates/2018_MidyearMSOA/Males_Scotland_2018.csv')
        ):
    # getting mye into the right format - 'melt' to get columns as rows, then rename them
    mye_2018 = mye_males.append(mye_females)
    mye_2018 = mye_2018.rename(columns = {'Area Codes':'msoacd'})
    mye_2018 = pd.melt(mye_2018, id_vars = ['msoacd','gender'], value_vars = 
                        ['under_16', '16-74', '75 or over'])
    mye_2018 = mye_2018.rename(columns= {'variable':'Age', 'value':'2018pop'})
    mye_2018 = mye_2018.replace({'Age':{'under_16': 'under 16'}})
   
    # MYE minus communal establishments; those are already formatted to include
    # the right split of gender and age over zone
    comm_sum = comm_sum.rename(columns={'msoa11cd':'msoacd'})
    comm_sum = comm_sum.drop(columns = {'All', 'Med', 'Def', 'Edu', 'Oth', 
                                        'Pri', 'Unnamed: 0', 'ladcd'})
    mye_adjust = mye_2018.merge(comm_sum, on = ['msoacd', 'gender', 'Age'])

    mye_adjust['mye_adj_pop'] = mye_adjust['2018pop'].values - mye_adjust['Total_people'].values
    
    # mye_adjust.to_csv('C:/NorMITs_Export/myeadjust.csv')
    # mye_adjust2 = pd.read_csv('C:/NorMITs_Export/myeadjust.csv')
    # children are a 'gender' in NTEM, then need to sum the two rows (as previously
    # included the split between female and male)
    mye_adjust.loc[mye_adjust['Age'] == 'under 16', 'gender'] = 'Children'
    mye_adjust = mye_adjust.rename(columns = {'gender':'Gender'})
    mye_adjust = mye_adjust.replace({'Gender':{ 'male':'Male', 
                                         'female':'Females'}})
    print('ONS population MYE 2018 for E+W is:', 
          mye_adjust['mye_adj_pop'].sum())

    mye_adjust['myeadj'] = mye_adjust.groupby(['msoacd', 'Age', 'Gender'])['mye_adj_pop'].transform('sum')
    mye_adjust = mye_adjust.drop(columns={'mye_adj_pop', 'Total_people', '2018pop'})
    mye_adjust = mye_adjust.drop_duplicates()
    mye_adj = mye_adjust[['msoacd', 'Age', 'Gender', 'myeadj']]
    print(mye_adj['myeadj'].sum())
    # mye_adj.to_csv('C:/NorMITs_Export/mye2018adjusted.csv')
    
    landuseoutput = landuseoutput.drop(columns = {'Unnamed: 0'})
    # summarise land use output
    pop_pc_totals = landuseoutput.groupby(
            by = ['ZoneID', 'Age', 'Gender'],
            as_index=False
            ).sum().drop(columns ={ 'area_type', 
                 'census_property_type', 'household_composition', 'property_type', 
                 'properties'})
    
    mye_adj = mye_adj.rename(columns={'msoacd':'ZoneID'})
    myepops = pop_pc_totals.merge(mye_adj, on = ['ZoneID', 'Gender', 'Age'])
    
    myepops['pop_factor'] = myepops['myeadj']/myepops['people']
    myepops = myepops.drop(columns={'people'})
    popadj = landuseoutput.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
    popadj['newpop'] = popadj['people']*popadj['pop_factor']
    print('The adjusted 2018 population for England and Wales is', 
          popadj['newpop'].sum()/1000000, 'M')
       
    """
    need to retain the missing MSOAs for both population landuse outputs and 
    HOPs
    """
    popadj = popadj.drop(columns = {'myeadj', 'pop_factor','people'})
    popadj = popadj.rename(columns={'newpop':'people'})
    adjMSOA2018 = popadj['ZoneID'].drop_duplicates()
    # sort out Scotland as it only estimates half of the population
    restofUK = landuseoutput[~landuseoutput.ZoneID.isin(adjMSOA2018)]
    restofUK['ZoneID'].drop_duplicates()
    """
    decided to sort out Scotland as the totals didn't add up
    """
    # this has the translation from LAD to MSOAs
    LadTranslation = pd.read_csv('Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv')
    # Use ukLAD for the lookup on objetid and lad17cd
    # take tables for females and males,merge and sort out the format
    Scot_mye = Scot_males.append(Scot_females)
    Scot_mye = Scot_mye.rename(columns = {'Area code':'lad17cd'})
    Scot_mye = pd.melt(Scot_mye, id_vars = ['lad17cd','Gender'], value_vars = 
                        ['under 16', '16-74', '75 or over'])
    Scot_mye = Scot_mye.rename(columns= {'variable':'Age', 'value':'2018pop'})
    Scot_mye.loc[Scot_mye['Age'] == 'under 16', 'Gender'] = 'Children'
    Scot_mye['2018pop'].sum()
    
    ScotLad = Scot_mye.merge(ukLAD, on = 'lad17cd')
    ScotLad = ScotLad.rename(columns={'objectid':'ladZoneID'})
    
    LadTranslation = LadTranslation.rename(columns={'lad_zone_id':'ladZoneID'})
    ScotMSOA = ScotLad.merge(LadTranslation, on = 'ladZoneID')
    # final stage of the translation from LAD to MSOA for Scotland
    ScotMSOA['people2018'] = ScotMSOA['2018pop']*ScotMSOA['lad_to_msoa']
    ScotMSOA = ScotMSOA.drop(columns={'overlap_type', 'lad_to_msoa', 'msoa_to_lad',
                                     '2018pop', 'lad17cd', 'ladZoneID'})
    ScotMSOA['ZoneID'].drop_duplicates()
    # MYE 2018 people in MSOA    
    restofUK2  = restofUK.groupby(
            by=['ZoneID', 'Age', 'Gender'],
            as_index=False
            ).sum().drop(columns = {'area_type', 
                 'census_property_type', 'household_composition', 'property_type', 
                 'properties'})
    ScotMSOA = ScotMSOA.rename(columns={'msoa_zone_id':'ZoneID'})
    Scot = restofUK2.merge(ScotMSOA, how = 'outer', on = ['ZoneID', 'Gender', 'Age'])
    Scot['pop_factor'] = Scot['people2018']/Scot['people']
    #Scot = landuseoutput.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
    Scot['newpop'] = Scot['people']*Scot['pop_factor']
    Scot = Scot.drop(columns={'people'})
    Scot_Adj = restofUK.merge(Scot, on =['ZoneID','Gender', 'Age'])
    Scot_Adj['newpop'] = Scot_Adj['people']*Scot_Adj['pop_factor']
    Scot_Adj = Scot_Adj.drop(columns={'people', 'people2018', 'pop_factor'})
    Scot_Adj = Scot_Adj.rename(columns= {'newpop':'people'})
    print('The adjusted 2018 population for Scotland is', 
         Scot_Adj['newpop'].sum()/1000000, 'M')
    Scot_EW = popadj.append(Scot_Adj)
    check = Scot_EW['ZoneID'].drop_duplicates()
    otherrestofUK = landuseoutput[~landuseoutput.ZoneID.isin(check)]
    fullUK2018adjustment = Scot_EW.append(otherrestofUK)
    
    print('Full population for 2018 is now =', 
          fullUK2018adjustment['people'].sum())
    print('check all MSOAs are present, should be 8480:', 
          fullUK2018adjustment['ZoneID'].drop_duplicates().count())
    fullUK2018adjustment.to_csv('landUseOutputMSOA_2018.csv')
    #msoaAudit1 = popadj.reindex(['ZoneID', 'newpop'], axis=1).groupby('ZoneID').sum()
    #soaAudit.to_csv('C:/NorMITs_Export/oldpop.csv')
    #msoaAudit1.to_csv('C:/NorMITs_Export/newpop.csv')
    #testsegment
    # Adjusting the HOPs 
    g1 = fullUK2018adjustment.groupby(
        by = ['ZoneID', 'census_property_type', 'properties'],
        as_index = False
        ).sum()
    g1 = g1.drop(columns={'property_type', 'area_type', 
              'household_composition' })
   
    #need to change the MSOA from objectid to zone
    msoaShp = gpd.read_file(msoaRef).reindex(['objectid','msoa11cd'],axis=1)
    hops2011 = hops2011.rename(columns={'msoaZoneID':'objectid'})
    msoaShp = msoaShp.rename(columns={
                'msoaZoneID':'ZoneID'})
    hops2011= hops2011.rename(columns={'objectid':'ZoneID'})
    hops2011 = hops2011.merge(msoaShp, on = 'ZoneID')
    hops2011 = hops2011.rename(columns={'msoa11cd':'ZoneID'})

    hopsadj = hops2011.merge(g1, on = ['ZoneID', 'census_property_type'])
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
    
def CreateEmploymentSegmentation(bsq, ksEmpImportPath='Y:/NorMITs Land Use/import/KS601-3UK/uk_msoa_ks601equ_w_gender.csv'):
# Synthesise in employment segmentation using 2011 data
# TODO: Growth 2011 employment segments
# TODO employment category should probably be conscious of property type
# Get to segments:
# full time employment
# part time employment
# students
# not employed/students

# Shapes
    msoaShp = gpd.read_file(msoaRef).reindex(['objectid','msoa11cd'],axis=1)
    
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

# Function to combine working age population factors to create NTEM employment segements
    def AggWapFactor(ksSub, newSeg):
        ksSub = ksSub.groupby(['msoaZoneID','Gender']).sum().reset_index()
        ksSub['employment_type']=newSeg
        return(ksSub)
    
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
    
def CreateNtemSegmentation(bsqImportPath='Y:/NorMITs Land Use/import/Bespoke Census Query/formatted_long_bsq.csv',
                    areaTypeImportPath = 'Y:/NorMITs Land Use/import/CTripEnd/ntem_zone_area_type.csv',
                    ksEmpImportPath='Y:/NorMITs Land Use/import/KS601-3UK/uk_msoa_ks601equ_w_gender.csv'):
    bsq = CreateNtemAreas(bsqImportPath)
    bsq = CreateEmploymentSegmentation(bsq)
    bsq.to_csv('bsq.csv')
    return(bsq)

###
## Core functions

def PropertyTypeAnalysis(subset = None, zonalAggregation=defaultZoneName, \
                         writeSteps=False): 
    # Params = read from flat or database, another subset builder by shapefile
    # Function to assign property types to the contents of the ABP data.
    # This is based on ABP classifications to census property type maps first 
    # and then logic from case when statements.
    
    # Define case when handler for classification
    def ResClassCaseWhen(row):
        if row['census_property_type'] == 0:
            return 'ignored'
        elif row['census_property_type'] >= 1 and \
            row['census_property_type'] <= 9:
            return 'classified'
        elif row['census_property_type'] == 99:
            return 'more work required'
        else:
            return 'other'
    
    print('Importing datasets from gbpd build')
    censusPropertyTypes = pd.read_excel('Y:/NorMITs Land Use/import/Census_Property_Type_Maps.xlsx', sheet_name=0)
    
    # allCodeCount = pd.read_csv('allCodeCount.csv')
    # allResCount = pd.read_csv('allResCodeCount.csv')
    allProperty = pd.read_csv('allProperties.csv')
    allAddresses = pd.read_csv('allAddresses.csv')
    
    # This is absolute misery to audit & takes years to run
    # TODO: Move into seperate function and import again here
    # before classification starts
    # Because it's really useful & could use these cold code counts for 
    # other things towards applying ML algo
    
    print('Merging commercial counts')

    # Create count of organisation name by point
    orgCols = ['UPRN', 'ORGANISATION_NAME']
    propCols = ['UPRN', 'X_COORDINATE','Y_COORDINATE']
    orgNameCount = allProperty.reindex(propCols,axis=1).merge(allAddresses.reindex(orgCols,axis=1), 
                                       how='inner', on='UPRN').drop('UPRN',axis=1).groupby(['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    orgNameCount = orgNameCount.rename(columns={'ORGANISATION_NAME':'org_count'})

    # Create count of commercial properties per coordinate
    print('Creating commercial property counts')
    allCommercialProperty = allProperty[allProperty.loc[:,'CLASSIFICATION_CODE'].str.startswith('C')]
    allCommercialPropertyCount = allCommercialProperty.groupby(
                        ['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    allCommercialPropertyCount = allCommercialPropertyCount.reindex(
                        ['X_COORDINATE', 'Y_COORDINATE', 'UPRN'], axis=1)
    allCommercialPropertyCount = allCommercialPropertyCount.rename(
                         columns={'UPRN':'commercial_property_count'})

    # Create count of properties per coordinate
    allResProperty = allProperty[allProperty.loc[:,'CLASSIFICATION_CODE'].str.startswith('R')]
    allResPropertyCount = allResProperty.groupby(\
                        ['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    allResPropertyCount = allResPropertyCount.reindex(\
                        ['X_COORDINATE', 'Y_COORDINATE', 'UPRN'], axis=1)
    allResPropertyCount = allResPropertyCount.rename(columns={'UPRN':'property_count'})
    
    # TODO: Establish some other useful bits here
    # TODO: Nearby property type counts in a bounding box - c
    # ircle is too hard & resource intensive

    print('Remerging property data')
    # Merge back into the property data
    allResProperty = allResProperty.merge(orgNameCount, how='left', \
                            on=['X_COORDINATE', 'Y_COORDINATE'])
    allResProperty = allResProperty.merge(allCommercialPropertyCount, \
                            how='left', on=['X_COORDINATE', 'Y_COORDINATE'])
    allResProperty = allResProperty.merge(allResPropertyCount, \
                            how='left', on=['X_COORDINATE', 'Y_COORDINATE'])
    # Join the classification codes on
    allResProperty = allResProperty.merge(censusPropertyTypes, \
                            how='left', left_on='CLASSIFICATION_CODE', \
                            right_on='abp_code')
    
    del(allCommercialProperty, allProperty, orgNameCount, \
        allCommercialPropertyCount, allResPropertyCount)
    gc.collect()

    # Do classification counts and write out to csv
    audit = ClassificationCount(allResProperty)
    print('Pre Classification Counts:')
    print(audit[0])
    print(audit[1])
    
    nup.CreateFolder('Land Use Audits')
    audit[0].to_csv('Land Use Audits/pre_classification_land_use.csv')
    audit[1].to_csv('Land Use Audits/classification_os.csv')
    #zonalPRoperties = BuildCorrespondence(allResProperty)
    
    # Join zone codes on at this point to allow for spatial groupings 
    # in classification algo
    # TODO: This should take any of the 3 zoning systems in the UPRN 
    # lookup as described in the function parameters
    #do we need to filter out the parent/child UPRNs, this would make a diff for flats
    
    # Remove non-existing property types:
    # 1 under construction, 2 In use, 3 Unoccupied / vacant / derelict, 
    # 4 Demolished, 6 Planning permission granted
    # technical spec: 
    # https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/addressbase-premium-technical-specification.pdf
    
    print('Removing demolished or properties')
    noneProperties=[3,4,6]
    allResProperty = allResProperty[~allResProperty.BLPU_STATE.isin(noneProperties)]

    #LOGICAL_STATUS shows whether the address is provisional, live or historical
    #8 is historical, 6 is provisional, 3 - alternative, 1 is approved 
    print('Removing all historical/alternative and provisional properties')
    historicalprops=[3,6,8]
    allResProperty = allResProperty[~allResProperty.LOGICAL_STATUS.isin(historicalprops)]

    #let's get rid of non-postal addresses too
    nonpostal = ['N']
    allResProperty = allResProperty[~allResProperty.ADDRESSBASE_POSTAL.isin(nonpostal)]
    
    audit = ClassificationCount(allResProperty)
    print('Active Property Counts:')
    print(audit[0])
    print(audit[1])

    allResProperty = BuildZoneCorrespondence(allResProperty, \
                                             zoningShpName=zonalAggregation)
    ZonalPropertyCount(allResProperty, writeOut=True, reportName='unclassified')
    
    # Import here for debugging - write out for new runs
    if writeSteps:
        allResProperty.to_csv('allResProperty' + zonalAggregation + 'Unclassified.csv',index=False)
    # allResProperty = pd.read_csv('allResProperty' + zonalAggregation
    # + 'Unclassified.csv')

    gc.collect()

    # TODO: Keep improving this application of logic to residential property
    # TODO: Write logic processes for RD08, RI02

    def ApplyClassificationLogic(allResProperty, logic=None):
        # Function to process census types for RD06
        # Main function can be applied to a dataframe, sub function is 
        # a case when for pandas apply method
        def Rd06CaseWhen(row):
            # Case when tree to assign census types based on logic
            
            # RD06 condition loop
            # Code describes Self-Contained Flat Includes Maisonette Apartment
            # - scf
            # Can resolve to census property type 4, 5 or 6
            # 4 = flat in purpose built block of flats
            # 5 = flat in a converted or shared house
            # 6 = in a commercial building

            if row['property_count'] >= 4:
                # if there are more than 4 properties it's purpose built
                return(4)
            elif row['org_count'] > 0:
                # if there's a business there it's a 6             
                return(6)
                # need a loop to identify the 5s ie shared house properties
            else:
                # anything left is propably purpose built - on the balance 
                # of probability   
                return(4)
                
        def RdCaseWhen(row):
            # Case when tree to assign census types based on logic
            # Uses the most common single point properties and multipoint 
            # properties to fill in RD blanks
            # if row['BLPU_STATE'] == 'nan' and row['LAST_UPDATE_DATE'] == 
            # '2016-02-10' and row['type_desc'] == 'New or Demolished':
                # if it's a new or demolished from 2016, exclude it
               # return(99)
            if row['property_count'] == 1:
            # If s just one property it's whatever the 
            # most common single property is 
                return(row['zpsSP'])
            if row['property_count'] > 1:
            # If s just one property it's whatever the 
            # most common multi property is 
                return(row['zpsMP'])
            if row['BUILDING_NAME'] == 'nan' and row['BUILDING_NUMBER'] == 'nan':
                return(99)
            if row['ORGANISATION_NAME'] != 'nan':
                # If there's an organisation name and it's in RD it should 
                # be excluded
                return(99)
            else:
                return(99)

        # Build scf - property classification subset - also useful for audits
        scf = allResProperty[allResProperty.loc[:, 'CLASSIFICATION_CODE'] == logic]        
        scf = scf.merge(allAddresses, how='left', on='UPRN').reset_index(drop=True)
        
        # Redefine cols we need to test for blanks as strings
        scf['ORGANISATION_NAME'] = scf['ORGANISATION_NAME'].astype(str)
        scf['SUB_BUILDING_NAME'] = scf['SUB_BUILDING_NAME'].astype(str)
        scf['BUILDING_NAME'] = scf['BUILDING_NAME'].astype(str)
        scf['BUILDING_NUMBER'] = scf['BUILDING_NUMBER'].astype(str)
        # TODO: Make sure this doesn't break everything
        scf['BLPU_STATE'] = scf['BLPU_STATE'].astype(str)

        # Let's export this to play with the classifications
        # scf.to_csv('scfRD06logic.csv', index=False)
        rejoin = allResProperty[allResProperty.loc[:, 'CLASSIFICATION_CODE'] != logic]
        
        if logic == 'RD06':
            scf['census_property_type'] = scf.apply(Rd06CaseWhen, axis=1)
            scf = scf.drop(['ORGANISATION_NAME', 'DEPARTMENT_NAME', \
                            'SUB_BUILDING_NAME', 'BUILDING_NAME', \
                            'BUILDING_NUMBER'], axis=1)
            allResProperty = pd.concat([scf.reset_index(drop=True), \
                                        rejoin.reset_index(drop=True)], \
                                        axis=0, sort=True)

            # Check nothing has gone
            if sum([len(scf), len(rejoin)]) != len(allResProperty):
                # Error handle
                raise ValueError('Some properties have disappeared in \
                             the process - check case when loops and import')
        elif logic == 'RD':
            # Classify RD properties by zone depending on share of 
            # existing classifications
            zpsCols = ['ZoneID', 'census_property_type', 'UPRN']
            exclTypes = [0,8,99]
            zonalPropertyShare = allResProperty[~allResProperty.census_property_type.isin(exclTypes)].groupby(['ZoneID', 'census_property_type']).count().reset_index().reindex(zpsCols,axis=1)            
            zpsUnqZones = allResProperty['ZoneID'].drop_duplicates().reset_index(drop=True)
            # do max where prop == 1 & max prop > 1  
            spTypes = [1,2,3]
            zpsSingleProp = zonalPropertyShare[zonalPropertyShare.census_property_type.isin(spTypes)].sort_values('UPRN', ascending=False).drop_duplicates(['ZoneID']).drop(['UPRN'],axis=1)
            zpsSingleProp = zpsSingleProp.rename(columns = {'census_property_type':'zpsSP'})
            zpsMultiProp = zonalPropertyShare[~zonalPropertyShare.census_property_type.isin(spTypes)].sort_values('UPRN', ascending=False).drop_duplicates(['ZoneID']).drop(['UPRN'],axis=1)
            zpsMultiProp = zpsMultiProp.rename(columns = {'census_property_type':'zpsMP'})

            zpsSingleUnqZones = zpsSingleProp['ZoneID'].drop_duplicates()
            zpsSingleMissing = zpsUnqZones[~zpsUnqZones.isin(zpsSingleUnqZones)]
            zpsMultiUnqZones = zpsMultiProp['ZoneID'].drop_duplicates()
            zpsMultiMissing = zpsUnqZones[~zpsUnqZones.isin(zpsMultiUnqZones)]
            
            # Low populations default to 1 in single, 6 in multi 
            zpsSingleMissing = zpsSingleMissing.to_frame()
            zpsSingleMissing['zpsSP'] = 1
            zpsMultiMissing = zpsMultiMissing.to_frame()
            zpsMultiMissing['zpsMP'] = 4
            
            zpsSingleProp = pd.concat([zpsSingleProp.reset_index(drop=True), \
                            zpsSingleMissing.reset_index(drop=True)], \
                            axis=0, sort=True)
            # CHECK FOR ALL ZONES
            zpsMultiProp = pd.concat([zpsMultiProp.reset_index(drop=True), \
                            zpsMultiMissing.reset_index(drop=True)], \
                            axis=0, sort=True)
            # CHECK FOR ALL ZONES
            
            scf = scf.merge(zpsSingleProp, how='left', on='ZoneID')
            scf = scf.merge(zpsMultiProp, how='left', on='ZoneID')
            del(zpsSingleProp, zpsMultiProp)
            
            scf['census_property_type'] = scf.apply(RdCaseWhen, axis=1)
            scf = scf.drop(['ORGANISATION_NAME', 'DEPARTMENT_NAME', \
                            'SUB_BUILDING_NAME', 'BUILDING_NAME', \
                            'BUILDING_NUMBER', 'zpsSP', 'zpsMP'], axis=1)
            # Can't remember if this is the best way to bring 
            # matrices back together
            allResProperty = pd.concat([scf.reset_index(drop=True), rejoin.reset_index(drop=True)], axis=0, sort=True)

            # Check nothing has gone
            if sum([len(scf), len(rejoin)]) != len(allResProperty):
                # Error handle
                raise ValueError('Some properties have disappeared in the \
                                 process - check case when loops and import')
        else:
            print('No logic provided')
            
        gc.collect()

        return(allResProperty)
    
    # Apply property logic defined above
    # Needs to be done in this order otherwise werid things will 
    # happen (mapping all multi-occupancy properties to 99 type in RD logic)

    print('Applying RD06 segmentation logic')
    allResProperty = ApplyClassificationLogic(allResProperty, logic='RD06')
    audit = ClassificationCount(allResProperty)
    print('Post RD06 classification cpt:')
    print(audit[0])
    print(audit[1])
    
    if writeSteps:
        allResProperty.to_csv('allResProperty' + zonalAggregation + 'RD06.csv',index=False)
    # allResProperty = pd.read_csv('allResProperty' + zonalAggregation 
    # + 'RD06.csv')

    print('Applying RD segmentation logic')
    allResProperty = ApplyClassificationLogic(allResProperty, logic = 'RD')
    
    audit = ClassificationCount(allResProperty)
    print('Post RD classification cpt:')
    print(audit[0])
    print(audit[1])

    audit[0].to_csv('Land Use Audits/post_classification_land_use.csv')
    audit[1].to_csv('Land Use Audits/classification_nlu.csv')

    ZonalPropertyCount(allResProperty, writeOut=True, reportName='classified_untrimmed')
    
    # Import here for debugging - left write out in for new runs
    if writeSteps:
        allResProperty.to_csv('allResProperty' + zonalAggregation + 'Untrimmed.csv',index=False)
    # allResProperty = pd.read_csv('allResProperty' + zonalAggregation + 
    # 'Untrimmed.csv')
    # Sample - make sure it looks fine
    # arpSample = allResProperty.iloc[0:1000]

    # Trim data frame to remove unclassified properties
    requiredClassifications = [1,2,3,4,5,6,7,8]
    allResProperty = allResProperty[allResProperty.loc[:,'census_property_type'].isin(requiredClassifications)].copy()

    audit = ClassificationCount(allResProperty)
    print(audit[0])
    print(audit[1])
    audit[0].to_csv('Land Use Audits/trimmed_post_classification_land_use.csv')
    audit[1].to_csv('Land Use Audits/trimmed_classification_nlu.csv')

    # Zone audits - also writes out a list - see function above
    ZonalPropertyCount(allResProperty, writeOut=True, reportName='classified')

    # Yeah these gaps look really really weird - let's find out what's going 
    # on in there!
    # Done - that took 3 days and it's 9PM - this is actually quite difficult
    trimCols = ['ZoneID','POSTCODE_LOCATOR','UPRN','census_property_type', \
                'X_COORDINATE', 'Y_COORDINATE']
    allResProperty = allResProperty.reindex(trimCols,axis=1)
    # Write out
    allResProperty.to_csv('allResProperty' + zonalAggregation + 'Classified.csv', index=False)

    gc.collect()
    return(allResProperty)

def ApplyHouseholdOccupancy(doImport=False, writeOut=True, \
        level='MSOA', hopsPath='Y:/NorMITs Land Use/import/HOPs/hops_growth_factors.csv'):
    # Import household occupancy data and apply to property data
    # TODO: toggles for unused 'level' parameter. Want to be able to run
    # at LSOA level when point correspondence is done.
    # TODO: Folders for outputs to seperate this process from the household 
    # classification
    # TODO: should probably cap this at a max occupancy based on ONS stats?
    level = 'MSOA'
    
    if doImport:
        balancedCptData = pd.read_csv('UKHouseHoldOccupancy2011.csv')
       # lsoacpt = pd.read_csv('cptlsoa2011.csv')
    else:
        censusDat = PathConfig('Y:/NorMITs Land Use/import/Nomis Census 2011 Head & Household')
        print(censusDat[1])
        
        # TODO: - This was a patch to get it to work fast and it did. 
        # Make these a bit cleverer to reduce risk of importing the wrong thing
        EWQS401 = 'QS401UK_LSOA.csv'
        SQS401 = 'QS_401UK_DZ_2011.csv'
        EWQS402 = 'QS402UK_LSOA.csv'
        SQS402 = 'QS402UK_DZ_2011.csv'
        # KS401 = 'KS401_UK_LSOA.csv'
        
        #cptData = LSOACensusDataPrep(censusDat[0], EWQS401, SQS401, EWQS402, SQS402, KS40EW, geography = lsoaRef))    
        cptData = LSOACensusDataPrep(censusDat[0], EWQS401, SQS401, EWQS402, SQS402, geography = lsoaRef)
        
        # ZoneUp here to MSOA aggregations
        balancedCptData = BalanceMissingHOPs(cptData, groupingCol='msoaZoneID')
        balancedCptData = balancedCptData.fillna(0)
        FilledProperties = FilledProperties()
        balancedCptData = balancedCptData.merge(FilledProperties, how = 'outer', on = 'msoaZoneID')
        balancedCptData['household_occupancy'] = balancedCptData['household_occupancy']*balancedCptData['Prob_DwellsFilled']
        balancedCptData = balancedCptData.drop(columns={'Prob_DwellsFilled'})
        balancedCptData.to_csv('UKHouseHoldOccupancy2011.csv', index=False)

    msoaCols = ['objectid', 'msoa11cd']
    ladCols = ['ladZoneID','msoaZoneID']
    ukMSOA = gpd.read_file(msoaRef)
    ukMSOA = ukMSOA.loc[:,msoaCols]
    
    # Visual spot checks - count zones, check cpt
    audit = balancedCptData.groupby(['msoaZoneID']).count().reset_index()
    print('census hops zones =', audit['msoaZoneID'].drop_duplicates().count(),'should be', len(ukMSOA))
    print('counts of property type by zone', audit['census_property_type'].drop_duplicates())
    
    # Join MSOA ids to balanced cptdata
    ukMSOA = ukMSOA.rename(columns={'msoa11cd':'msoaZoneID'})
    balancedCptData = balancedCptData.merge(ukMSOA, how='left', \
                        on='msoaZoneID').drop('objectid', axis=1)
    # Import msoa to lad translation
    ladTranslation = pd.read_csv('Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv')
    ladTranslation = ladTranslation.rename(columns={'lad_zone_id':'ladZoneID', 
                                                    'msoa_zone_id':'msoaZoneID'}).loc[:,ladCols]
    unqLad = ladTranslation['ladZoneID'].unique()
   # ladTranslation = ladTranslation.merge(ukMSOA, how='left', \
                  #      left_on='msoaZoneID'.\
                   #     drop('msoaZoneID',axis=1)
    #balancedCptData = balancedCptData.merge(ladTranslation, how='left', \
                    #    on='msoaZoneID').drop('objectid',axis=1)
    balancedCptData = balancedCptData.merge(ladTranslation, how='left', \
                        on='msoaZoneID')

    joinLad = balancedCptData['ladZoneID'].unique()

    ladCols = ['objectid','lad17cd']
    ukLAD = gpd.read_file(ladRef)
    ukLAD = ukLAD.loc[:,ladCols]

    balancedCptData = balancedCptData.merge(ukLAD, how='left', \
                        left_on='ladZoneID', right_on='objectid').drop(['ladZoneID', 'objectid'],axis=1)

    if len(joinLad) == len(unqLad):
        print('All LADs joined properly')
    else:
        print('Some LAD zones not accounted for')
    del(unqLad, joinLad)

    hgCols = ['Area code','11_to_18']
    hopsGrowth = pd.read_csv(hopsPath).loc[:,hgCols]

    balancedCptData = balancedCptData.merge(hopsGrowth, \
                        how='left', left_on='lad17cd', \
                        right_on='Area code').drop('Area code',axis=1).reset_index(drop=True)
    balancedCptData['household_occupancy_18'] = balancedCptData['household_occupancy']*1+balancedCptData['11_to_18']

    trimCols = ['msoaZoneID', 'msoa11cd', 'census_property_type', \
                'household_occupancy_18', 'ho_type']
    balancedCptData = balancedCptData.reindex(trimCols,axis=1)
    #balancedCptData = balancedCptData.drop(columns = {'msoa11cd'})

    # Read in all res property for the level of aggregation
    allResProperty = pd.read_csv('allResProperty' + level + 'Classified.csv')
    
    allResProperty = allResProperty.reindex(['ZoneID','census_property_type',\
                                             'UPRN'],axis=1)
    allResPropertyZonal = allResProperty.groupby(['ZoneID', \
                            'census_property_type']).count().reset_index()
    del(allResProperty)
    
    
    if level == 'MSOA':
        
        allResPropertyZonal = allResPropertyZonal.merge(balancedCptData, \
                            how='inner', \
                            left_on=['ZoneID', 'census_property_type'], \
                            right_on=['msoaZoneID','census_property_type']).\
                            drop('msoaZoneID',axis=1)
    
    # Audit join - ensure all zones accounted for
        if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukMSOA['msoaZoneID'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')
        
       # FilledProperties = pd.read_csv('Y:/NorMITs Land Use/import/HOPs/filledprops.csv')
        
       # allResPropertyZonal.merge(FilledProperties, on = 'ZoneID')
        allResPropertyZonal['population'] = allResPropertyZonal['UPRN'] * allResPropertyZonal['household_occupancy_18'] 
   
    # Create folder for exports
        arpMsoaAudit = allResPropertyZonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'],axis=1)
        hpaFolder = 'Hops Population Audits'
        nup.CreateFolder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        if writeOut:
            allResPropertyZonal.to_csv('classifiedResProperty' + level + '.csv')
    
        return(allResPropertyZonal)

        
    if level == 'LSOA':
        
        
        #here change msoa that is expected in the function above to lsoa Zone ID.
        #To do that we need the zone translations and lsoa table with zoneID objectid and also msoa lookup for the objectid!
        lsoa_lookup = pd.read_csv('Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz_cleaned_dbf.csv')
        LSOAzonetranslation = pd.read_csv("Y:/NorMITs Synthesiser/Zone Translation/Export/msoa_to_lsoa/msoa_to_lsoa_pop_weighted.csv")  
        msoa_lookup = pd.read_csv("Y:/Data Strategy/GIS Shapefiles/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz_dbf.csv")
        
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
      #  if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukLSOA['lsoa11cd'].drop_duplicates().count():
       #     ValueError('Some zones dropped in Hops join')
       # else:
        #    print('All Hops areas accounted for')
                                 
    # TODO: Insert some logic here to make it work for LSOA and OA level#
    # Going to look something like assigning areas their MSOA, 
    # joining on correspondence and dropping thee MSOA again.
        # Audit join - ensure all zones accounted for
        ukLSOA = gpd.read_file(lsoaRef)
        ukLSOA = ukLSOA.loc[:,lsoaCols]
        
        if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukLSOA['lsoa11cd'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')
            
        allResPropertyZonal['population'] = allResPropertyZonal['UPRN'] * allResPropertyZonal['household_occupancy_18']
      #  allResPropertyZonal = allResPropertyZonal.drop(
       #         columns={'msoaZoneID_x','overlap_msoa_split_factor_x','msoa11cd_x', 
        #                 'msoa11cd_y', 'ho_type_x', 'lsoaZoneID_y', 'msoaZoneID_y'})
        #this might be needed
    # Create folder for exports
        arpMsoaAudit = allResPropertyZonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'],axis=1)
        hpaFolder = 'Hops Population Audits'
        nup.CreateFolder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        arpMsoaAudit = arpMsoaAudit['population'].sum()
        print(arpMsoaAudit)
        
        if writeOut:
            allResPropertyZonal.to_csv('classifiedResPropertyLSOA.csv')
            
            #it has 1900 too many people
       
    else:
            print ("no support for this zone")
    
def ApplyNtemSegments(classifiedResPropertyImportPath = 'classifiedResPropertyMSOA.csv',
                      bsqImportPath='Y:/NorMITs Land Use/import/Bespoke Census Query/formatted_long_bsq.csv',
                      areaTypeImportPath = 'Y:/NorMITs Land Use/import/CTripEnd/ntem_zone_area_type.csv',
                      ksEmpImportPath='Y:/NorMITs Land Use/import/KS601-3UK/uk_msoa_ks601equ_w_gender.csv',
                      writeSteps = False, level = 'MSOA'):
    
        # Function to join the bespoke census query to the classified residential 
        # property data
        # Problem here is that there are segments with attributed population 
        # coming in from the bespoke census query that don't have properties to join on.
        # so we need classified properties by msoa for this to work atm
        # Import bespoke census query - this function creates it
        # TODO: look at the household composition and the number of occupants to 
        # check whether those correlate
        
        bsq = CreateNtemSegmentation()
        
    #if level == 'MSOA':
        # Split bsq pop factors out to represent property type as well as zone
        crp = pd.read_csv(classifiedResPropertyImportPath)
    
        crpCols = ['ZoneID', 'census_property_type', 'UPRN', \
               'household_occupancy_18', 'population']
        crp = crp.reindex(crpCols, axis=1)
        #TODO probs should rename MSOA related to zones or something standard
        unqMSOA = crp.reindex(['ZoneID'],axis=1).drop_duplicates()
        len(unqMSOA)
        crp['population'].sum()
    
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

        print('Should be near to zones x property types - ie. 8480 x 6 = 50880 :',\
          bsq['pop_factor'].sum())
  
 #   if level == 'MSOA':

        msoaCols = ['objectid', 'msoa11cd']
        ukMSOA = gpd.read_file(msoaRef)
        ukMSOA = ukMSOA.loc[:,msoaCols]

    # Join in MSOA names and format matrix for further steps
    # TODO: Some handling required here for other zoning systems
        bsq = bsq.merge(ukMSOA, how='left', left_on='msoaZoneID', \
                    right_on='objectid')
        bsqCols = ['msoa11cd', 'Age', 'Gender', 'employment_type', 
               'household_composition', 'property_type', 'R', 'pop_factor']
        bsq = bsq.reindex(bsqCols,axis=1)
        bsq = bsq.rename(columns={'R':'area_type'})

    # audits  
        
        unqMSOA = bsq.reindex(['msoa11cd'],axis=1).drop_duplicates()
        len(unqMSOA)
    
        audit = bsq.reindex(['msoa11cd','property_type', 'pop_factor'], 
                            axis=1).groupby(['msoa11cd','property_type']).sum().reset_index()
        audit.to_csv(segFolder + '/Zone_PT_Factor_Audit_Inter_Join.csv',index=False)

        bsqAudit = bsq.groupby(['msoa11cd', 'property_type']).count().reset_index()
        print(bsqAudit['pop_factor'].drop_duplicates()) 
        crpAudit = crp['population'].sum()
        print(crpAudit)
    # So far so good

    # TODO: Fix join issue.
    # inner join crp - will lose land use bits on non-classified & 
    # communal establishments
        crp = crp.merge(bsq, how='outer', \
                    left_on=['ZoneID','census_property_type'],\
                    right_on=['msoa11cd','property_type'])
        print('pop factor needs to be same as no of zones - 8480')
        print('population needs to resolve back to 60+ million \
          once duplicates are removed')
        crp['pop_factor'].sum()
        crpAudit = crp['population'].drop_duplicates().sum()
        print(crpAudit)
    # Still fine
    
        crp['pop_factor'].sum()
        crpAudit = crp['population'].drop_duplicates().sum()
        print(crpAudit)    
    # Audit bank
        unqMSOA = crp.reindex(['ZoneID'],axis=1).drop_duplicates()
        len(unqMSOA)
        print(crp['population'].sum())
        print(crp['pop_factor'].sum())
    
    # This is where it used to fall to bits
    # Apply population factor to populations to get people by property type
        crp['people'] =  crp['UPRN'] * crp['pop_factor']
    
        outputCols = ['ZoneID', 'area_type', 'census_property_type', 
                  'property_type', 'UPRN', 'household_composition', 
                  'Age', 'Gender', 'employment_type', 'people']
        crp = crp.reindex(outputCols,axis=1)
        crp = crp.rename(columns={'UPRN':'properties'})
    
        pop = crp['people'].sum()
        print('Final population', pop)

        crp.to_csv('landUseOutputMSOA.csv')
        msoaAudit = crp.reindex(['ZoneID', 'people'], axis=1).groupby('ZoneID').sum()
        msoaAudit.to_csv(segFolder +'2018MSOAPopulation_OutputEnd.csv',index=False)
        
        return(crp, bsq)
        '''
        if level == 'LSOA':
        
            crplsoa = pd.read_csv('classifiedResPropertyLSOA.csv')    
        # get the bsq - join to lsoa then use factor by property type method as done for msoa
            msoa_lookup = pd.read_csv('Y:/NorMITs Land Use/import/Documentation/msoa_lsoa_zonetranslations.csv')
            msoa_lookup = msoa_lookup.drop(columns={
                    'lsoaZoneID', 'lsoa_var', 'msoa_var', 'overlap_lsoa_split_factor', 'overlap_type', 'overlap_var'})
            
            bsqlsoa = bsq.merge(msoa_lookup, on = 'msoaZoneID')
            factorPropertyType = bsqlsoa.reindex(['lsoa11cd','property_type',\
                        'pop_factor'],axis=1).groupby(['lsoa11cd','property_type']).sum().reset_index()
            factorPropertyType = factorPropertyType.rename(columns={'pop_factor':'pt_pop_factor'})
            bsqlsoa = bsqlsoa.merge(factorPropertyType, how='left', \
                    on=['lsoa11cd','property_type'])
            bsqlsoa['pop_factor'] = bsqlsoa['pop_factor']/bsqlsoa['pt_pop_factor']
    
            bsqlsoa = bsqlsoa.drop('pt_pop_factor',axis=1)
    
            segFolder = 'NTEM Segmentation Audits'
            nup.CreateFolder(segFolder)
            audit = bsqlsoa.reindex(['lsoa11cd','property_type', 'pop_factor'],\
                        axis=1).groupby(['lsoa11cd','property_type']).\
                        sum().reset_index()
            audit.to_csv(segFolder + '/Zone_PT_Factor_Pre_Join_Audit_lsoa.csv',index=False)

            print('Should be near to zones x property types - ie. 42000 x 6 = 246k :',\
            bsqlsoa['pop_factor'].sum())
    
        lsoaCols = ['objectid', 'lsoa11cd']
        ukLSOA = gpd.read_file(lsoaRef)
        ukLSOA = ukLSOA.loc[:,lsoaCols]

        bsqlsoa = bsqlsoa.merge(ukLSOA, how='left', left_on='lsoa11cd', \
                    right_on='lsoa11cd')
        bsqlCols = ['lsoa11cd', 'Age', 'Gender', 'employment_type', 
               'household_composition', 'property_type', 'R', 'pop_factor']

        bsqlsoa = bsqlsoa.reindex(bsqlCols,axis=1)
        bsqlsoa = bsqlsoa.rename(columns={'R':'area_type'})
        unqLSOA = bsqlsoa.reindex(['lsoa11cd'],axis=1).drop_duplicates()
        len(unqLSOA)

        audit = bsqlsoa.reindex(['lsoa11cd','property_type', 'pop_factor'], axis=1).groupby(
            ['lsoa11cd','property_type']).sum().reset_index()
        audit.to_csv(segFolder + '/Zone_PT_Factor_Audit_Inter_Joinlsoa.csv',index=False)

        bsqlAudit = bsqlsoa.groupby(['lsoa11cd', 'property_type']).count().reset_index()
        print(bsqlAudit['pop_factor'].drop_duplicates()) 
    
        crplAudit = crplsoa['population'].sum()
        print(crplAudit)
        crplsoa = crplsoa.drop(columns={'Unnamed: 0'})
        crplsoa = crplsoa.merge(bsqlsoa, how='outer', \
                    left_on=['ZoneID','census_property_type'],\
                    right_on=['lsoa11cd','property_type'])
        print('pop factor needs to be same as no of zones - 42000')
        print('population needs to resolve back to 60+ million \
          once duplicates are removed')
        crplsoa['pop_factor'].sum()
        crpAudit = crplsoa['population'].drop_duplicates().sum()
        unqLSOA = crplsoa.reindex(['lsoa11cd'],axis=1).drop_duplicates()
        len(unqLSOA)
        print(crplsoa['population'].sum())
        print(crplsoa['pop_factor'].sum())
        crplsoa['people'] =  crplsoa['UPRN'] * crplsoa['pop_factor']
        crplsoa = crplsoa.rename(columns={'lsoa11cd':'ZoneID'})
        outputCols = ['ZoneID', 'area_type', 'census_property_type', 
                  'property_type', 'UPRN', 'household_composition', 
                  'Age', 'Gender', 'employment_type', 'people']
        crplsoa = crplsoa.reindex(outputCols,axis=1)
        

        return(crplsoa, bsqlsoa)
        '''
    
# Master functions
def RunLandUseSuite(projectName, importFromSQL=False, subset=None, importClassifiedProperty=False,
                    zoningShpPath=defaultZoningPath, level = 'MSOA',
                    zoningShpName=defaultZoneName, XYcols=['X_COORDINATE', 'Y_COORDINATE']):

    # TODO: What does this do?
    SetWd(homeDir = 'C:/NorMITs_Export', iteration=projectName)

    if not importClassifiedProperty:

        if importFromSQL:
            print('Importing from TfN SQL Server')
            BuildCorePropertyData(writeOut=True)
        else :
            print('SQL import disabled - please check ABP subsets are in project folder') 

        if subset == None:
            print('Beginning property type analysis for Mainland GB')
            allResProperty = PropertyTypeAnalysis(subset = None, zonalAggregation=defaultZoneName, writeSteps=True)
        else :
            print('Beginning property type analysis for geo-subset provided')
            allResProperty = PropertyTypeAnalysis(subset = subset, zonalAggregation=defaultZoneName, writeSteps=False)
        gc.collect()
    else:
        allResProperty=pd.read_csv('allResProperty' + level + 'Classified.csv')

    # Where on earth are these 99s coming from?
    finalAudit = allResProperty.groupby('census_property_type').count().reset_index().reindex(['census_property_type', 'UPRN'],axis=1)
    print(finalAudit)
    print(finalAudit['UPRN'].sum()/1000000)
    audits = LandUseAudits(allResProperty, writeOut=True, reportName = 'full_run')
    # Remember the audit is EW only
    audits['msoa_property_count'].sum()
    audits['ZoneID'].drop_duplicates().count()
    
    ApplyHouseholdOccupancy()
    #ApplyControltoMYE here?
    ApplyNtemSegments()
    # TODO: Furness function from Land Use Audits to move up or down towards counts
    Myeadjustment()
    return(allResProperty)

def main(iteration=defaultIter):

    RunLandUseSuite(iteration, importClassifiedProperty=True)

    return(None)
