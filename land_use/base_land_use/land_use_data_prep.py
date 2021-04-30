# -*- coding: utf-8 -*-
"""
Created on Tue May 26 17:38:48 2020

@author: ESRIAdmin

AddressBase prep
- get addressbase extract
- build zone correspondence
- uprn audit
- classification count
- property type analysis
- zonal count
#TODO: change to lowercase for functions
once run the source data should be ready for other functions
# TODO: change the RD06 to categiry 4 = 'flats' in next iter
# TODO: change 'ONS reports 27.4M in 2017' to be VOA based
# TODO: take into account classifications for communal establishments

"""
import gc
import os
import sys
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Demand Tool/Python/ZoneTranslation')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/TAME shared resources/Python/')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Utilities/Python')

import nu_project as nup
import sql_connect as sc
import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import *


# default file paths
_default_iter = 'iter4'
_default_home_dir = ('D:/NorMITs_Export/')
_import_folder = 'Y:/NorMITs Land Use/import/'
_import_file_drive = 'Y:/'
_default_zone_folder = ('I:/NorMITs Synthesiser/Zone Translation/')
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'


_default_uprn_lookup_path = (_import_folder + 'AddressBase/2018/uprnLookup.csv')
_default_census_property_types_path = (_import_folder+'Census_Property_Type_Maps.xlsx')
_default_alladdresses_path = (_import_folder + 'AddressBase/2018/allAddresses.csv')
_default_allproperties_path = (_import_folder + 'AddressBase/2018/allProperties.csv')

_default_lsoaRef = _default_zone_ref_folder+'/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_msoaRef = _default_zone_ref_folder+'/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
_default_ladRef = _default_zone_ref_folder+'/GIS Shapefiles/LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
_default_mladRef = _default_zone_ref_folder+'/GIS Shapefiles/Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'


_default_zone_name = 'MSOA'  #_defaultZoneNames = ['LSOA','MSOA'] choose from
_default_zoning_path = _default_msoaRef 

def set_wd(homeDir = _default_home_dir, iteration=_default_iter):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()

def get_uprn_lookup(path = _default_uprn_lookup_path):
    """
    Import a csv of Uprn lookup between each address by UPRN and zones.

    This might point to pre-processing function or modelling folders in the future

    Parameters
    ----------
    _default_uprn_lookup_path:
        Path to csv of UPRN lookup

    Returns
    ----------
    UPRN lookup:
        DataFrame containing UPRN lookup.
    """
    print('Reading in UPRN lookup')

    uprnLookup = pd.read_csv(path)
    return(uprnLookup)
    
def get_all_properties(path=_default_allproperties_path):
    """
    Import a csv of AddressBase all properties.

    This might point to pre-processing function or modelling folders in the future

    Parameters
    ----------
    _default_allproperties_path:
        Path to csv of all properties

    Returns
    ----------
    UPRN lookup:
        DataFrame containing all properties from AddressBase extract.
    """
    print('Reading in all properties')

    allProperties = pd.read_csv(path)
    
    return(allProperties)
  
def get_all_addresses(path=_default_alladdresses_path):
    """
    Import a csv of All addresses.

    This might point to pre-processing function or modelling folders in the future

    Parameters
    ----------
    _default_allproperties_path:
        Path to csv of all properties

    Returns
    ----------
    UPRN lookup:
        DataFrame containing all properties from addressBase extract.
    """
    print('Reading in all addresses')

    allAddresses = pd.read_csv(path)
    
    return(allAddresses)
   

def CountListShp(shp=_default_zoning_path, idCol=None):
    """
    Go and fetch a shape and return a count and a list of unq values 
    and get a count of zones. Useful for audits later.
    """
    shp = gpd.read_file(shp)
    if idCol is None:
        idCol=list(shp)[0]
    shp = shp.loc[:,idCol]
    return(len(shp),shp)

def BuildCorePropertyData(writeOut=False):
    """
    # TODO: Check if the data has been imported already - if so skip the calls
    This will run a qeury to SQL server to extract the AddressBase information, 
    and will save it on a local drive within your homedir.
    Shouldn't be needed to run after the extract is done, the files are big and
    
    This can potentially take a geographic limiter off the bat 
    - needs the ABP to be spatially enabled ie. PostGIS
    Bring in data from SQL server, could be parametised to work with 
    any AB or ABP data. 
    """
    importString = (os.getcwd() + '/')
    writtenFiles = os.listdir(importString)
    
    if writeOut:
        print('Writing project files to project folder as csv - this may take a while')

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
            
    return(print('Property database built'))

def GeoEnable(ABPFile, XYcols = ['X_COORDINATE', 'Y_COORDINATE']): 
    """
    Take an addressbase premium file and write coords as WKT
    
    Function to take the X and Y coordinates in ABP data and turn them 
    into OS1936 points for spatial work
    Great to have - no longer needed for this as we have the ONS lookup - 
    may move to utils
    """
    ABPFile['Coordinates'] = list(zip(ABPFile[XYcols[0]], ABPFile[XYcols[1]]))
    # if successful? don't want it doing this otherwise
    ABPFile['Coordinates'] = ABPFile['Coordinates'].apply(Point)
    ABPFile = gpd.GeoDataFrame(ABPFile, geometry='Coordinates')
    ABPFile.drop(['X_COORDINATE', 'Y_COORDINATE'], axis=1)
    ABPFile.crs = nf.osgbCrs
    
    return(ABPFile)

def SubsetBuild(ABPFile, subsetShape):

    """
    Function to subset a zonal dataframe by a given shapefile
    gpd.sjoin is super inefficient and this may need rewriting to go by line
    This could also just live in utils
    """

    subsetShape = gpd.read_file(subsetShape)
    ABPFile = GeoEnable(ABPFile)
    # TODO - build a way to make this work using polygon exclusion - 
    # may work already, but check.
    subsetABP = gpd.sjoin(ABPFile, subsetShape)
    return(subsetABP)

def BuildZoneCorrespondence(ABPFile, writeOut = False, subsetShape = None, \
                            zoningShpName=_default_zone_name, 
                            path = _default_uprn_lookup_path):
    """
    # Function to take classified property data and apply a spatial category
    # Reads classified property data direct from the current iteration folder
    # Takes a subset shape (oa, lsoa, msoa) on which it applies the subset build function
    # Process uprns
    """
    uprnLookup = pd.read_csv(path)
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
    
def ResClassCaseWhen(row):
    """
    Defines case when handler for classification 
    
    """
    if row['census_property_type'] == 0:
        return 'ignored'
    elif row['census_property_type'] >= 1 and \
    row['census_property_type'] <= 9:
        return 'classified'
    elif row['census_property_type'] == 99:
        return 'more work required'
    else:
        return 'other'
    
def Rd06CaseWhen(row):
    """
    Case when tree to assign census types based on logic
    Left this in to match to census property types.
    This is kind of redundant now as we aggregate all flats together at a later stage.
    Left here in case we do more work on the classifications and Rd06 logic is 
    replaced with something that matches the property across.

            
   # RD06 condition loop
           # Code describes Self-Contained Flat Includes Maisonette Apartment
           # - scf
            # Can resolve to census property type 4, 5 or 6
            # 4 = flat in purpose built block of flats
            # 5 = flat in a converted or shared house
            # 6 = in a commercial building
    """
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
        
def ApplyClassificationLogic(allResProperty, logic=None):
    """
    # Function to process census types for RD06
    # Main function can be applied to a dataframe, sub function is 
    # a case when for pandas apply method
    
    # TODO: RD06 is now just flats category 4
    """        
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
        scf = scf.drop(['ORGANISATION_NAME', 'DEPARTMENT_NAME', 
                        'SUB_BUILDING_NAME', 'BUILDING_NAME',
                        'BUILDING_NUMBER'], axis=1)
        allResProperty = pd.concat([scf.reset_index(drop=True), 
                                    rejoin.reset_index(drop=True)], 
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
    
def ZonalPropertyCount(RD, 
                       groupingCol=None,
                       targetLen=CountListShp(shp=_default_zoning_path)[0],
                       targetZones=CountListShp(shp=_default_zoning_path)[1],
                       writeOut=False,
                       reportName=''):
    """
    Another standard report but outputs at zonal grouping
    Uses the ZoneID cols by default
    Takes grouping variable as a column name
    TODO: Make properly zone agnostic - this will probably break with LSOA
    """
    if groupingCol is None:
        pByZone = RD.groupby(['ZoneID']).count().reindex(['UPRN'],axis=1).reset_index()
    else:
        pByZone = RD.groupby(['ZoneID', groupingCol]).count().reindex(['UPRN',groupingCol],axis=1)
    
    print('Most properties by Zone:')
    print(pByZone.UPRN.nlargest(n=10))
    print('Least properties by Zone:')
    print(pByZone.UPRN.nsmallest(n=10))
    print('Total Mainland GB properties:')
    print(pByZone.UPRN.sum(), ', ', round(pByZone.UPRN.sum()/1000000,2), 'M')
    print('VOA reports 25.6M residential props in E+W in 2018')
    
    zoneCount = len(pByZone)
    
    if zoneCount < targetLen:
        print('Some zones missing, writing missing zones to audit folder')
        mz = targetZones[~targetZones.isin(pByZone.ZoneID)]
        mz = mz.to_frame()
        mz['exists'] = 'No'
        mz.to_csv('Land Use Audits/' + reportName + '_missing_zones.csv', \
                  index=False)
    else:
        print('All zones accounted for')
    
    if writeOut:
        pByZone.to_csv('Land Use Audits/' + reportName + '_property_by_zone.csv', index=True)
    
    return(pByZone)
              
  
def PropertyTypeAnalysis(subset = None, 
                         zonalAggregation=_default_zone_name, 
                         path = _default_census_property_types_path,
                         writeSteps=True): 
    """
    Params = read from flat or database, another subset builder by shapefile
    This is based on ABP classifications to census property type maps first 
    and then logic from case when statements.
    
    Define case when handler for classification
    """
    print('Importing datasets from gbpd build')
    censusPropertyTypes = pd.read_excel(path, sheet_name=0)
    allProperties = get_all_properties()
    allAddresses = get_all_addresses()      
    
    print('Merging commercial counts')
    # Create count of organisation name by point
    orgCols = ['UPRN', 'ORGANISATION_NAME']
    propCols = ['UPRN', 'X_COORDINATE','Y_COORDINATE']
    orgNameCount = allProperties.reindex(propCols,axis=1).merge(allAddresses.reindex(orgCols,axis=1), 
                                       how='inner', on='UPRN').drop('UPRN',axis=1).groupby(['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    orgNameCount = orgNameCount.rename(columns={'ORGANISATION_NAME':'org_count'})

    # Create count of commercial properties per coordinate
    print('Creating commercial property counts')
    allCommercialProperty = allProperties[allProperties.loc[:,'CLASSIFICATION_CODE'].str.startswith('C')]
    allCommercialPropertyCount = allCommercialProperty.groupby(
                        ['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    allCommercialPropertyCount = allCommercialPropertyCount.reindex(
                        ['X_COORDINATE', 'Y_COORDINATE', 'UPRN'], axis=1)
    allCommercialPropertyCount = allCommercialPropertyCount.rename(
                         columns={'UPRN':'commercial_property_count'})

    # Create count of properties per coordinate
    allResProperty = allProperties[allProperties.loc[:,'CLASSIFICATION_CODE'].str.startswith('R')]
    allResPropertyCount = allResProperty.groupby(
                        ['X_COORDINATE','Y_COORDINATE'],as_index=False).count()
    allResPropertyCount = allResPropertyCount.reindex(
                        ['X_COORDINATE', 'Y_COORDINATE', 'UPRN'], axis=1)
    allResPropertyCount = allResPropertyCount.rename(columns={'UPRN':'property_count'})
    
    # TODO: Establish some other useful bits here
    # TODO: Nearby property type counts in a bounding box - c
    # ircle is too hard & resource intensive

    print('Remerging property data')
    # Merge back into the property data
    allResProperty = allResProperty.merge(orgNameCount, how='left', 
                            on=['X_COORDINATE', 'Y_COORDINATE'])
    allResProperty = allResProperty.merge(allCommercialPropertyCount, 
                            how='left', on=['X_COORDINATE', 'Y_COORDINATE'])
    allResProperty = allResProperty.merge(allResPropertyCount, 
                            how='left', on=['X_COORDINATE', 'Y_COORDINATE'])
    # Join the classification codes on
    allResProperty = allResProperty.merge(censusPropertyTypes, 
                            how='left', left_on='CLASSIFICATION_CODE', 
                            right_on='abp_code')
    
    del(allCommercialProperty, allProperties, orgNameCount, 
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
    """
    TODO: This should take any of the 3 zoning systems in the UPRN lookup 
    as described in the function parameters
    do we need to filter out the parent/child UPRNs, this would make a diff for flats
    
    Remove non-existing property types:
    1 under construction, 2 In use, 3 Unoccupied / vacant / derelict, 
    4 Demolished, 6 Planning permission granted
    technical spec: 
    https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/addressbase-premium-technical-specification.pdf
    """
    print('Removing demolished or under construction properties')
    noneProperties=[3,4,6]
    allResProperty = allResProperty[~allResProperty.BLPU_STATE.isin(noneProperties)]

    #LOGICAL_STATUS shows whether the address is provisional, live or historical
    #8 is historical, 6 is provisional, 3 - alternative, 1 is approved 
    print('Removing all historical/alternative and provisional properties')
    historicalprops=[3,6,8]
    allResProperty = allResProperty[~allResProperty.LOGICAL_STATUS.isin(historicalprops)]

    # Let's get rid of non-postal addresses too
    nonpostal = ['N']
    allResProperty = allResProperty[~allResProperty.ADDRESSBASE_POSTAL.isin(nonpostal)]
    
    audit = ClassificationCount(allResProperty)
    print('Active Property Counts:')
    print(audit[0])
    print(audit[1])

    allResProperty = BuildZoneCorrespondence(allResProperty, zoningShpName=zonalAggregation)
    ZonalPropertyCount(allResProperty, writeOut=True, reportName='unclassified')
    
    # Import here for debugging - write out for new runs
    if writeSteps:
        allResProperty.to_csv('allResProperty' + zonalAggregation + 'Unclassified.csv',index=False)
    # allResProperty = pd.read_csv('allResProperty' + zonalAggregation
    # + 'Unclassified.csv')
    gc.collect()
    
    def ApplyClassificationLogic(allResProperty, logic=None):
        """
        Function to process census types for RD06
        Main function can be applied to a dataframe, sub function is 
        a case when for pandas apply method
        
        TODO: RD06 is now just flats category 4
        """        
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
            scf = scf.drop(['ORGANISATION_NAME', 'DEPARTMENT_NAME', 
                            'SUB_BUILDING_NAME', 'BUILDING_NAME',
                            'BUILDING_NUMBER'], axis=1)
            allResProperty = pd.concat([scf.reset_index(drop=True), 
                                        rejoin.reset_index(drop=True)], 
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
    
def ResClassCaseWhen(row):
    """
    Classification for census property types
    To be used with pandas .apply.
    Parameters
    ----------
    row:
        Row to be applied to or iterated over.

    Returns
    ----------
    row['census_property_type']:
        value written over or appended to row.
    """
    if row['census_property_type'] == 0:
        return 'ignored'
    elif row['census_property_type'] >= 1 \
    and row['census_property_type'] <= 9:
        return 'classified'
    elif row['census_property_type'] == 99:
        return 'more work required'
    else:
        return 'other'

    
def ClassificationCount(cRD, allResCountPath = _default_home_dir+_default_iter+'/allResCodeCount.csv'):
    """
    cRD = Classified Residential
    Standard classification audit report
    Takes classified residential data and counts the classifications
    """
    
    reportCols = ['CLASSIFICATION_CODE', 'census_property_type', 'UPRN']
    # classifiedResidentialData
    compCatCount = cRD.groupby(
            ['CLASSIFICATION_CODE', 'census_property_type'], as_index=False).count().reindex(reportCols,axis=1)
    #compCatCount = compCatCount.iloc[:, 0:3]
    
    allResCount = pd.read_csv(allResCountPath)
    compCatCount = allResCount.merge(compCatCount,
                                     how='left',
                                     left_on='CLASSIFICATION_CODE',
                                     right_on='CLASSIFICATION_CODE')
    
    compCatCount.rename(columns={'UPRN':'n'}, inplace=True)

    compCatCount['pc'] = compCatCount.loc[:,'n']/np.nansum(compCatCount.loc[:,'n'])
        
    compCatCount['classification_status'] = compCatCount.apply(ResClassCaseWhen, axis=1)
    
    print(compCatCount.n.sum(), 'properties')
    
    abpTypeAudit = compCatCount.groupby(['CLASSIFICATION_CODE', 
                                         'census_property_type']
                                            ).sum().drop(columns = 'pc'
                                                 ).reset_index()
    censusTypeAudit = compCatCount.groupby('classification_status'
                                           ).sum().drop(columns = 'census_property_type'
                                                ).reset_index()    
    return(abpTypeAudit, censusTypeAudit)


def UprnAudit(defaultZoneName):
    """
    Check the MSOA zoning system we're all in on matches across types
    Needed because of everyone using different scottish Geographies
    TODO: Get this to work with any zoning system in the UPRN lookup
    """
           
    uprnLookup = get_uprn_lookup()
    unqUprnLsoa = uprnLookup.loc[:,'lsoa11'].drop_duplicates().reset_index()
  
    lsoaCols = ['objectid', 'lsoa11cd']
    ukLSOA = gpd.read_file(_default_lsoaRef)
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
    ukMSOA = gpd.read_file(_default_msoaRef)
    ukMSOA = ukMSOA.loc[:,msoaCols]
    auditSet = ukMSOA.merge(unqUprnMsoa, how='outer', left_on='msoa11cd', right_on='msoa11')

    if len(unqUprnMsoa) == len(ukMSOA) & len(unqUprnMsoa) == len(auditSet):
           return(print('Uprn lookup and spatial index match'))
    else:       
           return(print('Uprn lookup and spatial index zones don\'t match, \
                            check ONS UPRN lookup and match zones to that'))


def LandUseAudits(zonalResProperty,
                  auditDatImportPath = _import_folder+'/ONS Audits/2011_household_type_audit_format_msoa.csv',
                  writeOut=True, 
                  reportName='Audit', level = 'MSOA'):
    """   
    TODO: Replace the import path with the VOA properties for 2018
    Function to count classified zonal property outputs and compare to and audit dataset
    TODO: ATM it's just either LSOA or MSOA
    Need a loop to convert the property types from the audit into other
    zoning systems as required, OA as well in the future?
    TODO: Have this error handle if the lookup doesn't work as it's non-crucial
    """
    if level == 'MSOA':     
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
            temp.to_csv('Land Use Audits/census_property_type_' + repr(pt) + '_audit.csv') 
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
            temp.to_csv('Land Use Audits/census_property_type_' + repr(pt) + '_audit.csv') 
        del(temp)
    
        if writeOut:
                zoneResPropertyCount.to_csv('Land Use Audits/' + reportName + \
                                '_lsoa_ons_comparison.csv', index=False)
    # Import and check against MSOA 2011 counts by property type
    # Will need to call ZonalCount function with an MSOA breakdown 
    # for England and Wales only.
        return(zoneResPropertyCount)
        
def run_abp_prep(project = _default_iter, importFromSQL=False, subset=None, importClassifiedProperty=False,
                    zoningShpPath=_default_zoning_path, level = 'MSOA',
                    zoningShpName=_default_zone_name, XYcols=['X_COORDINATE', 'Y_COORDINATE']):
    """
    This runs the ABP data prep with audits

    Parameters
    ----------
    Raw data from AddressBase

    Returns
    ----------
    Fully classified property data.
    """  
    print('Starting the ABP data prep')

    set_wd(homeDir = _default_home_dir, iteration=project)

    if not importClassifiedProperty:

        if importFromSQL:
            print('Importing from TfN SQL Server')
            BuildCorePropertyData(writeOut=True)
        else :
            print('SQL import disabled - please check ABP subsets are in project folder') 

        if subset == None:
            print('Beginning property type analysis for Mainland GB')
            allResProperty = PropertyTypeAnalysis(subset = None, zonalAggregation=_default_zone_name, writeSteps=True)
        else :
            print('Beginning property type analysis for geo-subset provided')
            allResProperty = PropertyTypeAnalysis(subset = subset, zonalAggregation=_default_zone_name, writeSteps=True)
        gc.collect()
    else:
        allResProperty=pd.read_csv('allResProperty' + level + 'Classified.csv')

    finalAudit = allResProperty.groupby('census_property_type').count().reset_index().reindex(['census_property_type', 'UPRN'],axis=1)
    print(finalAudit)
    print(finalAudit['UPRN'].sum()/1000000)
    audits = LandUseAudits(allResProperty, writeOut=True, reportName = 'full_run')
    # Remember the audit is EW only - 7201 MSOAs
    audits['msoa_property_count'].sum()
    audits['ZoneID'].drop_duplicates().count()


