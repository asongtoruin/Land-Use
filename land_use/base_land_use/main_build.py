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
import os  # File operations
import sys

sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Demand Tool/Python/ZoneTranslation')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/TAME shared resources/Python/')
sys.path.append('C:/Users/ESRIAdmin/Desktop/Code-Blob/NorMITs Utilities/Python')

import numpy as np  # Vector operations
import pandas as pd  # main module
import geopandas as gpd
import shutil as sh
from land_use import utils
import land_use.lu_constants as consts

# Default file paths
# TODO: Implement taking these from the base year object. Comments below indicate relevant attributes

_default_iter = 'iter4'  # take from self.iteration
_default_home = 'E:/NorMITs_Export/'  # needs to be on Y drive, self.model_folder?
_default_home_dir = _default_home + _default_iter  # perhaps this can stay as-is if the above two are in base object
_import_folder = 'Y:/NorMITs Land Use/import/'  # self.import_folder
_default_zone_folder = 'I:/NorMITs Synthesiser/Zone Translation/'  # these are for zone translations, I think not yet in base object
# Default zone names
_default_zone_name = 'MSOA'  # MSOA or LSOA, perhaps needs to be self.model_zoning

# As these are constructed from the above, perhaps it's ok to leave as they are but it would still be better in the constants
_default_addressbase_extract_path = (_default_home_dir + '/allResProperty' + _default_zone_name + 'Classified.csv')
_default_census_property_types_path = (_import_folder + 'Census_Property_Type_Maps.xlsx')
_default_communal_types_path = (_import_folder + 'Communal Establishments 2011 QS421EW/communal_msoaoutput.csv')
_default_communal_employment_path = (_import_folder + 'Communal Establishments 2011 QS421EW/DC6103EW_sums.csv')
_default_census_population = (_import_folder + 'Communal Establishments 2011 QS421EW/DC11004_formatted.csv')
_default_lad_translation = (_default_zone_folder + 'Export/lad_to_msoa/lad_to_msoa.csv')
_default_census_dat = (_import_folder + 'Nomis Census 2011 Head & Household')
_default_zone_ref_folder = 'Y:/Data Strategy/GIS Shapefiles/'
_default_area_types = ('Y:/NorMITs Land Use/import/area_types_msoa.csv')

_default_lsoaRef = _default_zone_ref_folder + 'UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
_default_msoaRef = _default_zone_ref_folder + 'UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
_default_ladRef = _default_zone_ref_folder + 'LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
_default_mladRef = _default_zone_ref_folder + 'Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'
_nssecPath = _import_folder + 'NPR Segmentation/processed data/TfN_households_export.csv'
_file_path_list = r"Y:\NorMITs Land Use\import\AddressBase\2018\List of ABP datasets.csv"


# 1. Get AddressBase data
def copy_addressbase_files():
    """
    Copy the relevant ABP files from import drive to _default_home_dir for use in later functions.
    _file_path_list: Path to csv of AddressBase extract paths.
    """
    dest = _default_home_dir
    files = pd.read_csv(_file_path_list)

    for file in files.FilePath:
        try:
            sh.copy(file, dest)
            print("Copied over file into default iter folder: " + file)
        except IOError:
            print("File not found")


# 2. Main analysis functions - everything related to census and segmentation
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


def zone_up(cpt_data,
            zone_translation_path=_default_zone_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv',
            grouping_col='msoaZoneID'):
    """
    Function to raise up a level of spatial aggregation & aggregate at that level, then bring new factors back down
    # TODO: Might be nice to have this zone up any level of zonal aggregation
    Raise LSOA to MSOA for spatial aggregation
    """
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


# TODO: improve the docstring here
def balance_missing_hops(cpt_data, grouping_col='msoaZoneID', hlsaName=_default_zone_name,
                         zone_translation_path=(_default_zone_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv')):
    """
    # TODO: Replace global with LAD or Country - likely to be marginal improvements. Currently UK-wide
    This resolves the  msoa/lad household occupancy
    """
    msoa_agg = zone_up(cpt_data,
                       zone_translation_path=_default_zone_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv',
                       grouping_col=grouping_col)
    msoa_agg = msoa_agg.loc[:, [grouping_col, 'census_property_type',
                                'household_occupancy']].rename(columns={'household_occupancy': 'msoa_ho'})

    global_agg = zone_up(cpt_data,
                         zone_translation_path=_default_zone_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa.csv',
                         grouping_col=grouping_col)
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


# TODO: review this function
def create_employment_segmentation(bsq,
                                   ksEmpImportPath=_import_folder + '/KS601-3UK/uk_msoa_ks601equ_w_gender.csv'):
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
    msoaShp = gpd.read_file(_default_msoaRef).reindex(['objectid', 'msoa11cd'], axis=1)

    nonWa = ['under 16', '75 or over']
    workingAgePot = bsq[~bsq.Age.isin(nonWa)]
    nonWorkingAgePot = bsq[bsq.Age.isin(nonWa)]
    # Add non working age placeholder
    placeholderValue = 'non_wa'
    nonWorkingAgePot['employment_type'] = placeholderValue
    del placeholderValue

    # Import UK MSOA Employment - tranformed to long in R - most segments left in for aggregation here
    # Factors are already built in R - will aggregate to 2 per msoa 1 for Males 1 for Females
    ksEmp = pd.read_csv(ksEmpImportPath).reindex(['msoaZoneID', 'Gender', 'employment_type', 'wap_factor'], axis=1)

    # Change MSOA codes to objectids
    ksEmp = ksEmp.merge(msoaShp, how='left', left_on='msoaZoneID',
                        right_on='msoa11cd').drop(['msoa11cd', 'msoaZoneID'],
                                                  axis=1).rename(columns=
                                                                 {'objectid': 'msoaZoneID'})

    def _agg_wap_factor(ks_sub, new_seg):
        """
        Function to combine working age population factors to create NTEM
        employment categories
        """
        ks_sub = ks_sub.groupby(['msoaZoneID', 'Gender']).sum().reset_index()
        ks_sub['employment_type'] = new_seg
        return ks_sub

    # full time employment =  sum(emp_ft, emp_se)
    ksFte = ksEmp[ksEmp.employment_type.isin(['emp_ft', 'emp_se'])]
    ksFte = _agg_wap_factor(ksFte, new_seg='fte')
    # part time employment = sum(emp_pt)
    ksPte = ksEmp[ksEmp.employment_type.isin(['emp_pt'])]
    ksPte = _agg_wap_factor(ksPte, new_seg='pte')
    # students = sum(emp_stu)
    ksStu = ksEmp[ksEmp.employment_type.isin(['emp_stu'])]
    ksStu = _agg_wap_factor(ksStu, new_seg='stu')
    # not employed/students = sum(unemp, unemp_ret, unemp_stu, unemp_care,
    # unemp_lts, unemp_other)
    ksUnm = ksEmp[ksEmp.employment_type.isin(['unemp', 'unemp_ret', 'unemp_stu',
                                              'unemp_care', 'unemp_lts',
                                              'unemp_other'])]
    ksUnm = _agg_wap_factor(ksUnm, new_seg='unm')

    ksEmp = ksFte.append(ksPte).append(ksStu).append(ksUnm).reset_index(drop=True)

    workingAgePot = workingAgePot.merge(ksEmp, how='left', on=['msoaZoneID', 'Gender'])
    workingAgePot['w_pop_factor'] = workingAgePot['pop_factor'] * workingAgePot['wap_factor']
    workingAgePot = workingAgePot.drop(['pop_factor', 'wap_factor'], axis=1).rename(
        columns={'w_pop_factor': 'pop_factor'})

    bsq = workingAgePot.append(nonWorkingAgePot, sort=True)
    bsq = bsq.reindex(['msoaZoneID', 'Age', 'Gender', 'employment_type',
                       'household_composition', 'property_type', 'B', 'R',
                       'Zone_Desc', 'pop_factor'], axis=1)

    return bsq


def create_ntem_areas(bsq_import_path=_import_folder + '/Bespoke Census Query/formatted_long_bsq.csv',
                      area_type_import_path=_import_folder + '/CTripEnd/ntem_zone_area_type.csv'):
    """
    Reduce age & gender categories down to NTEM requirements and add area type for each zone.

    bsq_import_path: location of the bespoke census query data
    area_type_import_path: NTEM area type classifications by zone
    """
    # Import Bespoke Census Query - already transformed to long format in R
    print('Importing bespoke census query')
    bsq = pd.read_csv(bsq_import_path)

    # Import area types
    area_types = pd.read_csv(area_type_import_path)

    # Shapes
    mlaShp = gpd.read_file(_default_mladRef).reindex(['objectid', 'cmlad11cd'], axis=1)
    msoaShp = gpd.read_file(_default_msoaRef).reindex(['objectid', 'msoa11cd'], axis=1)

    # Lookups
    # Bespoke census query types
    # TODO: make these a dictionary in LU constants
    pType = pd.read_csv(_import_folder + '/Bespoke Census Query/bsq_ptypemap.csv')
    hType = pd.read_csv(_import_folder + '/Bespoke Census Query/bsq_htypemap.csv')

    # Zonal conversions
    mlaLookup = pd.read_csv(_default_zone_folder + 'Export/merged_la_to_msoa/merged_la_to_msoa.csv').reindex(
        ['msoaZoneID', 'merged_laZoneID'], axis=1)
    ntem_to_msoa = pd.read_csv(_default_zone_folder + 'Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv').reindex(
        ['ntemZoneID', 'msoaZoneID', 'overlap_ntem_pop_split_factor'], axis=1)

    # Reduce age & gender categories down to NTEM requirements
    def _segment_tweaks(bsq, asisSegments, groupingCol, aggCols, targetCol, newSegment):
        """
        Take a bsq set, segments to leave untouched and a target column.
        Sum and reclassify all values not in the untouched segments.
        """
        bsq[targetCol] = np.where(bsq[groupingCol].isin(asisSegments), bsq[targetCol], newSegment)
        aggCols += [targetCol]
        bsq = bsq.groupby(aggCols).sum().reset_index()

        return bsq

    # All working age population comes in one monolithic block - 16-74
    bsq = _segment_tweaks(bsq,
                          asisSegments=['under 16', '75 or over'],
                          groupingCol='Age',
                          aggCols=['LAD_code', 'LAD_Desc', 'Gender', 'Dwelltype', 'household_type'],
                          targetCol='Age',
                          newSegment='16-74')

    # Children have no gender in NTEM - Aggregate & replace gender with 'Children'
    bsq = _segment_tweaks(bsq,
                          asisSegments=['16-74', '75 or over'],
                          groupingCol='Age',
                          aggCols=['LAD_code', 'LAD_Desc', 'Age', 'Dwelltype', 'household_type'],
                          targetCol='Gender',
                          newSegment='Children')

    # TODO: use dictionaries to normalise these values instead
    bsq = bsq.merge(pType, how='left', left_on='Dwelltype',
                    right_on='c_type').drop(['Dwelltype', 'c_type'], axis=1)
    bsq = bsq.merge(hType, how='left', on='household_type').drop('household_type', axis=1)

    # Calculate the total population in each LAD and the proportion of this that each segment represents
    bsq_total = bsq.groupby(['LAD_code', 'LAD_Desc'])['population'].sum().reset_index()
    bsq_total = bsq_total.rename(columns={'population': 'lad_pop'})
    bsq = bsq.merge(bsq_total, how='left', on=['LAD_code', 'LAD_Desc'])
    del bsq_total  # save some memory
    bsq['pop_factor'] = bsq['population'] / bsq['lad_pop']
    bsq = bsq[['LAD_code', 'LAD_Desc', 'Gender', 'Age', 'property_type', 'household_composition', 'pop_factor']]

    # Merge on msoaZoneID - includes only English & Welsh MSOAs
    bsq = bsq.merge(mlaShp, how='left', left_on='LAD_code',
                    right_on='cmlad11cd').drop('cmlad11cd', axis=1)
    bsq = bsq.merge(mlaLookup, how='left', left_on='objectid',
                    right_on='merged_laZoneID').drop('objectid', axis=1)

    print('Number of unique MSOA zones:', len(bsq.msoaZoneID.unique()), 'should be 8480 with Scotland')

    # Fix Scotland and get the area types
    # Solution for Scotland is to get an NTEM Zone for every MSOA - use the population lookup - ie. get the one
    # with the most people. This is 1:1 in England and Wales
    largest_ntem = ntem_to_msoa.groupby('msoaZoneID')['overlap_ntem_pop_split_factor'].max().reset_index()
    ntem_to_msoa = ntem_to_msoa.merge(largest_ntem, on=['msoaZoneID', 'overlap_ntem_pop_split_factor'])
    ntem_to_msoa = ntem_to_msoa.merge(area_types, how='left', on='ntemZoneID')
    ntem_to_msoa = ntem_to_msoa[['msoaZoneID', 'R', 'B', 'Zone_Desc']]  # retain only the area type info for each zone

    # Save the zone-area type lookup to a csv file for future reference
    ntem_to_msoa.to_csv('areaTypesMSOA.csv', index=False)

    # Merge area types onto bsq
    bsq = bsq.merge(ntem_to_msoa, how='left', on='msoaZoneID')

    # Derive North East and North West bsq data by area type, used to infill Scottish values
    # TODO: review this generic north section... taking first 72 LADs makes me nervous
    unqMergedLad = bsq.reindex(['LAD_code', 'LAD_Desc'], axis=1).drop_duplicates().reset_index(drop=True)
    northUnqMergedLad = unqMergedLad.iloc[0:72]
    del unqMergedLad
    northMsoaBsq = bsq[bsq.LAD_code.isin(northUnqMergedLad.LAD_code)]
    genericNorthTypeBsq = northMsoaBsq.drop(['msoaZoneID',
                                             'merged_laZoneID',
                                             'B'], axis=1).groupby(['R',
                                                                    'Gender', 'Age',
                                                                    'property_type',
                                                                    'household_composition']).mean().reset_index()
    del northMsoaBsq

    # Identify and add the missing Scottish zones to bsq
    missing_zones = ntem_to_msoa[~ntem_to_msoa.msoaZoneID.isin(bsq.msoaZoneID)]
    missing_zones = missing_zones.merge(genericNorthTypeBsq, how='left', on='R')
    bsq = bsq[list(missing_zones)]
    bsq = bsq.append(missing_zones).reset_index(drop=True)
    print('Number of unique MSOA zones:', len(bsq.msoaZoneID.unique()), 'should be 8480 with Scotland')

    # Create and export pop_factor and land audits
    audit = bsq.groupby(['msoaZoneID']).sum().reindex(['pop_factor'], axis=1)
    audit.to_csv('msoa_pop_factor_audit.csv', index=False)
    land_audit = bsq[['msoaZoneID', 'Zone_Desc']].drop_duplicates().merge(msoaShp, how='inner',
                                                                          left_on='msoaZoneID',
                                                                          right_on='objectid').drop('objectid', axis=1)
    land_audit.to_csv('landAudit.csv', index=False)

    return bsq


def filled_properties(by_lu_obj):
    """
    This is a rough account for unoccupied properties using KS401UK at LSOA level to infer whether the properties
    have any occupants.
        by_lu_obj: base year land use object, which includes the following paths:
            zone_translation_path: correspondence between LSOAs and the zoning system (default MSOA)
            KS401path: csv file path for the census KS401 table
    """
    # Read in the census filled property data
    filled_properties_df = pd.read_csv(by_lu_obj.KS401path)
    filled_properties_df = filled_properties_df.rename(columns={
        'Dwelling Type: All categories: Household spaces; measures: Value': 'Total_Dwells',
        'Dwelling Type: Household spaces with at least one usual resident; measures: Value': 'Filled_Dwells',
        'geography code': 'geography_code'
    })
    filled_properties_df = filled_properties_df.reindex(columns=['geography_code',
                                                                 'Total_Dwells',
                                                                 'Filled_Dwells'])

    # Calculate the probability that a property is filled
    filled_properties_df['Prob_DwellsFilled'] = filled_properties_df['Filled_Dwells'] / filled_properties_df[
        'Total_Dwells']
    filled_properties_df = filled_properties_df.drop(columns={'Filled_Dwells', 'Total_Dwells'})

    # Read in the zone translation (default LSOA to MSOA)
    zone_translation = pd.read_csv(by_lu_obj.zone_translation_path)
    zone_translation = zone_translation.rename(columns={'lsoa_zone_id': 'lsoaZoneID',
                                                        'msoa_zone_id': 'msoaZoneID'})
    zone_translation = zone_translation[['lsoaZoneID', 'msoaZoneID']]

    # Merge and apply the zone translation onto the census data
    # TODO: instead of the mean, could we do sum of filled dwells and total dwells and calc the probability later on?
    filled_properties_df = filled_properties_df.rename(columns={'geography_code': 'lsoaZoneID'})
    filled_properties_df = filled_properties_df.merge(zone_translation, on='lsoaZoneID')
    filled_properties_df = filled_properties_df.drop(columns={'lsoaZoneID'})
    filled_properties_df = filled_properties_df.groupby(['msoaZoneID']).mean().reset_index()

    # The above filled properties probability is based on E+W so need to join back to Scottish MSOAs
    uk_msoa = gpd.read_file(_default_msoaRef).reindex(columns={'msoa11cd'}).rename(columns={'msoa11cd': 'msoaZoneID'})
    filled_properties_df = uk_msoa.merge(filled_properties_df, on='msoaZoneID', how='outer')
    filled_properties_df = filled_properties_df.fillna(1)  # default to all properties being occupied
    filled_properties_df.to_csv('ProbabilityDwellfilled.csv', index=False)

    return filled_properties_df


def apply_household_occupancy(do_import=False,
                              write_out=True,
                              level=_default_zone_name,
                              hops_path=_import_folder + '/HOPs/hops_growth_factors.csv'):
    """
    # Import household occupancy data and apply to property data
    # TODO: toggles for unused 'level' parameter. Want to be able to run
    # at LSOA level when point correspondence is done.
    # TODO: Folders for outputs to seperate this process from the household 
    # classification
    """

    if do_import:
        balanced_cpt_data = pd.read_csv('UKHouseHoldOccupancy2011.csv')

    else:
        # TODO: put in constants?
        EWQS401 = 'QS401UK_LSOA.csv'
        SQS401 = 'QS_401UK_DZ_2011.csv'
        EWQS402 = 'QS402UK_LSOA.csv'
        SQS402 = 'QS402UK_DZ_2011.csv'

        cpt_data = lsoa_census_data_prep(_default_census_dat, [EWQS401, SQS401], [EWQS402, SQS402],
                                         geography=_default_lsoaRef)

        # Zone up here to MSOA aggregations
        balanced_cpt_data = balance_missing_hops(cpt_data, grouping_col='msoaZoneID')
        balanced_cpt_data = balanced_cpt_data.fillna(0)
        filled_properties = pd.read_csv(_default_home_dir + '/ProbabilityDwellfilled.csv')
        balanced_cpt_data = balanced_cpt_data.merge(filled_properties, how='outer', on='msoaZoneID')
        balanced_cpt_data['household_occupancy'] = balanced_cpt_data['household_occupancy'] * balanced_cpt_data[
            'Prob_DwellsFilled']
        balanced_cpt_data = balanced_cpt_data.drop(columns={'Prob_DwellsFilled'})
        balanced_cpt_data.to_csv('UKHouseHoldOccupancy2011.csv', index=False)

    # TODO: this section largely checks and validation, refine
    msoaCols = ['objectid', 'msoa11cd']
    ladCols = ['ladZoneID', 'msoaZoneID']
    ukMSOA = gpd.read_file(_default_msoaRef)
    ukMSOA = ukMSOA.loc[:, msoaCols]

    # Visual spot checks - count zones, check cpt
    audit = balanced_cpt_data.groupby(['msoaZoneID']).count().reset_index()
    print('census hops zones =', audit['msoaZoneID'].drop_duplicates().count(), 'should be', len(ukMSOA))
    print('counts of property type by zone', audit['census_property_type'].drop_duplicates())

    # Join MSOA ids to balanced cptdata
    ukMSOA = ukMSOA.rename(columns={'msoa11cd': 'msoaZoneID'})
    balanced_cpt_data = balanced_cpt_data.merge(ukMSOA, how='left',
                                                on='msoaZoneID').drop('objectid', axis=1)
    # Import msoa to lad translation
    lad_translation = pd.read_csv(_default_zone_folder + 'Export/lad_to_msoa/lad_to_msoa.csv')
    lad_translation = lad_translation.rename(columns={'lad_zone_id': 'ladZoneID',
                                                      'msoa_zone_id': 'msoaZoneID'}).loc[:, ladCols]
    unqLad = lad_translation['ladZoneID'].unique()
    balanced_cpt_data = balanced_cpt_data.merge(lad_translation, how='left',
                                                on='msoaZoneID')

    joinLad = balanced_cpt_data['ladZoneID'].unique()

    ladCols = ['objectid', 'lad17cd']
    ukLAD = gpd.read_file(_default_ladRef)
    ukLAD = ukLAD.loc[:, ladCols]

    balanced_cpt_data = balanced_cpt_data.merge(ukLAD, how='left',
                                                left_on='ladZoneID', right_on='objectid').drop(
        ['ladZoneID', 'objectid'],
        axis=1)

    if len(joinLad) == len(unqLad):
        print('All LADs joined properly')
    else:
        print('Some LAD zones not accounted for')
    del (unqLad, joinLad)

    hops_growth = pd.read_csv(hops_path).loc[:, ['Area code', '11_to_18']]

    # using HOPS growth data to uplift the figures to 2018

    balanced_cpt_data = balanced_cpt_data.merge(hops_growth,
                                                how='left', left_on='lad17cd',
                                                right_on='Area code').drop('Area code', axis=1).reset_index(drop=True)

    balanced_cpt_data['household_occupancy_18'] = balanced_cpt_data['household_occupancy'] * \
                                                  (1 + balanced_cpt_data['11_to_18'])

    trim_cols = ['msoaZoneID', 'msoa11cd', 'census_property_type',
                 'household_occupancy_18', 'ho_type']
    balanced_cpt_data = balanced_cpt_data.reindex(trim_cols, axis=1)

    # Read in all res property for the level of aggregation
    print('Reading in AddressBase extract')
    all_res_property = pd.read_csv(_default_addressbase_extract_path)

    all_res_property = all_res_property.reindex(['ZoneID', 'census_property_type',
                                                 'UPRN'], axis=1)
    all_res_property_zonal = all_res_property.groupby(['ZoneID',
                                                       'census_property_type']).count().reset_index()
    del all_res_property

    if level == 'MSOA':

        all_res_property_zonal = all_res_property_zonal.merge(balancedCptData,
                                                              how='inner',
                                                              left_on=['ZoneID', 'census_property_type'],
                                                              right_on=['msoaZoneID', 'census_property_type'])
        all_res_property_zonal = all_res_property_zonal.drop('msoaZoneID', axis=1)

        # Audit join - ensure all zones accounted for
        if all_res_property_zonal['ZoneID'].drop_duplicates().count() != ukMSOA['msoaZoneID'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')

        # allResPropertyZonal.merge(filled_properties, on = 'ZoneID')
        all_res_property_zonal['population'] = all_res_property_zonal['UPRN'] * all_res_property_zonal[
            'household_occupancy_18']

        # Create folder for exports
        arpMsoaAudit = all_res_property_zonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'], axis=1)
        hpaFolder = 'Hops Population Audits'
        utils.create_folder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        if write_out:
            all_res_property_zonal.to_csv('classifiedResProperty' + level + '.csv', index=False)

        return all_res_property_zonal

    if level == 'LSOA':
        # here change msoa that is expected in the function above to lsoa Zone ID.
        # To do that we need the zone translations and lsoa table with zoneID objectid and also msoa lookup for the objectid!
        lsoaCols = ['objectid', 'lsoa11cd']
        # TODO: change the path for translations
        lsoa_lookup = pd.read_csv(
            'Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz_cleaned_dbf.csv')
        LSOAzonetranslation = pd.read_csv(_default_zone_folder + 'Export/msoa_to_lsoa/msoa_to_lsoa_pop_weighted.csv')
        msoa_lookup = pd.read_csv(
            'Y:/Data Strategy/GIS Shapefiles/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz_dbf.csv')

        lsoa_lookup = lsoa_lookup.rename(
            columns={
                "objectid": "lsoaZoneID"
            })

        msoa_lookup = msoa_lookup.rename(
            columns={
                "objectid": "msoaZoneID"})

        zonetrans = pd.merge(LSOAzonetranslation, lsoa_lookup, how="outer", on='lsoaZoneID')
        zonetrans = zonetrans.merge(msoa_lookup, how="outer", on='msoaZoneID')

        zonetrans = zonetrans.drop(columns={'lsoa_var', 'msoa_var',
                                            'overlap_lsoa_split_factor',
                                            'overlap_type', 'overlap_var',
                                            'lsoa11nm', 'lsoa11nmw', 'st_areasha_x',
                                            'st_lengths_x', 'msoa11nm', 'msoa11nmw',
                                            'st_areasha_y', 'st_lengths_y'})

        zonetrans = zonetrans.rename(columns={
            "lsoa11cd": "ZoneID"})
        allResPropertyZonal = pd.merge(allResPropertyZonal, zonetrans, on='ZoneID')

        allResPropertyZonal = pd.merge(allResPropertyZonal, balancedCptData, \
                                       how='inner', \
                                       left_on=['msoaZoneID', 'census_property_type'], \
                                       right_on=['msoaZoneID', 'census_property_type'])
        allResPropertyZonal = allResPropertyZonal.drop(columns=
                                                       {'lsoaZoneID', 'msoaZoneID',
                                                        'overlap_msoa_split_factor'}
                                                       )
        # TODO need to introduce lsoa cols for this below to work

        # TODO: Insert some logic here to make it work for LSOA and OA level#
        # Going to look something like assigning areas their MSOA,
        # joining on correspondence and dropping thee MSOA again.
        # Audit join - ensure all zones accounted for
        ukLSOA = gpd.read_file(_default_lsoaRef)
        ukLSOA = ukLSOA.loc[:, lsoaCols]

        if allResPropertyZonal['ZoneID'].drop_duplicates().count() != ukLSOA['lsoa11cd'].drop_duplicates().count():
            ValueError('Some zones dropped in Hops join')
        else:
            print('All Hops areas accounted for')

        allResPropertyZonal['population'] = allResPropertyZonal['UPRN'] * allResPropertyZonal['household_occupancy_18']
        # Create folder for exports
        arpMsoaAudit = allResPropertyZonal.groupby('ZoneID').sum().reset_index()
        arpMsoaAudit = arpMsoaAudit.reindex(['ZoneID', 'population'], axis=1)
        hpaFolder = 'Hops Population Audits'
        utils.create_folder(hpaFolder)
        arpMsoaAudit.to_csv(hpaFolder + '/' + level + '_population_from_2018_hops.csv', index=False)
        arpMsoaAudit = arpMsoaAudit['population'].sum()
        print(arpMsoaAudit)

        if write_out:
            allResPropertyZonal.to_csv('classifiedResPropertyLSOA.csv', index=False)

            # TODO: put checks for the following comment into the code eg print warning?
            # it has 1900 too many people

    else:
        print("no support for this zone")


def apply_ntem_segments(classified_res_property_import_path='classifiedResPropertyMSOA.csv',
                        bsq_import_path=_import_folder + 'Bespoke Census Query/formatted_long_bsq.csv',
                        areaTypeImportPath=_import_folder + 'CTripEnd/ntem_zone_area_type.csv',
                        ksEmpImportPath=_import_folder + 'KS601-3UK/uk_msoa_ks601equ_w_gender.csv',
                        level=_default_zone_name, writeSteps=False):
    """
    Function to join the bespoke census query to the classified residential
    property data
    Problem here is that there are segments with attributed population
    coming in from the bespoke census query that don't have properties to join on.
    so we need classified properties by msoa for this to work atm
    Import bespoke census query - this function creates it
    At the moment only works for MSOA
    """
    crp = pd.read_csv(classified_res_property_import_path)
    crp_cols = ['ZoneID', 'census_property_type', 'UPRN', 'household_occupancy_18', 'population']
    crp = crp.reindex(crp_cols, axis=1)
    crp['population'].sum()

    # Read in the Bespoke Census Query and create NTEM areas and employment segmentation
    bsq = create_ntem_areas(bsq_import_path, areaTypeImportPath)
    bsq = create_employment_segmentation(bsq, ksEmpImportPath)

    factor_property_type = bsq.reindex(['msoaZoneID', 'property_type', 'pop_factor'],
                                       axis=1).groupby(['msoaZoneID',
                                                        'property_type']).sum().reset_index()
    factor_property_type = factor_property_type.rename(columns={'pop_factor': 'pt_pop_factor'})
    bsq = bsq.merge(factor_property_type, how='left', on=['msoaZoneID', 'property_type'])

    bsq['pop_factor'] = bsq['pop_factor'] / bsq['pt_pop_factor']
    bsq = bsq.drop('pt_pop_factor', axis=1)

    seg_folder = 'NTEM Segmentation Audits'
    utils.create_folder(seg_folder)
    audit = bsq.reindex(['msoaZoneID', 'property_type', 'pop_factor'],
                        axis=1).groupby(['msoaZoneID',
                                         'property_type']).sum().reset_index()
    audit.to_csv(seg_folder + '/Zone_PT_Factor_Pre_Join_Audit.csv', index=False)

    print('Should be near to zones x property types - ie. 8480 x 6 = 50880 :', bsq['pop_factor'].sum())

    msoaCols = ['objectid', 'msoa11cd']
    ukMSOA = gpd.read_file(_default_msoaRef)
    ukMSOA = ukMSOA.loc[:, msoaCols]

    # Join in MSOA names and format matrix for further steps
    # TODO: Some handling required here for other zoning systems
    bsq = bsq.merge(ukMSOA, how='left', left_on='msoaZoneID', right_on='objectid')
    bsq_cols = ['msoa11cd', 'Age', 'Gender', 'employment_type', 'household_composition', 'property_type', 'R',
                'pop_factor']
    bsq = bsq.reindex(bsq_cols, axis=1)
    bsq = bsq.rename(columns={'R': 'area_type'})

    audit = bsq.reindex(['msoa11cd', 'property_type', 'pop_factor'],
                        axis=1).groupby(['msoa11cd', 'property_type']).sum().reset_index()
    audit.to_csv(seg_folder + '/Zone_PT_Factor_Audit_Inter_Join.csv', index=False)

    # TODO: record these audits more formally? Lots of examples of audits in the rest of this function
    bsq_audit = bsq.groupby(['msoa11cd', 'property_type']).count().reset_index()
    print(bsq_audit['pop_factor'].drop_duplicates())
    crp_audit = crp['population'].sum()
    print(crp_audit)

    crp = crp.merge(bsq, how='outer', left_on=['ZoneID', 'census_property_type'],
                    right_on=['msoa11cd', 'property_type'])
    print('pop factor needs to be same as no of zones - 8480')
    print('population needs to resolve back to 60+ million once duplicates are removed')
    crp_audit = crp['population'].drop_duplicates().sum()
    print(crp_audit)

    crp['pop_factor'].sum()
    crp_audit = crp['population'].drop_duplicates().sum()
    print(crp_audit)

    # Audit bank
    print(crp['population'].sum())
    print(crp['pop_factor'].sum())

    # This is where it used to fall to bits
    # Apply population factor to populations to get people by property type
    crp['people'] = crp['population'] * crp['pop_factor']

    output_cols = ['ZoneID', 'area_type', 'census_property_type', 'property_type', 'UPRN',
                   'household_composition', 'Age', 'Gender', 'employment_type', 'people']
    crp = crp.reindex(output_cols, axis=1)
    crp = crp.rename(columns={'UPRN': 'properties'})

    pop = crp['people'].sum()
    print('Final population', pop)
    crp = crp.dropna()
    print('Final population after removing nans', pop)

    crp.to_csv(_default_home_dir + '/landUseOutput' + level + '.csv', index=False)

    # Total MSOA Pop Audit
    msoa_audit = crp.reindex(['ZoneID', 'people'], axis=1).groupby('ZoneID').sum()
    msoa_audit.to_csv(seg_folder + '/2018MSOAPopulation_OutputEnd.csv', index=False)

    return crp, bsq


# TODO: normalise the gender/age?
def communal_establishments_splits():
    """
    Function to establish the proportions of communal establishment population 
    across zones and gender and age
    
    """
    print('Reading in Communal Establishments by type 2011 totals')
    communal_types = pd.read_csv(_default_communal_types_path)
    communal_types = communal_types.reindex(columns=['msoa11cd', 'Age', 'gender',
                                                     'Total_people']).rename(columns={'msoa11cd': 'msoacd',
                                                                                      'gender': 'Gender'})

    communal_types = communal_types.replace({'Gender': {'male': 'Male', 'female': 'Female'}})

    communal_types.loc[communal_types['Age'] == 'under 16', 'Gender'] = 'Children'
    communal_types['communal_total'] = communal_types.groupby(['msoacd', 'Age', 'Gender']
                                                              )['Total_people'].transform('sum')

    communal_types = communal_types.drop(columns={'Total_people'}).drop_duplicates()
    # merge with 2011 census totals per Zone per gender and age
    print('Reading in census population by age and gender 2011 totals')

    census_population = pd.read_csv(_default_census_population).replace(
        {'Gender': {'male': 'Male', 'female': 'Female'}})
    census_populationB = pd.melt(census_population, id_vars=['msoacd', 'Gender'],
                                 value_vars=['under 16', '16-74', '75 or over'
                                             ]).rename(columns={'variable': 'Age',
                                                                'value': '2011pop'})

    census_populationB.loc[census_populationB['Age'] == 'under 16', 'Gender'] = 'Children'
    census_populationB['2011pop2'] = census_populationB.groupby(['msoacd', 'Age', 'Gender']
                                                                )['2011pop'].transform('sum')

    census_population = census_populationB.drop(columns={'2011pop'}).drop_duplicates()

    print('Population in 2011 was', census_population['2011pop2'].sum())
    communal_establishments = communal_types.merge(census_population, on=['msoacd', 'Age', 'Gender'],
                                                   how='outer')
    print('Working out the communal establishment population splits per zone, age and gender for 2011')

    communal_establishments['CommunalFactor'] = (
            communal_establishments['communal_total'] / communal_establishments['2011pop2'])

    communal_establishments = communal_establishments.drop(columns={'2011pop2'})
    print('Communal establishments average split for 2011 was', communal_establishments['CommunalFactor'].mean())
    communal_establishments = communal_establishments.replace({'Gender': {'Female': 'Females'}})

    return communal_establishments


def communal_establishments_employment():
    print('Reading in Communal Establishments by employment 2011 totals')
    communal_emp = pd.read_csv(_default_communal_employment_path)
    communal_emp = pd.melt(communal_emp, id_vars=['Gender', 'Age'], value_vars=
    ['fte', 'pte', 'unm', 'stu']).rename(columns={'variable': 'employment_type',
                                                  'value': 'splits'})
    communal_emp['total'] = communal_emp.groupby(['Age', 'Gender']
                                                 )['splits'].transform('sum')
    communal_emp['emp_factor'] = communal_emp['splits'] / communal_emp['total']
    communal_emp = communal_emp.drop(columns={'splits', 'total'})

    print('Communal establishments employment splits done')
    return communal_emp


def join_establishments(level=_default_zone_name,
                        landusePath=_default_home_dir + '/landuseOutput' + _default_zone_name + '.csv'):
    areatypes = pd.read_csv(_default_area_types)
    areatypes = areatypes.drop(columns={'zone_desc'})
    areatypes = areatypes.rename(columns={'msoa_zone_id': 'ZoneID'})
    # add areatypes
    communal_establishments = communal_establishments_splits()
    communal_emp = communal_establishments_employment()

    # need to split out the NonWork Age to be able to sort out employment
    NonWorkAge = ['under 16', '75 or over']
    CommunalEstActive = communal_establishments[~communal_establishments.Age.isin(NonWorkAge)].copy()
    CommunalNonWork = communal_establishments[communal_establishments.Age.isin(NonWorkAge)].copy()
    # non_wa for employment_type for under 16 and 75 or over people
    CommunalNonWork.loc[CommunalNonWork['Age'] == 'under 16', 'employment_type'] = 'non_wa'
    CommunalNonWork.loc[CommunalNonWork['Age'] == '75 or over', 'employment_type'] = 'non_wa'
    CommunalNonWork = CommunalNonWork.rename(columns={'communal_total': 'people'}
                                             ).drop(columns={'CommunalFactor'})

    # apply employment segmentation to the active age people
    CommunalEstActive = CommunalEstActive.merge(communal_emp, on=['Age', 'Gender'],
                                                how='outer')
    CommunalEstActive['comm_people'] = CommunalEstActive['communal_total'] * CommunalEstActive['emp_factor']
    CommunalEstActive['comm_people'].sum()
    CommunalEstActive = CommunalEstActive.drop(columns={'communal_total', 'CommunalFactor', 'emp_factor'}
                                               ).rename(columns={'comm_people': 'people'})
    communal_establishments = CommunalEstActive.append(CommunalNonWork, sort=True)
    print('Communal Establishment total for 2018 should be ~1.1m and is ',
          communal_establishments['people'].sum() / 1000000)

    communal_establishments['property_type'] = 8
    communal_establishments = communal_establishments.rename(columns={'msoacd': 'ZoneID'})
    communal_establishments = communal_establishments.merge(areatypes, on='ZoneID')

    #### bring in landuse for hc ####
    landuse = pd.read_csv(landusePath)
    zones = landuse["ZoneID"].drop_duplicates().dropna()
    Ezones = zones[zones.str.startswith('E')]
    Elanduse = landuse[landuse.ZoneID.isin(Ezones)]
    Restlanduse = landuse[~landuse.ZoneID.isin(Ezones)].drop(columns={'properties', 'census_property_type'})

    # work out household composition - arguably not needed - see IW's comments 09/06/20
    HouseComp = Elanduse.groupby(by=['area_type', 'Age', 'Gender',
                                     'employment_type', 'household_composition'],
                                 as_index=False).sum()

    HouseComp['total'] = HouseComp.groupby(by=['area_type', 'Age', 'Gender',
                                               'employment_type'])['people'].transform('sum')
    HouseComp['hc'] = HouseComp['people'] / HouseComp['total']
    HouseComp = HouseComp.drop(columns={'total', 'people', 'property_type'})

    CommunalEstablishments = communal_establishments.merge(HouseComp,
                                                           on=['area_type', 'Age', 'Gender',
                                                               'employment_type'], how='outer')
    CommunalEstablishments['newpop'] = CommunalEstablishments['people'] * CommunalEstablishments['hc']
    CommunalEstablishments = CommunalEstablishments.drop(
        columns={'people', 'hc', 'properties', 'census_property_type'}).rename(columns={'newpop': 'people'})
    CommunalEstablishments['people'].sum()
    CommunalEstFolder = 'CommunalEstablishments'
    utils.create_folder(CommunalEstFolder)
    CommunalEstablishments.to_csv(CommunalEstFolder + '/' + _default_zone_name +
                                  'CommunalEstablishments2011.csv', index=False)
    CommunalEstablishments['people'].sum()
    cols = ['ZoneID', 'area_type', 'property_type', 'Age', 'Gender', 'employment_type',
            'household_composition', 'people']
    CommunalEstablishments = CommunalEstablishments.reindex(columns=cols)
    landuse = landuse.reindex(columns=cols)
    landusewComm = landuse.append(CommunalEstablishments)
    print('Joined communal communitiies. Total pop for GB is now', landusewComm['people'].sum())
    landusewComm.to_csv(_default_home_dir + '/landuseOutput' + _default_zone_name + '_withCommunal.csv', index=False)


def land_use_formatting(land_use_path=_default_home_dir + '/landuseOutput' + _default_zone_name + '_withCommunal.csv'):
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
    land_use = pd.read_csv(land_use_path)
    land_use['property_type'] = land_use['property_type'].map(consts.PROPERTY_TYPE)
    land_use = land_use.to_csv(_default_home_dir + '/landuseOutput' + _default_zone_name + '_stage3.csv', index=False)

    return land_use


def apply_ns_sec_soc_splits(land_use_path=_default_home_dir + '/landuseOutput' + _default_zone_name + '_stage3.csv'):
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

    # Read in NS-SeC table and map the house types and NS-SeC categories to the bespoke classes in lu_constants
    nssec = pd.read_csv(_nssecPath)
    nssec['property_type'] = nssec['house_type'].map(consts.HOUSE_TYPE)
    nssec['ns_sec'] = nssec['NS_SeC'].map(consts.NS_SEC)
    nssec = nssec.drop(columns={'house_type', 'NS_SeC'}).rename(columns={'MSOA name': 'ZoneID'})

    # all economically active in one group
    nssec = nssec.rename(columns={'Economically active FT 1-3': 'FT higher',
                                  'Economically active FT 4-7': 'FT medium',
                                  'Economically active FT 8-9': 'FT skilled',
                                  'Economically active PT 1-3': 'PT higher',
                                  'Economically active PT 4-7': 'PT medium',
                                  'Economically active PT 8-9': 'PT skilled',
                                  'Economically active unemployed': 'unm',
                                  'Economically inactive': 'children',
                                  'Economically inactive retired': '75 or over',
                                  'Full-time students': 'stu'}).drop(columns={'TfN area type'})

    # rename columns and melt it down
    nssec = nssec.rename(columns={'msoa_name': 'ZoneID'})
    nssec_melt = pd.melt(nssec, id_vars=['ZoneID', 'property_type', 'ns_sec'],
                         value_vars=['FT higher', 'FT medium', 'FT skilled',
                                     'PT medium', 'PT skilled',
                                     'PT higher', 'unm', 'children', '75 or over', 'stu'])
    nssec_melt = nssec_melt.rename(columns={'variable': 'employment_type',
                                            'value': 'numbers'})
    # map out categories to match the landuse format
    nssec_melt['SOC_category'] = nssec_melt['employment_type']
    nssec_melt['Age'] = '16-74'
    nssec_melt.loc[nssec_melt['employment_type'] == 'children', 'Age'] = 'under 16'
    nssec_melt.loc[nssec_melt['employment_type'] == '75 or over', 'Age'] = '75 or over'
    nssec_melt = nssec_melt.replace({'employment_type': {'FT higher': 'fte',
                                                         'FT medium': 'fte',
                                                         'FT skilled': 'fte',
                                                         'PT higher': 'pte',
                                                         'PT skilled': 'pte',
                                                         'PT medium': 'pte',
                                                         'children': 'non_wa',
                                                         '75 or over': 'non_wa'
                                                         }})
    nssec_melt = nssec_melt.replace({'SOC_category': {'FT higher': '1',
                                                      'FT medium': '2',
                                                      'FT skilled': '3',
                                                      'PT higher': '1',
                                                      'PT medium': '2',
                                                      'PT skilled': '3',
                                                      'unm': 'NA',
                                                      '75 or over': 'NA',
                                                      'children': 'NA'}})
    # split the nssec into inactive and active
    nssec_formatted = nssec_melt.to_csv(_default_home_dir + '/NSSECformatted' + _default_zone_name + '.csv',
                                        index=False)
    inactive = ['stu', 'non_wa']
    InactiveNSSECPot = nssec_melt[nssec_melt.employment_type.isin(inactive)].copy()
    ActiveNSSECPot = nssec_melt[~nssec_melt.employment_type.isin(inactive)].copy()

    del (nssec_melt, nssec)

    #### Active Splits ####
    areatypes = pd.read_csv(_default_area_types).rename(columns={
        'msoa_zone_id': 'ZoneID'}).drop(columns={'zone_desc'})
    ActiveNSSECPot = ActiveNSSECPot.merge(areatypes, on='ZoneID')
    # MSOAActiveNSSECSplits for Zones
    MSOAActiveNSSECSplits = ActiveNSSECPot.copy()
    MSOAActiveNSSECSplits['totals'] = MSOAActiveNSSECSplits.groupby(['ZoneID', 'property_type',
                                                                     'employment_type'])['numbers'].transform('sum')
    MSOAActiveNSSECSplits['empsplits'] = MSOAActiveNSSECSplits['numbers'] / MSOAActiveNSSECSplits['totals']
    # For Scotland
    GlobalActiveNSSECSplits = ActiveNSSECPot.copy()
    GlobalActiveNSSECSplits = ActiveNSSECPot.groupby(['area_type', 'property_type',
                                                      'employment_type', 'Age',
                                                      'ns_sec', 'SOC_category'],
                                                     as_index=False).sum()
    GlobalActiveNSSECSplits['totals'] = GlobalActiveNSSECSplits.groupby(['area_type', 'property_type',
                                                                         'Age', 'employment_type'])[
        'numbers'].transform('sum')
    GlobalActiveNSSECSplits['global_splits'] = GlobalActiveNSSECSplits['numbers'] / GlobalActiveNSSECSplits['totals']

    GlobalActiveNSSECSplits = GlobalActiveNSSECSplits.drop(columns={'numbers', 'totals'})
    # for communal establishments
    AverageActiveNSSECSplits = ActiveNSSECPot.copy()

    AverageActiveNSSECSplits = AverageActiveNSSECSplits.groupby(by=['area_type',
                                                                    'employment_type',
                                                                    'Age', 'ns_sec'],
                                                                as_index=False).sum()
    AverageActiveNSSECSplits['totals2'] = AverageActiveNSSECSplits.groupby(['area_type',
                                                                            'Age', 'employment_type'])[
        'numbers'].transform('sum')
    AverageActiveNSSECSplits['average_splits'] = AverageActiveNSSECSplits['numbers'] / AverageActiveNSSECSplits[
        'totals2']
    AverageActiveNSSECSplits['SOC_category'] = 'NA'
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
                                                                         'Age', 'employment_type'])[
        'numbers'].transform('sum')
    MSOAInactiveNSSECSplits['msoa_splits'] = MSOAInactiveNSSECSplits['numbers'] / MSOAInactiveNSSECSplits['totals']
    MSOAInactiveNSSECSplits['SOC_category'] = 'NA'
    MSOAInactiveNSSECSplits = MSOAInactiveNSSECSplits.drop(columns={'totals', 'numbers'})
    MSOAInactiveNSSECSplits['SOC_category'] = 'NA'
    # For Scotland
    GlobalInactiveNSSECSplits = InactiveNSSECPot.copy()
    GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.groupby(by=['area_type',
                                                                      'property_type',
                                                                      'employment_type',
                                                                      'Age', 'ns_sec'],
                                                                  as_index=False).sum()
    GlobalInactiveNSSECSplits['totals2'] = GlobalInactiveNSSECSplits.groupby(['area_type', 'property_type',
                                                                              'Age', 'employment_type'])[
        'numbers'].transform('sum')
    GlobalInactiveNSSECSplits['global_splits'] = GlobalInactiveNSSECSplits['numbers'] / GlobalInactiveNSSECSplits[
        'totals2']
    GlobalInactiveNSSECSplits['SOC_category'] = 'NA'
    GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.drop(columns={'totals2', 'numbers'})
    # for communal establishments
    AverageInactiveNSSECSplits = InactiveNSSECPot.copy()
    AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.groupby(by=['area_type',
                                                                        'employment_type',
                                                                        'Age', 'ns_sec'],
                                                                    as_index=False).sum()
    AverageInactiveNSSECSplits['totals2'] = AverageInactiveNSSECSplits.groupby(['area_type',
                                                                                'Age', 'employment_type'])[
        'numbers'].transform('sum')
    AverageInactiveNSSECSplits['average_splits'] = AverageInactiveNSSECSplits['numbers'] / AverageInactiveNSSECSplits[
        'totals2']
    AverageInactiveNSSECSplits['SOC_category'] = 'NA'
    AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.drop(columns={'totals2', 'numbers', 'property_type'})

    InactiveSplits = MSOAInactiveNSSECSplits.merge(GlobalInactiveNSSECSplits,
                                                   on=['area_type', 'property_type',
                                                       'employment_type', 'Age',
                                                       'SOC_category', 'ns_sec'], how='right')

    InactiveSplits = InactiveSplits.merge(AverageInactiveNSSECSplits,
                                          on=['area_type',
                                              'employment_type', 'SOC_category', 'ns_sec', 'Age'
                                              ], how='right')
    # check where there's no splitting factors, use the zone average for age?
    InactiveNSSECPot['SOC_category'] = 'NA'
    InactiveSplits['splits2'] = InactiveSplits['msoa_splits']
    InactiveSplits['splits2'] = InactiveSplits['splits2'].fillna(InactiveSplits['global_splits'])
    InactiveSplits['splits2'] = InactiveSplits['splits2'].fillna(InactiveSplits['average_splits'])
    InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['msoa_splits'], 'type'] = 'msoa_splits'
    InactiveSplits.loc[(InactiveSplits['splits2'] == InactiveSplits['global_splits']), 'type'] = 'global_splits'
    InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['average_splits'], 'type'] = 'average_splits'
    # InactiveSplits = InactiveSplits.drop(columns={'numbers_x','numbers_y', 'totals2_x', 'totals2_y', 'msoa_splits'})
    # InactiveSplits = InactiveSplits.drop(columns={'average_splits', 'global_splits'})
    InactiveSplits = InactiveSplits.drop(columns={'msoa_splits', 'global_splits'})
    InactiveSplitsAudit = InactiveSplits.groupby(by=['ZoneID', 'property_type', 'Age']).sum()

    ###### APPLICATION OF SPLITS Apply EW splits ####
    # England and Wales - applies splits for inactive people
    # apply splits
    landuse = pd.read_csv(landusePath)
    Inactive_categs = ['stu', 'non_wa']
    # landuse = pd.read_csv(_default_home_dir+'/landuseOutput'+_default_zone_name+'_stage3.csv')
    ActivePot = landuse[~landuse.employment_type.isin(Inactive_categs)].copy()
    InactivePot = landuse[landuse.employment_type.isin(Inactive_categs)].copy()

    # take out Scottish MSOAs from this pot - nssec/soc data is only for E+W
    # Scotland will be calculated based on area type
    areatypes = pd.read_csv(_default_area_types).drop(columns={'zone_desc'}
                                                      ).rename(columns={'msoa_zone_id': 'ZoneID'})
    ZoneIDs = ActivePot['ZoneID'].drop_duplicates().dropna()
    Scott = ZoneIDs[ZoneIDs.str.startswith('S')]
    ActiveScot = ActivePot[ActivePot.ZoneID.isin(Scott)].copy()
    ActiveScot = ActiveScot.drop(columns={'area_type'})
    ActiveScot = ActiveScot.merge(areatypes, on='ZoneID')
    # ActiveEng['people'].sum()
    ActiveEng = ActivePot[~ActivePot.ZoneID.isin(Scott)].copy()
    InactiveScot = InactivePot[InactivePot.ZoneID.isin(Scott)].copy()
    InactiveScot = InactiveScot.drop(columns={'area_type'})
    InactiveScot = InactiveScot.merge(areatypes, on='ZoneID')
    InactiveEng = InactivePot[~InactivePot.ZoneID.isin(Scott)].copy()
    print("total number of economically active people in the E+W landuse" +
          "pot should be around 41m and is ", ActiveEng['people'].sum() / 1000000)

    CommunalEstablishments = [8]
    CommunalInactive = InactiveEng[InactiveEng.property_type.isin(CommunalEstablishments)].copy()
    CommunalInactive['people'].sum()
    InactiveNotCommunal = InactiveEng[~InactiveEng.property_type.isin(CommunalEstablishments)].copy()
    Inactive_Eng = InactiveSplits.merge(InactiveNotCommunal, on=['ZoneID', 'area_type',
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'],
                                        how='right')
    Inactive_Eng['newpop'] = Inactive_Eng['people'].values * Inactive_Eng['splits2'].values

    CommunalInactive = CommunalInactive.merge(AverageInactiveNSSECSplits, on=['area_type',
                                                                              'employment_type', 'Age'
                                                                              ], how='left')
    CommunalInactive['newpop'] = CommunalInactive['people'] * CommunalInactive['average_splits']
    print("Communal Inactive should be about 600k and is ", CommunalInactive['newpop'].sum())

    #### Apply Scottish Inactive ####

    # Scotland - applies splits for inactive people
    InactiveScot['people'].sum()
    InactiveScotland = GlobalInactiveNSSECSplits.merge(InactiveScot, on=['area_type',
                                                                         'property_type',
                                                                         'Age',
                                                                         'employment_type'],
                                                       how='right')
    InactiveScotland['newpop'] = InactiveScotland['people'].values * InactiveScotland['global_splits'].values
    InactiveScotland['newpop'].sum()

    #### Apply EW active splits ####
    # England and Wales - applies splits for active people
    CommunalEstablishments = [8]
    CommunalActive = ActiveEng[ActiveEng.property_type.isin(CommunalEstablishments)].copy()
    ActiveNotCommunal = ActiveEng[~ActiveEng.property_type.isin(CommunalEstablishments)].copy()
    Active_emp = ActiveNotCommunal.merge(MSOAActiveNSSECSplits, on=['ZoneID', 'Age',
                                                                    'property_type', 'employment_type'
                                                                    ], how='outer')
    # apply the employment splits for ActivePot to work out population
    Active_emp.groupby('employment_type')
    Active_emp['newpop'] = Active_emp['people'] * Active_emp['empsplits']

    Active_emp = Active_emp.drop(columns={'area_type_x', 'area_type_y'})
    Active_emp = Active_emp.merge(areatypes, on='ZoneID')
    # TODO: hard-coded values. If correct, put them in landuse_constants?
    # TODO: possibly total active population, check if that looks right. Most likely scrap
    if Active_emp['people'].sum() < (40.6 * 0.01):
        print('something has gone wrong with splits')
    else:
        print('EWsplits has worked fine')
    CommunalActive = CommunalActive.merge(AverageActiveNSSECSplits, on=['area_type',
                                                                        'employment_type', 'Age'
                                                                        ], how='left')
    CommunalActive['newpop'] = CommunalActive['people'] * CommunalActive['average_splits']

    # apply error catcher here if it's within 10% then accept

    #### Apply Scottish Active splits ####
    ActiveScotland = GlobalActiveNSSECSplits.merge(ActiveScot, on=['area_type',
                                                                   'property_type',
                                                                   'Age',
                                                                   'employment_type'],
                                                   how='right')
    ActiveScotland['newpop'] = ActiveScotland['people'].values * ActiveScotland['global_splits'].values
    ActiveScotland['newpop'].sum()

    #### AppendAllGroups ####

    NPRSegments = ['ZoneID', 'area_type', 'property_type', 'Age', 'Gender', 'employment_type',
                   'ns_sec', 'household_composition', 'SOC_category', 'newpop']
    CommunalInactive = CommunalInactive.reindex(columns=NPRSegments)
    Inactive_Eng = Inactive_Eng.reindex(columns=NPRSegments)
    ActiveScotland = ActiveScotland.reindex(columns=NPRSegments)
    CommunalActive = CommunalActive.reindex(columns=NPRSegments)
    Active_emp = Active_emp.reindex(columns=NPRSegments)
    InactiveScotland = InactiveScotland.reindex(columns=NPRSegments)

    All = CommunalInactive.append(Inactive_Eng).append(CommunalActive).append(Active_emp)
    All = All.append(InactiveScotland).append(ActiveScotland)
    All = All.rename(columns={'newpop': 'people'})
    All['SOC_category'] = All['SOC_category'].fillna(0)
    All.to_csv(_default_home_dir + '/landuseOutput' + _default_zone_name + '_NS_SEC_SOC.csv', index=False)
    print(All['people'].sum())


# TODO: remove once in run_by_lu
def run_main_build(abp_import=True):
    """
    Set ABPImport to True if you want to copy over the ABP files to iter folder
    """
    # Make a new sub folder of the home directory for the iteration, if needed, and set this as the working directory.
    os.chdir(_default_home)
    utils.create_folder(_default_iter, ch_dir=True)

    if abp_import:
        copy_addressbase_files()
    else:
        filled_properties()
        apply_household_occupancy()
        apply_ntem_segments()
        join_establishments()
        land_use_formatting()
        apply_ns_sec_soc_splits()
