# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:10:36 2019
@author: mags15
Updated: 14/02/2020 using @author: sclayton' uplift to 2018 built for NELUM's work
more info: Y:/NorMITs Land Use/import/NPR Segmentation/DataPrepProcess.doc
Uplift steps: 
# Code takes SOC data sectored by industry sectors and by MSOA and factors 
# total number of employees in MSOA according to ONS employment data by 
# Steps are:
#   1. Read in 'employees' data (at LAD and County level for 2017)
#   2. Read in 'people in work' data for 2018, to be used for overall control
#   3. Read in HSL MSOA data for 2018, which has spatial detail but not right total
#   4. Scale up 2017 LAD and County level employees data to 2018 people-in-work control
#   5. Group HSL MSOA level data to LAD/County where appropriate and calculate new scaling factors
#   6. Apply scaling factors at an MSOA level

"""
"""
For future years:
    1. combine all SIC to SOC splits into 1 for all regions
    2. shorten the SOCs
    3. get the absolute values for emp fy from nelum processed figures 
    4. use the attractions divided by sic to work out the sic distribution in the fy
    5. use the translation from sic to soc to work out the distribution of the workers across soc categories
    6. use qualifications to soc to work out the fy control for soc (north level)
    7. balance out the figures    
    8. balance out unemployed and children, take the fy population and 
    substitute the working population then either use 2018 splits for children/over75 or
    use NPIER age splits to work out how many elderly and children there will be in the future
    9. use soc to NS-SEC to uplift NS-SEC figures for fy
    """

import os
import sys

import pandas as pd
from functools import reduce

_UTILS_GIT = ('C:/Users/' +
              os.getlogin() +
              '/Desktop/NorMITs-Demand/normits_demand/utils')

_LU_UTILS= ('C:/Users/' +
              os.getlogin() +
              '/Desktop/Land-Use/land-use/utils')
sys.path.append(_UTILS_GIT)
import utils as nup
sys.path.append(_LU_UTILS)
import utils as fyu

# Default regional splits path. Soure: HSL. SIC categories are differnt for HSL and CE.
_regional_splits_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/'
_default_sic_2digit_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/processed data/UK_SICtoSOC_2018_CEcategories_uplifted.csv'
_sic_to_soc = 'Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/SIC_to_SOC_2020.xlsx'
_sic_regional_2019 = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/'

_sic_fy_path =  'Y:/NorMITs Demand/import - test/scenarios/NPIER/North_summary_040920_formatted.csv'
_soc_qual_path = 'Y:/NorMITs Demand/import - test/scenarios/ONS NFQ quals by occ_raw - TfN.csv'
_npierqualifications = 'Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/NPIER fy quals.xlsx'
_qualstosoctranslation = 'Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/ONS NFQ quals by occ_raw - TfN_formatted.xlsx'
_sic_lookup = 'Y:/NorMITs Land Use/import/NPR Segmentation/processed data/NPIER_SIC_to_CE.csv'
_import_folder = 'Y:/NorMITs Demand/import - test/scenarios/'
sc01_jam = _import_folder + 'SC01_JAM/employment/future_growth_values.csv'
sc02_pp = _import_folder +'SC02_PP/employment/future_growth_values.csv'
sc03_dd = _import_folder + 'SC03_DD/employment/future_growth_values.csv'
sc04_uzc = _import_folder + 'SC04_UZC/employment/future_growth_values.csv'
# read in data for the uplift
ons_employees = pd.ExcelFile('Employees by LAD - table52017p.xlsx') # ONS data in thousands of employees to scale to - use 2017 as 2018 data is provisional
ons_people_in_work = pd.ExcelFile('lfs_people_in_employment_2018.xlsx')
employees_socPath = (defaultHomeDir +output+'/UK_SICtoSOC_HSL.csv') # SICtoSOC output split into 12 TfN sectors
area_code_lookup = pd.read_csv('MSOA_LAD_APR19.csv') # lookup msoa, LA, county, _APR19 changes E06000028 and E06000029 to E06000058

# TODO: Standard col format names for input

def get_emp_values_fy(scenario):
    """
    Reads in processed emp figures for fy for a given scenario
    Parameters
    ----------
    scenario_emp:
        Path to csv employment values 

    Returns
    ----------
       Reads in the processed emp values for fy for a given scenario
    """
    if scenario == sc01_jam:
        scenario_emp = pd.read_csv(sc01_jam)
    elif scenario == sc02_pp:
        scenario_emp = pd.read_csv(sc02_pp)
    elif scenario == sc03_dd:
        scenario_emp = pd.read_csv(sc03_dd)
    elif scenario == sc04_uzc:
        scenario_emp = pd.read_csv(sc04_uzc)
    
    
    scenario_emp = scenario_emp.groupby(by=['msoa_zone_id'], as_index = False).sum().drop(columns={'soc', '% of MSOA_people_sum', 'MSOA people sum'})
    return(scenario_emp)
    
def combine_splits_fy(target_folder = _sic_to_soc):
    """
    Reads in all the sic to soc translators split in Excel file by region (for fy)
    Parameters
    ----------
    _sic_to_soc:
        Path to Excel sic to soc translations

    Returns
    ----------
       Combines all regional splits into one DataFrame    
    """
    
    df_sheet_map = pd.read_excel(target_folder, sheetname=None)
    sicsoc = pd.concat(df_sheet_map, axis=0, ignore_index=True)
    sicsoc = sicsoc.drop(columns={2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018})
    sicsoc['SIC'] = sicsoc['SIC'].str[:1]
    sicsoc['SOC'] = sicsoc['SOC'].str[:2]
   
    return(sicsoc)   
    
def classify_soc(soc_value):
    """
    SOC 1-3: high
    SOC 4-7: skilled
    SOC 8-9: Low skilled
    Classifies socs as per above rule
   
    Parameters
    ----------
    soc_value:       

    Returns
    ----------
       Shortened classification of SOC skill level
    """
  
    if soc_value < 40:
        soc_class = 'high'
    elif soc_value >= 40 and soc_value <80:
        soc_class = 'medium'
    elif soc_value >= 80 and soc_value <100:
        soc_class = 'skilled'
    else:
        soc_class = None
    return(soc_class)
         
def sic_splits_regions(scenario):
    # the values in the spreadsheets are in thousands but since we're deriving splits, it doesn't matter
    if scenario == sc01_jam or scenario == sc02_pp:
        sic_npier_bau = pd.read_excel('Y:/NorMITs Demand/import - test/scenarios/NPIER/North_summary_040920_v2_sic_formatted.xlsx', 
                                      sheetname = 'BAU - North econ')
        sic_npier_bau = sic_npier_bau.groupby(by=['SIC'], as_index = False).sum()

        bau = []
    
        for year in range(int(2019), int(2051)):
            npier = sic_npier_bau[[(year), 'SIC']]
            npier['total'] = npier[year].sum()
            npier['splits'] = npier[year]/npier['total']
            npier = npier.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            npier = npier[['SIC',(year)]]
            bau.append(npier)
        sic_fy = reduce(lambda x, npier : pd.merge(x, npier, on = 'SIC'), bau)
        return(sic_fy)
        sic_fy.to_csv('E:/NorMITs_Export/sic_bau_splits.csv')
    
    elif scenario == sc03_dd or scenario == sc04_uzc:
        sic_npier_tra = pd.read_excel('Y:/NorMITs Demand/import - test/scenarios/NPIER/North_summary_040920_v2_sic_formatted.xlsx', 
                                      sheetname = 'TRA - North econ')
        sic_npier_tra = sic_npier_tra.groupby(by=['SIC'], as_index = False).sum()
    
        tra= []
    
        for year in range(int(2019), int(2051)):
            npier = sic_npier_tra[[(year), 'SIC']]
            npier['total'] = npier[year].sum()
            npier['splits'] = npier[year]/npier['total']
            npier = npier.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            npier = npier[['SIC',(year)]]
            tra.append(npier)
        sic_fy = reduce(lambda x, npier : pd.merge(x, npier, on = 'SIC'), tra)
        return(sic_fy)

def work_out_2018_splits():
    landuse = pd.read_csv('Y:/NorMITs Land Use/iter3b/outputs/land_use_output_msoa.csv')
    landuse_emp = landuse.groupby(by=['soc_cat', 'msoa_zone_id'], as_index = False).sum()
    msoa_lookup = pd.read_csv('Y:/NorMITs Land Use/import/MSOAtoRGNlookup.csv').rename(columns={'MSOA11CD':'msoa_zone_id'})
    msoa_lookup = msoa_lookup[['msoa_zone_id', 'RGN11NM']]
    msoa_lookup= msoa_lookup.rename(columns={'RGN11NM':'Region'})


    landuse_emp = landuse_emp.merge(msoa_lookup, on = 'msoa_zone_id')
    North = ['North West', 'North East', 'Yorkshire and The Humber', 'East Midlands']

    landuse_emp = landuse_emp[landuse_emp.Region.isin(North)]
    landuse_splits = landuse_emp.groupby(by=['soc_cat'], as_index = False).sum()
    
def derive_soc_fy(scenario):
    # TODO: regions outside North
    scenario_emp = get_emp_values_fy(scenario) 
       
    scenario_emp = scenario_emp.groupby(by=['msoa_zone_id'], as_index = False).sum()
    # need north lookup for MSOAs
    msoa_lookup = pd.read_csv('Y:/NorMITs Land Use/import/MSOAtoRGNlookup.csv').rename(columns={'MSOA11CD':'msoa_zone_id'})
    msoa_lookup = msoa_lookup[['msoa_zone_id', 'RGN11NM']]
    msoa_lookup= msoa_lookup.rename(columns={'RGN11NM':'Region'})

    North = ['North West', 'North East', 'Yorkshire and The Humber', 'East Midlands']
    scenario_emp = scenario_emp.merge(msoa_lookup, on = 'msoa_zone_id')
    scenario_emp = scenario_emp[scenario_emp.Region.isin(North)]
    scenario_emp_north = scenario_emp.groupby(by=['Region'], as_index = False).sum().drop(columns={'2018'})
    scenario_emp_north['North'] = 1
    # for regions outside assume 2018 split? unless there's another split for regions outside
    # split using the same splits for SIC for every region
    
    if scenario == sc01_jam or scenario == sc02_pp:
        sic_bau_fy = sic_splits_regions(scenario)
        sic_bau_fy = sic_bau_fy.add_prefix('splits_').rename(columns={'splits_SIC':'sic'})
        sic_bau_fy['North']=1
    
        # add prefix to each column in one of the dataframes to make it easier before joining
        fy_scenario = sic_bau_fy.merge(scenario_emp_north, on = 'North')
        years = ['2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028',
                 '2029', '2030', '2031', '2032', '2033', '2034', '2035', '2036', '2037', '2038',
                 '2039', '2040', '2041', '2042', '2043', '2044', '2045', '2046', '2047', '2048', 
                 '2049', '2050']
        for year in years:
            fy_scenario['jobs_'+(year)] = fy_scenario[(year)]*fy_scenario['splits_'+(year)]
            fy_scenario = fy_scenario.drop(columns={(year), 'splits_'+(year)})
        
    elif scenario == sc03_dd or scenario == sc04_uzc:
        sic_tra_fy = sic_splits_regions(scenario)
        sic_tra_fy = sic_tra_fy.add_prefix('splits_').rename(columns={'splits_SIC':'sic'})
        sic_tra_fy['North']=1
    
        # add prefix to each column in one of the dataframes to make it easier before joining
        fy_scenario = sic_tra_fy.merge(scenario_emp_north, on = 'North')
        years = ['2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028',
                 '2029', '2030', '2031', '2032', '2033', '2034', '2035', '2036', '2037', '2038',
                 '2039', '2040', '2041', '2042', '2043', '2044', '2045', '2046', '2047', '2048', 
                 '2049', '2050']
        for year in years:
            fy_scenario['jobs_'+(year)] = fy_scenario[(year)]*fy_scenario['splits_'+(year)]
            fy_scenario = fy_scenario.drop(columns={(year), 'splits_'+(year)})
    
    regional_splits = combine_splits_fy()
    regional_splits['SOC'] = pd.to_numeric(regional_splits['SOC'])
    regional_splits['soc_class'] = regional_splits['SOC'].apply(classify_soc)

    regional_splits = regional_splits.groupby(by =['Region', 'soc_class', 'SIC'], as_index = False).sum().drop(columns={'SOC'})
    regional_splits = regional_splits.replace('EM','East Midlands')
    regional_splits = regional_splits.replace('YH','Yorkshire and The Humber')
    regional_splits = regional_splits.replace('NW','North West')
    regional_splits = regional_splits.replace('NE','North East')
    regional_splits = regional_splits.replace('high','higher')
    regional_splits = regional_splits.rename(columns={'SIC':'sic'})
    
    fy_scenario_soc = fy_scenario.merge(regional_splits, on = ['sic', 'Region']) 
    
    for x in range(int(2019), int(2051)):
        fy_scenario_soc['emp_'+str(x)] = fy_scenario_soc[(x)]*fy_scenario_soc['jobs_'+str(x)]
        fy_scenario_soc = fy_scenario_soc.drop(columns={(x), 'jobs_'+str(x)})
   
    soc_fy = fy_scenario_soc.groupby(by=['Region', 'soc_class'], as_index = False).sum().drop(columns={'North'})
    #soc_fy.to_csv('E:/NorMITS_Export/sc01_jam_socs.csv')
    
    
    soc_fy['North'] = 1
    soc_fy_north = soc_fy.groupby(by = ['North', 'soc_class'], as_index = False).sum()
    # do a loop for all years
    soc_fy_north = soc_fy_north[['North', 'soc_class', str(year)]]
    soc_fy_north_2019['total_2019'] = soc_fy_north_2019['emp_2019'].sum() 
    soc_fy_north_2019['splits'] = soc_fy_north_2019['emp_2019']/soc_fy_north_2019['total_2019']

    return(soc_fy)  # here's the 3 socs by region for every year in the future   
    
def compare_w_mype_soc_splits():
# placeholder for comparison between the fy_splits from SIC and landuse 2018 splits
# work out factor to multiply by        
    
def workout_northern_soc_splits():
    soc_fy['North'] = 1
    soc_fy_north = soc_fy.groupby(by = ['North', 'soc_class'], as_index = False).sum()
    soc_fy_north_2019 = soc_fy_north[['North', 'soc_class', 'emp_2019']]
    soc_fy_north_2019['total_2019'] = soc_fy_north_2019['emp_2019'].sum() 
    soc_fy_north_2019['splits'] = soc_fy_north_2019['emp_2019']/soc_fy_north_2019['total_2019']
                           
def format_attractions():
    # get a 
    
def quals_to_soc(scenario):
    # this will be a control for SOC splits in the North
    # based on qualifications for fy from NPIER 
    # then relies on ONS historical data on qualifications to SOC translator
    if scenario == sc01_jam or scenario == sc02_pp:
        npier_quals = pd.read_excel(_npierqualifications, Sheet = 'BAU - North demog')
        quals_to_soc = pd.read_excel(_qualstosoctranslation)
        
        # BAU scenario       
        fy_soc = npier_quals.merge(quals_to_soc, on = 'Labour force by highest qualification')
        g = []
        for year in range(int(2019), int(2051)):
            
            fy_quals = fy_soc[['Labour force by highest qualification', year]]
            fy_quals = fy_quals.merge(quals_to_soc, on = 'Labour force by highest qualification')
       
            fy_quals['Highly_skilled'] = fy_quals[year]*fy_quals['High'] 
            fy_quals['Medium_skilled'] = fy_quals[year]*fy_quals['Medium']
            fy_quals['Skilled_'] = fy_quals[year]*fy_quals['Skilled']
            
            # work out totals for each column
            summarise = fy_quals[['Highly_skilled', 'Medium_skilled', 'Skilled_']].sum()
            summarise = summarise.to_frame()
            summarise.reset_index(inplace=True)
            summarise = summarise.rename(columns={'index':'soc', 0:year})
            summarise['total'] = summarise[(year)].sum()
            summarise['splits'] = summarise[(year)]/summarise['total']
            summarise = summarise.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            g.append(summarise)
        
        soc_bau_fy = reduce(lambda x, summarise : pd.merge(x, summarise, on = 'soc'), g)
        soc_bau_fy = soc_bau_fy.rename(columns={'Skilled_':'Skilled'})
        soc_bau_fy.to_csv('Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/fy_soc_splits/soc_bau_splits.csv')

    if scenario == sc03_dd or scenario == sc04_uzc:
        npier_quals = pd.read_excel(_npierqualifications, Sheet = 'TRA - North demog')
        
        fy_soc = npier_quals.merge(quals_to_soc, on = 'Labour force by highest qualification')
        g = []
        for year in range(int(2019), int(2051)):
            
            fy_quals = fy_soc[['Labour force by highest qualification', year]]
            fy_quals = fy_quals.merge(quals_to_soc, on = 'Labour force by highest qualification')
       
            fy_quals['Highly_skilled'] = fy_quals[year]*fy_quals['High'] 
            fy_quals['Medium_skilled'] = fy_quals[year]*fy_quals['Medium']
            fy_quals['Skilled_'] = fy_quals[year]*fy_quals['Skilled']
            
            # work out totals for each column
            summarise = fy_quals[['Highly_skilled', 'Medium_skilled', 'Skilled_']].sum()
            summarise = summarise.to_frame()
            summarise.reset_index(inplace=True)
            summarise = summarise.rename(columns={'index':'soc', 0:year})
            summarise['total'] = summarise[(year)].sum()
            summarise['splits'] = summarise[(year)]/summarise['total']
            summarise = summarise.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            g.append(summarise)
        
        soc_tra_fy = reduce(lambda x, summarise : pd.merge(x, summarise, on = 'soc'), g)
    
        soc_tra_fy = soc_tra_fy.rename(columns={'Skilled_':'Skilled'})
        soc_tra_fy.to_csv('Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/fy_soc_splits/soc_tra_splits.csv')
    
                          
def build_sic_to_soc(balance_soc_factors = True,
                     output = 'emp',
                     sic_2digit_path = _default_sic_2digit_path,
                     defaultHomeDir = 'C:/output',
                     write = False):

    nup.set_wd('C:/NorMITs_Export', iteration=output)

    # File paths
    # MSOA divisions HSL data
    # TODO: Already contains a regional split - should be done 'live' with a path to a replaceable data source
    msoa_sic = pd.read_csv(sic_2digit_path)

    # Get region splits    
    regional_splits = combine_regional_splits()

    # Classify SOCs
    # TODO: Again - replace placeholders in function
    regional_splits['soc_class'] = regional_splits['soc_cat'].apply(classify_soc)

    # Reindex and group above SOC - (drop SOC)
    regional_splits = regional_splits.reindex(['RGN11nm', 'jobs', 'CE_SIC', 'soc_class'], axis=1)

    regional_splits = regional_splits.groupby(['RGN11nm', 'CE_SIC', 'soc_class']).sum().reset_index()

    if balance_soc_factors:
        tot = regional_splits.reindex(
                ['RGN11nm', 'CE_SIC', 'jobs'], axis=1).groupby(
                        ['RGN11nm', 'CE_SIC']).sum().reset_index()

        tot['adj'] = 1+(1-tot['jobs'])
        del(tot['jobs'])
        regional_splits = regional_splits.merge(tot, how = 'left',
                                                on = ['RGN11nm', 'CE_SIC'])
        regional_splits['jobs'] = regional_splits['jobs'] * regional_splits['adj']
        del(regional_splits['adj'])

    # Some Nan segments here - might be normal
    sic_trans = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/CE Industry categories.csv')

    # Melt the HSL MSOA division table
    sic_columns = list(msoa_sic)
    sic_columns = [x for x in sic_columns if 'Wrkrs' in x]
    del(msoa_sic['RGN11cd'])
    msoatrans = pd.melt(msoa_sic, id_vars = ['MSOA', 'RGN11nm'],
                        value_vars = sic_columns,
                        var_name = 'HSL_SIC',
                        value_name = 'total')
    print('Total jobs: ' + str(msoatrans['total'].sum()))

    # TODO: Is this a function?

    # Get sic categories ready to join
    sic_trans = sic_trans.drop(columns = {'CE_SIC_categories'})
    # Seriously though, what problem does it have with 97 & 98?

    msoatrans2 = msoatrans.merge(sic_trans, on = 'HSL_SIC', how = 'outer')
    msoatrans2 = msoatrans2.rename(columns={'CE_cat':'CE_SIC'})

    socs = msoatrans2.merge(regional_splits, on = ['RGN11nm', 'CE_SIC'], how = 'outer')

    socs['seg_jobs'] = socs['total'] * socs['jobs']

    if write:
        socs.to_csv('Y:/NorMITs Land Use/import/NPR Segmentation/UK_SICtoSOCv3.csv')
        splits.to_csv('Y:/NorMITs Land Use/import/NPR Segmentation/splits.csv')
        
def build_employers_splits(scenario = sc01_jam):
    # scenario can be sc01_jam, sc02_pp, sc03_dd, sc04_uzc
    derive_soc_fy(scenario)
    # constrain to North using quals_to_soc
    # balance out soc splits msoa level
    # work out SOC 0 using fy pop for each msoa
    
    
    
    


