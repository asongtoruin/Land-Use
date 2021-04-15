# -*- coding: utf-8 -*-

"""

For future years:
    1. combine all SIC to SOC splits into 1 for all regions
    2. shorten the SOCs
    3. get the absolute values for emp fy from nelum processed figures 
    4. use the emp values divided by sic to work out the sic distribution in the fy
    5. use the translation from sic to soc to work out the distribution of the workers across soc categories
    6. use qualifications to soc to work out the fy control for soc (north level)
    7. apply the control to fy regional splits
    8. balance out the msoa to regions but keeping the msoa variation
    9. balance out children/75 or over population, use 2018 splits for empl types for fy (might need readressing)
    10.use soc to NS-SEC to uplift NS-SEC figures for fy
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

_home_dir = 'E:/NorMITs_Export/'
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
sc01_pop =_import_folder + 'SC01_JAM/population/future_growth_values.csv'
sc02_pop = _import_folder + 'SC02_PP/population/future_growth_values.csv'
sc03_pop = _import_folder + 'SC03_DD/population/future_growth_values.csv'
sc04_pop = _import_folder + 'SC04_UZC/population/future_growth_values.csv'
_landuse_path = 'Y:/NorMITs Land Use/iter3b/outputs/land_use_output_msoa.csv'


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
    else:
        return(scenario) 
        print('not supported')
    
    
    scenario_emp = scenario_emp.groupby(by=['msoa_zone_id'], as_index = False).sum().drop(columns={'soc', '% of MSOA_people_sum', 'MSOA people sum'})
    return(scenario_emp)
    
def combine_sic_splits_fy(target_folder = _sic_to_soc):
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

def derive_soc_fy(scenario):
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
    
    regional_splits = combine_sic_splits_fy()
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
            
    # totals for each region by SOC (exclulding SOC 0)
    soc_fy = fy_scenario_soc.groupby(by=['Region', 'soc_class'], as_index = False).sum().drop(columns={'North'})
    soc_fy.to_csv(_home_dir+'soc_fy.csv', index= False)
    
    # Need to convert to splits
    soc_fy['North'] = 1
    soc_fy_splits = soc_fy.groupby(by = ['North', 'soc_class'], as_index = False).sum()
    for x in range(int(2019), int(2051)):
        soc_fy_splits['total_'+str(x)] = soc_fy_splits['emp_'+str(x)].sum()
        soc_fy_splits['splits_'+str(x)] = soc_fy_splits['emp_'+str(x)]/soc_fy_splits['total_'+str(x)]
        soc_fy_splits = soc_fy_splits.drop(columns={'emp_'+str(x), 'total_'+str(x)}) 
    soc_fy_splits.to_csv(_home_dir+'soc_fy_splits.csv', index = False)    
    return(soc_fy_splits)  # here's the 3 socs by region for every year in the future as splits   
           
def quals_to_soc(scenario):
    # this will be a control for SOC splits in the North
    # based on qualifications for fy from NPIER 
    # then relies on ONS historical data on qualifications to SOC translator
    # for non-North regions use 2018 splits for now
    
    if scenario == sc01_jam or scenario == sc02_pp:
        npier_quals = pd.read_excel(_npierqualifications, Sheet = 'BAU - North demog')
        quals = pd.read_excel(_qualstosoctranslation)
        
        # BAU scenario       
        fy_soc = npier_quals.merge(quals, on = 'Labour force by highest qualification')
        g = []
        s = []
        for year in range(int(2019), int(2051)):
            
            fy_quals = fy_soc[['Labour force by highest qualification', year]]
            fy_quals = fy_quals.merge(quals, on = 'Labour force by highest qualification')
       
            fy_quals['higher'] = fy_quals[year]*fy_quals['High'] 
            fy_quals['medium'] = fy_quals[year]*fy_quals['Medium']
            fy_quals['skilled'] = fy_quals[year]*fy_quals['Skilled']
            
            # work out totals for each column
            summarise = fy_quals[['higher', 'medium', 'skilled']].sum()
            summarise = summarise.to_frame()
            summarise.reset_index(inplace=True)
            summarise = summarise.rename(columns={'index':'soc', 0:year})
            g.append(summarise)
            # work out splits for each
            splits = summarise.copy()
            splits['total'] = splits[(year)].sum()
            splits['splits'] = splits[(year)]/splits['total']
            splits = splits.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            s.append(splits)
        
        soc_quals_fy = reduce(lambda x, summarise : pd.merge(x, summarise, on = 'soc'), g)
        soc_quals_fy.to_csv(_home_dir+'soc_quals_fy.csv', index = False)
        soc_quals_splits = reduce(lambda x, splits : pd.merge(x, splits, on = 'soc'), s)
        # soc_quals_splits.to_csv(_home_dir+'soc_quals_splits_fy.csv', index = False)
      
        # soc_bau_fy.to_csv('Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/fy_soc_splits/soc_bau_splits.csv')
        return soc_quals_splits

    if scenario == sc03_dd or scenario == sc04_uzc:
        npier_quals = pd.read_excel(_npierqualifications, Sheet = 'TRA - North demog')
        quals = pd.read_excel(_qualstosoctranslation)

        fy_soc = npier_quals.merge(quals, on = 'Labour force by highest qualification')
        g = []
        s = []
        for year in range(int(2019), int(2051)):
            
            fy_quals = fy_soc[['Labour force by highest qualification', year]]
            fy_quals = fy_quals.merge(quals, on = 'Labour force by highest qualification')
       
            fy_quals['higher'] = fy_quals[year]*fy_quals['High'] 
            fy_quals['medium'] = fy_quals[year]*fy_quals['Medium']
            fy_quals['skilled'] = fy_quals[year]*fy_quals['Skilled']
            
            # work out totals for each column
            summarise = fy_quals[['higher', 'medium', 'skilled']].sum()
            summarise = summarise.to_frame()
            summarise.reset_index(inplace=True)
            summarise = summarise.rename(columns={'index':'soc', 0:year})
            g.append(summarise)
           
            splits = summarise.copy()
            splits['total'] = splits[(year)].sum()
            splits['splits'] = splits[(year)]/splits['total']
            splits = splits.drop(columns={(year), 'total'}).rename(columns={'splits':(year)})
            s.append(splits)
        
        soc_quals_fy = reduce(lambda x, summarise : pd.merge(x, summarise, on = 'soc'), g)
        soc_quals_fy.to_csv(_home_dir+'soc_quals_fy.csv', index = False)

        soc_quals_splits = reduce(lambda x, splits : pd.merge(x, splits, on = 'soc'), s)
        # soc_quals_splits.to_csv(_home_dir+'soc_quals_splits_fy.csv', index = False)

        return soc_quals_splits

def balance_northern_socs_fy(scenario,
                             verbose=True):
    """
    takes the soc splits by region for fy and constraints to the North
    
    """
    # read in socs for fy  
    regional_socs = regional_socs = derive_soc_fy(scenario)
    regional_socs = regional_socs.rename(columns={'soc_class':'soc'})
    # read in the constraining splits for the North
    North_socs = quals_to_soc(scenario)
       
    # work out the factor for balancing regions' socs
    blc = regional_socs.merge(North_socs, on = 'soc')
    for year in range(int(2019), int(2051)):
        blc['fc_'+str(year)] = blc[(year)]/blc['splits_'+str(year)]
        blc = blc.drop(columns=[(year), 'splits_'+str(year)])
    
    # apply the factors to regions
    # bring back soc_fy here to join the factor to values
    soc_fy = pd.read_csv(_home_dir+'soc_fy.csv')
    
    soc_fy = soc_fy.rename(columns={'soc_class':'soc'})
    # Are totals the same
    rgn_socs = soc_fy.merge(blc, on = 'soc')
    for year in range(int(2019), int(2051)):
        rgn_socs[(year)] = rgn_socs['fc_'+str(year)]*rgn_socs['emp_'+str(year)]
        rgn_socs = rgn_socs.drop(columns=['fc_'+str(year), 'emp_'+str(year)])
    
    # Check in is the same as out
    before = soc_fy['emp_2019'].sum()
    after = rgn_socs[2019].sum()
    
    if verbose:
        print('%d before' % before)
        print('%d after' % after)
    
    return rgn_socs

def base_yr_soc_splits():
    # use the landuse base year splits here

    landuse = pd.read_csv(_landuse_path)
    landuse_emp = landuse.groupby(by=['soc_cat', 'msoa_zone_id'], as_index = False).sum()
    msoa_lookup = pd.read_csv('Y:/NorMITs Land Use/import/MSOAtoRGNlookup.csv').rename(columns={'MSOA11CD':'msoa_zone_id'})
    msoa_lookup = msoa_lookup[['msoa_zone_id', 'RGN11NM']]
    msoa_lookup= msoa_lookup.rename(columns={'RGN11NM':'Region'})

    socs = [1,2,3] 

    land = landuse_emp[landuse_emp.soc_cat.isin(socs)]

    land = land.groupby(by=['msoa_zone_id', 'soc_cat'], as_index = False).sum()
    # first work out the SOC splits
    tot = land.groupby(by=['msoa_zone_id'],as_index = False).sum().rename(columns={'people':'total'}).drop(columns={'soc_cat'})
    land = land.merge(tot, on = 'msoa_zone_id', how = 'left')
    land['soc_splits'] = land['people']/land['total']
    return land

def balance_msoa_fy_soc_splits(scenario):
    # get the emp fy figures
    fy_emp = get_emp_values_fy(scenario)
    # get landuse soc splits
    soc_splits = base_yr_soc_splits()
    soc_splits = soc_splits[['msoa_zone_id', 'soc_cat','soc_splits']]
    
    # join the splits with fy emp values

    by_socs = fy_emp.merge(soc_splits, on = 'msoa_zone_id')

    for year in range(int(2019), int(2051)):
        by_socs['emp_'+str(year)] = by_socs[str(year)]*by_socs['soc_splits']
        by_socs = by_socs.drop(columns={str(year)})
    by_socs = by_socs.drop(columns={'soc_splits'}).rename(columns={'soc_cat':'soc'})
    by_socs['soc'] = by_socs['soc'].replace(1,'higher')
    by_socs['soc'] = by_socs['soc'].replace(2,'medium')
    by_socs['soc'] = by_socs['soc'].replace(3,'skilled')
    
    # group landuse to region level
    msoa_lookup = pd.read_csv('Y:/NorMITs Land Use/import/MSOAtoRGNlookup.csv').rename(columns={'MSOA11CD':'msoa_zone_id'})
    msoa_lookup = msoa_lookup[['msoa_zone_id', 'RGN11NM']]
    msoa_lookup= msoa_lookup.rename(columns={'RGN11NM':'Region'})

    by_socs = by_socs.merge(msoa_lookup, on = ['msoa_zone_id']).drop(columns={'2018'})
    North = ['North West', 'North East', 'Yorkshire and The Humber', 'East Midlands']

    # North regions only  - change into splits
    by_socs_north = by_socs[by_socs.Region.isin(North)]
    by_socs_reg = by_socs_north.groupby(by = ['Region', 'soc'], as_index = False).sum()
    rgtots = by_socs_reg.groupby(by=['Region'], as_index = False).sum()
    land_rg_splits = by_socs_reg.merge(rgtots, on = ['Region'])
    for year in range(int(2019), int(2051)):    
        land_rg_splits[str(year)+'_lusplits'] = land_rg_splits['emp_'+str(year)+'_x']/land_rg_splits['emp_'+str(year)+'_y']
        land_rg_splits = land_rg_splits.drop(columns={'emp_'+str(year)+'_x', 'emp_'+str(year)+'_y'})
    
    # bring in regional constraint - change into splits
    rgn_socs = balance_northern_socs_fy(scenario)
    tots = rgn_socs.groupby(by=['Region'], as_index = False).sum()
    rgn_splits = rgn_socs.merge(tots, on = ['Region'])
    for year in range(int(2019), int(2051)):    
        rgn_splits[str(year)+'_splits'] = rgn_splits[str(year)+'_x']/rgn_splits[str(year)+'_y']
        rgn_splits = rgn_splits.drop(columns={str(year)+'_x', str(year)+'_y'})
    
    # combine the two to get the factor
    blcsocs = land_rg_splits.merge(rgn_splits, on = ['soc', 'Region'])
    for year in range(int(2019), int(2051)):
        blcsocs['fc_'+str(year)] = blcsocs[str(year)+'_splits']/blcsocs[str(year)+'_lusplits']
        blcsocs = blcsocs.drop(columns=[str(year)+'_splits', str(year)+'_lusplits'])

    # join and apply factors to msoa
    
    zone_socs = by_socs_north.merge(blcsocs, on =['Region', 'soc'])
    
    for year in range(int(2019), int(2051)):
        zone_socs[(year)] = zone_socs['fc_'+str(year)]*zone_socs['emp_'+str(year)]
        zone_socs = zone_socs.drop(columns=['fc_'+str(year), 'emp_'+str(year)])
    zone_socs = zone_socs.drop(columns={'North_x', 'North_y'})  
    zone_socs_reg = zone_socs.groupby(by=['Region'], as_index= False).sum()

    
    by_socs_out = by_socs[~by_socs.Region.isin(North)]
    #by_socs_out = by_socs_out[by_socs_out.soc.isin(socs)]

    for year in range(int(2019), int(2051)):
        by_socs_out[(year)] = by_socs_out['emp_'+str(year)]
        by_socs_out = by_socs_out.drop(columns={'emp_'+str(year)})
 
    #zone_socs = zone_socs.drop(columns={'North_x', 'North_y'})
    zone_socs_all = zone_socs.append(by_socs_out)
    if scenario == sc01_jam: 
        sc = 'sc01_jam'
    elif scenario == sc02_pp:
        sc = 'sc02_pp'
    elif scenario == sc03_dd:
        sc= 'sc03_dd'
    elif scenario == sc04_uzc:
        sc='sc04_uzc'
    
    zone_socs_all.to_csv('Y:/NorMITs Land Use/iter3b/outputs/scenarios/soc splits/'+sc+'_msoa_socs.csv', index = False)
    
    return rgn_socs

def get_pop_values_fy(scenario):
    """
    Reads in processed pop figures for fy for a given scenario
    Parameters
    ----------
    scenario_pop:
        Path to csv population values 

    Returns
    ----------
        Reads in the processed pop values for fy for a given scenario
    """
    if scenario == sc01_jam:
        scenario_pop = pd.read_csv(sc01_pop)
    elif scenario == sc02_pp:
        scenario_pop = pd.read_csv(sc02_pop)
    elif scenario == sc03_dd:
        scenario_pop = pd.read_csv(sc03_pop)
    elif scenario == sc04_uzc:
        scenario_pop = pd.read_csv(sc04_pop)
    
    scenario_pop = scenario_pop.groupby(by=['msoa_zone_id'], as_index = False).sum().drop(columns={'soc', 'ns','% of MSOA_people_sum', 'MSOA_people_sum'})
    return scenario_pop
                          
def add_soc_zero(scenario):
    # this comes up with minus figures for soc0 for some msoas
    if scenario == sc01_jam: 
        sc = 'sc01_jam'
    elif scenario == sc02_pp:
        sc = 'sc02_pp'
    elif scenario == sc03_dd:
        sc= 'sc03_dd'
    elif scenario == sc04_uzc:
        sc='sc04_uzc'

    emp_values = pd.read_csv('Y:/NorMITs Demand/import - test/scenarios/FY SIC to SOC/'+sc+'_msoa_socs.csv')
    scenario_pop = get_pop_values_fy(scenario)
    wa_pop = emp_values.groupby(by=['msoa_zone_id'], as_index = False).sum()
    
    pop_fy = scenario_pop.merge(wa_pop, on = ['msoa_zone_id'])
    for year in range(int(2019), int(2051)):
        pop_fy[(year)] = pop_fy[str(year)+'_x']- pop_fy[str(year)+'_y']
        pop_fy = pop_fy.drop(columns={str(year)+'_x', str(year)+'_y'})
        
def fy_age_splits(scenario): 
    """
    This reads in simple splits for nonworking category by age, so for children and 75 or over
    No data on unemployed numbers from NPIER'20 into the future
    
    """
    if scenario == sc01_jam or scenario == sc02_pp:
        nwa_age_splits = pd.read_excel('Y:/NorMITs Demand/import - test/scenarios/NPIER/North_summary_040920_v2_age.xlsx', Sheet = 'BAU')
        
    if scenario == sc03_dd or scenario == sc04_uzc:
        nwa_age_splits = pd.read_excel('Y:/NorMITs Demand/import - test/scenarios/NPIER/North_summary_040920_v2_age.xlsx', Sheet = 'TRA')
        
    return(nwa_age_splits)
    
        
def build_employers_splits(scenario):
    # scenario can be sc01_jam, sc02_pp, sc03_dd, sc04_uzc
    balance_northern_socs_fy(scenario)
    balance_msoa_fy_soc_splits(scenario) # this should write out the MSOA level SOC split
    
        
    
    
    


