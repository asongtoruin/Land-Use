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

import os
import sys

import pandas as pd

_UTILS_GIT = ('C:/Users/' +
              os.getlogin() +
              '/Documents/GitHub/Normits-Utils')
sys.path.append(_UTILS_GIT)
import normits_utils as nup # Folder build utils

# Default regional splits path. Soure: HSL. SIC categories are differnt for HSL and CE.
_regional_splits_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/'
_default_sic_2digit_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/sic_div_msoa_2018.csv'
_uplift_path = 'Y:/NorMITs Land Use/Results/'
_processed_data = 'Y:/NorMITs Land Use/import/NPR Segmentation/processed data/'

# read in data for the uplift
ons_employees = pd.ExcelFile((_uplift_path + 'Employees by LAD - table52017p.xlsx')) # ONS data in thousands of employees to scale to - use 2017 as 2018 data is provisional
ons_people_in_work = pd.ExcelFile((_uplift_path + 'lfs_people_in_employment_2018.xlsx'))
employees_socPath = (_processed_data + '/UK_SICtoSOC_2018_HSLcategories.csv') # SICtoSOC output split into 12 TfN sectors
area_code_lookup = pd.read_csv(_processed_data + 'LAD controls 2018/MSOA_LAD_APR19.csv') # lookup msoa, LA, county, _APR19 changes E06000028 and E06000029 to E06000058

# TODO: Standard col format names for input

def combine_regional_splits(target_folder = _regional_splits_path):


    target_dir = os.listdir(target_folder)
    import_list = [x for x in target_dir if 'SIC_' in x]

    ph = []
    for mat in import_list:
        print('Importing ' + mat + ' w cols')

        # Import
        dat = pd.read_csv(target_folder + '/' + mat)
        print(list(dat))

        # Get area and clean
        area = mat.replace('SIC_','')
        area = area.replace('.csv','')
        area = area.replace('_',' ')
        area = area.replace('NorthEast','North East')
        area = area.replace('SouthEast','South East')
        area = area.replace('NorthWest','North West')
        area = area.replace('SouthWest','South West')
        if area == 'Yorkshire':
            area = 'Yorkshire and The Humber'

        # Rename first col
        dat = dat.rename(columns={list(dat)[0]:'CE_SIC'})

        dat = pd.wide_to_long(dat, stubnames='SOC',
                              i ='CE_SIC',
                              j='soc_cat').reset_index()

        # Simplify var names
        dat = dat.rename(columns={'SOC':'jobs'})

        # Add area
        dat['RGN11nm'] = area

        # Append to ph
        ph.append(dat)
    
    # Concat
    out = pd.concat(ph, sort=True)

    return(out)

def classify_soc(soc_value):

    # TODO: All placeholders! Replace with accurate lookup!!
  
    if soc_value <= 30:
        soc_class = 1
    elif soc_value > 30 and soc_value <=50:
        soc_class = 2
    elif soc_value > 50 and soc_value <100:
        soc_class = 3
    else:
        soc_class = None
    return(soc_class)

def build_sic_to_soc(balance_soc_factors = True,
                     output = 'emp',
                     sic_2digit_path = _default_sic_2digit_path,
                     write = False):

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

    return(socs)
        
# TODO: functionalise this and might need to replace the employees_socPath
############################# GENERATE LAD LEVEL CONTROL ################################

# rename LAD columns
def build_employees_by_lad(ons_employees,
                           ons_people_in_work):
    """
    ons_employees = Excel object of ONS employees data
    ons_people_in_work = Excel object of ONS people in work
    """
    # Import employees by LAD
    employees_lad = pd.read_excel(ons_employees, sheet_name=2, header=1, usecols=[0, 2, 13])
    employees_lad.columns = ['LA Code', 'County Code', 'Total Employees']

    # factor Employees by LAD 2017 up to 2018 (provisional) total
    people_in_work_18 = pd.read_excel(ons_people_in_work, sheet_name=0, header=7, usecols=[0, 3])
    people_in_work_18.columns = ['Region', 'Total in employment - aged 16 and over']
    people_in_work_18[['Total in employment - aged 16 and over']] =people_in_work_18[['Total in employment - aged 16 and over']].div(1000)

    # calculate factor to scale up employees per LAD (2017) to people in work (2018)
    ons_factor = people_in_work_18[['Total in employment - aged 16 and over']].sum().div(employees_lad['Total Employees'].sum(), axis='index')
    employees_lad['Total Employees Factored'] = employees_lad['Total Employees'] * ons_factor[0]
    employees_lad = employees_lad.drop(['Total Employees'], axis=1)
    employees_lad['Total Employees Factored'] = employees_lad['Total Employees Factored']*1000

    return(employees_lad)

############################# GENERATE LAD/COUNTY/SCOT-WALES LEVEL SCALING FACTORS ################################
employees_soc = pd.read_csv(employees_socPath).drop(columns={'Unnamed: 0'})
employees_soc_code_join = area_code_lookup.join(employees_soc.set_index('MSOA'), on='MSOA_code')

# group soc data by LA Code and sum total
employees_soc_la_gr = employees_soc_code_join.groupby(['LAD18CD'], as_index=False).sum().rename(
    columns={'check': 'SOC total'}).drop(['NELUM_zone', 'higher', 'medium', 'skilled'], axis=1)
# group soc data by County Code and sum total
employees_soc_county_gr = employees_soc_code_join.groupby(['CTY18CD'], as_index=False).sum().rename(
    columns={'check': 'SOC total'}).drop(['NELUM_zone', 'higher', 'medium', 'skilled'], axis=1)
# group LAD data by LA code to group Scotland and Wales rows
employees_lad_la_gr = employees_lad.groupby(['LA Code'], as_index=False).sum()

# Two domains - LA and County, due to way ONS data is presented
# join LA grouped soc data to LAD table for comparison (LA)
employees_la_comp = employees_soc_la_gr.join(employees_lad_la_gr.set_index('LA Code'), on='LAD18CD').rename(
    columns={'Total Employees Factored': 'LAD total LA'})
# join County grouped soc data to LAD table for comparison (County)
employees_county_comp = employees_soc_county_gr.join(employees_lad.set_index('County Code'), on='CTY18CD').rename(
    columns={'Total Employees Factored': 'LAD total County'}).drop(['LA Code'], axis=1)

# now calculate % difference between SOC and LAD for both LA and County df's
employees_la_comp['soc_to_lad_factor_la'] = employees_la_comp['SOC total'] / employees_la_comp['LAD total LA']
employees_county_comp['soc_to_lad_factor_county'] = employees_county_comp['SOC total'] / employees_county_comp['LAD total County']
employees_county_comp = employees_county_comp.drop(columns={'total'})
############################# JOIN BACK TO MSOA ################################

# multiple joins required since some areas are in the LA domain but others in County domain
# join on LAD18CD and drop totals
msoa_factors = area_code_lookup.join(employees_la_comp.set_index('LAD18CD'), on='LAD18CD').drop(
    ['SOC total', 'LAD total LA', 'NELUM_zone'], axis=1)
# join on CTY18CD and drop totals
msoa_factors = msoa_factors.join(employees_county_comp.set_index('CTY18CD'), on='CTY18CD').drop(columns=
    {'SOC total', 'LAD total County'}).rename(columns={'MSOA_code':'MSOA'})
# create new column of 'LA or County factor' - fillna 
msoa_factors['factor'] = msoa_factors['soc_to_lad_factor_la'].fillna(msoa_factors['soc_to_lad_factor_county'])
msoa_factors = msoa_factors.drop(columns = {'total'})
# join factors to employees by msoa
employees_soc_factors = employees_soc.join(msoa_factors.set_index('MSOA'), on='MSOA', how='left').drop(
    ['LAD18CD', 'CTY18CD', 'soc_to_lad_factor_la', 'soc_to_lad_factor_county'], axis=1).set_index(
            ['MSOA', 'HSL_SIC'])

############################# APPLY CONTROL AND WRITE OUT FILE ################################
factored_employees = employees_soc_factors[['higher', 'medium', 'skilled']].div(employees_soc_factors['factor'], axis='index')
factored_employees['check']=factored_employees['higher']+factored_employees['medium']+factored_employees['skilled']
factored_employees.reset_index().to_csv('C:/NorMITs_Export/'+'jobs_by_industry_skill_2018.csv', index=False)
    


def build_attraction_employments():
        
    # Run sic/soc lookup
    sic_soc = build_sic_to_soc(balance_soc_factors = True,
                               output = 'emp',
                               sic_2digit_path = _default_sic_2digit_path,
                               write = False)
    
    # Replace non integer values in jobs - get 2 digit SIC
    # 2 digit SIC is basis for weighting - hence why we needed 5 digit lookup.
    # Potentially not anymore.
    unq_sic = sic_soc['HSL_SIC'].drop_duplicates()
    print(unq_sic)
    sic_soc['sic_2d'] = sic_soc['HSL_SIC'].str.replace('s','')
    sic_soc['sic_2d'] = sic_soc['sic_2d'].str.replace('_Wrkr','')
    
    # Group, sum & tidy columns
    sic_soc = sic_soc.reindex(
            ['MSOA', 'sic_2d', 'soc_class', 'seg_jobs'],
            axis=1).groupby(
                    ['MSOA', 'sic_2d', 'soc_class']).sum(
                            ).reset_index()

    sic_soc = sic_soc.sort_values(
            ['MSOA','soc_class']).reset_index(
                    drop=True)

    # Bit stuck - can't remember:
    # How we're supposed to overlay purpose weighting
    # Why I needed the lookup to 5 digit SIC
    # How NS-sec is supposed to be important (if at all)

    # Check MSOAs are intact
    msoa_audit = sic_soc['MSOA'].drop_duplicates()
    if len(msoa_audit) == 8480:
        print('All MSOAs accounted for')
    else:
        print('Some MSOAs missing')

    # Export to Y:/Data or a database
    sic_soc.to_csv('soc_2_digit_sic_2018.csv', index=False)

    return(sic_soc)


