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
import pandas as pd
import sys
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Utilities/Python')
import nu_project as nup

output = 'emp'
defaultHomeDir = 'C:/NorMITs_export/'

def SetWd(homeDir = defaultHomeDir, iteration=output):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()
    
# File paths
# MSOA divisions HSL data
msoa_sic = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/sic_div_msoa_2018.csv')
path = 'Y:\\NorMITs Land Use\\import\\NPR Segmentation\\processed data\\LAD controls 2018\\'
os.chdir(path) # changes directory up one folder


# region splits. Source: HSL. Categories for SIC here are different to HSL data
ee_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_East of England.csv')
ne_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_NorthEast.csv')
se_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_SouthEast.csv')
yor_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_Yorkshire.csv')
wal_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_Wales.csv')
sw_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_SouthWest.csv')
lon_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_London.csv')
em_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_East Midlands.csv')
nw_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_NorthWest.csv')
scot_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_Scotland.csv')
wm_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/SIC_West Midlands.csv')

Sictrans = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/CE industry categories.csv')

# tfn industry sectors
tfnindsectors = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/TfN industrial sector splits.csv')

# read in data for the uplift
ons_employees = pd.ExcelFile('Employees by LAD - table52017p.xlsx') # ONS data in thousands of employees to scale to - use 2017 as 2018 data is provisional
ons_people_in_work = pd.ExcelFile('lfs_people_in_employment_2018.xlsx')
employees_socPath = (defaultHomeDir +output+'/UK_SICtoSOC_HSL.csv') # SICtoSOC output split into 12 TfN sectors
area_code_lookup = pd.read_csv('MSOA_LAD_APR19.csv') # lookup msoa, LA, county and NELUM zone, _APR19 changes E06000028 and E06000029 to E06000058


SetWd(homeDir = 'C:/NorMITs_Export', iteration=output)

# Regionsplits combine
    
  #  nw_splits = nw_splits.rename(columns={'Unnamed: 0': 'Siccat'})

ee_splits['RGN11nm'] = 'East of England'
ne_splits['RGN11nm'] = 'North East'
se_splits['RGN11nm'] = 'South East'
yor_splits['RGN11nm'] = 'Yorkshire and The Humber'
wal_splits['RGN11nm'] = 'Wales'
sw_splits['RGN11nm'] = 'South West'
lon_splits['RGN11nm'] = 'London'
em_splits['RGN11nm'] = 'East Midlands'
nw_splits['RGN11nm'] = 'North West'
scot_splits['RGN11nm'] = 'Scotland'
wm_splits['RGN11nm'] = 'West Midlands'

frames = [ee_splits, ne_splits, se_splits, yor_splits, wal_splits, sw_splits, 
      em_splits, lon_splits, nw_splits,scot_splits, wm_splits]

splits = pd.concat(frames, sort=True)
del(wm_splits, scot_splits, nw_splits, em_splits, lon_splits, sw_splits, 
    wal_splits, yor_splits, se_splits, ne_splits, ee_splits)

# get an estimation for high/medium/skilled and apply to the totals
splits['higher'] = splits.iloc[:,1:12].sum(axis=1)
splits['medium'] = splits.iloc[:,12:22].sum(axis=1)
splits['skilled'] = splits.iloc[:,22:26].sum(axis=1)

# the splits don't add up to 100% so need an uplift so no numbers are missing
splits['diff'] = 1-(splits['higher']+splits['medium']+splits['skilled'])
splits['higher'] = splits['higher']+(splits['diff']/3)
splits['medium'] = splits['medium']+(splits['diff']/3)
splits['skilled'] = splits['skilled']+(splits['diff']/3)
splits['total'] = splits['higher']+splits['medium']+splits['skilled']
splits = splits[['RGN11nm', 'Siccat', 'higher', 'medium', 'skilled']]
splits = splits.rename(columns={'Siccat':'CE_SIC'})
# splits['total'].sum() # check all add up to 1
# now they should add up to 100%


# Melt the HSL MSOA division table
msoa_sic = msoa_sic.drop(columns = {'RGN11cd'})
sic_columns = msoa_sic.columns[2:]   

msoatrans = pd.melt(msoa_sic, id_vars = ['MSOA', 'RGN11nm'], value_vars = sic_columns)
msoatrans = msoatrans.rename(columns = {'variable':'HSL_SIC', 'value':'total'})
msoatrans['total'].sum()
#msoatrans = msoatrans.rename(columns={'Siccat': 'HSL_SIC'})
Sictrans = Sictrans.drop(columns = {'CE_SIC_categories'})
msoatrans2 = msoatrans.merge(Sictrans, on = 'HSL_SIC', how = 'outer')

# need to join the CE codes
# msoa_sic = msoa_sic.drop(columns = {'RGN11cd'})

#Sictrans = Sictrans.drop(columns = {'CE_cat.1'})
msoatrans2 = msoatrans.merge(Sictrans, on = 'HSL_SIC', how = 'left')
msoatrans2 = msoatrans2.rename(columns={'CE_cat':'CE_SIC'}).drop(columns={'CE_cat.1'})
socs = msoatrans2.merge(splits, on = ['RGN11nm', 'CE_SIC'], how = 'left')
socs['total'].sum()

socs.update(socs.iloc[:, 5:8].mul(socs.total, 0))
socs['check']= socs['higher']+socs['medium']+socs['skilled']
socs= socs.drop(columns={'RGN11nm', 'CE_SIC'})
print(socs['check'].sum())

socs.to_csv('UK_SICtoSOC_HSL.csv')

# Use TfN Industry sectors weights
"""
tfnindsectors = tfnindsectors.rename(columns = {'HsL_sIC':'HSL_SIC'})
tfnindsectors = tfnindsectors.drop(columns = {'SIC_division', 'Description', 'North sector weights'})
indcols = tfnindsectors.columns[1:]
print(indcols)
tfnindsectors = pd.melt(tfnindsectors, id_vars = 'HSL_SIC', value_vars = indcols)
tfnindsectors = tfnindsectors.rename(columns={'variable':'TfN industry', 'value':'splits'})
tfnsocs = socs.merge(tfnindsectors, on = 'HSL_SIC', how = 'outer')
tfnsocs['splits'] = tfnsocs['splits'].fillna(0)
tfnsocs2 = tfnsocs.copy()
tfnsocs2.update(tfnsocs2.iloc[:, 5:8].mul(tfnsocs2.splits,0))
   
tfnsocs2['check'] = tfnsocs2['higher']+tfnsocs2['medium']+tfnsocs2['skilled']
tfnsocs2['check'].sum()
tfnsocs2 = tfnsocs2.drop(columns = {'HSL_SIC', 'check'})
tfnind = tfnsocs2[['MSOA', 'TfN industry', 'higher', 'medium', 'skilled']]
tfnsocs2 = tfnsocs2.drop(columns = {'RGN11nm', 'total', 'CE_SIC','HSL_SIC', 'splits'})

tfnind = tfnsocs2.groupby(
        by = ['MSOA', 'TfN industry'], 
        as_index = False, 
        ).sum(axis = 0)
#.drop(columns = {'splits', 'total'})
socs_check =socs[['MSOA', 'check']]
socs_check = socs_check.groupby(
    by = ['MSOA'],
    as_index = False,
    ).sum(axis= 0)
tfnsocs2.to_csv('C:/NorMITs_Export/SOCbyTfNindsutrysectors.csv')
"""

############################# GENERATE LAD LEVEL CONTROL ################################

# rename LAD columns
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
    

"""
# This is now redundant as it calculates the SIC definitions proposed by CE  (Cambridge Econometrics) which are different.

msoa_sic['SIC1'] = msoa_sic['SIC_1']+msoa_sic['SIC_1.1']+msoa_sic['SIC_1.2']
msoa_sic['SIC2'] = msoa_sic['SIC_2']+msoa_sic['SIC_2.1']+msoa_sic['SIC_2.2']+msoa_sic['SIC_2.3']+msoa_sic['SIC_2.4']
msoa_sic['SIC3'] = msoa_sic['SIC_3']+msoa_sic['SIC_3.1']+msoa_sic['SIC_3.2']
msoa_sic['SIC4'] = msoa_sic['SIC_4']+msoa_sic['SIC_4.1']+msoa_sic['SIC_4.2']
msoa_sic['SIC5'] = msoa_sic['SIC_5']+msoa_sic['SIC_5.1']
msoa_sic['SIC10'] = msoa_sic['SIC_10']+msoa_sic['SIC_10.1']
msoa_sic['SIC11']= msoa_sic['SIC_11']+msoa_sic['SIC_11.1']
msoa_sic['SIC17']= msoa_sic['SIC_17']+msoa_sic['SIC_17.1']+msoa_sic['SIC_17.2']
msoa_sic['SIC19']= msoa_sic['SIC_19']+msoa_sic['SIC_19.1']+msoa_sic['SIC_19.2']+msoa_sic['SIC_19.3']
msoa_sic['SIC20'] = msoa_sic['SIC_20']+msoa_sic['SIC_20.1']+msoa_sic['SIC_20.2']
msoa_sic['SIC27'] = msoa_sic['SIC_27']+msoa_sic['SIC_27.1']
msoa_sic['SIC30'] = msoa_sic['SIC_30']+msoa_sic['SIC_30.1']+msoa_sic['SIC_30.2']
msoa_sic['SIC31'] = msoa_sic['SIC_31']+msoa_sic['SIC_31.1']+msoa_sic['SIC_31.2']
msoa_sic['SIC32'] = msoa_sic['SIC_32']+msoa_sic['SIC_32.1']+msoa_sic['SIC_32.2']
msoa_sic['SIC37'] = msoa_sic['SIC_37']+msoa_sic['SIC_37.1']+msoa_sic['SIC_37.2']+msoa_sic['SIC_37.3']
msoa_sic['SIC38'] = msoa_sic['SIC_38']+msoa_sic['SIC_38.1']+msoa_sic['SIC_38.2']+msoa_sic['SIC_38.3']+msoa_sic['SIC_38.4']+msoa_sic['SIC_38.5']
msoa_sic['SIC42'] = msoa_sic['SIC_42']+msoa_sic['SIC_42.1']
msoa_sic['SIC43'] = msoa_sic['SIC_43']+msoa_sic['SIC_43.1']
msoa_sic['SIC44'] = msoa_sic['SIC_44']+msoa_sic['SIC_44.1']
msoa_sic['SIC45'] = msoa_sic['SIC_45']+msoa_sic['SIC_45.1']+msoa_sic['SIC_45.2']+msoa_sic['SIC_45.3']


msoa_sic = msoa_sic.rename(columns={'SIC_6':'SIC6', 'SIC_7':'SIC7', 
                           'SIC_8':'SIC8', 'SIC_9':'SIC9', 'SIC_12':'SIC12',
                           'SIC_13':'SIC13', 'SIC_14':'SIC14', 'SIC_15':'SIC15',
                           'SIC_16':'SIC16', 'SIC_18':'SIC18', 'SIC_21':'SIC21',
                           'SIC_22':'SIC22', 'SIC_23':'SIC23', 'SIC_24':'SIC24',
                           'SIC_25':'SIC25', 'SIC_26':'SIC26', 'SIC_28':'SIC28',
                           'SIC_29':'SIC29', 'SIC_33':'SIC33', 'SIC_34':'SIC34',
                           'SIC_35':'SIC35', 'SIC_36':'SIC36', 'SIC_39':'SIC39',
                           'SIC_40':'SIC40', 'SIC_41':'SIC41'
                           })
    
msoa_sic = msoa_sic.drop(columns={'SIC_45', 'SIC_45.1', 'SIC_45.2', 'SIC_45.3',
                                    'SIC_44.1', 'SIC_44', 'SIC_43', 'SIC_43.1',
                                    'SIC_42', 'SIC_42.1','SIC_38', 'SIC_38.1',
                                    'SIC_38.2','SIC_38.3','SIC_38.5','SIC_38.5',
                                    'SIC_37', 'SIC_37.1', 'SIC_37.2', 'SIC_37.3',
                                    'SIC_32', 'SIC_32.1', 'SIC_32.2', 'SIC_31', 
                                    'SIC_31.1', 'SIC_31.2', 'SIC_30', 'SIC_30.1', 
                                    'SIC_30.2', 'SIC_27', 'SIC_27.1', 'SIC_20.2', 
                                    'SIC_20.1', 'SIC_20', 'SIC_19.3', 'SIC_19.2', 
                                    'SIC_19.1', 'SIC_19', 'SIC_17', 'SIC_17.1',
                                    'SIC_17.2', 'SIC_17', 'SIC_11', 'SIC_11.1', 
                                    'SIC_10', 'SIC_10.1', 'SIC_5', 'SIC_5.1', 
                                    'SIC_4', 'SIC_4.1', 'SIC_4.2', 'SIC_3.2', 
                                    'SIC_3.1', 'SIC_3', 'SIC_2', 'SIC_2.1', 
                                    'SIC_2.2', 'SIC_2.3', 'SIC_2.4', 'SIC_1', 
                                    'SIC_1.2', 'SIC_1.1', 'SIC_38.4', '#N/A', 
                                    '#N/A.1'})
msoa_sic = msoa_sic[['MSOA','RGN11nm', 'SIC1', 'SIC2', 'SIC3', 'SIC4', 'SIC5', 'SIC6', 'SIC7', 'SIC8', 
                    'SIC9', 'SIC10', 'SIC11', 'SIC12', 'SIC13', 'SIC14', 'SIC15', 'SIC16', 
                    'SIC17', 'SIC18', 'SIC19', 'SIC20', 'SIC21', 'SIC22', 'SIC23', 'SIC24',
                    'SIC25', 'SIC26', 'SIC27', 'SIC28', 'SIC29', 'SIC30', 'SIC31',
                    'SIC32', 'SIC33', 'SIC34', 'SIC35', 'SIC36', 'SIC37', 'SIC38', 
                    'SIC39', 'SIC40', 'SIC41', 'SIC42', 'SIC43', 'SIC44', 'SIC45']]      
"""

