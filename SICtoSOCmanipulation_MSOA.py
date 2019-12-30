# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:10:36 2019

@author: mags15
"""

import os
import sys

sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Utilities/Python')

import pandas as pd

output = 'emp'
defaultHomeDir = 'C:/output'

def SetWd(homeDir = defaultHomeDir, iteration=output):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()
    
# File paths
msoa_sic = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/sic_div_msoa_2018B.csv')
#msoa_regTranslation =pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/OA_to_LSOA_to_MSOA_to_LAD_December_2017_Lookup_in_Great_Britain.csv')

SetWd(homeDir = 'C:/NorMITs_Export', iteration=output)
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
siccols = msoa_sic.columns[2:]   

                                   
ee_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_East of England.csv')
ne_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_NorthEast.csv')
se_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_SouthEast.csv')
yor_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_Yorkshire.csv')
wal_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_Wales.csv')
sw_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_SouthWest.csv')
lon_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_London.csv')
em_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_East Midlands.csv')
nw_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_NorthWest.csv')
nw_splits = nw_splits.rename(columns={'Unnamed: 0': 'Siccat'})
scot_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_Scotland.csv')
wm_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/Regional_SICsplits/SIC_West Midlands.csv')

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
#splits = splits.replace({'RGN11nm':{'Yorkshire and the Humber':'Yorkshire and The Humber'}})
del(wm_splits, scot_splits, nw_splits, em_splits, lon_splits, sw_splits, 
    wal_splits, yor_splits, se_splits, ne_splits, ee_splits)

sic_columns = msoa_sic.columns[2:]

msoatrans = pd.melt(msoa_sic, id_vars = ['MSOA', 'RGN11nm'], value_vars = sic_columns)
msoatrans = msoatrans.rename(columns = {'variable':'Siccat', 'value':'total'})
msoatrans['total'].sum()

#get a rough estimation for high/medium/skilled and apply to the totals
splits['higher'] = splits.iloc[:,1:12].sum(axis=1)
splits['medium'] = splits.iloc[:,12:22].sum(axis=1)
splits['skilled'] = splits.iloc[:,22:26].sum(axis=1)
splits['higher'] = splits['higher'].round(1)
splits['medium'] = splits['medium'].round(1)
splits['skilled'] = splits['skilled'].round(1)

splits['diff'] = 1-(splits['higher']+splits['medium']+splits['skilled'])
splits['higher'] = splits['higher']+(splits['diff']/3)
splits['medium'] = splits['medium']+(splits['diff']/3)
splits['skilled'] = splits['skilled']+(splits['diff']/3)
splits['total'] = splits['higher']+splits['medium']+splits['skilled']
splits = splits[['RGN11nm', 'Siccat', 'higher', 'medium', 'skilled']]
# now they add up to 100%

socs = msoatrans.merge(splits, on = ['RGN11nm', 'Siccat'], how = 'outer')

socs.update(socs.iloc[:, 4:7].mul(socs.total, 0))
socs['check']= socs['higher']+socs['medium']+socs['skilled']
socs['check'].sum()

socs.to_csv('Y:/NorMITs Land Use/import/NPR Segmentation/UK_SICtoSOCv2.csv')
splits.to_csv('Y:/NorMITs Land Use/import/NPR Segmentation/splits.csv')
