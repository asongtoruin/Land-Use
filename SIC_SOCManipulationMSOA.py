# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:10:36 2019

@author: ESRIAdmin
"""

import os
import sys

sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Utilities/Python')

import pandas as pd
import numpy as np
import nu_project as nup

output = 'emp'
defaultHomeDir = 'C:/output'

def SetWd(homeDir = defaultHomeDir, iteration=output):
    os.chdir(homeDir)
    nup.CreateProjectFolder(iteration)
    return()
    
# File paths
msoa_sic = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/sic_div_msoa_2018B.csv')

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
                                    
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_East of England.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_NorthEast.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_SouthEast.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_Yorkshire.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_Wales.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_SouthWest.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_London.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_East Midlands.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_NorthWest.csv')
#region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_Scotland.csv')
region_splits = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/SIC to SOC/SIC_West Midlands.csv')


#Splits are available for each region, so each needs to be separated and applied
 # to the totals per MSOA 


wm = msoa_sic[msoa_sic.RGN11nm == 'West Midlands']
wm = wm.drop(columns={'RGN11nm'})

wm = wm.rename(columns={"MSOA":"Siccat"})
wm = wm.T

wm.columns = wm[:1].values[0]
wm = wm[1:]
wm["Siccat"] = wm.index

msoas = wm.columns[:-1]
region_splits_list = []
soc_columns = region_splits.columns[1:-1]

for msoa in msoas:    
    print("Now working on: " + msoa)
    region_splits_new = region_splits.merge(wm[[msoa, "Siccat"]])
    for i in range(0, len(region_splits)):
        region_splits_new.loc[
                i,
                soc_columns
                # also only loc relevant columns
                ] = (
                region_splits_new.loc[
                        i,
                        soc_columns
                    ]
                * region_splits_new[msoa][i:i+1].values[0]
                )
    region_splits_new = region_splits_new[soc_columns]
    region_splits_new = region_splits_new.sum()
    region_splits_new = pd.DataFrame(region_splits_new).T
    region_splits_new["MSOA"] = msoa
    region_splits_list.append(region_splits_new)    

region_split_full = pd.concat(region_splits_list)
region_split_full.to_csv('West Midlands.csv')






