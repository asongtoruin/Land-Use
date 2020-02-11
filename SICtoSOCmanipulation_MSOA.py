# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:10:36 2019

@author: mags15

more info: Y:/NorMITs Land Use/import/NPR Segmentation/DataPrepProcess.doc

"""

import os
import sys

import pandas as pd

_UTILS_GIT = ('C:/Users/' +
              os.getlogin() +
              '/Documents/GitHub/Normits-Utils')
sys.path.append(_UTILS_GIT)
import normits_utils as nup # Folder build utils

# Default regional splits path
_regional_splits_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/'
_default_sic_2digit_path = 'Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/sic_div_msoa_2018.csv'

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
        soc_class = 'high'
    elif soc_value > 30 and soc_value <=50:
        soc_class = 'medium'
    elif soc_value > 50 and soc_value <100:
        soc_class = 'skilled'
    else:
        soc_class = None

    return(soc_class)

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

    # Use TfN Industry sectors weights
    # What for?
    tfnindsectors = pd.read_csv('Y:/NorMITs Land Use/import/NPR Segmentation/raw data and lookups/TfN industrial sector splits.csv')
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
    
    # TODO: Define final sic_to_soc output as NELUM ready export
    
    return(sic_to_soc)

def build_attraction_employments():
    
    # TODO: Replace placeholders
    
    # Run sic/soc lookup
    sic_soc = build_sic_to_soc()

    # TODO: Import 5 digit SIC from Bres data
    sic_soc = # TODO: 5 digit split

    # Overlay - bring though SIC/SOC splits
    attraction_employments = None # Placeholder

    # Export to Y:/Data or a database

    return(attraction_employments)


