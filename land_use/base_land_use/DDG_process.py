# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 2022

@author: yanzhu
Version number:

Written using: Python 3.9

Module versions used for writing:
    pandas v1.4.4
    numpy v1.23.1

Purpose of this python file to adjust Base year 2018 pop output to be aligned to DDG 2018
"""

import pandas as pd
import numpy as np
import os
#from ipfn import ipfn
import datetime
#import pyodbc
#import geopandas as gpd
from land_use.utils import file_ops as utils
from land_use.utils import compress
from land_use import lu_constants
import logging

# Other paths
_Zone_LA_path = 'Lookups/MSOA_1991LA_2011LA_2021LA_LAgroups.csv'
_normits_seg_to_tfn_tt_path = 'Lookups\NorMITs_segments_to_TfN_tt\normits_segs_to_tfn_tt.csv'

# Set Model Year
# Model year could be any year beyond 2018, for DDG compliant process, model year is 2018
ModelYear = '2018'


# Directory and file paths for the DDG
# Directory Paths
DDG_CAS_regional_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG'
                                       ' '.join(['CAS', 'Regional', 'Scenario']), 'Drivers')
DDG_CAS_low_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG'
                                       ' '.join(['CAS', 'Low']))
DDG_CAS_high_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG'
                                       ' '.join(['CAS', 'High']))

# File names
DDG_CAS_regional_pop_path = '_'.join(['DD', 'Nov21', 'CASReg', 'Pop', 'LA.csv'])
DDG_CAS_regional_wkrfrac_path = '_'.join(['DD', 'Nov21', 'CASReg', 'frac{WOR}{WAP}', 'LA.csv'])

DDG_CAS_low_pop_path = '_'.join(['DD', 'Nov21', 'CASLo', 'Pop', 'LA.csv'])
DDG_CAS_low_wkrfrac_path = '_'.join(['DD', 'Nov21', 'CASLo', 'frac{WOR}{WAP}', 'LA.csv'])

DDG_CAS_high_pop_path = '_'.join(['DD', 'Nov21', 'CASHi', 'Pop', 'LA.csv'])
DDG_CAS_high_wkrfrac_path = '_'.join(['DD', 'Nov21', 'CASHi', 'frac{WOR}{WAP}', 'LA.csv'])

# Function based audit/process directory names
DDG_process_dir = '3.2.12_process_DDG_data'

# Process/audit/output directory name
process_dir = '01 Process'
audit_dir = '02 Audits'
output_dir = '03 Outputs'

def DDGaligned_pop_process(by_lu_obj):
    logging.info('Running Step 3.2.12')
    print('Running Step 3.2.12')
    output_folder = by_lu_obj.home_folder
    # Distrctory and file from base year population process
    output_working_dir_path = os.path.join(output_folder, output_dir)
    BYpop_process_output_file = os.path.join(output_working_dir_path, ''.join(['output_3_resi_gb_lad_msoa_tfn_tt_agg_prt_',
                                                                            ModelYear, '_hh_pop']))
    BYpop_MYE = compress.read_in(BYpop_process_output_file)
    logging.info('MYE complied population currently {}'.format(BYpop_MYE.people.sum()))
    # get 2011 LA in
    Zone_2011LA = pd.read_csv(
        os.path.join(
            by_lu_obj.import_folder, _Zone_LA_path))[['NorMITs Zone', '2011 LA', '2021 LA Name']]
    BYpop_MYE = BYpop_MYE.merge(Zone_2011LA, how='left',
                        left_on=['z'],
                        right_on=['NorMITs Zone']).drop(columns={'NorMITs Zone'})
    BYpop_MYE = BYpop_MYE.rename(columns={'2011 LA': '2011_LA_code', '2011 LA Name': '2011_LA_Name'})

    # get correspondence table between tfn_tt and NorMITs segs a,g,h,e,n,s
    tfn_tt_segs = pd.read_csv(
        os.path.join(
            by_lu_obj.import_folder, _normits_seg_to_tfn_tt_path))
    BYpop_MYE = BYpop_MYE.merge(tfn_tt_segs, how='left', on=['tfn_tt'])

    #define NWAP, and wkr and nwkr within WAP
    WAP = {
    1: 'wkr', # full-time worker
    2: 'wkr', # part-time worker
    3: 'nwkr', # student not in any employment
    4: 'nwkr', #other unemployed
    5: 'nwap' # children and elderly outside 16-74
    }
    BYpop_MYE['WAP'] = BYpop_MYE['e'].map(WAP)

    # sum up LAD total of MYE compiled base year pop
    BYpop_MYE_LAD = BYpop_MYE.groupby(['2011_LA_code'])[['people']].sum().reset_index()

    # sum up LAD total WAP

    # get DDG pop for base year 2018 to adjust all population for base year
    pop_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_CAS_regional_directory, DDG_CAS_regional_pop_path))[['LAD13CD', ModelYear]]

    BYpop_MYE_LAD = BYpop_MYE_LAD.merge(pop_DDG_LAD, how='left',
                        left_on=['2011_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    BYpop_MYE_LAD = BYpop_MYE_LAD.rename(columns={ModelYear: 'DDG_pop'})
    BYpop_MYE_LAD['pop_aj_fac'] = BYpop_MYE_LAD['DDG_pop'] / BYpop_MYE_LAD['people']
    BYpop_MYE_LAD['pop_aj_fac'] = BYpop_MYE_LAD['pop_aj_fac'].fillna(1)
    BYpop_MYE_LAD_fac = BYpop_MYE_LAD[['2011_LA_code', 'pop_aj_fac']]
    BYpop_DDG = BYpop_MYE.merge(BYpop_MYE_LAD_fac, how='left', on=['2011_LA_code'])
    BYpop_DDG['pop_aj'] = BYpop_DDG['people'] * BYpop_DDG['pop_aj_fac']
    logging.info('DDG aligned population currently {}'.format(BYpop_DDG.pop_aj.sum()))







