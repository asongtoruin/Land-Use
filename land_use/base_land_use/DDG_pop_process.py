# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 2022

@author: yanzhu
Version number:

Written using: Python 3.9

Module versions used for writing:
    pandas v1.5.0
    numpy v1.23.3

Purpose of this python file to adjust Base year 2018 pop output to be aligned to DDG 2018
"""

import pandas as pd
import numpy as np
import os
#from ipfn import ipfn
import datetime
#import pyodbc
#import geopandas as gpd
#from land_use.utils import file_ops as utils
from land_use.utils import compress
#from land_use import lu_constants
import logging

# Other paths
_Zone_LA_path = 'Lookups/MSOA_1991LA_2011LA_2021LA_LAgroups.csv'
normits_seg_to_tfn_tt_file = r'I:\NorMITs Land Use\import\Lookups\NorMITs_segments_to_TfN_tt\normits_segs_to_tfn_tt.csv'

# Set Model Year
# Model year could be any year beyond 2018, for DDG compliant process, model year is 2018
ModelYear = '2018'


# Directory and file paths for the DDG
# Directory Paths
# YZ - make regional, Low, High as variable later
DDG_CAS_regional_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG',
                                       ' '.join(['CAS', 'Regional', 'Scenario']), 'Drivers')
DDG_CAS_low_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG',
                                       ' '.join(['CAS', 'Low']))
DDG_CAS_high_directory = os.path.join(r'I:\NorMITs Land Use', 'import', 'DDG',
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
    BYpop_process_output_file = os.path.join(output_working_dir_path, ''.join(['output_6_resi_gb_msoa_tfn_tt_prt_',
                                                                            ModelYear, '_pop']))
    BYpop_MYE = compress.read_in(BYpop_process_output_file)
    logging.info('Initial check on MYE complied population currently {}'.format(BYpop_MYE.people.sum()))

    # get 2013 LA in
    Zone_2013LA = pd.read_csv(
        os.path.join(
            by_lu_obj.import_folder, _Zone_LA_path))[['NorMITs Zone', '2013 LA', '2013 LA Name']]
    BYpop_MYE = BYpop_MYE.merge(Zone_2013LA, how='left',
                        left_on=['z'],
                        right_on=['NorMITs Zone']).drop(columns={'NorMITs Zone'})
    BYpop_MYE = BYpop_MYE.rename(columns={'2013 LA': '2013_LA_code', '2013 LA Name': '2013_LA_name'})

    # get correspondence table between tfn_tt and NorMITs segs a,g,h,e,n,s
    tfn_tt_segs = pd.read_csv(normits_seg_to_tfn_tt_file)
    BYpop_MYE = BYpop_MYE.merge(tfn_tt_segs, how='left', on=['tfn_tt'])

    #define NWAP, and wkr and nwkr within WAP
    wkr = {
        1: 'wkr', # full-time worker
        2: 'wkr', # part-time worker
        3: 'nwkr', # student not in any employment
        4: 'nwkr', #other unemployed
        5: 'nwap' # children and elderly outside 16-74
        }
    BYpop_MYE['worker_type'] = BYpop_MYE['e'].map(wkr)

    # sum up LAD total of MYE compiled base year pop
    BYpop_MYE_LAD = BYpop_MYE.groupby(['2013_LA_code'])[['people']].sum().reset_index()
    logging.info('MYE complied pop aggregated from LAD currently {}'.format(BYpop_MYE_LAD.people.sum()))
    BYpop_MYE_LAD = BYpop_MYE_LAD.rename(columns={'people': 'pop_MYE'})
    # sum up LAD total of WAP and wkr from MYE compiled base year pop
    BYpop_MYE_agg_da = BYpop_MYE.groupby(['2013_LA_code', 'a'])[['people']].sum().reset_index()
    BYWAP_MYE_LAD = BYpop_MYE_agg_da.loc[(BYpop_MYE_agg_da['a'] == 2)]
    logging.info('MYE complied WAP currently {}'.format(BYWAP_MYE_LAD.people.sum()))
    BYWAP_MYE_LAD = BYWAP_MYE_LAD.rename(columns={'people': 'WAP_MYE'})

    BYpop_MYE_agg_dw = BYpop_MYE.groupby(['2013_LA_code', 'worker_type'])[['people']].sum().reset_index()
    BYwkr_MYE_LAD = BYpop_MYE_agg_dw.loc[(BYpop_MYE_agg_dw['worker_type'] =='wkr')]
    logging.info('MYE complied wkr currently {}'.format(BYwkr_MYE_LAD.people.sum()))
    BYwkr_MYE_LAD = BYwkr_MYE_LAD.rename(columns={'people': 'wkr_MYE'})

    #Merge LAD WAP and worker with total pop from BY MYE compiled process
    # Columns in df BYpop_MYE_LAD after merging is: ['2013_LA_code','pop_MYE','WAP_MYE','wkr_MYE']
    BYWAP_MYE_LAD = BYWAP_MYE_LAD.merge(BYwkr_MYE_LAD, how='left',
                                        on=['2013_LA_code']).drop(columns={'a','worker_type'})
    BYpop_MYE_LAD = BYpop_MYE_LAD.merge(BYWAP_MYE_LAD, how='left', on=['2013_LA_code'])

    # addtional three columns created-- work out ration of worker over total pop as well as over total WAP from MYE
    BYpop_MYE_LAD['nwkr_MYE'] = BYpop_MYE_LAD['WAP_MYE'] - BYpop_MYE_LAD['wkr_MYE']
    BYpop_MYE_LAD['fact_wkr_pop_MYE'] = BYpop_MYE_LAD['wkr_MYE'] / BYpop_MYE_LAD['pop_MYE']
    BYpop_MYE_LAD['fact_wkr_WAP_MYE'] = BYpop_MYE_LAD['wkr_MYE'] / BYpop_MYE_LAD['WAP_MYE']

    # audit1- dump df BYpop_MYE_LAD for checking purpose
    pop_MYE_LAD_audit = BYpop_MYE_LAD.copy()
    pop_MYE_LAD_audit_path = os.path.join(by_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        DDG_process_dir,
                                        '_'.join(['audit_1_gb_LAD_mye', ModelYear, 'pop.csv']))
    pop_MYE_LAD_audit.to_csv(pop_MYE_LAD_audit_path, index=False)


    # get DDG pop for base year 2018 to adjust all population for base year
    pop_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_CAS_regional_directory, DDG_CAS_regional_pop_path))[['LAD13CD', ModelYear]]
    # get DDG proportion of worker over total WAP for base year
    wrkfac_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_CAS_regional_directory, DDG_CAS_regional_wkrfrac_path))[['LAD13CD', ModelYear]]


    # Adjustments: step 1 to make sure pop by segs are scaled to meet DDG LAD totals;
    # Adjustments: step 2 to incorporate worker ratio into scaled WAP to produce worker;
    # Adjustment1:
    #Merge LAD population with DDG population
    BYpop_MYE_LAD = BYpop_MYE_LAD.merge(pop_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    BYpop_MYE_LAD = BYpop_MYE_LAD.rename(columns={ModelYear: 'pop_DDG'})
    #calculate adjustment factor on total pop per LAD
    BYpop_MYE_LAD['pop_aj_fac'] = BYpop_MYE_LAD['pop_DDG'] / BYpop_MYE_LAD['pop_MYE']
    BYpop_MYE_LAD['pop_aj_fac'] = BYpop_MYE_LAD['pop_aj_fac'].fillna(1)
    BYpop_MYE_LAD_fac = BYpop_MYE_LAD[['2013_LA_code', 'pop_aj_fac']]
    BYpop_DDG = BYpop_MYE.merge(BYpop_MYE_LAD_fac, how='left', on=['2013_LA_code'])

    #scale MYE pop by segments to be compliant with DDG
    BYpop_DDG['pop_DDG_aj1'] = BYpop_DDG['people'] * BYpop_DDG['pop_aj_fac']
    BYpop_DDG = BYpop_DDG.rename(columns={'people': 'pop_MYE'})
    logging.info('DDG population after adjustment 1 currently {}'.format(BYpop_DDG.pop_DDG_aj1.sum()))

    # sum up LAD total of DDG aj1 base year pop
    BYpop_DDG_LAD = BYpop_DDG.groupby(['2013_LA_code'])[['pop_DDG_aj1']].sum().reset_index()
    logging.info('DDG aj1 pop aggregated from LAD currently {}'.format(BYpop_DDG_LAD.pop_DDG_aj1.sum()))

    # sum LAD level WAP and wkr based on DDG_pop_aj1
    BYpop_DDG_agg_da = BYpop_DDG.groupby(['2013_LA_code', 'a'])[['pop_DDG_aj1']].sum().reset_index()
    BYWAP_DDG_LAD = BYpop_DDG_agg_da.loc[(BYpop_DDG_agg_da['a'] == 2)]
    logging.info('DDG aj1 WAP currently {}'.format(BYWAP_DDG_LAD.pop_DDG_aj1.sum()))
    BYWAP_DDG_LAD = BYWAP_DDG_LAD.rename(columns={'pop_DDG_aj1': 'WAP_DDG_aj1'})
    BYpop_DDG_agg_dw = BYpop_DDG.groupby(['2013_LA_code', 'worker_type'])[['pop_DDG_aj1']].sum().reset_index()
    BYwkr_DDG_LAD = BYpop_DDG_agg_dw.loc[(BYpop_DDG_agg_dw['worker_type'] =='wkr')]
    logging.info('DDG aj1 worker currently {}'.format(BYwkr_DDG_LAD.pop_DDG_aj1.sum()))
    BYwkr_DDG_LAD = BYwkr_DDG_LAD.rename(columns={'pop_DDG_aj1': 'wkr_DDG_aj1'})

    # Merge LAD WAP and worker with total pop from BY DDG aj1 process
    # Columns in df BYpop_DDG_LAD after merging is: ['2013_LA_code','pop_DDG_aj1','pop_DDG_aj1','wkr_DDG_aj1']
    BYWAP_DDG_LAD = BYWAP_DDG_LAD.merge(BYwkr_DDG_LAD, how='left',
                                        on=['2013_LA_code']).drop(columns={'a','worker_type'})
    BYpop_DDG_LAD = BYpop_DDG_LAD.merge(BYWAP_DDG_LAD, how='left', on=['2013_LA_code'])

    # addtional three columns created-- work out ration of worker over total pop as well as over total WAP from DDG aj1
    BYpop_DDG_LAD['nwkr_DDG_aj1'] = BYpop_DDG_LAD['WAP_DDG_aj1'] - BYpop_DDG_LAD['wkr_DDG_aj1']
    BYpop_DDG_LAD['fact_wkr_pop_DDGaj1'] = BYpop_DDG_LAD['wkr_DDG_aj1'] / BYpop_DDG_LAD['pop_DDG_aj1']
    BYpop_DDG_LAD['fact_wkr_WAP_DDGaj1'] = BYpop_DDG_LAD['wkr_DDG_aj1'] / BYpop_DDG_LAD['WAP_DDG_aj1']

    # Adjustment2:
    # Merge LAD WAP and worker with DDG wkr ratio over WAP
    BYpop_DDG_LAD = BYpop_DDG_LAD.merge(wrkfac_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    BYpop_DDG_LAD = BYpop_DDG_LAD.rename(columns={ModelYear: 'wrkfac_DDG'})
    BYpop_DDG_LAD['wkr_DDG_aj2'] = BYpop_DDG_LAD['WAP_DDG_aj1'] * BYpop_DDG_LAD['wrkfac_DDG']
    BYpop_DDG_LAD['nwkr_DDG_aj2'] = BYpop_DDG_LAD['WAP_DDG_aj1'] - BYpop_DDG_LAD['wkr_DDG_aj2']

    # audit2- dump df BYpop_MYE_LAD for checking purpose
    pop_DDG_LAD_audit = BYpop_DDG_LAD.copy()
    pop_DDG_LAD_audit_path = os.path.join(by_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        DDG_process_dir,
                                        '_'.join(['audit_2_gb_LAD_DDG', ModelYear, 'pop.csv']))
    pop_DDG_LAD_audit.to_csv(pop_DDG_LAD_audit_path, index=False)

    # Calculate adjustment factor by worker_type per LAD
    BYpop_DDG_LAD['wkr_aj_fac'] = BYpop_DDG_LAD['wkr_DDG_aj2'] / BYpop_DDG_LAD['wkr_DDG_aj1']
    BYpop_DDG_LAD['wkr_aj_fac'] = BYpop_DDG_LAD['wkr_aj_fac'].fillna(1)
    BYpop_DDG_LAD['nwkr_aj_fac'] = BYpop_DDG_LAD['nwkr_DDG_aj2'] / BYpop_DDG_LAD['nwkr_DDG_aj1']
    BYpop_DDG_LAD['nwkr_aj_fac'] = BYpop_DDG_LAD['nwkr_aj_fac'].fillna(1)
    BYpop_DDG_LAD_fac = BYpop_DDG_LAD[['2013_LA_code', 'wkr_aj_fac', 'nwkr_aj_fac']]
    BYpop_DDG_LAD_fac = BYpop_DDG_LAD_fac.rename(columns={'wkr_aj_fac': 'wkr', 'nwkr_aj_fac': 'nwkr'})
    BYpop_DDG_LAD_fac = BYpop_DDG_LAD_fac.melt(id_vars=['2013_LA_code'], var_name='worker_type', value_name='aj2_fac')
    BYpop_DDG_LAD_fac_append = BYpop_DDG_LAD[['2013_LA_code']]
    BYpop_DDG_LAD_fac_append['worker_type'] = "nwap"
    BYpop_DDG_LAD_fac_append['aj2_fac'] = 1
    BYpop_DDG_LAD_fac = BYpop_DDG_LAD_fac.append(BYpop_DDG_LAD_fac_append).reset_index()

    # audit3- dump aj_factor
    ajfac_DDG_LAD_audit = BYpop_DDG_LAD_fac.copy()
    ajfac_DDG_LAD_audit_path = os.path.join(by_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        DDG_process_dir,
                                        '_'.join(['audit_3_gb_LAD', ModelYear, 'ajfac.csv']))
    ajfac_DDG_LAD_audit.to_csv(ajfac_DDG_LAD_audit_path, index=False)

    # scale DDG aj1 pop by worker_type to be compliant with DDG on worker and non worker
    BYpop_DDG = BYpop_DDG.merge(BYpop_DDG_LAD_fac, how='left', on=['2013_LA_code', 'worker_type'])

    BYpop_DDG['pop_DDG_aj2'] = BYpop_DDG['pop_DDG_aj1'] * BYpop_DDG['aj2_fac']
    logging.info('DDG population after adjustment 2 currently {}'.format(BYpop_DDG.pop_DDG_aj2.sum()))

    # audit4
    # check LAD level pop is consistent with DDG LAD
    BYpop_DDG_LAD_audit = BYpop_DDG.groupby(['2013_LA_code'])[['pop_MYE', 'pop_DDG_aj1', 'pop_DDG_aj2']].sum().reset_index()
    BYpop_DDG_LAD_audit = BYpop_DDG_LAD_audit.merge(pop_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    BYpop_DDG_LAD_audit = BYpop_DDG_LAD_audit.rename(columns={ModelYear: 'pop_DDG'})
    BYpop_DDG_LAD_audit['pop_deviation'] = BYpop_DDG_LAD_audit['pop_DDG_aj2']/BYpop_DDG_LAD_audit['pop_DDG']-1
    logging.info('The min %age diff is ' + str(BYpop_DDG_LAD_audit['pop_deviation'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(BYpop_DDG_LAD_audit['pop_deviation'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(BYpop_DDG_LAD_audit['pop_deviation'].mean() * 100) + '%')
    logging.info('The overall deviation is ' + str(
        BYpop_DDG_LAD_audit['pop_DDG_aj2'].sum() - BYpop_DDG_LAD_audit['pop_DDG'].sum()) + ' people')
    BYpop_DDG_LAD_audit_path = os.path.join(by_lu_obj.out_paths['write_folder'],
                                           audit_dir,
                                           DDG_process_dir,
                                           '_'.join(['audit_4_gb_lad', ModelYear, 'check_pop.csv']))
    BYpop_DDG_LAD_audit.to_csv(BYpop_DDG_LAD_audit_path, index=False)
    # Auditing text for Step 3.2.12 pop process
    audit_3_2_12_header = '\n'.join(['Audit for  Step 3.2.12',
                                     'Created ' + str(datetime.datetime.now())])
    audit_3_2_12_text = '\n'.join(['The total ' + ModelYear + ' population at the end of the running process is:',
                                   '\t' + str(BYpop_DDG['pop_DDG_aj2'].sum()),
                                   'Checking final district total population against DDG district population:',
                                   '\tThe min %age diff is ' + str(
                                       BYpop_DDG_LAD_audit['pop_deviation'].min() * 100) + '%',
                                   '\tThe max %age diff is ' + str(
                                       BYpop_DDG_LAD_audit['pop_deviation'].max() * 100) + '%',
                                   '\tThe mean %age diff is ' + str(
                                       BYpop_DDG_LAD_audit['pop_deviation'].mean() * 100) + '%',
                                   'The overall deviation is ' + str(
                                       BYpop_DDG_LAD_audit['pop_DDG_aj2'].sum() -
                                       BYpop_DDG_LAD_audit['pop_DDG'].sum()) + ' people',
                                   'All of the above values should be equal (or close) to 0.',
                                   'A full breakdown of the ' + ModelYear + 'population by d can be found at:',
                                   BYpop_DDG_LAD_audit_path])
    audit_3_2_12_content = '\n'.join([audit_3_2_12_header, audit_3_2_12_text])
    audit_3_2_12_path = os.path.join(by_lu_obj.out_paths['write_folder'],
                                     audit_dir,
                                     DDG_process_dir,
                                     ''.join(['Audit_3.2.12_', ModelYear, '.txt']))
    with open(audit_3_2_12_path, 'w') as text_file:
        text_file.write(audit_3_2_12_content)

    # Format ouputs
    BYpop_DDG = BYpop_DDG.rename(columns={'pop_DDG_aj2': 'people'})
    BYpop_DDG_out = BYpop_DDG[['2013_LA_code', '2013_LA_name', 'z', 'MSOA', 'tfn_tt', 't','people']]

    #Also groupby this output by removing t
    groupby_cols = ['2013_LA_code', '2021_LA_code', 'z', 'MSOA', 'tfn_tt']
    BYpop_DDG_exc_t_out = BYpop_DDG_out.groupby(groupby_cols)['people'].sum().reset_index()

    #Dump outputs
    DDG_aligned_pop_output_path = os.path.join(by_lu_obj.out_paths['write_folder'], output_dir)
    BYpop_DDG_pop_allsegs_filename = '_'.join(['output_8_DDG_resi_gb_msoa_tfn_tt', ModelYear, 'pop'])
    BYpop_DDG_pop_tfn_tt_filename = '_'.join(['output_9_DDG_resi_gb_msoa_tfn', ModelYear, 'pop'])

    BYpop_DDG_pop_allsegs_path = os.path.join(DDG_aligned_pop_output_path, BYpop_DDG_pop_allsegs_filename)
    BYpop_DDG_pop_tfn_tt_path = os.path.join(DDG_aligned_pop_output_path, BYpop_DDG_pop_tfn_tt_filename)
    compress.write_out(BYpop_DDG_out, BYpop_DDG_pop_allsegs_path)
    compress.write_out(BYpop_DDG_exc_t_out, BYpop_DDG_pop_tfn_tt_path)
    by_lu_obj.state['3.2.12_process_DDG_data'] = 1
    logging.info('Step 3.2.12 completed')
    print('Step 3.2.12 completed')

















