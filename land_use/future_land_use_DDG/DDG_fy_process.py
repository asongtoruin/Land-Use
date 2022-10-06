# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 2022

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
import pyodbc
#import geopandas as gpd
#from land_use.utils import file_ops as utils
from land_use.utils import compress
#from land_use import lu_constants
import logging

# Other paths
Zone_LA_path = '/Lookups/MSOA_1991LA_2011LA_2013LA_2021LA_LAgroups.csv'
normits_seg_to_tfn_tt_file = r'I:\NorMITs Land Use\import\Lookups\NorMITs_segments_to_TfN_tt\normits_segs_to_tfn_tt.csv'

# Process/audit/output directory name
process_dir = '01 Process'
audit_dir = '02 Audits'
output_dir = '03 Outputs'


def ntem_fy_pop_growthfactor(fy_lu_obj):
    logging.info('Calculating growth factor of NTEM pop')
    print('Calculating growth factor of NTEM pop')
    base_year = fy_lu_obj.base_year
    future_year = fy_lu_obj.future_year
    Pop_Segmentation_path = fy_lu_obj.import_folder + '/CTripEnd/Pop_Segmentations.csv'
    ntem_allyear_path = fy_lu_obj.import_folder + '/CTripEnd/All_year/ntem_gb_z_ntem_tt_allyear_pop.csv.bz2'
    # read in ntem pop
    Pop_Segmentation = pd.read_csv(Pop_Segmentation_path)
    ntem_pop = pd.read_csv(ntem_allyear_path)
    print(ntem_pop)
    # read in future year ntem pop
    ntem_pop_growfac = ntem_pop[['z', 'tt', base_year, future_year]]
    ntem_pop_growfac = ntem_pop_growfac.merge(Pop_Segmentation, left_on=['tt'], right_on=['NTEM_Traveller_Type'],
                                              how='right').drop(columns={'NTEM_Traveller_Type'})
    print(ntem_pop_growfac)
    ntem_pop_growfac = ntem_pop_growfac.rename(columns={'Age_code': 'a',
                                              'Gender_code': 'g',
                                              'Household_composition_code': 'h',
                                              'Employment_type_code': 'e'})
    cols_chosen = ['z', 'tt', 'a', 'g', 'h', 'e', base_year, future_year]
    ntem_pop_growfac = ntem_pop_growfac[cols_chosen]
    ntem_pop_growfac['growthfac'] = ntem_pop_growfac[future_year] / ntem_pop_growfac[base_year]
    ntem_pop_growfac['growthfac'] = ntem_pop_growfac['growthfac'].fillna(1)



    ntem_pop_growfac['z'] = ntem_pop_growfac['z'].astype(int)
    ntem_pop_growfac_iterator = zip(ntem_pop_growfac['z'],
                               ntem_pop_growfac['a'],
                               ntem_pop_growfac['g'],
                               ntem_pop_growfac['h'],
                               ntem_pop_growfac['e'])

    ntem_pop_growfac['zaghe_Key'] = ['_'.join([str(z), str(a), str(g), str(h), str(e)])
                               for z, a, g, h, e in ntem_pop_growfac_iterator]
    ntem_pop_growfac_cols = ['zaghe_Key', 'growthfac']
    ntem_pop_growfac = ntem_pop_growfac[ntem_pop_growfac_cols]
    #output growth factor for future year
    Gfactor_Output = '_'.join(['ntem_gb_zaghe', future_year, 'growth_factor.csv'])
    ntem_pop_growfac.to_csv(os.path.join(fy_lu_obj.out_paths['write_folder'],
                                         process_dir,
                                         Gfactor_Output), index=False)
    logging.info('ntem_fy_pop_growthfactor function complete')
    print('ntem_fy_pop_growthfactor function complete')

def base_year_pop(fy_lu_obj):
    logging.info('Getting dimension details for base year pop')
    print('Getting dimension details for base year pop')
    base_year = fy_lu_obj.base_year
    # future_year = fy_lu_obj.future_year
    by_output_folder = fy_lu_obj.by_home_folder
    # fy_output_folder = fy_lu_obj.fy_home_folder
    # Distrctory and file from base year population process
    output_working_dir_path = os.path.join(by_output_folder, output_dir)
    BYpop_process_output_file = os.path.join(output_working_dir_path, ''.join(['output_6_resi_gb_msoa_tfn_tt_prt_',
                                                                            base_year, '_pop']))
    BYpop_MYE_pre = compress.read_in(BYpop_process_output_file)
    logging.info('Initial check on MYE complied population currently {}'.format(BYpop_MYE_pre.people.sum()))
    # get 2013 LA in
    _2013LA_path = fy_lu_obj.import_folder + Zone_LA_path
    Zone_2013LA = pd.read_csv(_2013LA_path)[['NorMITs Zone', '2013 LA', '2013 LA Name']]
    BYpop_MYE_pre = BYpop_MYE_pre.merge(Zone_2013LA, how='left',
                        left_on=['z'],
                        right_on=['NorMITs Zone']).drop(columns={'NorMITs Zone'})
    BYpop_MYE_pre = BYpop_MYE_pre.rename(columns={'2013 LA': '2013_LA_code', '2013 LA Name': '2013_LA_name', 'people': 'pop_by'})

    # get correspondence table between tfn_tt and NorMITs segs a,g,h,e,n,s
    tfn_tt_segs = pd.read_csv(normits_seg_to_tfn_tt_file)
    BYpop_MYE_pre = BYpop_MYE_pre.merge(tfn_tt_segs, how='left', on=['tfn_tt'])
    BYpop_MYE_pre['z'] = BYpop_MYE_pre['z'].astype(int)
    BYpop_MYE_pre_iterator = zip(BYpop_MYE_pre['z'],
                             BYpop_MYE_pre['a'],
                             BYpop_MYE_pre['g'],
                             BYpop_MYE_pre['h'],
                             BYpop_MYE_pre['e'])

    BYpop_MYE_pre['zaghe_Key'] = ['_'.join([str(z), str(a), str(g), str(h), str(e)])
                                     for z, a, g, h, e in BYpop_MYE_pre_iterator]

    BYpop_MYE_pre = BYpop_MYE_pre[['2013_LA_code', '2013_LA_name', 'z', 'MSOA',
                           'tfn_tt', 't', 'zaghe_Key', 'a', 'g', 'h', 'e', 'n', 's', 'pop_by']]
    BYpop_MYE_name = '_'.join(['output_0_MYE_allsegs', base_year, 'pop.csv.bz2'])
    BYpop_MYE_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                  output_dir,
                                  BYpop_MYE_name)
    BYpop_MYE_pre.to_csv(BYpop_MYE_path)
    logging.info('Step completed-- base_year_pop')
    print('Step completed -- base_year_pop')
    return 0

def DDGaligned_fy_pop_process(fy_lu_obj):
    logging.info('Processing fy pop to be aligned with DDG')
    print('Processing fy pop to be aligned with DDG')
    base_year = fy_lu_obj.base_year
    future_year = fy_lu_obj.future_year
    # by_output_folder = fy_lu_obj.by_home_folder
    fy_output_folder = fy_lu_obj.fy_home_folder
    scenario_name = fy_lu_obj.scenario_name
    CAS_scen = fy_lu_obj.CAS_scen

    # Directory and file paths for the DDG
    # Directory Paths
    DDG_directory = os.path.join(fy_lu_obj.import_folder, 'DDG',
                                 ' '.join(['CAS', scenario_name]))
    # File names
    DDG_pop_path = '_'.join(['DD', 'Nov21', CAS_scen, 'Pop', 'LA.csv'])
    DDG_wkrfrac_path = '_'.join(['DD', 'Nov21', CAS_scen, 'frac{WOR}{WAP}', 'LA.csv'])



    # Directiry and file paths for ntem growth factor
    ntem_growth_path = os.path.join(fy_output_folder, process_dir)
    ntem_growth_file = '_'.join(['ntem_gb_zaghe', future_year, 'growth_factor.csv'])
    ntem_growth_factor = pd.read_csv(
        os.path.join(
            ntem_growth_path, ntem_growth_file))
    # Directiry and file paths for by pop
    output_working_dir_path = os.path.join(fy_output_folder, output_dir)
    BYpop_process_output_file = os.path.join(output_working_dir_path, '_'.join(['output_0_MYE_allsegs', base_year, 'pop.csv.bz2']))
    BYpop_MYE = pd.read_csv(BYpop_process_output_file)

    # merge by_pop with ntem growth factor to get base pop adjusted for future year
    FYpop_MYE = BYpop_MYE.copy()
    FYpop_MYE = FYpop_MYE.merge(ntem_growth_factor, how='left', on=['zaghe_Key'])
    FYpop_MYE['pop_fy'] = FYpop_MYE['pop_by'] * FYpop_MYE['growthfac']

    #define NWAP, and wkr and nwkr within WAP
    wkr = {
        1: 'wkr', # full-time worker
        2: 'wkr', # part-time worker
        3: 'nwkr', # student not in any employment
        4: 'nwkr', #other unemployed
        5: 'nwap' # children and elderly outside 16-74
        }
    FYpop_MYE['worker_type'] = FYpop_MYE['e'].map(wkr)

    # sum up LAD total of MYE compiled ntem scaled future year pop
    FYpop_MYE_LAD = FYpop_MYE.groupby(['2013_LA_code'])[['pop_fy']].sum().reset_index()
    logging.info('MYE complied fy pop aggregated from LAD currently {}'.format(FYpop_MYE_LAD.pop_fy.sum()))
    # sum up LAD total of WAP and wkr from MYE compiled base year pop
    FYpop_MYE_agg_da = FYpop_MYE.groupby(['2013_LA_code', 'a'])[['pop_fy']].sum().reset_index()
    FYWAP_MYE_LAD = FYpop_MYE_agg_da.loc[(FYpop_MYE_agg_da['a'] == 2)]
    logging.info('MYE complied fy WAP currently {}'.format(FYWAP_MYE_LAD.pop_fy.sum()))
    FYWAP_MYE_LAD = FYWAP_MYE_LAD.rename(columns={'pop_fy': 'WAP_fy'})


    FYpop_MYE_agg_dw = FYpop_MYE.groupby(['2013_LA_code', 'worker_type'])[['pop_fy']].sum().reset_index()
    FYwkr_MYE_LAD = FYpop_MYE_agg_dw.loc[(FYpop_MYE_agg_dw['worker_type'] =='wkr')]
    logging.info('MYE complied fy wkr currently {}'.format(FYwkr_MYE_LAD.pop_fy.sum()))
    FYwkr_MYE_LAD = FYwkr_MYE_LAD.rename(columns={'pop_fy': 'wkr_fy'})

    #Merge LAD WAP and worker with total pop from BY MYE compiled process
    # Columns in df FYWAP_MYE_LAD after merging is: ['2013_LA_code','pop_fy','WAP_fy','wkr_fy']
    FYWAP_MYE_LAD = FYWAP_MYE_LAD.merge(FYwkr_MYE_LAD, how='left',
                                        on=['2013_LA_code']).drop(columns={'a', 'worker_type'})
    FYpop_MYE_LAD = FYpop_MYE_LAD.merge(FYWAP_MYE_LAD, how='left', on=['2013_LA_code'])

    # addtional three columns created-- work out ration of worker over total pop as well as over total WAP from MYE
    FYpop_MYE_LAD['nwkr_fy'] = FYpop_MYE_LAD['WAP_fy'] - FYpop_MYE_LAD['wkr_fy']
    FYpop_MYE_LAD['fact_wkr_pop_fy'] = FYpop_MYE_LAD['wkr_fy'] / FYpop_MYE_LAD['pop_fy']
    FYpop_MYE_LAD['fact_wkr_WAP_fy'] = FYpop_MYE_LAD['wkr_fy'] / FYpop_MYE_LAD['WAP_fy']

    # audit1- dump df FYpop_MYE_LAD for checking purpose
    pop_MYE_LAD_audit = FYpop_MYE_LAD.copy()
    pop_MYE_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        '_'.join(['audit_1_gb_LAD_mye', future_year, 'pop.csv']))
    pop_MYE_LAD_audit.to_csv(pop_MYE_LAD_audit_path, index=False)


    # get DDG pop for base year 2018 to adjust all population for base year
    pop_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_directory, DDG_pop_path))[['LAD13CD', future_year]]
    # get DDG proportion of worker over total WAP for base year
    wrkfac_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_directory, DDG_wkrfrac_path))[['LAD13CD', future_year]]


    # Adjustments: step 1 to make sure pop by segs are scaled to meet DDG LAD totals;
    # Adjustments: step 2 to incorporate worker ratio into scaled WAP to produce worker;
    # Adjustment1:
    #Merge LAD population with DDG population
    FYpop_MYE_LAD = FYpop_MYE_LAD.merge(pop_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    FYpop_MYE_LAD = FYpop_MYE_LAD.rename(columns={future_year: 'pop_fy_DDG'})
    #calculate adjustment factor on total pop per LAD
    FYpop_MYE_LAD['pop_aj_fac'] = FYpop_MYE_LAD['pop_fy_DDG'] / FYpop_MYE_LAD['pop_fy']
    FYpop_MYE_LAD['pop_aj_fac'] = FYpop_MYE_LAD['pop_aj_fac'].fillna(1)
    FYpop_MYE_LAD_fac = FYpop_MYE_LAD[['2013_LA_code', 'pop_aj_fac']]
    FYpop_DDG = FYpop_MYE.merge(FYpop_MYE_LAD_fac, how='left', on=['2013_LA_code'])

    #scale MYE pop by segments to be compliant with DDG
    FYpop_DDG['pop_DDG_aj1'] = FYpop_DDG['pop_fy'] * FYpop_DDG['pop_aj_fac']
    logging.info('DDG population after adjustment 1 currently {}'.format(FYpop_DDG.pop_DDG_aj1.sum()))

    # sum up LAD total of DDG aj1 base year pop
    FYpop_DDG_LAD = FYpop_DDG.groupby(['2013_LA_code'])[['pop_DDG_aj1']].sum().reset_index()
    logging.info('DDG aj1 pop aggregated from LAD currently {}'.format(FYpop_DDG_LAD.pop_DDG_aj1.sum()))

    # sum LAD level WAP and wkr based on DDG_pop_aj1
    FYpop_DDG_agg_da = FYpop_DDG.groupby(['2013_LA_code', 'a'])[['pop_DDG_aj1']].sum().reset_index()
    FYWAP_DDG_LAD = FYpop_DDG_agg_da.loc[(FYpop_DDG_agg_da['a'] == 2)]
    logging.info('DDG aj1 WAP currently {}'.format(FYWAP_DDG_LAD.pop_DDG_aj1.sum()))
    FYWAP_DDG_LAD = FYWAP_DDG_LAD.rename(columns={'pop_DDG_aj1': 'WAP_DDG_aj1'})
    FYpop_DDG_agg_dw = FYpop_DDG.groupby(['2013_LA_code', 'worker_type'])[['pop_DDG_aj1']].sum().reset_index()
    FYwkr_DDG_LAD = FYpop_DDG_agg_dw.loc[(FYpop_DDG_agg_dw['worker_type'] =='wkr')]
    logging.info('DDG aj1 worker currently {}'.format(FYwkr_DDG_LAD.pop_DDG_aj1.sum()))
    FYwkr_DDG_LAD = FYwkr_DDG_LAD.rename(columns={'pop_DDG_aj1': 'wkr_DDG_aj1'})

    # Merge LAD WAP and worker with total pop from BY DDG aj1 process
    # Columns in df BYpop_DDG_LAD after merging is: ['2013_LA_code','pop_DDG_aj1','pop_DDG_aj1','wkr_DDG_aj1']
    FYWAP_DDG_LAD = FYWAP_DDG_LAD.merge(FYwkr_DDG_LAD, how='left',
                                        on=['2013_LA_code']).drop(columns={'a','worker_type'})
    FYpop_DDG_LAD = FYpop_DDG_LAD.merge(FYWAP_DDG_LAD, how='left', on=['2013_LA_code'])

    # addtional three columns created-- work out ration of worker over total pop as well as over total WAP from DDG aj1
    FYpop_DDG_LAD['nwkr_DDG_aj1'] = FYpop_DDG_LAD['WAP_DDG_aj1'] - FYpop_DDG_LAD['wkr_DDG_aj1']
    FYpop_DDG_LAD['fact_wkr_pop_DDGaj1'] = FYpop_DDG_LAD['wkr_DDG_aj1'] / FYpop_DDG_LAD['pop_DDG_aj1']
    FYpop_DDG_LAD['fact_wkr_WAP_DDGaj1'] = FYpop_DDG_LAD['wkr_DDG_aj1'] / FYpop_DDG_LAD['WAP_DDG_aj1']

    # Adjustment2:
    # Merge LAD WAP and worker with DDG wkr ratio over WAP
    FYpop_DDG_LAD = FYpop_DDG_LAD.merge(wrkfac_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    FYpop_DDG_LAD = FYpop_DDG_LAD.rename(columns={future_year: 'wrkfac_DDG'})
    FYpop_DDG_LAD['wkr_DDG_aj2'] = FYpop_DDG_LAD['WAP_DDG_aj1'] * FYpop_DDG_LAD['wrkfac_DDG']
    FYpop_DDG_LAD['nwkr_DDG_aj2'] = FYpop_DDG_LAD['WAP_DDG_aj1'] - FYpop_DDG_LAD['wkr_DDG_aj2']

    # audit2- dump df FYpop_DDG_LAD for checking purpose
    pop_DDG_LAD_audit = FYpop_DDG_LAD.copy()
    pop_DDG_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                          audit_dir,
                                          scenario_name,
                                        '_'.join(['audit_2_gb_LAD_DDG', future_year, scenario_name, 'pop.csv']))
    pop_DDG_LAD_audit.to_csv(pop_DDG_LAD_audit_path, index=False)

    # Calculate adjustment factor by worker_type per LAD
    FYpop_DDG_LAD['wkr_aj_fac'] = FYpop_DDG_LAD['wkr_DDG_aj2'] / FYpop_DDG_LAD['wkr_DDG_aj1']
    FYpop_DDG_LAD['wkr_aj_fac'] = FYpop_DDG_LAD['wkr_aj_fac'].fillna(1)
    FYpop_DDG_LAD['nwkr_aj_fac'] = FYpop_DDG_LAD['nwkr_DDG_aj2'] / FYpop_DDG_LAD['nwkr_DDG_aj1']
    FYpop_DDG_LAD['nwkr_aj_fac'] = FYpop_DDG_LAD['nwkr_aj_fac'].fillna(1)
    FYpop_DDG_LAD_fac = FYpop_DDG_LAD[['2013_LA_code', 'wkr_aj_fac', 'nwkr_aj_fac']]
    FYpop_DDG_LAD_fac = FYpop_DDG_LAD_fac.rename(columns={'wkr_aj_fac': 'wkr', 'nwkr_aj_fac': 'nwkr'})
    FYpop_DDG_LAD_fac = FYpop_DDG_LAD_fac.melt(id_vars=['2013_LA_code'], var_name='worker_type', value_name='aj2_fac')
    FYpop_DDG_LAD_fac_append = FYpop_DDG_LAD[['2013_LA_code']]
    FYpop_DDG_LAD_fac_append['worker_type'] = "nwap"
    FYpop_DDG_LAD_fac_append['aj2_fac'] = 1
    FYpop_DDG_LAD_fac = FYpop_DDG_LAD_fac.append(FYpop_DDG_LAD_fac_append).reset_index()

    # audit3- dump aj_factor
    ajfac_DDG_LAD_audit = FYpop_DDG_LAD_fac.copy()
    ajfac_DDG_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        scenario_name,
                                        '_'.join(['audit_3_gb_LAD', future_year, scenario_name, 'ajfac.csv']))
    ajfac_DDG_LAD_audit.to_csv(ajfac_DDG_LAD_audit_path, index=False)

    # scale DDG aj1 pop FY worker_type to be compliant with DDG on worker and non worker
    FYpop_DDG = FYpop_DDG.merge(FYpop_DDG_LAD_fac, how='left', on=['2013_LA_code', 'worker_type'])

    FYpop_DDG['pop_DDG_aj2'] = FYpop_DDG['pop_DDG_aj1'] * FYpop_DDG['aj2_fac']
    logging.info('DDG population after adjustment 2 currently {}'.format(FYpop_DDG.pop_DDG_aj2.sum()))
    # audit4
    # sum LAD level WAP and wkr based on DDG_pop_aj2
    FYpop_DDGaj2_agg_da = FYpop_DDG.groupby(['2013_LA_code', 'a'])[['pop_DDG_aj2']].sum().reset_index()
    FYWAP_DDGaj2_LAD = FYpop_DDGaj2_agg_da.loc[(FYpop_DDGaj2_agg_da['a'] == 2)]
    logging.info('DDG aj2 WAP currently {}'.format(FYWAP_DDGaj2_LAD.pop_DDG_aj2.sum()))
    FYWAP_DDGaj2_LAD = FYWAP_DDGaj2_LAD.rename(columns={'pop_DDG_aj2': 'WAP_DDG_aj2'})
    FYpop_DDGaj2_agg_dw = FYpop_DDG.groupby(['2013_LA_code', 'worker_type'])[['pop_DDG_aj2']].sum().reset_index()
    FYwkr_DDGaj2_LAD = FYpop_DDGaj2_agg_dw.loc[(FYpop_DDGaj2_agg_dw['worker_type'] =='wkr')]
    logging.info('DDG aj2 worker currently {}'.format(FYwkr_DDGaj2_LAD.pop_DDG_aj2.sum()))
    FYwkr_DDGaj2_LAD = FYwkr_DDGaj2_LAD.rename(columns={'pop_DDG_aj2': 'wkr_DDG_aj2'})

    # Columns in df FYWAP_MYE_LAD after merging is: ['2013_LA_code','pop_DDG_aj2','WAP_DDG_aj2','wkr_DDG_aj2']
    FYWAP_DDGaj2_LAD = FYWAP_DDGaj2_LAD.merge(FYwkr_DDGaj2_LAD, how='left',
                                        on=['2013_LA_code']).drop(columns={'a', 'worker_type'})

    # addtional two columns created-- work out ration of worker over total pop as well as over total WAP from MYE
    # FYWAP_DDGaj2_LAD['nwkr_DDG_aj2'] = FYpop_MYE_LAD['WAP_DDG_aj2'] - FYpop_MYE_LAD['wkr_DDG_aj2']
    FYWAP_DDGaj2_LAD['fact_wkr_WAP_fy'] = FYWAP_DDGaj2_LAD['wkr_DDG_aj2'] / FYWAP_DDGaj2_LAD['WAP_DDG_aj2']
    FYWkrfac_DDG_LAD_audit = FYWAP_DDGaj2_LAD[['2013_LA_code', 'fact_wkr_WAP_fy']]
    FYWkrfac_DDG_LAD_audit = FYWkrfac_DDG_LAD_audit.merge(wrkfac_DDG_LAD, how='left',
                                                          left_on=['2013_LA_code'],
                                                          right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    FYWkrfac_DDG_LAD_audit = FYWkrfac_DDG_LAD_audit.rename(columns={future_year: 'wrkfac_DDG'})
    FYWkrfac_DDG_LAD_audit['ratio_deviation'] = FYWkrfac_DDG_LAD_audit['wrkfac_DDG']\
                                                /FYWkrfac_DDG_LAD_audit['fact_wkr_WAP_fy']-1

    logging.info('The min %age diff is ' + str(FYWkrfac_DDG_LAD_audit['ratio_deviation'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(FYWkrfac_DDG_LAD_audit['ratio_deviation'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(FYWkrfac_DDG_LAD_audit['ratio_deviation'].mean() * 100) + '%')

    FYWkrfac_DDG_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                            audit_dir,
                                            scenario_name,
                                           '_'.join(['audit_4_gb_lad', future_year, scenario_name, 'worker_ratio.csv']))
    FYWkrfac_DDG_LAD_audit.to_csv(FYWkrfac_DDG_LAD_audit_path, index=False)

    # audit5
    # check LAD level pop is consistent with DDG LAD
    FYpop_DDG_LAD_audit = FYpop_DDG.groupby(['2013_LA_code'])[['pop_fy', 'pop_DDG_aj1', 'pop_DDG_aj2']].sum().reset_index()
    FYpop_DDG_LAD_audit = FYpop_DDG_LAD_audit.merge(pop_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    FYpop_DDG_LAD_audit = FYpop_DDG_LAD_audit.rename(columns={future_year: 'pop_DDG'})
    FYpop_DDG_LAD_audit['pop_deviation'] = FYpop_DDG_LAD_audit['pop_DDG_aj2']/FYpop_DDG_LAD_audit['pop_DDG']-1
    logging.info('The min %age diff is ' + str(FYpop_DDG_LAD_audit['pop_deviation'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(FYpop_DDG_LAD_audit['pop_deviation'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(FYpop_DDG_LAD_audit['pop_deviation'].mean() * 100) + '%')
    logging.info('The overall deviation is ' + str(
        FYpop_DDG_LAD_audit['pop_DDG_aj2'].sum() - FYpop_DDG_LAD_audit['pop_DDG'].sum()) + ' people')
    FYpop_DDG_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                            audit_dir,
                                            scenario_name,
                                           '_'.join(['audit_5_gb_lad', future_year, scenario_name, 'check_pop.csv']))
    FYpop_DDG_LAD_audit.to_csv(FYpop_DDG_LAD_audit_path, index=False)
    # Auditing text for DDG aligned pop process
    audit_header = '\n'.join(['Audit for  DDG aligned fy pop process',
                                     'Created ' + str(datetime.datetime.now())])
    audit_text = '\n'.join(['The total ' + future_year + ' population at the end of the running process is:',
                                   '\t' + str(FYpop_DDG['pop_DDG_aj2'].sum()),
                                   'Checking final district total population against DDG district population:',
                                   '\tThe min %age diff is ' + str(
                                       FYpop_DDG_LAD_audit['pop_deviation'].min() * 100) + '%',
                                   '\tThe max %age diff is ' + str(
                                       FYpop_DDG_LAD_audit['pop_deviation'].max() * 100) + '%',
                                   '\tThe mean %age diff is ' + str(
                                       FYpop_DDG_LAD_audit['pop_deviation'].mean() * 100) + '%',
                                   'The overall deviation is ' + str(
                                       FYpop_DDG_LAD_audit['pop_DDG_aj2'].sum() -
                                       FYpop_DDG_LAD_audit['pop_DDG'].sum()) + ' people',
                                   'All of the above values should be equal (or close) to 0.',
                                   'A full breakdown of the ' + future_year + scenario_name + 'population FY d can be found at:',
                                   FYpop_DDG_LAD_audit_path])

    audit_DDG_fypop_process_content = '\n'.join([audit_header, audit_text])
    audit_DDG_fypop_process_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                     audit_dir,
                                     scenario_name,
                                     ''.join(['Audit_DDG_pop_process_', future_year, CAS_scen, '.txt']))
    with open(audit_DDG_fypop_process_path, 'w') as text_file:
        text_file.write(audit_DDG_fypop_process_content)

    # Format ouputs
    FYpop_DDG = FYpop_DDG.rename(columns={'pop_DDG_aj2': 'people'})
    FYpop_DDG_out = FYpop_DDG[['2013_LA_code', '2013_LA_name', 'z', 'MSOA', 'tfn_tt', 't','people']]

    #Also groupby this output FY removing t
    groupby_cols = ['2013_LA_code', '2013_LA_name', 'z', 'MSOA', 'tfn_tt']
    FYpop_DDG_exc_t_out = FYpop_DDG_out.groupby(groupby_cols)['people'].sum().reset_index()

    #Dump outputs
    #DDG_aligned_pop_output_path = os.path.join(fy_lu_obj.out_paths['write_folder'], output_dir)
    FYpop_DDG_pop_allsegs_filename = '_'.join(['output_1_DDG_resi_gb_msoa_tfn_tt', future_year, CAS_scen,'pop'])
    FYpop_DDG_pop_tfn_tt_filename = '_'.join(['output_2_DDG_resi_gb_msoa_tfn', future_year, CAS_scen,'pop'])

    FYpop_DDG_pop_allsegs_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                  output_dir,
                                  scenario_name,
                                  FYpop_DDG_pop_allsegs_filename)
    FYpop_DDG_pop_tfn_tt_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                  output_dir,
                                  scenario_name,
                                  FYpop_DDG_pop_tfn_tt_filename)
    compress.write_out(FYpop_DDG_out, FYpop_DDG_pop_allsegs_path)
    compress.write_out(FYpop_DDG_exc_t_out, FYpop_DDG_pop_tfn_tt_path)
    logging.info('Step completed-- processing fy pop to be aligned with DDG')
    print('Step completed -- processing fy pop to be aligned with DDG')
    return 0


def DDGaligned_fy_emp_process(fy_lu_obj):
    # Distrctory and file from standard base year employment process in employment.py
    logging.info('Processing fy emp to be aligned with DDG')
    print('Processing fy emp to be aligned with DDG')
    base_year = fy_lu_obj.base_year
    future_year = fy_lu_obj.future_year
    by_output_folder = fy_lu_obj.by_home_folder
    fy_output_folder = fy_lu_obj.fy_home_folder
    scenario_name = fy_lu_obj.scenario_name
    CAS_scen = fy_lu_obj.CAS_scen
    BYemp_process_output_file = os.path.join(by_output_folder, ''.join(['land_use_',
                                                                            base_year, '_emp.csv']))
    BYemp_unm = pd.read_csv(BYemp_process_output_file)
    # Directory and file paths for the DDG
    # Directory Paths
    DDG_directory = os.path.join(fy_lu_obj.import_folder, 'DDG',
                                 ' '.join(['CAS', scenario_name]))
    # File names
    DDG_emp_path = '_'.join(['DD', 'Nov21', CAS_scen, 'Emp', 'LA.csv'])
    # Extract employment only
    BYemp = BYemp_unm.query("soc_cat == 1 or soc_cat == 2 or soc_cat == 3")
    # Extract unemployment-- could not understand why there a category of unemployment for job?
    BYunemp = BYemp_unm.query("soc_cat == 4")
    # Extract total original employment loc to E01
    BYemp_tot = BYemp[BYemp['e_cat'] == 'E01']

    logging.info('Initial check on total employment currently {}'.format(BYemp.employment.sum()))
    BYemp_MSOA = BYemp_tot.groupby(['msoa_zone_id'])[['employment']].sum().reset_index()

    # get 2013 LA in
    _2013LA_path = fy_lu_obj.import_folder + Zone_LA_path
    Zone_2013LA = pd.read_csv(_2013LA_path)[['NorMITs Zone', '2013 LA', '2013 LA Name']]
    BYemp = BYemp.merge(Zone_2013LA, how='left',
                                left_on=['msoa_zone_id'],
                                right_on=['MSOA']).drop(columns={'MSOA'})
    BYemp = BYemp.rename(columns={'2013 LA': '2013_LA_code', '2013 LA Name': '2013_LA_name'})
    BYemp_MSOA = BYemp_MSOA.merge(Zone_2013LA, how='left',
                                left_on=['msoa_zone_id'],
                                right_on=['MSOA']).drop(columns={'MSOA'})
    BYemp_MSOA = BYemp_MSOA.rename(columns={'2013 LA': '2013_LA_code', '2013 LA Name': '2013_LA_name'})
    BYemp_LAD = BYemp_MSOA.groupby(['2013_LA_code'])[['employment']].sum().reset_index()
    logging.info('Employment aggregated from LAD currently {}'.format(BYemp_LAD.employment.sum()))

    # get DDG employment for base year
    emp_DDG_LAD = pd.read_csv(
        os.path.join(
            DDG_directory, DDG_emp_path))[['LAD13CD', future_year]]
    BYemp_LAD = BYemp_LAD.merge(emp_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    BYemp_LAD = BYemp_LAD.rename(columns={future_year: 'emp_DDG'})

    #calculate adjustment factor on total emp per LAD
    BYemp_LAD['emp_aj_fac'] = BYemp_LAD['emp_DDG'] / BYemp_LAD['employment']
    BYemp_LAD['emp_aj_fac'] = BYemp_LAD['emp_aj_fac'].fillna(1)
    FYemp_LAD_fac = BYemp_LAD[['2013_LA_code', 'emp_aj_fac']]
    FYemp_DDG = BYemp.merge(FYemp_LAD_fac, how='left', on=['2013_LA_code'])

    #scale base year employments by segments to be compliant with DDG future year
    FYemp_DDG['emp_aj'] = FYemp_DDG['employment'] * FYemp_DDG['emp_aj_fac']
    FYemp_DDG = FYemp_DDG.rename(columns={'employment': 'emp_by', 'emp_aj': 'employment'})
    logging.info('DDG aligned employment total currently {}'.format(FYemp_DDG.employment.sum()))

    # sum up LAD total of DDG employment for audit
    FYemp_DDG_LAD_ecat = FYemp_DDG.groupby(['2013_LA_code','e_cat'])[['emp_by', 'employment']].sum().reset_index()
    FYemp_DDG_LAD_audit = FYemp_DDG_LAD_ecat[FYemp_DDG_LAD_ecat['e_cat'] == 'E01']

    FYemp_DDG_LAD_audit = FYemp_DDG_LAD_audit.merge(emp_DDG_LAD, how='left',
                        left_on=['2013_LA_code'],
                        right_on=['LAD13CD']).drop(columns={'LAD13CD'})
    FYemp_DDG_LAD_audit = FYemp_DDG_LAD_audit.rename(columns={future_year: 'emp_DDG'})
    FYemp_DDG_LAD_audit['emp_deviation'] = FYemp_DDG_LAD_audit['employment']/FYemp_DDG_LAD_audit['emp_DDG']-1
    logging.info('The min %age diff is ' + str(FYemp_DDG_LAD_audit['emp_deviation'].min() * 100) + '%')
    logging.info('The max %age diff is ' + str(FYemp_DDG_LAD_audit['emp_deviation'].max() * 100) + '%')
    logging.info('The mean %age diff is ' + str(FYemp_DDG_LAD_audit['emp_deviation'].mean() * 100) + '%')
    logging.info('The overall deviation is ' + str(
        FYemp_DDG_LAD_audit['employment'].sum() - FYemp_DDG_LAD_audit['emp_DDG'].sum()) + ' employment')
    FYemp_DDG_LAD_audit_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                        audit_dir,
                                        scenario_name,
                                           '_'.join(['audit_gb_lad', future_year, CAS_scen, 'check_employment.csv']))
    FYemp_DDG_LAD_audit.to_csv(FYemp_DDG_LAD_audit_path, index=False)

    FYemp_DDG_output = FYemp_DDG[['msoa_zone_id', 'e_cat', 'soc_cat', 'employment']]
    # Dump outputs
    #DDG_aligned_emp_output_path = fy_lu_obj.out_paths['write_folder']
    FYemp_DDG_filename = '_'.join(['output_3_DDG_gb_msoa', future_year, CAS_scen, 'emp.csv'])
    FYemp_DDG_path = os.path.join(fy_lu_obj.out_paths['write_folder'],
                                  output_dir,
                                  scenario_name,
                                  FYemp_DDG_filename)
    FYemp_DDG_output.to_csv(FYemp_DDG_path, index=False)
    logging.info('Step completed-- processing fy emp to be aligned with DDG')
    print('Step completed -- processing fy emp to be aligned with DDG')
    return 0


