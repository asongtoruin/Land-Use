"""
Author - ART, 13/12/2021
Created - 13/12/2021

Purpose - Just a little widget to read in compressed files for QA at the end of a new Base Year iteration
"""

import os
from land_use.utils import compress
import pandas as pd

starting_dir = os.getcwd()
ModelYear = '2018'
iteration = 'iter4l'
process_working_dir = os.path.join(r'I:\NorMITs Land Use\base_land_use', iteration, '01 Process')
audit_working_dir = os.path.join(r'I:\NorMITs Land Use\base_land_use', iteration, '02 Audits')
output_working_dir = os.path.join(r'I:\NorMITs Land Use\base_land_use', iteration, '03 Outputs')

run_check_326 = True
run_check_327 = False
run_check_328 = False
run_check_3210 = False
run_check_3211 = False

manchester_zones = ['E02001045', 'E02001046', 'E02001047', 'E02001048', 'E02001049', 'E02001050', 'E02001051',
                    'E02001052', 'E02001053', 'E02001055', 'E02001056', 'E02001057', 'E02001059', 'E02001061',
                    'E02001062', 'E02001063', 'E02001064', 'E02001065', 'E02001066', 'E02001067', 'E02001068',
                    'E02001069', 'E02001070', 'E02001071', 'E02001072', 'E02001073', 'E02001074', 'E02001075',
                    'E02001076', 'E02001077', 'E02001078', 'E02001079', 'E02001080', 'E02001081', 'E02001082',
                    'E02001083', 'E02001084', 'E02001085', 'E02001086', 'E02001087', 'E02001088', 'E02001089',
                    'E02001090', 'E02001091', 'E02001092', 'E02001093', 'E02001094', 'E02001095', 'E02001096',
                    'E02001097', 'E02006902', 'E02006912', 'E02006913', 'E02006914', 'E02006915', 'E02006916',
                    'E02006917']

manchester_la = ['E08000003']


def check_326():
    print('running check_326')
    s326_testfile = os.path.join('3.2.6_expand_NTEM_pop',
                                 ''.join(['gb_msoa_tfn_tt_agg_prt_', ModelYear, '_pop']))
    s326_data = compress.read_in(s326_testfile)
    manchester_s326_data = s326_data[s326_data['msoa11cd'].isin(manchester_zones)]
    manchester_s326_dump_path = os.path.join('3.2.6_expand_NTEM_pop',
                                             ''.join(['manchester_msoa_tfn_tt_agg_prt_', ModelYear, '_pop.csv']))
    print('dumping to csv')
    manchester_s326_data.to_csv(manchester_s326_dump_path, index=False)
    print(manchester_s326_data)


def check_327():
    print('running check_327')
    s327_testfile = os.path.join('3.2.7_verify_population_profile_by_dwelling_type',
                                 ''.join(['gb_lad_msoa_tfn_tt_agg_prt_', ModelYear, '_properties+hh_pop']))
    s327_data = compress.read_in(s327_testfile)
    manchester_s327_data = s327_data[s327_data['2021_LA_code'].isin(manchester_la)]
    manchester_s327_dump_path = os.path.join('3.2.7_verify_population_profile_by_dwelling_type',
                                             ''.join(['manchester_lad_msoa_tfn_tt_agg_prt_', ModelYear, '_properties+hh_pop.csv']))
    print('dumping to csv')
    manchester_s327_data.to_csv(manchester_s327_dump_path, index=False)
    print(manchester_s327_data)


def check_328():
    print('running check_328')
    pd.set_option('display.max_columns', 20)
    s328_testfile = os.path.join('3.2.8_subsets_of_workers+nonworkers',
                                 ''.join(['audit_11_gb_msoa_tfn_tt_agg_prt_', ModelYear, '_hh_pop+wkrs+nwkrs']))
    s328_data = compress.read_in(s328_testfile)
    manchester_s328_data = s328_data[s328_data['2021_LA_code'].isin(manchester_la)]
    print(manchester_s328_data)
    manchester_s328_data_notnull = manchester_s328_data[manchester_s328_data.worker_pop.notnull()]
    print(manchester_s328_data_notnull)


def check_3210():
    print('running check_3210')
    s3210_testfile = os.path.join(''.join(['output_3_resi_gb_lad_msoa_tfn_tt_agg_prt_', ModelYear, '_hh_pop']))
    s3210_data = compress.read_in(s3210_testfile)
    manchester_s3210_data = s3210_data[s3210_data['2021_LA_code'].isin(manchester_la)]
    manchester_s3210_dump_path = os.path.join(''.join(['output_3_resi_manchester_lad_msoa_tfn_tt_agg_prt_', ModelYear, '_hh_pop.csv']))
    print('dumping to csv')
    manchester_s3210_data.to_csv(manchester_s3210_dump_path, index=False)
    print(manchester_s3210_data)


def check_3211():
    print('running check_3211')
    s3211_testfile = os.path.join(''.join(['output_6_resi_gb_msoa_tfn_tt_prt_', ModelYear, '_pop']))
    s3211_data = compress.read_in(s3211_testfile)
    manchester_s3211_data = s3211_data[s3211_data['2021_LA_Name'].isin(['Manchester'])]
    manchester_s3211_dump_path = os.path.join(''.join(['output_6_resi_manchester_msoa_tfn_tt_prt_',
                                                       ModelYear, '_pop.csv']))
    print('dumping to csv')
    manchester_s3211_data.to_csv(manchester_s3211_dump_path, index=False)
    print(manchester_s3211_data)

if run_check_326:
    os.chdir(process_working_dir)
    check_326()
if run_check_327:
    os.chdir(process_working_dir)
    check_327()
if run_check_328:
    os.chdir(audit_working_dir)
    check_328()
if run_check_3210:
    os.chdir(output_working_dir)
    check_3210()
if run_check_3211:
    os.chdir(output_working_dir)
    check_3211()

os.chdir(starting_dir)
