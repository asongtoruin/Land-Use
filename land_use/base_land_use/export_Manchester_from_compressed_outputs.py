"""
Author - ART, 13/12/2021
Created - 13/12/2021

Purpose - Just a little widget to read in compressed files for QA at the end of a new Base Year iteration
"""

import os
from land_use.utils import compress
import pandas as pd

starting_dir = os.getcwd()

iteration = 'iter4g'
working_dir = os.path.join(r'I:\NorMITs Land Use\base_land_use', iteration, 'outputs')

run_check_327 = True
run_check_3210 = True
run_check_328 = True
run_check_3211 = True

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


def check_327():
    print('running check_327')
    s327_testfile = os.path.join('3.2.7_verify_population_profile_by_dwelling_type',
                                 'HhPop_byfullNorMITsSegs_initial_2018_MSOA')
    s327_data = compress.read_in(s327_testfile)
    manchester_s327_data = s327_data[s327_data['2021_LA_code'].isin(manchester_la)]
    manchester_s327_dump_path = os.path.join('3.2.7_verify_population_profile_by_dwelling_type', 'Check_against_Excel',
                                             'Manchester_post_3.2.7_pop.csv')
    print('dumping to csv')
    manchester_s327_data.to_csv(manchester_s327_dump_path, index=False)
    print(manchester_s327_data)


def check_328():
    print('running check_328')
    pd.set_option('display.max_columns', 20)
    s328_testfile = os.path.join('3.2.8_subsets_of_workers+nonworkers', 'Audits',
                                 'Audit_3_2_8_pop_vs_workers_and_non-workers_df_dump')
    s328_data = compress.read_in(s328_testfile)
    manchester_s328_data = s328_data[s328_data['2021_LA_code'].isin(manchester_la)]
    print(manchester_s328_data)
    manchester_s328_data_notnull = manchester_s328_data[manchester_s328_data.worker_pop.notnull()]
    print(manchester_s328_data_notnull)


def check_3210():
    print('running check_3210')
    s3210_testfile = os.path.join('3.2.10_adjust_zonal_pop_with_full_dimensions', '2018_household_population_processed')
    s3210_data = compress.read_in(s3210_testfile)
    manchester_s3210_data = s3210_data[s3210_data['2021_LA_code'].isin(manchester_la)]
    manchester_s3210_dump_path = os.path.join('3.2.10_adjust_zonal_pop_with_full_dimensions', 'Check_against_Excel',
                                              'Manchester_post_3.2.10_HHpop.csv')
    print('dumping to csv')
    manchester_s3210_data.to_csv(manchester_s3210_dump_path, index=False)
    print(manchester_s3210_data)


def check_3211():
    print('running check_3211')
    s3211_testfile = os.path.join('3.2.10_adjust_zonal_pop_with_full_dimensions',
                                  '2018_total_population_independent_of_property_type')
    s3211_data = compress.read_in(s3211_testfile)
    manchester_s3211_data = s3211_data[s3211_data['2021_LA_Name'].isin(['Manchester'])]
    manchester_s3211_dump_path = os.path.join('3.2.10_adjust_zonal_pop_with_full_dimensions',
                                              'Audits',
                                              'Check_against_Excel',
                                              'Manchester_final_result.csv')
    print('dumping to csv')
    manchester_s3211_data.to_csv(manchester_s3211_dump_path, index=False)
    print(manchester_s3211_data)


os.chdir(working_dir)

if run_check_327:
    check_327()
if run_check_328:
    check_328()
if run_check_3210:
    check_3210()
if run_check_3211:
    check_3211()

os.chdir(starting_dir)
