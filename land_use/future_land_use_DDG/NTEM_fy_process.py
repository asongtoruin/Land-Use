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
_Zone_LA_path = '/Lookups/MSOA_1991LA_2011LA_2021LA_LAgroups.csv'
normits_seg_to_tfn_tt_file = r'I:\NorMITs Land Use\import\Lookups\NorMITs_segments_to_TfN_tt\normits_segs_to_tfn_tt.csv'

# Process/audit/output directory name
process_dir = '01 Process'
audit_dir = '02 Audits'
output_dir = '03 Outputs'

def ntem_fy_pop_interpolation(fy_lu_obj):
    """
    Process population data from NTEM CTripEnd database:
    Interpolate population to the target year, in this case it is for the base year, as databases
    are available in 5 year interval;
    Translate NTEM zones in Scotland into NorNITs zones; for England and Wales, NTEM zones = NorMITs zones (MSOAs)
    """

    # The year of data is set to define the upper and lower NTEM run years and interpolate as necessary between them.
    # The base year for NTEM is 2011 and it is run in 5-year increments from 2011 to 2051.
    # The year selected below must be between 2011 and 2051 (inclusive).
    # As we are running this inside the main base year script, we can set Year = ModelYear
    # However, we do still need to retain Year, as it is assumed Year is an int, not a str (as ModelYear is).
    logging.info('Extracting NTEM pop from database')
    print('Extracting NTEM pop from database')
    Year = int(fy_lu_obj.future_year)
    Heading_year = fy_lu_obj.future_year
    # Year = int('2051')
    # Heading_year = 2051

    logging.info('Running NTEM_Pop_Interpolation function for Year ' + str(Year))
    print('Running NTEM_Pop_Interpolation function for Year ' + str(Year))

    # if Year < 2011 | Year > 2061:
    #     raise ValueError("Please select a valid year of data.")
    # else:
    #     pass
    ntem_output_path = fy_lu_obj.import_folder + '/CTripEnd/All_year'

    logging.info('NTEM_Pop_Interpolation output being written in:')
    logging.info(ntem_output_path)
    LogFile = os.path.join(ntem_output_path, ''.join(['NTEM_Pop_Interpolation_LogFile_',
                                                                                   str(Year), '.txt']))
    # 'I:/NorMITs Synthesiser/Zone Translation/'
    Zone_path = fy_lu_obj.zones_folder + 'Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv'
    Pop_Segmentation_path = fy_lu_obj.import_folder + '/CTripEnd/Pop_Segmentations.csv'

    with open(LogFile, 'w') as o:
        o.write("ntem interpolation run on - " + str(datetime.datetime.now()) + "\n")
        o.write("\n")
        o.write("Data Year - " + str(Year) + "\n")
        o.write("\n")
        o.write("Correspondence Lists:\n")
        o.write(Zone_path + "\n")
        o.write(Pop_Segmentation_path + "\n")
        o.write("\n")

    # Data years
    # NTEM is run in 5-year increments with a base of 2011.
    # This section calculates the upper and lower years of data that are required
    InterpolationYears = Year % 5
    LowerYear = Year - ((InterpolationYears - 1) % 5)
    UpperYear = Year + ((1 - InterpolationYears) % 5)

    logging.info("Lower Interpolation Year - " + str(LowerYear))
    logging.info("Upper Interpolation Year - " + str(UpperYear))
    print("Lower Interpolation Year - " + str(LowerYear))
    print("Upper Interpolation Year - " + str(UpperYear))

    # Import Upper and Lower Year Tables
    # 'I:/Data/NTEM/NTEM 7.2 outputs for TfN/'
    LowerNTEMDatabase = fy_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(LowerYear) + '.accdb'
    UpperNTEMDatabase = fy_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(UpperYear) + '.accdb'
    # UpperNTEMDatabase = fy_lu_obj.CTripEnd_Database_path + r"\CTripEnd7_" + str(UpperYear) + r".accdb"
    cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' +
                          '{};'.format(UpperNTEMDatabase))

    query = r"SELECT * FROM ZoneData"
    UZoneData = pd.read_sql(query, cnxn)
    cnxn.close()

    cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' +
                          '{};'.format(LowerNTEMDatabase))

    query = r"SELECT * FROM ZoneData"
    LZoneData = pd.read_sql(query, cnxn)
    cnxn.close()

    # Re-format Tables
    LZonePop = LZoneData.copy()
    UZonePop = UZoneData.copy()
    LZonePop.drop(
        ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07', 'E08', 'E09', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'K01',
         'K02', 'K03', 'K04', 'K05', 'K06', 'K07', 'K08', 'K09', 'K10', 'K11', 'K12', 'K13', 'K14', 'K15'], axis=1,
        inplace=True)
    UZonePop.drop(
        ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07', 'E08', 'E09', 'E10', 'E11', 'E12', 'E13', 'E14', 'E15', 'K01',
         'K02', 'K03', 'K04', 'K05', 'K06', 'K07', 'K08', 'K09', 'K10', 'K11', 'K12', 'K13', 'K14', 'K15'], axis=1,
        inplace=True)
    LZonePop_long = pd.melt(LZonePop, id_vars=["I", "R", "B", "Borough", "ZoneID", "ZoneName"],
                            var_name="LTravellerType", value_name="LPopulation")
    UZonePop_long = pd.melt(UZonePop, id_vars=["I", "R", "B", "Borough", "ZoneID", "ZoneName"],
                            var_name="UTravellerType", value_name="UPopulation")

    LZonePop_long.rename(columns={"I": "LZoneID", "B": "LBorough", "R": "LAreaType"}, inplace=True)
    UZonePop_long.rename(columns={"I": "UZoneID", "B": "UBorough", "R": "UAreaType"}, inplace=True)

    LZonePop_long['LIndivID'] = LZonePop_long.LZoneID.map(str) + "_" + LZonePop_long.LAreaType.map(
        str) + "_" + LZonePop_long.LBorough.map(str) + "_" + LZonePop_long.LTravellerType.map(str)
    UZonePop_long['UIndivID'] = UZonePop_long.UZoneID.map(str) + "_" + UZonePop_long.UAreaType.map(
        str) + "_" + UZonePop_long.UBorough.map(str) + "_" + UZonePop_long.UTravellerType.map(str)

    # Join Upper and Lower Tables
    TZonePop_DataYear = LZonePop_long.join(UZonePop_long.set_index('UIndivID'), on='LIndivID', how='right',
                                           lsuffix='_left', rsuffix='_right')
    TZonePop_DataYear.drop(['UZoneID', 'UBorough', 'UAreaType', 'UTravellerType'], axis=1, inplace=True)

    # Interpolate Between Upper and Lower Years
    TZonePop_DataYear['GrowthinPeriod'] = TZonePop_DataYear.eval('UPopulation - LPopulation')
    TZonePop_DataYear['GrowthperYear'] = TZonePop_DataYear.eval('GrowthinPeriod / 5')
    TZonePop_DataYear = TZonePop_DataYear.assign(GrowthtoYear=TZonePop_DataYear['GrowthperYear'] * (Year - LowerYear))
    TZonePop_DataYear['Population'] = TZonePop_DataYear.eval('LPopulation + GrowthtoYear')

    # Tidy up
    TZonePop_DataYear.rename(
        columns={"LZoneID": "ZoneID", "LBorough": "Borough", "LAreaType": "AreaType", "LTravellerType": "TravellerType",
                 "LIndivID": "IndivID"}, inplace=True)
    TZonePop_DataYear.drop(
        ['GrowthinPeriod', 'GrowthperYear', 'GrowthtoYear', 'LPopulation', 'UPopulation', 'ZoneID_left', 'ZoneID_right',
         'ZoneName_right', 'ZoneName_left', 'Borough_left', 'Borough_right', 'IndivID'], axis=1, inplace=True)
    print(TZonePop_DataYear.Population.sum())

    # Translating zones for those in Scotland
    Zone_List = pd.read_csv(Zone_path)
    TZonePop_DataYear = TZonePop_DataYear.join(Zone_List.set_index('ntemZoneID'), on='ZoneID', how='right')
    # TZonePop_DataYear.rename(columns={'msoaZoneID': 'ModelZone'}, inplace=True)
    TZonePop_DataYear[
        'Population_RePropped'] = TZonePop_DataYear['Population'] * TZonePop_DataYear['overlap_ntem_pop_split_factor']

    Segmentation_List = pd.read_csv(Pop_Segmentation_path)
    TZonePop_DataYear = TZonePop_DataYear.join(Segmentation_List.set_index('NTEM_Traveller_Type'), on='TravellerType',
                                               how='right')
    TZonePop_DataYear.drop(
        ['Population', 'ZoneID', 'overlap_population', 'ntem_population', 'msoa_population',
         'overlap_msoa_pop_split_factor', 'overlap_type'], axis=1, inplace=True)
    TZonePop_DataYear.rename(columns={"Population_RePropped": "Population"}, inplace=True)
    print(TZonePop_DataYear.Population.sum())
    TZonePop_DataYear = TZonePop_DataYear.groupby(['msoaZoneID', 'AreaType', 'Borough', 'TravellerType',
                                                   'NTEM_TT_Name', 'Age_code', 'Age',
                                                   'Gender_code', 'Gender', 'Household_composition_code',
                                                   'Household_size', 'Household_car', 'Employment_type_code',
                                                   'Employment_type'])[
        ['Population']].sum().reset_index()
    NTEM_HHpop = TZonePop_DataYear[['msoaZoneID', 'TravellerType', 'Population']]
    Total_pop = NTEM_HHpop.Population.sum()
    print(NTEM_HHpop.Population.sum())
    NTEM_HHpop['Population'] = NTEM_HHpop['Population'].fillna(0)
    print(NTEM_HHpop.head(10))
    NTEM_HHpop = NTEM_HHpop.rename(columns={'msoaZoneID': 'z',
                                            'TravellerType': 'tt',
                                            'Population': Heading_year})
    print(NTEM_HHpop.tail(10))
    NTEM_HHpop = NTEM_HHpop.sort_values(by=['z', 'tt'])
    print(NTEM_HHpop.head(10))
    #Dump the output
    PopOutput = '_'.join(['ntem_gb_z_areatype_ntem_tt', str(Year), 'pop.csv'])
    PopOutput_file = os.path.join(ntem_output_path, PopOutput)
    NTEM_HHpop.to_csv(PopOutput_file, index=False)

    with open(LogFile, 'a') as o:
        o.write(str(Year) + ' total Population: \n')
        o.write(str(Total_pop) + "\n")
        o.write('\n')
        o.write('\n')
    print('Export complete.')
    print(NTEM_HHpop.head(5))
    logging.info('NTEM_Pop_Interpolation function complete')
    print('NTEM_Pop_Interpolation function complete')
    # return NTEM_HHpop
    return 0

def clean_base_ntem_pop(fy_lu_obj):
    logging.info('Cleanning NTEM pop for base year 2018')
    print('Cleanning NTEM pop for base year 2018')
    base_year = fy_lu_obj.base_year
    # future_year = fy_lu_obj.future_year
    # read in base year ntem pop
    ntem_output_path = os.path.join(fy_lu_obj.import_folder, 'CTripEnd', 'All_year')
    baseyear_ntem_path = os.path.join(fy_lu_obj.by_home_folder, '01 Process', '3.2.5_uplifting_base_year_pop_base_year_MYPE')
    # read in base year ntem pop
    by_ntem_file_name = '_'.join(['ntem_gb_z_areatype_ntem_tt', base_year, 'pop.csv'])
    by_ntem_pop = pd.read_csv(os.path.join(baseyear_ntem_path, by_ntem_file_name))
    # read in future year ntem pop
    # fy_ntem_file_name = '_'.join(['ntem_gb_z_areatype_ntem_tt', future_year, 'pop.csv'])
    # fy_ntem_pop = pd.read_csv(os.path.join(ntem_output_path, fy_ntem_file_name))
    # rename the heading of ntem data:
    by_ntem_pop['Population'] = by_ntem_pop['Population'].fillna(0)
    by_ntem_pop = by_ntem_pop.rename(columns={'msoaZoneID': 'z',
                                              'TravellerType': 'tt',
                                              'Age_code': 'a',
                                              'Gender_code': 'g',
                                              'Household_composition_code': 'h',
                                              'Employment_type_code': 'e',
                                              'Population': base_year})
    by_ntem_pop_cols=['z', 'tt', base_year]
    base = by_ntem_pop[by_ntem_pop_cols]
    base = base.sort_values(by=['z', 'tt'])
    print(base.head(10))
    base_output = '_'.join(['ntem_gb_z_areatype_ntem_tt', base_year, 'pop.csv'])
    base.to_csv(os.path.join(ntem_output_path, base_output), index=False)
    # base = pd.concat([base, fy_ntem_pop], axis=1)
    # return [base, fy_ntem_pop]
    logging.info('clean_base_ntem_pop function complete')
    print('clean_base_ntem_pop function complete')