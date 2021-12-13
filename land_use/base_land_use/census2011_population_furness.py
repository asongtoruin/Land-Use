# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 2021 - Thu Oct 28 2021

@author: adamtebbs
Version number:

Written using: Python 3.7.1

Module versions used for writing:
    pandas v0.25.3
    numpy v1.17.3

census2011_population_furness:
    - imports NTEM mid-year population estimates and 2011 Census micro data
    - applies 2011 census segmentation
    - and ns-sec and soc segmentation
    - runs the IPFN process
    - does not process communal establishments
    - outputs f factor to be used in 2018
## TODO: Needs checking. Also need to ensure that the final f factors are actually produced at some point!
## TODO: Ensure this script gets called properly! NB - DO NOT ACTUALLY RUN IT! It would take weeks to finish!
"""

import pandas as pd
import numpy as np
import os
import itertools
import datetime
import pyodbc
from ipfn import ipfn
import logging


# Data input paths
_census_micro_path = 'I:/NorMITs Land Use/import/2011 Census Microdata'
_QS_census_queries_path = 'I:/NorMITs Land Use/import/Nomis Census 2011 Head & Household'
_lookup_tables_path = 'I:/NorMITs Land Use/import/2011 Census Micro lookups'

# Read in data for the 2011 furness set up
# 2011 Census micro data
census_micro = pd.read_csv(os.path.join(_census_micro_path, 'recodev12.csv'))

# 2011 census queries
QS401_raw_census = pd.read_csv(os.path.join(
    _QS_census_queries_path, '210817_QS401UK -Dwelling type - Persons_MSOA.csv'), skiprows=7)
QS606_raw_census = pd.read_csv(os.path.join(
    _QS_census_queries_path, '210817_QS606UK - Occupation- ER_MSOA.csv'), skiprows=7)
QS609_raw_census = pd.read_csv(os.path.join(
    _QS_census_queries_path, '210817_QS609UK - NS-SeC of HRP- Persons_MSOA.csv'), skiprows=6)
# Trim the footers off the tables (they are always in the 2nd column, so dropna on Area (1st column))
QS401_raw_census = QS401_raw_census.dropna(subset=['Area'])
QS606_raw_census = QS606_raw_census.dropna(subset=['Area'])
QS609_raw_census = QS609_raw_census.dropna(subset=['Area'])

# Read in NTEM -> NorMITs lookup tables...
lookup_ageh = pd.read_csv(os.path.join(_lookup_tables_path, 'ageh.csv'))
lookup_sex = pd.read_csv(os.path.join(_lookup_tables_path, 'sex.csv'))
lookup_ahchuk11 = pd.read_csv(os.path.join(_lookup_tables_path, 'ahchuk11.csv'))
lookup_carsnoc = pd.read_csv(os.path.join(_lookup_tables_path, 'carsnoc.csv'))
lookup_h = pd.read_csv(os.path.join(_lookup_tables_path, 'h.csv'))
lookup_ecopuk11 = pd.read_csv(os.path.join(_lookup_tables_path, 'ecopuk11.csv'))
lookup_hours = pd.read_csv(os.path.join(_lookup_tables_path, 'hours.csv'))
lookup_nsshuk11 = pd.read_csv(os.path.join(_lookup_tables_path, 'nsshuk11.csv'))
lookup_occg = pd.read_csv(os.path.join(_lookup_tables_path, 'occg.csv'))
lookup_typaccom = pd.read_csv(os.path.join(_lookup_tables_path, 'Typaccom.csv'))
lookup_geography = pd.read_csv(os.path.join(_lookup_tables_path, 'geography.csv'))
# ...and get rid of spaces in geography lookup table headers immediately
lookup_geography.columns = lookup_geography.columns.str.replace(' ', '_')

# Define model name and output folder
# Note that output folder is IPFN input folder
ModelName = 'NorMITs'
Output_Folder = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs'

# Define function that can be used to get 2011 NTEM data
def NTEM_Pop_Interpolation(census_and_by_lu_obj):
    """
    Process population data from NTEM CTripEnd database:
    Interpolate population to the target year, in this case, it is for base year 2011 as databases
    are available in 5 year interval;
    Translate NTEM zones in Scotland into NorNITs zones; for England and Wales, NTEM zones = NorMITs zones (MSOAs)
    """

    # The year of data is set to define the upper and lower NTEM run years and interpolate as necessary between them.
    # The base year for NTEM is 2011 and it is run in 5-year increments from 2011 to 2051.
    # The year selected below must be between 2011 and 2051 (inclusive).
    Year = 2011

    if Year < 2011 | Year > 2051:
        raise ValueError("Please select a valid year of data.")
    else:
        pass

    Output_Folder = census_and_by_lu_obj.home_folder + '/Outputs/'
    print(Output_Folder)
    LogFile = Output_Folder + 'LogFile.txt'
    # 'I:/NorMITs Synthesiser/Zone Translation/'
    Zone_path = census_and_by_lu_obj.zones_folder + 'Export/ntem_to_msoa/ntem_msoa_pop_weighted_lookup.csv'
    Pop_Segmentation_path = census_and_by_lu_obj.import_folder + 'CTripEnd/Pop_Segmentations.csv'
    with open(LogFile, 'w') as o:
        o.write("Notebook run on - " + str(datetime.datetime.now()) + "\n")
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

    print("Lower Interpolation Year - " + str(LowerYear))
    print("Upper Interpolation Year - " + str(UpperYear))

    # Import Upper and Lower Year Tables
    # 'I:/Data/NTEM/NTEM 7.2 outputs for TfN/'
    LowerNTEMDatabase = census_and_by_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(LowerYear) + '.accdb'
    UpperNTEMDatabase = census_and_by_lu_obj.CTripEnd_Database_path + 'CTripEnd7_' + str(UpperYear) + '.accdb'
    # UpperNTEMDatabase = census_and_by_lu_obj.CTripEnd_Database_path + r"\CTripEnd7_" + str(UpperYear) + r".accdb"
    cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + \
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
    NTEM_HHpop = TZonePop_DataYear
    # Export
    Export_SummaryPop = TZonePop_DataYear.groupby(['TravellerType', 'NTEM_TT_Name']).sum()
    print(Export_SummaryPop.Population.sum())
    # Export_SummaryPop.drop(['msoaZoneID'], inplace=True, axis=1)
    PopOutput = "NTEM_{}_Population.csv".format(Year)

    with open(Output_Folder + PopOutput, "w", newline='') as f:
        TZonePop_DataYear.to_csv(f, header=True, sep=",")
    f.close()

    with open(LogFile, "a") as o:
        o.write("Total Population: \n")
        Export_SummaryPop.to_csv(o, header=False, sep="-")
        o.write("\n")
        o.write("\n")
    print("Export complete.")
    print(NTEM_HHpop.head(5))

    census_and_by_lu_obj.state['3.1.1 derive 2011 population from NTEM and convert Scottish zones'] = 1
    logging.info('3.1.1 derive 2011 population from NTEM and convert Scottish zones completed')

    return NTEM_HHpop

def Create_IPFN_Inputs_2011(census_and_by_lu_obj):
    """
    Create seed and control files at district sector level for IPFN process by:
    Calling function to obtain 2011 NTEM pop data;
    Expand this to have dimensions for dwelling type, NS-SEC and SOC using 2011 Census Data
    including processing Scotland in this way using an average of the North East and North West of England;
    Scaling QS606, QS609 and QS401 totals to NTEM population/worker totals to obtain control files for IPFN; and
    Chunking the outputs to districts as the IPFN process is incredibly slow to run on any larger area.
    Note that Scotland's chunks are defined as being the average district size in the North West of England
    in terms of number of zones within a district and are allocated based on the numeracy of the zones rather
    than on any geographical arrangement of zones in Scotland.
    Also produces a 'checks' output file to allow the user to check that all population totals that should match do
    """
    # Obtain 2011 Census data
    NTEM_pop_2011 = NTEM_Pop_Interpolation(census_and_by_lu_obj)

    # Start processing data
    # Only the following vairiables are required from the census micro data
    census_micro_trimmed = census_micro[['caseno',
                                         'country',
                                         'region',
                                         'la_group',
                                         'residence_type',
                                         'ageh',
                                         'ahchuk11',
                                         'carsnoc',
                                         'ecopuk11',
                                         'hours',
                                         'nsshuk11',
                                         'occg',
                                         'sex',
                                         'typaccom']]
    # Split into household (hh) population and shared dwelling population
    census_micro_hh_pop = census_micro_trimmed[census_micro_trimmed.residence_type == 2]
    census_micro_cer_pop = census_micro_trimmed[census_micro_trimmed.residence_type == 1]


    # Process age
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop,
        lookup_ageh[['Age', 'NorMITs_Segment Band Value']],
        left_on = 'ageh',
        right_on = 'Age',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'typaccom':'t',
                   'la_group':'d',
                   'NorMITs_Segment Band Value':'a'})

    # Process NSSEC
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_nsshuk11[['HRP NSSEC','NorMITs_Segment Band Value']],
        left_on = 'nsshuk11',
        right_on = 'HRP NSSEC',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Band Value':'n'}).dropna(subset = ['n'])
    census_micro_hh_pop_working['n'] = census_micro_hh_pop_working['n'].astype(int)

    # Process HH comp - lookup adults and cars to NorMITs and then the combination
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_ahchuk11[['Household size', 'NorMITs_Segment Band Value']],
        left_on = 'ahchuk11',
        right_on = 'Household size',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Band Value':'NorMITs_adults'})
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_carsnoc[['Household car','NorMITs_Segment Band Value']],
        left_on = 'carsnoc',
        right_on = 'Household car',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns={'NorMITs_Segment Band Value':'NorMITs_cars'})
    census_micro_hh_pop_working['NorMITs_adults'] = census_micro_hh_pop_working[
        'NorMITs_adults'].astype(str)
    census_micro_hh_pop_working['NorMITs_cars'] = census_micro_hh_pop_working[
        'NorMITs_cars'].astype(str)
    census_micro_hh_pop_working['adults_cars'] = census_micro_hh_pop_working[
        ['NorMITs_adults', 'NorMITs_cars']].agg('_'.join, axis = 1)
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_h[['Household Composition Key','Household_composition_code']],
        left_on = 'adults_cars',
        right_on = 'Household Composition Key',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'Household_composition_code':'h'})

    # Process gender
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_sex[['Sex','NorMITs_Segment Band Value']],
        left_on = 'sex',
        right_on = 'Sex',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Band Value':'g'})
    census_micro_hh_pop_working['g'] = np.where(
        census_micro_hh_pop_working['a'] == 1,
        1,
        census_micro_hh_pop_working['g'])

    # Process employment type
    #  - Check emplyoment type
    #  - Check age (children and retirees cannot work)
    #  - Check if students (ecopuk11 type 8) are fte or pte via hours
    census_micro_hh_pop_working=pd.merge(
        census_micro_hh_pop_working,
        lookup_ecopuk11[['Employment type code','NorMITs_Segment Band Value']],
        left_on = 'ecopuk11',
        right_on = 'Employment type code',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Band Value':'e'})
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_hours[['Hours worked ','NorMITs_Segment Definition']],
        left_on = 'hours',
        right_on = 'Hours worked ',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Definition':'ft-pt'})
    census_micro_hh_pop_working['e'] = np.where(
        census_micro_hh_pop_working['a'] == 2,
        np.where(
            census_micro_hh_pop_working['Employment type code'] == 8,
            np.where(
                census_micro_hh_pop_working['ft-pt'] == 'fte',1, 2),
                census_micro_hh_pop_working['e']), 5)
    census_micro_hh_pop_working['e'].replace('',np.nan, inplace = True)
    census_micro_hh_pop_working.dropna(subset = ['e'], inplace = True)
    census_micro_hh_pop_working['e'] = census_micro_hh_pop_working['e'].astype(int)

    # Process SOC
    census_micro_hh_pop_working = pd.merge(
        census_micro_hh_pop_working,
        lookup_occg[['SOC','NorMITs_Segment Band Value']],
        left_on = 'occg',
        right_on = 'SOC',
        how = 'left')
    census_micro_hh_pop_working = census_micro_hh_pop_working.rename(
        columns = {'NorMITs_Segment Band Value':'s'})
    census_micro_hh_pop_working['s'] = np.where(
        census_micro_hh_pop_working['e'] > 2, 4, census_micro_hh_pop_working['s'])
    census_micro_hh_pop_working['s'] = census_micro_hh_pop_working['s'].astype(int)
    census_micro_hh_pop_working['t'] = census_micro_hh_pop_working['t'].astype(int)

    # Have now produced all values
    # Trim down to just d,a,g,h,e,t,n,s (plus caseno)
    census_micro_hh_pop_by_caseno = census_micro_hh_pop_working[['caseno', 'd', 'a',
                                                                 'g', 'h', 'e',
                                                                 't', 'n', 's']]

    # Group into unique a, g, h, e, t, n, s to get workers "pivot table"
    census_micro_hh_pop_pivot = census_micro_hh_pop_by_caseno.copy()
    census_micro_hh_pop_pivot = census_micro_hh_pop_pivot.groupby(
        ['d','a','g','h','e','t','n','s'])['caseno'].nunique().reset_index()
    census_micro_hh_pop_pivot_workers = census_micro_hh_pop_pivot.loc[
        (census_micro_hh_pop_pivot['e'] <= 2) &
        (census_micro_hh_pop_pivot['s'] < 4)]

    worker_iterator = zip(census_micro_hh_pop_pivot_workers['d'],
                          census_micro_hh_pop_pivot_workers['a'],
                          census_micro_hh_pop_pivot_workers['g'],
                          census_micro_hh_pop_pivot_workers['h'],
                          census_micro_hh_pop_pivot_workers['e'])
    census_micro_hh_pop_pivot_workers['aghe_Key'] = ['_'.join(
        [str(d), str(a), str(g), str(h), str(e)]) for d, a, g, h, e in worker_iterator]

    # Create df of all possible worker ntem_tt, t, n, s combos as not all are used,
    #    furness process may require them.
    ntem_tt_workers = census_micro_hh_pop_pivot_workers['aghe_Key'].str[-7:].unique()
    workers_t = census_micro_hh_pop_pivot_workers['t'].unique()
    workers_n = census_micro_hh_pop_pivot_workers['n'].unique()
    workers_s = census_micro_hh_pop_pivot_workers['s'].unique()
    all_workers_tt_t_n_s = pd.DataFrame(itertools.product(ntem_tt_workers, workers_t, workers_n, workers_s))
    all_workers_tt_t_n_s = all_workers_tt_t_n_s.rename(columns = {0:'ntem_tt_Key', 1:'t', 2:'n', 3:'s'})

    # Group into unique a, g, h, e, t, n to get non-workers "pivot table"
    census_micro_hh_pop_pivot2 = census_micro_hh_pop_by_caseno.groupby(
        ['d','a','g','h','e','t','n'])['caseno'].nunique().reset_index()
    census_micro_hh_pop_pivot_non_workers = census_micro_hh_pop_pivot2.loc[
        (census_micro_hh_pop_pivot2['e'] > 2)]


    census_micro_hh_pop_pivot_non_workers['s'] = 4 # Explictally state we are looking at non-workers
    census_micro_hh_pop_pivot_non_workers = census_micro_hh_pop_pivot_non_workers[['d', 'a',
                                                                                   'g', 'h',
                                                                                   'e', 't',
                                                                                   'n', 's',
                                                                                   'caseno']]
    non_worker_iterator = zip(census_micro_hh_pop_pivot_non_workers['d'],
                              census_micro_hh_pop_pivot_non_workers['a'],
                              census_micro_hh_pop_pivot_non_workers['g'],
                              census_micro_hh_pop_pivot_non_workers['h'],
                              census_micro_hh_pop_pivot_non_workers['e'])
    census_micro_hh_pop_pivot_non_workers['aghe_Key'] = ['_'.join(
        [str(d), str(a), str(g), str(h), str(e)]) for d, a, g, h, e in non_worker_iterator]

    # Create df of all possible nonworker ntem_tt, t, n, s combos as not all are used,
    #    furness process may require them.
    ntem_tt_non_workers = census_micro_hh_pop_pivot_non_workers['aghe_Key'].str[-7:].unique()
    non_workers_t = census_micro_hh_pop_pivot_non_workers['t'].unique()
    non_workers_n = census_micro_hh_pop_pivot_non_workers['n'].unique()
    all_non_workers_tt_t_n = pd.DataFrame(itertools.product(ntem_tt_non_workers, non_workers_t, non_workers_n))
    all_non_workers_tt_t_n = all_non_workers_tt_t_n.rename(columns = {0:'ntem_tt_Key', 1:'t', 2:'n'})
    all_non_workers_tt_t_n_s = all_non_workers_tt_t_n.copy()
    all_non_workers_tt_t_n_s['s'] = 4 # s is always 4 for nonworkers, see above

    # Group into unique a, g, h, e to get P_(a, g, h, e)
    census_micro_hh_pop_pivot_pop_aghe = census_micro_hh_pop_by_caseno.groupby(
        ['d','a','g','h','e'])['caseno'].nunique().reset_index()
    population_iterator = zip(census_micro_hh_pop_pivot_pop_aghe['d'],
                              census_micro_hh_pop_pivot_pop_aghe['a'],
                              census_micro_hh_pop_pivot_pop_aghe['g'],
                              census_micro_hh_pop_pivot_pop_aghe['h'],
                              census_micro_hh_pop_pivot_pop_aghe['e'])
    census_micro_hh_pop_pivot_pop_aghe['aghe_Key'] = ['_'.join(
        [str(d), str(a), str(g), str(h), str(e)]) for d, a, g, h, e in population_iterator]
    census_micro_hh_pop_pivot_pop_aghe = census_micro_hh_pop_pivot_pop_aghe.drop(columns = ['d', 'a',
                                                                                            'g', 'h',
                                                                                            'e'])

    all_pop_aghetns_combos = all_workers_tt_t_n_s.append(
        all_non_workers_tt_t_n_s, ignore_index=True)

    # Find and replace missing zonal Census micro NTEM traveller types with EW averages
    # Find missing NTEM_tts.
    missing_tt_df = census_micro_hh_pop_pivot_pop_aghe.copy()
    missing_tt_df = missing_tt_df[['aghe_Key']]
    missing_tt_df['aghe_Key'] = missing_tt_df['aghe_Key'].str[-7:]
    missing_tt_df = missing_tt_df.drop_duplicates(subset=['aghe_Key']).reset_index()
    missing_tt_df = missing_tt_df.drop(columns = ['index'])
    expected_tt_count = 88
    if len(missing_tt_df) == expected_tt_count:
        print('No globally missing NTEM_tt')
    else:
        print('INCORRECT GLOBAL NTEM_tt TOTAL!')
        print('Expected', expected_tt_count)
        print('Got', len(missing_tt_df))

    model_districts = lookup_geography.copy()
    model_districts = model_districts['Grouped_LA']
    model_districts = model_districts.drop_duplicates().reset_index()
    model_districts = model_districts.dropna(subset=['Grouped_LA'])
    model_districts['Grouped_LA'] = model_districts['Grouped_LA'].astype(int)
    model_districts = model_districts.sort_values('Grouped_LA').reset_index()
    model_districts = model_districts.drop(columns = ['index', 'level_0'])

    missing_tt_df = pd.DataFrame(itertools.product(model_districts['Grouped_LA'], missing_tt_df['aghe_Key']))
    missing_tt_df = missing_tt_df.rename(columns = {0:'z', 1:'aghe_key'})
    missing_tt_df['aghe_Key'] = [
        '_'.join([str(x), y]) for x, y in zip(missing_tt_df['z'], missing_tt_df['aghe_key'])]
    missing_tt_df = missing_tt_df[['aghe_Key', 'z', 'aghe_key']]

    # START - Formula from next (original f creating) cell
    # Create function that relates tns to aghe
    census_micro_hh_pop_pivot_aghe = census_micro_hh_pop_pivot_workers.append(
        census_micro_hh_pop_pivot_non_workers, ignore_index=True)
    census_micro_hh_pop_pivot_aghe = census_micro_hh_pop_pivot_aghe.rename(
        columns = {'caseno':'Persons'})
    census_micro_hh_pop_pivot_aghe = pd.merge(census_micro_hh_pop_pivot_aghe,
                                              census_micro_hh_pop_pivot_pop_aghe,
                                              on='aghe_Key')
    census_micro_hh_pop_pivot_aghe = census_micro_hh_pop_pivot_aghe.rename(
        columns = {'caseno':'P_aghe'})
    # New bit
    average_EW_f = census_micro_hh_pop_pivot_aghe.copy()
    average_EW_f['aghe_Key'] = average_EW_f['aghe_Key'].str[-7:]
    average_EW_f = average_EW_f.groupby(
        ['aghe_Key', 't', 'n', 's'])['Persons'].sum().reset_index()
    P_for_average_EW_f = census_micro_hh_pop_pivot_aghe.copy()
    P_for_average_EW_f = P_for_average_EW_f.drop_duplicates(subset=['aghe_Key']).reset_index()
    P_for_average_EW_f['aghe_Key'] = P_for_average_EW_f['aghe_Key'].str[-7:]
    P_for_average_EW_f = P_for_average_EW_f.groupby('aghe_Key')['P_aghe'].sum().reset_index()

    average_EW_f = pd.merge(average_EW_f, P_for_average_EW_f, how = 'left')
    average_EW_f['f_tns/aghe'] = (average_EW_f ['Persons'] / average_EW_f ['P_aghe'])
    average_EW_f = average_EW_f.rename(columns = {'aghe_Key':'aghe_key'})
    average_EW_f = average_EW_f.drop(columns = ['Persons', 'P_aghe'])

    #End new bit
    census_micro_hh_pop_pivot_aghe['f_tns/aghe'] = (census_micro_hh_pop_pivot_aghe['Persons']
                                                    / census_micro_hh_pop_pivot_aghe['P_aghe'])
    census_micro_hh_pop_pivot_aghe = census_micro_hh_pop_pivot_aghe.drop(
        columns=['Persons', 'P_aghe'])
    # END - Formula from next (original f creating) cell

    fill_missing_aghe = missing_tt_df.copy() # Create df to become output of process
    fill_missing_aghe = pd.merge(fill_missing_aghe, census_micro_hh_pop_pivot_aghe, how = 'outer')
    find_missing_aghe = fill_missing_aghe.copy() # Create df to become 'missing' rows only
    find_missing_aghe = find_missing_aghe[fill_missing_aghe.isnull().any(axis=1)].reset_index()
    find_missing_aghe = find_missing_aghe[['aghe_Key']]
    find_missing_aghe['flag'] = 'flag' # flag the missing rows to attach to a df of all possible daghe combos

    # Drop the missing values, we'll add in some replacements later
    fill_missing_aghe = fill_missing_aghe.dropna(subset=['d'])
    daghents_variables = ['d', 'a', 'g', 'h', 'e', 't', 'n', 's']
    for x in daghents_variables:
        fill_missing_aghe[x] = fill_missing_aghe[x].astype(int)

    # Create every possible d, a, g, h, e, t, n, s combination
    fill_missing_aghe_all_combos = missing_tt_df.copy()
    fill_missing_aghe_all_combos = pd.merge(fill_missing_aghe_all_combos, average_EW_f, how = 'outer')

    # Attach the flags that say a combination is missing in the Census Microdata
    #     to the matching instances in the df that contains every possible
    #     d, a, g, h, e, t, n, s combination.
    # Then cut the df down to just the 'missing' rows.
    # Finally reformat the df to match fill_missing_aghe frame for easy appending.
    missing_aghe = fill_missing_aghe_all_combos.copy()
    missing_aghe = pd.merge(missing_aghe, find_missing_aghe, how = 'outer')
    missing_aghe = missing_aghe.dropna(subset=['flag']).drop(columns = ['flag'])
    missing_aghe['d'] = missing_aghe['z']
    missing_aghe['a'] = missing_aghe['aghe_key'].str[0].astype(int)
    missing_aghe['g'] = missing_aghe['aghe_key'].str[2].astype(int)
    missing_aghe['h'] = missing_aghe['aghe_key'].str[4].astype(int)
    missing_aghe['e'] = missing_aghe['aghe_key'].str[6].astype(int)
    missing_aghe = missing_aghe[['aghe_Key', 'z', 'aghe_key',
                                 'd', 'a', 'g', 'h',
                                 'e', 't', 'n', 's',
                                 'f_tns/aghe']]

    # Append all the rows we flagged as being required to fill in missing values to the df
    #     where we first noted they were missing.
    fill_missing_aghe = fill_missing_aghe.append(
        missing_aghe, ignore_index = True)
    fill_missing_aghe.sort_values(by = ['d', 'aghe_key', 't', 'n', 's',], inplace = True)
    fill_missing_aghe = fill_missing_aghe.reset_index().drop(columns = ['index'])
    fill_missing_aghe = fill_missing_aghe[census_micro_hh_pop_pivot_aghe.columns]
    # Name output of this process something that makes a bit more sense in later stages!
    cencus_micro_complete_f = fill_missing_aghe.copy()

    # Check total f is correct
    EW_f_sum = round(fill_missing_aghe['f_tns/aghe'].sum())
    EW_d_count = fill_missing_aghe['d'].nunique()
    EW_f_by_d = EW_f_sum / EW_d_count
    if EW_f_by_d == expected_tt_count:
        print('f conbinations appear valid')
    else:
        print('ISSUE WITH f PROCESSING!')

    # Trim NTEM to just the useful cols
    NTEM_pop_2011_trim = NTEM_pop_2011[['msoaZoneID',
                                        'AreaType',
                                        'TravellerType',
                                        'Age_code',
                                        'Gender_code',
                                        'Household_composition_code',
                                        'Employment_type_code',
                                        'Population']]
    NTEM_pop_2011_trim = NTEM_pop_2011_trim.rename(
        columns = {'msoaZoneID':'z',
                   'AreaType':'A',
                   'TravellerType':'ntem_tt',
                   'Age_code':'a',
                   'Gender_code':'g',
                   'Household_composition_code':'h',
                   'Employment_type_code':'e',
                   'Population':'P_NTEM'})

    # Join the districts and regions to the zones
    lookup_geography_z2d2r = lookup_geography[['NorMITs_Zone',
                                               'Grouped_LA',
                                               'NorMITs_Region']]
    lookup_geography_z2d2r = lookup_geography_z2d2r.rename(
        columns={'NorMITs_Zone':'z','Grouped_LA':'d', 'NorMITs_Region':'r'})
    NTEM_pop_2011_trim = pd.merge(NTEM_pop_2011_trim,
                                  lookup_geography_z2d2r,
                                  on='z')

    # Drop the Scottish districts and apply f to England and Wales
    NTEM_pop_2011_EW = NTEM_pop_2011_trim.copy()
    NTEM_pop_2011_EW = NTEM_pop_2011_EW.dropna(subset = ['d']) # Only Scotland has n/a in districts
    test_tot_EW = NTEM_pop_2011_EW['P_NTEM'].sum()
    NTEM_pop_2011_EW['d'] = NTEM_pop_2011_EW['d'].astype(int)
    NTEM_population_iterator_EW = zip(NTEM_pop_2011_EW['d'],
                                      NTEM_pop_2011_EW['a'],
                                      NTEM_pop_2011_EW['g'],
                                      NTEM_pop_2011_EW['h'],
                                      NTEM_pop_2011_EW['e'])
    NTEM_pop_2011_EW['aghe_Key'] = ['_'.join([str(d), str(a), str(g), str(h), str(e)])
                                  for d, a, g, h, e in NTEM_population_iterator_EW]
    NTEM_pop_2011_EW = pd.merge(NTEM_pop_2011_EW,
                                cencus_micro_complete_f,
                                on='aghe_Key')
    NTEM_pop_2011_EW = NTEM_pop_2011_EW.drop(
        columns = ['d_x','a_x','g_x','h_x','e_x'])
    NTEM_pop_2011_EW = NTEM_pop_2011_EW.rename(
        columns = {'d_y':'d', 'a_y':'a', 'g_y':'g', 'h_y':'h', 'e_y':'e'})

    # Filter to obtain just North East/North West.
    # Recalculate f by A for these regions.
    NTEM_pop_2011_NENW = NTEM_pop_2011_EW.copy()
    NTEM_pop_2011_NENW = NTEM_pop_2011_NENW.loc[(
        NTEM_pop_2011_NENW['r'] == 'North East') | (NTEM_pop_2011_NENW['r'] == 'North West')]
    NTEM_pop_2011_NENW_Aaghe = NTEM_pop_2011_NENW.copy()
    NTEM_pop_2011_NENW_Aaghe = NTEM_pop_2011_NENW_Aaghe.groupby(
        ['A', 'a', 'g', 'h', 'e'])['P_NTEM'].sum().reset_index()
    NTEM_pop_2011_NENW = NTEM_pop_2011_NENW.groupby(
        ['A', 'a', 'g', 'h', 'e', 't', 'n', 's'])['P_NTEM'].sum().reset_index()
    NTEM_pop_2011_NENW = pd.merge(NTEM_pop_2011_NENW,
                                  NTEM_pop_2011_NENW_Aaghe,
                                  how='left',
                                  left_on=['A','a', 'g', 'h', 'e'],
                                  right_on = ['A','a', 'g', 'h', 'e'])
    NTEM_pop_2011_NENW = NTEM_pop_2011_NENW.rename(
        columns = {'P_NTEM_x':'Persons',
                   'P_NTEM_y':'P_aghe'})
    NTEM_pop_2011_NENW['f_tns/aghe'] = (NTEM_pop_2011_NENW['Persons']
                                        / NTEM_pop_2011_NENW['P_aghe'])
    NTEM_pop_2011_NENW = NTEM_pop_2011_NENW [['A', 'a', 'g', 'h', 'e', 't', 'n', 's', 'f_tns/aghe']]
    NTEM_population_iterator_NENW = zip(NTEM_pop_2011_NENW['A'],
                                        NTEM_pop_2011_NENW['a'],
                                        NTEM_pop_2011_NENW['g'],
                                        NTEM_pop_2011_NENW['h'],
                                        NTEM_pop_2011_NENW['e'])
    NTEM_pop_2011_NENW['Aaghe_Key'] = [
        '_'.join([str(A), str(a), str(g), str(h), str(e)])
        for A, a, g, h, e in NTEM_population_iterator_NENW]

    # Get just the Scottish NTEM Pop data and apply f to it
    NTEM_pop_2011_S = NTEM_pop_2011_trim.copy()
    NTEM_pop_2011_S = NTEM_pop_2011_S[NTEM_pop_2011_S['d'].isnull()]
    test_tot_S = NTEM_pop_2011_S['P_NTEM'].sum()
    NTEM_population_iterator_S = zip(NTEM_pop_2011_S['A'],
                                     NTEM_pop_2011_S['a'],
                                     NTEM_pop_2011_S['g'],
                                     NTEM_pop_2011_S['h'],
                                     NTEM_pop_2011_S['e'])
    NTEM_pop_2011_S['Aaghe_Key'] = [
        '_'.join([str(A), str(a), str(g), str(h), str(e)])
        for A, a, g, h, e in NTEM_population_iterator_S]
    NTEM_pop_2011_S = pd.merge(NTEM_pop_2011_S,
                               NTEM_pop_2011_NENW,
                               on = 'Aaghe_Key')
    NTEM_pop_2011_S = NTEM_pop_2011_S.drop(
        columns = ['A_x', 'a_x', 'g_x', 'h_x', 'e_x', 'Aaghe_Key'])
    NTEM_pop_2011_S = NTEM_pop_2011_S.rename(
        columns = {'A_y':'A', 'a_y':'a', 'g_y':'g', 'h_y':'h', 'e_y':'e'})
    NTEM_pop_2011_S['d'] = NTEM_pop_2011_S['d'].fillna(0).astype(int)
    NTEM_pop_2011_S['r'] = NTEM_pop_2011_S['r'].fillna('Scotland')

    # Merge the England&Wales and Scotland dataframes
    NTEM_pop_2011_EW = NTEM_pop_2011_EW.drop(columns = ['aghe_Key'])
    NTEM_pop_2011_col_order = ['z', 'A', 'd', 'r', 'a', 'g', 'h', 'e', 't', 'n', 's',
                               'ntem_tt', 'P_NTEM', 'f_tns/aghe']
    NTEM_pop_2011_EW = NTEM_pop_2011_EW[NTEM_pop_2011_col_order]
    NTEM_pop_2011_S = NTEM_pop_2011_S[NTEM_pop_2011_col_order]
    NTEM_pop_2011_GB = NTEM_pop_2011_EW.append(NTEM_pop_2011_S, ignore_index=True)

    # Print some totals out to check...
    print('Actual EW tot:' + str(test_tot_EW))
    print('Actual S tot: ' + str(test_tot_S))
    print('Actual GB tot:' + str(test_tot_S + test_tot_EW))
    print('Scaled EW tot:' + str((NTEM_pop_2011_EW['P_NTEM']*NTEM_pop_2011_EW['f_tns/aghe']).sum()))
    print('Scaled S tot: ' + str((NTEM_pop_2011_S['P_NTEM']*NTEM_pop_2011_S['f_tns/aghe']).sum()))
    print('Scaled GB tot:' + str((NTEM_pop_2011_S['P_NTEM']*NTEM_pop_2011_S['f_tns/aghe']).sum()
          + (NTEM_pop_2011_EW['P_NTEM']*NTEM_pop_2011_EW['f_tns/aghe']).sum()))

    # Create zonal worker pop for furness control files
    NTEM_workers_2011_GB = NTEM_pop_2011_trim.loc[
        NTEM_pop_2011_trim['e'] < 3].reset_index()
    NTEM_workers_2011_GB = NTEM_workers_2011_GB.groupby(
        ['z'])['P_NTEM'].sum().reset_index()
    NTEM_workers_2011_GB = NTEM_workers_2011_GB.rename(
        columns = {'P_NTEM':'Workers_NTEM'})

    # Start seed creation process
    all_zones = NTEM_pop_2011_GB['z'].unique()
    all_aghetns_combos_iterator = zip(all_pop_aghetns_combos['ntem_tt_Key'],
                                      all_pop_aghetns_combos['t'],
                                      all_pop_aghetns_combos['n'],
                                      all_pop_aghetns_combos['s'])
    all_pop_aghetns_combos['all_aghetns'] = [
        '_'.join([str(ntem_tt_Key), str(t), str(n), str(s)])
        for ntem_tt_Key, t, n, s in all_aghetns_combos_iterator]
    all_pop_z_tt_t_n_s = pd.DataFrame(itertools.product(
        all_zones,
        all_pop_aghetns_combos['all_aghetns']))
    all_pop_z_tt_t_n_s = all_pop_z_tt_t_n_s.rename(columns = {0:'z', 1:'aghetns'})
    all_pop_z_tt_t_n_s['zaghetns'] = [
        '_'.join(i) for i in zip(all_pop_z_tt_t_n_s['z'].map(str),
                                 all_pop_z_tt_t_n_s['aghetns'])]
    all_pop_z_tt_t_n_s = all_pop_z_tt_t_n_s[['z','zaghetns']]

    # This step is required to remove duplicate zaghetns combos in Scotland.
    # These occur as Scottish zones can have multiple area types
    #    and are determined by Area type.
    NTEM_pop_2011_GB['P_aghetns'] = (NTEM_pop_2011_GB['f_tns/aghe']
                                     * NTEM_pop_2011_GB['P_NTEM'])
    NTEM_aghetns_iterator = zip(NTEM_pop_2011_GB['z'],
                                NTEM_pop_2011_GB['a'],
                                NTEM_pop_2011_GB['g'],
                                NTEM_pop_2011_GB['h'],
                                NTEM_pop_2011_GB['e'],
                                NTEM_pop_2011_GB['t'],
                                NTEM_pop_2011_GB['n'],
                                NTEM_pop_2011_GB['s'],)
    NTEM_pop_2011_GB['zaghetns'] = [
        '_'.join([str(z), str(a), str(g), str(h), str(e), str(t), str(n), str(s)])
        for z, a, g, h, e, t, n, s in NTEM_aghetns_iterator]
    NTEM_pop_2011_GB_for_seeds = NTEM_pop_2011_GB.groupby(
        ['z', 'a', 'g', 'h', 'e', 't', 'n', 's', 'ntem_tt', 'zaghetns'])['P_aghetns'].sum().reset_index()

    # Drop z again as we don't really need it until later
    NTEM_pop_2011_GB_for_seeds = NTEM_pop_2011_GB_for_seeds.drop(columns = ['z'])

    NTEM_pop_2011_GB_for_seeds = all_pop_z_tt_t_n_s.merge(NTEM_pop_2011_GB_for_seeds, on = 'zaghetns', how = 'left')

    NTEM_pop_2011_GB_for_seeds['a'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-13:-12].astype(int)
    NTEM_pop_2011_GB_for_seeds['g'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-11:-10].astype(int)
    NTEM_pop_2011_GB_for_seeds['h'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-9:-8].astype(int)
    NTEM_pop_2011_GB_for_seeds['e'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-7:-6].astype(int)
    NTEM_pop_2011_GB_for_seeds['t'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-5:-4].astype(int)
    NTEM_pop_2011_GB_for_seeds['n'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-3:-2].astype(int)
    NTEM_pop_2011_GB_for_seeds['s'] = NTEM_pop_2011_GB_for_seeds['zaghetns'].str[-1:].astype(int)
    NTEM_pop_2011_GB_for_seeds['P_aghetns'] = NTEM_pop_2011_GB_for_seeds['P_aghetns'].fillna(0)
    NTEM_pop_2011_GB_for_seeds['ntem_tt'] = NTEM_pop_2011_GB_for_seeds['ntem_tt'].str[-3:].astype(float)

    ntem_tt_key = NTEM_pop_2011_GB_for_seeds.groupby(['a', 'g', 'h', 'e'])['ntem_tt'].mean().reset_index()
    ntem_tt_key['ntem_tt'] = ntem_tt_key['ntem_tt'].astype(int)

    NTEM_pop_2011_GB_for_seeds = NTEM_pop_2011_GB_for_seeds.drop(columns = ['ntem_tt'])
    NTEM_pop_2011_GB_for_seeds = pd.merge(
        NTEM_pop_2011_GB_for_seeds,
        ntem_tt_key,
        how = 'left',
        on = ['a', 'g', 'h', 'e'])

    NTEM_pop_2011_GB_for_dr_seeds = pd.merge(NTEM_pop_2011_GB_for_seeds,
                                            lookup_geography_z2d2r,
                                            on='z')
    NTEM_pop_2011_GB_for_dr_seeds['d'] = NTEM_pop_2011_GB_for_dr_seeds['d'].fillna(999).astype(int)
    NTEM_pop_2011_GB_for_dr_seeds['r'] = NTEM_pop_2011_GB_for_dr_seeds['r'].fillna('Scotland')
    seed_r_NW = NTEM_pop_2011_GB_for_dr_seeds.loc[
        NTEM_pop_2011_GB_for_dr_seeds['r'] == 'North West'].reset_index()
    seed_d_184 = NTEM_pop_2011_GB_for_dr_seeds.loc[
        NTEM_pop_2011_GB_for_dr_seeds['d'] == 184].reset_index()
    seed_headers = ['z', 'ntem_tt', 't', 'n', 's', 'P_aghetns']
    seed_r_NW = seed_r_NW[seed_headers]
    seed_d_184 = seed_d_184[seed_headers]
    seed_r_NW = seed_r_NW.rename(columns = {'P_aghetns':'population'})
    seed_d_184 = seed_d_184.rename(columns = {'P_aghetns':'population'})

    seed_df = NTEM_pop_2011_GB_for_seeds[['z', 'ntem_tt', 't', 'n', 's', 'P_aghetns']]
    seed_df = seed_df.rename(columns = {'P_aghetns':'population'})
    seed_df_dr = NTEM_pop_2011_GB_for_dr_seeds[['d', 'r', 'z', 'ntem_tt', 't', 'n', 's', 'P_aghetns']]
    seed_df_dr = seed_df_dr.rename(columns = {'P_aghetns':'population'})

    headers_QS606 = list(QS606_raw_census)
    grouped_headers_QS606 = []
    for h in headers_QS606:
        if h[0] == '1' or h[0] == '2' or h[0] == '3':
            QS606_head_name_iterator = ['h_', h[0]]
        elif h[0] == '4' or h[0] == '5' or h[0] == '6' or h[0] == '7':
            QS606_head_name_iterator = ['m_', h[0]]
        elif h[0] == '8' or h[0] == '9':
            QS606_head_name_iterator = ['s_', h[0]]
        else:
            QS606_head_name_iterator = ['', h]
        grouped_headers_QS606.append(''.join(QS606_head_name_iterator))
    QS606_working = QS606_raw_census.copy()
    QS606_working.columns = grouped_headers_QS606
    QS606_working['higher'] = (QS606_working['h_1'] + QS606_working['h_2']
                               + QS606_working['h_3'])
    QS606_working['medium'] = (QS606_working['m_4'] + QS606_working['m_5']
                               + QS606_working['m_6'] + QS606_working['m_7'])
    QS606_working['skilled'] = QS606_working['s_8'] + QS606_working['s_9']
    QS606_working = QS606_working[['mnemonic', 'higher', 'medium', 'skilled',
                                   'All categories: Occupation']]
    QS606_working = QS606_working.rename(
        columns = {'All categories: Occupation':'Workers_Census'})

    # Get zonal geography
    lookup_geography_la2z = lookup_geography[['MSOA', 'NorMITs_Zone']]
    lookup_geography_la2z.columns = ['mnemonic', 'z']
    QS606_working = pd.merge(QS606_working, lookup_geography_la2z, on='mnemonic')
    QS606_working = pd.merge(QS606_working, NTEM_workers_2011_GB, on='z')

    # Get nonworkers (NTEM values, not scaled)
    QS606_nonworkers = NTEM_pop_2011_trim.loc[
        NTEM_pop_2011_trim['e'] >= 3].reset_index() # SOC categories >= 3 == nonworkers
    QS606_nonworkers = QS606_nonworkers.rename(
        columns = {'P_NTEM':'non-workers'})
    QS606_nonworkers = QS606_nonworkers.groupby(['z'])['non-workers'].sum().reset_index()
    QS606_working = pd.merge(QS606_working, QS606_nonworkers, on='z')

    # Scale workers and reformat to output style
    QS606_working['Scaler'] = (QS606_working['Workers_NTEM']
                               / QS606_working['Workers_Census'])
    QS606_working['higher'] = QS606_working['higher'] * QS606_working['Scaler']
    QS606_working['medium'] = QS606_working['medium'] * QS606_working['Scaler']
    QS606_working['skilled'] = QS606_working['skilled'] * QS606_working['Scaler']
    QS606_working = QS606_working.melt(id_vars = ['z'],
                                       value_vars = ['higher',
                                                     'medium',
                                                     'skilled',
                                                     'non-workers'])
    QS606_working = QS606_working.rename(
        columns = {'variable':'SOC','value':'Persons'})
    QS606_working['s'] = np.where(QS606_working['SOC'] == 'higher', 1,
                                  np.where(QS606_working['SOC'] == 'medium', 2,
                                          np.where(QS606_working['SOC'] == 'skilled', 3, 4)))
    QS606_working = QS606_working.sort_values(by = ['z', 's']).reset_index()
    QS606_working = QS606_working[['z', 's', 'Persons']]

    headers_QS609 = list(QS609_raw_census)
    NSSeC_headers_QS609 = []
    for h in headers_QS609:
        if h[0].isdigit() == True:
            QS609_head_name_iterator = ['NS-SeC ', h[0]]
        elif h[0:3] == "L15":
            QS609_head_name_iterator = ['NS-SeC ', h[0:3]]
        else:
            QS609_head_name_iterator = ['', h]
        NSSeC_headers_QS609.append(''.join(QS609_head_name_iterator))
    QS609_working = QS609_raw_census.copy()
    QS609_working.columns = NSSeC_headers_QS609
    QS609_working['NS-SeC 1-2'] = (QS609_working['NS-SeC 1']
                                   + QS609_working['NS-SeC 2'])
    QS609_working['NS-SeC 3-5'] = (QS609_working['NS-SeC 3']
                                   + QS609_working['NS-SeC 4']
                                   + QS609_working['NS-SeC 5'])
    QS609_working['NS-SeC 6-7'] = (QS609_working['NS-SeC 6']
                                   + QS609_working['NS-SeC 7'])
    QS609_working = QS609_working[['mnemonic', 'NS-SeC 1-2', 'NS-SeC 3-5',
                                   'NS-SeC 6-7', 'NS-SeC 8', 'NS-SeC L15',
                                   'All categories: NS-SeC']]
    QS609_working = QS609_working.rename(
        columns = {'All categories: NS-SeC':'Total'})
    NTEM_pop_2011_zonal = NTEM_pop_2011_trim.groupby(['z'])['P_NTEM'].sum().reset_index()
    QS609_working = pd.merge(QS609_working, lookup_geography_la2z,
                             on = 'mnemonic')
    QS609_working = pd.merge(QS609_working, NTEM_pop_2011_zonal, on = 'z')
    QS609_working['Scaler'] = QS609_working['P_NTEM'] / QS609_working['Total']
    QS609_working['NS-SeC 1-2'] = (QS609_working['NS-SeC 1-2']
                                   * QS609_working['Scaler'])
    QS609_working['NS-SeC 3-5'] = (QS609_working['NS-SeC 3-5']
                                   * QS609_working['Scaler'])
    QS609_working['NS-SeC 6-7'] = (QS609_working['NS-SeC 6-7']
                                   * QS609_working['Scaler'])
    QS609_working['NS-SeC 8'] = (QS609_working['NS-SeC 8']
                                 * QS609_working['Scaler'])
    QS609_working['NS-SeC L15'] = (QS609_working['NS-SeC L15']
                                   * QS609_working['Scaler'])
    QS609_working = QS609_working.melt(id_vars = ['z'],
                                       value_vars = ['NS-SeC 1-2',
                                                     'NS-SeC 3-5',
                                                     'NS-SeC 6-7',
                                                     'NS-SeC 8',
                                                     'NS-SeC L15'])
    QS609_working = QS609_working.sort_values(by = ['z', 'variable']).reset_index()
    QS609_working = QS609_working.rename(
        columns = {'variable':'NSSEC', 'value':'Persons'})
    QS609_working['n'] = np.where(QS609_working['NSSEC'] == 'NS-SeC 1-2', 1,
                                  np.where(QS609_working['NSSEC'] == 'NS-SeC 3-5', 2,
                                           np.where(QS609_working['NSSEC'] == 'NS-SeC 6-7', 3,
                                                    np.where(QS609_working['NSSEC'] == 'NS-SeC 8', 4,
                                                             5))))
    QS609_working = QS609_working.drop(columns = ['index', 'NSSEC'])
    QS609_working = QS609_working[['z', 'n', 'Persons']]

    headers_QS401 = list(QS401_raw_census)
    DT_headers_QS401 = []
    for h in headers_QS401:
        if 'Detached' in h:
            DT_headers_QS401.append('Detached')
        elif 'Semi-detached' in h:
            DT_headers_QS401.append('Semi-detached')
        elif 'Terraced' in h:
            DT_headers_QS401.append('Terraced')
        elif 'Flat' in h:
            h_list = h.split()
            flat_name_iterator = ['Flat', h_list[-1]]
            DT_headers_QS401.append('_'.join(flat_name_iterator))
        else:
            h_list = h.split()
            if len(h_list) > 8:
                DT_headers_QS401.append('Caravan')
            else:
                DT_headers_QS401.append(h)
    QS401_working = QS401_raw_census.copy()
    QS401_working.columns = DT_headers_QS401
    QS401_working = QS401_working[['mnemonic',
                                   'Detached',
                                   'Semi-detached',
                                   'Terraced',
                                   'Flat_Total',
                                   'Caravan',
                                   'Shared dwelling',
                                   'All categories: Accommodation type']]
    QS401_working['Flat'] = (QS401_working['Flat_Total']
                             + QS401_working['Caravan']
                             + QS401_working['Shared dwelling'])
    QS401_working = QS401_working[['mnemonic',
                                   'Detached',
                                   'Semi-detached',
                                   'Terraced',
                                   'Flat',
                                   'All categories: Accommodation type']]
    QS401_working = QS401_working.rename(
        columns = {'All categories: Accommodation type':'Census_Pop'})
    QS401_working = pd.merge(QS401_working, lookup_geography_la2z, on='mnemonic')
    QS401_working = pd.merge(QS401_working, NTEM_pop_2011_zonal, on='z')
    QS401_working['Scaler'] = QS401_working['P_NTEM'] / QS401_working['Census_Pop']
    QS401_working['Detached'] = QS401_working['Detached'] * QS401_working['Scaler']
    QS401_working['Semi-detached'] = QS401_working['Semi-detached'] * QS401_working['Scaler']
    QS401_working['Terraced'] = QS401_working['Terraced'] * QS401_working['Scaler']
    QS401_working['Flat'] = QS401_working['Flat'] * QS401_working['Scaler']
    QS401_working = QS401_working.melt(id_vars = ['z'],
                                       value_vars = ['Detached',
                                                     'Semi-detached',
                                                     'Terraced',
                                                     'Flat'])
    QS401_working = QS401_working.sort_values(by = ['z', 'variable']).reset_index()
    QS401_working = QS401_working.rename(
        columns = {'variable':'DT', 'value':'Persons'})
    QS401_working['t'] = np.where(QS401_working['DT'] == 'Detached', 1,
                                  np.where(QS401_working['DT'] == 'Semi-detached', 2,
                                           np.where(QS401_working['DT'] == 'Terraced', 3, 4)))
    QS401_working = QS401_working.drop(columns = ['index', 'DT'])
    QS401_working = QS401_working[['z', 't', 'Persons']]

    # Sort out district lookups and apply 'districts' to Scotland
    # by grouping zones numerically to the NW average district size.
    lookup_geography_EW = lookup_geography.dropna(subset = ['Grouped_LA'])
    lookup_geography_EW = lookup_geography_EW.copy() # Prevents next line tripping a warning for no apparent reason!
    lookup_geography_EW['d'] = lookup_geography_EW['Grouped_LA'].astype(int)
    lookup_geography_GB = lookup_geography_EW.copy()
    lookup_geography_EW = lookup_geography_EW.groupby(['d'])['NorMITs_Zone'].nunique().reset_index()
    average_EW_district_size = lookup_geography_EW['NorMITs_Zone'].mean()
    max_EW_district = lookup_geography_EW['d'].max()
    ave_district_size_rounded = round(average_EW_district_size)

    lookup_geography_S = lookup_geography[lookup_geography['Grouped_LA'].isnull()].reset_index(drop = True)
    lookup_geography_S = lookup_geography_S[['NorMITs_Zone', 'Grouped_LA']]
    lookup_geography_S = lookup_geography_S.rename(columns = {'NorMITs_Zone':'z', 'Grouped_LA':'d'})
    lookup_geography_S['scottish_z_count'] = lookup_geography_S.index
    lookup_geography_S['d'] = (lookup_geography_S['scottish_z_count'] // ave_district_size_rounded) + 1
    lookup_geography_S['d'] = lookup_geography_S['d'] + max_EW_district
    lookup_geography_S = lookup_geography_S[['z', 'd']]

    lookup_geography_GB = lookup_geography_GB[['NorMITs_Zone', 'd']]
    lookup_geography_GB = lookup_geography_GB.rename(columns = {'NorMITs_Zone':'z', 'Grouped_LA':'d'})
    lookup_geography_GB = lookup_geography_GB.append(lookup_geography_S, ignore_index=True)

    lookup_geography_z2d2r_with_S = lookup_geography_GB.merge(lookup_geography_z2d2r,
                                                              left_on=['z', 'd'],
                                                              right_on=['z', 'd'],
                                                              how = 'left')
    lookup_geography_z2d2r_with_S['r'] = lookup_geography_z2d2r_with_S['r'].fillna('Scotland')

    # Apply districts to the seed and control file dataframes (including the Scottish 'districts'),
    # then produce the various IPFN input files at the district level, ensuring they are sensibly filed!
    # Bits (mostly groupbys/merges) that do not need looping over
    QS606_working_dr = pd.merge(QS606_working,
                                lookup_geography_z2d2r_with_S,
                                on='z')
    QS609_working_dr = pd.merge(QS609_working,
                                lookup_geography_z2d2r_with_S,
                                on='z')
    QS401_working_dr = pd.merge(QS401_working,
                                lookup_geography_z2d2r_with_S,
                                on='z')
    NTEM_pop_2011_GB_for_dr_seeds = pd.merge(NTEM_pop_2011_GB_for_seeds,
                                             lookup_geography_z2d2r_with_S,
                                             on='z')
    Ctrl1_NTEM = seed_df_dr.drop(columns=['d', 'r'])
    Ctrl1_NTEM = pd.merge(Ctrl1_NTEM,
                          lookup_geography_z2d2r_with_S,
                          on='z')
    Ctrl1_NTEM = Ctrl1_NTEM.groupby(['d', 'r', 'z', 'ntem_tt'])['population'].sum().reset_index()

    # set up for loop for 1 -> Max d in data (313)
    # str version of the number is to append to file names
    district_upper_limit = lookup_geography_z2d2r_with_S['d'].max() + 1
    os.chdir(Output_Folder)
    print('Creating seed and control files')
    print('Printing every 10th district as they are written out:')
    for district in range(1, district_upper_limit):
        dist_str = str(district)
        # Lookups and processing
        #     QS Control files
        QS606_working_d = QS606_working_dr.loc[
            QS606_working_dr['d'] == district].reset_index()
        QS609_working_d = QS609_working_dr.loc[
            QS609_working_dr['d'] == district].reset_index()
        QS401_working_d = QS401_working_dr.loc[
            QS401_working_dr['d'] == district].reset_index()
        #     Main seed file
        seed_d = NTEM_pop_2011_GB_for_dr_seeds.loc[
            NTEM_pop_2011_GB_for_dr_seeds['d'] == district].reset_index()
        seed_headers = ['z', 'ntem_tt', 't', 'n', 's', 'P_aghetns']
        seed_d = seed_d[seed_headers]
        seed_d = seed_d.rename(columns={'P_aghetns': 'population'})
        #     Ctrl1 control file
        Ctrl1_NTEM_d = Ctrl1_NTEM.loc[
            Ctrl1_NTEM['d'] == district].reset_index()
        Ctrl1_NTEM_headers = ['z', 'ntem_tt', 'population']
        Ctrl1_NTEM_d = Ctrl1_NTEM_d[Ctrl1_NTEM_headers]

        # Name outputs
        seed_filename_d = ['01 Seed Files/2011', str(ModelName), 'seed_d', dist_str, 'v0.1.csv']
        Ctrl1_NTEM_filename_d = [
            '02 Ctrl1 NTEM Control Files/2011', str(ModelName), 'Ctrl1_NTEM_d', dist_str, 'v0.1.csv']
        SOC_filename_d = ['03 SOC Control Files/2011', str(ModelName), 'Ctrl_SOC_d', dist_str, 'v0.1.csv']
        NSSEC_filename_d = ['04 NSSEC Control Files/2011', str(ModelName), 'Ctrl_NSSEC_d', dist_str, 'v0.1.csv']
        DT_filename_d = ['05 DT Control Files/2011', str(ModelName), 'Ctrl_DT_d', dist_str, 'v0.1.csv']

        # save outputs
        QS606_working_d.to_csv('_'.join(SOC_filename_d), index=False)
        QS609_working_d.to_csv('_'.join(NSSEC_filename_d), index=False)
        QS401_working_d.to_csv('_'.join(DT_filename_d), index=False)
        seed_d.to_csv('_'.join(seed_filename_d), index=False)
        Ctrl1_NTEM_d.to_csv('_'.join(Ctrl1_NTEM_filename_d), index=False)

        # Print out every tenth row to check on progress
        if district / 10 == district // 10:
            print(district)
    print('All district level seed and control files have been printed out as csvs')
    print('Now working on checks...')

    # Check control file totals
    QS606_check_tot = QS606_working.groupby(['z'])['Persons'].sum().reset_index()
    QS609_check_tot = QS609_working.groupby(['z'])['Persons'].sum().reset_index()
    QS401_check_tot = QS401_working.groupby(['z'])['Persons'].sum().reset_index()
    seed_df_zonal = seed_df.groupby(['z'])['population'].sum().reset_index()
    Ctrl1_NTEM_zonal = Ctrl1_NTEM.groupby(['z'])['population'].sum().reset_index()
    QS606_check_tot = QS606_check_tot.rename(columns = {'Persons':'QS606_pop'})
    QS609_check_tot = QS609_check_tot.rename(columns = {'Persons':'QS609_pop'})
    QS401_check_tot = QS401_check_tot.rename(columns = {'Persons':'QS401_pop'})
    seed_df_zonal = seed_df_zonal.rename(columns = {'population':'Seed_pop'})
    Ctrl1_NTEM_zonal = Ctrl1_NTEM_zonal.rename(columns = {'population':'Ctrl1_pop'})
    QS_check_totals = pd.merge(QS606_check_tot, QS609_check_tot,
                               left_on = 'z', right_on = 'z', how = 'left')
    QS_check_totals = pd.merge(QS_check_totals, QS401_check_tot,
                               left_on = 'z', right_on = 'z', how = 'left')
    QS_check_totals = pd.merge(QS_check_totals, seed_df_zonal,
                               left_on = 'z', right_on = 'z', how = 'left')
    QS_check_totals = pd.merge(QS_check_totals, Ctrl1_NTEM_zonal,
                               left_on = 'z', right_on = 'z', how = 'left')

    os.chdir(Output_Folder)
    QS_check_totals.to_csv(r'check_seed+control_totals.csv', index=False)
    print('Checks completed and dumped to csv')

    census_and_by_lu_obj.state['3.1.2 expand population segmentation'] = 1
    logging.info('3.1.2 expand population segmentation completed')

def IPFN_Process_2011(census_and_by_lu_obj):
    """
    Reads in the district chunked IPFN seed and control files
    and creates a compressed file output containing final 2011 f by z, a, g, h, e, t, n, s
    !!!!! IMPORTANT NOTE !!!!! - ART, 28/10/2021 - I think this has now been resolved & it only takes 20 mins to write
    This script was NOT how the IPFN process was actually run
    In order for it to complete in a timely manner, 16 Jupyter Notebooks
    containing the script were run in parallel, each handling up to 20
    districts, even then total tun time on the longest running workbook
    was OVER 24 HOURS. Should anyone ever wish to rerun this process, it
    is recommended that the jupyter notebooks are run in parallel again,
    although this script will still produce the outputs. However, it will
    do it one at a time, so COULD TAKE UP TO A FORTNIGHT!
    The jupyter notebooks that were actually used can be found here:
    I:/NorMITs Land Use/import/2011 Census Furness/02 Process
    """
    # Set min and max districts to process
    # Note that these should be in range 1 to 313 inclusive
    # Setting range to 1 -> 313 inclusive will run ipfn on everything
    # In reality, use the Jupyter notebooks in parallel as mentioned above
    min_d = 1
    max_d = 313

    # Set read in/out paths
    ModelName = "NorMITs"
    Output_Folder = r'I:\NorMITs Land Use\import\2011 Census Furness\03 Output'
    seed_path_start = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\01 Seed Files\2011_NorMITs_seed_d'
    ctrl_NTEM_p_path_start = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\02 Ctrl1 NTEM Control Files\2011_NorMITs_Ctrl1_NTEM_d'
    ctrl_dt_p_path_start = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\05 DT Control Files\2011_NorMITs_Ctrl_DT_d'
    ctrl_NSSEC_p_path_start = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\04 NSSEC Control Files\2011_NorMITs_Ctrl_NSSEC_d'
    ctrl_SOC_p_path_start = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\03 SOC Control Files\2011_NorMITs_Ctrl_SOC_d'
    # Review paths for these
    t_ntemtt_code_path = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\06 Lookups\t_ntemtt_code.csv'
    n_t_ntemtt_code_path = r'I:\NorMITs Land Use\import\2011 Census Furness\01 Inputs\06 Lookups\n_t_ntemtt_code.csv'
    t_ntemtt_code = pd.read_csv(t_ntemtt_code_path)
    n_t_ntemtt_code = pd.read_csv(n_t_ntemtt_code_path)

    # Run through the districts specified above in a for loop
    # Note the 3 ipfn process in each iteration of the loop due to the 3 dimensions that need fitting
    for district in range(min_d, max_d):
        dist_str = str(district)
        print('Working on District', dist_str)
        # ----- Format input data ------
        seed_path = '_'.join([seed_path_start, dist_str, 'v0.1.csv'])
        ctrl_NTEM_p_path = '_'.join([ctrl_NTEM_p_path_start, dist_str, 'v0.1.csv'])
        ctrl_dt_p_path = '_'.join([ctrl_dt_p_path_start, dist_str, 'v0.1.csv'])
        ctrl_NSSEC_p_path = '_'.join([ctrl_NSSEC_p_path_start, dist_str, 'v0.1.csv'])
        ctrl_SOC_p_path = '_'.join([ctrl_SOC_p_path_start, dist_str, 'v0.1.csv'])
        seed = pd.read_csv(seed_path)
        ctrl_NTEM_p = pd.read_csv(ctrl_NTEM_p_path)
        ctrl_dt_p = pd.read_csv(ctrl_dt_p_path)
        ctrl_NSSEC_p = pd.read_csv(ctrl_NSSEC_p_path)
        ctrl_SOC_p = pd.read_csv(ctrl_SOC_p_path)
        # convert heading on population to total
        seed.rename(columns={"population": "total"}, inplace=True)
        ctrl_NTEM_p.rename(columns={"population": "total"}, inplace=True)
        ctrl_dt_p.rename(columns={"Persons": "total"}, inplace=True)
        ctrl_NSSEC_p.rename(columns={"Persons": "total"}, inplace=True)
        ctrl_SOC_p.rename(columns={"Persons": "total"}, inplace=True)
        ctrl_NTEM_p = ctrl_NTEM_p.groupby(['z', 'ntem_tt'])['total'].sum()
        ctrl_dt_p = ctrl_dt_p.groupby(['z', 't'])['total'].sum()
        ctrl_NSSEC_p = ctrl_NSSEC_p.groupby(['z', 'n'])['total'].sum()
        ctrl_SOC_p = ctrl_SOC_p.groupby(['z', 's'])['total'].sum()

        # ----- ROUND 1 ------
        print('Round 1')
        aggregates = [ctrl_NTEM_p, ctrl_dt_p]
        dimensions = [['z', 'ntem_tt'], ['z', 't']]
        IPF = ipfn.ipfn(seed, aggregates, dimensions, convergence_rate=0.001)
        seed = IPF.iteration()
        ntem_p = seed.groupby(['z', 'ntem_tt'])['total'].sum()
        dt_p = seed.groupby(['z', 't'])['total'].sum()

        # ----- ROUND 2 ------
        print('Round 2')
        Popby_NTEMsegs_dt = seed.groupby(['z', 't', 'ntem_tt'])['total'].sum()
        ctrl_NTEMsegs_plus_dt = pd.DataFrame(Popby_NTEMsegs_dt)
        ctrl_NTEMsegs_plus_dt['z'] = ctrl_NTEMsegs_plus_dt.index.get_level_values(0)
        ctrl_NTEMsegs_plus_dt['t'] = ctrl_NTEMsegs_plus_dt.index.get_level_values(1)
        ctrl_NTEMsegs_plus_dt['ntem_tt'] = ctrl_NTEMsegs_plus_dt.index.get_level_values(2)
        ctrl_NTEMsegs_plus_dt['t_ntemtt'] = ctrl_NTEMsegs_plus_dt['t'].astype(str) + "_" + ctrl_NTEMsegs_plus_dt[
            'ntem_tt'].astype(str)
        ctrl_NTEMsegs_plus_dt = ctrl_NTEMsegs_plus_dt.merge(t_ntemtt_code, how='left', on='t_ntemtt')
        ctrl_NTEMsegs_plus_dt_cols = ['z', 't_ntemtt_code', 'total']
        ctrl_NTEMsegs_plus_dt = ctrl_NTEMsegs_plus_dt[ctrl_NTEMsegs_plus_dt_cols]
        ctrl_NTEMsegs_plus_dt = ctrl_NTEMsegs_plus_dt.rename(columns={'t_ntemtt_code': 't_ntemtt'})
        ctrl_NTEM_dt_p = ctrl_NTEMsegs_plus_dt.groupby(['z', 't_ntemtt'])['total'].sum()
        seed['t_ntemtt'] = seed['t'].astype(str) + "_" + seed['ntem_tt'].astype(str)
        seed = seed.merge(t_ntemtt_code, how='left', on='t_ntemtt')
        seed_cols = ['z', 't_ntemtt_code', 'n', 's', 'total']
        seed = seed[seed_cols]
        seed = seed.rename(columns={'t_ntemtt_code': 't_ntemtt'})
        aggregates = [ctrl_NTEM_dt_p, ctrl_NSSEC_p]
        dimensions = [['z', 't_ntemtt'], ['z', 'n']]
        IPF = ipfn.ipfn(seed, aggregates, dimensions, convergence_rate=0.001)
        seed = IPF.iteration()
        t_ntem_p = seed.groupby(['z', 't_ntemtt'])['total'].sum()
        nssec_p = seed.groupby(['z', 'n'])['total'].sum()

        # ----- ROUND 3 ------
        print('Round 3')
        Popby_NTEMsegsdt_plus_nssec = seed.groupby(['z', 'n', 't_ntemtt'])['total'].sum()
        ctrl_NTEMsegsdt_plus_nssec = pd.DataFrame(Popby_NTEMsegsdt_plus_nssec)
        ctrl_NTEMsegsdt_plus_nssec['z'] = ctrl_NTEMsegsdt_plus_nssec.index.get_level_values(0)
        ctrl_NTEMsegsdt_plus_nssec['n'] = ctrl_NTEMsegsdt_plus_nssec.index.get_level_values(1)
        ctrl_NTEMsegsdt_plus_nssec['t_ntemtt'] = ctrl_NTEMsegsdt_plus_nssec.index.get_level_values(2)
        ctrl_NTEMsegsdt_plus_nssec['n_t_ntemtt'] = ctrl_NTEMsegsdt_plus_nssec['n'].astype(str) + "_" + \
                                                   ctrl_NTEMsegsdt_plus_nssec['t_ntemtt'].astype(str)
        ctrl_NTEMsegsdt_plus_nssec = ctrl_NTEMsegsdt_plus_nssec.merge(n_t_ntemtt_code, how='left', on='n_t_ntemtt')
        ctrl_NTEMsegsdt_plus_nssec_cols = ['z', 'n_t_ntemtt_code', 'total']
        ctrl_NTEMsegsdt_plus_nssec = ctrl_NTEMsegsdt_plus_nssec[ctrl_NTEMsegsdt_plus_nssec_cols]
        ctrl_NTEMsegsdt_plus_nssec = ctrl_NTEMsegsdt_plus_nssec.rename(columns={'n_t_ntemtt_code': 'n_t_ntemtt'})
        ctrl_NTEM_dt_nssec_p = ctrl_NTEMsegsdt_plus_nssec.groupby(['z', 'n_t_ntemtt'])['total'].sum()
        seed['n_t_ntemtt'] = seed['n'].astype(str) + "_" + seed['t_ntemtt'].astype(str)
        seed = seed.merge(n_t_ntemtt_code, how='left', on='n_t_ntemtt')
        seed_cols = ['z', 'n_t_ntemtt_code', 's', 'total']
        seed = seed[seed_cols]
        seed = seed.rename(columns={'n_t_ntemtt_code': 'n_t_ntemtt'})
        aggregates = [ctrl_NTEM_dt_nssec_p, ctrl_SOC_p]
        dimensions = [['z', 'n_t_ntemtt'], ['z', 's']]
        IPF = ipfn.ipfn(seed, aggregates, dimensions, convergence_rate=0.001)
        seed = IPF.iteration()

        # ----- OUTPUT -----
        print('Writing output files...')
        os.chdir(Output_Folder)
        output_filename = '_'.join([dist_str, 'furnessed_2011Pop.csv'])
        seed.to_csv(output_filename, index=False)

    # Read the IPFN output files back in,
    # processes them into a single df and calculates f for 2011,
    # then saves out a compressed file with the results

    # Set read in/out variables
    n_t_ntemtt_lookup = pd.read_csv(r'Lookups\lookup_n_t_ntemtt_to_aghetns.csv')
    output_directory = 'Outputs'
    model_name = 'NorMITs'
    model_year = '2011'
    input_directory = r'I:\NorMITs Land Use\import\2011 Census Furness\03 Output\\'
    input_filename = r'_furnessed_2011Pop.csv'
    # Edit these parameters to only read in part of the dataset as a test
    start_reading_ipfn_output = 1
    files_to_read_in = 313

    list_of_df = []
    # Loop over all districts and append to a master df.
    for district in range(start_reading_ipfn_output, files_to_read_in + 1):
        dist_str = str(district)
        input_path = [input_directory, dist_str, input_filename]
        input_df = pd.read_csv(''.join(input_path))
        list_of_df.append(input_df)
    print('All', str(files_to_read_in), 'files read in ok')
    furnessed_df = pd.concat(list_of_df, axis=0, ignore_index=True)

    # Process to get f
    # Join the n_t_ntem_tt lookup and tidy
    expanded_furnessed_df = pd.merge(furnessed_df,
                                     n_t_ntemtt_lookup,
                                     how='left',
                                     left_on='n_t_ntemtt',
                                     right_on='n_t_ntemtt_code')
    expanded_furnessed_df = expanded_furnessed_df.drop(columns=['n_t_ntemtt_x'])
    # Create P grouped by zone and a, g, h, e
    # Then reapply this to the main df and tidy
    grouped_P_df = expanded_furnessed_df.groupby(['z', 'a', 'g', 'h', 'e']
                                                 )['total'].sum().reset_index()
    grouped_P_df = grouped_P_df.rename(columns={'total': 'P_zaghe'})
    expanded_furnessed_df = pd.merge(expanded_furnessed_df,
                                     grouped_P_df,
                                     how='left',
                                     on=['z', 'a', 'g', 'h', 'e'])
    expanded_furnessed_df = expanded_furnessed_df.rename(
        columns={'total': 'P_zaghetns', 'n_t_ntemtt_y': 'n_t_ntemtt'})
    # Calculate f
    expanded_furnessed_df['f_tns|zaghe'] = expanded_furnessed_df['P_zaghetns'] / expanded_furnessed_df['P_zaghe']

    # Assign f to empty categories
    # Get these f's from the district averages for unique a, g, h, e, t, n, s combinations
    grouped_P_by_d_df = expanded_furnessed_df.groupby(['d', 'a', 'g', 'h', 'e'])['P_zaghetns'].sum().reset_index()
    grouped_P_by_d_df = grouped_P_by_d_df.rename(columns={'P_zaghetns': 'P_daghe'})
    expanded_furnessed_df = pd.merge(expanded_furnessed_df,
                                     grouped_P_by_d_df,
                                     how='left',
                                     on=['d', 'a', 'g', 'h', 'e'])
    grouped_P_by_d_all_df = expanded_furnessed_df.groupby(['d', 'a', 'g', 'h', 'e', 't', 'n', 's'])[
        'P_zaghetns'].sum().reset_index()
    grouped_P_by_d_all_df = grouped_P_by_d_all_df.rename(columns={'P_zaghetns': 'P_daghetns'})
    expanded_furnessed_df = pd.merge(expanded_furnessed_df,
                                     grouped_P_by_d_all_df,
                                     how='left',
                                     on=['d', 'a', 'g', 'h', 'e', 't', 'n', 's'])
    expanded_furnessed_df['f_tnsdaghe'] = expanded_furnessed_df['P_daghetns'] / expanded_furnessed_df['P_daghe']
    # fill in f nans at the zonal level with district level f's
    expanded_furnessed_df = expanded_furnessed_df.rename(columns={'f_tns|zaghe': 'f_tnszaghe'})
    expanded_furnessed_df.f_tnszaghe.fillna(expanded_furnessed_df.f_tnsdaghe, inplace=True)
    expanded_furnessed_df = expanded_furnessed_df.rename(
        columns={'f_tnszaghe': 'f_tns|zaghe', 'f_tnsdaghe': 'f_tns|daghe'})

    # Do some checks
    print('Shape of expanded furnessed df is', expanded_furnessed_df.shape)
    check_f_by_zaghe = expanded_furnessed_df.groupby(['z'])['f_tns|zaghe'].sum().reset_index()
    print('Max f by z is:', str(check_f_by_zaghe['f_tns|zaghe'].max()))
    print('Min f by z is:', str(check_f_by_zaghe['f_tns|zaghe'].min()))
    check_2011_f_filename = '_'.join([model_name, model_year, 'check_sum_of_f_by_z_with_d_level_f_fills.csv'])
    check_f_by_zaghe.to_csv(check_2011_f_filename, index=False)
    print('Population in output dataframe is', expanded_furnessed_df['P_zaghetns'].sum())

    # Write out files.
    # Use the following two line to control which outputs to write out
    # Set the variables to 0 to stop write out and 1 to keep it.
    write_out_f = 1
    write_out_P = 1
    if write_out_f == 1:
        expanded_furnessed_f_df_out = expanded_furnessed_df[['z', 'a', 'g', 'h', 'e', 't', 'n', 's', 'f_tns|zaghe']]
        output_file_name = '_'.join([model_name, model_year, 'post_ipfn_f_values.csv'])
        output_file_path = os.path.join(output_directory, output_file_name)
        print('Starting to print output file at:', datetime.datetime.now())
        print('Hang tight, this could take a while!')
        expanded_furnessed_f_df_out.to_csv(output_file_path, index=False)
        print('Printing output file complete at:', datetime.datetime.now())
    if write_out_P == 1:
        expanded_furnessed_P_df_out = expanded_furnessed_df[['z', 'a', 'g', 'h', 'e', 't', 'n', 's', 'P_zaghetns']]
        output_file_name = '_'.join([model_name, model_year, 'post_ipfn_P_values.csv'])
        output_file_path = os.path.join(output_directory, output_file_name)
        print('Starting to print output file at:', datetime.datetime.now())
        print('Hang tight, this could take a while!')
        expanded_furnessed_P_df_out.to_csv(output_file_path, index=False)
        print('Printing output file complete at:', datetime.datetime.now())

    census_and_by_lu_obj.state['3.1.3 data synthesis'] = 1
    logging.info('3.1.3 data synthesis completed')
