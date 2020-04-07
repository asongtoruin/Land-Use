# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 12:09:28 2020

@author: genie
"""
import warnings

import pandas as pd

import utils as nup


def merge_car_availability(productions,
                           car_availability):
    """
    Join car availability from car availability model onto productions.

    Parameters
    ----------
    productions:
        a DataFrame with a single zone and category variables.
        Designed for a production vector.
    
    car_availability:
        Car availability model dervived from target segmenatation.
        Has scope to become a lot more complex.

    Returns
    ----------
    productions:
        Productions with car availability appended.
    """
    productions = productions.merge(car_availability,
                                    how='left',
                                    on=['household',
                                        'cars'])
    return(productions)

def get_target_segmentation(target_segmentation_path):
    """
    Import desired segmentation descriptions, to be joined onto land use
    characteristics.

    Parameters
    ----------
    target_segmentation_path:
        Path to ntem

    Returns
    ----------
    traveller_types:
        df of ntem traveller types
    """
    traveller_types = pd.read_csv(target_segmentation_path)

    for col in list(traveller_types):
        traveller_types = traveller_types.rename(columns={col:col.lower()})

    return(traveller_types)

def build_car_availability_model(row):
    """
    Function to get car availability from 'cars' column of target segmentation.

    Parameters
    ----------
    row:
        Row to be applied to or iterated over.

    Returns
    ----------
    1 or 2:
        Integer defining car availability.
    """
    if row['cars'] == '0':
        return(1)
    else:
        return(2)

def get_car_availability(ntem_segmentation):
    """
    Derive car availability from NTEM traveller types. Applies the
    car_availability_model function over a given segmentation.

    Parameters
    ----------
    ntem_segmentation:
        df of ntem traveller types

    Returns
    ----------
    [0] car_availability_index:
        Dictionary describing car availaiblity model. For index outputs.
    [1] car_availability_model:
    """
    # TODO: Make sure the index output is being saved
    # TODO: Figue out what this is doing and rewrite docs!
    car_availability_index = {'1':'No car available',
                              '2':'Car available'}

    car_availability_model = ntem_segmentation[['household',
                                                'cars']].drop_duplicates(
                                                ).reset_index(
                                                        drop=True)

    car_availability_model[
            'car_availability'] = car_availability_model.apply(
            build_car_availability_model,axis=1)
    return(car_availability_index,
           car_availability_model)

def get_land_use_output(land_use_output_path,
                        handle_gaps=True):
    """
    Read in, and take a sample from, output from Land Use, given an import path
    Parameters
    ----------
    land_use_output_path:
        Path to land use output.

    handle_gaps = True:
        Filter out any gaps in the land use that may cause issues in the
        productions model.

    do_format = True:
        Optimise Land Use data on import.

    Returns
    ----------
    land_use_output:
        A DataFrame containing output from the NorMITs Land use model in a
        given zoning system. Should be MSOA or LSOA.
    """
    with warnings.catch_warnings():
            warnings.simplefilter(action='ignore',
                                  category=FutureWarning)
            land_use_output = pd.read_csv(land_use_output_path,
                                          index_col=0)
    # Get a future warning from running previous line - details found here:
    for col in list(land_use_output):
        print(col)
        print(land_use_output.loc[:,col].drop_duplicates())

    return(land_use_output)

def merge_traveller_types(land_use_output,
                          target_segmentation):
    # TODO: Check what car segments are doing to productions.
    """
    Function to bring in category variables for desired segmentation using
    the parts of the target segmentation which already exist in land use.
    Currently joins on age, gender, household_composition & employment type.
    Provides traveller type.

    Parameters
    ----------
    land_use_output:
        A DataFrame containing output from the NorMITs Land use model in a
        given zoning system. Should be MSOA or LSOA.

    target_segmentation:
        Desired segmentation for productions. Contains NTEM traveller_type
        variable which is crucial for aggregation.

    Returns
    ----------
    land_use_output:
        Land use output DataFrame with target segmentation appended onto
        land use categories.
    """
    # TODO: temporary fix for case sensitivity
    # Replace with fixing land use output
    for col in list(land_use_output):
        if col != 'ZoneID':
            land_use_output = land_use_output.rename(columns={col:col.lower()})

    land_use_output = land_use_output.sort_values(['age',
                                                   'gender',
                                                   'household_composition',
                                                   'employment_type']).reset_index(drop=True)

    # People before
    print(land_use_output['people'].sum())

    land_use_output = land_use_output.merge(target_segmentation,
                                            how='left',
                                            on=['age',
                                                'gender',
                                                'household_composition',
                                                'employment_type'])
    
    
    return(land_use_output)

home_dir = 'Y:/NorMITs Synthesiser/Norms'
iteration = 'iter4'
# for example
lu_path = 'Y:/NorMITs Land Use/iter3/NPRSegments2.csv'

output_dir = (home_dir + '/' + iteration + '/')
output_f = 'Production Outputs'
lookup_f = (output_f + '/Production Lookups')
run_log_f =(output_f + '/Production Run Logs')
nup.create_folder((output_dir + output_f), chDir=False)
nup.create_folder((output_dir + lookup_f), chDir=False)
nup.create_folder((output_dir + run_log_f), chDir=False)

# TODO: Pass to Land use!
# TODO: Fix to handle flexibitlity
# Get ntem traveller types

target_segmentation_path = ('Y:/NorMITs Synthesiser/import/ntem_traveller_types.csv')
target_segmentation = get_target_segmentation(target_segmentation_path)

# Build car availability model - exports to lookups
car_availability = get_car_availability(target_segmentation)
pd.DataFrame.from_dict(car_availability[0],orient='index'
                           ).to_csv(output_dir + lookup_f +
                           '/car_availability.csv')

# Get and optimise land use data
land_use_output = get_land_use_output(lu_path)
    
lu_cols = list(land_use_output)
print(lu_cols)

land_use_output = merge_traveller_types(land_use_output, target_segmentation)
lu_cols = list(land_use_output)
print(lu_cols)

#
land_use_output = land_use_output.reindex(['ZoneID', 'area_type', 'property_type', 'age', 'gender', 'household', 'cars',
                                           'household_composition', 'employment_type', 'traveller_type',
                                           'soc_category', 'ns_sec', 'people'],axis=1).reset_index(drop=True)
land_use_output = land_use_output.rename(columns={'ZoneID':'msoa_zone_id',
                                                  'Age':'age',
                                                  'Gender':'gender',
                                                  'soc_category':'soc_cat'})

test = land_use_output[0:2000]

land_use_output['soc_cat'] = land_use_output['soc_cat'].fillna(0)

# Get some intelligence on the land use import
# TODO:: Functionalise

land_use_output = merge_car_availability(land_use_output, car_availability[1])



land_use_output['people'].sum()

# Fixed lu output
land_use_output = land_use_output.reindex(['msoa_zone_id', 'area_type', 'property_type',
                                               'age', 'gender', 'household', 'cars',
                                               'household_composition', 'employment_type',
                                               'traveller_type', 'soc_cat', 'ns_sec', 'car_availability',
                                               'people'], axis=1)

# land_use_output.to_csv('C:/NorMITs_Export/land_use_output_msoa.csv', index=False)
land_use_output.to_csv('Y:/NorMITs Land Use/iter3/land_use_output_msoa.csv', index=False)