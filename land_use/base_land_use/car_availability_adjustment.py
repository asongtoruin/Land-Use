# -*- coding: utf-8 -*-
"""
@author: mags15
"""
import pandas as pd

# TODO: Deprecated - remove

def nts_import(by_lu_obj):
    """
    Imports car availability from nts extract prepared by Humza.
    Survey year 2018 and 2019
    Weights used W1*W3
    """
    # Construct the file paths
    _nts_path = by_lu_obj.import_folder + 'Car availability/Car Availability Build.csv'
    _adults_lookup = by_lu_obj.import_folder + 'Car availability/adults_lookup.csv'
    _hc_lookup = by_lu_obj.import_folder + 'Car availability/household_composition.csv'
    _emp_lookup = by_lu_obj.import_folder + 'Car availability/emp_type.csv'
    _ward_to_msoa = by_lu_obj.zones_folder + 'Export/uk_ward_msoa_pop_weighted_lookup.csv'

    # First import the NTS extract
    nts_cols = ['HHoldOSWard_B01ID', 'HHoldNumAdults', 'NumCarVan_B02ID', 'EcoStat_B01ID', 'weighted_count']
    nts_extract = pd.read_csv(_nts_path)[nts_cols].rename(columns={'HHoldOSWard_B01ID': 'uk_ward_zone_id'})

    # Merge on the number of adults
    adults = pd.read_csv(_adults_lookup)
    nts_extract = nts_extract.merge(adults, on='HHoldNumAdults')

    # Use the number of adults and cars to obtain the household composition
    hc_lookup = pd.read_csv(_hc_lookup)
    hc_lookup = hc_lookup.rename(columns={'household': 'adults', 'cars': 'NumCarVan_B02ID'})
    nts_extract = nts_extract.merge(hc_lookup, on=['adults', 'NumCarVan_B02ID'])
    
    # Change employment type
    emp_lookup = pd.read_csv(_emp_lookup)
    nts_extract = nts_extract.merge(emp_lookup, on='EcoStat_B01ID')

    # Calculate the total number of cars for each ward, hh comp and employment type
    cars = nts_extract.groupby(by=['uk_ward_zone_id', 'household_composition', 'employment_type'], as_index=False).sum()
    cars = cars.rename(columns={'weighted_count': 'people'})

    # Convert from wards tp MSOAs
    ward_to_msoa = pd.read_csv(_ward_to_msoa).drop(columns={'overlap_var',
                                                            'uk_ward_var',
                                                            'msoa_var',
                                                            'overlap_type',
                                                            'overlap_msoa_split_factor'})
    cars_msoa = cars.merge(ward_to_msoa, on='uk_ward_zone_id')
    cars_msoa['population'] = cars_msoa['people'] * cars_msoa['overlap_uk_ward_split_factor']
    cars_msoa = cars_msoa.groupby(by=['msoa_zone_id', 'household_composition', 'employment_type'],
                                  as_index=False).sum().drop(columns={'overlap_uk_ward_split_factor', 'people'})
    
    # Now read in and merge the area types
    area_types = pd.read_csv(by_lu_obj.area_type_path).rename(columns={'msoa_area_code': 'msoa_zone_id',
                                                                       'tfn_area_type_id': 'area_type'})
    cars_msoa = cars_msoa.merge(area_types, on='msoa_zone_id')
    
    # Aggregate by area type (MSOA had too small sample sizes, with many zeroes)
    cars_at = cars_msoa.groupby(by=['area_type', 'household_composition', 'employment_type'], as_index=False).sum()
    cars_at['totals'] = cars_at.groupby(['area_type', 'employment_type'])['population'].transform('sum')
    cars_at['splits'] = cars_at['population'] / cars_at['totals']

    # check the number of survey results per area type
    cars_n = cars_at.groupby(by=['area_type'], as_index=False).sum()[['area_type', 'population']]
    cars_at = cars_at[['area_type', 'household_composition', 'employment_type', 'splits']]
    cars_at.to_csv(by_lu_obj.home_folder+'/nts_splits.csv', index=False)

    return cars_at
