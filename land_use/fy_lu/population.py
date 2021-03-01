# -*- coding: utf-8 -*-
"""
Created on: Mon March 1 09:43:32 2020
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:
WRITE PURPOSE
"""
import os

import pandas as pd

from fy_lu import utils

REPO_HOME = r"C:\Users\Sneezy\Desktop\GitHub\Land-Use"

POP_GROWTH_PATH = os.path.join(REPO_HOME, r"land_use\fy_lu\growth\population\future_population_growth.csv")


def grow_pop(base_year='2018',
             future_years=['2033', '2035', '2050'],
             segmentation_cols=None,
             population_infill=0.001,
             ):
    # Setup
    all_years = [str(x) for x in [base_year] + future_years]

    if segmentation_cols is None:
        segmentation_cols = [
            'area_type',
            'traveller_type',
            'soc',
            'ns',
            'ca'
        ]

    population_growth = pd.read_csv(POP_GROWTH_PATH)

    # ## BASE YEAR POPULATION ## #
    print("Loading the base year population data...")
    base_year_pop = get_land_use_data(imports['land_use'],
                                      segmentation_cols=segmentation_cols)
    base_year_pop = base_year_pop.rename(columns={'people': base_year})

    # Audit population numbers
    print("Base Year Population: %d" % base_year_pop[base_year].sum())

    # ## FUTURE YEAR POPULATION ## #
    print("Generating future year population data...")
    # Merge on all possible segmentations - not years
    merge_cols = utils.intersection(list(base_year_pop), list(population_growth))
    merge_cols = utils.list_safe_remove(merge_cols, all_years)

    population = utils.grow_to_future_years(
        base_year_df=base_year_pop,
        growth_df=population_growth,
        base_year=base_year,
        future_years=future_years,
        no_neg_growth=no_neg_growth,
        infill=population_infill,
        growth_merge_cols=merge_cols
    )

    # ## TIDY UP, WRITE TO DISK ## #
    # Reindex and sum
    group_cols = [zone_col] + segmentation_cols
    index_cols = group_cols.copy() + all_years
    population = population.reindex(index_cols, axis='columns')
    population = population.groupby(group_cols).sum().reset_index()

    # Population Audit
    if audits:
        print('\n', '-' * 15, 'Population Audit', '-' * 15)
        for year in all_years:
            print('. Total population for year %s is: %.4f'
                  % (year, population[year].sum()))
        print('\n')

    # Write the produced population to file
    print("Writing population to file...")
    population_output = os.path.join(out_path, self.pop_fname)
    population.to_csv(population_output, index=False)