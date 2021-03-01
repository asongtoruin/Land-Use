# -*- coding: utf-8 -*-
"""

@author: genie
"""


def population_growth(base_year='2018',
                      future_years=['2033', '2035', '2050'],
                      segmentation_cols=None,
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
    merge_cols = du.intersection(list(base_year_pop), list(population_growth))
    merge_cols = du.list_safe_remove(merge_cols, all_years)

    population = du.grow_to_future_years(
        base_year_df=base_year_pop,
        growth_df=population_growth,
        base_year=base_year,
        future_years=future_years,
        no_neg_growth=no_neg_growth,
        infill=population_infill,
        growth_merge_cols=merge_cols
    )
