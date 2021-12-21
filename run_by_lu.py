from land_use.base_land_use import census_lu, by_lu


def main(run_pop: bool = True,
         run_emp: bool = False,
         run_2011: bool = False,
         run_base_year: bool = True
         ):
    """
    Run function to be called below.
    :param run_pop: Run a base year or census model for population
    :param run_emp: Run a base year or census model for employment
    :param run_2011: Run a census model, or not
    :param run_base_year: Run a light rebase with MYPE, or not
    :return 0:
    """

    # TODO: I think this is where this is going, toggles for runs
    # Toggle Census run or soft rebase
    # TODO: Should be done by year

    print('Building lu run...')
    if run_pop:
        if run_2011:
            lu_run = census_lu.CensusYearLandUse(iteration='iter4h')
            lu_run.build_by_pop()
        if run_base_year:
            lu_run = by_lu.BaseYearLandUse(iteration='iter4h')
            lu_run.build_by_pop()

    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
