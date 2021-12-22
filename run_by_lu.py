from land_use.base_land_use import census_lu, by_lu


def main():

    run_census = False
    run_pop = True
    run_emp = False

    print('Building lu run...')

    if run_census:
        census_run = census_lu.CensusYearLandUse(iteration='iter4i')
        census_run.build_by_pop()

    if run_pop:
        lu_run = by_lu.BaseYearLandUse(iteration='iter4i')
        lu_run.build_by_pop()

    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
