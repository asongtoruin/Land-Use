from land_use.base_land_use import census_lu, by_lu


def main():

    run_census = False
    run_pop = True
    run_emp = False

    iteration = 'iter4l'
    census_year = '2011'
    base_year = '2019'

    print('Building lu run, %s' % iteration)
    print('Census year is %s' % census_year)
    print('Base year is %s' % base_year)

    if run_census:
        census_run = census_lu.CensusYearLandUse(iteration=iteration)
        census_run.build_by_pop()

    lu_run = by_lu.BaseYearLandUse(iteration=iteration, base_year=base_year)

    if run_pop:
        lu_run.build_by_pop()

    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
