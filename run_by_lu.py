from land_use.base_land_use import census_lu, by_lu


def main():

    run_census = False
    run_pop = True
    run_emp = True

    iteration = 'iter4n'
    census_year = '2011'
    base_year = '2018'

    print('Building lu run, %s' % iteration)
    print('Census year is %s' % census_year)
    print('Base year is %s' % base_year)

    if run_census:
        census_run = census_lu.CensusYearLandUse(iteration=iteration)
        census_run.build_by_pop()

    lu_run = by_lu.BaseYearLandUse(iteration=iteration, base_year=base_year)
    # lu_run.state['3.2.1 read in core property data'] == 1
    # lu_run.state['3.2.2 filled property adjustment'] == 1
    # lu_run.state['3.2.3 household occupancy adjustment'] == 1
    # lu_run.state['3.2.4 property type mapping'] == 1
    # lu_run.state['3.2.5 Uplifting Base Year population according to Base Year MYPE'] == 1
    # lu_run.state['3.2.6 and 3.2.7 expand NTEM population to full dimensions and verify pop profile'] == 1
    # lu_run.state['3.2.8 get subsets of worker and non-worker'] == 1
    # lu_run.state['3.2.9 verify district level worker and non-worker'] == 1
    # lu_run.state['3.2.10 adjust zonal pop with full dimensions'] == 1
    # lu_run.state['3.2.12 process DDG data'] == 0


    if run_pop:
        lu_run.build_by_pop()

    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
