from land_use.base_land_use import census_lu, by_lu


def main():

    run_census = False
    run_pop = False
    run_pop_DDG = True
    run_emp = False

    iteration = 'iter4q'
    census_year = '2011'
    base_year = '2018'

    print('Building lu run, %s' % iteration)
    print('Census year is %s' % census_year)
    print('Base year is %s' % base_year)

    if run_census:
        census_run = census_lu.CensusYearLandUse(iteration=iteration)
        census_run.state['3.1.1 derive 2011 population from NTEM and convert Scottish zones'] = 1
        census_run.state['3.1.2 expand population segmentation'] = 0
        census_run.state['3.1.3 data synthesis'] = 0


    # may need further effort to write out files from prev steps which to be used by later step
    # currently because each step is calling the dataframe from previous step using internal memory
    # need to run 3.2.1 to 3.2.10 firstly (1-10 set to be zero, 12 to be 1)
    # then to run 3.2.12 (1-10 set to be 1, 12 set to be zero)


    lu_run = by_lu.BaseYearLandUse(iteration=iteration, base_year=base_year)

    lu_run.state['3.2.1 Read in core property data'] = 1
    lu_run.state['3.2.2 Filled property adjustment'] = 1
    lu_run.state['3.2.3 Household occupancy adjustment'] = 1
    lu_run.state['3.2.4 Property type mapping'] = 1
    lu_run.state['3.2.5 Adjust Base Year population according to Base Year MYPE'] = 1
    lu_run.state['3.2.6 Expand NTEM population to full dimensions'] = 1
    lu_run.state['3.2.7 Furness household population according to control values'] = 1
    lu_run.state['3.2.8 Combine HHR with CER to form total population'] = 0


    if run_pop:
        lu_run.build_by_pop()

    if run_pop_DDG:
        lu_run.build_by_pop_DDG()

    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
