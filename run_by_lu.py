import land_use.base_land_use.census_and_by_lu as bylu
import land_use.base_land_use.by_lu as by_lu


def main():

    run_pop = True
    run_emp = True

    # lu_run = bylu.CensusYearLandUse(iteration='iter4f')
    lu_run = bylu.BaseYearLandUse(iteration='iter4f')
    # lu_run.state['3.2.1 read in core property data'] = 1
    # lu_run.state['3.2.2 filled property adjustment'] = 1
    # lu_run.state['3.2.3 household occupancy adjustment'] = 1
    # lu_run.state['3.2.4 property type mapping'] = 1
    # lu_run.state['3.2.5 uplifting 2018 population according to 2018 MYPE'] = 1
    # lu_run.state['3.2.6 and 3.2.7 expand NTEM population to full dimensions and verify pop profile'] = 1
    # lu_run.state['3.2.8 get subsets of worker and non-worker'] = 1
    # lu_run.state['3.2.9 verify district level worker and non-worker'] = 1
    # lu_run.state['3.2.10 adjust zonal pop with full dimensions'] = 1

    print('Building lu run...')
    if run_pop:
        lu_run.build_by_pop()
    if run_emp:
        lu_run.build_by_emp()

    return 0

if __name__ == '__main__':
    main()
