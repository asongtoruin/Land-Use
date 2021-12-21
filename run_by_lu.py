import land_use.base_land_use.census_lu as bylu
import land_use.base_land_use.by_lu as by_lu


def main():

    run_pop = True
    run_emp = False

    # lu_run = bylu.CensusYearLandUse(iteration='iter4h')
    lu_run = bylu.BaseYearLandUse(iteration='iter4h')

    print('Building lu run...')
    if run_pop:
        lu_run.build_by_pop()
    if run_emp:
        # TODO - Fix this call. It is currently trying to find build_by_emp in census_lu.py
        #  It is actually in by_lu.py
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
