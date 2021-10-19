
import land_use.base_land_use.by_lu as by_lu


def main():

    run_pop = True
    run_emp = True

    lu_run = by_lu.BaseYearLandUse(iteration='iter3d')
    if run_pop:
        lu_run.build_by_pop()
    if run_emp:
        lu_run.build_by_emp()

    return 0


if __name__ == '__main__':
    main()
