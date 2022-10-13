

import land_use.future_land_use_DDG.fy_lu as fylu

def main():
    run_base_pop = False
    run_fy_ntem = False
    run_pop = True
    run_emp = False
    iteration = 'iter4m'
    scenarios = ['CAS Regional Scenario', 'CAS High', 'CAS Low', 'Nov 21 central']
    # CAS_scen = ['CASReg', 'CASHi', 'CASLo', 'Central']
    by = '2018'
    all_fy = range(2019, 2071)
    future_years = list()
    for i in all_fy:
        future_years.append(str(i))

    if run_base_pop:
        by_run = fylu.FutureYearLandUse(iteration=iteration, base_year=by)
        by_run.by_pop()

    if run_fy_ntem:
        for fy in future_years:
            ntem_run = fylu.FutureYearLandUse(iteration=iteration, base_year=by, future_year=fy)
            ntem_run.build_fy_pop_ntem()

    if run_pop:
        for scenario in scenarios:
            if scenario == 'CAS Regional Scenario':
                CAS_scen = 'CASReg'
            elif scenario == 'CAS High':
                CAS_scen = 'CASHi'
            elif scenario == 'CAS Low':
                CAS_scen = 'CASLo'
            else:
                CAS_scen = 'Central'
            for fy in future_years:
                fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name=scenario, CAS_scen=CAS_scen)
                fy_run.build_fy_pop_DDG()

        # for fy in future_years:
        #      # fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name='CAS Low', CAS_scen='CASLo')
        #      fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name='Nov 21 central', CAS_scen='Central')
        #      fy_run.build_fy_pop_DDG()

    if run_emp:
        for scenario in scenarios:
            if scenario == 'CAS Regional Scenario':
                CAS_scen = 'CASReg'
            elif scenario == 'CAS High':
                CAS_scen = 'CASHi'
            elif scenario == 'CAS Low':
                CAS_scen = 'CASLo'
            else:
                CAS_scen = 'Central'
            for fy in future_years:
                fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name=scenario, CAS_scen=CAS_scen)

                fy_run.build_fy_emp()





    return 0


if __name__ == '__main__':
    main()