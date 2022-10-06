

import land_use.future_land_use_DDG.fy_lu as fylu

def main():
    run_base_pop = False
    run_pop = True
    run_emp = True
    iteration = 'iter4m'
    scenarios = ['Regional Scenario', 'High', 'Low']
    # CAS_scen = ['CASReg', 'CASHi', 'CASLo']
    by = '2018'
    all_fy = range(2020, 2051)
    future_years = list()
    for i in all_fy:
        future_years.append(str(i))

    if run_base_pop:
        by_run = fylu.FutureYearLandUse(iteration=iteration, base_year=by)
        by_run.by_pop()

    if run_pop:
        for scenario in scenarios:
            if scenario == 'Regional Scenario':
                CAS_scen = 'CASReg'
            elif scenario == 'High':
                CAS_scen = 'CASHi'
            else:
                CAS_scen = 'CASLo'
            for fy in future_years:
                fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name=scenario, CAS_scen=CAS_scen)

                fy_run.build_fy_pop()
    if run_emp:
        for scenario in scenarios:
            if scenario == 'Regional Scenario':
                CAS_scen = 'CASReg'
            elif scenario == 'High':
                CAS_scen = 'CASHi'
            else:
                CAS_scen = 'CASLo'
            for fy in future_years:
                fy_run = fylu.FutureYearLandUse(iteration=iteration, future_year=fy, scenario_name=scenario, CAS_scen=CAS_scen)

                fy_run.build_fy_emp()





    return 0


if __name__ == '__main__':
    main()