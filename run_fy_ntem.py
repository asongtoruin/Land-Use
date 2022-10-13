

import land_use.future_land_use_DDG.fy_lu as fylu
import pandas as pd
import os
def main():

    run_ntem = False
    join_ntem = False
    extrapolate = True

    iteration = 'iter4m'
    by = '2018'
    ntem_path = r'I:\NorMITs Land Use\import\CTripEnd\All_year'
    ntem_2018_2051_file_name = r'I:\NorMITs Land Use\import\CTripEnd\All_year\ntem_gb_z_ntem_tt_18_51_pop.csv.bz2'
    # ntem_2018_2070_file_name = r'I:\NorMITs Land Use\import\CTripEnd\All_year\ntem_gb_z_ntem_tt_18_70_pop.csv.bz2'
    ntem_2018_2070_file_name = r'I:\NorMITs Land Use\import\CTripEnd\All_year\ntem_gb_z_ntem_tt_allyear_pop.csv.bz2'
    ## scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    # scenarios = ['Regional Scenario', 'High', 'Low']
    # CAS_scen = ['CASReg', 'CASLo','CASHi']
    all_ntem_fy = range(2019, 2052)
    # all_fy = range(2019, 2071)
    ntem_fy_extra = range(2052, 2071)
    ntem_future_years = list()
    for i in all_ntem_fy:
        ntem_future_years.append(str(i))


    if run_ntem:
        for ntem_fy in ntem_future_years:
            fy_ntem_run = fylu.FutureYearLandUse(iteration=iteration, future_year=ntem_fy)
            fy_ntem_run.NTEM_pop()
    if join_ntem:
        call_base = fylu.FutureYearLandUse(iteration=iteration, base_year=by)
        call_base.clean_base_ntem_pop()
        by_ntem = pd.read_csv(os.path.join(ntem_path, ''.join(['ntem_gb_z_areatype_ntem_tt_', by, '_pop.csv'])))
        base = by_ntem.copy()
        base = base.set_index(['z', 'tt'])
        for ntem_fy in ntem_future_years:
            fy_ntem = pd.read_csv(os.path.join(ntem_path,''.join(['ntem_gb_z_areatype_ntem_tt_', ntem_fy, '_pop.csv'])))
            fy_ntem = fy_ntem.set_index(['z', 'tt'])
            # by_ntem = get_ntem[0]
            # fy_ntem = get_ntem[1]
            base = pd.concat([base, fy_ntem], axis=1)
            print(base)
        base.to_csv(ntem_2018_2051_file_name)

    if extrapolate:
        ntem_2018_2051 = pd.read_csv(ntem_2018_2051_file_name)
        lower, upper = ntem_future_years[-2:]
        start_year = int(upper)
        annual_growth = (ntem_2018_2051[upper] / ntem_2018_2051[lower])
        annual_growth = annual_growth.fillna(0)
        ntem_all = ntem_2018_2051.copy()
        for target_year in ntem_fy_extra:
            year_diff = target_year - start_year
            ntem_all[str(target_year)] = ntem_all[str(start_year)] * (annual_growth ** year_diff)
            ntem_all[str(target_year)] = ntem_all[str(target_year)].fillna(0)
            print(ntem_all)
        ntem_all.to_csv(ntem_2018_2070_file_name)
    return 0


if __name__ == '__main__':
    main()