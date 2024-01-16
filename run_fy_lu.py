
import land_use.future_land_use.fy_lu as fylu
import land_use.lu_constants as consts

# Full scenarios
# scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
# Full future years
# future_years = ['2027', '2033', '2035', '2040', '2050']

if __name__ == '__main__':

    fy_iter = 'iter4m_20240109_validation'

    by_resi_lu_path = r'I:\NorMITs Land Use\base_land_use\iter3e\outputs\land_use_output_msoa.csv'
    by_non_resi_lu_path = r'I:\NorMITs Land Use\base_land_use\iter3e\outputs\land_use_2018_emp.csv'

    scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    # scenarios = ['Regional Scenario', 'High', 'Low']
    CAS_scen = ['CASReg', 'CASLo','CASHi']
    all_fy = range(2019, 2051)
    future_years = list()
    for i in all_fy:
        future_years.append(str(i))
    # future_years = ['2019', '2033', '2035', '2040', '2050']


    pop = True
    emp = True
    export = True
    balance_demographics = True

    for scenario in scenarios:
        for fy in future_years:

            print(scenario, fy)

            fym = fylu.FutureYearLandUse(
                future_year=fy,
                scenario_name=scenario,
                iteration=fy_iter,
                base_resi_land_use_path=consts.RESI_LAND_USE_MSOA,
                base_non_resi_land_use_path=consts.NON_RESI_LAND_USE_MSOA,
                sub_for_defaults=False)

            print(fym.in_paths)

            # Define run preferences
            if scenario == 'NTEM':
                adjust_area_type = False
            else:
                adjust_area_type = True
            ca_growth_method = 'factor'
            adjust_soc = False

            if pop:
                fym.build_fy_pop(
                    balance_demographics=balance_demographics,
                    adjust_ca=True,
                    ca_growth_method=ca_growth_method,
                    adjust_soc=adjust_soc,
                    adjust_area_type=adjust_area_type,
                    reports=True,
                    normalise=True,
                    export=export
                    )
            if emp:
                fym.build_fy_emp(
                    export=export
                    )
