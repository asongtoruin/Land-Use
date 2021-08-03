
import land_use.future_land_use.fy_lu as fylu

# Full scenarios
# scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
# Full future years
# future_years = ['2027', '2033', '2035', '2040', '2050']

if __name__ == '__main__':

    bylu_path = r'I:\NorMITs Land Use\base_land_use\iter3d\outputs\land_use_output_msoa.csv'

    fy_iter = 'iter3d'

    scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    all_fy = range(2019, 2051)
    future_years = list()
    for i in all_fy:
        future_years.append(str(i))
    # future_years = ['2019', '2033', '2035', '2040', '2050']

    pop = False
    emp = True
    export = True
    balance_demographics = True

    for scenario in scenarios:
        for fy in future_years:

            fym = fylu.FutureYearLandUse(
                future_year=fy,
                base_land_use_path=bylu_path,
                scenario_name=scenario,
                iteration=fy_iter,
                sub_for_defaults=True)  # Naughty - should be explicit

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
