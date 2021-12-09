
import land_use.future_land_use.fy_lu as fylu
import land_use.lu_constants as consts

# Full scenarios
# scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
# Full future years
# future_years = ['2027', '2033', '2035', '2040', '2050']

if __name__ == '__main__':

    fy_iter = 'iter3d'

    base_land_use = 'I:/NorMITs Land Use/base_land_use/iter3d/outputs/land_use_output_msoa.csv'

    scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    future_years = ['2027', '2033', '2035', '2040', '2050', '2055']
    future_years = list(range(2051, 2056))
    future_years = [str(x) for x in future_years]

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
                base_land_use_path=base_land_use,
                base_employment_path=consts.EMPLOYMENT_MSOA,
                base_soc_mix_path=consts.SOC_2DIGIT_SIC,
                sub_for_defaults=False)  # Naughty - should be explicit

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
