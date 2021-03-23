
import land_use.future_land_use.fy_lu as fylu

# Full scenarios
# scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
# Full future years
# future_years = ['2033', '2035', '2050']

if __name__ == '__main__':
    scenarios = ['NTEM']
    future_years = ['2033', '2035', '2050']
    pop = True
    emp = False

    for target_scenario in scenarios:
        for target_fy in future_years:

            fy = fylu.FutureYearLandUse(
                future_year=target_fy,
                scenario_name=target_scenario,
                )

            # Define run preferences
            if target_scenario == 'NTEM':
                adjust_area_type = False
            else:
                adjust_area_type = True
            ca_growth_method = 'factor'
            adjust_soc = False

            if pop:
                fy.build_fy_pop(
                    adjust_ca=True,
                    ca_growth_method='factor',
                    adjust_soc=adjust_soc,
                    adjust_area_type=adjust_area_type,
                    export=True
                    )
            if emp:
                fy.build_fy_emp(
                    export=True
                    )