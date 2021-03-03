
import land_use.future_land_use.fy_lu as fylu

if __name__ == '__main__':
    scenarios = ['NTEM', 'JAM', 'PP', 'DD', 'UZC']
    future_years = ['2033', '2035', '2050']
    for target_scenario in scenarios:
        for target_fy in future_years:
            if target_scenario == 'NTEM':
                adjust_ca = True
            else:
                adjust_ca = False

            fy = fylu.FutureYearLandUse(
                future_year=target_fy,
                scenario_name=target_scenario,
                )
            fy.build_fy_pop(
                adjust_ca=adjust_ca,
                export=True
            )
            """
            fy.build_fy_emp(
                export=True
            )
            """