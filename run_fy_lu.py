
import land_use.future_land_use.fy_lu as fylu

# Full scenarios
# scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
# Full future years
# future_years = ['2033', '2035', '2040', '2050']

if __name__ == '__main__':
    scenarios = ['NTEM', 'SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    future_years = ['2033', '2035', '2050']
    pop = True
    emp = False

    for scenario in scenarios:
        for fy in future_years:

            fy_model = fylu.FutureYearLandUse(
                future_year=fy,
                scenario_name=scenario,
                sub_for_defaults=True)  # Naughty - should be explicit

            # Define run preferences
            if scenario == 'NTEM':
                balance_demographics = True
                adjust_area_type = False
            else:
                balance_demographics = False
                adjust_area_type = True
            ca_growth_method = 'factor'
            adjust_soc = False

            if pop:
                fy_model.build_fy_pop(
                    balance_demographics=balance_demographics,
                    adjust_ca=True,
                    ca_growth_method='factor',
                    adjust_soc=adjust_soc,
                    adjust_area_type=adjust_area_type,
                    reports=True,
                    export=False
                    )
            if emp:
                fy_model.build_fy_emp(
                    export=False
                    )