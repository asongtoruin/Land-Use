
import land_use.future_land_use.fy_lu as fylu

if __name__ == '__main__':
    future_years = ['2033', '2035', '2050']
    for target_fy in future_years:
        fy = fylu.FutureYearLandUse(
            future_year=target_fy
        )
        fy.build_fy_pop(
            export=True
        )
        fy.build_fy_emp(
            export=True
        )
