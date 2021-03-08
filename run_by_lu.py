import land_use.base_land_use.by_lu as bylu

if __name__ == '__main__':

    by = bylu.BaseYearLandUse()

    """
    by.build_by_pop()
    """

    by.build_by_emp(
        export=True
        )