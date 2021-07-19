
import land_use.base_land_use.by_lu as bylu

if __name__ == '__main__':

    lu_run = bylu.BaseYearLandUse(iteration='iter3d')

    pop_out = lu_run.build_by_pop()
