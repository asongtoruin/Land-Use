
import land_use.base_land_use.by_lu as bylu

if __name__ == '__main__':

    lu_run = bylu.BaseYearLandUse(iteration='iter4a')
    lu_run.state['5.2.2 read in core property data'] = 1
    lu_run.state['5.2.4 filled property adjustment'] = 1
    lu_run.state['5.2.5 household occupancy adjustment'] = 1
    lu_run.state['5.2.6 NTEM segmentation'] = 1
    lu_run.state['5.2.7 communal establishments'] = 1
    lu_run.state['5.2.3 property type mapping'] = 1
    lu_run.build_by_pop()
