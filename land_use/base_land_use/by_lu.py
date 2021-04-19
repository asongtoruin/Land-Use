import os

import pandas as pd

import land_use.lu_constants as consts
from land_use import utils as fyu

###
1. Run base_land_use/Land Use data prep.py - this prepares the AddressBase extract and classifies the properties to prep the property data
2. Run base_land_use/main_build_hh_and_ persons_census_segmentation.py - this prepares evertything to do with Census and joins to property data
3. Run mid_year_ pop_adjustments.py - this does the uplift to 2018

###
class BaseYearLandUse:
    def __init__():
