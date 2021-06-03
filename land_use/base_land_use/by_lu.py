import os
import pandas as pd
import land_use.lu_constants as consts
from land_use.utils import file_ops as utils
from land_use.base_land_use import main_build, car_availability_adjustment
# from land_use.base_land_use import mid_year_pop_adjustments as mypa

"""
1. Run base_land_use/Land Use data prep.py - this prepares the AddressBase extract and classifies the properties to prep the property data
2. Run base_land_use/main_build_hh_and_persons_census_segmentation.py - this prepares evertything to do with Census and joins to property data
3. Run mid_year_ pop_adjustments.py - this does the uplift to 2018

TODO:
1. Inherit from a pathing object?
2. See normits_demand.utils.compression for ways to speed up I/O
"""


class BaseYearLandUse:
    def __init__(self,
                 model_folder=consts.LU_FOLDER,
                 iteration=consts.LU_MR_ITER,
                 import_folder=consts.LU_IMPORTS,
                 model_zoning='MSOA',
                 zones_folder=consts.ZONES_FOLDER,
                 zone_translation_path=consts.ZONE_TRANSLATION_PATH,
                 KS401path=consts.KS401_PATH,
                 area_type_path=consts.LU_AREA_TYPES,
                 # base_land_use_path=None,
                 # base_employment_path=None,
                 # base_soc_mix_path=None,
                 base_year='2018',
                 scenario_name=None,
                 # pop_segmentation_cols=None,
                 sub_for_defaults=False):
        """
        parse parameters from run call (file paths, requested outputs and audits)
        area types: NTEM / TfN
        output zoning system
        versioning
        state property
        """

        # TODO: Add versioning

        # File ops
        self.model_folder = model_folder
        self.iteration = iteration
        self.home_folder = model_folder + '/' + iteration
        self.import_folder = model_folder + '/' + import_folder + '/'

        # Inputs
        self.addressbase_path_list = consts.ADDRESSBASE_PATH_LIST
        self.zones_folder = zones_folder
        self.zone_translation_path = zone_translation_path
        self.KS401path = KS401path
        self.area_type_path = area_type_path

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.scenario_name = scenario_name.upper() if scenario_name is not None else ''

        # Build paths
        write_folder = os.path.join(
            model_folder,
            iteration,
            'outputs',
            'scenarios',
            self.scenario_name)

        pop_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_pop.csv')
        emp_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_emp.csv')
        report_folder = os.path.join(write_folder, 'reports')

        # Build folders
        if not os.path.exists(write_folder):
            utils.create_folder(write_folder)
        if not os.path.exists(report_folder):
            utils.create_folder(report_folder)

        # Set object paths
        self.out_paths = {
            'write_folder': write_folder,
            'report_folder': report_folder,
            'pop_write_path': pop_write_name,
            'emp_write_path': emp_write_name
        }

        # Establish a state dictionary recording which steps have been run
        # These are aligned with the section numbers in the documentation
        # TODO: enable a way to init from a point part way through the process
        self.state = {
            '5.2.2 read in core property data': 0,
            '5.2.3 property type mapping': 0,
            '5.2.4 filled property adjustment': 0,
            '5.2.5 household occupancy adjustment': 0,
            '5.2.6 NTEM segmentation': 0,
            '5.2.7 communal establishments': 0,
            '5.2.8 MYPE adjustment': 0,
            '5.2.9 employment adjustment': 0,
            '5.2.10 SEC/SOC': 0,
            '5.2.11 car availability': 0
        }

    def build_by_pop(self):
        """

        Returns
        -------

        """
        # Check which parts of the process need running
        # Make a new sub folder of the home directory for the iteration and set this as the working directory
        os.chdir(self.model_folder)
        utils.create_folder(self.iteration, ch_dir=True)

        # Run through the main build process
        if self.state['5.2.2 read in core property data'] == 0:
            main_build.copy_addressbase_files(self)

        # Steps from main build
        if self.state['5.2.4 filled property adjustment'] == 0:
            main_build.filled_properties(self)

        if self.state['5.2.5 household occupancy adjustment'] == 0:
            main_build.apply_household_occupancy(self)

        if self.state['5.2.6 NTEM segmentation'] == 0:
            main_build.apply_ntem_segments(self)

        if self.state['5.2.7 communal establishments'] == 0:
            main_build.join_establishments(self)

        # Property type mapping is done after communal establishments are added, despite position in the documentation
        if self.state['5.2.3 property type mapping'] == 0:
            main_build.land_use_formatting(self)

        # TODO: main_build then runs the following function, how relate to documentation?
        main_build.apply_ns_sec_soc_splits(self)

        # Steps from mid-year population estimate adjustment
        """
        if self.state['5.2.8 MYPE adjustment'] == 0:
            mypa.control_to_lad(self)
            mypa.adjust_landuse_to_specific_yr(self)
            mypa.sort_out_hops_uplift(self)

        if self.state['5.2.9 employment adjustment'] == 0:
            mypa.Country_emp_control(self)

        if self.state['5.2.10 SEC/SOC'] == 0:
            mypa.adjust_soc_gb(self)
        """

        # Car availability
        if self.state['5.2.11 car availability'] == 0:
            # TODO: what was this function for?
            # mypa.get_ca(self)

            # First prepare the NTS data
            car_availability_adjustment.nts_import(self)
            print('NTS import completed successfully')

            # Then apply the function from mid_year_pop_adjustments
            # TODO: uncomment line below once MYPE script has been fixed
            # mypa.adjust_car_availability(self)

