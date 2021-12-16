import logging
import os
import land_use.lu_constants as consts
from land_use.utils import file_ops as utils
from land_use.base_land_use import main_build, car_availability_adjustment, census2011_population_furness

from land_use.base_land_use import mid_year_pop_adjustments as mypa


"""
TODO:
1. Inherit from a pathing object?
2. See normits_demand.utils.compression for ways to speed up I/O
"""


class BaseYearLandUse:
    def __init__(self,
                 model_folder=consts.LU_FOLDER,
                 output_folder=consts.BY_FOLDER,
                 iteration=consts.LU_MR_ITER,
                 import_folder=consts.LU_IMPORTS,
                 model_zoning='MSOA',
                 zones_folder=consts.ZONES_FOLDER,
                 zone_translation_path=consts.ZONE_TRANSLATION_PATH,
                 KS401path=consts.KS401_PATH,
                 area_type_path=consts.LU_AREA_TYPES,
                 CTripEnd_Database_path=consts.CTripEnd_Database,
                 emp_e_cat_data_path=consts.E_CAT_DATA,
                 emp_soc_cat_data_path=consts.SOC_2DIGIT_SIC,
                 emp_unm_data_path=consts.UNM_DATA,
                 base_year='2018',
                 scenario_name=None):
        """
        parse parameters from run call (file paths, requested outputs and audits)
        area types: NTEM / TfN
        output zoning system
        versioning
        state property
        """

        # TODO: Add versioning

        # File ops
        self.model_folder = model_folder + '/' + output_folder
        self.iteration = iteration
        self.home_folder = model_folder + '/' + output_folder + '/' + iteration
        self.import_folder = model_folder + '/' + import_folder + '/'

        # Inputs
        self.addressbase_path_list = consts.ADDRESSBASE_PATH_LIST
        self.zones_folder = zones_folder
        self.zone_translation_path = zone_translation_path
        self.KS401path = KS401path
        self.area_type_path = area_type_path
        self.CTripEnd_Database_path = CTripEnd_Database_path

        self.e_cat_emp_path = emp_e_cat_data_path
        self.soc_emp_path = emp_soc_cat_data_path
        self.unemp_path = emp_unm_data_path

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.scenario_name = scenario_name.upper() if scenario_name is not None else ''

        # Build paths
        write_folder = os.path.join(
            model_folder,
            output_folder,
            iteration,
            'outputs')

        pop_write_name = 'land_use_' + str(self.base_year) + '_' + model_zoning.lower() + '_pop.csv'
        emp_write_name = 'land_use_' + str(self.base_year) + '_' + model_zoning.lower() + '_emp.csv'

        pop_write_path = os.path.join(write_folder, pop_write_name)
        emp_write_path = os.path.join(write_folder, emp_write_name)

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
            'pop_write_path': pop_write_path,
            'emp_write_path': emp_write_path
        }

        # Establish a state dictionary recording which steps have been run
        # These are aligned with the section numbers in the documentation
        # TODO: enable a way to init from a point part way through the process
        self.pop_state = {
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
        logging.basicConfig(filename='base_year_land_use.log', level=logging.INFO, format='%(asctime)s: %(message)s')

        # TODO: Change from copy to check imports
        if self.state['3.1 2011 NTEM pop and zone conversion for Scotland'] == 0:
            logging.info('Running step 3.1, 2011 NTEM pop and zone conversion for Scotland')
            census2011_population_furness.NTEM_Pop_Interpolation(self)

        # Run through the main build process
        if self.state['5.2.2 read in core property data'] == 0:
            logging.info('Running step 5.2.2, reading in core property data')
            main_build.copy_addressbase_files(self)

        # Steps from main build
        if self.state['5.2.4 filled property adjustment'] == 0:
            logging.info('Running step 5.2.4, calculating the filled property adjustment factors')
            main_build.filled_properties(self)

        if self.state['5.2.5 household occupancy adjustment'] == 0:
            logging.info('Running step 5.2.5, household occupancy adjustment')
            main_build.apply_household_occupancy(self)

        if self.state['5.2.6 NTEM segmentation'] == 0:
            logging.info('Running step 5.2.6, NTEM segmentation')
            main_build.apply_ntem_segments(self)

        if self.state['5.2.7 communal establishments'] == 0:
            logging.info('Running step 5.2.7, adding in communal establishments')
            main_build.join_establishments(self)

        # Property type mapping is done after communal establishments are added, despite position in the documentation
        if self.state['5.2.3 property type mapping'] == 0:
            logging.info('Running step 5.2.3, combining flat types')
            main_build.land_use_formatting(self)

        # Steps from mid-year population estimate adjustment
        if self.state['5.2.8 MYPE adjustment'] == 0:
            logging.info('Running first part of 5.2.10, SEC/SOC segmentation')
            main_build.apply_ns_sec_soc_splits(self)  # part of step 5.2.10 but required as input to 5.2.8
            logging.info('Running step 5.2.8, mid-year population estimate adjustment')
            mypa.adjust_landuse_to_specific_yr(self)
            mypa.control_to_lad_employment_ag(self)
            mypa.sort_out_hops_uplift(self)  # for audit

        if self.state['5.2.9 employment adjustment'] == 0:
            logging.info('Running step 5.2.9, employment adjustment')
            mypa.country_emp_control(self)

        if self.state['5.2.10 SEC/SOC'] == 0:
            logging.info('Running the rest of step 5.2.10, SEC/SOC segmentation')
            mypa.adjust_soc_gb(self)
            mypa.adjust_soc_lad(self)  # TODO: fix this function

        # Car availability
        if self.pop_state['5.2.11 car availability'] == 0:
            # First prepare the NTS data
            car_availability_adjustment.nts_import(self)
            print('NTS import completed successfully')

            # Then apply the function from mid_year_pop_adjustments
            # TODO: uncomment line below once MYPE script has been replaced
            # mypa.adjust_car_availability(self)

    def build_by_emp(self):
        """
        """
        os.chdir(self.model_folder)
        utils.create_folder(self.iteration, ch_dir=True)

        employment.get_emp_data(self)

        employment.skill_weight_e_cats(self)

        employment.unemp_infill(self)

        self.emp_out.to_csv(self.out_paths['emp_write_path'],
                            index=False)
