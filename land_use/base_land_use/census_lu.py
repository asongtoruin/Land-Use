import logging
import os
import land_use.lu_constants as consts
from land_use.utils import file_ops as utils
from land_use.base_land_use import census2011_population_furness

class CensusYearLandUse:
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
                 base_year='2011',
                 scenario_name=None,
                 ):
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

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.scenario_name = scenario_name.upper() if scenario_name is not None else ''

        # Build paths
        print('Building Census Year paths and preparing to run')
        write_folder = os.path.join(
            model_folder,
            output_folder,
            iteration,
            'outputs')

        pop_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_pop.csv')
        emp_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_emp.csv')
        report_folder = os.path.join(write_folder, 'reports')

        list_of_step_folders = [
            '3.1.1 derive 2011 population from NTEM and convert Scottish zones',
            '3.1.2 expand population segmentation',
            '3.1.3 data synthesis']

        # Build folders
        if not os.path.exists(write_folder):
            utils.create_folder(write_folder)
        if not os.path.exists(report_folder):
            utils.create_folder(report_folder)
        for listed_folder in list_of_step_folders:
            if not os.path.exists(os.path.join(write_folder, listed_folder)):
                utils.create_folder(os.path.join(write_folder, listed_folder))

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
            '3.1.1 derive 2011 population from NTEM and convert Scottish zones': 1,
            '3.1.2 expand population segmentation': 1,
            '3.1.3 data synthesis': 1,
        }

    def build_by_pop(self):
        """

        Returns
        -------

        """
        # Check which parts of the process need running
        # Make a folder of the home directory for the iteration, set as wd
        os.chdir(self.model_folder)
        utils.create_folder(self.iteration, ch_dir=True)
        logging.basicConfig(filename='census_year_land_use.log', level=logging.INFO, format='%(asctime)s: %(message)s')

        # TODO: Change from copy to check imports
        # Start by processing 2011 census year data
        if self.state['3.1.1 derive 2011 population from NTEM and convert Scottish zones'] == 0:
            logging.info('')
            print('\n' + '=' * 75)
            logging.info('Running step 3.1.1 derive 2011 population from NTEM and convert Scottish zones')
            census2011_population_furness.NTEM_Pop_Interpolation(self)

        if self.state['3.1.2 expand population segmentation'] == 0:
            logging.info('')
            print('\n' + '=' * 75)
            logging.info('Running step 3.1.2 expand population segmentation')
            census2011_population_furness.create_ipfn_inputs_2011(self)

        if self.state['3.1.3 data synthesis'] == 0:
            logging.info('')
            print('\n' + '=' * 75)
            logging.info('Running step 3.1.3 data synthesis')
            census2011_population_furness.IPFN_Process_2011(self)


