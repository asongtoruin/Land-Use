import logging
import os
from land_use import lu_constants
from land_use.utils import file_ops
from land_use.base_land_use import base_year_population_process, employment


class BaseYearLandUse:
    def __init__(self,
                 model_folder=lu_constants.LU_FOLDER,
                 output_folder=lu_constants.BY_FOLDER,
                 iteration=lu_constants.LU_MR_ITER,
                 import_folder=lu_constants.LU_IMPORTS,
                 model_zoning='MSOA',
                 zones_folder=lu_constants.ZONES_FOLDER,
                 zone_translation_path=lu_constants.ZONE_TRANSLATION_PATH,
                 ks401path=lu_constants.KS401_PATH,
                 area_type_path=lu_constants.LU_AREA_TYPES,
                 ctripend_database_path=lu_constants.CTripEnd_Database,
                 emp_e_cat_data_path=lu_constants.E_CAT_DATA,
                 emp_soc_cat_data_path=lu_constants.SOC_BY_REGION,
                 emp_unm_data_path=lu_constants.UNM_DATA,
                 base_year='2019',
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

        # Resi inputs
        self.addressbase_path_list = lu_constants.ADDRESSBASE_PATH_LIST
        self.zones_folder = zones_folder
        self.zone_translation_path = zone_translation_path
        self.ks401path = ks401path
        self.area_type_path = area_type_path
        self.CTripEnd_Database_path = ctripend_database_path

        # Non resi inputs
        self.e_cat_emp_path = emp_e_cat_data_path
        self.soc_emp_path = emp_soc_cat_data_path
        self.unemp_path = emp_unm_data_path

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.scenario_name = scenario_name.upper() if scenario_name is not None else ''

        # Build paths
        print('Building Base Year paths and preparing to run')
        write_folder = os.path.join(
            model_folder,
            output_folder,
            iteration)

        pop_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_pop.csv')
        emp_write_name = os.path.join(write_folder, 'land_use_' + str(self.base_year) + '_emp.csv')
        # Report folder not currently in use.
        # report_folder = os.path.join(write_folder, 'reports')

        # TODO: Implement this way of checking state
        self.step_keys = lu_constants.BY_POP_BUILD_STEPS

        self.step_descs = lu_constants.BY_POP_BUILD_STEP_DESCS
        
        # self._check_state()

        list_of_type_folders = ['01 Process', '02 Audits']
        # '03 Outputs' will also be a directory at this level,
        # but does not have the step sub-directories

        list_of_step_folders = [
            '3.2.1_read_in_core_property_data',
            '3.2.2_filled_property_adjustment',
            '3.2.3_apply_household_occupancy',
            '3.2.4_land_use_formatting',
            '3.2.5_uplifting_base_year_pop_base_year_MYPE',
            '3.2.6_expand_NTEM_pop',
            '3.2.7_verify_population_profile_by_dwelling_type',
            '3.2.8_subsets_of_workers+nonworkers',
            '3.2.9_verify_district_level_worker_and_nonworker',
            '3.2.10_adjust_zonal_pop_with_full_dimensions',
            '3.2.11_process_CER_data']

        # Build folders
        if not os.path.exists(write_folder):
            file_ops.create_folder(write_folder)
        # Report folder not currently in use.
        # if not os.path.exists(report_folder):
        #     file_ops.create_folder(report_folder)
        for folder_type in list_of_type_folders:
            for listed_folder in list_of_step_folders:
                if not os.path.exists(os.path.join(write_folder, folder_type, listed_folder)):
                    file_ops.create_folder(os.path.join(write_folder, folder_type, listed_folder))
        if not os.path.exists(os.path.join(write_folder, '03 Outputs')):
            file_ops.create_folder(os.path.join(write_folder, '03 Outputs'))
        if not os.path.exists(os.path.join(write_folder, '00 Logging')):
            file_ops.create_folder(os.path.join(write_folder, '00 Logging'))

        # Set object paths
        self.out_paths = {
            'write_folder': write_folder,
            # Report folder not currently in use.
            # 'report_folder': report_folder,
            'pop_write_path': pop_write_name,
            'emp_write_path': emp_write_name
        }

        # Establish a state dictionary recording which steps have been run
        # These are aligned with the section numbers in the documentation
        # TODO: enable a way to init from a point part way through the process

        self.state = {
            '3.2.1 read in core property data': 0,
            '3.2.2 filled property adjustment': 0,
            '3.2.3 household occupancy adjustment': 0,
            '3.2.4 property type mapping': 0,
            '3.2.5 Uplifting Base Year population according to Base Year MYPE': 0,
            '3.2.6 and 3.2.7 expand NTEM population to full dimensions and verify pop profile': 0,
            '3.2.8 get subsets of worker and non-worker': 0,
            '3.2.9 verify district level worker and non-worker': 0,
            '3.2.10 adjust zonal pop with full dimensions': 0,
            '3.2.11 process CER data': 0
        }

        self.norcom = 'import from NorCOM'
        # self.norcom = 'export to NorCOM'

    def build_by_pop(self):
        # TODO: Method name, this is more of an adjustment to a base now
        """

        Returns
        -------

        """
        # Check which parts of the process need running
        # Make a new sub folder of the home directory for the iteration and set this as the working directory
        os.chdir(self.model_folder)
        file_ops.create_folder(self.iteration, ch_dir=True)
        os.chdir('00 Logging')
        # Create log file without overwriting existing files
        base_year_log_name = '_'.join([self.base_year, 'base_year_land_use.log'])
        base_year_log_dir = os.getcwd()
        if os.path.exists(os.path.join(base_year_log_dir, base_year_log_name)):
            log_v_count = 1
            og_base_year_log_name = base_year_log_name[:-4]
            while os.path.exists(os.path.join(base_year_log_dir, base_year_log_name)):
                base_year_log_name = ''.join([og_base_year_log_name, '_', str(log_v_count), '.log'])
                log_v_count = log_v_count + 1
                print('The last log name I tried was already taken!')
                print('Now trying log name: %s' % base_year_log_name)
        logging.basicConfig(filename=base_year_log_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(message)s')


        # TODO: Check that this picks up the 2011 process!

        # TODO: Change from copy to check imports
        # Run through the Base Year Build process
        # Steps from main build
        # TODO: Decide if this is used for anything anymore
        if self.state['3.2.1 read in core property data'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            logging.info('Running step 3.2.1, reading in core property data')
            print('\n' + '=' * 75)
            base_year_population_process.copy_addressbase_files(self)

        if self.state['3.2.2 filled property adjustment'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            logging.info('Running step 3.2.2, calculating the filled property adjustment factors')
            print('\n' + '=' * 75)
            base_year_population_process.filled_properties(self)

        if self.state['3.2.3 household occupancy adjustment'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.3, household occupancy adjustment')
            base_year_population_process.apply_household_occupancy(self)

        if self.state['3.2.4 property type mapping'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.4, combining flat types')
            base_year_population_process.property_type_mapping(self)

        if self.state['3.2.5 Uplifting Base Year population according to Base Year MYPE'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.5, uplifting 2018 population according to 2018 MYPE')
            base_year_population_process.mye_pop_compiled(self)

        if self.state['3.2.6 and 3.2.7 expand NTEM population to full dimensions and verify pop profile'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.6, expand NTEM population to full dimensions')
            logging.info('Also running step 3.2.7, verify population profile by dwelling type')
            base_year_population_process.pop_with_full_dimensions(self)

        if self.state['3.2.8 get subsets of worker and non-worker'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.8, get subsets of worker and non-worker')
            logging.info('Called from "census_lu.py" so saving outputs to files')
            logging.info('but not saving any variables to memory')
            logging.info('Note that this function will get called again by other functions')
            base_year_population_process.subsets_worker_nonworker(self, 'census_and_by_lu')

        if self.state['3.2.9 verify district level worker and non-worker'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.9, verify district level worker and non-worker')
            base_year_population_process.la_level_adjustment(self)

        if self.state['3.2.10 adjust zonal pop with full dimensions'] == 0:
            logging.info('')
            logging.info('\n' + '=' * 75)
            print('\n' + '=' * 75)
            logging.info('Running step 3.2.10, adjust zonal pop with full dimensions')
            base_year_population_process.adjust_zonal_workers_nonworkers(self)

        # Step 3.2.11 should always be called from Step 3.2.10 (to save read/writing massive files)
        # Syntax for calling it is maintained here (albeit commented out) for QA purposes
        # if self.state['3.2.11 process CER data'] == 0:
        #     logging.info('')
        #     logging.info('=========================================================================')
        #     print('\n' + '=' * 75)
        #     logging.info('Running step 3.2.11, process CER data')
        #     BaseYear2018_population_process.process_cer_data(self, hhpop_combined_from_3_2_10, la_2_z_from_3_2_10)

    def _check_state(self,
                     step_key: str = None):
        """
        Check state function, in development.
        """
        if step_key in self.step_keys:
            check = 1  # TODO: Check run status
            self.status[self.step_key]['status'] = check

        else:
            state_dict = dict()
            # TODO: Derive from another step list

            for step in range(0, len(self.step_keys)):
                state_dict.update(
                    {self.step_keys[step]: {'desc': self.steps_descs[step],
                                            'status': 0}})

            for step, dat in state_dict.iteritems():
                # Pull from key
                self.status[self.step_key]['status'] = self._check_state(step)

        pass

    def build_by_emp(self):
        """
        """

        # TODO: Build from NTEM aligned in more detail?
        # Need to see what's available

        os.chdir(self.model_folder)
        file_ops.create_folder(self.iteration, ch_dir=True)

        employment.get_emp_data(self)

        employment.skill_weight_e_cats(self)

        employment.unemp_infill(self)

        self.emp_out.to_csv(self.out_paths['emp_write_path'],
                            index=False)
