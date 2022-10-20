import os
import logging
import pandas as pd

import land_use.lu_constants as consts
import land_use.utils.file_ops as fo
from land_use.future_land_use_DDG import NTEM_fy_process, DDG_fy_process


class FutureYearLandUse:
    def __init__(self,
                 model_folder=consts.LU_FOLDER,
                 iteration=consts.FYLU_MR_ITER,
                 import_folder=consts.LU_IMPORTS,
                 by_folder=consts.BY_FOLDER,
                 fy_folder=consts.FY_FOLDER,
                 model_zoning='msoa',
                 zones_folder=consts.ZONES_FOLDER,
                 base_resi_land_use_path=None,
                 base_non_resi_land_use_path=None,
                 area_type_path=consts.LU_AREA_TYPES,
                 ctripend_database_path=consts.CTripEnd_Database,
                 fy_demographic_path=None,
                 fy_at_mix_path=None,
                 fy_soc_mix_path=None,
                 base_year='2018',
                 future_year=None,
                 scenario_name=None,
                 CAS_scen=None,
                 pop_growth_path=None,
                 emp_growth_path=None,
                 ca_growth_path=None,
                 ca_shares_path=None,
                 pop_segmentation_cols=None,
                 sub_for_defaults=True):

        # TODO: Add versioning

        # File ops
        self.model_folder = model_folder + '/' + fy_folder
        self.iteration = iteration
        self.import_folder = model_folder + '/' + import_folder
        self.zones_folder = zones_folder
        self.by_folder = by_folder
        self.fy_folder = fy_folder
        self.by_home_folder = model_folder + '/' + by_folder + '/' + iteration
        self.fy_home_folder = model_folder + '/' + fy_folder + '/' + iteration

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.future_year = future_year
        self.scenario_name = scenario_name
        self.CAS_scen = CAS_scen
        self.area_type_path = area_type_path
        self.CTripEnd_Database_path = ctripend_database_path

        # Build paths
        write_folder = os.path.join(
            model_folder,
            fy_folder,
            iteration)
        # write_folder = os.path.join(
        #     model_folder,
        #     consts.FY_FOLDER,
        #     iteration,
        #     'outputs',
        #     'scenarios',
        #     scenario_name)

        # pop_write_name = os.path.join(
        #     write_folder,
        #     ('land_use_' + str(self.future_year) + '_pop.csv'))
        #
        # emp_write_name = os.path.join(
        #     write_folder,
        #     ('land_use_' + str(self.future_year) + '_emp.csv'))


        # Build folders
        if not os.path.exists(write_folder):
            fo.create_folder(write_folder)

        list_of_type_folders = ['02 Audits', '03 Outputs']

        list_of_scenario_folders = ['CAS Regional Scenario', 'CAS High', 'CAS Low', 'Nov 21 central']
        # Build folders

        # Report folder not currently in use.
        # if not os.path.exists(report_folder):
        #     file_ops.create_folder(report_folder)
        for folder_type in list_of_type_folders:
            for listed_folder in list_of_scenario_folders:
                if not os.path.exists(os.path.join(write_folder, folder_type, listed_folder)):
                    fo.create_folder(os.path.join(write_folder, folder_type, listed_folder))
        if not os.path.exists(os.path.join(write_folder, '00 Logging')):
            fo.create_folder(os.path.join(write_folder, '00 Logging'))
        if not os.path.exists(os.path.join(write_folder, '01 Process')):
            fo.create_folder(os.path.join(write_folder, '01 Process'))


        # Set object paths
        self.in_paths = {
            'iteration': iteration,
            'model_zoning': model_zoning,
            'base_year': base_year,
            'future_year': future_year,
            'scenario_name': scenario_name,
            'base_resi_land_use': base_resi_land_use_path,
            'base_non_resi_land_use': base_non_resi_land_use_path,
            'fy_dem_mix': fy_demographic_path,
            'fy_at_mix': fy_at_mix_path,
            'fy_soc_mix': fy_soc_mix_path,
            'pop_growth': pop_growth_path,
            'emp_growth': emp_growth_path,
            'ca_growth': ca_growth_path,
            'ca_shares': ca_shares_path
        }

        self.out_paths = {
            'write_folder': write_folder,
            # 'report_folder': report_folder,
            # 'pop_write_path': pop_write_name,
            # 'emp_write_path': emp_write_name
        }

        # # Write init reports for param audits
        # init_report = pd.DataFrame(self.in_paths.values(),
        #                            self.in_paths.keys()
        #                            )
        # init_report.to_csv(
        #     os.path.join(self.out_paths['report_folder'],
        #                  '%s_%s_input_params.csv' % (self.scenario_name,
        #                                              self.future_year))
        # )

    def NTEM_pop(self):
        NTEM_fy_process.ntem_fy_pop_interpolation(self)

    def clean_base_ntem_pop(self):
        NTEM_fy_process.clean_base_ntem_pop(self)

    def by_pop(self):
        DDG_fy_process.base_year_pop(self)

    def build_fy_pop_ntem(self):

        os.chdir(self.model_folder)
        fo.create_folder(self.iteration, ch_dir=True)
        os.chdir('00 Logging')
        # Create log file without overwriting existing files
        future_year_log_name = '_'.join(['future_year_ntem_complied_pop.log'])
        future_year_log_dir = os.getcwd()
        if os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
            log_v_count = 1
            og_future_year_log_name = future_year_log_name[:-4]
            while os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
                future_year_log_name = ''.join([og_future_year_log_name, '_', str(log_v_count), '.log'])
                log_v_count = log_v_count + 1
                print('The last log name I tried was already taken!')
                print('Now trying log name: %s' % future_year_log_name)
        logging.basicConfig(filename=future_year_log_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(message)s')

        DDG_fy_process.ntem_fy_pop_growthfactor(self)
        DDG_fy_process.NTEMaligned_pop_process(self)

    def build_fy_pop_DDG(self):
        # TODO: Method name, this is more of an adjustment to a base now
        """

        Returns
        -------

        """
        # Check which parts of the process need running
        # Make a new sub folder of the home directory for the iteration and set this as the working directory
        os.chdir(self.model_folder)
        fo.create_folder(self.iteration, ch_dir=True)
        os.chdir('00 Logging')
        # Create log file without overwriting existing files
        future_year_log_name = '_'.join(['future_year_land_use.log'])
        future_year_log_dir = os.getcwd()
        if os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
            log_v_count = 1
            og_future_year_log_name = future_year_log_name[:-4]
            while os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
                future_year_log_name = ''.join([og_future_year_log_name, '_', str(log_v_count), '.log'])
                log_v_count = log_v_count + 1
                print('The last log name I tried was already taken!')
                print('Now trying log name: %s' % future_year_log_name)
        logging.basicConfig(filename=future_year_log_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(message)s')



        DDG_fy_process.DDGaligned_fy_pop_process(self)



    def build_fy_emp(self):
        os.chdir(self.model_folder)
        fo.create_folder(self.iteration, ch_dir=True)
        os.chdir('00 Logging')
        # Create log file without overwriting existing files
        future_year_log_name = '_'.join(['future_year_employment.log'])
        future_year_log_dir = os.getcwd()
        if os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
            log_v_count = 1
            og_future_year_log_name = future_year_log_name[:-4]
            while os.path.exists(os.path.join(future_year_log_dir, future_year_log_name)):
                future_year_log_name = ''.join([og_future_year_log_name, '_', str(log_v_count), '.log'])
                log_v_count = log_v_count + 1
                print('The last log name I tried was already taken!')
                print('Now trying log name: %s' % future_year_log_name)
        logging.basicConfig(filename=future_year_log_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(message)s')

        DDG_fy_process.DDGaligned_fy_emp_process(self)



