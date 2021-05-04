import os
import pandas as pd
import land_use.lu_constants as consts
from land_use import utils
from land_use.base_land_use import main_build

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
                 model_zoning='msoa',
                 base_land_use_path=None,
                 base_employment_path=None,
                 base_soc_mix_path=None,
                 base_year='2018',
                 scenario_name=None,
                 pop_segmentation_cols=None,
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
        self.import_folder = import_folder

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.scenario_name = scenario_name.upper()

        # If Nones passed in, set defaults
        # This is for base datasets that don't vary between scenarios
        if base_land_use_path is None:
            if sub_for_defaults:
                print('Using default Residential Land Use')
                base_land_use_path = consts.RESI_LAND_USE_MSOA
            else:
                raise ValueError('No base land use provided')
        if base_employment_path is None:
            if sub_for_defaults:
                print('Using default Employment Land Use')
                base_employment_path = consts.EMPLOYMENT_MSOA
            else:
                raise ValueError('No base employment provided')
        if base_soc_mix_path is None:
            if sub_for_defaults:
                print('Using default Employment Skill Mix')
                base_soc_mix_path = consts.SOC_2DIGIT_SIC
            else:
                raise ValueError('No employment mix provided')

        # Segmentation
        self.pop_segmentation_cols = pop_segmentation_cols

        # Build paths
        write_folder = os.path.join(
            model_folder,
            iteration,
            'outputs',
            'scenarios',
            scenario_name)

        pop_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.base_year) + '_pop.csv'))

        emp_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.base_year) + '_emp.csv'))

        report_folder = os.path.join(write_folder,
                                     'reports')

        # Build folders
        if not os.path.exists(write_folder):
            utils.create_folder(write_folder)
        if not os.path.exists(report_folder):
            utils.create_folder(report_folder)

        # Set object paths
        self.in_paths = {
            'base_land_use': base_land_use_path,
            'base_employment': base_employment_path,
            'base_soc_mix': base_soc_mix_path
        }

        self.out_paths = {
            'write_folder': write_folder,
            'report_folder': report_folder,
            'pop_write_path': pop_write_name,
            'emp_write_path': emp_write_name
        }

        # Write init report for param audits
        init_report = pd.DataFrame(self.in_paths.values(),
                                   self.in_paths.keys())
        init_report.to_csv(
            os.path.join(self.out_paths['report_folder'],
                         '%s_%s_run_params.csv' % (self.scenario_name,
                                                   self.base_year))
        )

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
        # TODO: decide how to handle the 5.2.2 read in core property data and 5.2.3 property type mapping steps
        # TODO: we need main_build to inherit the paths/iteration number etc from this base object
        main_build.copy_addressbase_files()
        if self.state['5.2.2 read in core property data'] == 0:
            pass

        if self.state['5.2.3 property type mapping'] == 0:
            pass

        # Steps from main build
        if self.state['5.2.4 filled property adjustment'] == 0:
            main_build.FilledProperties()
            self.state['5.2.4 filled property adjustment'] = 1

        if self.state['5.2.5 household occupancy adjustment'] == 0:
            main_build.ApplyHouseholdOccupancy()
            self.state['5.2.5 household occupancy adjustment'] = 1

        if self.state['5.2.6 NTEM segmentation'] == 0:
            main_build.ApplyNtemSegments()
            self.state['5.2.6 NTEM segmentation'] = 1

        if self.state['5.2.7 communal establishments'] == 0:
            main_build.join_establishments()
            self.state['5.2.7 communal establishments'] = 1

        # TODO: work out what main_build.LanduseFormatting() and main_build.ApplyNSSECSOCsplits() are for

        # Steps from mid-year population estimate adjustment
        if self.state['5.2.8 MYPE adjustment'] == 0:
            pass

        if self.state['5.2.9 employment adjustment'] == 0:
            pass

        if self.state['5.2.10 SEC/SOC'] == 0:
            pass

        # Car availability
        if self.state['5.2.11 car availability'] == 0:
            pass

