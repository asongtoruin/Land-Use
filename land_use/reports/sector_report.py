# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 11:34:00 2020

@author: genie
"""

import os

import pandas as pd
import numpy as np

from typing import List


class SectorReporter:
    """
    Class to build sector reports from an output land use vector
    """

    def __init__(self,
                 target_folder: str,  # PathLike
                 output_folder: str,  # PathLike
                 zone_system: str = 'msoa',
                 input_type: str = None,
                 model_schema: str = None,  # PathLike
                 model_sectors: str = None,  # PathLike
                 target_file_types: List = ['.csv', '.bz2']
                 ):

        """
        target_folder: file or folder of files to do sector reports for
        model_name: name of the model to look for
        input_type: from 'vector', 'long_matrix', 'wide_matrix'
        model_schema: folder to look for correspondences. will search in
        default folder if nothing provided
        model_sectors = path to .csv correspondence to sectors
        """

        # Init
        _default_import_folder = 'I:/NorMITs Land Use/import'

        self.target_folder = target_folder

        self.output_folder = output_folder

        # Pass init params to object
        self.zone_system = zone_system
        self.zone_id = '%s_zone_id' % zone_system.lower()

        # If no model schema folder - find one
        if model_schema is None:
            home_list = os.listdir(_default_import_folder)
            model_schema = [x for x in home_list if zone_system in x][0]
            model_schema = os.path.join(
                _default_import_folder,
                model_schema,
                'model schema')

        # Figure out input type
        # TODO: If nothing provided, work it out
        self.input_type = input_type

        # Set model schema
        self.model_schema = model_schema

        # Find sector correspondence
        if model_sectors is None:
            schema_dir = os.listdir(model_schema)
            corr_name = '%s_sector_correspondence.csv' % zone_system.lower()
            sector_file = [x for x in schema_dir if corr_name in x][0]
            model_sectors = os.path.join(model_schema, sector_file)

        self.sectors = pd.read_csv(model_sectors)

        self.target_file_types = target_file_types


    def sector_report(self,
                      sector_type: str = 'ca_sector',
                      ca_report: bool = True,
                      three_sector_report: bool = True,
                      export: bool = False):

        """
        sector_type: report requested - takes
            'ca_sector', 'three_sector', 'ie_sector'
        export = False:
            Write to object output dir, or not
        """

        # Index folder
        # TODO: Pull imports and parsing into line with NorMITs standard
        target_mats = os.listdir(self.target_folder)
        # Filter down to target file types
        target_mats = [x for y in self.target_file_types for x in target_mats if y in x]

        # Subset sectors into ie and 3 sector reports
        ca_sectors = self.sectors  # reindex and such
        three_sectors_2d = self.sectors
        ie_2d = self.sectors

        # Apply translation
        mat_sector_reports = dict()
        # TODO: Assumptions galore - needs to be smarter and better integrated
        for tm in target_mats:

            print(tm)
            mat = fo.read_df(os.path.join(self.target_folder, tm))

            mat = mat.rename(columns={list(mat)[0]: 'norms_zone_id'})

            if self.input_type == 'wide_matrix':

                """
                long_data = pd.melt(mat,
                                    id_vars=list(mat)[0],
                                    var_name='a_zone',
                                    value_name='demand',
                                    col_level=0)
                                    
                sector_report = 
                """



            elif self.input_type == 'long_matrix':
                # TODO: Translate to wide, do mat wide trans
                print('Can\'t do these yet')
            elif self.input_type == 'vector':
                # TODO: Do simple vector trans by required subset(s)
                print('Can\'t do these yet')



        # Export

        sector_report = ''

        return sector_report

    def _sectors_join_method(self,
                             long_data):

        """
        Method for joining sectors length wise
        Expects format 'p_zone', 'a_zone', 'demand'

        """

        long_data = long_data.merge(self.sectors,
                                    how='left',
                                    on=self.zone_id)

        long_data = long_data.reindex(
            ['ca_sector_2020_zone_id', 'a_zone', 'demand'], axis=1)
        long_data = long_data.groupby(['ca_sector_2020_zone_id', 'a_zone']).sum().reset_index()

        long_data = long_data.rename(columns={'ca_sector_2020_zone_id': 'sector_p',
                                              'a_zone': self.zone_id})
        long_data['norms_zone_id'] = long_data[self.zone_id].astype(int)

        left_only = long_data.copy()
        left_only_sum = left_only.reindex(
            ['sector_p', 'demand'], axis=1).groupby('sector_p').sum().reset_index()

        long_data = long_data.merge(self.sectors,
                                    how='left',
                                    on=self.zone_id)

        long_data = long_data.reindex(
            ['sector_p', 'ca_sector_2020_zone_id', 'demand'], axis=1)
        long_data = long_data.groupby(['sector_p', 'ca_sector_2020_zone_id']).sum().reset_index()

        long_data = long_data.rename(columns={'ca_sector_2020_zone_id': 'sector_a'})

        pivoted_data = pd.pivot(long_data, index='sector_p', columns='sector_a', values='demand')

        return long_data, pivoted_data
