# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 11:34:00 2020

@author: genie
"""

import os

import pandas as pd
import numpy as np

from typing import List

import land_use.utils.file_ops as fo


class SectorReporter:
    """
    Class to build sector reports from an output land use vector
    """

    def __init__(self,
                 target_folder: str,  # PathLike
                 zone_system: str = 'msoa',
                 retain_cols: List = list(),
                 model_schema: str = None,  # PathLike
                 model_sectors: str = None,  # PathLike
                 target_file_types: List = ['.csv', '.pbz2']
                 ):

        """
        target_folder: file or folder of files to do sector reports for
        zone_system: name of the model to look for - should almost always be msoa
        model_schema: folder to look for correspondences. will search in
        default folder if nothing provided
        model_sectors: path to correspondence lookup - will find if None
        target_file_types: list of permissible file types to import
        """

        # Init
        _default_import_folder = 'I:/NorMITs Land Use/import'

        self.target_folder = target_folder

        # Pass init params to object
        self.zone_system = zone_system
        self.zone_id = '%s_zone_id' % zone_system.lower()

        # Pass retain cols to object
        self.retain_cols = retain_cols

        # If no model schema folder - find one
        if model_schema is None:
            home_list = os.listdir(_default_import_folder)
            home_list = [x for x in home_list if '.csv' not in x]
            model_schema = [x for x in home_list if zone_system in x][0]
            model_schema = os.path.join(
                _default_import_folder,
                model_schema,
                'model schema')

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
                      ca_report: bool = True,
                      three_sector_report: bool = False,
                      ie_sector_report: bool = False,
                      north_report: bool = False):

        """
        ca_report:
            True: Build CA sector report - or not
        three_sector_report:
            False: Build North/Scotland/South report or not
        ie_sector_report:
            False: Build internal analytical area/external report or not
        north_report:
            False: Build northern political internal report or not
        """

        # Index folder
        # TODO: Pull imports and parsing into line with NorMITs standard
        target_mats = os.listdir(self.target_folder)
        # Filter down to target file types
        target_mats = [x for y in self.target_file_types for x in target_mats if y in x]

        # Subset sectors into ie and 3 sector reports
        ca_sectors = self.sectors.reindex(
            ['msoa_zone_id', 'ca_sector_2020_zone_id'], axis=1).drop_duplicates()
        three_sectors = self.sectors.reindex(
            ['msoa_zone_id', '3_sector_id'], axis=1).drop_duplicates()
        ie_sectors = self.sectors.reindex(
            ['msoa_zone_id', 'ie_id'], axis=1).drop_duplicates()

        # Apply translation
        mat_sector_reports = dict()
        # TODO: Assumptions galore - needs to be smarter and better integrated
        for tm in target_mats:

            print('Importing land use data from %s' % tm)
            mat = fo.read_df(os.path.join(self.target_folder, tm))

            mat_dict = dict()

            if ca_report:
                ca_r = self._vector_sector_report_join_method(
                    long_data=mat,
                    sector_df=ca_sectors,
                    sector_heading=list(ca_sectors)[-1],
                    var_col=list(mat)[-1]
                )
                mat_dict.update({'ca_report': ca_r})

            if three_sector_report:
                ts_r = self._vector_sector_report_join_method(
                    long_data=mat,
                    sector_df=three_sectors,
                    sector_heading=list(three_sectors)[-1],
                    var_col=list(mat)[-1]
                )
                mat_dict.update({'three_sector_report': ts_r})

            if ie_sector_report:
                ie_r = self._vector_sector_report_join_method(
                    long_data=mat,
                    sector_df=ie_sectors,
                    sector_heading=list(ie_sectors)[-1],
                    var_col=list(mat)[-1]
                )
                mat_dict.update({'ie_sector_report': ie_r})

            mat_sector_reports.update({tm.replace('.csv', ''): mat_dict})

        return mat_sector_reports

    def _vector_sector_report_join_method(self,
                                          long_data: pd.DataFrame,
                                          sector_df: pd.DataFrame,
                                          sector_heading: str,
                                          var_col: str = None):

        """
        Method for joining sectors length wise, on single relational table

        long_data:  pd.DataFrame - long format land use data
        sector_df: pd.DataFrame - long format sector correspondence
        sector_heading: str - name of sectors to merge on
        retain_cols: str - any cols from land use to retain in joining
        var_col: str - name of variable to sum on
        """

        long_data = long_data.merge(sector_df,
                                    how='left',
                                    on=self.zone_id)

        present_retain_cols = [x for x in self.retain_cols if x in list(long_data)]

        # Build reindex and group cols
        group_cols = [sector_heading]
        [group_cols.append(x) for x in present_retain_cols]
        ri_cols = group_cols.copy()
        ri_cols.append(var_col)

        # Apply reindex and group cols
        long_data = long_data.reindex(ri_cols, axis=1)
        long_data = long_data.groupby(group_cols).sum().reset_index()
        long_data = long_data.sort_values(group_cols).reset_index(drop=True)

        return long_data
