# -*- coding: utf-8 -*-
"""Classes for managing the ABP processing config and parameters."""

##### IMPORTS #####
# Standard imports
import logging

# Third party imports
from caf.toolkit import config_base
import pydantic

# Local imports
from land_use.abp_processing import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####
class ABPConfig(config_base.BaseConfig):
    output_folder: pydantic.DirectoryPath
    database_connection_parameters: database.ConnectionParameters

##### FUNCTIONS #####
