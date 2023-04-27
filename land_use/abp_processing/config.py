# -*- coding: utf-8 -*-
"""Classes for managing the ABP processing config and parameters."""

##### IMPORTS #####
# Standard imports
from __future__ import annotations

import logging
from typing import Optional

# Third party imports
from caf.toolkit import config_base
import pydantic
from pydantic import dataclasses

# Local imports
from land_use.abp_processing import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####
class ABPConfig(config_base.BaseConfig):
    """Parameters for running the Address Base Premium data processing."""

    output_folder: pydantic.DirectoryPath  # pylint: disable=no-member
    database_connection_parameters: database.ConnectionParameters


@dataclasses.dataclass
class ShapefileParameters:
    """Parameters for an input shapefile."""

    path: pydantic.FilePath  # pylint: disable=no-member
    id_column: str
    crs: str = "EPSG:27700"


class WarehouseConfig(config_base.BaseConfig):
    """Parameters for extracting the LFT warehouse data from ABP."""

    database_connection_parameters: database.ConnectionParameters
    output_folder: pydantic.DirectoryPath  # pylint: disable=no-member
    lsoa_shapefile: ShapefileParameters
    year_filter: Optional[int] = None


##### FUNCTIONS #####
