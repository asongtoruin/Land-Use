# -*- coding: utf-8 -*-
"""Front-end script for running the Address Base Premium warehouse extraction process."""

##### IMPORTS #####
# Standard imports
import datetime as dt
import logging
import pathlib

# Third party imports

# Local imports
from land_use.abp_processing import abp_processing, warehousing, config

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
LOG_FILE = "ABP_warehouse.log"
CONFIG_FILE = pathlib.Path("abp_warehouse_config.yml")

##### CLASSES #####

##### FUNCTIONS #####
def main(parameters: config.WarehouseConfig) -> None:
    """Extract warehouse data from ABP database."""
    output_folder = (
        parameters.output_folder / f"{dt.date.today():%Y%m%d} ABP Warehouse Data"
    )
    output_folder.mkdir(exist_ok=True)

    abp_processing.initialise_logger(output_folder / LOG_FILE)
    LOG.info("Outputs saved to: %s", output_folder)

    warehousing.extract_warehouses(
        parameters.database_connection_parameters,
        output_folder,
        parameters.lsoa_shapefile,
    )


def _run() -> None:
    parameters = config.WarehouseConfig.load_yaml(CONFIG_FILE)
    main(parameters)


if __name__ == "__main__":
    _run()
