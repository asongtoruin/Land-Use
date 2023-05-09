# -*- coding: utf-8 -*-
"""Main module for performing ABP processing and analysis."""

# Built-Ins
from __future__ import annotations

import datetime as dt
import logging
import pathlib

# Third Party

# Local Imports
from land_use.abp_processing import config

# # # CONSTANTS # # #
LOG = logging.getLogger(__name__)
CONFIG_FILE = pathlib.Path("abp_processing_config.yml")
LOG_FILE = "ABP_processing.log"

# # # CLASSES # # #


# # # FUNCTIONS # # #
"""
Build a function to hit the PostGres database for a pre-specified region!
OR Specify SQL query to just get what you need
Build analysis ready table (joining all of the relevant data)
"""

"""
Is there anything we can build that provides added values
e.g if you have a house point, and a bus stop point, could you fuse in distance to bus stop by house
(if you're hitting compute problems can we pick the multiprocessing library)
"""

"""
Provide a condensed ABP dataset for:
* Residential land use model
Anything useful about property classifications, distance to transport infrastructure
* Non-residential land use model
Anything about business type, SIC classification, VOA classification (!)
"""

"""
Talk to Isaac about using caf.viz libraries to map 'live'
"""


def initialise_logger(log_file: pathlib.Path) -> None:
    # TODO(MB) Create a more complete class to handle initialising logging

    logger = logging.getLogger("land_use.abp_processing")
    logger.setLevel(logging.DEBUG)

    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(
        logging.Formatter("[{levelname:^8.8}] {message}", style="{")
    )
    logger.addHandler(streamhandler)

    filehandler = logging.FileHandler(log_file)
    filehandler.setFormatter(
        logging.Formatter("{asctime} [{levelname:^8.8}] {message}", style="{")
    )
    logger.addHandler(filehandler)
    LOG.info("Initialised log file: %s", log_file)


def main(parameters: config.ABPConfig) -> None:
    output_folder = (
        parameters.output_folder / f"{dt.date.today():%Y%m%d} ABP Processing"
    )
    output_folder.mkdir(exist_ok=True)

    initialise_logger(output_folder / LOG_FILE)
    LOG.info("Outputs saved to: %s", output_folder)


def _run() -> None:
    parameters = config.ABPConfig.load_yaml(CONFIG_FILE)
    main(parameters)


if __name__ == "__main__":
    _run()
