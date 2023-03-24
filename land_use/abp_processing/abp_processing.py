# -*- coding: utf-8 -*-
"""Main module for performing ABP processing and analysis."""

# Built-Ins
import datetime as dt
import logging
import pathlib

# Third Party
import pandas as pd

# Local Imports
from land_use.abp_processing import config, database

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


def _initialise_logger(log_file: pathlib.Path) -> None:
    # TODO(MB) Create a more complete class to handle initialising logging
    streamhandler = logging.StreamHandler()
    filehandler = logging.FileHandler(log_file)

    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[streamhandler, filehandler],
        format="{asctime} [{levelname:^8.8}] {message}",
        style="{",
    )
    LOG.info("Initialised log file: %s", log_file)


def testing_stuff(database_connection_parameters: database.ConnectionParameters, output_folder: pathlib.Path) -> None:
    # TODO(MB) Remove function for testing methodology

    connected_db = database.ABPDatabase(database_connection_parameters)

    LOG.info("Fetching table names")
    connected_db.cursor.execute(
        r"""
        SELECT table_name
            FROM information_schema.tables
        """
    )

    tables = "\n".join(" | ".join(i) for i in connected_db.cursor.fetchall())
    LOG.info("Fetched table names\n%s", tables)

    postcode = "CW12%"
    classification_code = "CR%"
    blpu_state = 2
    sql = """
    SELECT postcode_locator, abp_blpu.logical_status,
        abp_blpu.blpu_state, string_agg (
            distinct abp_classification.classification_code,
            ' ' order by abp_classification.classification_code
        ) as classy_code, abp_street_descriptor.street_description,
        abp_blpu.latitude, abp_blpu.longitude

        FROM abp_blpu
        JOIN abp_classification ON abp_blpu.UPRN=abp_classification.UPRN
        JOIN abp_lpi ON abp_blpu.UPRN=abp_lpi.UPRN
        JOIN abp_street_descriptor ON abp_lpi.USRN=abp_street_descriptor.USRN
            AND class_scheme LIKE 'AddressBase Premium Classification Scheme'
        
        WHERE postcode_locator LIKE %s ESCAPE ''
            AND classification_code LIKE %s ESCAPE ''
            AND blpu_state= %s
            
        GROUP BY postcode_locator, abp_blpu.logical_status,
            abp_blpu.blpu_state, abp_street_descriptor.street_description,
            abp_blpu.latitude, abp_blpu.longitude;
    """


    LOG.info("Extracting ABP data")
    connected_db.cursor.execute(sql, (postcode, classification_code, blpu_state))
    
    data = pd.DataFrame(connected_db.cursor.fetchall())
    output_path = output_folder / "ABP_data.csv"
    data.to_csv(output_path)
    LOG.info("Written: %s", output_path)


def main(parameters: config.ABPConfig) -> None:
    output_folder = (
        parameters.output_folder / f"{dt.date.today():%Y%m%d} ABP Processing"
    )
    output_folder.mkdir(exist_ok=True)

    _initialise_logger(output_folder / LOG_FILE)
    LOG.info("Outputs saved to: %s", output_folder)

    testing_stuff(parameters.database_connection_parameters, output_folder)


def _run() -> None:
    parameters = config.ABPConfig.load_yaml(CONFIG_FILE)
    main(parameters)


if __name__ == "__main__":
    _run()
