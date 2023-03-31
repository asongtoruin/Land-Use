# -*- coding: utf-8 -*-
"""Main module for performing ABP processing and analysis."""

# Built-Ins
from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Optional

# Third Party
import pandas as pd
from psycopg2 import sql

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


def query_to_csv(
    connected_db: database.Database,
    query: str,
    output_file: pathlib.Path,
    query_params: Optional[tuple] = None,
) -> pd.DataFrame:
    data = connected_db.query_to_dataframe(query, query_params)
    data.to_csv(output_file, index=False)
    LOG.info("Written: %s", output_file)


def voa_code_count(connected_db: database.Database, output_folder: pathlib.Path):
    LOG.info("Counting ABP classification codes")
    query = """
    SELECT class_scheme, count(*) AS "count"
        FROM data_common.abp_classification
        GROUP BY class_scheme;
    """
    class_counts = connected_db.query_to_dataframe(query)
    class_counts.loc[:, "perc_count"] = (
        class_counts["count"] / class_counts["count"].sum()
    )
    class_counts = class_counts.set_index("class_scheme")

    query = """
    SELECT classification_code AS voa_scat_code, count(*) AS "count"
        FROM data_common.abp_classification
        WHERE class_scheme = 'VOA Special Category'
        GROUP BY classification_code;
    """
    scat_counts = connected_db.query_to_dataframe(query)
    scat_counts.loc[:, "perc_count"] = scat_counts["count"] / scat_counts["count"].sum()
    scat_counts = scat_counts.set_index("voa_scat_code")

    excel_path = output_folder / "ABP_SCAT_counts.xlsx"
    # pylint: disable=abstract-class-instantiated
    with pd.ExcelWriter(excel_path) as excel:
        # pylint: ensable=abstract-class-instantiated
        class_counts.to_excel(excel, sheet_name="Class Counts")
        scat_counts.to_excel(excel, sheet_name="SCAT Counts")
    LOG.info("Written: %s", excel_path)


def get_warehouses(connected_db: database.Database, output_folder: pathlib.Path):
    query = """
    SELECT cl.*, cr.cross_reference, mm.descriptiveterm,
        mm.wkb_geometry, mm.calculatedareavalue AS area

    FROM (
        SELECT uprn, classification_code AS "scat_code", start_date
            end_date, last_update_date, entry_date
        FROM data_common.abp_classification
        WHERE class_scheme = 'VOA Special Category'
            AND classification_code IN ('217', '267')
    ) cl

    LEFT JOIN (
        SELECT uprn, cross_reference
        FROM data_common.abp_crossref
        WHERE "version" IS NOT NULL
    ) cr ON cl.uprn = cr.uprn
    
    LEFT JOIN data_common.mm_topographicarea mm ON cr.cross_reference = mm.fid;
    """
    LOG.info("Extracting warehouse geometries")
    data = connected_db.query_to_dataframe(query)
    out_file = output_folder / "warehouses.csv"
    data.to_csv(out_file, index=False)
    LOG.info("Written: %s", out_file)


def get_mmdata(connected_db: database.Database, output_folder: pathlib.Path) -> None:
    tables = [
        # "mm_boundaryline",
        # "mm_cartographicsymbol",
        # "mm_cartographictext",
        "mm_topographicarea",
        "mm_topographicline",
        "mm_topographicpoint",
    ]
    query = sql.SQL("SELECT * FROM {} LIMIT 1000;")
    for table in tables:
        LOG.info("Reading %s", table)
        data = connected_db.query_to_dataframe(query.format(sql.Identifier(table)))
        out_file = output_folder / f"{table}.csv"
        data.to_csv(out_file, index=False)
        LOG.info("Written: %s", out_file)


def testing_stuff(
    database_connection_parameters: database.ConnectionParameters,
    output_folder: pathlib.Path,
) -> None:
    # TODO(MB) Remove function for testing methodology

    with database.Database(database_connection_parameters) as connected_db:

        voa_code_count(connected_db, output_folder)
        get_warehouses(connected_db, output_folder)
        # get_mmdata(connected_db, output_folder)


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
