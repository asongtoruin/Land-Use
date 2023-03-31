# -*- coding: utf-8 -*-
"""Module to extract the warehousing data from ABP for use in the Local Freight Tool."""

##### IMPORTS #####
# Standard imports
import logging
import pathlib

# Third party imports
import pandas as pd
from psycopg2 import sql

# Local imports
from land_use.abp_processing import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####

##### FUNCTIONS #####
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


def get_warehouse_floorspace(
    connected_db: database.Database, output_folder: pathlib.Path
):
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


def extract_warehouses(
    database_connection_parameters: database.ConnectionParameters,
    output_folder: pathlib.Path,
) -> None:
    # TODO(MB) Refactor with finalised SQL queries

    with database.Database(database_connection_parameters) as connected_db:

        voa_code_count(connected_db, output_folder)
        get_warehouse_floorspace(connected_db, output_folder)
        # get_mmdata(connected_db, output_folder)
