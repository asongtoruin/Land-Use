# -*- coding: utf-8 -*-
"""Module to extract the warehousing data from ABP for use in the Local Freight Tool."""

##### IMPORTS #####
# Standard imports
import logging
import pathlib

# Third party imports
import geopandas as gpd
import pandas as pd
from psycopg2 import sql
from shapely import geometry

# Local imports
from land_use.abp_processing import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
CRS_BRITISH_GRID = "EPSG:27700"

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


def get_warehouse_positions(
    connected_db: database.Database, output_folder: pathlib.Path
) -> gpd.GeoDataFrame:
    query = """
    SELECT cl.*, blpu.x_coordinate, blpu.y_coordinate

    FROM (
        SELECT uprn, classification_code AS "scat_code", start_date
            end_date, last_update_date, entry_date
        FROM data_common.abp_classification
        WHERE class_scheme = 'VOA Special Category'
            AND classification_code IN ('217', '267')
    ) cl

    LEFT JOIN data_common.abp_blpu blpu ON cl.uprn = blpu.uprn
    """
    LOG.info("Extracting warehouse coordinates")
    data = connected_db.query_to_dataframe(query)

    missing = data["x_coordinate"].isna() | data["y_coordinate"].isna()
    if missing.sum() > 0:
        LOG.warning("Missing coordinates for %s rows", missing.sum())

    data.loc[:, "geometry"] = data.apply(
        lambda row: geometry.Point(row["x_coordinate"], row["y_coordinate"]), axis=1
    )
    geodata = gpd.GeoDataFrame(data, geometry="geometry", crs=CRS_BRITISH_GRID)

    for column in geodata.select_dtypes(exclude=("number", "geometry")).columns:
        geodata.loc[:, column] = geodata[column].astype(str)

    out_file = output_folder / "warehouses-positions.shp"
    geodata.to_file(out_file)
    LOG.info("Written: %s", out_file)

    return geodata


def get_warehouse_floorspace(
    connected_db: database.Database, output_folder: pathlib.Path
) -> gpd.GeoDataFrame:
    query = """
    SELECT cl.*, cr.cross_reference, mm.descriptiveterm,
        public.ST_AsText(mm.wkb_geometry) AS geom_wkt,
        mm.calculatedareavalue AS area

    FROM (
        SELECT uprn, classification_code AS "scat_code", start_date
            end_date, last_update_date, entry_date
        FROM data_common.abp_classification
        WHERE class_scheme = 'VOA Special Category'
            AND classification_code IN ('217', '267')
    ) cl

    LEFT JOIN (
        SELECT uprn, cross_reference, "version"
        FROM data_common.abp_crossref
        WHERE "version" IS NOT NULL
    ) cr ON cl.uprn = cr.uprn
    
    INNER JOIN data_common.mm_topographicarea mm ON cr.cross_reference = mm.fid;
    """
    LOG.info("Extracting warehouse floorspace")
    data = connected_db.query_to_dataframe(query)
    geom_column = "geom_wkt"

    missing = data[geom_column].isna()
    if missing.sum() > 0:
        LOG.warning("Missing geometries for %s rows", missing.sum())

    geom = gpd.GeoSeries.from_wkt(
        data.loc[~missing, geom_column], index=data.loc[~missing].index
    )

    data.loc[~missing, "geometry"] = geom
    geodata = gpd.GeoDataFrame(
        data.drop(columns=geom_column), geometry="geometry", crs=CRS_BRITISH_GRID
    )

    for column in geodata.select_dtypes(exclude=("number", "geometry")).columns:
        geodata.loc[:, column] = geodata[column].astype(str)

    out_file = output_folder / "warehouses-floorspace.shp"
    geodata.to_file(out_file)
    LOG.info("Written: %s", out_file)

    return geodata


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
        get_warehouse_positions(connected_db, output_folder)
        get_warehouse_floorspace(connected_db, output_folder)
        # TODO Compare floorspace results to warehouse positions
        # TODO Join to LSOA zoning using positions
        # get_mmdata(connected_db, output_folder)
