# -*- coding: utf-8 -*-
"""Module to extract the warehousing data from ABP for use in the Local Freight Tool."""

##### IMPORTS #####
# Standard imports
from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Sequence

# Third party imports
import geopandas as gpd
import pandas as pd
from psycopg2 import sql
from shapely import geometry

# Local imports
from land_use.abp_processing import config, database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
CRS_BRITISH_GRID = "EPSG:27700"
CLASSIFICATION_CODES = {"ABP": ("CI04PL",), "VOA SCAT": ("217", "267")}
WAREHOUSE_ABP_CODE = "CI04"
WEB_MERCATOR = "EPSG:4326"

##### CLASSES #####

##### FUNCTIONS #####
def to_kepler_geojson(geodata: gpd.GeoDataFrame, out_file: pathlib.Path) -> None:
    """Convert to Web mercator CRS and output to GeoJSON.

    Parameters
    ----------
    geodata : gpd.GeoDataFrame
        Data to output.
    out_file : pathlib.Path
        Path to GeoJSON file to create.
    """
    if out_file.suffix.lower() != ".geojson":
        LOG.warning(
            "Unexpected suffix for saving GeoJSON (%s) using '.geojson' instead",
            out_file.name,
        )
        out_file = out_file.with_suffix(".geojson")

    geodata = geodata.to_crs(WEB_MERCATOR)

    with open(out_file, "wt", encoding="utf-8") as file:
        file.write(geodata.to_json())
    LOG.info("Written: %s", out_file)


def voa_code_count(
    connected_db: database.Database, output_folder: pathlib.Path
) -> None:
    """Count rows with VOA and ABP codes and output summary spreadsheet.

    Parameters
    ----------
    connected_db : database.Database
        Database to count rows in.
    output_folder : pathlib.Path
        Folder to save Excel workbook to.
    """
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

    query = """
    SELECT cl.class_scheme, cl.classification_code, count(*) AS "count"
    FROM ({abp_classification}) cl
    GROUP BY cl.class_scheme, cl.classification_code
    """
    filtered_class_counts = connected_db.query_to_dataframe(
        sql.SQL(query).format(abp_classification=classification_codes_query())
    )
    filtered_class_counts.loc[:, "perc_count"] = (
        filtered_class_counts["count"] / class_counts["count"].sum()
    )

    excel_path = output_folder / "ABP_SCAT_counts.xlsx"
    # pylint: disable=abstract-class-instantiated
    with pd.ExcelWriter(excel_path) as excel:
        # pylint: enable=abstract-class-instantiated
        class_counts.to_excel(excel, sheet_name="Class Schame Counts")
        scat_counts.to_excel(excel, sheet_name="SCAT Counts")
        filtered_class_counts.to_excel(excel, sheet_name="Filtered Codes")
    LOG.info("Written: %s", excel_path)


def _positions_geodata(data: pd.DataFrame, out_file: pathlib.Path) -> gpd.GeoDataFrame:
    """Convert `data` into GeoDataframe using X / Y coordinate columns."""
    missing = data["x_coordinate"].isna() | data["y_coordinate"].isna()
    if missing.sum() > 0:
        LOG.warning("Missing coordinates for %s rows", missing.sum())

    data.loc[:, "geometry"] = data.apply(
        lambda row: geometry.Point(row["x_coordinate"], row["y_coordinate"]), axis=1
    )
    geodata = gpd.GeoDataFrame(data, geometry="geometry", crs=CRS_BRITISH_GRID)

    for column in geodata.select_dtypes(exclude=("number", "geometry")).columns:
        geodata.loc[:, column] = geodata[column].astype(str)

    to_kepler_geojson(geodata, out_file)

    duplicated = geodata["uprn"].duplicated().sum()
    if duplicated > 0:
        LOG.warning("%s duplicate UPRNs found in %s", duplicated, out_file.stem)

    return geodata


def get_warehouse_positions(
    connected_db: database.Database,
    output_file: pathlib.Path,
    warehouse_select_query: sql.Composable,
) -> gpd.GeoDataFrame:
    """Select warehouse data and join UPRN coordinates.

    Parameters
    ----------
    connected_db : database.Database
        ABP database to run query on.
    output_file: pathlib.Path
        GeoJSON file to save query results to.
    warehouse_select_query: sql.Composable
        Query to select warehouse data, must contain a 'uprn'
        column for joining coordinates to.
    """
    query = """
    SELECT q.*, blpu.x_coordinate, blpu.y_coordinate

    FROM (
    {query}
    ) q

    LEFT JOIN data_common.abp_blpu blpu ON q.uprn = blpu.uprn
    """
    LOG.info("Extracting warehouse coordinates to %s", output_file.name)
    data = connected_db.query_to_dataframe(
        sql.SQL(query).format(query=warehouse_select_query)
    )

    return _positions_geodata(data, output_file)


def get_warehouse_floorspace(
    connected_db: database.Database,
    output_file: pathlib.Path,
    warehouse_select_query: sql.Composable,
) -> gpd.GeoDataFrame:
    """Select warehouse data and join floorspace polygons.

    Parameters
    ----------
    connected_db : database.Database
        ABP database to run query on.
    output_file: pathlib.Path
        GeoJSON file to save query results to.
    warehouse_select_query: sql.Composable
        Query to select warehouse data, must contain a 'uprn'
        column for joining polygons to.
    """
    query = """
    SELECT q.*, cr.cross_reference, mm.descriptiveterm,
        public.ST_AsText(mm.wkb_geometry) AS geom_wkt,
        mm.calculatedareavalue AS area

    FROM (
    {query}
    ) q

    LEFT JOIN (
        SELECT uprn, cross_reference, "version"
        FROM data_common.abp_crossref
        WHERE "version" IS NOT NULL
    ) cr ON q.uprn = cr.uprn
    
    INNER JOIN data_common.mm_topographicarea mm ON cr.cross_reference = mm.fid;
    """
    LOG.info("Extracting warehouse floorspace to %s", output_file.name)
    data = connected_db.query_to_dataframe(
        sql.SQL(query).format(query=warehouse_select_query)
    )
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

    to_kepler_geojson(geodata, output_file)

    duplicated = geodata["uprn"].duplicated().sum()
    if duplicated > 0:
        LOG.warning("%s duplicate UPRNs found in warehouse floorspace", duplicated)

    return geodata


def classification_codes_query(
    voa_scat: Sequence[str] | None = None,
    abp: Sequence[str] = None,
    year: int | None = None,
) -> sql.Composable:
    """Select data from 'abp_classification' table with filters.

    Parameters
    ----------
    voa_scat: Sequence[str], optional
        VOA Scat codes for filtering, if not given uses
        `CLASSIFICATION_CODES["VOA SCAT"]`.
    abp: Sequence[str], optional
        ABP classification code for filtering, if not given
        uses `CLASSIFICATION_CODES["ABP"]`.
    year : int, optional
        Year to use for filtering data, excludes rows which
        have an end date before this or start date after this.

    Returns
    -------
    sql.Composable
        Select query.
    """
    if voa_scat is None:
        voa_scat = CLASSIFICATION_CODES["VOA SCAT"]
    if abp is None:
        abp = CLASSIFICATION_CODES["ABP"]

    query = """
    SELECT uprn, class_scheme, classification_code,
        start_date, end_date, last_update_date, entry_date
    FROM data_common.abp_classification
    WHERE (
        (
            class_scheme = 'VOA Special Category'
            AND classification_code IN ({scat})
        ) OR classification_code IN ({abp})
    )
    """
    sql_query = sql.SQL(query.strip()).format(
        scat=sql.SQL(",").join(sql.Literal(i) for i in voa_scat),
        abp=sql.SQL(",").join(sql.Literal(i) for i in abp),
    )

    if year is None:
        return sql_query

    date_query_str = [
        "(start_date ISNULL OR DATE_TRUNC('year', start_date) <= {date})",
        "(end_date ISNULL OR DATE_TRUNC('year', end_date) >= {date})",
    ]
    date = sql.Literal(dt.date(year, 1, 1).isoformat())
    date_queries = [sql.SQL(q).format(date=date) for q in date_query_str]

    sql_query = sql.SQL("\n\tAND ").join([sql_query, *date_queries])

    return sql_query


def warehouse_organisations_query(
    organisation: str,
    voa_scat: Sequence[str] | None = None,
    abp: Sequence[str] = None,
    year: int | None = None,
) -> sql.Composable:
    """Select warehouse data for given `organisation`.

    Parameters
    ----------
    organisation : str
        Name of the organisation to filter warehouses on.
    voa_scat: Sequence[str], optional
        VOA Scat codes for filtering, if not given uses
        `CLASSIFICATION_CODES["VOA SCAT"]`.
    abp: Sequence[str], optional
        ABP classification code for filtering, if not given
        uses `CLASSIFICATION_CODES["ABP"]`.
    year : int, optional
        Year to use for filtering data, excludes rows which
        have an end date before this or start date after this.

    Returns
    -------
    sql.Composable
        Select query.
    """
    query = r"""
    SELECT cl.*, o.organisation

    FROM (
    {abp_classification}
    ) cl

    JOIN (
        SELECT uprn, organisation
        FROM data_common.abp_organisation
        WHERE organisation ILIKE {org}
    ) o ON cl.uprn = o.uprn
    """

    if abp is None:
        abp = CLASSIFICATION_CODES["ABP"] + (WAREHOUSE_ABP_CODE,)

    return sql.SQL(query).format(
        abp_classification=classification_codes_query(voa_scat, abp, year),
        org=sql.Literal(f"%{organisation}%"),
    )


def get_classification_codes(
    connected_db: database.Database, output_folder: pathlib.Path
) -> None:
    """Select ABP classification codes from database."""
    query = """
    SELECT "Concatenated", "Class_Desc", "Primary_Code", "Primary_Desc",
        "Secondary_Code", "Secondary_Desc", "Tertiary_Code",
        "Tertiary_Desc", "Quaternary_Code", "Quaternary_Desc"

    FROM data_common.ab_classification_codes
    WHERE "Primary_Code" = 'C' AND "Secondary_Code" = 'I' AND "Tertiary_Code" = '4'
    ORDER BY "Primary_Desc";
    """
    data = connected_db.query_to_dataframe(query)
    output_file = output_folder / "ABP_classification_codes-warehouses.csv"
    data.to_csv(output_file, index=False)
    LOG.info("Written: %s", output_file)


def load_shapefile(parameters: config.ShapefileParameters) -> gpd.GeoDataFrame:
    """Load shapefile and set ID column as index."""
    LOG.info("Loading %s", parameters.path)
    data: gpd.GeoDataFrame = gpd.read_file(parameters.path)
    if parameters.id_column not in data.columns:
        raise KeyError(
            f"ID column {parameters.id_column} not found in {parameters.path.name}"
        )

    data = data[[parameters.id_column, "geometry"]]

    if data.crs is None:
        data.set_crs(parameters.crs, inplace=True)
    else:
        data.to_crs(parameters.crs, inplace=True)

    return data.set_index(parameters.id_column)


def warehouse_by_lsoa(
    connected_db: database.Database,
    query: sql.Composable,
    lsoas: gpd.GeoDataFrame,
    lsoa_id_column: str,
    output_file: pathlib.Path,
) -> pd.DataFrame:
    """Run select `query` and aggregate warehouse floorspace to LSOA.

    Saves `query` results to the following files:
    - {output_file}-floorspace.geojson
    - {output_file}-floorspace_lsoa.csv
    - {output_file}-floorspace_lsoa.geojson
    - {output_file}-positions.geojson
    - {output_file}-positions_with_lsoa.geojson

    Parameters
    ----------
    connected_db : database.Database
        ABP database to extract data from.
    query : sql.Composable
        Query for selecting warehouse data, this query
        should include a 'uprn' column.
    lsoas : gpd.GeoDataFrame
        LSOA polygons data.
    lsoa_id_column : str
        Name of column containing LSOA IDs.
    output_file : pathlib.Path
        Base file path to save all outputs to.

    Returns
    -------
    pd.DataFrame
        Floorspace area by LSOA ID and UPRN with columns:
        `lsoa_id_column`, 'uprn' and 'area'.
    """
    positions = get_warehouse_positions(
        connected_db,
        output_file.with_name(output_file.stem + "-positions.geojson"),
        query,
    )
    floorspace = get_warehouse_floorspace(
        connected_db,
        output_file.with_name(output_file.stem + "-floorspace.geojson"),
        query,
    )

    positions: gpd.GeoDataFrame = positions.merge(
        floorspace[["uprn", "area"]],
        on="uprn",
        how="outer",
        indicator="floorspace_merge",
    )
    positions.loc[:, "floorspace_merge"] = positions["floorspace_merge"].replace(
        {"left_only": "positions_only", "right_only": "floorspace_only"}
    )

    lsoa_positions: gpd.GeoDataFrame = gpd.sjoin(
        positions, lsoas.reset_index(), how="left", op="within"
    )

    for column in lsoa_positions.select_dtypes("category").columns:
        lsoa_positions.loc[:, column] = lsoa_positions[column].astype(str)

    to_kepler_geojson(
        lsoa_positions,
        output_file.with_name(output_file.stem + "-positions_with_lsoa.geojson"),
    )

    duplicated = lsoa_positions["uprn"].duplicated().sum()
    if duplicated > 0:
        LOG.warning("%s duplicate UPRNs found", duplicated)

    lsoa_warehouse: gpd.GeoDataFrame = (
        lsoa_positions[[lsoa_id_column, "area"]]
        .groupby(lsoa_id_column, as_index=False)
        .sum()
    )
    lsoa_warehouse = lsoa_warehouse.merge(lsoas, on=lsoa_id_column, validate="1:1")
    lsoa_warehouse = gpd.GeoDataFrame(
        lsoa_warehouse, geometry="geometry", crs=lsoas.crs
    )
    out_file = output_file.with_name(output_file.stem + "-floorspace_lsoa.geojson")
    to_kepler_geojson(lsoa_warehouse, out_file)

    columns = [lsoa_id_column, "area"]
    lsoa_warehouse.loc[:, columns].to_csv(out_file.with_suffix(".csv"), index=False)

    return lsoa_positions[[lsoa_id_column, "uprn", "area"]]


def combine_lsoa_areas(
    lsoa_data: list[pd.DataFrame], lsoa_id_column: str, output_file: pathlib.Path
) -> None:
    """Combine the output from `warehouse_by_lsoa` and aggregate to LSOA.

    Any duplicate UPRNs are dropped to avoid double counting.

    Parameters
    ----------
    lsoa_data : list[pd.DataFrame]
        Warehouse floorspace area from `warehouse_by_lsoa`.
    lsoa_id_column : str
        Name of column containing LSOA IDs.
    output_file : pathlib.Path
        Path to CSV to save output to.
    """
    lsoa_warehouse = pd.concat(lsoa_data)

    before = len(lsoa_warehouse)
    lsoa_warehouse.drop_duplicates("uprn", inplace=True)
    after = len(lsoa_warehouse)
    difference = before - after
    msg = (
        f"Dropped {difference:,.0f} ({difference / before:.1%}) rows "
        f"with duplicate UPRNs ({after:,.0f} remaining)"
    )
    LOG.info(msg)

    lsoa_warehouse = lsoa_warehouse.groupby(lsoa_id_column, as_index=False)[
        "area"
    ].sum()
    lsoa_warehouse.to_csv(output_file, index=False)
    LOG.info("Written combined warehouse floorspace: %s", output_file)


def extract_warehouses(
    database_connection_parameters: database.ConnectionParameters,
    output_folder: pathlib.Path,
    shapefile: config.ShapefileParameters,
    year: int | None = None,
) -> None:
    """Extract warehouse data from database and aggregate floorspace to LSOA.

    Saves warehouse data to various GeoJSON files and total LSOA
    areas to CSVs.

    Parameters
    ----------
    database_connection_parameters : database.ConnectionParameters
        Parameters for connecting to the ABP database.
    output_folder : pathlib.Path
        Folder for saving outputs to.
    shapefile : config.ShapefileParameters
        Parameters for the LSOA shapefile.
    year : int, optional
        Year to use for filtering data, excludes rows which
        have an end date before this or start date after this.
    """
    lsoa = load_shapefile(shapefile)

    with database.Database(database_connection_parameters) as connected_db:

        get_classification_codes(connected_db, output_folder)
        voa_code_count(connected_db, output_folder)

        queries = {
            "warehouses": classification_codes_query(year=year),
            "warehouses_amazon": warehouse_organisations_query("amazon", year=year),
        }

        lsoa_warehouse_floorspace: list[pd.DataFrame] = []

        for name, query in queries.items():
            folder = output_folder / name
            folder.mkdir(exist_ok=True)

            if year is None:
                LOG.info("Extracting %s data for all years", name)
            else:
                LOG.info("Extracting %s for %s", name, year)
                name = f"{name}_{year}"

            lsoa_warehouse = warehouse_by_lsoa(
                connected_db, query, lsoa, shapefile.id_column, folder / name
            )
            lsoa_warehouse_floorspace.append(lsoa_warehouse)

        if year is None:
            out_file = output_folder / "warehouse_floorspace_by_lsoa_inc_amazon.csv"
        else:
            out_file = (
                output_folder / f"warehouse_floorspace_{year}_by_lsoa_inc_amazon.csv"
            )

        combine_lsoa_areas(lsoa_warehouse_floorspace, shapefile.id_column, out_file)
