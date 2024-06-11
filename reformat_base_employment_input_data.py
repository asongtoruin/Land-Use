import logging
from pathlib import Path

import pandas as pd

import land_use.preprocessing as pp
from land_use.constants import geographies, segments


# TODO consider sending this to a global config/settings file as shared with reformat population script
INPUT_DIR = Path(r"I:\NorMITs Land Use\2023\import")


# General structure is to repeat the following steps for a series of different data tables
# 1. set file_path, which will be stored in a subdirectory of input_dir
# 2. read file_path using a function from parsers (imported from pp)
# 3. convert using a function (imported from pp)
# 4. write info to hdf in a DVector compatible input format (imported from pp)


def main():
    convert_bres_2021_employees_2_digit_sic()
    convert_bres_2021_employment_2_digit_sic()
    convert_bres_2022_lsoa_employment()


def convert_bres_2021_employees_2_digit_sic():
    zoning = geographies.LSOA_EW_2011_NAME

    file_path = (
        INPUT_DIR / "BRES2021" / "Employees" / "bres_employees21_msoa11_2digit.csv"
    )

    df = pd.read_csv(file_path, skiprows=8)

    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df = df.drop(columns=["2011 super output area - middle layer"])

    df = df.rename(columns={"mnemonic": zoning})
    df = df.dropna(subset=[zoning])

    # keep only england and wales
    df[zoning] = extract_geography_code_in_countries(df[zoning], scotland=False)
    df = df.dropna(subset=[zoning])

    df_wide = reformat_for_sic_2_digit_output(
        df=df, id_vars=[zoning], var_name="sic_2_digit_description"
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def convert_bres_2021_employment_2_digit_sic():
    zoning = geographies.LSOA_EW_2011_NAME

    file_path = (
        INPUT_DIR
        / "BRES2021"
        / "Employment"
        / "bres_employment21_lsoa11_2digit_sic.csv"
    )

    df = pd.read_csv(file_path, skiprows=8, low_memory=False)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    mixed_input_col = (
        "01 : Crop and animal production, hunting and related service activities"
    )
    df[mixed_input_col] = pd.to_numeric(df[mixed_input_col], errors="coerce")
    df = df.dropna(axis=0, subset=[mixed_input_col])

    # filter rows to just lsoas in england and wales
    df[zoning] = extract_geography_code_in_countries(df["Area"], scotland=False)
    df = df.drop(columns=["Area"])
    df = df.dropna(subset=[zoning])

    df_wide = reformat_for_sic_2_digit_output(
        df=df, id_vars=[zoning], var_name="sic_2_digit_description"
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def reformat_for_sic_2_digit_output(
    df: pd.DataFrame, id_vars: list[str], var_name: str
) -> pd.DataFrame:
    df_long = df.melt(
        id_vars=id_vars, var_name="sic_2_digit_description", value_name="count"
    )

    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES["sic_2_digit"]
    # define dictionary of segmentation mapping
    inv_seg = {v: k for k, v in segmentation.items()}

    # map the definitions used to define the segmentation
    df_long["sic_2_digit"] = df_long[var_name].map(inv_seg)
    df_long["sic_2_digit"] = df_long["sic_2_digit"].astype(int)

    df_wide = df_long.pivot(index="sic_2_digit", columns=id_vars, values="count")
    df_wide = df_wide.astype("int")

    return df_wide


def extract_geography_code_in_countries(
    col: pd.Series, england: bool = True, wales: bool = True, scotland: bool = True
) -> pd.Series:
    include = ""

    if england:
        include += "E"
    if wales:
        include += "W"
    if scotland:
        include += "S"

    if include == "":
        raise ValueError(f"No countries selected.")

    return col.str.extract(rf"([{include}]\d{{8}})", expand=False)


def convert_bres_2022_lsoa_employment():

    file_path = INPUT_DIR / "BRES2022" / "264980261866694.csv"
    zoning = geographies.LSOA_EW_2011_NAME
    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES["big"]

    # may be able to hard code this if we feel the csv won't change
    tables_and_line_starts = find_contained_tables_and_line_starts(file_path=file_path)

    tables_names_to_wide_df = {}

    for table_type, skiprows in tables_and_line_starts.items():
        wide_df = process_bres_table(
            file_path=file_path,
            skiprows=skiprows,
            zoning=zoning,
            segmentation=segmentation,
        )
        tables_names_to_wide_df[table_type] = wide_df

    for table_type, wide_df in tables_names_to_wide_df.items():
        clean_table_type = table_type.replace(" ", "-")
        save_hdf_with_table_type(
            source_file_path=file_path, df=wide_df, data_type=clean_table_type
        )


def save_hdf_with_table_type(source_file_path: Path, df: pd.DataFrame, data_type: str):
    """Save a dataframe to HDF5 format, in a "preprocessing" subfolder.

    The output file location will be a subfolder in the file_path location named 'preprocessing'
    and the file name will have the same name as the file_path file.

    This is done to help maintain the link between the original input file (e.g. csv or excel)
    and the converted output file.

    Parameters
    ----------
    source_file_path : Path
        File path to the input file that has been read in and converted into the
        DVector-readable format by one of the parsing functions.

    df : pd.DataFrame
        Data to be saved in HDF5 format with the same name as file_path.

    data_type : str
        Name to be appended to the output filename to allow multiple files to be saved to the same folder.
    Returns
    -------

    """
    output_folder = source_file_path.parent / "preprocessing"
    output_folder.mkdir(exist_ok=True)

    output_filename = f"{source_file_path.stem}_{data_type}.hdf"

    logging.info(f"Writing to {output_folder / output_filename}")
    # key kept as df to allow consistency with previous process
    df.to_hdf(output_folder / output_filename, key="df")


def find_contained_tables_and_line_starts(file_path: Path) -> dict[str, int]:
    tables_and_line_starts = {}

    with open(file_path, "r") as f:
        for idx, line in enumerate(f):
            if line.lower().startswith('"employment status:"'):
                _, table_type = line.rstrip().split(",")
                table_type = table_type.replace('"', "").lower()
                skip_rows = idx + 3
                # print(f"{table_type} table starts on index {skip_rows}")
                tables_and_line_starts[table_type] = skip_rows
    return tables_and_line_starts


def process_bres_table(
    file_path: Path, skiprows: int, zoning: str, segmentation: dict[int, str]
) -> pd.DataFrame:
    # feels like this could be a global parameter or derived from an lsoa list
    number_of_lsoas = 34753
    df = pd.read_csv(
        filepath_or_buffer=file_path,
        skiprows=skiprows,
        nrows=number_of_lsoas,
    )

    zoning_col = "2011 super output area - lower layer"

    verify_lsoa_table(df=df, expected_first_col=zoning_col)

    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    df_long = df.melt(id_vars=[zoning_col], var_name="big_full", value_name="people")

    # define dictionary of segmentation mapping
    inv_seg = {v: k for k, v in segmentation.items()}

    # map the definitions used to define the segmentation,
    # drop na to remove any missing values in the dataframe
    df_long["big"] = df_long["big_full"].map(inv_seg)
    df_long["big"] = df_long["big"].astype(int)

    df_long[zoning] = df_long[zoning_col].str.split(" ", expand=True)[0]
    df_wide = df_long.pivot(index="big", columns=[zoning], values="people")

    return df_wide


def verify_lsoa_table(df: pd.DataFrame, expected_first_col: str) -> None:
    """Verifying that table is of the right shape.
    Maybe able to be removed from process if we deem DVector checks sufficient.

    Args:
        df (pd.DataFrame): Table to be checked.
        expected_first_col (str): Check first column is expected (lsoa name)
    """

    act_first_col = df.columns[0]
    verify_table_entries(
        actual_entry=act_first_col,
        expected_entry=expected_first_col,
        entry_type="first column name",
    )
    expected_first_row = "E01000001 : City of London 001A"
    act_first_row = df.iloc[0, 0]
    verify_table_entries(
        actual_entry=str(act_first_row),
        expected_entry=expected_first_row,
        entry_type="first row",
    )

    expected_last_row = "W01001958 : Swansea 025H"
    act_last_row = df.iloc[-1, 0]
    verify_table_entries(
        actual_entry=str(act_last_row),
        expected_entry=expected_last_row,
        entry_type="last_row",
    )


def verify_table_entries(
    actual_entry: str, expected_entry: str, entry_type: str
) -> None:
    """Confirm that a table column name or cell entry matches expectations.

    Args:
        actual_entry (str): The actual entry in the table
        expected_entry (str): Entry that is expected
        entry_type (str): What type of entry is compared, used in the error message if there is one.

    Raises:
        ValueError: If the actual entry does not match the expected entry
    """
    if actual_entry == expected_entry:
        return
    raise ValueError(
        f"Given '{actual_entry}' as {entry_type} but expected '{expected_entry}.'"
    )


if __name__ == "__main__":
    main()
