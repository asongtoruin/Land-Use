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
    convert_bres_2022_lsoa_employment()


def convert_bres_2022_lsoa_employment():

    file_path = INPUT_DIR / "BRES2022" / "264980261866694.csv"
    zoning = geographies.LSOA_NAME
    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES["big"]

    print(file_path)

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
        print(table_type)
        # TODO: discuss how to save this as have multiple dataframes we wish to save
        # option here is to separate by key
        clean_key = table_type.replace(" ", "_").replace("-", "_")
        save_multi_key_preprocessed_hdf(
            source_file_path=file_path, df=wide_df, key=clean_key
        )


def save_multi_key_preprocessed_hdf(
    source_file_path: Path, df: pd.DataFrame, key: str = "df"
):
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

    key : str, default 'df'
        group identifier name to separate the df from other dfs stored in the same place.
        Defaults to df which gives the same behaviour as save_preprocessed_hdf

    Returns
    -------

    """
    output_folder = source_file_path.parent / "preprocessing"
    output_folder.mkdir(exist_ok=True)

    filename = source_file_path.with_suffix(".hdf").name
    logging.info(f"Writing to {output_folder / filename}")
    # TODO: appending to file with new key, check that it overwrites info for key if key is already present
    df.to_hdf(output_folder / filename, key=key, mode="a")


def find_contained_tables_and_line_starts(file_path: Path) -> dict[str, int]:
    tables_and_line_starts = {}

    with open(file_path, "r") as f:
        for idx, line in enumerate(f):
            if line.lower().startswith('"employment status:"'):
                _, table_type = line.rstrip().split(",")
                table_type = table_type.replace('"', "").lower()
                skip_rows = idx + 3
                print(f"{table_type} table starts on index {skip_rows}")
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

    # df_long["big"] = df_long["big_full"].str.split(" ", expand=True)[0]

    # TODO: not sure this line is required if the dictionary is int already but included just in case
    df_long["big"] = df_long["big"].astype(int)

    # TODO consider if big is the variable we want this stored in or if there is a more

    df_long[zoning] = df_long[zoning_col].str.split(" ", expand=True)[0]
    df_wide = df_long.pivot(index="big", columns=[zoning], values="people")

    print(df_wide)

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
