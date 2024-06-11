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
    process_bres_2021_employees_2_digit_sic()
    process_bres_2021_employment_2_digit_sic()
    process_bres_2022_lsoa_employment()


def process_bres_2021_employees_2_digit_sic():
    zoning = geographies.MSOA_EW_2011_NAME

    file_path = (
        INPUT_DIR / "BRES2021" / "Employees" / "bres_employees21_msoa11_2digit.csv"
    )

    df = read_bres_2021_employees_2_digit_sic(file_path=file_path, zoning=zoning)

    # keep only england and wales
    df_wide = convert_bres_2021_employees_2_digit_sic(df=df, zoning=zoning)

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def process_bres_2021_employment_2_digit_sic():
    zoning = geographies.LSOA_EW_2011_NAME

    file_path = (
        INPUT_DIR
        / "BRES2021"
        / "Employment"
        / "bres_employment21_lsoa11_2digit_sic.csv"
    )

    df = read_bres_2021_employment_2_digit_sic(file_path=file_path)

    # filter rows to just lsoas in england and wales
    df_wide = convert_bres_2021_employment_2_digit_sic(df=df, zoning=zoning)

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def process_bres_2022_lsoa_employment():

    file_path = INPUT_DIR / "BRES2022" / "264980261866694.csv"
    zoning = geographies.LSOA_EW_2011_NAME
    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES["big"]

    # may be able to hard code this if we feel the csv won't change
    tables_and_line_starts = find_contained_tables_and_line_starts(file_path=file_path)

    tables_names_to_wide_df = {}

    for table_type, skiprows in tables_and_line_starts.items():
        wide_df = process_individual_bres_2022_table(
            file_path=file_path,
            skiprows=skiprows,
            zoning=zoning,
            segmentation=segmentation,
        )
        tables_names_to_wide_df[table_type] = wide_df

    for table_type, wide_df in tables_names_to_wide_df.items():
        clean_table_type = table_type.replace(" ", "-")
        pp.save_preprocessed_hdf(
            source_file_path=file_path, df=wide_df, multiple_output_ref=clean_table_type
        )


def read_bres_2021_employees_2_digit_sic(file_path: Path, zoning: str) -> pd.DataFrame:
    df = pd.read_csv(file_path, skiprows=8)

    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df = df.drop(columns=["2011 super output area - middle layer"])

    df = df.rename(columns={"mnemonic": zoning})
    df = df.dropna(subset=[zoning])
    return df


def convert_bres_2021_employees_2_digit_sic(
    df: pd.DataFrame, zoning: str
) -> pd.DataFrame:
    df[zoning] = pp.extract_geo_code(df[zoning], scotland=False)
    df = df.dropna(subset=[zoning])

    df_wide = reformat_for_sic_2_digit_output(
        df=df, id_vars=[zoning], var_name="sic_2_digit_description"
    )

    return df_wide


def convert_bres_2021_employment_2_digit_sic(
    df: pd.DataFrame, zoning: str
) -> pd.DataFrame:
    df[zoning] = pp.extract_geo_code(df["Area"], scotland=False)
    df = df.drop(columns=["Area"])
    df = df.dropna(subset=[zoning])

    df_wide = reformat_for_sic_2_digit_output(
        df=df, id_vars=[zoning], var_name="sic_2_digit_description"
    )

    return df_wide


def read_bres_2021_employment_2_digit_sic(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path, skiprows=8, low_memory=False)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    mixed_input_col = (
        "01 : Crop and animal production, hunting and related service activities"
    )
    df[mixed_input_col] = pd.to_numeric(df[mixed_input_col], errors="coerce")
    df = df.dropna(axis=0, subset=[mixed_input_col])
    return df


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


def process_individual_bres_2022_table(
    file_path: Path, skiprows: int, zoning: str, segmentation: dict[int, str]
) -> pd.DataFrame:
    # feels like this could be a global parameter or derived from an lsoa list
    number_of_lsoas = 34753
    df = pd.read_csv(
        filepath_or_buffer=file_path,
        skiprows=skiprows,
        nrows=number_of_lsoas,
    )

    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    zoning_col = "2011 super output area - lower layer"

    df_long = df.melt(id_vars=[zoning_col], var_name="big_full", value_name="people")

    # define dictionary of segmentation mapping
    inv_seg = {v: k for k, v in segmentation.items()}

    # map the definitions used to define the segmentation
    df_long["big"] = df_long["big_full"].map(inv_seg)
    df_long["big"] = df_long["big"].astype(int)

    df_long[zoning] = pp.extract_geo_code(col=df_long[zoning_col], scotland=False)

    df_wide = df_long.pivot(index="big", columns=[zoning], values="people")

    return df_wide


if __name__ == "__main__":
    main()
