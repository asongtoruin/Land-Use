from os import PathLike
from pathlib import Path
from typing import Tuple

import land_use.preprocessing as pp
from land_use.constants import geographies, segments
import pandas as pd

# TODO consider sending this to a global config/settings file as shared with reformat population script
INPUT_DIR = Path(r"I:\NorMITs Land Use\2023\import")


# General structure is to repeat the following steps for a series of different data tables
# 1. set file_path, which will be stored in a subdirectory of input_dir
# 2. read file_path using a function from parsers (imported from pp)
# 3. convert using a function (imported from pp)
# 4. write info to hdf in a DVector compatible input format (imported from pp)


def main():
    lad_4_digit()
    msoa_2_digit()
    lsoa_1_digit()


def lad_4_digit():
    filename = "bres_employment22_lad_4digit_sic.csv"
    zoning = geographies.LAD_EW_2021_NAME
    seg_name = "sic_4_digit"
    header_string = "Industry"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, seg_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, encoding="Latin-1"
    )

    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES[seg_name]

    lad_lu = fetch_lad_lu(zoning=zoning)

    df_wide = pp.reformat_2021_lad_4digit(
        df=df,
        lad_lu=lad_lu,
        segmentation=segmentation,
        seg_col=seg_col,
        seg_name=seg_name,
        zoning=zoning,
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def fetch_lad_lu(zoning:str) -> pd.DataFrame:
    """Provide a correspondence between LAD 2021 code and name, for England and Wales.
    Rhondda Cynon Taf is spelled with and without two fs at the end so duplicated here to avoid issues.

    Args:
        zoning (str): Column name to use for the geo code

    Returns:
        pd.DataFrame: Correspondence between the LAD names and geo codes
    """
    lad_lu_file_path = (
        INPUT_DIR / "ONS" / "Correspondence_lists" / "LAD_2021_EW_LADS_BFC.csv"
    )
    lad_lu = pd.read_csv(lad_lu_file_path, usecols=["LAD21CD", "LAD21NM"])
    lad_lu = lad_lu.rename(columns={"LAD21CD": zoning})

    missing_lad = lad_lu[lad_lu["LAD21NM"] == "Rhondda Cynon Taf"].copy()

    missing_lad["LAD21NM"] = "Rhondda Cynon Taff"

    return pd.concat([lad_lu, missing_lad])


def msoa_2_digit():
    filename = "bres_employment22_msoa2011_2digit_sic.csv"
    zoning = geographies.MSOA_EW_2011_NAME
    seg_name = "sic_2_digit"
    header_string = "Area"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, heading_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, low_memory=False
    )

    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES[seg_name]

    df_wide = pp.reformat_xsoa_sic_digits_to_dvector(
        df=df,
        heading_col=heading_col,
        segmentation=segmentation,
        seg_name=seg_name,
        zoning=zoning,
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def lsoa_1_digit():

    filename = "bres_employment22_lsoa2011_1digit_sic.csv"
    zoning = geographies.LSOA_EW_2011_NAME
    seg_name = "sic_1_digit"
    header_string = "Area"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, heading_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, low_memory=False
    )

    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES[seg_name]

    df_wide = pp.reformat_xsoa_sic_digits_to_dvector(
        df=df,
        heading_col=heading_col,
        segmentation=segmentation,
        seg_name=seg_name,
        zoning=zoning,
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


if __name__ == "__main__":
    main()
