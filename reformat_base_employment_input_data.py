from pathlib import Path

from caf.core.segments import SegmentsSuper

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
    find_sic_soc_splits_by_region()
    wfj_2023()
    soc_4_factors()

def lad_4_digit():
    filename = "bres_employment22_lad_4digit_sic.csv"
    zoning = geographies.LAD_NAME
    seg_name = "sic_4_digit"
    header_string = "Industry"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, seg_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, encoding="Latin-1"
    )

    segmentation = SegmentsSuper(seg_name).get_segment().values

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


def fetch_lad_lu(zoning: str) -> pd.DataFrame:
    """Provide a correspondence between LAD 2021 code and name, for England and Wales.
    Rhondda Cynon Taf is spelled with and without two fs at the end so duplicated here to avoid issues.

    Args:
        zoning (str): Column name to use for the geo code

    Returns:
        pd.DataFrame: Correspondence between the LAD names and geo codes
    """
    lad_lu_file_path = (
        INPUT_DIR / "ONS" / "Correspondence_lists" / "LAD2021_CD_NM_EWS.csv"
    )
    lad_lu = pd.read_csv(lad_lu_file_path, usecols=["LAD21CD", "LAD21NM"])
    lad_lu = lad_lu.rename(columns={"LAD21CD": zoning})

    missing_lad = lad_lu[lad_lu["LAD21NM"] == "Rhondda Cynon Taf"].copy()

    missing_lad["LAD21NM"] = "Rhondda Cynon Taff"

    return pd.concat([lad_lu, missing_lad])


def msoa_2_digit():
    filename = "bres_employment22_msoa2011_2digit_sic.csv"
    zoning = geographies.MSOA_2011_NAME
    seg_name = "sic_2_digit"
    header_string = "Area"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, heading_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, low_memory=False
    )

    segmentation = SegmentsSuper(seg_name).get_segment().values

    df_wide = pp.reformat_xsoa_sic_digits_to_dvector(
        df=df,
        heading_col=heading_col,
        segmentation=segmentation,
        seg_name=seg_name,
        zoning=zoning,
    )
    pp.save_preprocessed_hdf(
        source_file_path=file_path, df=df_wide, multiple_output_ref="jobs"
    )

    # now move onto working out proportions for sic_1_digit, sic_2_digit by msoa
    df_sic_1_sic_2 = df_wide.copy().reset_index()

    df_sic_1_sic_2["sic_1_digit"] = df_sic_1_sic_2["sic_2_digit"].map(
        pp.SIC_2_DIGIT_TO_SIC_1_DIGIT_AGGREGATIONS
    )

    df_long = df_sic_1_sic_2.melt(id_vars=["sic_1_digit", "sic_2_digit"])

    df_long["sic_1_to_sic_2_split"] = df_long["value"] / df_long.groupby(
        ["MSOA2011", "sic_1_digit"]
    )["value"].transform("sum")

    # infill nas with 0's note this might be risky
    # if a certain sic_1_digit MSOA category does not have any jobs in this dataset
    # so the splits are all 0% for that combination.
    # But it is then applied to a dataset where there are jobs at the sic_1_digit level
    # Needs to be checked for when applying the split, as it may need more careful infilling possible from LAD (or even GOR) or neighbouring MSOAs
    df_long = df_long.fillna(0)

    df_wide = df_long.pivot(
        index=["sic_1_digit", "sic_2_digit"],
        columns=["MSOA2011"],
        values="sic_1_to_sic_2_split",
    )

    pp.save_preprocessed_hdf(
        source_file_path=file_path, df=df_wide, multiple_output_ref="1digit_sic_splits"
    )


def lsoa_1_digit():

    filename = "bres_employment22_lsoa2011_1digit_sic.csv"
    zoning = geographies.LSOA_2011_NAME
    seg_name = "sic_1_digit"
    header_string = "Area"

    file_path = INPUT_DIR / "BRES2022" / "Employment" / filename

    df, heading_col = pp.read_headered_and_tailed_csv(
        file_path=file_path, header_string=header_string, low_memory=False
    )

    segmentation = SegmentsSuper(seg_name).get_segment().values

    df_wide = pp.reformat_xsoa_sic_digits_to_dvector(
        df=df,
        heading_col=heading_col,
        segmentation=segmentation,
        seg_name=seg_name,
        zoning=zoning,
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def find_sic_soc_splits_by_region():

    filename = "population_region_1sic_soc.csv"

    file_path = INPUT_DIR / "ONS" / "industry_occupation" / filename

    df = pd.read_csv(
        file_path,
        usecols=[
            "Regions Code",
            "Occupation (current) (10 categories) Code",
            "Industry (current) (19 categories) Code",
            "Observation",
        ],
    )

    df_wide = pp.reformat_ons_sic_soc_correspondence(df=df)

    # Infill Scotland (S92000003) based on NE (E12000001) + North-West (E12000002) + Yorkshire & Humber (E12000003)
    # As we are only using proportions then the totals do not matter
    df_wide["S92000003"] = df_wide["E12000001"] + df_wide["E12000002"] + df_wide["E12000003"]

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def wfj_2023():
    file_path = INPUT_DIR / "BRES2022" / "Employment" / "Employment investigation" / "WFJ.xlsx"
    # can't use pp.read_headered_and_tailed_csv as we want the second table in the sheet (Sept 2023) which is same column headings as the first (Sept 2022)
    df = pd.read_excel(file_path, skiprows=31, usecols=["region", "total workforce jobs"])

    # Rename East to East of England to make it clearer and match lookup
    df.loc[df["region"] == "East", "region"] = "East of England"

    # attach codes for the regions
    gor_lu_file_path = INPUT_DIR / "ONS" / "Correspondence_lists" / "GOR2021_CD_NM_EWS.csv"
    gor_lu = pd.read_csv(gor_lu_file_path, usecols=["RGN21CD", "RGN21NM"])
    gor_lu = gor_lu.rename(columns={"RGN21NM": "region"})

    df_with_codes = pd.merge(df, gor_lu, how="left", on=["region"])
    df_with_codes = df_with_codes.dropna(subset=["RGN21CD"])

    # Add in a total segementation. Note that this needs to be a number i.e. 1 here.
    df_with_codes["total"] = 1

    # Copy from utlities, will don from there when fixed the import issue
    # pp.reformat_2021_lad_4digit
    df_wide = pp.pivot_to_dvector(
        data=df_with_codes,
        zoning_column="RGN21CD",
        index_cols=["total"],
        value_column="total workforce jobs",
    )

    pp.save_preprocessed_hdf(source_file_path=file_path, df=df_wide)


def soc_4_factors():
    soc_4_path = INPUT_DIR / "SOC" / "Table 8 WFJ-adjusted Land Use SOC4.csv"

    soc_4 = pd.read_csv(soc_4_path, usecols=["Regions", "WFJ-adjusted SOC 4%"])

    soc_4 = soc_4.rename(columns={"WFJ-adjusted SOC 4%": "soc_4_perc"})

    soc_4["soc_4_perc"] = soc_4["soc_4_perc"].str.replace("%", "").astype(float)

    soc_4["soc_4_factor"] = soc_4["soc_4_perc"] / (100 - soc_4["soc_4_perc"])

    soc_4["total"] = 1

    # update reference to match lu
    soc_4["Regions"] = soc_4["Regions"].str.replace(
        "Yorkshire and the Humber", "Yorkshire and The Humber"
    )

    # attach codes for the regions
    gor_lu_file_path = (
        INPUT_DIR / "ONS" / "Correspondence_lists" / "GOR2021_CD_NM_EWS.csv"
    )
    gor_lu = pd.read_csv(gor_lu_file_path, usecols=["RGN21CD", "RGN21NM"])
    gor_lu = gor_lu.rename(columns={"RGN21NM": "Regions"})

    df_with_codes = pd.merge(soc_4, gor_lu, how="left", on=["Regions"])
    df_with_codes = df_with_codes.dropna(subset=["RGN21CD"])

    df_wide = df_with_codes.pivot(
        index=["total"], columns=["RGN21CD"], values="soc_4_factor"
    )

    pp.save_preprocessed_hdf(source_file_path=soc_4_path, df=df_wide)


if __name__ == "__main__":
    main()
