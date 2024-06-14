from pathlib import Path

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
    process_bres_2022_lsoa_employment()


def process_bres_2022_lsoa_employment():

    file_path = INPUT_DIR / "BRES2022" / "264980261866694.csv"
    zoning = geographies.LSOA_EW_2011_NAME
    segmentation = segments._CUSTOM_SEGMENT_CATEGORIES["big"]

    # may be able to hard code this if we feel the csv won't change
    tables_and_line_starts = pp.find_contained_tables_and_line_starts(
        file_path=file_path
    )

    tables_names_to_wide_df = {}

    for table_type, skiprows in tables_and_line_starts.items():
        wide_df = pp.process_individual_bres_2022_table(
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


if __name__ == "__main__":
    main()
