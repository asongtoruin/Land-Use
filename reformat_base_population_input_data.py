from pathlib import Path

import land_use.preprocessing as pp
from land_use.constants import geographies, segments

# ****** RM002 Tables
# Try reading in data with leading and trailing lines, auto-detecting header row based on this string
header_string = 'output area'

# Two files to read, but the same processing
rm_002_folder = Path(r'I:\NorMITs Land Use\2023\import\RM002 accom type by household size')

for file_name in ('2072764328175065 zero.csv', '2672385425907310 all.csv'):
    file_path = rm_002_folder / file_name
    # Read the data and reformat for DVector (this involves auto-detecting the column header row but this is excluded from this example for now)
    df = pp.read_rm002(
        file_path=file_path, header_string=header_string, 
        zoning=geographies.LSOA_NAME, 
        segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['h']
    )
    pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** ONS Custom Download Tables
# define dictionary of segmentation for ONS Table 1
# TODO: This is very similar to "h". Could we standardise?
ons_segmentation = {
    1: "Unshared dwelling: Detached",
    2: "Unshared dwelling: Semi-detached",
    3: "Unshared dwelling: Terraced",
    4: "Unshared dwelling: Flat, maisonette or apartment",
    5: "Unshared dwelling: A caravan or other mobile or temporary structure"
}

# define path to ONS table 1
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210212census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path, zoning=geographies.LSOA_NAME, index_col=0
)
df = pp.convert_ons_table_1(
    df=df, segmentation=ons_segmentation, zoning=geographies.LSOA_NAME
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# define path to ONS table 2
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210213census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path, zoning=geographies.MSOA_NAME, 
    index_col=[0, 1, 2], header=[0, 1]
)

df = pp.convert_ons_table_2(
    df=df, 
    dwelling_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['hr'],
    adults_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['ha'],
    children_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['hc'],
    car_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['car'],
    zoning=geographies.MSOA_NAME
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** AddressBase 
# AddressBase database
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ABP\ABP2021'
    r'\output_results_all_2021(no red).xlsx'
)
# read in addressbase data and reformat for DVector
df = pp.read_abp(file_path=file_path, zoning=geographies.LSOA_NAME)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)
