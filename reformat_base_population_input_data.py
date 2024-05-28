from pathlib import Path

from caf.core.segmentation import SegmentsSuper

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
        segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ACCOMODATION_TYPE_H
        ).values
    )
    pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** ONS Custom Download Tables

# *** define path to ONS table 1
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210212census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path, zoning=geographies.LSOA_NAME, index_col=0
)
df = pp.convert_ons_table_1(
    df=df, segmentation=pp.ONS_DWELLINGS, zoning=geographies.LSOA_NAME
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** define path to ONS table 2
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210213census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path,
    zoning=geographies.MSOA_NAME,
    index_col=[0, 1, 2],
    header=[0, 1]
)

df = pp.convert_ons_table_2(
    df=df, 
    dwelling_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ACCOMODATION_TYPE_H
        ).values,
    adults_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ADULTS
        ).values,
    children_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.CHILDREN
        ).values,
    car_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.CAR_AVAILABILITY
        ).values,
    zoning=geographies.MSOA_NAME
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** define path to ONS table 4
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210215census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path,
    zoning=geographies.LSOA_NAME,
    index_col=[0, 1]
)

df = pp.convert_ons_table_4(
    df=df,
    dwelling_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ACCOMODATION_TYPE_H
        ).values,
    ns_sec_segmentation=pp.ONS_NSSEC,
    zoning=geographies.LSOA_NAME
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** define path to ONS table 3
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210214census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path,
    zoning=geographies.MSOA_NAME,
    index_col=[0, 1],
    header=[0, 1]
)

dfs = pp.convert_ons_table_3(
    df=df,
    dwelling_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ACCOMODATION_TYPE_H
        ).values,
    ns_sec_segmentation=pp.ONS_NSSEC_ANNOYING,
    all_segmentation=pp.ONS_ECON_EMP_SOC_COMBO,
    zoning=geographies.MSOA_NAME
)
for ref, df in dfs.items():
    pp.save_preprocessed_hdf(source_file_path=file_path, df=df, multiple_output_ref=ref)

# ****** AddressBase 
# *** AddressBase database
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ABP\ABP2021'
    r'\output_results_all_2021(no red).xlsx'
)
# read in addressbase data and reformat for DVector
df = pp.read_abp(file_path=file_path, zoning=geographies.LSOA_NAME)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** MYPE
# *** MYPE database
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\MYPE'
    r'\sapelsoasyoatablefinal.xlsx'
)
# read in mype data and reformat for DVector
df = pp.read_mype(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    age_mapping=SegmentsSuper.get_segment(
            SegmentsSuper.AGE
        ).values,
    gender_mapping=SegmentsSuper.get_segment(
            SegmentsSuper.GENDER
        ).values
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** ONS Data Downloads
# *** ONS age and gender by dwelling type
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS'
    r'\population_hh_age11_gender_MSOA.csv'
)

# read in ons data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=pp.ONS_DWELLING_AGE_SEX_MAPPINGS,
    segment_aggregations={'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS}
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** ONS population in communal establishments
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\TS048  CERs by type'
    r'\2741727163807526.csv'
)

# read in ons data and reformat for DVector
df, zone_col = pp.read_headered_csv(
    file_path=file_path,
    header_string='middle'
)

df = pp.convert_ces_by_type(
    df=df,
    zoning=geographies.MSOA_NAME,
    zoning_column=zone_col,
    ce_type_map=pp.CE_POP_BY_TYPE
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** ONS gender, age, and economic status splits in communal establishments
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS\ONS 2021 CERs'
    r'\CERs_GOR_age11_gender_economicstatus.csv'
)

# read in ons data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.RGN_NAME,
    zoning_column='Regions Code',
    segment_mappings=pp.ONS_ECON_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS,
        'Economic activity status (7 categories)': pp.ECON_6_TO_4_AGGREGATIONS
    }
)

pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** ONS gender, age, and occupation splits in communal establishments
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS\ONS 2021 CERs'
    r'\CERs_GOR_age11_gender_occupation.csv'
)

# read in ons data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.RGN_NAME,
    zoning_column='Regions Code',
    segment_mappings=pp.ONS_OCC_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS,
        'Occupation (current) (10 categories)': pp.SOC_10_TO_4_AGGREGATIONS
    }
)

pp.save_preprocessed_hdf(source_file_path=file_path, df=df)
