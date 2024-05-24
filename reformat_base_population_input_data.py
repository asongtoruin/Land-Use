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

# *** define path to ONS table 1
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210212census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path, zoning=geographies.LSOA_NAME, index_col=0
)
df = pp.convert_ons_table_1(
    df=df, segmentation=ons_segmentation, zoning=geographies.LSOA_NAME
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
    dwelling_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['h'],
    adults_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['ha'],
    children_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['hc'],
    car_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['car'],
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

ons_segmentation = {
    1: "NS-SeC of HRP: 1. Higher managerial, administrative and professional occupations; 2. Lower managerial, administrative and professional occupations",
    2: "NS-SeC of HRP: 3. Intermediate occupations; 4. Small employers and own account workers; 5. Lower supervisory and technical occupations",
    3: "NS-SeC of HRP: 6. Semi-routine occupations; 7. Routine occupations",
    4: "NS-SeC of HRP: 8. Never worked or long-term unemployed*",
    5: "NS-SeC of HRP: L15: Full-time student"
}

df = pp.convert_ons_table_4(
    df=df,
    dwelling_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['h'],
    ns_sec_segmentation=ons_segmentation,
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

ns_sec_segmentation = {
    1: "NS-SeC of HRP: 1. Higher managerial, administrative and professional occupations; 2. Lower managerial, administrative and professional occupations",
    2: "NS-SeC of HRP: 3. Intermediate occupations; 4. Small employers and own account workers; 5. Lower supervisory and technical occupations",
    3: "NS-SeC of HRP: 6. Semi-routine occupations; 7. Routine occupations",
    4: "NS-SeC of HRP: 8. Never worked and long-term unemployed*",
    5: "NS-SeC of HRP: L15: Full-time student"
}

# order of list is pop_econ, pop_emp, pop_soc
all_segmentation = {'Economically active (excluding full-time students): In employment: part-time: Occupation: 1. Managers, '
                    'directors and senior officials; 2. Professional occupations; 3. Associate professional and technical occupations': {'pop_econ': 1, 'pop_emp': 2, 'pop_soc': 1},
                    'Economically active (excluding full-time students): In employment: part-time: Occupation: 4. Administrative and secretarial occupations; '
                    '5. Skilled trades occupations; 6. Caring, leisure and other service occupations; 7. Sales and customer service occupations': {'pop_econ': 1, 'pop_emp': 2, 'pop_soc': 2},
                    'Economically active (excluding full-time students): In employment: part-time: Occupation: 8. Process, plant and machine operatives; '
                    '9. Elementary occupations': {'pop_econ': 1, 'pop_emp': 2, 'pop_soc': 3},
                    'Economically active (excluding full-time students): In employment: full-time: Occupation: 1. Managers, '
                    'directors and senior officials; 2. Professional occupations; 3. Associate professional and technical occupations': {'pop_econ': 1, 'pop_emp': 1, 'pop_soc': 1},
                    'Economically active (excluding full-time students): In employment: full-time: Occupation: 4. Administrative and secretarial occupations; '
                    '5. Skilled trades occupations; 6. Caring, leisure and other service occupations; 7. Sales and customer service occupations': {'pop_econ': 1, 'pop_emp': 1, 'pop_soc': 2},
                    'Economically active (excluding full-time students): In employment: full-time: Occupation: 8. Process, plant and machine operatives; '
                    '9. Elementary occupations': {'pop_econ': 1, 'pop_emp': 1, 'pop_soc': 3},
                    'Economically active (excluding full-time students): Unemployed': {'pop_econ': 2, 'pop_emp': 3, 'pop_soc': 4},
                    'Economically inactive: Retired': {'pop_econ': 3, 'pop_emp': 5, 'pop_soc': 4},
                    'Full-time students': {'pop_econ': 4, 'pop_emp': 4, 'pop_soc': 4},
                    'Economically inactive: Other': {'pop_econ': 3, 'pop_emp': 3, 'pop_soc': 4}}

dfs = pp.convert_ons_table_3(
    df=df,
    dwelling_segmentation=segments._CUSTOM_SEGMENT_CATEGORIES['h'],
    ns_sec_segmentation=ns_sec_segmentation,
    all_segmentation=all_segmentation,
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
    age_mapping=segments._CUSTOM_SEGMENT_CATEGORIES['age'],
    gender_mapping=segments._CUSTOM_SEGMENT_CATEGORIES['gender']
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# ****** ONS Data Downloads
# *** ONS age and gender by dwelling type
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS\population_hh_age5_gender_MSOA'
    r'\MSOA_age5_gender_hh_EnglandWales.csv'
)

# define dictionary of columns in the input data and segments to map to
segment_mappings = {
    'Accommodation type (5 categories)': ['h', segments._CUSTOM_SEGMENT_CATEGORIES['h']],
    'Age (5 categories)': ['agg_age', segments._CUSTOM_SEGMENT_CATEGORIES['agg_age']],
    'Sex (2 categories)': ['gender', segments._CUSTOM_SEGMENT_CATEGORIES['gender']]
}

# read in ons data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=segment_mappings
)
pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** ONS age and gender by dwelling type
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS'
    r'\population_hh_age11_gender_MSOA.csv'
)

# define dictionary of columns in the input data and segments to map to
segment_mappings = {
    'Age (11 categories)': ['age', segments._CUSTOM_SEGMENT_CATEGORIES['age']],
    'Sex (2 categories)': ['gender', segments._CUSTOM_SEGMENT_CATEGORIES['gender']],
    'Accommodation type (5 categories)': ['h', segments._CUSTOM_SEGMENT_CATEGORIES['h']]
}

aggregations = {'Age (11 categories)': {
    'Aged 4 years and under': '0 to 4 years',
    'Aged 5 to 9 years': '5 to 9 years',
    'Aged 10 to 15 years': '10 to 15 years',
    'Aged 16 to 19 years': '16 to 19 years',
    'Aged 20 to 24 years': '20 to 34 years',
    'Aged 25 to 34 years': '20 to 34 years',
    'Aged 35 to 49 years': '35 to 49 years',
    'Aged 50 to 64 years': '50 to 64 years',
    'Aged 65 to 74 years': '65 to 74 years',
    'Aged 75 to 84 years': '75+ years',
    'Aged 85 years and over': '75+ years'}}

# read in ons data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=segment_mappings,
    segment_aggregations=aggregations
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
