from pathlib import Path

from caf.core.segmentation import SegmentsSuper
import pandas as pd

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

# *** ONS table 3
# ONS age by MSOA
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS'
    r'\population_age11_MSOA.csv'
)

# read in ons data and reformat for DVector
ages = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=pp.ONS_AGE_11_MAPPING,
    segment_aggregations={'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS}
)

# calculate adjustment factors by MSOA to deflate the economically inactive
# population to ignore 75+ (these will get added in later)
# TODO tidy this up
ages.loc['factor'] = ages.loc[9] / (ages.loc[4] + ages.loc[5] +
                                    ages.loc[6] + ages.loc[7] +
                                    ages.loc[8])
ages = pd.melt(
    ages.loc[['factor']],
    var_name=geographies.MSOA_NAME,
    value_name='factor'
)

# ONS economic status by MSOA
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\ONS'
    r'\population_economicstatus_MSOA.csv'
)

# read in ons data and reformat for DVector
economic_status = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=pp.ONS_ECON_MAPPING
)

# calculate adjustment factors by MSOA to split students into
# economically active student in employment, economically active
# student not in employment, and economically inactive students
# TODO tidy this up
economic_status.loc['student_total'] = (
        economic_status.loc[3] + economic_status.loc[4] + economic_status.loc[6]
)
economic_status.loc['students_employed'] = (
        economic_status.loc[3] / economic_status.loc['student_total']
)
economic_status.loc['students_unemployed'] = (
        economic_status.loc[4] / economic_status.loc['student_total']
)
economic_status.loc['students_inactive'] = (
        economic_status.loc[6] / economic_status.loc['student_total']
)
economic_status = pd.melt(
    economic_status.loc[['students_employed', 'students_unemployed', 'students_inactive']].reset_index(),
    id_vars=['econ'],
    var_name=geographies.MSOA_NAME,
    value_name='factor'
)

# define path to ONS table 3
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210214census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(
    file_path,
    zoning=geographies.MSOA_NAME,
    index_col=[0, 1],
    header=[0, 1]
)

df = pp.convert_ons_table_3(
    df=df,
    dwelling_segmentation=SegmentsSuper.get_segment(
            SegmentsSuper.ACCOMODATION_TYPE_H
        ).values,
    ns_sec_segmentation=pp.ONS_NSSEC_ANNOYING,
    all_segmentation=pp.ONS_ECON_EMP_SOC_COMBO,
    zoning=geographies.MSOA_NAME,
    ages=ages,
    economic_status=economic_status
)

# get dataframe of non-working data
missing = df.copy().reset_index()
missing['soc'] = 4
missing['pop_emp'] = 5
missing['economic_status'] = -8
missing = missing.drop_duplicates(
    subset=['accom_h', 'ns_sec', 'economic_status', 'pop_emp', 'soc']
).set_index(
    ['accom_h', 'ns_sec', 'economic_status', 'pop_emp', 'soc']
)
missing.iloc[:] = 1

# combine into a single output
combo = pd.concat([df, missing])

pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=combo,
    multiple_output_ref='-8_modified'
)

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

# duplicate the pop_econ values to be consistent with the economic_status segmentation
df = df.reset_index()
mapping = {
    1: 1,
    2: 2,
    3: 6,
    4: 3
}
df['economic_status'] = df['pop_econ'].map(mapping)
category_4 = df.loc[df['economic_status'] == 3]
category_5 = df.loc[df['economic_status'] == 3]
category_4['economic_status'] = 4
category_5['economic_status'] = 5
output = pd.concat([df, category_4, category_5])

# add in the -8 economic status category
children_age9 = [1, 2, 3]
children = output.loc[
    (output['pop_econ'] == 3) &
    (output['age_9'].isin(children_age9))
]
children['economic_status'] = -8
non_children = output.loc[
    ~(output['age_9'].isin(children_age9))
]
combo = pd.concat([children, non_children])

# set the index and save
dvec = combo.set_index(
    ['age_9', 'g', 'economic_status']
).drop(columns=['pop_econ'])
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=dvec,
    multiple_output_ref='-8_modified'
)

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

# drop exlusions
# TODO Is there a way to do this better / using the SegmentSuper exclusion definitions?
df = df.loc[
    ~((df.index.isin([1, 2, 3, 9], level='age_9')) & (df.index.isin([1, 2, 3], level='soc')))
]

pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** ONS population in communal establishments by LSOA
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\TS001 pop_hh_ce'
    r'\1226171533660024.csv'
)

# read in ons data and reformat for DVector
df, zone_col = pp.read_headered_csv(
    file_path=file_path,
    header_string='super'
)

df = pp.convert_ces(
    df=df,
    zoning=geographies.LSOA_NAME,
    zoning_column=zone_col
)

pp.save_preprocessed_hdf(source_file_path=file_path, df=df)

# *** scotland population data
file_path = Path(
    r'I:\NorMITs Land Use\2023\import\Census Scotland'
    r'\Population_age6_gender_DZ2011.csv'
)

df, _ = pp.read_headered_csv(
    file_path=file_path,
    header_string='Summation Options'
)
# TODO this data source has every column name written in a line before the data starts!
# TODO currently finding a previous line and then shifting the data up one, but probably needs sorting
# set column names to be first row
df.columns = df.iloc[0]
# drop first row (of old column names)
df = df[1:]

df = pp.convert_scotland(
    df=df,
    zoning=geographies.SCOTLAND_NAME,
    zoning_column='Intermediate Zone - Data Zone 2011',
    age_segmentation={i: j for j, i in segments._CUSTOM_SEGMENT_CATEGORIES['scot_age'].items()},
    gender_segmentation={i: j for j, i in SegmentsSuper.GENDER.get_segment().values.items()}
)

pp.save_preprocessed_hdf(source_file_path=file_path, df=df)
