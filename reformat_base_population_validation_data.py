from pathlib import Path

import land_use.preprocessing as pp
from land_use.constants import geographies

# Main input path for verification datasets
data_path = Path(r'I:\NorMITs Land Use\2023\import\ONS-validation')

# ****** Mono-variate datasets
# *** population by SOC9 and LSOA
file_path = data_path / 'population_soc9_lsoa.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_OCC_MAPPINGS,
    segment_aggregations={
        'Occupation (current) (10 categories)': pp.SOC_10_TO_4_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='soc-aggregated'
)

# *** population by economic status and LSOA
file_path = data_path / 'population_status_lsoa.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_ECONOMIC_STATUS_MAPPING
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df
)

# *** households by number of cars and LSOA
file_path = data_path / 'households_cars_lsoa_3.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_CAR_MAPPING
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df
)

# *** households by ns-sec and LSOA
file_path = data_path / 'households_nssec_lsoa.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_NSSEC_MAPPINGS,
    segment_aggregations={
        'National Statistics Socio-economic Classification (NS-SeC) (10 categories)':
            pp.ONS_NSSEC_10_TO_5_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df
)

# ****** Multi-variate datasets
# *** households by number of adults and number of children and LSOA
file_path = data_path / 'households_adults_children_lsoa.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_ADULT_CHILDREN_MAPPINGS,
    segment_aggregations={
        'Adults and children in household (11 categories)':
            pp.ONS_CHILDREN_AGGREGATIONS,
        'Adults and children in household (11 categories) Code':
            pp.ONS_ADULT_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='adults-children-aggregated'
)

# *** households by nssec and number of adults and number of children and LAD
file_path = data_path / 'households_nssec_adults_children_lad.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LAD_NAME,
    zoning_column='Lower tier local authorities Code',
    segment_mappings=pp.ONS_ADULT_CHILDREN_NSSEC_MAPPINGS,
    segment_aggregations={
        'Adults and children in household (11 categories)':
            pp.ONS_CHILDREN_AGGREGATIONS,
        'Adults and children in household (11 categories) Code':
            pp.ONS_ADULT_AGGREGATIONS,
        'National Statistics Socio-economic Classification (NS-SeC) (10 categories)':
            pp.ONS_NSSEC_10_TO_5_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='adults-children-nssec-aggregated'
)

# *** households by nssec and number of cars and LSOA
file_path = data_path / 'households_nssec_car_lad.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LAD_NAME,
    zoning_column='Lower tier local authorities Code',
    segment_mappings=pp.ONS_CAR_NSSEC_MAPPINGS,
    segment_aggregations={
        'National Statistics Socio-economic Classification (NS-SeC) (10 categories)':
            pp.ONS_NSSEC_10_TO_5_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='nssec-aggregated'
)

# *** population by age, gender, and occupation and MSOA
file_path = data_path / 'population_age11_gender_occupation_msoa_4missing.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=pp.ONS_OCC_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS,
        'Occupation (current) (10 categories)': pp.SOC_10_TO_4_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='age-occ-aggregated'
)

# *** population by age, gender, and occupation and MSOA
file_path = data_path / 'population_age11_gender_occupation_lad.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LAD_NAME,
    zoning_column='Lower tier local authorities Code',
    segment_mappings=pp.ONS_OCC_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS,
        'Occupation (current) (10 categories)': pp.SOC_10_TO_4_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='age-occ-aggregated'
)

# *** population by age, gender, and economic status and MSOA
file_path = data_path / 'population_age11_gender_economicstatus_msoa_1missing.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.MSOA_NAME,
    zoning_column='Middle layer Super Output Areas Code',
    segment_mappings=pp.ONS_ECON_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS,
        'Economic activity status (7 categories)': pp.ECON_6_TO_4_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='age-econ-aggregated'
)

# *** population by age, gender, and economic status and region
file_path = data_path / 'population_age11_gender_economicstatus_region.csv'
# read in data and reformat for DVector
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
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='age-econ-aggregated'
)

# *** population by age, gender, and LSOA
file_path = data_path / 'population_age11_gender_LSOA.csv'
# read in data and reformat for DVector
df = pp.read_ons(
    file_path=file_path,
    zoning=geographies.LSOA_NAME,
    zoning_column='Lower layer Super Output Areas Code',
    segment_mappings=pp.ONS_AGE_SEX_MAPPINGS,
    segment_aggregations={
        'Age (11 categories)': pp.AGE_11_TO_9_AGGREGATIONS
    }
)
pp.save_preprocessed_hdf(
    source_file_path=file_path,
    df=df,
    multiple_output_ref='age-aggregated'
)
