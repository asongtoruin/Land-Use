from pathlib import Path

from caf.core.segmentation import SegmentsSuper
import pandas as pd

import land_use.preprocessing as pp
from land_use.constants import geographies, segments

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
file_path = data_path / 'households_cars_lsoa.csv'
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
