from pathlib import Path

import util
import table_functions as tf
from caf.core.data_structures import DVector
from caf.core.segments import Segment
from caf.core.segmentation import Segmentation, SegmentationInput
from caf.core.zoning import ZoningSystem

""" SET UP """
# define defaults
CACHE_FOLDER = Path(r'.\CACHE')
ZONING = 'lsoa2021'
OUTPUT_DIR = Path(r'.\OUTPUTS')

# Create zoning objects for both LSOA and MSOA zone systems
zoning = ZoningSystem.get_zoning(ZONING, search_dir=CACHE_FOLDER)

# define path to tfn inputs
input_path = Path(r'I:\NorMITs Land Use\2023\import\RM002 accom type by household size')

# define path to RM002 tables
no_occupants = input_path / '2072764328175065 zero.csv'
yes_occupants = input_path / '2672385425907310 all.csv'

# define path to ONS table 1
ons_tab_1 = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210212census2021.xlsx')

# AddressBase database
adp = Path(r'I:\NorMITs Land Use\2023\import\ABP\ABP2021\output_results_all_2021(no red).xlsx')

# define dictionary of segmentation for RM002 tables
segmentation = {1: "Whole house or bungalow: Detached",
                2: "Whole house or bungalow: Semi-detached",
                3: "Whole house or bungalow: Terraced",
                4: "Flat, maisonette or apartment",
                5: "A caravan or other mobile or temporary structure"
                }

# define dictionary of segmentation for ONS Table 1
ons_segmentation = {1: "Unshared dwelling: Detached",
                    2: "Unshared dwelling: Semi-detached",
                    3: "Unshared dwelling: Terraced",
                    4: "Unshared dwelling: Flat, maisonette or apartment",
                    5: "Unshared dwelling: A caravan or other mobile or temporary structure"
                    }

# create default segmentation
custom_segmentation = [Segment(name='h', values=segmentation)]
segmentation_input = SegmentationInput(enum_segments=[], naming_order=['h'], custom_segments=custom_segmentation)
model_segmentation = Segmentation(segmentation_input)

""" DATA READING """
# Try reading in data with leading and trailing lines, auto-detecting header row based on this string
header_string = 'output area'

# Read the data and reformat for DVector (this involves auto-detecting the column header row but this is excluded from this example for now)
df = tf.read_rm002(file_path=no_occupants, header_string=header_string, zoning=ZONING, segmentation=segmentation)
unoccupied_properties = DVector(segmentation=model_segmentation, zoning_system=zoning, import_data=df)

# Do the same again for non-empty dwellings and reformat for DVector
df = tf.read_rm002(file_path=yes_occupants, header_string=header_string, zoning=ZONING, segmentation=segmentation)
occupied_properties = DVector(segmentation=model_segmentation, zoning_system=zoning, import_data=df)

# read in excel format, preprocess, and reformat for DVector
df = tf.read_ons_custom(ons_tab_1, zoning=ZONING)
df = tf.convert_ons_table_1(df=df, segmentation=ons_segmentation, zoning=ZONING)
census_population = DVector(segmentation=model_segmentation, zoning_system=zoning, import_data=df)

# read in addressbase data and reformat for DVector
# note this is in the same segmentation as the ONS data
df = tf.read_abp(file_path=adp, zoning=ZONING)
addressbase_dwellings = DVector(segmentation=model_segmentation, zoning_system=zoning, import_data=df)

""" CALCULATIONS """
# Create a total dvec of total number of households based on occupied_properties + unoccupied_properties
all_properties = unoccupied_properties + occupied_properties

# Calculate adjustment factors by zone to get proportion of households occupied by dwelling type by zone
non_empty_proportion = occupied_properties / all_properties
non_empty_proportion.data = non_empty_proportion.data.fillna(0)

# average occupancy for all dwellings
occupancy = (census_population / occupied_properties) * non_empty_proportion
# occ_2 = population / all_properties

# infill missing occupancies with average value of other properties in the LSOA
# i.e. based on column
occupancy.data = occupancy.data.fillna(occupancy.data.mean(axis=0), axis=0)

# multiply occupancy by the addressbase dwellings to get total population by zone
addressbase_population = occupancy * addressbase_dwellings

# save output to hdf
addressbase_population.save(OUTPUT_DIR / 'Output 1.hdf')
util.output_csv(df=addressbase_population.data, output_path=OUTPUT_DIR, file='Output 1.csv', index=True)
