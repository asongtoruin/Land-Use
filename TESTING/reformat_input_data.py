from pathlib import Path

import preprocessing as pp
from constants import geographies


""" RM002 Tables """
# Try reading in data with leading and trailing lines, auto-detecting header row based on this string
header_string = 'output area'

# define dictionary of segmentation for RM002 tables
segmentation = {1: "Whole house or bungalow: Detached",
                2: "Whole house or bungalow: Semi-detached",
                3: "Whole house or bungalow: Terraced",
                4: "Flat, maisonette or apartment",
                5: "A caravan or other mobile or temporary structure"
                }
# Define input path to file
file_path = Path(r'I:\NorMITs Land Use\2023\import\RM002 accom type by household size\2072764328175065 zero.csv')
# Read the data and reformat for DVector (this involves auto-detecting the column header row but this is excluded from this example for now)
df = pp.read_rm002(file_path=file_path, header_string=header_string, zoning=geographies.MODEL_ZONING, segmentation=segmentation)
pp.save_to_hdf(file_path=file_path, df=df)

# Define input path to file
file_path = Path(r'I:\NorMITs Land Use\2023\import\RM002 accom type by household size\2672385425907310 all.csv')
# Read the data and reformat for DVector (this involves auto-detecting the column header row but this is excluded from this example for now)
df = pp.read_rm002(file_path=file_path, header_string=header_string, zoning=geographies.MODEL_ZONING, segmentation=segmentation)
pp.save_to_hdf(file_path=file_path, df=df)

""" ONS Custom Download Tables """
# define dictionary of segmentation for ONS Table 1
ons_segmentation = {1: "Unshared dwelling: Detached",
                    2: "Unshared dwelling: Semi-detached",
                    3: "Unshared dwelling: Terraced",
                    4: "Unshared dwelling: Flat, maisonette or apartment",
                    5: "Unshared dwelling: A caravan or other mobile or temporary structure"
                    }
# define path to ONS table 1
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210212census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(file_path, zoning=geographies.MODEL_ZONING, index_col=0)
df = pp.convert_ons_table_1(df=df, segmentation=ons_segmentation, zoning=geographies.MODEL_ZONING)
pp.save_to_hdf(file_path=file_path, df=df)

# define dictionaries of segmentation for ONS Table 2
ons_table_2_dwelling_seg = {1: 'Whole house or bungalow: Detached',
                            2: 'Whole house or bungalow: Semi-detached',
                            3: 'Whole house or bungalow: Terraced',
                            4: 'Flat, maisonette or apartment'
                            }

ons_table_2_adults_seg = {1: 'No adults or 1 adult in household',
                          2: '2 adults in household',
                          3: '3 or more adults in household',
                          }

ons_table_2_children_seg = {1: 'Household with no children or all children non-dependent',
                            2: 'Household with one or more dependent children',
                            }

ons_table_2_car_seg = {1: 'No cars or vans in household',
                       2: '1 car or van in household',
                       3: '2 or more cars or vans in household',
                       }
# define path to ONS table 2
file_path = Path(r'I:\NorMITs Land Use\2023\import\ONS custom\ct210213census2021.xlsx')
# read in excel format, preprocess, and reformat for DVector
df = pp.read_ons_custom(file_path, zoning=geographies.MSOA, index_col=[0, 1, 2], header=[0, 1])
df = pp.convert_ons_table_2(df=df,
                            dwelling_segmentation=ons_table_2_dwelling_seg,
                            adults_segmentation=ons_table_2_adults_seg,
                            children_segmentation=ons_table_2_children_seg,
                            car_segmentation=ons_table_2_car_seg,
                            zoning=geographies.MSOA)
pp.save_to_hdf(file_path=file_path, df=df)

""" AddressBase """
# AddressBase database
file_path = Path(r'I:\NorMITs Land Use\2023\import\ABP\ABP2021\output_results_all_2021(no red).xlsx')
# read in addressbase data and reformat for DVector
df = pp.read_abp(file_path=file_path, zoning=geographies.MODEL_ZONING)
pp.save_to_hdf(file_path=file_path, df=df)
