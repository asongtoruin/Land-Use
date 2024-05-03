from pathlib import Path
import yaml

import data_processing as dp
import constants as cn
import util

# load configuration file
with open(r'scenario_configurations\iteration_5\base_population_config.yml', 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])

# read in the data to calculate average occupancy, accounting for unoccupied households
occupied_households = dp.read_dvector_data(**config['occupied_households'])
unoccupied_households = dp.read_dvector_data(**config['unoccupied_households'])
census_population = dp.read_dvector_data(**config['census_population'])
addressbase_dwellings = dp.read_dvector_data(**config['addressbase_dwellings'])
# TODO dont like this naming convention, what do we want to do?
census_hhs_by_hh_adult_car_children = dp.read_dvector_data(**config['census_hhs_by_hh_adult_car_children'])

# Create a total dvec of total number of households based on occupied_properties + unoccupied_properties
all_properties = unoccupied_households + occupied_households

# Calculate adjustment factors by zone to get proportion of households occupied by dwelling type by zone
non_empty_proportion = occupied_households / all_properties
non_empty_proportion.data = non_empty_proportion.data.fillna(0)

# average occupancy for all dwellings
occupancy = (census_population / occupied_households) * non_empty_proportion
# occ_2 = population / all_properties

# infill missing occupancies with average value of other properties in the LSOA
# i.e. based on column
occupancy.data = occupancy.data.fillna(occupancy.data.mean(axis=0), axis=0)

# multiply occupancy by the addressbase dwellings to get total population by zone
addressbase_population = occupancy * addressbase_dwellings

# save output to hdf and csv for readablity
addressbase_population.save(OUTPUT_DIR / 'Output 1.hdf')
output = dp.dvector_to_dataframe(dvec=addressbase_population, value_name='population')
util.output_csv(df=output, output_path=OUTPUT_DIR, file='Output 1.csv', index=False)

# calculate splits of households with or without children and by car availability by
# dwelling type and number of adults by MSOA
total_hh_by_hh = census_hhs_by_hh_adult_car_children.aggregate(segs=['h'])
proportion_hh_by_hh_and_adults_by_car_and_child = census_hhs_by_hh_adult_car_children / total_hh_by_hh

# convert the MSOA based factors to LSOAs (duplicate MSOA factor for relevant LSOAs)
# TODO Need to set up an existing translation to stop it being run everytime
proportions_by_lsoa = proportion_hh_by_hh_and_adults_by_car_and_child.translate_zoning(new_zoning=cn.MODEL_ZONING_SYSTEM,
                                                                                       cache_path=cn.CACHE_FOLDER)
# multiply the total population by the derived proportions at LSOA level
# TODO here we're applying household based proportions to population, inconsistent?
abpop_by_hh_and_adults_by_car_and_child = addressbase_population * proportions_by_lsoa

# TODO Read in and control to 2022 age and gender distributions for full output 2
# save output to hdf and csv for readablity
abpop_by_hh_and_adults_by_car_and_child.save(OUTPUT_DIR / 'Output 2.hdf')
output = dp.dvector_to_dataframe(dvec=abpop_by_hh_and_adults_by_car_and_child, value_name='population')
util.output_csv(df=output, output_path=OUTPUT_DIR, file='Output 2.csv', index=False)
