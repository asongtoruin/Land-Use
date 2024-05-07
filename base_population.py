from pathlib import Path

import yaml

import land_use.data_processing as dp
import land_use.constants as cn


# load configuration file
with open(r'scenario_configurations\iteration_5\base_population_config.yml', 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])
OUTPUT_DIR.mkdir(exist_ok=True)

# read in the data to calculate average occupancy, accounting for unoccupied households
occupied_households = dp.read_dvector_data(**config['occupied_households'])
unoccupied_households = dp.read_dvector_data(**config['unoccupied_households'])
census_population = dp.read_dvector_data(**config['census_population'])
addressbase_dwellings = dp.read_dvector_data(**config['addressbase_dwellings'])
# TODO dont like this naming convention, what do we want to do?
hhs_by_h_hc_ha_car = dp.read_dvector_data(**config['census_hhs_by_hh_adult_car_children'])
mype_2022 = dp.read_dvector_data(**config['mype_2022'])

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
output = dp.dvector_to_long(dvec=addressbase_population, value_name='population')
output.to_csv(OUTPUT_DIR / 'Output 1.csv', index=False)

# calculate splits of households with or without children and by car availability by
# dwelling type and number of adults by MSOA
total_hh_by_hh = hhs_by_h_hc_ha_car.aggregate(segs=['h'])
proportion_hhs_by_h_hc_ha_car = hhs_by_h_hc_ha_car / total_hh_by_hh

# convert the MSOA based factors to LSOAs (duplicate MSOA factor for relevant LSOAs)
proportions_by_lsoa = proportion_hhs_by_h_hc_ha_car.translate_zoning(
    new_zoning=cn.LSOA_ZONING_SYSTEM,
    cache_path=cn.CACHE_FOLDER
)

# multiply the total population by the derived proportions at LSOA level
# TODO here we're applying household based proportions to population, inconsistent?
abpop_by_h_hc_ha_car = addressbase_population * proportions_by_lsoa

# TODO Control to 2022 age and gender distributions for full output 2. Need a better way to aggregate a DVector based on no segmentation (i.e. just total by zone, no segmentation)
# calculate age and gender factors by LSOA
# total_lsoa_population = mype_2022.data.sum().sum()
# age_gender_proportions = mype_2022.data.div(total_lsoa_population)

# TODO Segmentation doesn't match - need to re-segment the abpop data to have at least overlapping segments with age and gender. Needs a bit of thinking.
# multiply population by age / gender constraints
# this fails!
# test = abpop_by_h_hc_ha_car * age_gender_proportions

# save output to hdf and csv for readablity
abpop_by_h_hc_ha_car.save(OUTPUT_DIR / 'Output 2.hdf')
output = dp.dvector_to_long(dvec=abpop_by_h_hc_ha_car, value_name='population')
output.to_csv(OUTPUT_DIR / 'Output 2.csv', index=False)

