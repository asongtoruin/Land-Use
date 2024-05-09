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

# read in the data from the config file
occupied_households = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['occupied_households'])
unoccupied_households = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['unoccupied_households'])
ons_table_1 = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_1'])
addressbase_dwellings = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['addressbase_dwellings'])
ons_table_2 = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_2'])
mype_2022 = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['mype_2022'])
ons_table_4 = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['ons_table_4'])
hh_age_gender_2021 = dp.read_dvector_data(input_root_directory=config['input_root_directory'], **config['hh_age_gender_2021'])

# Create a total dvec of total number of households based on occupied_properties + unoccupied_properties
all_properties = unoccupied_households + occupied_households

# Calculate adjustment factors by zone to get proportion of households occupied by dwelling type by zone
non_empty_proportion = occupied_households / all_properties
non_empty_proportion.data = non_empty_proportion.data.fillna(0)

# average occupancy for all dwellings
occupancy = (ons_table_1 / occupied_households) * non_empty_proportion
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
total_hh_by_hh = ons_table_2.aggregate(segs=['h'])
proportion_hhs_by_h_hc_ha_car = ons_table_2 / total_hh_by_hh

# convert the MSOA based factors to LSOAs (duplicate MSOA factor for relevant LSOAs)
proportions_by_lsoa = proportion_hhs_by_h_hc_ha_car.translate_zoning(
    new_zoning=cn.LSOA_ZONING_SYSTEM,
    cache_path=cn.CACHE_FOLDER
)

# multiply the total population by the derived proportions at LSOA level
# TODO here we're applying household based proportions to population, inconsistent?
abpop_by_h_hc_ha_car = addressbase_population * proportions_by_lsoa

# convert 2022 MYPE to MSOA
mype_2022_msoa = mype_2022.translate_zoning(
    new_zoning=cn.MSOA_ZONING_SYSTEM,
    cache_path=cn.CACHE_FOLDER
)

# calculate age band proportions by MSOA and gender from 2022 MYPE
mype_proportions_by_msoa = mype_2022_msoa / mype_2022_msoa.aggregate(segs=['gender'])
# apply these proportions to the 2021 gender and dwelling types, effectively controlling 2021-based gender/dwelling
# splits to the age dimension from mype_2022. Total is still 2021 number.
hh_age_gender_adjusted = mype_proportions_by_msoa * hh_age_gender_2021.aggregate(segs=['h', 'gender'])

# calculate age and gender based factors based on this adjusted output
hh_age_gender_adjusted_proportions = hh_age_gender_adjusted / hh_age_gender_adjusted.aggregate(segs=['age', 'gender'])

# expand this back out to lsoa level to apply to the population (same factors for all LSOAs in same MSOA)
proportions_by_lsoa = hh_age_gender_adjusted_proportions.translate_zoning(
    new_zoning=cn.LSOA_ZONING_SYSTEM,
    cache_path=cn.CACHE_FOLDER
)

# apply the factors
abpop_by_h_hc_ha_car_age_gender = abpop_by_h_hc_ha_car * proportions_by_lsoa

# save output to hdf and csv for readablity
abpop_by_h_hc_ha_car_age_gender.save(OUTPUT_DIR / 'Output 2.hdf')
output = dp.dvector_to_long(dvec=abpop_by_h_hc_ha_car_age_gender, value_name='population')
output.to_csv(OUTPUT_DIR / 'Output 2.csv', index=False)

# calculate NS-SeC splits of households by
# dwelling type by LSOA
total_hh_by_hh = ons_table_4.aggregate(segs=['h'])
proportion_ns_sec = ons_table_4 / total_hh_by_hh
# fill missing proportions with 1 as they are where the total is zero
# don't think it matters what this is infilled with, but this will work for now
proportion_ns_sec.data = proportion_ns_sec.data.fillna(1)

# multiply the total population by the derived proportions at LSOA level
abpop_by_h_hc_ha_car_age_gender_nssec = abpop_by_h_hc_ha_car_age_gender * proportion_ns_sec

# save output to hdf and csv for readablity
abpop_by_h_hc_ha_car.save(OUTPUT_DIR / 'Output 3.hdf')
# runs out of space to save, TODO implement summary outputs
# output = dp.dvector_to_long(dvec=abpop_by_h_hc_ha_car_age_gender_nssec, value_name='population')
# output.to_csv(OUTPUT_DIR / 'Output 3.csv', index=False)
