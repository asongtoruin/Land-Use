"""
Author - ART, 13/12/2021
Created - 13/12/2021

Purpose - Just a little widget to read in compressed files for QA at the end of a new Base Year iteration
"""

from land_use.utils import compress

testfile = r'I:\NorMITs Land Use\base_land_use\iter4d\outputs\3.2.10_adjust_zonal_pop_with_full_dimensions\2018_total_population_independent_of_property_type'
data = compress.read_in(testfile)
print(data)
