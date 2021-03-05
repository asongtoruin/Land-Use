# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 19:26:56 2021

@author: genie
"""

import pandas as pd

lu_path = 'Y://NorMITs Land Use//iter3b//outputs//land_use_output_msoa.csv'
lu_out = 'Y://NorMITs Land Use//iter3b//outputs//land_use_output_safe_msoa.csv'

lu = pd.read_csv(lu_path)

list(lu)

gender = lu['gender'].drop_duplicates()
gender_nt = pd.DataFrame({'gender':['Females', 'Male', 'Children'],
                          'g':['1', '2', '3']})


# CA Model, such as it is
hh = lu['household_composition'].drop_duplicates()
hh_comp = pd.DataFrame({'household_composition':[1,2,3,4,5,6,7,8],
                        'ca':[1, 2, 1, 2, 2, 1, 2, 2]})

# EG
age = lu['age'].drop_duplicates()
age = pd.DataFrame({'age':['under 16', '16-74', '75 or over'],
                   'age_code':[1 ,2 ,3]})

lu = lu.merge(hh_comp,
              how='left',
              on='household_composition')

lu = lu.rename(columns={'soc_cat':'soc',
                        'ns_sec':'ns'})

# Build safe out
safe_out = lu.reindex(['msoa_zone_id',
                       'area_type',
                       'household_composition',
                       'ca',
                       'traveller_type',
                       'soc',
                       'ns',
                       'people'], axis=1).groupby(['msoa_zone_id',
                       'area_type',
                       'household_composition',
                       'ca',
                       'traveller_type',
                       'soc',
                       'ns']).sum().reset_index()
safe_out = safe_out.sort_values(['msoa_zone_id',
                       'area_type',
                       'household_composition',
                       'ca',
                       'traveller_type',
                       'soc',
                       'ns']).reset_index(drop=True)
list(safe_out)

safe_out.to_csv(lu_out, index=False)
