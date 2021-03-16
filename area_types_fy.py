# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 07:46:51 2021

@author: ESRIAdmin
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 08:38:36 2021

@author: MagdaSmith
"""

import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import gc

_default_home_dir = 'Y:/NorMITs Land Use/area types/fy_areatypes/'
_default_landuse_dir = 'Y:/NorMITs Land Use/'
import_folder = 'Y:/NorMITs Demand/import - test/scenarios/'

_iter = 'fy'
year = 2050


def establish_pop_per_area(inputdata, year, scenario):
        
    inputdata = inputdata.groupby(by=['msoa_zone_id'], as_index = False).sum()
    inputdata = inputdata[['msoa_zone_id', str(year)]]
    cols_name = ['msoa_zone_id', 'population']
    inputdata.columns = cols_name
    inputdata['scenario'] = scenario
    return(inputdata)

def establish_wor_per_area(inputdata, year, scenario):
        
    inputdata = inputdata.groupby(by=['msoa_zone_id'], as_index = False).sum()
    inputdata = inputdata[['msoa_zone_id', str(year)]]
    cols_name = ['msoa_zone_id', 'workers']
    inputdata.columns = cols_name
    inputdata['scenario'] = scenario
    return(inputdata)
    
def calculate_values_per_hectare(inputdata1, inputdata2):
    data = pd.read_csv('Y:/NorMITs Land Use/area types/fy_areatypes/regression_dataWithScotland.csv').rename(columns={'msoa_area_code':'msoa_zone_id'})
    data = data[['msoa_zone_id','north', 'ntem_area_type_id', 'area_hectares', 'pop_per_hectare', 'workers_per_hectare',    'flat_pop_percent']]
 
    data = pd.merge(inputdata1, data, on = 'msoa_zone_id')
    data['pop_per_hectare'] = data['population']/data['area_hectares']
    data = pd.merge(inputdata2, data, on = ['msoa_zone_id', 'scenario'])
    data['workers_per_hectare'] = data['workers']/data['area_hectares']
    return (data)
 
def get_scenario_data():    
# scenario 1
    jam_pop = pd.read_csv(import_folder +'SC01_JAM/population/future_growth_values.csv')
    jam_emp = pd.read_csv(import_folder + 'SC01_JAM/employment/future_growth_values.csv')
    jam_pop = establish_pop_per_area(jam_pop, year,1)
    jam_emp = establish_wor_per_area(jam_emp, year,1) 
    jam_data = calculate_values_per_hectare(jam_pop, jam_emp)
    del(jam_pop, jam_emp)
    #scenario 2
    pp_pop = pd.read_csv(import_folder +'SC02_PP/population/future_growth_values.csv')
    pp_emp = pd.read_csv(import_folder +'SC02_PP/employment/future_growth_values.csv')
    pp_pop = establish_pop_per_area(pp_pop, year,2)
    pp_emp = establish_wor_per_area(pp_emp, year,2) 
    pp_data = calculate_values_per_hectare(pp_pop, pp_emp)
    del(pp_pop, pp_emp)
    # scenario 3
    dd_pop = pd.read_csv(import_folder +'SC03_DD/population/future_growth_values.csv')
    dd_emp = pd.read_csv(import_folder + 'SC03_DD/employment/future_growth_values.csv')
    dd_pop = establish_pop_per_area(dd_pop, year,3)
    dd_emp = establish_wor_per_area(dd_emp, year,3) 
    dd_data = calculate_values_per_hectare(dd_pop, dd_emp)
    del(dd_pop, dd_emp)
    
    # scenario 4
    uzc_pop = pd.read_csv(import_folder +'SC04_UZC/population/future_growth_values.csv')
    uzc_emp = pd.read_csv(import_folder + 'SC04_UZC/employment/future_growth_values.csv')
    uzc_pop = establish_pop_per_area(uzc_pop, year,4)
    uzc_emp = establish_wor_per_area(uzc_emp, year,4) 
    uzc_data = calculate_values_per_hectare(uzc_pop, uzc_emp)
    del(uzc_pop, uzc_emp)
    
    gc.collect()
    #combine them
    data = jam_data.append(pp_data).append(dd_data).append(uzc_data)
    return (data)

def classify_areas():
    
    data = get_scenario_data()
    start_points =  data[(data["msoa_zone_id"] == "E02006917")&(data["scenario"]==1)]
    start_points = start_points.append (data[((data["msoa_zone_id"] == "E02001063")&(data["scenario"]==1))])
    start_points = start_points.append (data[((data["msoa_zone_id"] == "E02001050")&(data["scenario"]==1))])
    start_points = start_points.append (data[((data["msoa_zone_id"] == "E02001036")&(data["scenario"]==1))])
    
    data = data.set_index(keys = "msoa_zone_id")
    start_points = start_points.set_index(keys = "msoa_zone_id")
    
    # print(init)
    
    start_points = start_points[[
            "pop_per_hectare",
            "workers_per_hectare",
            "flat_pop_percent"
            ]]
    
    north_data = data[(data["north"] == 1) & (data["ntem_area_type_id"] < 5)]
    
    north_data = north_data[[
            "pop_per_hectare",
            "workers_per_hectare",
            "flat_pop_percent"
            ]]
    
    full_data = data[[
            "pop_per_hectare",
            "workers_per_hectare",
            "flat_pop_percent"
            ]]


    kmeans = KMeans(n_clusters = 4, init=start_points)

    kmeans.fit(north_data)

    print(kmeans.cluster_centers_)

    y_km = kmeans.predict(full_data)
    full_data["y_km"] = y_km

    area_types = data[["ntem_area_type_id"]]

    full_data = pd.merge(
        full_data,
        area_types,
        left_index = True,
        right_index = True
        )
    full_data["tfn_area_type_id"] = np.where(full_data["ntem_area_type_id"]>4,full_data["ntem_area_type_id"], full_data["y_km"]+1)
    
    full_data2 = pd.merge(full_data, data, on =['msoa_zone_id', 'pop_per_hectare', 'workers_per_hectare', 'flat_pop_percent'])
    full_data2 = full_data2.drop_duplicates()
    full_data2.to_csv(_default_home_dir+'/full_data_w_scotland_'+_iter+'_'+str(year)+'.csv',
                     index = True, 
                     index_label = "msoa_zone_id")
    
    return(full_data)