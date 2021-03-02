# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 08:23:23 2021

@author: Systra, mags15
"""
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

_default_home_dir = 'Y:/NorMITs Land Use/area types/'
_default_landuse_dir = 'Y:/NorMITs Land Use/'
_iter = 'iter3b'



def classify_area_types(midyear = True):
    
    if midyear:
        data = pd.read_csv(_default_home_dir + '/regression_dataWithScotland.csv')
    else:
        # placeholder for import from landuse 
        data_prep = pd.read_csv(_default_home_dir+'/area_types_prep_data.csv')
        
        landuseoutput = pd.read_csv(_default_landuse_dir+_iter+'/outputs/land_use_output_msoa.csv')
        population = landuseoutput.groupby(by=['msoa_zone_id'], as_index = False).sum().reindex(columns={'msoa_zone_id', 
                                          'people'}).rename(columns={'msoa_zone_id':'msoa_area_code', 'people':'population'})
        works = ['fte', 'pte']
        workers = landuseoutput[landuseoutput.employment_type.isin(works)]
        workers = workers.groupby(by=['msoa_zone_id'], as_index = False).sum().reindex(columns= {'msoa_zone_id', 
                                 'people'}).rename(columns={'msoa_zone_id':'msoa_area_code', 'people':'workers'})
        del (landuseoutput, works)
        data = data_prep.merge(population, on = ['msoa_area_code'])
        data = data.merge(workers, on = ['msoa_area_code'])
        data['pop_per_hectare'] = data['population'] /data['area_hectares']
        data['workers_per_hectare'] = data['workers'] /data['area_hectares']
         
        
        #define starting points
        start_points =  data[(data["msoa_area_code"] == "E02006917")]
        start_points = start_points.append (data[(data["msoa_area_code"] == "E02001063")])
        start_points = start_points.append (data[(data["msoa_area_code"] == "E02001050")])
        start_points = start_points.append (data[(data["msoa_area_code"] == "E02001036")])
        
        data = data.set_index(keys = "msoa_area_code")
        start_points = start_points.set_index(keys = "msoa_area_code")
        
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
    
        area_types = data[["ntem_area_type_id"]]
    
        full_data = pd.merge(
            full_data,
            area_types,
            left_index = True,
            right_index = True
            )
        full_data["y_km"] = y_km
        full_data["tfn_area_type_id"] = np.where(full_data["ntem_area_type_id"]>4,full_data["ntem_area_type_id"], full_data["y_km"]+1)
        full_data["north"] = data["north"]
        full_data.to_csv(_default_home_dir+'/full_data_scotland_'+_iter+'.csv',
                         index = True, 
                         index_label = "msoa_area_code")
            
        
        return(full_data)