# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 13:44:50 2020
# -*- coding: utf-8 -*-
Created on Tue Feb 18 13:31:24 2020

@author: Systra
Updated by: Mags15
"""

import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

data = pd.read_csv("Y:/NorMITs Land Use/area types/regression_data.csv")

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
full_data.to_csv(
        "Y:/NorMITs Land Use/area types/full_data.csv",
        index = True,
        index_label = "msoa_area_code"
        )
