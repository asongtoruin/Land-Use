# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 17:05:20 2020

@author: mags15
Title: NPR Segmentation
Adding employment data to our land use based population segmentation
TODO: add gender and UK-born characteristic
TODO: optimise the code
TODO: LAD controls - segmenting over the employment economically active and unm
"""

import gc
import os
import sys
sys.path.append('C:/Users/' + os.getlogin() + '/S/TAME shared resources/Python')
sys.path.append('C:/Users/' + os.getlogin() + '/S/NorMITs Utilities/Python')


import nu_project as nup
import pandas as pd
import geopandas as gpd
import numpy as np

from shapely.geometry import *
###

# Set shapefile references
lsoaRef = 'Y:/Data Strategy/GIS Shapefiles/UK LSOA and Data Zone Clipped 2011/uk_ew_lsoa_s_dz.shp'
msoaRef = 'Y:/Data Strategy/GIS Shapefiles/UK MSOA and Intermediate Zone Clipped 2011/uk_ew_msoa_s_iz.shp'
ladRef = 'Y:/Data Strategy/GIS Shapefiles/LAD GB 2017/Local_Authority_Districts_December_2017_Full_Clipped_Boundaries_in_Great_Britain.shp'
mladRef = 'Y:/Data Strategy/GIS Shapefiles/Merged_LAD_December_2011_Clipped_GB/Census_Merged_Local_Authority_Districts_December_2011_Generalised_Clipped_Boundaries_in_Great_Britain.shp'

# inputs
landusePath = 'Y:/NorMITs Land Use/iter2_MSOA/landuseOuputMSOA_GBMYE.csv'
nssecPath = 'Y:/NorMITs Land Use/import/NPR Segmentation/processed data/TfN_households_export.csv'
# properties = pd.read_csv('Y:/NorMITs Land Use/iter2_MSOA/Land Use Audits/full_run_msoa_ons_comparison.csv').iloc[:,0:3]
areatypesPath = 'Y:/NorMITs Land Use/import/area_types_msoa.csv'
ladPath = 'Y:/NorMITs Synthesiser/Zone Translation/Export/lad_to_msoa/lad_to_msoa.csv'
LADControlPath = 'Y:/NorMITs Land Use/import/NPR Segmentation/processed data/LAD controls 2018/nomis_2018_segment_lad_constraints.csv'

# do we need properties? we could add them later

    def LanduseFormatting():
        
        # 1.Combine all flat types. Sort out flats on the landuse side; actually there's no 7
        landuse = pd.read_csv(landusePath).drop(columns={'household_composition','Gender', 'Unnamed: 0'})
        flats = [4,5,6]
        flats_landuse = landuse[landuse.property_type.isin(flats)].copy()
        landuseNEW = landuse[~landuse.property_type.isin(flats)].copy()

        # print(flats_landuse['people'].sum())
        flats_landuse2 = flats_landuse.groupby(by = ['ZoneID', 
              'Age', 'employment_type', 'area_type', 'property_type'],
              as_index = False).sum()
        flats_landuse2['property_type2'] = 4
        flats_landuse2['people2'] = flats_landuse2.groupby(['ZoneID', 'Age','employment_type',
                  'property_type2'])['people'].transform('sum')
        flats_landuse2 = flats_landuse2.drop(columns={'people', 'property_type'}
                ).rename(columns={'people2':'people', 
                'property_type2':'property_type'}).drop_duplicates()

        # flats_landuse2['people'].sum()
        # formatting so it's the same on append
        landuseCols = ['ZoneID', 'area_type', 'property_type', 'Age', 'employment_type',
               'people']
        flats_landuse2 = flats_landuse2.reindex(columns=landuseCols)
        landuseNEW = landuseNEW.reindex(columns=landuseCols)

        landuseNEW2 = landuseNEW.append(flats_landuse2)
        landuseNEW2.to_csv('C:/NorMITs_Export/iter3/landuseGBMYE_flatcombined.csv')

        if landuseNEW2['people'].sum() < 64000000:
            print('Flat merging failed')
        else: print('population after merging all types of flats is still 64m')
       
    
        del(flats_landuse2)
        del(flats_landuse)
    
        #next split them into active and inactive for future work
        Inactive_categs = ['stu', 'non_wa']
        ActivePot = landuseNEW2[~landuseNEW2.employment_type.isin(Inactive_categs)].copy()
        InactivePot = landuseNEW2[landuseNEW2.employment_type.isin(Inactive_categs)].copy()
       
        InactivePot = InactivePot.groupby(by=['ZoneID', 'area_type', 'property_type',
                                              'Age', 'employment_type'], as_index = False).sum()
            
        # take out Scottish MSOAs from this pot - nssec/soc data is only for E+W
        # Scotland will be calculated based on area type
        areatypes = pd.read_csv(areatypesPath).drop(columns={'zone_desc'}
                                    ).rename(columns={'msoa_zone_id':'ZoneID'})
        ZoneIDs = ActivePot['ZoneID'].drop_duplicates().dropna()
        Scott = ZoneIDs[ZoneIDs.str.startswith('S')]
        ActiveScot = ActivePot[ActivePot.ZoneID.isin(Scott)].copy()
        ActiveScot = ActiveScot.drop(columns={'area_type'})
        ActiveScot = ActiveScot.merge(areatypes, on = 'ZoneID')
        ActiveEng = ActivePot[~ActivePot.ZoneID.isin(Scott)].copy()
        InactiveScot = InactivePot[InactivePot.ZoneID.isin(Scott)].copy()
        InactiveScot = InactiveScot.drop(columns={'area_type'})
        InactiveScot = InactiveScot.merge(areatypes, on = 'ZoneID')
        InactiveEng = InactivePot[~InactivePot.ZoneID.isin(Scott)].copy()
        print("total number of economically active people in the E+W landuse"+
              "pot should be around 41m and is ",ActivePot['people'].sum()/1000000)

        """
        # around 18m (17.997) in Inactive Eng category
      
        """
    def NSSECSOCsplits():
        # house type definition: change NS-SEC house types detached etc to 1/2/3/4
        nssec = pd.read_csv(nssecPath)
        nssec.loc[nssec['house_type'] == 'Detached', 'property_type'] = 1
        nssec.loc[nssec['house_type'] == 'Semi-detached', 'property_type'] = 2
        nssec.loc[nssec['house_type'] == 'Terraced', 'property_type'] = 3
        nssec.loc[nssec['house_type'] == 'Flat', 'property_type'] = 4
        nssec = nssec.drop(columns={'house_type'})

        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 1-2', 'ns_sec'] = 1
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 3-5', 'ns_sec'] = 2
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 6-7', 'ns_sec'] = 3
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC 8', 'ns_sec'] = 4
        nssec.loc[nssec['NS_SeC'] == 'NS-SeC L15', 'ns_sec'] = 5
        nssec = nssec.drop(columns={'NS_SeC'}).rename(columns = {'MSOA name': 'ZoneID'})

        # all economically active in one group 
        nssec = nssec.rename(columns = {'Economically active FT 1-3':'FT higher',
                                'Economically active FT 4-7': 'FT medium',
                                'Economically active FT 8-9':'FT skilled',
                                'Economically active PT 1-3':'PT higher',
                                'Economically active PT 4-7':'PT medium',
                                'Economically active PT 8-9':'PT skilled',
                                'Economically active unemployed':'unm',
                                'Economically inactive':'children',
                                'Economically inactive retired':'75 or over',
                                'Full-time students':'stu'}).drop(columns={
                                        'TfN area type'})
        nssec2 = nssec.copy()
        # rename columns and melt it down
        nssec = nssec.rename(columns = {'msoa_name':'ZoneID'})
        nssec_melt = pd.melt(nssec, id_vars = ['ZoneID', 'property_type', 'ns_sec'], 
                     value_vars = ['FT higher', 'FT medium', 'FT skilled', 
                                   'PT medium', 'PT skilled',
                                   'PT higher', 'unm', 'children', '75 or over', 'stu'])
        nssec_melt = nssec_melt.rename(columns = {'variable':'employment_type', 
                                      'value':'numbers'})
        # map out categories to match the landuse format
        nssec_melt['SOC_category'] = nssec_melt['employment_type']  
        nssec_melt['Age'] = '16-74'
        nssec_melt.loc[nssec_melt['employment_type'] == 'children', 'Age']= 'under 16'
        nssec_melt.loc[nssec_melt['employment_type'] == '75 or over', 'Age']= '75 or over'
        nssec_melt = nssec_melt.replace({'employment_type':{ 'FT higher':'fte', 
                                         'FT medium':'fte',
                                         'FT skilled':'fte',
                                         'PT higher':'pte',
                                         'PT skilled':'pte',
                                         'PT medium':'pte',
                                         'children':'non_wa',
                                         '75 or over':'non_wa'
                                         }})
        nssec_melt = nssec_melt.replace({'SOC_category':{'FT higher':'1',
                                             'FT medium':'2',
                                             'FT skilled':'3',
                                             'PT higher':'1',
                                             'PT medium':'2',
                                             'PT skilled':'3',
                                             'unm':'NA',
                                             '75 or over':'NA',
                                             'children':'NA'}})
        # return(nssec_melt)
        # split the nssec into inactive and active
        inactive = ['stu', 'non_wa']
        InactiveNSSECPot = nssec_melt[nssec_melt.employment_type.isin(inactive)].copy()
        ActiveNSSECPot = nssec_melt[~nssec_melt.employment_type.isin(inactive)].copy()
        # return(InactiveNSSECPot, ActiveNSSECPot)
   
    def ActiveSplits(ActiveNSSECPot, areatypesPath):
        areatypes = pd.read_csv(areatypesPath).rename(columns={
                         'msoa_zone_id':'ZoneID'}).drop(columns={'zone_desc'})
        ActiveNSSECPot = ActiveNSSECPot.merge(areatypes, on = 'ZoneID')
        # MSOAActiveNSSECSplits for Zones
        MSOAActiveNSSECSplits = ActiveNSSECPot.copy()
        MSOAActiveNSSECSplits['totals'] = MSOAActiveNSSECSplits.groupby(['ZoneID', 'property_type',
                      'employment_type'])['numbers'].transform('sum')
        MSOAActiveNSSECSplits['empsplits'] = MSOAActiveNSSECSplits['numbers']/MSOAActiveNSSECSplits['totals']
        # area types splits for Scotland
        # ActiveNSSECPot = ActiveNSSECPot.drop(columns = {'totals', 'numbers'})
        # For Scotland
        GlobalActiveNSSECSplits = ActiveNSSECPot.copy()
        GlobalActiveNSSECSplits = ActiveNSSECPot.groupby(['area_type', 'property_type',
                                                   'employment_type', 'Age', 
                                                   'ns_sec', 'SOC_category'], 
                                                    as_index = False).sum()
        GlobalActiveNSSECSplits['totals'] = GlobalActiveNSSECSplits.groupby(['area_type', 'property_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        GlobalActiveNSSECSplits['global_splits'] = GlobalActiveNSSECSplits['numbers']/GlobalActiveNSSECSplits['totals']
                
        GlobalActiveNSSECSplits = GlobalActiveNSSECSplits.drop(columns = {'numbers', 'totals'})
        # for communal establishments
        AverageActiveNSSECSplits = ActiveNSSECPot.copy()

        AverageActiveNSSECSplits = AverageActiveNSSECSplits.groupby(by=['area_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        AverageActiveNSSECSplits['totals2'] = AverageActiveNSSECSplits.groupby(['area_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        AverageActiveNSSECSplits['average_splits']= AverageActiveNSSECSplits['numbers']/AverageActiveNSSECSplits['totals2']
        AverageActiveNSSECSplits['SOC_category']= 'NA'
        AverageActiveNSSECSplits = AverageActiveNSSECSplits.drop(columns={'totals2', 'numbers', 'property_type'})


    ### SplitInactive(InactiveNSSECPot):
        # there is 17m in this category
        # CommunalEstablishment segments won't have splits so use Area types for 'globalsplits'
    def InactiveSplits():        
        areatypes = pd.read_csv(areatypesPath).rename(columns={
                         'msoa_zone_id':'ZoneID'}).drop(columns={'zone_desc'})
        InactiveNSSECPot = InactiveNSSECPot.merge(areatypes, on='ZoneID')
        # Zone splits
        MSOAInactiveNSSECSplits = InactiveNSSECPot.copy()
        MSOAInactiveNSSECSplits['totals'] = MSOAInactiveNSSECSplits.groupby(['ZoneID', 'property_type',
                        'Age','employment_type'])['numbers'].transform('sum')
        MSOAInactiveNSSECSplits['msoa_splits'] = MSOAInactiveNSSECSplits['numbers']/MSOAInactiveNSSECSplits['totals']
        MSOAInactiveNSSECSplits['SOC_category'] ='NA' 
        MSOAInactiveNSSECSplits = MSOAInactiveNSSECSplits.drop(columns={'totals', 'numbers'})
        MSOAInactiveNSSECSplits['SOC_category'] ='NA' 
        # MSOAInactiveNSSECSplits = MSOAInactiveNSSECSplits.merge(areatypes, on = 'ZoneID')
        # For Scotland
        GlobalInactiveNSSECSplits = InactiveNSSECPot.copy()
        GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.groupby(by=['area_type', 
                                                                          'property_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        GlobalInactiveNSSECSplits['totals2'] = GlobalInactiveNSSECSplits.groupby(['area_type', 'property_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        GlobalInactiveNSSECSplits['global_splits'] = GlobalInactiveNSSECSplits['numbers']/GlobalInactiveNSSECSplits['totals2']
        GlobalInactiveNSSECSplits['SOC_category'] ='NA' 
        GlobalInactiveNSSECSplits = GlobalInactiveNSSECSplits.drop(columns={'totals2', 'numbers'})
        # for communal establishments
        AverageInactiveNSSECSplits = InactiveNSSECPot.copy()
        AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.groupby(by=['area_type', 
                                                                          'employment_type',
                                                                          'Age', 'ns_sec'], 
                                                                            as_index = False).sum()
        AverageInactiveNSSECSplits['totals2'] = AverageInactiveNSSECSplits.groupby(['area_type',
                      'Age','employment_type'])['numbers'].transform('sum')
        AverageInactiveNSSECSplits['average_splits']= AverageInactiveNSSECSplits['numbers']/AverageInactiveNSSECSplits['totals2']
        AverageInactiveNSSECSplits['SOC_category']= 'NA'
        AverageInactiveNSSECSplits = AverageInactiveNSSECSplits.drop(columns={'totals2', 'numbers', 'property_type'})
        
        InactiveSplits = MSOAInactiveNSSECSplits.merge(GlobalInactiveNSSECSplits, 
                                                       on = ['area_type', 'property_type',
                                                             'employment_type', 'Age',
                                                             'SOC_category', 'ns_sec'], how = 'right')
                                                                
        InactiveSplits = InactiveSplits.merge(AverageInactiveNSSECSplits, 
                                              on = ['area_type',
                                                'employment_type', 'SOC_category','ns_sec','Age'
                                                ], how = 'right')
        
          # "Split out NS-SEC and SOC categories"  
          InactivePot['SOC_category'] ='NA' 
          InactiveSplits['splits2'] = InactiveSplits['msoa_splits']  
          InactiveSplits['splits2']= InactiveSplits['splits2'].fillna(InactiveSplits['global_splits'])          
          InactiveSplits['splits2']= InactiveSplits['splits2'].fillna(InactiveSplits['average_splits'])
          InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['msoa_splits'], 'type'] = 'msoa_splits' 
          InactiveSplits.loc[(InactiveSplits['splits2'] == InactiveSplits['global_splits']), 'type'] = 'global_splits' 
          InactiveSplits.loc[InactiveSplits['splits2'] == InactiveSplits['average_splits'], 'type'] = 'average_splits' 
         # InactiveSplits = InactiveSplits.drop(columns={'numbers_x','numbers_y', 'totals2_x', 'totals2_y', 'msoa_splits'})
          #InactiveSplits = InactiveSplits.drop(columns={'average_splits', 'global_splits'})
          InactiveSplits = InactiveSplits.drop(columns={'msoa_splits', 'global_splits'})
          InactiveSplitsAudit = InactiveSplits.groupby(by = ['ZoneID', 'property_type', 'Age' ]).sum()
          
          # InactiveSplits Audit 
         #  Inactive = Inactive.replace({'Age': {'Children':'under 16'}})

    def ApplyEWInactive():
        # apply splits
        CommunalEstablishments = [8]
        CommunalInactive = InactiveEng[InactiveEng.property_type.isin(CommunalEstablishments)].copy()
        InactiveNotCommunal = InactiveEng[~InactiveEng.property_type.isin(CommunalEstablishments)].copy()
        Inactive_Eng = InactiveSplits.merge(InactiveNotCommunal, on = ['ZoneID', 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        Inactive_Eng['newpop']= Inactive_Eng['people'].values*Inactive_Eng['splits2'].values
              
        CommunalInactive = CommunalInactive.merge(AverageInactiveNSSECSplits, on =[ 'area_type',
                                                'employment_type','Age'
                                                ], how = 'left')
        CommunalInactive['newpop'] = CommunalInactive['people']*CommunalInactive['average_splits']
        print("Communal Inactive should be about 600k and is ", CommunalInactive['newpop'].sum())
              
    def ApplyScotInactive():
        InactiveScotland = GlobalInactiveNSSECSplits.merge(InactiveScot, on = [ 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        InactiveScotland['newpop']= InactiveScotland['people'].values*InactiveScotland['global_splits'].values
        InactiveScotland['newpop'].sum()
              
    def ApplyEWActive():
        CommunalEstablishments = [8]
        CommunalActive = ActiveEng[ActiveEng.property_type.isin(CommunalEstablishments)].copy()
        ActiveNotCommunal = ActiveEng[~ActiveEng.property_type.isin(CommunalEstablishments)].copy()

        Active_emp = ActiveNotCommunal.merge(MSOAActiveNSSECSplits, on = ['ZoneID', 'Age', 
                                                                          'property_type', 'employment_type'
                                                                          ],how = 'outer')
        # apply the employment splits for ActivePot to work out population
        Active_emp.groupby('employment_type')
        Active_emp['newpop'] = Active_emp['people']*Active_emp['empsplits']
 
        Active_emp = Active_emp.drop(columns = {'area_type_x', 'area_type_y'})
        Active_emp = Active_emp.merge(areatypes, on = 'ZoneID')
        if (Active_emp['people'].sum() < (40.6*0.01)):
            print('something has gone wrong with splits')
        else: print('ApplyEWsplits has worked fine')
        CommunalActive = CommunalActive.merge(AverageActiveNSSECSplits, on =[ 'area_type',
                                                'employment_type','Age'
                                                ], how = 'left')
        CommunalActive['newpop'] = CommunalActive['people']*CommunalActive['average_splits']

        # apply error catcher here if it's within 10% then accept  
        # audit
        # work out population for InactivePot for E+W:
    def ApplyScotActive():
        ActiveScotland = GlobalActiveNSSECSplits.merge(ActiveScot, on = [ 'area_type' ,
                                                                 'property_type',
                                                                 'Age',
                                                                 'employment_type'], 
                                                                how = 'right')
        ActiveScotland['newpop']= ActiveScotland['people'].values*ActiveScotland['global_splits'].values
        ActiveScotland['newpop'].sum()
              
         
     def AppendAllGroups(Scottish_emp, Active_emp):
        NPRSegments = ['ZoneID', 'area_type', 'property_type', 'Age', 'employment_type', 
                       'ns_sec', 'SOC_category', 'newpop'] 
        CommunalInactive = CommunalInactive.reindex(columns= NPRSegments)
        Inactive_Eng = Inactive_Eng.reindex(columns=NPRSegments)
        ActiveScotland = ActiveScotland.reindex(columns = NPRSegments)
        CommunalActive = CommunalActive.reindex(columns=NPRSegments)
        Active_emp = Active_emp.reindex(columns= NPRSegments)
        InactiveScotland= InactiveScotland.reindex(columns= NPRSegments)
        
        All = CommunalInactive.append(Inactive_Eng).append(CommunalActive).append(Active_emp)
        All = All.append(InactiveScotland).append(ActiveScotland)
        All.to_csv('Y:/NorMITs_Export/NPRSegments.csv')
        All.to_csv('Y:/NorMITs Land Use/iter3/NPR_Segments v2.csv')

         

         