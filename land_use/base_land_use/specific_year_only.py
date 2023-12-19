
# TODO: Deprecated, remove

import pandas as pd

def adjust_landuse_to_specific_yr(landusePath = _landuse_segments,
                                  midyear = True, 
                                  verbose: bool = True):
    """    
    Takes adjusted landuse (after splitting out communal establishments)
    Parameters
    ----------
    landuseoutput:
        Path to csv of landuseoutput 2011 with all the segmentation (emp type, soc, ns_sec, gender, hc, prop_type),
        to get the splits

    Returns
    ----------
    
    """
       
    if midyear:
        # Read in the land use segments as-is
        landusesegments = pd.read_csv(landusePath, usecols = ['ZoneID', 'area_type',
                                             'property_type', 'Age',
                                             'Gender', 'employment_type',
                                             'ns_sec', 'household_composition',
                                             'SOC_category', 'people']).drop_duplicates()

        # TODO: lu_constants has these, so import them
        # Dictionaries to apply normalisation
        gender_nt = {'Male': 2,
                     'Females': 3,
                     'Children': 0}
        age_nt = {'under 16': 1,
                  '16-74': 2,
                  '75 or over': 3}
        emp_nt = {'fte': 1,
                  'pte': 2,
                  'unm': 3,
                  'stu': 4,
                  'non_wa': 5}
                  
        # Set inactive SOC category to 0 and normalise the data
        landusesegments['SOC_category'] = landusesegments['SOC_category'].fillna(0)        
        landusesegments['gender'] = landusesegments['Gender'].map(gender_nt)
        landusesegments['age_code'] = landusesegments['Age'].map(age_nt)
        landusesegments['emp'] = landusesegments['employment_type'].map(emp_nt)
    
        # Get the base year total population by zone, age and gender
        # TODO: looks like this includes communal establishments but then factored to MYPE excluding them! Need to drop those entries?
        pop_pc_totals = landusesegments.groupby(
                by = ['ZoneID', 'age_code', 'gender'],as_index=False
                ).sum().reindex(columns={'ZoneID', 'age_code', 'gender', 'people'})

        # Get the MYPE for Scotland and England/Wales separately, then combine and normalise
        Scot_adjust = get_scotpopulation()
        ewmype = adjust_mype()  # adjust_mype removes the population in communal establishments. Handled separately later on
        
        mype_gb = pd.concat([ewmype, Scot_adjust])
        mype_gb['gender'] = mype_gb['Gender'].map(gender_nt)
        mype_gb['age_code'] = mype_gb['Age'].map(age_nt)
        mype_gb = mype_gb.drop(columns={'Gender', 'Age'})
        
        # Merge the base year population and MYPE together and calculate population growth factors for each zone/age/gender
        mypepops = pop_pc_totals.merge(mype_gb, on = ['ZoneID', 'gender', 'age_code'])
        del(Scot_adjust, ewmype, mype_gb)
        mypepops['pop_factor'] = mypepops['pop']/mypepops['people']
        gc.collect()
        
        mypepops = mypepops.reindex(columns={'ZoneID', 'gender', 'age_code', 'pop_factor'}).drop_duplicates().reset_index(drop=True)
        
        # Merge these factors onto the full land use segments data
        print('Splitting the population after the uplift')
        landuse = pd.merge(landusesegments, mypepops, how = 'inner', on = ['ZoneID', 'gender', 'age_code'])
        
        # Apply the population growth factors to the base population
        landuse['newpop'] = landuse['people']*landuse['pop_factor']

        print('The population for England, Wales and Scotland, before adjustment, is', 
              landuse['people'].sum()/1000000, 'M')
        landuse = landuse.drop(columns = {'pop_factor', 'people'}).rename(columns={'newpop': 'people'})
        print('The adjusted 2018 population for England, Wales and Scotland is', 
              landuse['people'].sum()/1000000, 'M')
        cols = ['ZoneID', 'area_type', 'property_type', 'Gender', 'Age', 'employment_type', 
        'SOC_category', 'ns_sec', 'household_composition', 'people']
        landuse = landuse.reindex(columns=cols)
        
        return landuse  # TODO: remove this return when communal establishments is ready

        ##### COMMUNAL ESTABLISHMENTS ######
        # Get base year population in communal establishments
        landuse_comm = landusesegments[landusesegments.property_type == 8] 
        pop_pc_comms = landuse_comm.groupby(by=['ZoneID', 'Age', 'Gender'], 
                                              as_index = False).sum().reindex(
                                                      columns={'ZoneID', 'Age', 
                                                               'Gender', 'people'})
        
        # TODO: define mype_communal
        
        # Merge the base year and MYPE communal establishment populations together and calculate growth factors
        myepops = pop_pc_comms.merge(mype_communal, on = ['ZoneID', 'Gender', 'Age'])
        myepops['pop_factor'] = myepops['communal_mype']/myepops['people']
        myepops = myepops.drop(columns={'communal_mype', 'people'})
        communal_pop = landuse_comm.merge(myepops, on = ['ZoneID', 'Gender', 'Age'])
        
        # Apply the population growth factors
        communal_pop['newpop'] = communal_pop['people']*communal_pop['pop_factor']
        communal_pop = communal_pop.drop(columns={'people', 'pop_factor'}).rename(columns={'newpop': 'people'}) 
        communal_pop = communal_pop.reindex(columns=cols)
        
        # TODO: ensure communal pop or landuse columns is the same as Scottish
        # Append the communal establishment entries to the rest of the land use data
        gb_adjusted = pd.concat([landuse, communal_pop])
        isnull_any(gb_adjusted)
        
        # Add back any missing MSOAs
        # TODO: if MSOAs going missing is a problem, why not change the merge from inner? This would surely mess up the population constraint?
        # this might not be needed but there were some zones that weren't behaving properly before
        check_zones = gb_adjusted['ZoneID'].drop_duplicates()   
        missingMSOAs = landusesegments[~landusesegments.ZoneID.isin(check_zones)]
        fullGBadjustment = pd.concat([gb_adjusted, missingMSOAs])

        print('Full population for 2018 is now =', 
              fullGBadjustment['people'].sum())
        print('check all MSOAs are present, should be 8480:', 
              fullGBadjustment['ZoneID'].drop_duplicates().count())
        fullGBadjustment = fullGBadjustment.reindex(columns = {'ZoneID', 'property_type', 
                                                       'household_composition', 
                                                       'employment_type', 'SOC_category', 
                                                       'ns_sec', 'people'
                                                       })
        fullGBadjustment = fullGBadjustment.drop_duplicates()
        
        # Export/return the MYPE-adjusted land use
        fullGBadjustment.to_csv(_default_home_dir + '/landUseOutputMSOA_2018.csv', index = False)
        print('Checking for Nans', isnull_any(fullGBadjustment)) 

        return fullGBadjustment

 
    else:
        print('FY not set up yet')
