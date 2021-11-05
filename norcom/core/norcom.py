# -*- coding: utf-8 -*-
"""
Created on Wed Sep 22 13:58:07 2021

@author: FLYN4694
"""

import pandas as pd
import numpy as np
import os
import re
import math
import time

from scipy.optimize import minimize
from scipy.optimize import Bounds
from scipy.optimize import SR1

# TODO: Define constants from constants
# TODO: Object orient

""" 
NorCOM: a car ownership model, using estimated parameters from NTS data to model 
the number of households that own 1+ or 2+|1+ cars within an msoa. This value is calculated
by estimating the probability of car ownership referred to as 'pcar'.

"""

start = time.time()

######################### MODEL SPECS #########################

model_year = 2018
input_path = 'P:/GBCBA/HandT/CQ/Projects/5203594-NorMITS Demand Partner-ADDY4067/40 - Technical/WP3/01 Car Ownership/04 Python model/inputs/'
output_path = 'P:/GBCBA/HandT/CQ/Projects/5203594-NorMITS Demand Partner-ADDY4067/40 - Technical/WP3/01 Car Ownership/04 Python model/outputs/'

######################### INPUT FILES #########################

# inputs direct from land use model
landuse_population_file = 'NTEM_HHpop_Aj.csv'
landuse_prob_occupied_file = 'ProbabilityDwellfilled.csv'
landuse_property_file = 'classifiedResPropertyMSOA.csv'
lu_conversion_file = 'PT_TT_conversion_lookup.csv'

# other input files
prototypical_hh_file = 'PrototypicalHH20_cross_class_0.31.csv'  # from NTS data
target_weights_file = 'ps_target_weights.csv'
model_parameters_file = 'estimated_logit_model_params_model13_1&2plus_11102021.csv'
msoa_file = 'Forecast_NorCOM_Zones_TfN.csv'  # list of zones and zone area
car_costs_file = 'Car_Cost_Indices_v0.1.csv'
zonal_asc_file = 'Zonal_ASCs_v1.0.csv'

######################### READ FILES #########################

# list of msoas for which model is to be run
msoa_inputs = pd.read_csv(os.path.join(input_path, msoa_file))

# the prototypical household data as calculated from NTS survey data
prototypical_hh = pd.read_csv(os.path.join(input_path, prototypical_hh_file))

# weighting of each target variable when minimising the Daly expression
ps_weights = pd.read_csv(os.path.join(input_path, target_weights_file))

# estimated model parameters
model_params = pd.read_csv(os.path.join(input_path, model_parameters_file))

# running and purchase cost indices
car_costs = pd.read_csv(os.path.join(input_path, car_costs_file))

# zone specific constant - calculated from 2011 calibration run
zonal_ascs = pd.read_csv(os.path.join(input_path, zonal_asc_file))

# population data from land use model
lu_population = pd.read_csv(os.path.join(input_path, landuse_population_file))

# dwelling occupancy per msoa from land use model
lu_prob_occupied = pd.read_csv(os.path.join(input_path, landuse_prob_occupied_file))

# dwelling types in each msoa from land use model
lu_property = pd.read_csv(os.path.join(input_path, landuse_property_file))

# lookup between 88 traveller types in land use model and 11 person types
# used in NorCOM prototypical sampling
lu_conversion = pd.read_csv(os.path.join(input_path, lu_conversion_file))

######################### SET PCI, RCI #########################

rci = car_costs.loc[car_costs.Year == model_year, 'Running_Cost_Index'].values[0]
pci = car_costs.loc[car_costs.Year == model_year, 'Purchase_Cost_Index'].values[0]

# 1+ cars
purchase_coeff_1plus = model_params.loc[model_params.model_var ==
                                        'Purchase_Cost_Index', 'value_p1_plus'].values[0]

running_coeff_1plus = model_params.loc[model_params.model_var ==
                                       'Running_Cost_Index', 'value_p1_plus'].values[0]

# 2+ cars
purchase_coeff_2plus = model_params.loc[model_params.model_var ==
                                        'Purchase_Cost_Index', 'value_p2_plus'].values[0]

running_coeff_2plus = model_params.loc[model_params.model_var ==
                                       'Running_Cost_Index', 'value_p2_plus'].values[0]

######################### VARIABLE CLASSIFICATION #########################

## Not explicitly used in current version v0.11.0 but useful for checking calculation
## implementation is correct and may be necessary for updating model to use different
## parameters.

# list all all model variables
model_vars = list(model_params.model_var)
model_vars.remove('intercept')

# specify which model params are numeric
numeric_vars = list(['Purchase_Cost_Index', 'Running_Cost_Index'])

# categorical vars for cross classification
categorical_vars = set(model_vars) - set(numeric_vars)

# zone specific variables
zone_specific_cat_vars = ['HHoldPopDensity_B01ID']
zone_specific_numeric_vars = []
zone_specific_vars = zone_specific_cat_vars + zone_specific_numeric_vars

######################### MODEL VARS - CROSS CLASS ######################

# calculating pcar for a multivariate model with more than one categorical variable
# requires the number of each cross classification in the dataset to be determined

# catch all columns pertaining to each of the categorical variables
nssec = 'HRP_NSSec_B03ID_*'
age = 'HRP_Age_Banded_*'
hhstruct = 'NorCOM_hh_struct_*'

colnames = model_params.model_var

# Filter out all elements that match the above pattern and create a list of
# each to iterate over later.
nssec_list = [x for x in colnames if re.match(nssec, x)]
age_list = [x for x in colnames if re.match(age, x)]
hhstruct_list = [x for x in colnames if re.match(hhstruct, x)]


######################### FUNCTION DEFINITIONS #########################

def create_target_array(targets, ps_weights, msoa):
    """
    Creates a dataframe with the target value of each target variable
    per household
    """

    # filter to one MSOA
    targets_msoa = targets.loc[targets.ZoneID == msoa, :]

    # creating a dataframe with the target values, estimates and weights
    target_array = targets_msoa.T.reset_index()
    target_array.columns = ['target_var', 'value']

    # drop 'ZoneID' and 'ZoneName' rows
    target_array = target_array[target_array['target_var'] != 'ZoneID']
    target_array = target_array[target_array['target_var'] != 'ZoneName']

    # calculate the total no hh in the msoa
    target_no_hh = (targets_msoa['NorCOM_hh_adults_1'].values[0] +
                    targets_msoa['NorCOM_hh_adults_2'].values[0])

    # calculate the value per hh for each target var
    target_array['value_per_hh'] = target_array['value'] / target_no_hh

    # add weights to the target array
    target_array = pd.merge(target_array, ps_weights,
                            how='inner',
                            on='target_var')

    return target_array, target_no_hh


def calculate_estimate(target_array, phi_k, prototypical_hh):
    """
    Calculates the estimated value of each target var per hh
    """

    # create a list of target variables to iterate over
    target_var_list = list(target_array.target_var)

    # initialise list to contain the estimated values for each target variable
    estimate_list = []

    # for each target variable
    for t in target_var_list:
        # calculate the estimated value
        estimate = phi_k * prototypical_hh[t]

        estimate = estimate.sum()
        estimate_list.append(estimate)

    return estimate_list


def daly_expression(phi_k):
    """
    Calculates the value of Q from the estimated value of phi_k

    The Daly expression has two terms.

    term1:  Sum of squared differences between the observed prototypical hh
        proportion (f_k) and the estimated proportions of protptypical hh
        (phi_k)

        term2:  Sum of the difference between the estimated value of each target var
        (calculated from prototypical hh data * phi_k) and the target values
        multiplied by the weighting factor

        Daly expression (Q) = term1 + term 2

    We retain the value of phi_k that minimises Q

    """
    estimate_list = calculate_estimate(target_array, phi_k, prototypical_hh)

    difference = estimate_list - target_array.value_per_hh

    Q = ((f_k - phi_k) ** 2).sum() + (difference * difference * weighting).sum()

    return Q


def get_zone_specific_model_variable_value(zone_specific_vars, census_data, msoa):
    """
    Function that retrurns the value of a model variable that has a single value
    for the entire zone (e.g. population desnity) as opposed to the population
    within the zone
    As of v0.11.0 only one zone specific value is used in the model could update this
    functin in future should more zone specific values be needed.
    """
    zonal_values = {}

    for z in list(zone_specific_vars):
        value = census_data.loc[census_data.msoa == msoa, z].values[0]
        zonal_values = {'z': z, 'value': value}

    return zonal_values


def generate_synthetic_data_cross_class():
    """
    This function generates the synthetic data from the prototypical household input
    data for each cross classificaation of the categorical model variables
    """

    parameter_dict = {}
    synthetic_data = pd.DataFrame()

    # iterating over the list of categorical variable categories
    for a in age_list:
        for n in nssec_list:
            for h in hhstruct_list:
                # p_name is the banded category of population density e.g.
                # 'HHoldPopDensity_B01ID_10'
                term = a + '_&_' + n + '_&_' + h + '_&_' + p_name

                # we're also going to need to get the relevant model parameter
                # for each cross classification so each cross classification
                # should have a dictionary associated with it

                # TODO create dictionary dynamically from a list of categorical vars
                params = {term:
                              {'age': a,
                               'nssec': n,
                               'hhstruct': h,
                               'popdens': p_name
                               }
                          }

                # add the parameters for each cross classification to a dict
                parameter_dict.update(params)

                # create a list of cross classification column names for iterating over
                cross_class_vars = list(parameter_dict.keys())

        for v in cross_class_vars:
            # for each cross classification, put the prototypical value in place
            synthetic_data[v] = prototypical_hh[v]

        # multiply each categorical column with the estimated number of each hh
        synthetic_data = synthetic_data[cross_class_vars].multiply(est_hh['est_hh'], axis='index')

    return synthetic_data, cross_class_vars, parameter_dict


def calc_est_hh():
    """
    Function that calculates and returns the number of households in each of the
    20 prototpical sampling categories as estimated using the Daly expression
    """
    est_hh = pd.DataFrame(data=(phi_k * target_no_hh), columns=['est_hh'])

    return est_hh


def pcar(exponential_term):
    """
    Seperate method for calculating the exponential term as it allows
    us to easily calculate the exponeital term for a hh type using the rule

    e^(a + b)= e^a * e^b

    """
    pcar = 1 / (1 + exponential_term)

    return pcar


def get_categorical_coefficients(model_no_cars, cross_class_vars, c):
    """
    Function to return the relevant model parameters for each cross classification of
    model variables
    """

    # different estimated parameters for 1+ and 2+ cars
    if model_no_cars == 1:
        parameter_string = 'value_p1_plus'
    else:
        parameter_string = 'value_p2_plus'

    # TODO update so this iterates over the categorical variables as defined at the top of the script
    # v0.11.0 uses four categorical variables only
    age_loc = parameter_dict[c]['age']
    age_coeff = model_params.loc[model_params.model_var == age_loc, parameter_string].values[0]

    nssec_loc = parameter_dict[c]['nssec']
    nssec_coeff = model_params.loc[model_params.model_var == nssec_loc, parameter_string].values[0]

    hhstruct_loc = parameter_dict[c]['hhstruct']
    hhstruct_coeff = \
    model_params.loc[model_params.model_var == hhstruct_loc, parameter_string].values[0]

    popdens_loc = parameter_dict[c]['popdens']
    popdens_coeff = \
    model_params.loc[model_params.model_var == popdens_loc, parameter_string].values[0]

    return age_coeff, nssec_coeff, hhstruct_coeff, popdens_coeff


def calculate_population_density(msoa):
    """
    calculates and returns population density for msoa
    Uses the msoa_inputs file which has a list of model zones and the area of the msoa
    in hectares and the population data from the land use model
    """

    msoa_area = msoa_inputs.loc[msoa_inputs.MSOA11CD == msoa, 'Area (hectares)'].values[0]

    row = targets.loc[targets.ZoneID == msoa]
    msoa_population = row[person_type_cols].sum(axis=1).values[0]

    msoa_pop_density = msoa_population / msoa_area

    return msoa_pop_density


def get_population_density_banding(msoa_pop_density):
    """
    Function that returns the numerical banding (between 1 and 14)
    for population density in hectares
    """

    if msoa_pop_density < 1.0:
        density_band = 1
    elif msoa_pop_density < 50:
        # bandings are [5, 10) etc til 50
        density_band = math.floor(2 + (msoa_pop_density / 5))
    elif msoa_pop_density < 60:
        density_band = 12
    elif msoa_pop_density < 75:
        density_band = 13
    else:
        density_band = 14

    return density_band


###################CREATE PERSON TARGETS FROM LAND USE INPUTS ##############

# convert traveller type to NorCOM person type
lu_population = pd.merge(lu_population, lu_conversion, left_on='TravellerType',
                         right_on='lu_TravellerType')

# calculate the number of each target person type in each msoa
person_type_targets = lu_population.groupby(['msoa11cd', 'NorCOM_person_type']).agg(
    {'pop_aj': 'sum'})
person_type_targets = person_type_targets.unstack(level=1)
person_type_targets.columns = person_type_targets.columns.get_level_values(1)
person_type_targets = person_type_targets.reset_index()

################### CREATE HH TARGETS FROM LAND USE INPUTS ##############

# first get number of hh by multipying UPRN with prob dwelling filled
sum_UPRN = lu_property.groupby(['ZoneID']).agg({'UPRN': 'sum'}).reset_index()
sum_UPRN = pd.merge(sum_UPRN, lu_prob_occupied, left_on='ZoneID', right_on='msoaZoneID')

sum_UPRN['number_hh'] = sum_UPRN.UPRN * sum_UPRN.Prob_DwellsFilled

# now count up the population in 1 adult hh from pop data
# the population of adults in one adult households is the same as the number of one adult households

# excluding children
lu_population_adults = lu_population.loc[lu_population.Gender != 'Children']

one_adult_households = lu_population_adults.groupby(['msoa11cd', 'Household_size']).agg(
    {'pop_aj': 'sum'})
one_adult_households = one_adult_households.reset_index()
one_adult_households = one_adult_households.loc[one_adult_households.Household_size == '1 Adult']

# rename/drop some columns
one_adult_households = one_adult_households.drop(columns=['Household_size'])
one_adult_households = one_adult_households.rename(columns={'pop_aj': 'NorCOM_hh_adults_1'})

# now merge the target no hh onto the df with target number of each person type
target_households = pd.merge(one_adult_households, sum_UPRN, right_on='ZoneID', left_on='msoa11cd')

# number of adults in 2+ adult hh is total hh - adults in 1 adult hh
target_households[
    'NorCOM_hh_adults_2'] = target_households.number_hh - target_households.NorCOM_hh_adults_1

# drop some columns
target_households = target_households[['msoa11cd', 'NorCOM_hh_adults_1', 'NorCOM_hh_adults_2']]

# now merge to create on dataframe with person targets and hh targets
targets = pd.merge(target_households, person_type_targets, on='msoa11cd')

# TODO tidy up/constent zone name/ZoneID msoa11cd, MSOA etc
targets = targets.rename(columns={'msoa11cd': 'ZoneID'})

########################CREATE A LIST OF PERSON TYPE COLUMNS##############

target_colnames = targets.columns

person_type = 'PT*'
person_type_cols = [x for x in target_colnames if re.match(person_type, x)]

####################### ITERATE MSOAs #######################

# create list of MSOA zones to iterate over
msoa_list = list(msoa_inputs.MSOA11CD)
for msoa in msoa_list:

    t1 = time.time()

    print(msoa)

    mid = time.time()
    print("{} taken so far...".format(mid - start))

    ####################### PROTOTYPICAL SAMPLING #######################

    # function call to create the target array for prototypical sampling
    # also get the no hh as a variable for later calcs
    target_array, target_no_hh = create_target_array(targets, ps_weights, msoa)

    # observed proportion of each hh type in the NTS data
    f_k = np.array(prototypical_hh.observed_prop_f_k)

    # Initial 'guess' at the proportions that will minimise Q, use the
    # observed proportions from NTS data
    phi_k = f_k

    # function call to calculate the estimate value for each target var
    estimate_list = calculate_estimate(target_array, phi_k, prototypical_hh)

    # adding the estimate to the target array
    target_array['estimate'] = np.array(estimate_list)

    # calcuating the difference between the estimated target val and the target
    # value of the target variable
    target_array['difference'] = target_array.estimate - target_array.value_per_hh

    # create weightings series
    weighting = target_array.weight

    ####################### HH PROPRTIONS ESTIMATION #######################

    # estimated proportion of each hh type must be between 0 and 1
    bounds = Bounds(lb=0, ub=1)

    # and the total of all proportions must sum to 1
    cons = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}

    # use the minimize optimiser to find a value of phi_k that minimises the Daly
    # expression and meets the bounds and constraints

    # TODO optimize, alternative algorithms, jacobian and hessian methods might be used
    res = minimize(daly_expression, phi_k, method='trust-constr', jac="2-point", hess=SR1(),
                   options={'verbose': 0},
                   # 'xtol': 1e-04},
                   bounds=bounds,
                   constraints=cons)

    # use the solution for the estimated proportion of households
    # that best hit our targets
    phi_k = res.x

    ####################### GET/CALC ZONE SPECIFIC VALUES #######################

    # calculate the population density for the msoa
    msoa_pop_density = calculate_population_density(msoa)

    # get the band the population density belongs to
    popdens_band = get_population_density_banding(msoa_pop_density)

    # create a string with relevant categorical variable name
    p_name = 'HHoldPopDensity_B01ID_' + str(popdens_band)

    ####################### CREATE SYNTHETIC DATASET #######################

    # calculate the estimated number of households of each hh type for this msoa
    est_hh = calc_est_hh()

    # calculate the synthetic dataset for this msoa. That is, the number of persons
    # in each of the cross classified categorical model variables from the estimated
    # proportion of each hosehold type and the number of each persons in the cross classification
    # as per NTS data
    synthetic_data, cross_class_vars, parameter_dict = generate_synthetic_data_cross_class()

    probs = synthetic_data.copy()
    exponents = synthetic_data.copy()

    for model_type in range(1, 3):

        # 1+ or 2+ params
        if model_type == 1:
            parameter_string = 'value_p1_plus'
            census_target_name = 'p1_plus_car'
            target_name = 'HasCarVanMc'
            r_name = 'pcar_1plus_modelled'
            asc_name = 'P1_Zonal_ASC'
            purchase_coeff = purchase_coeff_1plus
            running_coeff = running_coeff_1plus

        elif model_type == 2:
            parameter_string = 'value_p2_plus'
            census_target_name = 'p2_plus_car'
            target_name = 'Has2CarVanMc'
            r_name = 'pcar_2plus_modelled'
            asc_name = 'P2_Zonal_ASC'
            purchase_coeff = purchase_coeff_2plus
            running_coeff = running_coeff_2plus

        ####################### GET RELEVANT MODEL PARAMS #######################

        # set intercept
        intercept = \
        model_params.loc[model_params.model_var == 'intercept', parameter_string].values[0]

        # set zone specific constant
        asc = zonal_ascs.loc[zonal_ascs.msoa == msoa, asc_name].values[0]

        for c in cross_class_vars:
            # get the relevant estimated parameter from the input file
            a, n, h, p = get_categorical_coefficients(model_type, cross_class_vars, c)

            # calculate the exponential term based on the intercept and four categorical
            # variables
            exponent = np.exp(-(a + n + h + p + intercept))

            # add the exponential term to the relevant cross classified column
            exponents[c] = exponent

            # TODO incorporate check for numerical model variables here
            # v0.11.0 has none

            # now use rule of addition to cross multiply and include numerical
            # pci and rci and asc
            exponents[c] = exponents[c] * (
                np.exp(-((pci * purchase_coeff) + (rci * running_coeff) + asc)))

            # calculate pcar from exponential term
            probs[c] = pcar(exponents[c])

        # number of cars = probability of car ownership (probs) * synthetic_data (number of
        # persons in each cross classification)
        modelled_cars = synthetic_data.mul(probs)

        # summing up the number of cars across all cross classifications
        pcar_result = modelled_cars.sum(axis=1)

        # total divided by the number of hosueholds of each type which matches the target vars
        pcar_result = pcar_result.sum() / target_no_hh

        # calculate pcar using the estimated proportions of each protypical hh
        # and the average number of HasCarVanMC per prototypical hh
        synthetic_data[target_name] = prototypical_hh[target_name].multiply(est_hh['est_hh'],
                                                                            axis='index')
        synthetic_cars = synthetic_data[target_name].sum() / target_no_hh

        # results placeholders for organising outputs
        result = {}
        out = []

        # time check
        t2 = time.time()
        t = t2 - t1

        # various parameters to be exported
        result = {'msoa': msoa,
                  r_name: pcar_result,
                  'Q': res.fun,
                  'estimated_props': res.x,
                  target_name: synthetic_cars,
                  }

        out.append(result)

        # organising output
        df = pd.DataFrame(data=out)

        # specify filename, seperate files for 1+ and 2+ model results but with same suffix
        # m13 = model version (param estimation)
        # R18 = run18 - see run log
        file_out = os.path.join(output_path, str(model_type) + 'plus_m13_R18_TfN.csv', )

        df.to_csv(file_out, mode='a', header=(not os.path.exists(file_out)), index=False)

        # clear results
        out = []



