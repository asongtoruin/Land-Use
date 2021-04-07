# -*- coding: utf-8 -*-
"""
File purpose:
Generic utils for land use build
"""
import os

from typing import Any
from typing import List
from typing import Union

from math import isclose

import pandas as pd

import land_use.lu_constants as consts

def create_folder(folder, ch_dir=False, verbose=True):
    """
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
        if ch_dir:
            os.chdir(folder)
        if verbose:
            print("New project folder created in " + folder)
    else:
        if ch_dir:
            os.chdir(folder)
        if verbose:
            print('Folder already exists')

def is_almost_equal(v1: float,
                    v2: float,
                    significant: int = 7
                    ) -> bool:
    """
    Checks v1 and v2 are equal to significant places

    Parameters
    ----------
    v1:
        The first value to compare

    v2:
        The second value to compare

    significant:
        The number of significant bits to compare over

    Returns
    -------
    almost_equal:
        True if v1 and v2 are equal to significant bits, else False
    """
    return isclose(v1, v2, abs_tol=10 ** -significant)


def intersection(l1: List[Any], l2: List[Any]) -> List[Any]:
    """
    Efficient method to return the intersection between l1 and l2
    """
    # Want to loop through the smaller list for efficiency
    if len(l1) > len(l2):
        big = l1.copy()
        small = l2
    else:
        big = l2
        small = l1

    # Get the intersection
    temp = set(big)
    return [x for x in small if x in temp]


def list_safe_remove(lst: List[Any],
                     remove: List[Any],
                     raise_error: bool = False,
                     inplace: bool = False
                     ) -> List[Any]:
    """
    Removes remove items from lst without raising an error

    Parameters
    ----------
    lst:
        The list to remove items from

    remove:
        The items to remove from lst

    raise_error:
        Whether to raise and error or not when an item is not contained in
        lst

    inplace:
        Whether to remove the items in-place, or return a copy of lst

    Returns
    -------
    lst:
        lst with removed items removed from it
    """
    # Init
    if not inplace:
        lst = lst.copy()

    for item in remove:
        try:
            lst.remove(item)
        except ValueError as e:
            if raise_error:
                raise e

    return lst


def get_land_use(
        path,
        model_zone_col='msoa_zone_id',
        segmentation_cols=None,
        col_limit=None,
        add_total=False,
        total_col_name='E01',
        to_long=False,
        long_var_name=None,
        long_value_name=None
        ):
    """
    Import land use from somewhere.
    Subset cols if required.
    Apply ca lookup on cars, if required.
    Col limit if subset required
    """

    lu = pd.read_csv(path, nrows=col_limit, dtype={'soc': str})

    if segmentation_cols is not None:
        # Get cols to reindex with
        ri_cols = list([model_zone_col])
        for col in segmentation_cols:
            ri_cols.append(col)
        group_cols = ri_cols.copy()
        if 'people' not in segmentation_cols:
            ri_cols.append('people')
        lu = lu.reindex(
            ri_cols, axis=1).groupby(group_cols).sum().reset_index()
        lu = lu.sort_values(group_cols)

    cols = list(lu.columns)
    cols.remove(model_zone_col)

    if add_total:
        lu[total_col_name] = lu[cols].sum(axis='columns')

    if to_long:
        lu = lu.melt(id_vars=model_zone_col,
                     var_name=long_var_name,
                     value_name=long_value_name)
        lu = lu.sort_values([model_zone_col,
                             long_var_name]).reset_index(drop=True)

    return lu


def infill_traveller_types(land_use_build: pd.DataFrame,
                           traveller_type_lookup=consts.TT_INDEX,
                           attribute_subset=None,
                           left_tt_col='traveller_type',
                           right_tt_col='traveller_type'):

    """
    Function to reapply traveller type to a normalised land use build
    where normalisation means the removal of all non-traveller type data

    Parameters
    ----------
    land_use_build:
        DataFrame containing a NTEM style 88 integer normalised traveller
        type vector
    traveller_type_lookup:
        vector containing normalised constituent values of traveller type
    attribute_subset:
        List or None, which attributes do you want to retain in the lookup
    left_tt_col:
        Join left on
    right_tt_col:
        Join right on

    Returns
    -------
    land_use_build:
        Land use build with added normalised categories
    references: List:
        list of Dataframes with descriptions of normalised values
    """

    # Check traveller type column in both sides
    if left_tt_col not in list(land_use_build):
        raise ValueError('Traveller type not in land use')
    if right_tt_col not in list(traveller_type_lookup):
        raise ValueError('Traveller type not in lookup, try default')

    # Subset the traveller type, if the user wants
    tt_cols = list(traveller_type_lookup)
    lu_cols = list(land_use_build)
    if attribute_subset is not None:
        if right_tt_col in attribute_subset:
            attribute_subset.remove(right_tt_col)
        tt_cols = [right_tt_col] + attribute_subset
    #  Drop any cols in the lookup that are already in land use
    # This should pick up the reindex above too.
    # All too concise, very sorry if this fails.
    tt_cols = ['traveller_type'] + [x for x in tt_cols if x not in lu_cols]
    traveller_type_lookup = traveller_type_lookup.reindex(
        tt_cols, axis=1)

    # Join traveller type
    land_use_build = land_use_build.merge(
        traveller_type_lookup,
        how='left',
        left_on=left_tt_col,
        right_on=right_tt_col
    )

    return land_use_build

def normalise_attribute(attribute: pd.DataFrame,
                        attribute_name: str,
                        reference: pd.DataFrame,
                        reference_desc_field=None,
                        attribute_out_name: str = None,
                        fuzzy_match=False):
    """
    Function to join a normalising index onto a descriptive variable

    Parameters
    ----------
    attribute:
        Dataframe with a normalisable attribute
    attribute_name:
        Name of the attribute to normalise
    reference:
        Dataframe of reference. If string should attempt to import
    reference_desc_field:
        Name of the column in reference to join on. If none will look for non
        text col
    attribute_out_name:
        What to call the attribute if not whatever arrives. Defaults to in value
    fuzzy_match:
        Some tricks to make a decent match.
        'numbers_only': reduce attribute to numbers and join

    Returns
    -------
    attribute:
        Attribute normalised to lookup standard
    """

    # Work out target join field
    # Asking for trouble - must be a better way to do this







    return attribute