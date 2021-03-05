# -*- coding: utf-8 -*-
"""
Created on: Mon March 1 09:43:45 2020
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:
WRITE PURPOSE
"""
import os

from typing import Any
from typing import List
from typing import Union

from math import isclose

import pandas as pd


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
