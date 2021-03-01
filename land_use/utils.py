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

from typing import Any
from typing import List
from typing import Union

from math import isclose

import pandas as pd


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


def convert_growth_off_base_year(growth_df: pd.DataFrame,
                                 base_year: str,
                                 future_years: List[str]
                                 ) -> pd.DataFrame:
    """
    Converts the multiplicative growth value of each future_years to be
    based off of the base year.

    Parameters
    ----------
    growth_df:
        The starting dataframe containing the growth values of all_years
        and base_year

    base_year:
        The new base year to base all the all_years growth off of.

    future_years:
        The years in growth_dataframe to convert to be based off of
        base_year growth

    Returns
    -------
    converted_growth_dataframe:
        The original growth dataframe with all growth values converted

    """
    # Init
    growth_df = growth_df.copy()
    growth_df.columns = growth_df.columns.astype(str)

    # Do base year last, otherwise conversion won't work
    for year in future_years + [base_year]:
        growth_df[year] /= growth_df[base_year]

    return growth_df


def get_growth_values(base_year_df: pd.DataFrame,
                      growth_df: pd.DataFrame,
                      base_year_col: str,
                      future_year_cols: List[str],
                      merge_cols: Union[str, List[str]] = 'model_zone_id'
                      ) -> pd.DataFrame:
    """
    Returns base_year_df extended to include the growth values in
    future_year_cols

    Parameters
    ----------
    base_year_df:
        Dataframe containing the base year data. Must have at least 2 columns
        of merge_col, and base_year_col

    growth_df:
        Dataframe containing the growth factors over base year for all future
        years i.e. The base year column would be 1 as it cannot grow over
        itself. Must have at least the following cols: merge_col and all
        future_year_cols.

    base_year_col:
        The column name that the base year data is in

    future_year_cols:
        The columns names that contain the future year growth factor data.

    merge_cols:
        Name of the column(s) to merge base_year_df and growth_df on.

    Returns
    -------
    Growth_values_df:
        base_year_df extended and populated with the future_year_cols
        columns.
    """
    # Init
    base_year_df = base_year_df.copy()
    growth_df = growth_df.copy()
    base_year_pop = base_year_df[base_year_col].sum()

    base_year_df.columns = base_year_df.columns.astype(str)
    growth_df.columns = growth_df.columns.astype(str)

    # Avoid clashes in the base year
    if base_year_col in growth_df:
        growth_df = growth_df.drop(base_year_col, axis='columns')

    # Avoid future year clashes
    base_year_df = base_year_df.drop(future_year_cols,
                                     axis='columns',
                                     errors='ignore')

    # Merge on merge col
    growth_values = pd.merge(base_year_df,
                             growth_df,
                             on=merge_cols)

    # Grow base year value by values given in growth_df - 1
    # -1 so we get growth values. NOT growth values + base year
    for year in future_year_cols:
        growth_values[year] = (
                (growth_values[year] - 1)
                *
                growth_values[base_year_col]
        )

    # If these don't match, something has gone wrong
    new_by_pop = growth_values[base_year_col].sum()
    if not is_almost_equal(base_year_pop, new_by_pop):
        raise ValueError(
            "Base year totals have changed before and after growing the "
            "future years - something must have gone wrong. Perhaps the "
            "merge columns are wrong and data is being replicated.\n"
            "Total base year before growth:\t %.4f\n"
            "Total base year after growth:\t %.4f\n"
            % (base_year_pop, new_by_pop)
        )

    return growth_values


def growth_recombination(df: pd.DataFrame,
                         base_year_col: str,
                         future_year_cols: List[str],
                         in_place: bool = False,
                         drop_base_year: bool = True
                         ) -> pd.DataFrame:
    """
    Combines the future year and base year column values to give full
    future year values

     e.g. base year will get 0 + base_year_population

    Parameters
    ----------
    df:
        The dataframe containing the data to be combined

    base_year_col:
        Which column in df contains the base year data

    future_year_cols:
        A list of all the growth columns in df to convert

    in_place:
        Whether to do the combination in_place, or make a copy of
        df to return

    drop_base_year:
        Whether to drop the base year column or not before returning.

    Returns
    -------
    growth_df:
        Dataframe with full growth values for all_year_cols.
    """
    if not in_place:
        df = df.copy()

    for year in future_year_cols:
        df[year] += df[base_year_col]

    if drop_base_year:
        df = df.drop(labels=base_year_col, axis=1)

    return df


def grow_to_future_years(base_year_df: pd.DataFrame,
                         growth_df: pd.DataFrame,
                         base_year: str,
                         future_years: List[str],
                         growth_merge_cols: Union[str, List[str]] = 'msoa_zone_id',
                         infill: float = 0.001,
                         ) -> pd.DataFrame:
    """
    Grows the base_year dataframe using the growth_dataframe to produce future
    year values.

    Can ensure there is no negative growth through an infill if requested.

    Parameters
    ----------
    base_year_df:
        Dataframe containing the base year values. The column named with
        base_year value will be grown.

    growth_df:
        Dataframe containing the growth factors for future_years. The base year
        population will be multiplied by these factors to produce future year
        growth.

    base_year:
        The column name containing the base year data in base_year_df and
        growth_df.

    future_years:
        The columns names containing the future year data in growth_df.

    growth_merge_cols:
        The name of the column(s) to merge the base_year_df and growth_df
        dataframes. This is usually the model_zone column plus any further
        segmentation

    no_neg_growth:
        Whether to ensure there is no negative growth. If True, any growth
        values below 0 will be replaced with infill.

    infill:
        If no_neg_growth is True, this value will be used to replace all values
        that are less than 0.

    Returns
    -------
    grown_df:
        base_year_df extended to include future_years, which will contain the
        base year data grown by the factors provided in growth_df.
    """
    # Init
    all_years = [base_year] + future_years

    # Get the growth factors based from base year
    growth_df = convert_growth_off_base_year(
        growth_df,
        base_year,
        future_years
    )

    # Convert growth factors to growth values
    grown_df = get_growth_values(
        base_year_df,
        growth_df,
        base_year,
        future_years,
        merge_cols=growth_merge_cols
    )

    # Add base year back in to get full grown values
    grown_df = growth_recombination(
        grown_df,
        base_year_col=base_year,
        future_year_cols=future_years,
        drop_base_year=False
    )

    return grown_df


def get_land_use(
        path,
        segmentation_cols=None,
        apply_ca_model=False,
        col_limit=None
        ):
    """
    Import land use from somewhere.
    Subset cols if required.
    Apply ca lookup on cars, if required.
    Col limit if subset required
    """

    lu = pd.read_csv(path, nrows=col_limit)

    if segmentation_cols is not None:
        # Get cols to reindex with
        ri_cols = segmentation_cols.copy()
        group_cols = ri_cols.copy()
        if 'people' not in segmentation_cols:
            ri_cols.append('people')
        lu = lu.reindex(
            ri_cols, axis=1).groupby(group_cols).sum().reset_index()
        lu = lu.sort_values(group_cols)

    return lu
