# -*- coding: utf-8 -*-
"""
Created on: Fri September 11 12:05:31 2020
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:
General utils for use in EFS.
TODO: After integrations with TMS, combine with old_tms.utils.py
  to create a general utils file
"""

import os
import re
import shutil
import random
import inspect
import operator
import time

import pandas as pd
import numpy as np

from typing import Any
from typing import List
from typing import Dict
from typing import Tuple
from typing import Union
from typing import Callable
from typing import Iterable
from typing import Iterator

from pathlib import Path

from math import isclose

import functools
from tqdm import tqdm
from itertools import product
from collections import defaultdict

# Local imports
from land_use import lu_constants as consts

# TODO: Utils is getting big. Refactor into smaller, more specific modules


def print_w_toggle(*args, verbose, **kwargs):
    """
    Small wrapper to only print when verbose=True

    Parameters
    ----------
    *args:
        The text to print - can be passed in the same format as a usual
        print function

    verbose:
        Whether to print the text or not

    **kwargs:
        Any other kwargs to pass directly to the print function call
    """
    if verbose:
        print(*args, **kwargs)



def convert_msoa_naming(df: pd.DataFrame,
                        msoa_col_name: str,
                        msoa_path: str,
                        msoa_str_col: str = 'model_zone_code',
                        msoa_int_col: str = 'model_zone_id',
                        to: str = 'string'
                        ) -> pd.DataFrame:
    """
    Returns df with the msoa zoning given converted to either string or int
    names, as requested.

    Parameters
    ----------
    df:
        The dataframe to convert. Must have a column named as msoa_col_name

    msoa_col_name:
        The name of the column in df to convert.

    msoa_path:
        The full path to the file to use to do the conversion.

    msoa_str_col:
        The name of the column in msoa_path file which contains the string
        names for all msoa zones.

    msoa_int_col:
        The name of the column in msoa_path file which contains the integer
        ids for all msoa zones.

    to:
        The format to convert to. Supports either 'int' or 'string'.

    Returns
    -------
    converted_df:
        df, in the same order, but the msoa_col_name has been converted to the
        desired format.
    """
    # Init
    column_order = list(df)
    to = to.strip().lower()

    # Validate
    if msoa_col_name not in df:
        raise KeyError("Column '%s' not in given dataframe to convert."
                       % msoa_col_name)

    # Rename everything to make sure there are no clashes
    df = df.rename(columns={msoa_col_name: 'df_msoa'})

    # Read in MSOA conversion file
    msoa_zones = pd.read_csv(msoa_path).rename(
        columns={
            msoa_str_col: 'msoa_string',
            msoa_int_col: 'msoa_int'
        }
    )

    if to == 'string' or to == 'str':
        merge_col = 'msoa_int'
        keep_col = 'msoa_string'
    elif to == 'integer' or to == 'int':
        merge_col = 'msoa_string'
        keep_col = 'msoa_int'
    else:
        raise ValueError("Invalid value received. Do not know how to convert "
                         "to '%s'" % str(to))

    # Convert MSOA strings to id numbers
    df = pd.merge(df,
                  msoa_zones,
                  left_on='df_msoa',
                  right_on=merge_col)

    # Drop unneeded columns and rename
    df = df.drop(columns=['df_msoa', merge_col])
    df = df.rename(columns={keep_col: msoa_col_name})

    return df.reindex(column_order, axis='columns')



def copy_and_rename(src: str, dst: str) -> None:
    """
    Makes a copy of the src file and saves it at dst with the new filename.

    If no filename is given for dst, then the file will be copied over with
    the same name as used in src.

    Parameters
    ----------
    src:
        Path to the file to be copied.

    dst:
        Path to the new save location.

    Returns
    -------
    None
    """
    if not os.path.exists(src):
        raise IOError("Source file does not exist.\n %s" % src)

    if not os.path.isfile(src):
        raise IOError("The given src file is not a file. Cannot handle "
                      "directories.")

    # If no filename given, don't need to rename - just use src filename
    if '.' not in os.path.basename(dst):
        # Copy over with same filename
        shutil.copy(src, dst)
        return

    # Split paths
    src_head, src_tail = os.path.split(src)
    dst_head, dst_tail = os.path.split(dst)

    # Avoid case where src and dist is same locations
    if dst_head == src_head:
        shutil.copy(src, dst)
        return

    # Copy then rename
    shutil.copy(src, dst_head)
    shutil.move(os.path.join(dst_head, src_tail), dst)


def add_fname_suffix(fname: str, suffix: str):
    """
    Adds suffix to fname - in front of the file type extension

    Parameters
    ----------
    fname:
        The fname to be added to - must have a file type extension
        e.g. .csv
    suffix:
        The string to add between the end of the fname and the file
        type extension

    Returns
    -------
    new_fname:
        fname with suffix added

    """
    f_type = '.' + fname.split('.')[-1]
    new_fname = '.'.join(fname.split('.')[:-1])
    new_fname += suffix + f_type
    return new_fname


def safe_read_csv(file_path: str,
                  print_time: bool = False,
                  **kwargs
                  ) -> pd.DataFrame:
    """
    Reads in the file and performs some simple file checks

    Parameters
    ----------
    file_path:
        Path to the file to read in

    print_time:
        Whether to print out some info on how long the file read has taken

    kwargs:
        ANy kwargs to pass onto pandas.read_csv()

    Returns
    -------
    dataframe:
        The data from file_path
    """
    # Init
    if kwargs is None:
        kwargs = dict()

    # TODO: Add any more error checks here
    # Check file exists
    if not os.path.exists(file_path):
        raise IOError("No file exists at %s" % file_path)

    # Just return the file
    if not print_time:
        return pd.read_csv(file_path, **kwargs)

    # Print out some timing info while reading
    start = time.perf_counter()
    print('\tReading "%s"' % file_path, end="")
    df = pd.read_csv(file_path, **kwargs)
    print(" - Done in %fs" % (time.perf_counter() - start))
    return df


def is_none_like(o) -> bool:
    """
    Checks if o is none-like

    Parameters
    ----------
    o:
        Object to check

    Returns
    -------
    bool:
        True if o is none-like else False
    """
    if o is None:
        return True

    if isinstance(o, str):
        if o.lower().strip() == 'none':
            return True

    if isinstance(o, list):
        return all([is_none_like(x) for x in o])

    return False


def starts_with(s: str, x: str) -> bool:
    """
    Boolean test to see if string s starts with string x or not.

    Parameters
    ----------
    s:
        The string to test

    x:
        The string to search for

    Returns
    -------
    Bool:
        True if s starts with x, else False.
    """
    search_string = '^' + x
    return re.search(search_string, s) is not None

def get_segmentation_mask(df: pd.DataFrame,
                          col_vals: dict,
                          ignore_missing_cols=False
                          ) -> pd.Series:
    """
    Creates a mask on df, optionally skipping non-existent columns

    Parameters
    ----------
    df:
        The dataframe to make the mask from.

    col_vals:
        A dictionary of column names to wanted values.

    ignore_missing_cols:
        If True, and error will not be raised when a given column in
        col_val does not exist.

    Returns
    -------
    segmentation_mask:
        A pandas.Series of boolean values
    """
    # Init Mask
    mask = pd.Series([True] * len(df))

    # Narrow down mask
    for col, val in col_vals.items():
        # Make sure column exists
        if col not in df.columns:
            if ignore_missing_cols:
                continue
            else:
                raise KeyError("'%s' does not exist in DataFrame."
                               % str(col))

        mask &= (df[col] == val)

    return mask


def segment_loop_generator(seg_dict: Dict[str, List[Any]],
                           ) -> Iterator[Dict[str, Any]]:
    """
    Yields seg_values dictionary for all unique combinations of seg_dict

    Parameters
    ----------
    seg_dict:
        Dictionary of {seg_names: [seg_vals]}. All possible combinations of
        seg_values will be iterated through

    Returns
    -------
    seg_values:
        A dictionary of {seg_name: seg_value}
    """
    # Separate keys and values
    keys, vals = zip(*seg_dict.items())

    for unq_seg in product(*vals):
        yield {keys[i]: unq_seg[i] for i in range(len(keys))}

def long_to_wide_out(df: pd.DataFrame,
                     v_heading: str,
                     h_heading: str,
                     values: str,
                     out_path: str,
                     unq_zones: List[str] = None,
                     round_dp: int = 12,
                     ) -> None:
    """
    Converts a long format pd.Dataframe, converts it to long and writes
    as a csv to out_path

    Parameters
    ----------
    df:
        The dataframe to convert and output

    v_heading:
        Column name of df to be the vertical heading.

    h_heading:
        Column name of df to be the horizontal heading.

    values:
        Column name of df to be the values.

    out_path:
        Where to write the converted matrix.

    unq_zones:
        A list of all the zone names that should exist in the output matrix.
        If zones in this list are not in the given df, they are infilled with
        values of 0.
        If left as None, it assumes all zones in the range 1 to max zone number
        should exist.

    round_dp:
        The number of decimal places to round the output to

    Returns
    -------
        None
    """
    # Init
    df = df.copy()

    # Get the unique column names
    if unq_zones is None:
        unq_zones = df[v_heading].drop_duplicates().reset_index(drop=True).copy()
        unq_zones = list(range(1, max(unq_zones)+1))

    # Make sure all unq_zones exists in v_heading and h_heading
    df = ensure_multi_index(
        df=df,
        index_dict={v_heading: unq_zones, h_heading: unq_zones},
    )

    # Convert to wide format and round
    df = df.pivot(
        index=v_heading,
        columns=h_heading,
        values=values
    ).round(decimals=round_dp)

    # Finally, write to disk
    df.to_csv(out_path)


def wide_to_long_out(df: pd.DataFrame,
                     id_vars: str,
                     var_name: str,
                     value_name: str,
                     out_path: str
                     ) -> None:
    # TODO: Write wide_to_long_out() docs
    # This way we can avoid the name of the first col
    df = df.melt(
        id_vars=df.columns[:1],
        var_name=var_name,
        value_name=value_name
    )
    id_vars = id_vars[0] if isinstance(id_vars, list) else id_vars
    df.columns.values[0] = id_vars

    df.to_csv(out_path, index=False)


def get_compile_params_name(matrix_format: str,
                            year: str,
                            suffix: str = None
                            ) -> str:
    """
    Generates the compile params filename
    """
    if suffix is None:
        return "%s_yr%s_compile_params.csv" % (matrix_format, year)

    return "%s_yr%s_%s_compile_params.csv" % (matrix_format, year, suffix)


def get_split_factors_fname(matrix_format: str,
                            year: str,
                            suffix: str = None
                            ) -> str:
    """
    Generates the splitting factors filename
    """
    ftype = consts.COMPRESSION_SUFFIX.strip('.')
    if suffix is None:
        return "%s_yr%s_splitting_factors.%s" % (matrix_format, year, ftype)

    return "%s_yr%s_%s_splitting_factors.%s" % (matrix_format, year, suffix, ftype)


def build_full_paths(base_path: str,
                     fnames: Iterable[str]
                     ) -> List[str]:
    """
    Prepends the base_path name to all of the given fnames
    """
    return [os.path.join(base_path, x) for x in fnames]


def list_files(path: str,
               ftypes: List[str] = None,
               include_path: bool = False
               ) -> List[str]:
    """
    Returns the names of all files (excluding directories) at the given path

    Parameters
    ----------
    path:
        Where to search for the files

    ftypes:
        A list of filetypes to accept. If None, all are accepted.

    include_path:
        Whether to include the path with the returned filenames

    Returns
    -------
    files:
        Either filenames, or the paths to the found files

    """
    if include_path:
        file_paths = build_full_paths(path, os.listdir(path))
        paths = [x for x in file_paths if os.path.isfile(x)]
    else:
        fnames = os.listdir(path)
        paths = [x for x in fnames if os.path.isfile(os.path.join(path, x))]

    if ftypes is None:
        return paths

    # Filter down to only the filetypes asked for
    keep_paths = list()
    for file_type in ftypes:
        temp = [x for x in paths if file_type in x]
        keep_paths = list(set(temp + keep_paths))

    return keep_paths


def is_in_string(vals: Iterable[str],
                 string: str
                 ) -> bool:
    """
    Returns True if any of vals is in string, else False
    """
    for v in vals:
        if v in string:
            return True
    return False

def write_csv(headers: Iterable[str],
              out_lines: List[Iterable[str]],
              out_path: str
              ) -> None:
    """
    Writes the given headers and outlines as a csv to out_path

    Parameters
    ----------
    headers
    out_lines
    out_path

    Returns
    -------
    None
    """
    # Make sure everything is a string
    headers = [str(x) for x in headers]
    out_lines = [[str(x) for x in y] for y in out_lines]

    all_out = [headers] + out_lines
    all_out = [','.join(x) for x in all_out]
    with open(out_path, 'w') as f:
        f.write('\n'.join(all_out))

def defaultdict_to_regular(d):
    """
    Iteratively converts nested default dicts to nested regular dicts.

    Useful for pickling - keeps the unpickling of the dict simple

    Parameters
    ----------
    d:
        The nested defaultdict to convert

    Returns
    -------
    converted_d:
        nested dictionaries with same values
    """
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regular(v) for k, v in d.items()}
    return d


def file_write_check(path: Union[str, Path], wait: bool=True) -> Path:
    """Attempts to write to given path to see if file is in use.

    Will either wait for the file to be closed or it will append numbers
    to the file name until and path can be found that isn't in use.

    Parameters
    ----------
    path : str
        Path to the file to check.

    wait : bool, optional
        Whether or not to wait for the file to be closed, by default True.
        If False appends number to the end of file name to find a path that
        isn't in use.

    Returns
    -------
    Path
        Path that isn't currently in use, will be the same as given `path`
        if wait is True.

    Raises
    ------
    ValueError
        When wait is False and a path can't be found, that isn't in use, in less
        than 100 attempts.
    """
    path = Path(path)
    new_path = path
    count = 1
    waiting = False
    while True:
        try:
            with open(new_path, 'wb') as f:
                pass
            return new_path
        except PermissionError:
            if wait:
                if not waiting:
                    print(f"Cannot write to file at {new_path.absolute()}.",
                          "Please ensure it is not open anywhere.",
                          "Waiting for permission to write...", sep='\n')
                    waiting = True
                time.sleep(1)
            else:
                new_path = path.parent / (path.stem + f'_{count}' + path.suffix)
                count += 1
                if count > 100:
                    raise ValueError('Too many files in use!')


def safe_dataframe_to_csv(df: pd.DataFrame,
                          out_path: str,
                          flatten_header: bool = False,
                          **to_csv_kwargs: Any,
                          ) -> None:
    """
    Wrapper around df.to_csv. Gives the user a chance to close the open file.

    Parameters
    ----------
    df:
        pandas.DataFrame to write to call to_csv on

    out_path:
        Where to write the file to. TO first argument to df.to_csv()

    flatten_header: bool, optional
        Whether or not MultiIndex column names should be flattened into a single level,
        default False.

    to_csv_kwargs:
        Any other kwargs to be passed straight to df.to_csv()

    Returns
    -------
        None
    """
    if flatten_header and len(df.columns.names) > 1:
        # Combine multiple columns levels into a single name split by ':'
        df.columns = [' : '.join(str(i) for i in c) for c in df.columns]

    written_to_file = False
    waiting = False
    while not written_to_file:
        try:
            df.to_csv(out_path, **to_csv_kwargs)
            written_to_file = True
        except PermissionError:
            if not waiting:
                print("Cannot write to file at %s.\n" % out_path +
                      "Please ensure it is not open anywhere.\n" +
                      "Waiting for permission to write...\n")
                waiting = True
            time.sleep(1)


def fit_filter(df: pd.DataFrame,
               df_filter: Dict[str, Any],
               raise_error: bool = False
               ) -> Dict[str, Any]:
    """
    Whittles down filter to only include relevant items

    Any columns that do not exits in the dataframe will be removed from the
    filter; optionally raises an error if a filter column is not in the given
    dataframe. Furthermore, any items that are 'none like' as determined by
    is_none_like() will also be removed.

    Parameters
    ----------
    df:
        The dataframe that the filter is to be applied to.

    df_filter:
        The filter dictionary in the format of {df_col_name: filter_values}

    raise_error:
        Whether to raise an error or not when a df_col_name does not exist in
        the given dataframe.

    Returns
    -------
    fitted_filter:
        A filter with non-relevant (as defined in the function description)
        items removed.
    """
    # Init
    fitted_filter = dict()
    df = df.copy()
    df.columns = df.columns.astype(str)

    # Check each item in the given filter
    for col, vals in df_filter.items():

        # Check the column exists
        if col not in df.columns:
            if raise_error:
                raise KeyError("'%s' Column not found in given dataframe"
                               % str(col))
            else:
                continue

        # Check the given value isn't None
        if is_none_like(vals):
            continue

        # Should only get here for valid combinations
        fitted_filter[col] = vals

    return fitted_filter


def remove_none_like_filter(df_filter: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes all None-like items from df_filter

    Parameters
    ----------
    df_filter:
        The filter dictionary in the format of {df_col_name: filter_values}

    Returns
    -------
    df_filter:
        df_filter with None-like items removed
    """
    # Init
    new_df_filter = dict()

    for k, v in df_filter.items():
        if not is_none_like(v):
            new_df_filter[k] = v

    return new_df_filter


def filter_df(df: pd.DataFrame,
              df_filter: Dict[str, Any],
              fit: bool = False,
              **kwargs,
              ) -> pd.DataFrame:
    """
    Filters a dataframe down to a given filter

    Can handle flexible segmentation if fit is set to True - all unnecessary
    columns will be removed, and any 'None like' filters will be removed. This
    follows the convention of settings segmentation splits to None when it
    is not needed.

    Parameters
    ----------
    df:
        The dataframe that the filter is to be applied to.

    df_filter:
        The filter dictionary in the format of {df_col_name: filter_values}.

    fit:
        Whether to try and fit the given filter to the dataframe before
        application. If using flexible segmentation and filter has not already
        been fit, set to True.

    kwargs:
        Any additional kwargs that should be passed to fit_filter() if fit is
        set to True.
    Returns
    -------
    filtered_df:
        The original dataframe given, segmented to the given filter level.
    """
    # Init
    df = df.copy()
    df_filter = df_filter.copy()

    # Wrap each item if a list to avoid errors
    for k, v in df_filter.items():
        if not pd.api.types.is_list_like(v):
            df_filter[k] = [v]

    # Ignore none-like filters to avoid dropping trips
    df_filter = remove_none_like_filter(df_filter)

    if fit:
        df_filter = fit_filter(df, df_filter.copy(), **kwargs)

    # Figure out the correct mask
    needed_cols = list(df_filter.keys())
    mask = df[needed_cols].isin(df_filter).all(axis='columns')

    return df[mask]


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


def ensure_index(df: pd.DataFrame,
                 index: List[Any],
                 index_col: str,
                 infill: float = 0.0
                 ) -> pd.DataFrame:
    """
    Ensures every value in index exists in index_col of df.
    Missing values are infilled with infill
    """
    # Make a dataframe with just the new index
    ph = pd.DataFrame({index_col: index})

    # Merge with the given and infill missing
    return ph.merge(df, how='left', on=index_col).fillna(infill)


def ensure_multi_index(df: pd.DataFrame,
                       index_dict: Dict[str, List[Any]],
                       infill: float = 0.0
                       ) -> pd.DataFrame:
    """
    Ensures every combination of values in index_list exists in df.

    This function is useful to ensure a conversion from long to wide will
    happen correctly

    Parameters
    ----------
    df:
        The dataframe to alter.

    index_dict:
        A dictionary of {column_name: index_vals} to ensure exist in all
        combinations within df.

    infill:
        Value to infill any other columns of df where the given indexes
        don't exist.

    Returns
    -------
    df:
        The given df given with all combinations of the index dict values
    """
    # Create a new placeholder df with every combination of the unique columns
    all_combos = zip(*product(*index_dict.values()))
    ph = {col: vals for col, vals in zip(index_dict.keys(), all_combos)}
    ph = pd.DataFrame(ph)

    # Merge with the given and infill missing
    merge_cols = list(index_dict.keys())
    return ph.merge(df, how='left', on=merge_cols).fillna(infill)


def match_pa_zones(productions: pd.DataFrame,
                   attractions: pd.DataFrame,
                   unique_zones: List[Any],
                   zone_col: str = 'model_zone_id',
                   infill: float = 0.0,
                   set_index: bool = False
                   ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Makes sure all unique zones exist in productions and attractions

    Any missing zones will be infilled with infill,

    Parameters
    ----------
    productions:
        Dataframe containing the productions data - must have a zone_col

    attractions:
        Dataframe containing the productions data - must have a zone_col

    unique_zones:
        List of the desired zones to have in productions and attractions

    zone_col:
        Column in productions and attractions that contains the zone data.

    infill:
        Value to infill missing rows with in productions and attractions when
        new zone may be added in.

    set_index:
        Whether to set the zone_col as the index before returning or not.

    Returns
    -------
    (productions, attractions):
        The provided productions and attractions with all zones from
        unique_zones either as the index, or in zone_col
    """
    # Match productions and attractions
    productions = ensure_index(
        df=productions,
        index=unique_zones,
        index_col=zone_col,
        infill=infill
    )

    attractions = ensure_index(
        df=attractions,
        index=unique_zones,
        index_col=zone_col,
        infill=infill
    )

    if set_index:
        productions = productions.set_index(zone_col)
        attractions = attractions.set_index(zone_col)

    return productions, attractions


def compile_efficient_df(eff_df: List[Dict[str, Any]],
                         col_names: List[Any]
                         ) -> pd.DataFrame:
    """
    Compiles an 'efficient df' and makes it a full dataframe.

    A efficient dataframe is a list of dictionaries, where each dictionary
    contains a df under the key 'df'. All other keys in this dictionary should
    be in the format {col_name, col_val}. All dataframes are expanded with
    the other columns from the dictionary then concatenated together

    Parameters
    ----------
    eff_df:
        Efficient df structure as described in the function description.

    col_names:
        The name and order of columns in the returned compiled_df

    Returns
    -------
    compiled_df:
        eff_df compiled into a full dataframe
    """
    # Init
    concat_ph = list()

    for part_df in eff_df:
        # Grab the dataframe
        df = part_df.pop('df')

        # Add all segmentation cols back into df
        for col_name, col_val in part_df.items():
            df[col_name] = col_val

        # Make sure all dfs are in the same format
        df = df.reindex(columns=col_names)
        concat_ph.append(df)

    return pd.concat(concat_ph).reset_index(drop=True)


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


def create_iter_name(iter_num: Union[int, str]) -> str:
    return 'iter' + str(iter_num)


def convert_to_weights(df: pd.DataFrame,
                       year_cols: List[str],
                       weight_by_col: str = 'p'
                       ) -> pd.DataFrame:
    """
    TODO: write convert_to_weights() doc
    """
    df = df.copy()
    unq_vals = df[weight_by_col].unique()

    for val in unq_vals:
        mask = (df[weight_by_col] == val)
        for year in year_cols:
            df.loc[mask, year] = (
                df.loc[mask, year]
                /
                df.loc[mask, year].sum()
            )
    return df


def purpose_to_user_class(purpose: Union[int, str]) -> str:
    """
    Returns a string of the user class that purpose belongs to

    Parameters
    ----------
    purpose:
        The purpose to convert to user class.

    Returns
    -------
    user_class:
        A string defining a user class
    """
    # Validate the input
    if not isinstance(purpose, int):
        try:
            purpose = int(purpose)
        except ValueError:
            raise ValueError(
                "Given a non-integer purpose and hit an error while trying "
                "to convert to and integer. Got %s" % purpose
            )

    # Convert the purpose
    user_class = None
    for uc, ps in consts.USER_CLASS_PURPOSES.items():
        if purpose in ps:
            user_class = uc

    if user_class is None:
        raise ValueError(
            "No user class exists for purpose '%s' "
            % purpose
        )

    return user_class


def merge_df_list(df_list, **kwargs):
    """
    Merge all dfs in df_list into a single dataframe

    Parameters
    ----------
    df_list:
        The list of dataframes to merge

    kwargs:
        ANy extra arguments to pass straight to pandas.merge()

    Returns
    -------
    merged_df:
        A single df of all items in df_list merged together
    """
    return functools.reduce(lambda l, r: pd.merge(l, r, **kwargs), df_list)


def get_default_kwargs(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
