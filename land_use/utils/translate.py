"""
Script to translate land use into model zoning systems
"""
from typing import List

import pandas as pd


def vector_join_translation(lu_data: pd.DataFrame,
                            trans_df: pd.DataFrame,
                            retain_cols: List,
                            join_id: str = None,
                            zone_id: str = None,
                            var_col: str = None,
                            weight_col: str = None,
                            verbose: bool = True):
    """
    Method for translating a land use vector to a model zoning system using
    a join method. Will be superceeded by demand objects.
    Assumes many:1 translation ie. MSOA is lowest level of zoning and doesn't split.

    lu_data:  pd.DataFrame - long format land use data
    trans_df: pd.DataFrame - long format zone correspondence
    sector_heading: str - name of sectors to merge on
    retain_cols: str - any cols from land use to retain in joining
    var_col: str - name of variable to sum on
    """

    # Get join id
    if join_id is None:
        join_id = list(lu_data)[0]
        print('join_id set to last item of cols, %s' % join_id)

    # Get zone id
    if zone_id is None:
        zone_id = list(trans_df)[0]
        print('zone_id set to first item of correspondence, %s' % zone_id)

    # Get var col
    if var_col is None:
        var_col = list(lu_data)[-1]
        print('var_col set to last item of cols, %s' % var_col)

    # Benchmark
    total_before = lu_data[var_col].sum()

    lu_data = lu_data.merge(trans_df,
                            how='left',
                            on=join_id)

    present_retain_cols = [x for x in retain_cols if x in list(lu_data)]

    # Build reindex and group cols
    group_cols = [zone_id]
    [group_cols.append(x) for x in present_retain_cols]
    ri_cols = group_cols.copy()
    ri_cols.append(var_col)

    if weight_col is not None:
        lu_data[var_col] *= lu_data[weight_col]

    # Apply reindex and group cols
    lu_data = lu_data.reindex(ri_cols, axis=1)
    lu_data = lu_data.groupby(group_cols).sum().reset_index()
    lu_data = lu_data.sort_values(group_cols).reset_index(drop=True)

    total_after = lu_data[var_col].sum()

    if verbose:
        print('Total before: %d' % total_before)
        print('Total after: %d' % total_after)
        print('NOTE: Most modelling systems ignore Scottish Islands and Scilly Isles')

    return lu_data
