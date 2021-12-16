import pandas as pd

import land_use.utils.general as utils
from land_use import lu_constants as consts

_standard_index_cols = ['msoa_zone_id', 'area_type', 'tfn_traveller_type', 'people']
_standard_group_cols = ['msoa_zone_id', 'area_type', 'tfn_traveller_type']


def normalised_to_expanded(land_use_data: pd.DataFrame,
                           norm_index: pd.DataFrame = consts.TFN_TT_INDEX,
                           drop_tt=True,
                           verbose=True) -> pd.DataFrame:

    """
    land_use_data: Dataframe of land use data, non-normalised
    norm_index: Path to a dataframe of normalisation params
    drop_tt: remove traveller_type column
    verbose: echo or no

    returns: expanded df
    """
    # Get var name
    var_name = list(land_use_data)[-1]

    total_before = land_use_data[var_name].sum()

    merge_cols = utils.intersection(list(land_use_data),
                                    list(norm_index))

    expanded_df = land_use_data.merge(norm_index,
                                      how='left',
                                      on=merge_cols)

    total_after = land_use_data[var_name].sum()

    if verbose:
        print('traveller type expansion')
        print('%d before, %d after' % (total_before, total_after))

    if drop_tt:
        expanded_df = expanded_df.drop('tfn_traveller_type', axis=1)

    return expanded_df


def expanded_to_normalised(land_use_data: pd.DataFrame,
                           norm_index: pd.DataFrame = consts.TFN_TT_INDEX,
                           var_col='people',
                           verbose=True) -> pd.DataFrame:
    """
    land_use_data: Dataframe of land use data, non-normalised
    norm_index: Path to a dataframe of normalisation params
    verbose: echo or no

    returns: normalised df
    """

    total_before = land_use_data[var_col].sum()

    # Resolve cols to actually index by
    index_cols = _standard_group_cols.copy()
    group_cols = index_cols.copy()
    index_cols.append(var_col)

    # if tt, group sum
    if 'tfn_traveller_type' not in list(land_use_data):
        merge_cols = utils.intersection(list(land_use_data),
                                        list(norm_index))

        land_use_data = land_use_data.merge(norm_index,
                                            how='left',
                                            on=merge_cols)


    # Reindex and sort
    normalised_df = land_use_data.reindex(index_cols, axis=1)
    normalised_df = normalised_df.groupby(
        group_cols).sum().reset_index()
    normalised_df = normalised_df.sort_values(
        group_cols).reset_index(drop=True)

    total_after = normalised_df[var_col].sum()

    if verbose:
        print('traveller type normalisation')
        print('%d before, %d after' % (total_before, total_after))

    return normalised_df


def infill_ntem_tt(land_use_build: pd.DataFrame,
                   traveller_type_lookup=consts.TT_INDEX,
                   attribute_subset=None,
                   left_tt_col='ntem_traveller_type',
                   right_tt_col='ntem_traveller_type'):

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
    # Changed from hardcoded ref to left tt col - may error
    tt_cols = [left_tt_col] + [x for x in tt_cols if x not in lu_cols]
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