import logging

import pandas as pd
from caf.core.data_structures import DVector

LOGGER = logging.getLogger(__name__)


def compare_dvectors(
        dvec1: DVector,
        dvec2: DVector
):
    """

    Parameters
    ----------
    dvec1: DVector

    dvec2: DVector


    Returns
    -------

    """
    # check if DVectors are in the same zone system
    if dvec1.zoning_system != dvec2.zoning_system:
        LOGGER.warning(
            f'DVector 1 has zoning {dvec1.zoning_system} whereas '
            f'DVector2 has zoning {dvec2.zoning_system}. This is not '
            f'a problem but please double check you are comparing the '
            f'right two DVectors.'
        )

    # check if the DVectors have overlapping segmentation, if they don't then
    # raise a warning so the user knows only absolute totals will be reported,
    # nothing more detailed
    common_segs = set(dvec1.segmentation.names).intersection(
        set(dvec2.segmentation.names)
    )

    if len(common_segs) == 0:
        LOGGER.warning(
            f'There are no common segmentations between the two DVectors. DVector '
            f'1 has segmentation {dvec1.segmentation.names}, and DVector 2 has '
            f'segmentation {dvec1.segmentation.names}. Only totals will be '
            f'reported from now on.'
        )

        # report totals
        total1 = dvec1.data.sum().sum()
        total2 = dvec2.data.sum().sum()
        LOGGER.info(f'DVector 1 total: {total1:,.0f}')
        LOGGER.info(f'DVector 2 total: {total2:,.0f}')

    else:
        # calculate totals across all zones
        df1 = dvec1.data.sum(axis=1).rename('total1').reset_index()
        df2 = dvec2.data.sum(axis=1).rename('total2').reset_index()

        # groupby the common segments
        df1 = df1.groupby(list(common_segs)).agg({'total1': 'sum'}).reset_index()
        df2 = df2.groupby(list(common_segs)).agg({'total2': 'sum'}).reset_index()

        # combine to single dataframe and sum totals
        output = pd.concat([df1, df2]).fillna(0).groupby(list(common_segs)).sum()
        output['change'] = output['total2'] - output['total1']

        # report changes
        max_change = output['change'].max()
        min_change = output['change'].min()
        average_change = output['change'].mean()
        LOGGER.info(f'Maximum change: {max_change:,.0f}')
        LOGGER.info(f'Minimum change: {min_change:,.0f}')
        LOGGER.info(f'Average change: {average_change:,.0f}')
