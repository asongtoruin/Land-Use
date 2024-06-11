import logging
from pathlib import Path
import warnings
import psutil
from warnings import warn
from typing import Any, Dict, List, Union
from functools import reduce

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput, Segment, SegmentsSuper
import pandas as pd
import numpy as np

from land_use.constants import segments, split_input_segments, CUSTOM_SEGMENTS
from land_use.constants.geographies import KNOWN_GEOGRAPHIES


LOGGER = logging.getLogger(__name__)

def add_total_segmentation(dvector: DVector) -> DVector:
    """Function to add a 'total' segmentation to the DVector.

    This does not actually "add" a segmentation, it is just effectively adding a
    common '1' to the index of a DVector to allow the DVector to have calculations
    on it based on the 'total' of each column. This will be used a lot with
    aggregate DVector I think.

    Parameters
    ----------
    dvector : DVector
        DVector to add 'total' segmentation to.

    Returns
    -------
    DVector
        DVector with additional 'total' segmentation (as defined in
        constants.segments.py) with the same zone system as dvector
    """
    # get data from dvector
    df = dvector.data.reset_index()
    df['total'] = 1

    # get zoning from dvector
    zoning = dvector.zoning_system

    # get original segmentation from dvector
    segmentation = dvector.segmentation.seg_dict

    # create total segmentation
    total_segmentation = Segment(name='total', values=segments._CUSTOM_SEGMENT_CATEGORIES['total'])

    # create DVector with the same segmentation, plus the 'total' segmentation,
    # in the same zone system
    # TODO this wont work with default TfN super segments so needs rethinking, this is good enough for now
    names_list = ['total'] + [val for val in segmentation.keys()]
    segment_list = [total_segmentation] + [seg for seg in segmentation.values()]

    new_segmentation_input = SegmentationInput(
        enum_segments=[],
        naming_order=names_list,
        custom_segments=segment_list
    )
    new_segmentation = Segmentation(new_segmentation_input)

    # set new index
    df = df.set_index(names_list)

    return DVector(
        segmentation=new_segmentation,
        zoning_system=zoning,
        import_data=df
    )


def expand_segmentation(
        dvector: DVector,
        segmentation_to_add: Segment
    ) -> DVector:
    """Function to basically copy the values in the existing segmentation of
    dvector and replicate the values for all segmentations within the
    segmentation to add.

    If dvector has segmentation of two categories, like
    {   'index': [1, 2],
        'zone1': [10, 20],
        'zone2': [50, 40]
    }

    and we want to expand the segmentation to another segmentation, with
    definition A B, then the expanded DVector will have the form of
    {   'index': [(1, a), (1, b), (2, a), (2, b)],
        'zone1': [10, 10, 20, 20],
        'zone2': [50, 50, 40, 40]
    }

    Parameters
    ----------
    dvector : DVector
        DVector of a given segmentation
    segmentation_to_add : Segment
        This should be a defined segment, either in the constants\segments.py,
        or in TfN's SuperSegment

    Returns
    -------
    DVector
        DVector with expanded segmentation
    """
    # get segmentation values from the segmentation_to_add
    val_dict = segmentation_to_add.values

    # get segmentation name from the segmentation_to_add
    new_name = segmentation_to_add.name

    # copy and append the data with the expanded segmentation
    expanded_segmentation = pd.concat([
        dvector.data.copy().assign(**{new_name: value}).set_index([new_name], append=True)
        for value in val_dict.keys()
    ])

    # create DVector with the same segmentation, plus the segmentation_to_add
    # segmentation, in the same zone system
    segment_list = list(dvector.segmentation.segments) + [segmentation_to_add]

    standard_segs = [s.name for s in segment_list if segments.is_standard_segment(s.name)]
    custom_segs = [s for s in segment_list if not segments.is_standard_segment(s.name)]

    new_segmentation_input = SegmentationInput(
        enum_segments=standard_segs,
        naming_order=[s.name for s in segment_list],
        custom_segments=custom_segs
    )
    new_segmentation = Segmentation(new_segmentation_input)

    return DVector(
        segmentation=new_segmentation,
        zoning_system=dvector.zoning_system,
        import_data=expanded_segmentation
    )


def replace_segment_combination(
        data: pd.DataFrame,
        segment_combination: Dict[str, List[int]],
        value: Any,
        how: str = 'all',
        include_zeroes: bool = True
    ) -> pd.DataFrame:
    """Function to replace values for specific segmentation (index) values in a
    dataframe.

    The assumption is the data will be in a DVector format with the index as a
    multi-level index of different values
    with index references of strings.

    Parameters
    ----------
    data : pd.DataFrame
        data with index as single or multi-level index in DVector friendly format.
    segment_combination : Dict
        Dictionary of {'index_label': [value1, value2], ... , 'x': [a],
        'y': [b], 'z': [c], ...} where the dictionary keys are the index
        references (i.e. for specific levels of the index) and the dictionary values
        are lists of values for that specific index label.
    value : Any
        value to insert into the dataframe in specific locations (e.g. set all
        values of a given index to 0)
    how : str, default 'all'
        'all' or 'any'. 'all' will change the cells of the dataframe to `value`
        if the segment_combination is satisfied with 'and' logic (i.e. rows of
        the dataframe where index 'x' = a *and* 'y' = b *and* 'z' = c) and 'any' will
        change the cells of the dataframe to 'value' if the segment_combination
        is satisified with 'or' logic (i.e. rows of the dataframe where
        index 'x' = a *or* 'y' = b *or* 'z' = c).
    include_zeroes : bool, default True
        boolean, if True then any zeros in the data (which when .div(False) result
        in np.nan, not np.inf) are infilled with the value provided. If False,
        they are infilled with zeros instead. Not sure why or when infilling
        with zeroes would be preferred, but it's an option anyway.

    Returns
    -------
    pd.DataFrame
        same as input data, but with specific cell values replaced with `value`
        based on the `segment_combination` and `how` provided
    """
    # all is and logic, any is or logic
    if how not in ('all', 'any'):
        raise ValueError(f'parmeter "how" must be either "all" or "any" (got {how})')
    if how == 'all':
        combination_method = np.multiply
    else:
        combination_method = np.add

    # check for any np.inf values in the frame before this
    if data.replace(np.inf, np.nan).isnull().values.sum() > 0:
        raise ValueError(
            'There are null values in your data, please sort before '
            'calling replace_segment_combination()'
        )

    # TODO: check each specified segmentation a) is present in the data, b) *possibly* that the segments are valid for that segmentation

    # Find out which records match our segmentation, aggregate in the appropriate way
    flags = reduce(
        combination_method,
        [
            data.index.isin(segments, level=segmentation)
            for segmentation, segments in segment_combination.items()
        ]
    )

    LOGGER.info(f'Replacing {flags.sum():,.0f} entries')

    # Divide by *inverted* flags (i.e. divide by 0) to get infinity, and replace
    # note: zeroes divided by false = nan not inf
    inverted = data.div(~flags, axis=0).replace(np.inf, value)

    # do you want to replace zeroes with value or no
    if include_zeroes:
        return inverted.fillna(value)
    else:
        return inverted.fillna(0)


def rebalance_zone_totals(
        input_dvector: DVector,
        desired_totals: Union[DVector, pd.Series, float, int]
    ) -> None:
    """Rebalance zone totals to some other value or values.

    Occasionally we may end up with a DVector that has lost some values e.g. due
    to proportions being applied without pre-consideration of exclusions. This
    allows for the zone totals to effectively be "reset", retaining the original
    distribution.

    Parameters
    ----------
    input_dvector : DVector
        The source object requiring rebalanced totals. The _data attribute is
        directly modified in-place
    desired_totals : Union[DVector, pd.Series, float, int]
        The zone totals to match. Can be one of several forms:
        - Another DVector, in which case the zone totals are calculated
        - A Series, with the required ZoneSystem zones as index
        - A constant value (e.g. 1, if proportions need to be rebalanced)
    """

    if isinstance(desired_totals, DVector):
        # Get the total per zone within the DVector
        desired_totals = desired_totals.data.sum(axis=0)
    elif isinstance(desired_totals, (int, float)):
        desired_totals = pd.Series(
            data=desired_totals, index=input_dvector.data.columns
        )

    LOGGER.info(
        f'Adjusting totals from {input_dvector.total:,.0f} '
        f'to {desired_totals.sum():,.0f}'
    )

    scaling_factors = input_dvector.data.sum(axis=0) / desired_totals

    input_dvector._data = input_dvector._data.div(scaling_factors, axis=1)


def _report_memory() -> str:
    current_memuse = psutil.virtual_memory()
    return (
        f'{psutil._common.bytes2human(current_memuse.used)}'
        f'/{psutil._common.bytes2human(current_memuse.total)}'
    )


def clear_dvectors(*dvectors: List[DVector]) -> None:
    logging.info(f'About to clear dataframes, current usage: {_report_memory()}')
    for dvec in dvectors:
        dvec._data = None
    logging.info(f'Finished clearing dataframes, current usage: {_report_memory()}')


def expand_segmentation_to_match(
        dvector: DVector, match_to: DVector, split_method: str = 'duplicate'
    ) -> DVector:
    """Utility function for expanding one DVector to match another's.

    Generally this should be used with proportions, and as a prep stage for 
    combining two DVectors together. Care should be taken, and outputs should
    be checked to ensure they contain the appropriate values.

    Parameters
    ----------
    dvector : DVector
        the DVector to be expanded
    match_to : DVector
        the DVector to match the segmentation to.
    split_method : str, optional
        how to "expand", by default 'duplicate'. The other option is "split",
        i.e. equally split the input values across the additional segments.

    Returns
    -------
    DVector
        dvector expanded to match the segmentation of match_to

    Raises
    ------
    ValueError
        if dvector's segmentation is not a strict subset of match_to's 
        segmentation
    """

    source_segmentation = set(dvector.segmentation.names)
    desired_segmentation = set(match_to.segmentation.names)

    # Can't match if source is not fully contained within desired
    if not source_segmentation < desired_segmentation:

        # If they're the same - nothing to do! Warn the user though, this might
        # suggest something unexpected
        if source_segmentation == desired_segmentation:
            warnings.warn(
                'No segments to add. This seems unexpected, but may be fine. '
                'Returning a copy of the original.'
            )
            return dvector.copy()

        missing_segments = source_segmentation - desired_segmentation
        raise ValueError(
            f'Cannot match segmentation when source segmentation is not fully '
            f'contained in desired segmentation - desired segmentation does not '
            f'feature {missing_segments}'
        )
    
    # Figure out what segments we want to add, and split into standard vs custom
    segment_dict = split_input_segments(
        desired_segmentation - source_segmentation
    )

    if segment_dict[False]:
        warnings.warn(
            f'The following segments seem to be custom: {segment_dict[False]}. '
            f'Ensure this is double-checked'
        )

    # Copy the object, then add in the standard and custom segments respectively
    working = dvector.copy()
    for standard_segment in segment_dict[True]:
        working = working.add_segment(
            SegmentsSuper(standard_segment).get_segment(),
            split_method=split_method
        )
    
    for custom_segment in segment_dict[False]:
        working = working.add_segment(
            CUSTOM_SEGMENTS.get(custom_segment),
            split_method=split_method
        )

    return working

def collapse_segmentation_to_match(
        dvector: DVector, match_to: DVector
    ) -> DVector:
    """Utility function for collapsing one DVector's segmentation to match another's.

    Values are summed when the collapsing is undertaken.

    Parameters
    ----------
    dvector : DVector
        the DVector to be collapsed
    match_to : DVector
        the DVector to take the desired segmentation from

    Returns
    -------
    DVector
        dvector aggregated to the segmentation of match_to

    Raises
    ------
    ValueError
        if match_to's segmentation is not a strict subset of dvector's 
        segmentation
    """
    
    source_segmentation = set(dvector.segmentation.names)
    desired_segmentation = set(match_to.segmentation.names)

    # Can't match if source is not fully contained within desired
    if not source_segmentation > desired_segmentation:

        # If they're the same - nothing to do! Warn the user though, this might
        # suggest something unexpected
        if source_segmentation == desired_segmentation:
            warnings.warn(
                'No segments to aggregate. This seems unexpected, but may be fine. '
                'Returning a copy of the original.'
            )
            return dvector.copy()
        
        missing_segments = desired_segmentation - source_segmentation
        raise ValueError(
            f'Cannot aggregate segmentation when desired segmentation is not fully '
            f'contained in source segmentation - source segmentation does not '
            f'feature {missing_segments}'
        )

    return dvector.aggregate(desired_segmentation)
