import logging
from pathlib import Path
from warnings import warn
from typing import Any, Dict, List
from functools import reduce

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput, Segment
import pandas as pd
import numpy as np

from land_use.constants import segments
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
