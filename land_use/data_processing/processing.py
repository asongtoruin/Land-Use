from pathlib import Path
from warnings import warn

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput, Segment
import pandas as pd

from land_use.constants import segments
from land_use.constants.geographies import KNOWN_GEOGRAPHIES


def add_total_segmentation(dvector: DVector) -> DVector:
    """Function to add a 'total' segmentation to the DVector.

    This does not actually "add" a segmentation, it is just effectively adding a common '1' to the index of a DVector
    to allow the DVector to have calculations on it based on the 'total' of each column. This will be used a lot with
    aggregate DVector I think.

    Parameters
    ----------
    dvector : DVector
        DVector to add 'total' segmentation to.

    Returns
    -------
    DVector
        DVector with additional 'total' segmentation (as defined in constants.segments.py) with the same zone system as dvector
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

    # create DVector with the same segmentation, plus the 'total' segmentation, in the same zone system
    # TODO this wont work with default TfN super segments so needs rethinking, this is good enough for now
    names_list = ['total'] + [val for val in segmentation.keys()]
    segment_list = [total_segmentation] + [seg for seg in segmentation.values()]

    new_segmentation_input = SegmentationInput(enum_segments=[], naming_order=names_list, custom_segments=segment_list)
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
    """Function to basically copy the values in the existing segmentation of dvector and replicate the values for all
    segmentations within the segmentation to add.

    If dvector has segmentation of two categories, like
    {   'index': [1, 2],
        'zone1': [10, 20],
        'zone2': [50, 40]
    }

    and we want to expand the segmentation to another segmentation, with definition A B, then the expanded DVector
    will have the form of
    {   'index': [(1, a), (1, b), (2, a), (2, b)],
        'zone1': [10, 10, 20, 20],
        'zone2': [50, 50, 40, 40]
    }

    Parameters
    ----------
    dvector : DVector
        DVector of a given segmentation
    segmentation_to_add : Segment
        This should be a defined segment, either in the constants\segments.py, or in TfN's SuperSegment

    Returns
    -------
    DVector
        DVector with expanded segmentation
    """
    # get segmentation values from the segmentation_to_add
    val_dict = segmentation_to_add.values

    # get current segmentation names of dvector
    current_seg_names = dvector.segmentation.names

    # get current data of dvector
    data = dvector.data.reset_index()

    # add the segment definitions from the segmentation to add
    dfs = []
    for value in val_dict.keys():
        df = data.copy()
        df[segmentation_to_add.name] = value
        dfs.append(df)

    # create output data with the additional segmentation
    output = pd.concat(dfs)

    # convert to data ready for DVector
    dvec = output.set_index(current_seg_names + [segmentation_to_add.name])

    # create DVector with the same segmentation, plus the segmentation_to_add segmentation, in the same zone system
    # TODO this wont work with default TfN super segments so needs rethinking, this is good enough for now?
    names_list = current_seg_names + [segmentation_to_add.name]
    segment_list = [segmentation_to_add] + [seg for seg in dvector.segmentation.segments]

    new_segmentation_input = SegmentationInput(enum_segments=[], naming_order=names_list, custom_segments=segment_list)
    new_segmentation = Segmentation(new_segmentation_input)

    return DVector(
        segmentation=new_segmentation,
        zoning_system=dvector.zoning_system,
        import_data=dvec
    )

