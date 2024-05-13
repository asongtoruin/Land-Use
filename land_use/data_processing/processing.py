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

    return DVector(segmentation=new_segmentation, zoning_system=zoning, import_data=df)
