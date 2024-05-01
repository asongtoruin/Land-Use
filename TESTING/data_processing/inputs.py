from pathlib import Path
from warnings import warn

import pandas as pd

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput
import TESTING.constants as cn


def read_dvector_data(file_path: Path, geographical_level: str, input_segments: list, **params) -> DVector:
    """Read DVector friendly data

    Parameters
    ----------
    file_path : Path
        path to input file in DVector friendly format to read in
    geographical_level : str
        specification of the zone system of the input file,
        TODO this needs to match or be consistent with the geographies defined in constants\geographies.py
    input_segments : list
        segmentation definition of the data to create the zone segmentation with for DVectors

    Returns
    -------
        DVector

    """
    # warn if extra stuff is passed unexpedtedly from yaml file
    if params:
        warn('Unexpected parameters passed, please check.\n'
             f'{params.keys()}.')

    # Get params from the arguments passed from the yaml file
    input_file = file_path
    zoning = geographical_level
    segmentation = input_segments

    # Read in the file, with the correct geography and segments.
    df = pd.read_hdf(input_file)
    df = pd.DataFrame(df)

    # TODO this needs thinking about, some segmentations will be lists, some strings, some existing, some not
    # Dislike this
    custom_segmentation = [Segment(name=segmentation[0], values=model_segmentation)]
    # TODO split segments into standard and not standard
    # TODO get non-standard ones from dictionary in segments
    # TODO checks
    segmentation_input = SegmentationInput(enum_segments=[], naming_order=segmentation, custom_segments=custom_segmentation)
    resulting_segmentation = Segmentation(segmentation_input)

    return DVector(segmentation=resulting_segmentation, zoning_system=cn.geographies.get(zoning), import_data=df)
