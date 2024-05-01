from pathlib import Path

import pandas as pd

from caf.core.data_structures import DVector
from caf.core.segments import Segment
from caf.core.segmentation import Segmentation, SegmentationInput
import TESTING.constants as cn


def read_dvector_data(file_path, geographical_level, input_segments) -> DVector:
    """

    Parameters
    ----------
    file_path : path to input file in DVector friendly format to read in
    geographical_level : specification of the zone system of the input file,
        TODO this needs to match or be consistent with the geographies defined in constants\geographies.py
    input_segments : segmentation definition of the data to create the zone segmentation with for DVectors

    Returns
    -------
        DVector

    """
    # Get params from the arguments passed from the yaml file
    input_file = Path(file_path)
    zoning = geographical_level
    segmentation = input_segments

    # Read in the file, with the correct geography and segments.
    df = pd.read_hdf(input_file)
    df = pd.DataFrame(df)

    # TODO need to think about how to bring this in for different segmentation definitions
    # This is the default segmentation dictionary for the two RM002 tables
    model_segmentation = {1: "Whole house or bungalow: Detached",
                          2: "Whole house or bungalow: Semi-detached",
                          3: "Whole house or bungalow: Terraced",
                          4: "Flat, maisonette or apartment",
                          5: "A caravan or other mobile or temporary structure"
                          }

    # TODO this needs thinking about, some segmentations will be lists, some strings, some existing, some not
    # Dislike this
    custom_segmentation = [Segment(name=segmentation[0], values=model_segmentation)]
    segmentation_input = SegmentationInput(enum_segments=[], naming_order=segmentation, custom_segments=custom_segmentation)
    resulting_segmentation = Segmentation(segmentation_input)

    return DVector(segmentation=resulting_segmentation, zoning_system=cn.geographies.get(zoning), import_data=df)
