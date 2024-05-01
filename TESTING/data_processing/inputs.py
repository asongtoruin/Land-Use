from pathlib import Path
from warnings import warn

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput
import pandas as pd

from TESTING import constants


def read_dvector_data(
        file_path: Path, 
        geographical_level: str, 
        input_segments: list, 
        **params
    ) -> DVector:
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

    zoning = geographical_level

    # Read in the file, with the correct geography and segments.
    df = pd.read_hdf(file_path)
    df = pd.DataFrame(df)


    segment_flags = constants.segments.split_input_segments(input_segments)

    # "False" are our custom segments - check they exist, error if they don't
    missing_segments = [
        seg for seg in segment_flags[False] 
        if seg not in constants.segments.CUSTOM_SEGMENTS.keys()
    ]
    if missing_segments:
        raise ValueError(f'Undefined segments provided: {",".join(missing_segments)}')

    # Get the hydrated segment objects
    custom_segments = [
        constants.segments.CUSTOM_SEGMENTS.get(seg) 
        for seg in segment_flags[False]
    ]

    # Configure the segmentation
    segmentation_input = SegmentationInput(
        enum_segments=segment_flags[True], 
        custom_segments=custom_segments,
        naming_order=input_segments
    )
    resulting_segmentation = Segmentation(segmentation_input)

    return DVector(
        segmentation=resulting_segmentation, 
        zoning_system=constants.geographies.get(zoning), 
        import_data=df
    )
