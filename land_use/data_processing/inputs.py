from pathlib import Path
from warnings import warn

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput
import pandas as pd

from land_use.constants import segments
from land_use.constants.geographies import KNOWN_GEOGRAPHIES


def read_dvector_data(
        file_path: Path, 
        geographical_level: str, 
        input_segments: list, 
        **params
    ) -> DVector:
    """Read DVector data from an HDF file formatted for input (i.e. "wide")

    Parameters
    ----------
    file_path : Path
        path to input file in to read in. This must be an HDF file, and should
        be in a suitable structure to be directly passed to `DVector`, i.e. 
        "wide"
    geographical_level : str
        specification of the zone system of the input file
    input_segments : list
        codes corresponding to the various segments contained within the data, 
        in the order they are provided.

    Returns
    -------
    DVector
        object representing the geographical, segmented data ready to be used by
        standard caf processes
    
    Raises
    ------
    ValueError
        if undefined segments are passed to the function
    """
    # TODO make sure geographical_level is consistent with the KNOWN_GEOGRAPHIES defined in constants\geographies.py

    # warn if extra stuff is passed unexpedtedly from yaml file
    if params:
        warn(
            f'Unexpected parameters passed, please check - {params.keys()}.'
        )

    zoning = geographical_level

    # Read in the file, with the correct geography and segments.
    df = pd.read_hdf(file_path)
    df = pd.DataFrame(df)

    # get flags for segments that are or are not TfN standard super segments
    segment_flags = segments.split_input_segments(input_segments)

    # "False" are our custom segments - check they exist, error if they don't
    missing_segments = [
        seg for seg in segment_flags[False] 
        if seg not in segments.CUSTOM_SEGMENTS.keys()
    ]
    if missing_segments:
        raise ValueError(
            f'Undefined segments provided: {",".join(missing_segments)}'
        )

    # Get the hydrated segment objects
    custom_segments = [
        segments.CUSTOM_SEGMENTS.get(seg) 
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
        zoning_system=KNOWN_GEOGRAPHIES.get(zoning),
        import_data=df
    )
