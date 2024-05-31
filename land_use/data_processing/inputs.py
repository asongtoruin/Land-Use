from pathlib import Path
from warnings import warn
import logging
from typing import Optional, Union

from caf.core.data_structures import DVector
from caf.core.segmentation import Segmentation, SegmentationInput
import pandas as pd

from land_use.constants import segments
from land_use.constants.geographies import KNOWN_GEOGRAPHIES

LOGGER = logging.getLogger(__name__)


def read_dvector_data(
        input_root_directory: Path,
        file_path: Path, 
        geographical_level: str, 
        input_segments: list, 
        geography_subset: Optional[str] = None,
        **params
    ) -> DVector:
    """Read DVector data from an HDF file formatted for input (i.e. "wide").

    File the is read in is `input_root_directory / file_path`.

    Parameters
    ----------
    input_root_directory : Path
        Main path directory where *all* inputs defined in the config will pivot from. This
        encourages the storage of data in one specific location to help with maintenance and
        traceability.
    file_path : Path
        Path to input file in to read in. This must be an HDF file, and should
        be in a suitable structure to be directly passed to `DVector`, i.e. 
        "wide".
    geographical_level : str
        specification of the zone system of the input file
    input_segments : list
        codes corresponding to the various segments contained within the data, 
        in the order they are provided.
    geography_subset: Optional[str], optional
        Subset to filter `geographical_level` to, if desired. Suitable 
        ZonzingSystem files must exist for this subset, and input data will 
        automatically be filtered to only match zones within this subset. By
        default None, in which case no filtering is applied

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

    if geography_subset:
        zoning = f'{geographical_level}_{geography_subset}'
    else:
        zoning = geographical_level

    # Read in the file, with the correct geography and segments.
    LOGGER.info(f'Reading in {Path(input_root_directory) / Path(file_path)}')
    df = pd.read_hdf(Path(input_root_directory) / Path(file_path))
    df = pd.DataFrame(df)

    # filter columns if necessary
    zones = KNOWN_GEOGRAPHIES.get(zoning).zone_ids
    filtered_data = df[zones]
    if len(filtered_data.columns) != len(df.columns):
        LOGGER.warning(
            f'The input data at {Path(input_root_directory) / Path(file_path)} '
            f'started with {len(df.columns):,.0f} columns. Filtering to '
            f' {geography_subset} results in {len(filtered_data.columns):,.0f} '
            'columns.'
        )

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

    # flag which segments are custom
    if segment_flags[False]:
        LOGGER.warning(
            f'Custom segments defined are: {segment_flags[False]}. '
            f'Please check this is what you were expecting.'
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
        import_data=filtered_data
    )


def try_loading_dvector(
        input_file: Path
    ) -> Union[None, DVector]:
    """Function to attempt to load a DVector from a given input file.

    Parameters
    ----------
    input_file : Path
        File path to the input file (assumed to be hdf) to load as a DVector

    Returns
    -------
    None, DVector
        None if the file does not exist
        DVector if the file does exist
    """
    LOGGER.info(f'Attempting to load {input_file} as a DVector')
    # try loading
    try:
        return DVector.load(in_path=input_file)
    except FileNotFoundError:
        return None

