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
        file_path: Path,
        geographical_level: str, 
        input_segments: list, 
        geography_subset: Optional[str] = None,
        input_root_directory: Path = None,
        **params
    ) -> DVector:
    """Read DVector data from an HDF file formatted for input (i.e. "wide").

    File the is read in is `input_root_directory / file_path`.

    Parameters
    ----------
    input_root_directory : Path, optional
        Main path directory where *all* inputs defined in the config will pivot from. This
        encourages the storage of data in one specific location to help with maintenance and
        traceability.
        Default None - in this case the file_path param is assumed to be the full file path.
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
        zoning = f'{geographical_level}-{geography_subset}'
    else:
        zoning = geographical_level

    # Read in the file, with the correct geography and segments.
    if input_root_directory is None:
        input_file = Path(file_path)
    else:
        input_file = Path(input_root_directory) / Path(file_path)
    LOGGER.info(f'Reading in {input_file}')
    # note this key is required in the save_processed_hdf()
    df = pd.read_hdf(input_file, key='df')

    # filter columns if necessary
    zones = KNOWN_GEOGRAPHIES.get(zoning).zone_ids
    filtered_data = df[zones]
    if len(filtered_data.columns) != len(df.columns):
        LOGGER.warning(
            f'The input data at {input_file} '
            f'started with {len(df.columns):,.0f} columns. Filtering to '
            f'{geography_subset} results in {len(filtered_data.columns):,.0f} '
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

    # Ensure we have consistently sorted segments
    sorted_segments = sorted(input_segments)
    if len(sorted_segments) > 1:
        sorted_data = filtered_data.reorder_levels(order=sorted(input_segments))
    else:
        sorted_data = filtered_data

    # Configure the segmentation
    segmentation_input = SegmentationInput(
        enum_segments=segment_flags[True], 
        custom_segments=custom_segments,
        naming_order=sorted_segments
    )
    resulting_segmentation = Segmentation(segmentation_input)

    return DVector(
        segmentation=resulting_segmentation, 
        zoning_system=KNOWN_GEOGRAPHIES.get(zoning),
        import_data=sorted_data,
        cut_read=True
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


def read_dvector_from_config(
        config: dict,
        data_block: str,
        key: str,
        **kwargs
) -> Union[DVector, list]:
    """Read DVector format input data (assumed to be HDF format) using
    information directly from the config file.

    Parameters
    ----------
    config: dict
        Config dictionary loaded directly from reading the yml file.
    data_block: dict
        Name of the block of data (i.e. the top level keys of the config) that
        contains `key`
    key: str
        Name of the table, or list of tables, in the config file.

    Returns
    -------
    DVector
        Calls read_dvector_data() and returns DVector, or a list of DVectors.

    """
    # check if the data_block key exists
    try:
        config[data_block]
    except KeyError:
        LOGGER.error(f'"{data_block}" is not a first level key in your config '
                     f'yaml file, and therefore nothing can be read from it. '
                     f'Please make sure the data_block parameter is a valid '
                     f'key from your config.')

    # after checking the data_block exists, check for the key
    try:
        config[data_block][key]
    except KeyError:
        LOGGER.error(f'"{key}" is not a second level key in your config '
                     f'within the "{data_block}" group in the yaml file. '
                     f'Please make sure the key parameter is a valid '
                     f'key from your "{data_block}" config.')

    # if the config has a list of data within a given key
    if isinstance(config[data_block][key], list):
        return [
            read_dvector_data(
                input_root_directory=config['input_root_directory'],
                **config[data_block][key][i],
                **kwargs
            )
            for i, _ in enumerate(
                config[data_block][key]
            )
        ]

    # else just return a normal DVector
    return read_dvector_data(
        input_root_directory=config['input_root_directory'],
        **config[data_block][key],
        **kwargs
    )
