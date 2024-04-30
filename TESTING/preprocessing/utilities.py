from pathlib import Path
from os import PathLike
from warnings import warn

import pandas as pd
from loguru import logger

from TESTING import util


def find_header_line(file_path: PathLike, header_string: str, max_lines: int = 100) -> (int, str):
    """
    Identifies (0-indexed) row within a file that contains some header text.

    Most useful for files that have some kind of information text stored above
    column headers that indicate the start of the actual data.

    Raises RuntimeError if `header_start` cannot be found in the first `max_lines` lines of
    text or the entirety of the file, whichever comes first.

    Parameters
    ----------
    file_path : PathLike
        File to search through
    header_string : str
        Text witin the "column header" line. Note this will be converted
          to lowercase by the function.
    max_lines : int, optional
        maximum number of line to check for the header text. Defaults to 100.

    Returns
    -------
    int: (0-indexed) number of the first line within the file that begins with
        `header_start`

    """
    if len(header_string) < 2:
        warn(
            f'A very short suggested header text ("{header_string}") has been provided'
            f' - a starting string of at least 2 characters is recommended.'
        )

    # Convert starting text to lowercase
    header_low = header_string.lower()

    with open(file_path, 'r') as input_file:
        for i, line in enumerate(input_file):
            if header_low in line.lower():
                # get the section of the string (assumed to be comma delimited because we're reading csv)
                # that contains header_low and pass that back to use in subsequent stuff
                list_of_strings = line.split(',')
                list_of_relevant_strings = [item for item in list_of_strings if header_low in item.lower()]
                if len(list_of_relevant_strings) > 1:
                    warn(
                        f'Multiple column names in this file contain the string "{header_string}", these are {list_of_relevant_strings}.'
                        f'By default this will return the first relevant column name: {list_of_relevant_strings[0]}.'
                        f'If this is picking up the wrong column, please refine your input string.'
                    )
                # Exit on first match
                return i, list_of_relevant_strings[0].replace('"', '').strip()

            if i >= max_lines:
                break

        # Determine whether we're here because we've finished the file or not
        try:
            # If we can get another line from the file, we've not reached the end
            next(input_file)
            extra_text = f' the first {max_lines} lines of '
        except StopIteration:
            # Otherwise, we've maxed out the file.
            extra_text = ' '

    # Raise the error
    raise RuntimeError(
        f'Unable to find "{header_string}" in{extra_text}{file_path}'
    )


def read_headered_csv(file_path: PathLike, header_string: str, **kwargs) -> (pd.DataFrame, str):
    """
    Reads a CSV (or similarly-delimited file), skipping metadata stored before actual data

    Parameters
    ----------
    file_path : PathLike
        path to the file to read
    header_string : str
        text within the "column header" line. Note this will be converted
        to lowercase by the function.
    kwargs : any other options to pass to `pd.read_csv`

    Returns
    -------
    pd.DataFrame:
          dataframe of the data found after the header row, with the header row
          as column names

    """
    logger.info(f'Reading in {file_path}')
    skip_rows, col_name = find_header_line(file_path, header_string)

    return pd.read_csv(file_path, skiprows=skip_rows, **kwargs).dropna(), col_name


def read_in_excel(file: Path, tab: str, names: list = None) -> pd.DataFrame:
    """
    Reads in a spreadsheet worksheet based on the tab name provided

    Parameters
    ----------
    file : Path
        Path to directory containing the spreadsheet
    tab : str
        Tab name within spreadsheet to read in as data.
        This tab name must exist in the excel book otherwise this function will fail.
    names : list of strings
        If the column headers in the spreadsheet are not suitable then a list here can be provided and
    prescribed and skiprows=1 will be applied. The length of this list must match the number of expected columns in
    the data that is being read in, otherwise pandas will throw an error.

    Returns
    -------
    pd.DataFrame: Dataframe of the data, either with headers supplied by the data itself, or by the names list provided.
    """

    logger.info(f'Reading in {file}')
    util.check_file_exists(file=file)
    if names is not None:
        df = pd.read_excel(file, sheet_name=tab, engine='openpyxl', skiprows=1, names=names)
    else:
        df = pd.read_excel(file, sheet_name=tab, engine='openpyxl', header=0)

    return df


def save_to_hdf(file_path: Path, df: pd.DataFrame):
    """

    This function will save a dataframe to HDF5 format.

    The output file location will be a subfolder in the file_path location named 'preprocessing'
    and the file name will have the same name as the file_path file.

    This is done to help maintain the link between the original input file (maybe csv or excel)
    and the converted output file.

    Parameters
    ----------
    file_path : PathLike
        File path to the input file that has been read in and converted into the DVector-readable
        format by one of the parsing functions.

    df : pd.DataFrame
        Data to be saved in HDF5 format with the same name as file_path.

    Returns
    -------

    """
    output_folder = file_path.parent / 'preprocessing'
    util.make_directory(output_folder)

    filename = f'{file_path.name.split(".")[0]}.hdf'
    logger.info(f'Writing to {output_folder / filename}')
    df.to_hdf(output_folder / filename, key='df', mode='w')

