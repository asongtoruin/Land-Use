from pathlib import Path
import shutil
from os import PathLike
from warnings import warn

from loguru import logger
import pandas as pd


# define class to help identify any missing inputs
class InputError(Exception):
    pass


def check_file_exists(file: Path):
    """
    Function to check file exists before being read in
    will raise InputError(Exception) if file is missing

    Parameters
    ----------
    file : Path

    Returns
    -------

    """
    if not file.is_file():
        raise InputError(f'{file} cannot be found')


def make_directory(directory: Path):
    """
    Function to create directory if it doesnt exist

    Parameters
    ----------
    directory : Path
        Directory to create if it doesn't already exist.

    Returns
    -------

    """
    if not directory.is_dir():
        directory.mkdir(parents=True)


def delete_if_exists(path_to_delete: Path):
    """
    Delete a folder or file if it exists. If a folder, delete its entire subtree.

    Parameters
    ----------
    path_to_delete : Path
        Directory or file to delete.

    Returns
    -------

    """
    if not path_to_delete.exists():
        return
    if path_to_delete.is_dir():
        shutil.rmtree(path_to_delete)
    else:
        path_to_delete.unlink()


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
    check_file_exists(file=file)
    if names is not None:
        df = pd.read_excel(file, sheet_name=tab, engine='openpyxl', skiprows=1, names=names)
    else:
        df = pd.read_excel(file, sheet_name=tab, engine='openpyxl', header=0)

    return df


def output_csv(df: pd.DataFrame, output_path: Path, file: str, **kwargs):
    """
    Function to output a pandas dataframe to csv format.
    By default, the index column is output, so **kwargs should be provided to avoid outputting the index column.

    output_path / file is deleted if the file already exists. This will overwrite any existing outputs.

    Parameters
    ----------
    df : pd.DataFrame
    output_path : Path
        This is created if the output target location does not already exist
    file : str
        This is the equivalent to Path.file.name so should include the file extension
    kwargs : any other options to pass to `pd.read_csv`
        Provide index=False if the index column should not be written to file

    Returns
    -------

    """
    logger.info(f'Outputting {file}')
    make_directory(directory=output_path)
    delete_if_exists(path_to_delete=output_path / file)
    df.to_csv(output_path / file, float_format='%.5f', **kwargs)


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
