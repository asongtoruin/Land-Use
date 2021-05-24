"""
Test implementation of matrix compression
from:
https://medium.com/better-programming/load-fast-load-big-with-compressed-pickles-5f311584507e
For further research!
"""
# Builtins
import bz2
import _pickle as cPickle
import pathlib

from typing import Any

# Third party


# Local imports
from land_use.utils import file_ops
import land_use.lu_constants as consts

import land_use as lu

"""
Test use

import os
import pandas as pd

some_mat = 'I:/NorMITs Synthesiser/Noham/iter8c/Distribution Outputs/Compiled OD Matrices/od_m3_business_tp1.csv'
mat_out = pd.read_csv(some_mat)

path = os.path.join(
            os.getcwd(),
            'matrix_name')

dat_out(path,
        mat_out)

in_path = path + '.pbz2'

mat_in = dat_in(in_path)

mat_out == mat_in

Out as 56mb
"""


def write_out(o: Any,
              path: lu.PathLike,
              overwrite_suffix: bool = True
              ) -> pathlib.Path:
    """
    Write the given object o to disk at the given out_path

    The written object will be compressed on write out.

    Parameters
    ----------
    o:
        The object to write to disk. Must be serializable.

    path:
        The path to write out to. If no filetype suffix is provided .pbz2
        is added.

    overwrite_suffix:
        Whether to overwrite the filetype suffix of the given path to the
        default compression suffix or not.

    Returns
    -------
    out_path:
        The output path that o was written to.
    """
    # Init
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)
    path = file_ops.maybe_add_suffix(path, consts.COMPRESSION_SUFFIX, overwrite_suffix)

    with bz2.BZ2File(path, 'w') as f:
        cPickle.dump(o, f)

    return path


def read_in(path: lu.PathLike) -> Any:
    """
    Reads the data at path, decompresses, and returns the object.

    Parameters
    ----------
    path:
        The full path to the object to read

    Returns
    -------
    object:
        The object that was read in from disk.
    """
    return cPickle.load(bz2.BZ2File(path, 'rb'))
