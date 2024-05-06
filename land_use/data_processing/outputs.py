import pandas as pd

from caf.core.data_structures import DVector


def dvector_to_long(dvec: DVector, value_name: str = 'value') -> pd.DataFrame:
    """Function to convert a DVector.data to a long format dataframe.

    The DVector.data is assumed to have either a single or multilevel index 
    detailing the segmentation of the data, and the column names are the zone 
    system of the data.

    Parameters
    ----------
    dvec : DVector
        DVector to export
    value_name : str, optional
        String to rename the numeric column of data to. This is likely to be 
        something like 'population', 'households', 'jobs' etc and is just to 
        help the user to interpret the output. By default, "value"

    Returns
    -------
    pd.DataFrame
        with columns corresponding to each level of segmentation (both as 
        numeric values and "_description" for the descriptions), the zone ID and
        the value
    """
    # get dataframe from dvec
    data = dvec.data.T.melt(ignore_index=False)

    # get segment names from dvec
    # TODO check this is the order specified for multi index dataframes
    # TODO does it need to be segmentation.naming_order or segmentation.segments?
    names = dvec.segmentation.naming_order

    # map the segmentation values to the actual descriptions for ease
    for segment in names:
        data[f'{segment}_description'] = data[segment].map(
            dvec.segmentation.seg_dict.get(segment).values
        )

    # move value column to the last column and rename to specified value
    data[value_name] = data.pop('value')

    return data.reset_index(names=['zone'])

