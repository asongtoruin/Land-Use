from pathlib import Path
import re

import pandas as pd
import numpy as np
from loguru import logger

from TESTING.preprocessing import utilities as util


def read_rm002(file_path: Path, header_string: str, zoning: str, segmentation: dict) -> pd.DataFrame:
    """

    Parameters
    ----------
    file_path : path to input file
    header_string : string to search for in the header row to identify if a row provides the column headers
    zoning : the zoning level of the input data (e.g. 'lsoa2021') which should match the relevant zoning
        in the ZONING_CACHE
    segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        column names defining the distinct dwelling types

    Returns
    -------
        pd.DataFrame with index of 'h' and column headers of 'zoning' in the correct format to convert to DVector
    """

    df, col_name = util.read_headered_csv(file_path=file_path, header_string=header_string, on_bad_lines='warn')
    df = df.drop(columns=['Whole house or bungalow'])
    df = df.melt(
        id_vars=[col_name],
        value_vars=[col for col in df.columns if col != col_name]
    )

    # define dictionary of segmentation mapping
    inv_seg = {v: k for k, v in segmentation.items()}

    # map the definitions used to define the segmentation, and drop na to remove any
    # missing values in the dataframe (e.g. 'Total' variable)
    df['h'] = df['variable'].map(inv_seg)
    df = df.dropna(subset='h')
    df['h'] = df['h'].astype(int)
    df['households'] = df['value'].astype(int)
    df[zoning] = df[col_name].str.split(' ', expand=True)[0]
    df = df.loc[:, [zoning, 'h', 'households']]
    df = df.set_index([zoning, 'h']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df


def read_ons_custom(file_path: Path, zoning: str, skip_rows: int = 9, **kwargs) -> pd.DataFrame:
    """

    Parameters
    ----------
    file_path :
    zoning :
    skip_rows :
    kwargs :

    Returns
    -------

    """
    logger.info(f'Reading in {file_path}')

    with pd.ExcelFile(file_path) as excel_file:
        data_sheets = [sheet for sheet in excel_file.sheet_names if 'meta' not in sheet.lower()]

        all_data = pd.concat([
            pd.read_excel(excel_file, sheet_name=sheet, skiprows=skip_rows, **kwargs).dropna()
            for sheet in data_sheets
        ])

    if isinstance(all_data.columns, pd.MultiIndex):
        all_data = all_data.melt(value_vars=all_data.columns.tolist(), ignore_index=False).reset_index()
    else:
        all_data = all_data.reset_index()
    all_data.columns.values[0] = zoning

    return all_data


def convert_ons_table_1(df: pd.DataFrame, segmentation: dict, zoning: str) -> pd.DataFrame:
    """

    Parameters
    ----------
    df : Output of read_ons_custom()
    segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        column names starting with 'Unshared'
    zoning : the zoning level of the input data (e.g. 'lsoa2021') which should match the relevant zoning
        in the ZONING_CACHE

    Returns
    -------
        pd.DataFrame with index of 'h' and column headers of 'zoning' in the correct format to convert to DVector
    """

    # define dictionary of segmentation mapping
    inv_segmentation = {v: k for k, v in segmentation.items()}

    # convert to required format for DVec
    df[zoning] = df[zoning].str.split(' ', expand=True)[0]
    # split the shared dwellings across the non-shared building types maintaining zone totals
    cols = [col for col in df.columns if col.startswith('Unshared')]
    df['total'] = df[cols].sum(axis=1)
    for col in cols:
        df[col.split(':')[1].strip()] = df[col] / df['total']
        df[col] = df[col] + (df[col.split(':')[1].strip()] * df['Shared dwelling'])
    df = df.loc[:, [zoning] + cols]

    # reformat so we have columns of dwelling types
    df = df.melt(
        id_vars=[zoning],
        value_vars=[col for col in df.columns if col != zoning]
    )

    # create column of DVector segmentation
    df['h'] = df['variable'].map(inv_segmentation)
    df = df.dropna(subset='h')

    # setting to int here will drop the redistribution of shared dwellings we've done above, maybe not necessary?
    df['population'] = df['value'].astype(int)
    df = df.loc[:, [zoning, 'h', 'population']]
    df = df.set_index([zoning, 'h']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df


def read_abp(file_path: Path, zoning: str, tab: str = 'ClassificationTable') -> pd.DataFrame:
    """
    Read in the AddressBase Premium data

    Parameters
    ----------
    file_path : path to the excel workbook containing the data
    zoning : the zoning level of the input data (e.g. 'lsoa2021') which should match the relevant zoning
        in the ZONING_CACHE
    tab : string of the name of the tab of the excel book to read in, this should have the main datatable in it

    Returns
    -------
        pd.DataFrame with index of 'h' and column headers of 'zoning' in the correct format to convert to DVector
    """
    # read in the excel sheet with the classification table tab defined
    df = util.read_in_excel(file=file_path, tab=tab)

    # remove any unnecessary columns, these are mainly where a correspondence table has been added in the excel book in
    # subsequent columns to the main data table
    df = df.dropna(axis=1)

    # also want to autodetect this column if possible
    zoning_col = 'lsoa21cd'

    # keep only rows which are explicitly `zoning` based data, there seems to be an `Averages` row lumped on the end
    # also exclude scotland from this
    pattern = r'([EW]\d{8})'
    df = df.loc[df[zoning_col].str.match(pattern)]

    # redistribute the unallocated dwelling types to the distribution of the other dwelling types by zone
    pattern = r'RD\d{2}'
    cols = [col for col in df.columns if re.search(pattern, col)]
    for col in cols:
        df[col] = (df[col] + (df['RD'] * (df[col] / df['Total RD'])))

    # reformat so we have column of dwelling types
    df = df.melt(
        id_vars=[zoning_col],
        value_vars=cols
    )

    # map the addressbase dwellings to the ons dwellings
    mapping = {'RD01': 5,
               'RD02': 1,
               'RD03': 2,
               'RD04': 3,
               'RD06': 4,
               'RD07': 5,
               'RD08': 5,
               'RD10': 5
               }
    df['h'] = df['variable'].map(mapping)

    # setting to int here will drop the redistribution of shared dwellings we've done above, maybe not necessary?
    df['dwellings'] = df['value'].astype(int)
    df[zoning] = df[zoning_col]
    df = df.groupby([zoning, 'h']).agg({'dwellings': 'sum'}).reset_index()
    df = df.set_index([zoning, 'h']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df


def convert_ons_table_2(df: pd.DataFrame, dwelling_segmentation: dict, adults_segmentation: dict,
                        children_segmentation: dict, car_segmentation: dict, zoning: str) -> pd.DataFrame:
    """

    Parameters
    ----------
    df : Output of read_ons_custom()
    dwelling_segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'dwelling' types in the ONS custom download dataset
    adults_segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'number of adults in the household' types in the ONS custom download dataset
    children_segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'number of children in the household' types in the ONS custom download dataset
    car_segmentation : dictionary of {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'car availability' types in the ONS custom download dataset
    zoning : the zoning level of the input data (e.g. 'lsoa2021') which should match the relevant zoning
        in the ZONING_CACHE

    Returns
    -------
        pd.DataFrame with index of 'h', 'a', 'c', 'car' and column headers of 'zoning' in the correct format to convert to DVector
    """

    # convert to required format for DVec
    df[zoning] = df[zoning].str.split(' ', expand=True)[0]

    # remap segmentation variables to be consistent with other mappings
    df['h'] = df['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    df['ha'] = df['level_2'].map({v: k for k, v in adults_segmentation.items()})
    df['hc'] = df['variable_0'].map({v: k for k, v in children_segmentation.items()})
    df['car'] = df['variable_1'].map({v: k for k, v in car_segmentation.items()})
    df['households'] = df['value'].astype(int)

    # convert to required format for DVector
    df = df.loc[:, [zoning, 'h', 'ha', 'hc', 'car', 'households']]
    df = df.set_index([zoning, 'h', 'ha', 'hc', 'car']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    # add in the missing segmentation category and fill with zeros
    # TODO this should be genericised, adding in a missing combination of indicies
    missing = df[df.index.get_level_values('h') == 1].reset_index()
    missing['h'] = 5
    missing = missing.set_index(['h', 'ha', 'hc', 'car'])
    missing.loc[:] = np.nan

    # combine with df for all segments
    df = pd.concat([df, missing])

    return df.fillna(0)
