import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import logging
from pathlib import Path
import re

import numpy as np
import pandas as pd

from .utilities import read_headered_csv, read_in_excel


def read_rm002(
        file_path: Path, 
        header_string: str, 
        zoning: str, 
        segmentation: dict
    ) -> pd.DataFrame:
    """

    Parameters
    ----------
    file_path : Path
        path to input file
    header_string : str
        text to search for in the header row to identify if a row provides the 
        column headers
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match 
        the relevant zoning in the ZONING_CACHE
    segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        column names defining the distinct dwelling types

    Returns
    -------
    pd.DataFrame 
        with index of 'h' and column headers of 'zoning' in the correct format 
        to convert to DVector
    """

    df, col_name = read_headered_csv(
        file_path=file_path, header_string=header_string, on_bad_lines='warn'
    )

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


def read_ons_custom(
        file_path: Path, 
        zoning: str, 
        skip_rows: int = 9, 
        **kwargs
    ) -> pd.DataFrame:
    """Reading in the custom download ONS tables.

    These tend to be formatted consistently with a different region on each tab,
    with leading and trailing lines sandwiching the data.

    Parameters
    ----------
    file_path : Path
        _description_
    zoning : str
        _description_
    skip_rows : int, optional
        _description_, by default 9

    Returns
    -------
    pd.DataFrame
        _description_
    """
    logging.info(f'Reading in {file_path}')

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


def convert_ons_table_1(
        df: pd.DataFrame, 
        segmentation: dict, 
        zoning: str
    ) -> pd.DataFrame:
    """

    Parameters
    ----------
    df : pd.DataFrame
        Output of read_ons_custom()
    segmentation : dict
        dictionary of {1: category1, 2: category2, ...} where the 
        segmentation categories are column names starting with 'Unshared'
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021'), which should 
        match the relevant zoning in the ZONING_CACHE

    Returns
    -------
    pd.DataFrame 
        with index of 'h' and column headers of 'zoning' in the correct format 
        to convert to DVector
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


def read_abp(
        file_path: Path, 
        zoning: str, 
        sheet_name: str = 'ClassificationTable'
    ) -> pd.DataFrame:
    """Read in the AddressBase Premium data

    Parameters
    ----------
    file_path : Path
        path to the excel workbook containing the data
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match 
        the relevant zoning in the ZONING_CACHE
    sheet_name : str
        string of the name of the tab of the excel book to read in, this should 
        have the main datatable in it

    Returns
    -------
    pd.DataFrame 
        with index of 'h' and column headers of 'zoning' in the correct format 
        to convert to DVector
    """
    # read in the excel sheet with the classification table tab defined
    df = read_in_excel(file=file_path, tab=sheet_name)

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
    mapping = {
        'RD01': 5,
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


def convert_ons_table_2(
        df: pd.DataFrame, 
        dwelling_segmentation: dict, 
        adults_segmentation: dict,
        children_segmentation: dict, 
        car_segmentation: dict, 
        zoning: str
    ) -> pd.DataFrame:
    """

    Parameters
    ----------
    df : pd.DataFrame
        Ideally, the output of a `read_ons_custom` call.
    dwelling_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'dwelling' types in the ONS custom download dataset
    adults_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'number of adults in the household' types in the 
        ONS custom download dataset
    children_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'number of children in the household' types in the 
        ONS custom download dataset
    car_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'car availability' types in the ONS custom download 
        dataset
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match 
        the relevant zoning in the ZONING_CACHE

    Returns
    -------
    pd.DataFrame 
        with index of 'h', 'a', 'c', 'car' and column headers of 'zoning' in the 
        correct format to convert to DVector
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


def read_mype(
        file_path: Path,
        zoning: str,
        age_mapping: dict,
        gender_mapping: dict,
        sheet_name: str = 'Mid-2022 LSOA 2021',
        skip_rows: int = 3
    ) -> pd.DataFrame:
    """
    Read in the AddressBase Premium data

    Parameters
    ----------
    file_path : Path
        path to the excel workbook containing the data
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match
        the relevant zoning in the ZONING_CACHE
    age_mapping : dict

    gender_mapping : dict

    sheet_name : str
        string of the name of the tab of the excel book to read in, this should
        have the main datatable in it
    skip_rows : int
        Number of rows of the excel workbook to skip before reaching the header row.

    Returns
    -------
    pd.DataFrame
        with index of 'h' and column headers of 'zoning' in the correct format
        to convert to DVector
    """
    # read in the excel sheet with the tab defined
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows).dropna()

    # also want to autodetect this column if possible
    zoning_col = 'LSOA 2021 Code'
    df[zoning] = df[zoning_col]

    # melt so the columns of gender and age become distinct long columns
    pattern = r'([MF]\d{1,2})'
    melted = df.melt(
        id_vars=[zoning],
        value_vars=[col for col in df.columns if re.match(pattern, col)],
        var_name='gender_age',
        value_name='population'
    )
    melted['gender'] = melted['gender_age'].str[:1]
    melted['age'] = melted['gender_age'].str[1:].astype(int)

    # define age groups to match with the "age" segments
    age_groups = [0, 5, 10, 16, 20, 35, 50, 65, 75, 999]
    labels = ["0 to 4 years", "5 to 9 years", "10 to 15 years", "16 to 19 years", "20 to 34 years", "35 to 49 years",
              "50 to 64 years", "65 to 74 years", "75+ years"]
    melted['age_band'] = pd.cut(
        melted['age'],
        age_groups,
        labels=labels,
        include_lowest=True
    ).astype(str)

    # remap male and female to definitions in the segments
    melted.loc[melted['gender'] == 'F', 'gender_seg'] = 'female'
    melted.loc[melted['gender'] == 'M', 'gender_seg'] = 'male'

    # remap based on segmentations
    melted['age'] = melted['age_band'].map({v: k for k, v in age_mapping.items()})
    melted['gender'] = melted['gender_seg'].map({v: k for k, v in gender_mapping.items()})

    # group by gender and age band and sum total population by LSOA
    totalled = melted.groupby([zoning, 'age', 'gender'], as_index=False).agg({'population': 'sum'})

    # set index column to gender/age
    df = totalled.set_index([zoning, 'gender', 'age']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df


def convert_ons_table_4(
        df: pd.DataFrame,
        dwelling_segmentation: dict,
        ns_sec_segmentation: dict,
        zoning: str
    ) -> pd.DataFrame:
    """

    Parameters
    ----------
    df : pd.DataFrame
        Ideally, the output of a `read_ons_custom` call.
    dwelling_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'dwelling' types in the ONS custom download dataset
    ns_sec_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'HRP NS-SeC' types in the
        ONS custom download dataset
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match
        the relevant zoning in the ZONING_CACHE

    Returns
    -------
    pd.DataFrame
        with index of 'h', 'ns_sec' and column headers of 'zoning' in the
        correct format to convert to DVector
    """

    # convert to required format for DVec
    df[zoning] = df[zoning].str.split(' ', expand=True)[0]

    # remap segmentation variables to be consistent with other mappings
    melted = df.melt(
        id_vars=[zoning, 'level_1'],
        value_vars=[col for col in df.columns if col != zoning if col != 'level_1'],
        value_name='hh_ref_persons',
        var_name='ns_sec_category'
    )
    melted['h'] = melted['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    melted['ns_sec'] = melted['ns_sec_category'].map({v: k for k, v in ns_sec_segmentation.items()})
    melted['hh_ref_persons'] = melted['hh_ref_persons'].astype(int)

    # convert to required format for DVector
    dvec = melted.loc[:, [zoning, 'h', 'ns_sec', 'hh_ref_persons']]
    dvec = dvec.set_index([zoning, 'h', 'ns_sec']).unstack(level=[zoning])
    dvec.columns = dvec.columns.get_level_values(zoning)

    # add in the missing segmentation category and fill with zeros
    # TODO this should be genericised, adding in a missing combination of indicies
    missing = dvec[dvec.index.get_level_values('h') == 1].reset_index()
    missing['h'] = 5
    missing = missing.set_index(['h', 'ns_sec'])
    missing.loc[:] = np.nan

    # combine with df for all segments
    df = pd.concat([dvec, missing])

    return df.fillna(0)


def convert_ons_table_3(
        df: pd.DataFrame,
        dwelling_segmentation: dict,
        ns_sec_segmentation: dict,
        all_segmentation: dict,
        zoning: str
    ) -> dict:
    """

    Parameters
    ----------
    df : pd.DataFrame
        Ideally, the output of a `read_ons_custom` call.
    dwelling_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'dwelling' types in the ONS custom download dataset
    ns_sec_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'HRP NS-SeC' types in the
        ONS custom download dataset
    all_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'SOC' types in the
        ONS custom download dataset
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match
        the relevant zoning in the ZONING_CACHE

    Returns
    -------
    dict
        Dictionary of three dataframes with index of 'h', 'ns_sec', and 'pop_soc' or 'pop_emp' or 'pop_econ' and column headers of 'zoning' in the
        correct format to convert to DVector
    """

    # convert to required format for DVec
    df[zoning] = df[zoning].str.split(' ', expand=True)[0]

    # remap dwelling and ns-sec segmentation variables to be consistent with other mappings
    df['h'] = df['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    df['ns_sec'] = df['variable_1'].map({v: k for k, v in ns_sec_segmentation.items()})

    # convert all segmentation to dataframe and merge with original data frame
    tmp = pd.DataFrame(all_segmentation).transpose()
    merged = pd.merge(df, tmp, left_on='variable_0', right_index=True, how='left')

    # set observation column to numeric
    merged['population'] = merged['value'].astype(int)

    return_dict = {}
    for col in tmp.columns:
        grouped = merged.groupby([zoning, 'h', 'ns_sec', col]).agg({'population': 'sum'}).reset_index()

        # convert to required format for DVector
        dvec = grouped.loc[:, [zoning, 'h', 'ns_sec', col, 'population']]
        dvec = dvec.set_index([zoning, 'h', 'ns_sec', col]).unstack(level=[zoning])
        dvec.columns = dvec.columns.get_level_values(zoning)

        # add in the missing segmentation category and fill with zeros
        # TODO this should be genericised, adding in a missing combination of indicies
        missing = dvec[dvec.index.get_level_values('h') == 1].reset_index()
        missing['h'] = 5
        missing = missing.set_index(['h', 'ns_sec', col])
        missing.loc[:] = np.nan

        # combine with df for all segments
        grouped = pd.concat([dvec, missing])

        # add to output
        return_dict[col] = grouped.fillna(0)

    return return_dict


def read_ons(
        file_path: Path,
        zoning: str,
        zoning_column: str,
        segment_mappings: dict,
        obs_column: str = 'Observation'
    ) -> pd.DataFrame:
    """Reading in a generic ONS data download csv.

    These are *typically* a similar format with no leading or trailing lines, and various categories of segmentation
    defined by the user when downloaded.

    The 'value' column of the downloads (i.e. maybe population, or households, or whatever the unit of observations is)
    is assumed to be "Observation". This will raise an error if this column does not exist and hasn't been provided as
    something else by the user.

    Parameters
    ----------
    file_path : Path
        csv file of the downloaded data
    zoning : str
        zoning as in constants/segments.py which the zoning_column will be renamed to
    zoning_column : str
        column to rename to standard zoning
    segment_mappings : dict
        dictionary of   {
                        column_name_1: [segment_ref, {value1: mapping1, value2: mapping2, ...}],
                        column_name_2: [segment_ref, {value1: mapping1, value2: mapping2, ...}],
                        ...}
        to map various columns to different segmentations.
    obs_column : str, optional
        column name containing the unit of the data (e.g. population or households or whatever), by default 'Observation'

    Returns
    -------
    pd.DataFrame
        DVector friendly format with multi index based on the segment_refs and columns defined by zoning
    """
    # read in csv
    df = pd.read_csv(file_path)

    # check obs_column exists in the data before trying to do anything else
    if obs_column not in df.columns:
        raise RuntimeError(f'{obs_column} is not in the input data. '
                           f'Please check your data and provide a custom column name if required.')

    # rename zoning_column based on the constant zoning provided
    df[zoning] = df[zoning_column]

    # go through the dictionary and remap all the values based on the segmentation provided
    # columns are named based on the segment_ref provided
    refs = [zoning]
    for column, [ref, remapping] in segment_mappings.items():
        df[ref] = df[column].str.lower().map({v.lower(): k for k, v in remapping.items()})
        refs.append(ref)

    # convert to required format for DVector
    dvec = df.loc[:, refs + [obs_column]].dropna()
    dvec = dvec.set_index(refs).unstack(level=[zoning])
    dvec.columns = dvec.columns.get_level_values(zoning)

    return dvec
