import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import logging
from pathlib import Path
import re

import pandas as pd

from .utilities import read_headered_csv, read_in_excel, pivot_to_dvector


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
        with index of 'h' and column headers of 'zoning' type, 
        given by the first entry for each row in the column which includes 'header_string'
        in the correct format to convert to DVector
    """

    df, col_name = read_headered_csv(
        file_path=file_path, header_string=header_string, on_bad_lines='warn'
    )

    # drop total row (sum of all the other columns we're interested in)
    df = df.drop(columns=['Whole house or bungalow'])
    df = df.melt(id_vars=[col_name])

    # define dictionary of segmentation mapping
    inv_seg = {v: k for k, v in segmentation.items()}

    # map the definitions used to define the segmentation, and drop na to remove any
    # missing values in the dataframe (e.g. 'Total' variable)
    df['accom_h'] = df['variable'].map(inv_seg)
    df = df.dropna(subset='accom_h')
    df['accom_h'] = df['accom_h'].astype(int)
    df['households'] = df['value'].astype(int)

    # take first 'word' within the col_name as the zone
    df[zoning] = df[col_name].str.split(' ', expand=True)[0]

    df = pivot_to_dvector(
        data=df,
        zoning_column=zoning,
        index_cols=['accom_h'],
        value_column='households'
    )

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
        data_sheets = [sheet for sheet in excel_file.sheet_names if 'meta' not in str(sheet).lower()]

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

    # define dictionary of segmentation mapping
    inv_segmentation = {v: k for k, v in segmentation.items()}
    
    # create column of DVector segmentation
    df['accom_h'] = df['variable'].map(inv_segmentation)
    df = df.dropna(subset='accom_h')

    # setting to int here will drop the redistribution of shared dwellings we've done above, maybe not necessary?
    df['population'] = df['value'].astype(int)
    df = pivot_to_dvector(
        data=df,
        zoning_column=zoning,
        index_cols=['accom_h'],
        value_column='population'
    )

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
    # TODO: consider what happens if this doesn't match any records?
    pattern = r'([EW]\d{8})'
    df = df.loc[df[zoning_col].str.match(pattern)]

    # redistribute the unallocated dwelling types to the distribution of the other dwelling types by zone
    # TODO: consider what happens if this doesn't match any records?
    pattern = r'RD\d{2}'
    cols = [col for col in df.columns if re.search(pattern, col)]
    # (re)calculate total dwellings across all types, for some reason the 2023
    # data do not have this total, but the 2021 data do
    df['Total RD'] = df[cols].sum(axis=1)
    for col in cols:
        df[col] = (df[col] + (df['RD'] * (df[col] / df['Total RD'])))

    # reformat so we have column of dwelling types
    df = df.melt(
        id_vars=[zoning_col],
        value_vars=cols
    )

    # map the addressbase dwellings to the ons dwellings
    # categories RD05 and RD09 are just not in data input
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
    df['accom_h'] = df['variable'].map(mapping)

    # setting to int here will drop the redistribution of shared dwellings we've done above, maybe not necessary?
    df['dwellings'] = df['value'].astype(int)
    df[zoning] = df[zoning_col]
    df = df.groupby([zoning, 'accom_h']).agg({'dwellings': 'sum'}).reset_index()
    df = pivot_to_dvector(
        data=df,
        zoning_column=zoning,
        index_cols=['accom_h'],
        value_column='dwellings'
    )

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
    df['accom_h'] = df['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    df['adults'] = df['level_2'].map({v: k for k, v in adults_segmentation.items()})
    df['children'] = df['variable_0'].map({v: k for k, v in children_segmentation.items()})
    df['car_availability'] = df['variable_1'].map({v: k for k, v in car_segmentation.items()})
    df['households'] = df['value'].astype(int)

    # convert to required format for DVector
    # Altenative using pivot_wider
    # index_cols = ["h", "ha", "hc", "car"]
    # df = df.pivot(index=index_cols, columns=["zoning"], values="households")
    dvec = pivot_to_dvector(
        data=df,
        zoning_column=zoning,
        index_cols=['accom_h', 'adults', 'children', 'car_availability'],
        value_column='households'
    )

    # caravan data is missing from data source
    # assume caravan data is the same as flat data (used for proportions mainly so
    # so shouldn't be too much of a problem??
    # TODO this should be genericised, adding in a missing combination of indicies
    # TODO REVIEW THIS ASSUMPTION, IS THIS OKAY?
    missing = dvec[dvec.index.get_level_values('accom_h') == 4].reset_index()
    missing['accom_h'] = 5
    missing = missing.set_index(
        ['accom_h', 'adults', 'children', 'car_availability']
    )

    return pd.concat([dvec, missing]).fillna(0)


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
    melted['accom_h'] = melted['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    melted['ns_sec'] = melted['ns_sec_category'].map({v: k for k, v in ns_sec_segmentation.items()})
    melted['hh_ref_persons'] = melted['hh_ref_persons'].astype(int)

    # convert to required format for DVector
    dvec = pivot_to_dvector(
        data=melted,
        zoning_column=zoning,
        index_cols=['accom_h', 'ns_sec'],
        value_column='hh_ref_persons'
    )

    # caravan data is missing from data source
    # assume caravan data is the same as flat data (used for proportions mainly so
    # so shouldn't be too much of a problem??
    # TODO this should be genericised, adding in a missing combination of indicies
    # TODO REVIEW THIS ASSUMPTION, IS THIS OKAY?
    missing = dvec[dvec.index.get_level_values('accom_h') == 4].reset_index()
    missing['accom_h'] = 5
    missing = missing.set_index(
        ['accom_h', 'ns_sec']
    )

    return pd.concat([dvec, missing]).fillna(0)


def convert_ons_table_3(
        df: pd.DataFrame,
        dwelling_segmentation: dict,
        ns_sec_segmentation: dict,
        all_segmentation: dict,
        zoning: str,
        ages: pd.DataFrame,
        economic_status: pd.DataFrame
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
    all_segmentation : dict
        {1: category1, 2: category2, ...} where the segmentation categories are
        the column values of 'SOC' types in the
        ONS custom download dataset
    zoning : str
        the zoning level of the input data (e.g. 'lsoa2021') which should match
        the relevant zoning in the ZONING_CACHE
    ages : pd.DataFrame
        DataFrame of [zoning, 'factor'] based on how to deflate the economically
         inactive economic status to exclude 75+s
    economic_status : pd.DataFrame
        DataFrame of [zoning, 'econ', 'factor'] based on how students are split between
        students_unemployed, students_employed, students_inactive (all of these values are
        in the 'econ' column).

    Returns
    -------
    pd.DataFrame
        Dataframes with index of 'accom_h', 'ns_sec', 'soc', 'pop_emp', and 'pop_econ'
        and column headers of 'zoning' in the correct format to convert to DVector
    """

    # convert to required format for DVec
    df[zoning] = df[zoning].str.split(' ', expand=True)[0]

    # remap dwelling and ns-sec segmentation variables to be consistent with other mappings
    df['accom_h'] = df['level_1'].map({v: k for k, v in dwelling_segmentation.items()})
    df['ns_sec'] = df['variable_1'].map({v: k for k, v in ns_sec_segmentation.items()})

    # convert all segmentation to dataframe and merge with original data frame
    tmp = pd.DataFrame(all_segmentation).transpose()
    merged = pd.merge(df, tmp, left_on='variable_0', right_index=True, how='left')

    # set observation column to numeric
    merged['population'] = merged['value'].astype(int)

    # adjust population to exclude 75+ from the calculation of proportions for
    # economically inactive
    merged = pd.merge(merged, ages, on=zoning, how='left')
    merged['factor'] = 1 - merged['factor']
    merged.loc[
        (merged['pop_econ'] == 3) &
        (merged['pop_emp'] == 5) &
        (merged['soc'] == 4), 'population'
    ] = merged['population'] * merged['factor']
    merged = merged.drop(columns=['factor'])

    # map the aggregated pop_econ values to economic_status
    mapping = {
        1: 1,
        2: 2,
        3: 6,
        4: 3
    }
    merged['economic_status'] = merged['pop_econ'].map(mapping)

    # split students into economically active in emp, economically active
    # not in emp, and economically inactive
    students = merged.loc[merged['pop_econ'] == 4]
    non_students = merged.loc[~(merged['pop_econ'] == 4)]

    # TODO THIS SHOULD BE IMPROVED ITS VERY MANUAL ATM AND I DONT LIKE IT
    list_of_student_types = [non_students]
    for student, mat in economic_status.groupby('econ'):
        if student == 'students_employed':
            economic_status_val = 3
            pop_emp_val = 1
        elif student == 'students_unemployed':
            economic_status_val = 4
            pop_emp_val = 3
        elif student == 'students_inactive':
            economic_status_val = 5
            pop_emp_val = 4
        else:
            raise RuntimeError(f'Your student type is {student} which'
                               f'has no corresponding economic_status value.')
        foo = pd.merge(students, mat, on=zoning, how='left')
        foo['population'] = foo['population'] * foo['factor']
        foo['economic_status'] = economic_status_val
        foo['pop_emp'] = pop_emp_val
        foo = foo[list(merged.columns)]
        list_of_student_types.append(foo)

    output = pd.concat(list_of_student_types)
    grouped = output.groupby(
        [zoning, 'accom_h', 'ns_sec', 'economic_status', 'pop_emp', 'soc']
    ).agg({'population': 'sum'}).reset_index()

    # split economically active students in to full time and part time
    # based on the splits of full time and part time economically active people
    workers = grouped.loc[
        grouped['economic_status'] == 1
    ]
    workers['ft_pt_splits'] = workers['population'] / workers.groupby(
        [zoning, 'accom_h', 'ns_sec', 'soc']
    )['population'].transform('sum')
    workers['ft_pt_splits'] = workers['ft_pt_splits'].fillna(0)

    # set these to be employed students now
    workers['economic_status'] = 3

    # drop the population column from the employed data
    workers = workers.drop(columns=['population'])

    # get the employed students from the main dataset
    employed = grouped.loc[grouped['economic_status'] == 3]
    non_employed = grouped.loc[~(grouped['economic_status'] == 3)]

    # get rid of soc categorisation from the employed students
    employed = employed.groupby(
        [zoning, 'accom_h', 'ns_sec', 'economic_status']
    )['population'].sum().reset_index()

    # merge the employed totals on the workers which are
    # segmented by pop_emp and soc
    employed = pd.merge(
        workers,
        employed,
        on=[zoning, 'accom_h', 'ns_sec', 'economic_status'],
        how='left'
    )

    # calculate new population over the full time and part time splits
    employed['population'] = employed['population'] * employed['ft_pt_splits']
    employed = employed.drop(columns=['ft_pt_splits'])

    # combine the non-employed students dataset with the newly expanded employed students
    combined = pd.concat([non_employed, employed])

    # convert to required format for DVector
    dvec = pivot_to_dvector(
        data=combined,
        zoning_column=zoning,
        index_cols=['accom_h', 'ns_sec', 'economic_status', 'pop_emp', 'soc'],
        value_column='population'
    )

    # caravan data is missing from data source
    # assume caravan data is the same as flat data (used for proportions mainly so
    # so shouldn't be too much of a problem??
    # TODO this should be genericised, adding in a missing combination of indicies
    # TODO REVIEW THIS ASSUMPTION, IS THIS OKAY?
    missing = dvec[dvec.index.get_level_values('accom_h') == 4].reset_index()
    missing['accom_h'] = 5
    missing = missing.set_index(
        ['accom_h', 'ns_sec', 'economic_status', 'pop_emp', 'soc']
    )

    return pd.concat([dvec, missing])


def read_ons(
        file_path: Path,
        zoning: str,
        zoning_column: str,
        segment_mappings: dict,
        obs_column: str = 'Observation',
        segment_aggregations: dict = None
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
    segment_aggregations : dict, optional
        Dictionary of column aggregations to get the values provided in the input data aggregated to the same segmentation
        as required by the population model. For example, if the input file had age in 11 categories, whereas the population
        model requires them at 9 categories, then the below dictionary would be provided where, for the age column value, the
        keys of the dictionary are the values provided in the input data and the values of the dictionary are the values
        in the population segmentation that the keys should be aggregated to.
        dictionary of   {
                        column_name_1: {disaggregate_value_1: aggregate_value_1,
                                        disaggregate_value_2: aggregate_value_2, ...}
                        column_name_2: {...}
                        ...}

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

    # replace the values in the specific columns of data with the values required for the default segmentations
    # if they are provided (i.e. if aggregation of some variables are needed to get to the tfn population segments)
    if segment_aggregations is not None:
        for col, mappings in segment_aggregations.items():
            df[col] = df[col].map(mappings)
            # drop na based on mappings (all relevant values must be in the mappings)
            missing = df.loc[df[col].isnull()]
            if not missing.empty:
                logging.warning(f'{len(missing)} missing values mapped using the {col} mapping. \n'
                                f'Please check your mapping values and the input data to make sure '
                                f'you are not losing data unexpectedly.')
            df = df.dropna(subset=[col])

    # go through the dictionary and remap all the values based on the segmentation provided
    # columns are named based on the segment_ref provided
    refs = [zoning]
    for column, [ref, remapping] in segment_mappings.items():
        df[ref] = df[column].str.lower().map({v.lower(): k for k, v in remapping.items()})
        refs.append(ref)

    # if some remapping of aggregations has been done, then group the data to these new aggregations before
    # converting to DVector format
    if segment_aggregations is not None:
        df = df.groupby(refs).agg({obs_column: 'sum'}).reset_index()

    # convert to required format for DVector
    dvec = df.loc[:, refs + [obs_column]].dropna()
    dvec = dvec.set_index(refs).unstack(level=[zoning])
    dvec.columns = dvec.columns.get_level_values(zoning)

    return dvec


def convert_ces_by_type(
        df: pd.DataFrame,
        zoning: str,
        zoning_column: str,
        ce_type_map: dict
    ) -> pd.DataFrame:
    """Function to convert the census file of population by Communal Establishment
    type to dvector format with the index as the CE types.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe output of read_headered_csv()
        Expects a column of MSOA names (including the codes) and columns of each
        CE type. Data is population.
    zoning : str
        standard zoning name from constants.geographies.py of the zone system
        the data are in
    zoning_column : str
        str output of read_headered_csv()
        column to rename to the standard zoning definition
    ce_type_map : dict
        dictionary of the relevant columns of data and the mappings to the numeric
        values corresponding to the 'ce' segmentation defined in constants.segments.py

    Returns
    -------
    pd.DataFrame
        dataframe with index of 'ce' and columns of zoning, data is population.

    """
    # get MSOA code from the first column of the data
    df[zoning] = df[zoning_column].str.split(' ', expand=True)[0]

    # get relevant columns in the dataframe (there are lots of columns which sum
    # up to the total "medical" column, for example, so just restricting to the
    # columns we are actually interested in to avoid duplicates)
    reduced = df.loc[:, [zoning] + list(ce_type_map.keys())]

    # melt to get in long format
    melted = reduced.melt(
        id_vars=zoning,
        value_vars=list(ce_type_map.keys()),
        var_name='ce_type',
        value_name='population'
    )

    # map the values in the input data to the groupings defined in the
    # 'ce' mapping in segments.py
    melted['ce'] = melted['ce_type'].map(ce_type_map)

    # group by ce type and calcualte total population
    # (the mappings applied are not one to one)
    total = melted.groupby([zoning, 'ce']).agg({'population': 'sum'}).reset_index()

    # convert to dvector format
    return pivot_to_dvector(
        data=total,
        zoning_column=zoning,
        index_cols=['ce'],
        value_column='population'
    )


def convert_ces(
        df: pd.DataFrame,
        zoning: str,
        zoning_column: str
    ) -> pd.DataFrame:
    """Convert the census file of population in Communal Establishments
    to dvector format.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe output of read_headered_csv()
        Expects a column of LSOA names (including the codes) and columns of 'all
        residents' and 'all residents in communal establishments'. Data is population.
    zoning : str
        standard zoning name from constants.geographies.py of the zone system
        the data are in
    zoning_column : str
        str output of read_headered_csv()
        column to rename to the standard zoning definition
    Returns
    -------
    pd.DataFrame
        dataframe with index of 'total' and columns of zoning, data is uplift factor.

    """
    # get LSOA code from the first column of the data
    df[zoning] = df[zoning_column].str.split(' ', expand=True)[0]

    # calculate usual residents (total - CEs)
    # we've double check this data set against ONS table 1
    # ons_table_1['E01034628'].sum() = 1128
    # in this table, E01034628: total = 3565, lives in CE = 2437
    # so 3565 - 2437 = 1128, hence to calculate an uplift factor we need
    # total / usual residents
    df['usual_residents'] = df['Total: All usual residents'].astype(int) - df['Lives in a communal establishment'].astype(int)

    # check for negatives
    if (df['usual_residents'] < 0).sum() > 0:
        raise RuntimeError('There are negatives in the communal establishment data'
                           'when calculating usual residents. Please check your data.')

    # calculate uplift factors
    df['uplift'] = 1 + (df['Lives in a communal establishment'].astype(int) / df['usual_residents'].astype(int))

    # set an index column named 'total', this uplift is not linked to any existing segmentation
    # so here's we're using the 'total' cusotm segmentation defined in constants.segments.py
    df['total'] = 1

    # convert to dvector format
    return pivot_to_dvector(
        data=df,
        zoning_column=zoning,
        index_cols=['total'],
        value_column='uplift'
    )


def convert_scotland(
        df: pd.DataFrame,
        zoning: str,
        zoning_column: str,
        age_segmentation: dict,
        gender_segmentation: dict
    ) -> pd.DataFrame:
    """Convert the census file of population in Scotland.

    This dataset has sex, age, and residence type.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe output of read_headered_csv()
        Expects a column of LSOA names (including the codes) and columns of 'all
        residents' and 'all residents in communal establishments'. Data is population.
    zoning : str
        standard zoning name from constants.geographies.py of the zone system
        the data are in
    zoning_column : str
        column to rename to the standard zoning definition
    age_segmentation : dict
        dictionary to map the values of the age column to a segmentation definition
    gender_segmentation : dict
        dictionary to map the values of the sex column to a segmentation definition
    Returns
    -------
    pd.DataFrame
        dataframe with index of 'total' and columns of zoning, data is uplift factor.

    """
    # get LSOA code from the first column of the data
    df[zoning] = df[zoning_column]

    # convert the count column to numeric
    df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0)

    # map the correspondences
    df['g'] = df['Sex'].str.lower().map(gender_segmentation)
    df['scot_age'] = df['Age'].map(age_segmentation)

    # drop anything that hasn't been mapped by the segmentations
    # there are categories for "all people" in sex and "total" in age,
    # so we want to avoid double counting this
    df = df.dropna(subset=['g', 'scot_age'])
    df['g'] = df['g'].astype(int)
    df['scot_age'] = df['scot_age'].astype(int)

    # get total population (includes both usual residents and communal establishments)
    total_population = df.loc[df['Residence Type Indicator'] == 'All people']
    total_population = total_population.groupby([zoning, 'g', 'scot_age'])[['Count']].sum().reset_index()
    total_population['Count'] = total_population['Count'].astype(int)

    # convert to dvector format
    return pivot_to_dvector(
        data=total_population,
        zoning_column=zoning,
        index_cols=['g', 'scot_age'],
        value_column='Count'
    )


def read_mype_2022(
        file_path: Path,
        zoning: str,
        age_mapping: dict,
        gender_mapping: dict,
        sheet_name: str = 'Mid-2022 LSOA 2021',
        skip_rows: int = 3
    ) -> pd.DataFrame:
    """Read in the ONS MYPE data at LSOA

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
        with index of ['age_9', 'g'] and column headers of 'zoning' in the correct format
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
    age_groups = [0, 4, 9, 15, 19, 34, 49, 64, 74, 999]
    labels = ['0 to 4 years', '5 to 9 years', '10 to 15 years', '16 to 19 years',
              '20 to 34 years', '35 to 49 years', '50 to 64 years', '65 to 74 years', '75+ years']
    melted['age_band'] = pd.cut(
        melted['age'],
        age_groups,
        labels=labels,
        include_lowest=True
    ).astype(str)

    # remap male and female to definitions in the segments
    melted['gender_seg'] = melted['gender'].map({
        'F': 'female',
        'M': 'male'
    })

    # remap based on segmentations
    melted['age_9'] = melted['age_band'].map({v: k for k, v in age_mapping.items()})
    melted['g'] = melted['gender_seg'].map({v: k for k, v in gender_mapping.items()})

    # group by gender and age band and sum total population by LSOA
    totalled = melted.groupby([zoning, 'age_9', 'g'], as_index=False).agg({'population': 'sum'})

    # set index column to gender/age
    df = totalled.set_index([zoning, 'g', 'age_9']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df


def read_mype_control(
        file_path: Path,
        zoning: str,
        age_mapping: dict,
        gender_mapping: dict,
        sheet_name: str = 'MYEB1',
        skip_rows: int = 1
    ) -> pd.DataFrame:
    """Read in the ONS MYPE data at LAD

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
        with index of ['age_9', 'g'] and column headers of 'zoning' in the correct format
        to convert to DVector
    """
    # read in the excel sheet with the tab defined
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows).dropna()

    # also want to autodetect this column if possible
    zoning_col = 'ladcode23'
    df[zoning] = df[zoning_col]

    # define age groups to match with the "age" segments
    age_groups = [0, 4, 9, 15, 19, 34, 49, 64, 74, 999]
    labels = ['0 to 4 years', '5 to 9 years', '10 to 15 years', '16 to 19 years',
              '20 to 34 years', '35 to 49 years', '50 to 64 years', '65 to 74 years', '75+ years']
    df['age_band'] = pd.cut(
        df['age'],
        age_groups,
        labels=labels,
        include_lowest=True
    ).astype(str)

    # remap male and female to definitions in the segments
    df['gender_seg'] = df['sex'].map({
        'F': 'female',
        'M': 'male'
    })

    # remap based on segmentations
    df['age_9'] = df['age_band'].map({v: k for k, v in age_mapping.items()})
    df['g'] = df['gender_seg'].map({v: k for k, v in gender_mapping.items()})

    # group by gender and age band and sum total population by LSOA
    totalled = df.groupby(
        [zoning, 'age_9', 'g'], as_index=False
    ).agg({'population_2023': 'sum'})

    # set index column to gender/age
    df = totalled.set_index([zoning, 'g', 'age_9']).unstack(level=[zoning])
    df.columns = df.columns.get_level_values(zoning)

    return df
