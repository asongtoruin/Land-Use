from pathlib import Path
import logging
from typing import Optional

import pandas as pd
from caf.core.data_structures import DVector

from land_use.constants import geographies
from .verifications import generate_segment_heatmaps

LOGGER = logging.getLogger(__name__)


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

    return data.reset_index(names=[dvec.zoning_system.name])


def summarise_dvector(
        dvector: DVector,
        output_directory: Path,
        output_reference: str,
        value_name: str = 'value'
    ):
    """Function to summarise any DVector based on the segmentation provided in the DVector object.

    The outputs will be:
    - a detailed summary of each segmentation total by the zone level in the input DVector (e.g. LSOA)
    - a detailed summary of each segmentation total by LAD
    - totals across all zones, and summary statistics, by the zone level of the input DVector (e.g. LSOA),
    MSOA (if the input DVector is not already MSOA level), and by LAD

    Parameters
    ----------
    dvector : DVector
        DVector to produce output summaries of
    output_directory : Path
        Path to the main output directory. This will create a 'verifications'
        folder if it does not already exist and write these outputs there.
    output_reference : str
        reference name for the outputs so outputs dont get overwritten. Might be
        something like "output1" or something equally useful.
    value_name : str
        reference for the dimension of the output (e.g. population or households
        or something similar)

    Returns
    -------

    """
    LOGGER.info(fr'Generating output summaries of {output_reference}')
    # create directory to write outputs to if it doesn't already exist
    write_folder = output_directory / 'verifications'
    write_folder.mkdir(exist_ok=True)

    # get summary distribution of segmentations across all zones
    group_cols = [f'{col}_description' for col in dvector.segmentation.naming_order]

    # get dataframe
    df = dvector.data.copy().reset_index()

    # get zone cols
    zone_cols = list(dvector.data.columns)

    # calculate value column
    df['total'] = df[zone_cols].sum(axis=1)
    df['average'] = df[zone_cols].mean(axis=1)
    df['max'] = df[zone_cols].max(axis=1)

    # map the segmentation values to the actual descriptions for ease
    for segment in dvector.segmentation.naming_order:
        df[f'{segment}_description'] = df[segment].map(
            dvector.segmentation.seg_dict.get(segment).values
        )

    # write cross-tab distribution summary to verifications folder
    df.loc[:, group_cols + ['total', 'average', 'max']].to_csv(
        write_folder / f'{output_reference}_{dvector.zoning_system.name}_all.csv',
        float_format='%.5f',
        index=False
    )

    # loop through all segmentations in the DVector
    for segment in dvector.segmentation.seg_dict.keys():
        disaggregate_total = dvector.aggregate([segment])
        lad_total = disaggregate_total.translate_zoning(
            geographies.ENG_LAD_PLUS_WALES_ZONING_SYSTEM,
            cache_path=geographies.CACHE_FOLDER
        )

        # Convert to long format
        disaggregate_seg_long = dvector_to_long(
            dvec=disaggregate_total,
            value_name=value_name
        )
        lad_seg_long = dvector_to_long(
            dvec=lad_total,
            value_name=value_name
        )

        # Write both out to a check folder
        disaggregate_seg_long.to_csv(
            write_folder / f'{output_reference}_{dvector.zoning_system.name}_{segment}.csv',
            index=False,
            float_format='%.5f'
        )
        lad_seg_long.to_csv(
            write_folder / f'{output_reference}_{lad_total.zoning_system.name}_{segment}.csv',
            index=False,
            float_format='%.5f'
        )

    # store the disaggregate and LAD based dataframes to total across all zones
    # long_dfs = [disaggregate_seg_long, lad_seg_long]
    #
    # # if the zone system of the DVector is not already MSOA, then generate the MSOA dataframe in the same format
    # # to export total summaries of
    # if not dvector.zoning_system.name == geographies.MSOA_NAME:
    #     # Do the last one only for MSOA, so we have something to work with.
    #     msoa_seg_long = dvector_to_long(
    #         dvector.translate_zoning(
    #             geographies.MSOA_ZONING_SYSTEM,
    #             cache_path=geographies.CACHE_FOLDER
    #         ), value_name=value_name
    #     )
    #     long_dfs.append(msoa_seg_long)
    #
    # # create total summary outputs by zone
    # for long_frame in long_dfs:
    #     # get the zone system name from the dataframe itself
    #     zone_system = long_frame.columns[0]
    #
    #     # calculate and output total summary by zone
    #     total = long_frame.groupby(zone_system).agg({value_name: 'sum'})
    #     LOGGER.info(fr'Writing to {write_folder}\{output_reference}_{zone_system}.csv')
    #     total.to_csv(
    #         write_folder / f'{output_reference}_{zone_system}.csv',
    #         float_format='%.5f'
    #     )


def summary_reporting(
        dvector: DVector,
        dimension: str,
        detailed_logs: bool = False
    ):
    """Function to log matrix totals to the log file (and prompt)

    Parameters
    ----------
    dvector : DVector
        DVector to summarise
    dimension : str
        Dimension of the DVector.data (e.g. maybe 'households' or 'population'),
        something to help the user know what totals are being logged.
    detailed_logs : bool, default False
        If this is set to True then detailed logs (still LOGGER.info) are also
        output, detailing totals by segment type and the proportional split
        between the different segmentation categories.

    """
    # log information
    LOGGER.info(f'Data has segmentation of {dvector.segmentation.naming_order}')
    LOGGER.info(f'Total {dimension} in data: {dvector.total:,.0f}')

    if detailed_logs:
        log_level = LOGGER.info
    else:
        log_level = LOGGER.debug
    # Remove the zoning
    non_spatial = dvector.remove_zoning()
    total = non_spatial.total
    for segmentation in dvector.segmentation.naming_order:
        summary = non_spatial.aggregate([segmentation])

        # Get lookup from numeric labels to descriptions
        mapping = dvector.segmentation.seg_dict[segmentation].values
        values = {desc: f"{summary.data.get(key, 0):,.0f}" for key, desc in mapping.items()}
        proportions = {desc: f"{summary.data.get(key, 0)/total:.0%}" for key, desc in mapping.items()}

        log_level(f'{segmentation} values: {values}')
        log_level(f'{segmentation} proportions: {proportions}')


def save_output(
        output_folder: Path,
        output_reference: str,
        dvector: DVector,
        dvector_dimension: str,
        generate_summary_outputs: bool = False,
        detailed_logs: bool = False

):
    """Output data and report logs of high level totals.
    Also contains the option to provide detailed logging and detailed summary
    outputs, which are not output by default.

    Parameters
    ----------
    output_folder : Path
        Output location to store outputs passed from the config.
        Directory should already exist before use in this function (the set up
        of the logging guarantees this).
    output_reference : str
        Helpful string reference for the user to report which outputs are being
        produced.
    dvector : DVector
        DVector of data that should be summarised / saved.
    dvector_dimension : str
        Helpful string reference for the user to report the units of the data
        stored in DVector. May be 'population', or 'households', or something
        similar.
    generate_summary_outputs : bool, default False
        Produces csv summary outputs grouped by geography and segment by calling
        the summarise_dvector() function.
    detailed_logs : bool, default False
        Provides detailed logging for all segments in dvector by setting
        detailed_logs=True in the summary_reporting() function.

    """

    # save to HDF
    LOGGER.info(fr'Writing to {output_folder}\{output_reference}.hdf')
    dvector.save(output_folder / f'{output_reference}.hdf')

    LOGGER.info('Output summary:')
    # logging information
    summary_reporting(
        dvector=dvector,
        dimension=dvector_dimension,
        detailed_logs=detailed_logs
    )

    LOGGER.info(fr'Generating heatmaps to {output_folder}\plots')
    # generate heat map plots
    plot_folder = output_folder / 'plots'
    plot_folder.mkdir(exist_ok=True, parents=True)
    segmentation_combination = 1
    for fig, ax, row_seg, col_seg in generate_segment_heatmaps(dvec=dvector):
        fig.savefig(plot_folder / f'{output_reference}-{row_seg}_by_{col_seg}.png')
        segmentation_combination += 1

    # produce output summaries in csv format if required
    if generate_summary_outputs:
        summarise_dvector(
            dvector=dvector,
            output_directory=output_folder,
            output_reference=output_reference,
            value_name=dvector_dimension
        )


def write_to_excel(
        output_folder: Path,
        file: str,
        dfs: list,
        sheet_names: Optional[list] = None
):
    """Output dataframes to an excel file. Different data can be put on
    different tabs with different names.

    Parameters
    ----------
    output_folder: Path
        Directory to save the output file.
    file: str
        Filename to be written to.
    dfs: list[pd.DataFrame]
        List of dataframes you wish to output
    sheet_names: Optional[list[str]]
        List of tab names that each element of dfs will be written to.
        If not provided, sheet names will be named ['Sheet1', 'Sheet2', ...,
        'Sheetx']
        LENGTH OF SHEET_NAMES MUST MATCH LENGTH OF DFS.
    """
    output_file = output_folder / file
    LOGGER.info(f'Writing to {output_file}')

    # delete file if it exists
    if output_file.is_file():
        output_file.unlink()

    # check dfs and sheet names are the same length
    if sheet_names is not None:
        if not len(dfs) == len(sheet_names):
            raise RuntimeError('Number of dataframes to output and number of excel '
                               'sheet names must be the same.')

    # write all dfs to each sheet name in a single excel file
    with pd.ExcelWriter(str(output_folder / file)) as writer:
        for count, _ in enumerate(dfs):
            if sheet_names is None:
                sheet = f'Sheet{count + 1}'
            else:
                sheet = sheet_names[count]
            df = dfs[count]

            df.to_excel(
                writer,
                sheet_name=sheet,
                index=False,
                float_format='%.5f'
            )
