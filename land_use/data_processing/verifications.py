from dataclasses import dataclass
import logging
from itertools import combinations
from typing import Generator, Iterable, Optional, Union
from textwrap import fill

import pandas as pd
from caf.core.data_structures import DVector
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.axes import Axes
from matplotlib.figure import Figure
import seaborn as sns

LOGGER = logging.getLogger(__name__)

plt.style.use(r'https://raw.githubusercontent.com/Transport-for-the-North/caf.viz/main/src/caf/viz/tfn.mplstyle')

@dataclass
class StaticSegmentPlot:
    figure: Figure
    axes: Iterable[Axes]
    segment_identifiers: Iterable[str]
    source_data: Optional[pd.DataFrame] = None

    @property
    def segments(self):
        return '_by_'.join(self.segment_identifiers)
    
    @property
    def summary_data(self):
        # Add in a total row
        working = self.source_data.copy()
        working.loc['TOTAL'] = working.sum(axis=0)

        # Round to whole numbers
        return working.round(0)


def compare_dvectors(
        dvec1: DVector,
        dvec2: DVector
) -> Union[None, pd.DataFrame]:
    """Compare the total values of two DVectors based on the overlapping
    segmentations. If there are no overlapping segmentations then only totals
    are reported, otherwise the differences are reported at a level of the
    overlapping segmentations.

    Parameters
    ----------
    dvec1: DVector
        DVector from the previous stage of processing (i.e. a change has been
        made to dvec1 to reach dvec2 and therefore dvec1 is the baseline for
        comparison)
    dvec2: DVector
        DVector from a subsequent processing step from dvec1 with which to
        compare against dvec1. Should have some overlapping segmentations with
        dvec1, otherwise only totals will be reported.

    """
    # check if DVectors are in the same zone system
    if dvec1.zoning_system != dvec2.zoning_system:
        LOGGER.warning(
            f'DVector 1 has zoning {dvec1.zoning_system} whereas '
            f'DVector2 has zoning {dvec2.zoning_system}. This is not '
            f'a problem but please double check you are comparing the '
            f'right two DVectors.'
        )

    # check if the DVectors have overlapping segmentation, if they don't then
    # raise a warning so the user knows only absolute totals will be reported,
    # nothing more detailed
    common_segs = set(dvec1.segmentation.names).intersection(
        set(dvec2.segmentation.names)
    )

    if len(common_segs) == 0:
        LOGGER.warning(
            f'There are no common segmentations between the two DVectors. DVector '
            f'1 has segmentation {dvec1.segmentation.names}, and DVector 2 has '
            f'segmentation {dvec1.segmentation.names}. Only totals will be '
            f'reported from now on.'
        )

        # report totals
        # dvector.total
        total1 = dvec1.total
        total2 = dvec2.total
        LOGGER.info(f'DVector 1 total: {total1:,.0f}')
        LOGGER.info(f'DVector 2 total: {total2:,.0f}')

    else:
        # calculate totals across all zones
        df1 = dvec1.data.sum(axis=1).rename('total1').reset_index()
        df2 = dvec2.data.sum(axis=1).rename('total2').reset_index()

        # groupby the common segments
        df1 = df1.groupby(list(common_segs)).agg({'total1': 'sum'})
        df2 = df2.groupby(list(common_segs)).agg({'total2': 'sum'})

        # combine to single dataframe
        output = df1.join(df2)
        output['change'] = output['total2'] - output['total1']

        # report changes
        # todo where are these min maxs
        max_change = output['change'].max()
        min_change = output['change'].min()
        average_change = output['change'].mean()
        LOGGER.info(f'Maximum change: {max_change:,.0f}')
        LOGGER.info(f'Minimum change: {min_change:,.0f}')
        LOGGER.info(f'Average change: {average_change:,.0f}')

        return output


def generate_segment_heatmaps(
        dvec: DVector
) -> Generator[StaticSegmentPlot, None, None]:
    """Produce plots of heatmaps between different combinations of segmentations
    in a given DVector.

    Parameters
    ----------
    dvec: DVector
        DVector with at least two levels of segmentation. If the DVector has 1
        segmentation level (e.g. only dwelling type) then no plots will be
        produced (as there are no combinations).

    Returns
    -------
    Generator
        Generator of figures based on all combinations of segmentations in
        dvector.

    """
    data = dvec.data

    segment_names = sorted(data.index.names)

    if len(segment_names) == 1:
        LOGGER.warning(f'DVector only has one level of segmentation so no '
                       f'heatmaps will be produced.')
        return

    total_values = data.sum(axis=1).to_frame(name='Total').reset_index()

    for row_seg, col_seg in combinations(segment_names, 2):
        # Get the "matrix" of values
        grouped_totals = total_values.pivot_table(
            index=row_seg, columns=col_seg, values='Total', aggfunc='sum'
        )

        fig, ax = plt.subplots()
        ax.grid(False)

        # plot heatmap
        sns.heatmap(grouped_totals, ax=ax, square=True, annot=True, fmt=',.0f')

        # set axis labels to the wordy values to help readability
        ax.set_yticks(
            list(grouped_totals.reset_index()[row_seg] - 0.5),
            labels=[
                f'{x}: {y}' for (x, y) in
                dvec.segmentation.seg_dict.get(row_seg).values.items()
            ]
        )
        ax.set_xticks(
            [(col - 0.5) for col in grouped_totals.columns],
            labels=[
                f'{x}: {y}' for (x, y) in
                dvec.segmentation.seg_dict.get(col_seg).values.items()
            ]
        )
        ax.tick_params(axis='x', labelrotation=45)
        ax.tick_params(axis='y', labelrotation=0)

        fig.set_size_inches(15, 15)

        yield StaticSegmentPlot(
            figure=fig, axes=[ax], segment_identifiers=[row_seg, col_seg],
            source_data=grouped_totals
        )

        plt.close(fig)


def generate_segment_bar_plots(
        dvec: DVector,
        unit: str,
        max_segments: int = 8
) -> Generator[StaticSegmentPlot, None, None]:

    mapping = dvec.zoning_system.zone_descriptions().to_dict()

    for seg in dvec.segmentation.names:

        # get category names within the segmentation to help readability of charts
        segment_mapping = dvec.segmentation.seg_dict[seg].values

        # find out how many categories and dont plot if there's too many
        if len(segment_mapping.values()) > max_segments:
            continue

        # group data by the specific segment (plotting only one at a time)
        data = dvec.aggregate([seg]).data.T

        # change the index and column values to zone names and category names, respectively
        data.index = data.index.map(mapping)
        data.columns = data.columns.map(segment_mapping)

        # output summary csv
        output_data = data.copy()

        # And now *after* we've written, shorten labels for graph
        data.index = data.index.map(lambda f: fill(f, 15))
        data.columns = data.columns.map(lambda f: fill(f, 20))

        percentages = data.copy()
        percentages = percentages.div(percentages.sum(axis=1), axis=0)

        fig, axes = plt.subplots(
            figsize=(10, 8), nrows=2, sharex=True, sharey=False,
            gridspec_kw=dict(height_ratios=[2, 1])
        )

        # plot grouped bar chart
        data.plot.bar(zorder=2, ax=axes[0], edgecolor='black')
        # labels = [fill(l, 20) for l in data.columns]
        axes[0].set_ylabel(unit)
        axes[0].yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ','))
        )
        axes[0].legend(title=seg, loc='center left', bbox_to_anchor=(1, 0.5))

        percentages.plot.bar(zorder=2, ax=axes[1], legend=False, edgecolor='black')
        axes[1].yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(xmax=1))
        axes[1].set_xlabel(None)

        yield StaticSegmentPlot(
            figure=fig, axes=axes, segment_identifiers=[seg], source_data=output_data
        )

        plt.close(fig)
