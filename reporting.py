from argparse import ArgumentParser
from pathlib import Path

import yaml

from land_use import data_processing
from land_use.reporting import templating

# TODO: expand on the documentation here
parser = ArgumentParser('Land-Use base population command line runner')
parser.add_argument('config_file', type=Path)
args = parser.parse_args()

# load configuration file
with open(args.config_file, 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory of main model outputs from config file
OUTPUT_DIR = Path(config['output_directory'])

# TODO: move this into the YAML
scenario_name = '2024-10-10 Example Reporting'

# Set up the root results page
docs_dir = Path(__file__).parent / 'docs' / 'Scenario Results' / scenario_name
if not docs_dir.is_dir():
    docs_dir.mkdir(exist_ok=True, parents=True)
    with open(docs_dir / 'index.rst', 'w') as docs_index:
        docs_index.write(templating.render_scenario_page(scenario_name))

# get files from existing output
data_dict = {
    'Households': list(OUTPUT_DIR.glob('Output P4.3_*.hdf')),
    'Population': list(OUTPUT_DIR.glob('Output P13_*.hdf')),
    'Employment': list(OUTPUT_DIR.glob('Output E4.hdf')),
}

# define zone system to translate to
REPORTING_ZONE_SYSTEM = 'RGN2021+SCOTLANDRGN'

for unit, input_files in data_dict.items():
    if not input_files:
        continue
    # Set up the output directory for that unit category
    unit_docs_dir = docs_dir / unit
    unit_docs_dir.mkdir(exist_ok=True)
    with open(unit_docs_dir / 'index.rst', 'w') as unit_index:
        unit_index.write(templating.render_data_type_page(data_type=unit))

    # And set up the folder for all the results to go into
    results_dir = unit_docs_dir / 'Segment Results'
    results_dir.mkdir(exist_ok=True)

    # generate combined dvector
    total_dvector = data_processing.translate_and_combine_dvectors(
        input_files=input_files,
        aggregate_zone_system=REPORTING_ZONE_SYSTEM
    )

    for segment_plot in data_processing.generate_segment_bar_plots(total_dvector, unit=unit):
        # First - save the figure
        segment_plot.figure.savefig(results_dir / f'{segment_plot.segments}.png')

        # And save the data
        segment_plot.summary_data.to_csv(
            results_dir / f'{segment_plot.segments}.csv', 
            float_format=lambda x: '{:,.0f}'.format(x)
        )

        # Then fill out the template
        with open(results_dir / f'{segment_plot.segments}.rst', 'w') as segment_page:
            segment_page.write(
                templating.render_segment_page(
                    segment_name=segment_plot.segments,
                    graph_paths=[f'{segment_plot.segments}.png'],
                    table_paths=[f'{segment_plot.segments}.csv']
                )
            )
