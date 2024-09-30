from argparse import ArgumentParser
from pathlib import Path

import yaml

from land_use import data_processing

# TODO: expand on the documentation here
parser = ArgumentParser('Land-Use base population command line runner')
parser.add_argument('config_file', type=Path)
args = parser.parse_args()

# load configuration file
with open(args.config_file, 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory of main model outputs from config file
OUTPUT_DIR = Path(config['output_directory'])

# get files from existing output
population_files = list(OUTPUT_DIR.glob(r'Output P13_*.hdf'))
household_files = list(OUTPUT_DIR.glob(r'Output P4.3_*.hdf'))
data_dict = {
    'households': household_files,
    'population': population_files
}

# define zone system to translate to
REPORTING_ZONE_SYSTEM = 'RGN2021+SCOTLANDRGN'
reference = 'regions'

for unit, input_files in data_dict.items():
    # generate combined dvector
    total_dvector = data_processing.translate_and_combine_dvectors(
        input_files=input_files,
        aggregate_zone_system=REPORTING_ZONE_SYSTEM
    )

    # generate bar plots
    data_processing.generate_reporting_plots(
        output_folder=OUTPUT_DIR,
        dvector=total_dvector,
        unit=unit,
        geography=reference
    )
