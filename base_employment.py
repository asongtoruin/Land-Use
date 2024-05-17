from pathlib import Path

import yaml
from caf.core.zoning import TranslationWeighting

from land_use import constants
from land_use import data_processing

# load configuration file
with open(r'scenario_configurations\iteration_5\base_employment_config.yml', 'r') as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config['output_directory'])
OUTPUT_DIR.mkdir(exist_ok=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config['output_intermediate_outputs'])

# # Currently not implemented as LSOA2011 has not been read into geographies which is what BRES 2022 uses
# # read in the data from the config file
# bres_2022_employees = data_processing.read_dvector_data(input_root_directory=config['input_root_directory'], **config['bres_2022_employees'])
