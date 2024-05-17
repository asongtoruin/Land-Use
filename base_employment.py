from pathlib import Path

import yaml
from caf.core.zoning import TranslationWeighting

from land_use import constants
from land_use import data_processing

# load configuration file
with open(
    r"scenario_configurations\iteration_5\base_employment_config.yml", "r"
) as text_file:
    config = yaml.load(text_file, yaml.SafeLoader)

# Get output directory for intermediate outputs from config file
OUTPUT_DIR = Path(config["output_directory"])
OUTPUT_DIR.mkdir(exist_ok=True)

# Define whether to output intermediate outputs, recommended to not output loads if debugging
generate_summary_outputs = bool(config["output_intermediate_outputs"])

# read in the data from the config file
# note this data is only for England and Wales
bres_2022_employees = data_processing.read_dvector_data(
    input_root_directory=config["input_root_directory"], **config["bres_2022_employees"]
)
bres_2022_employment = data_processing.read_dvector_data(
    input_root_directory=config["input_root_directory"],
    **config["bres_2022_employment"],
)
bres_2022_full_time_employees = data_processing.read_dvector_data(
    input_root_directory=config["input_root_directory"],
    **config["bres_2022_full_time_employees"],
)
bres_2022_part_time_employees = data_processing.read_dvector_data(
    input_root_directory=config["input_root_directory"],
    **config["bres_2022_part_time_employees"],
)

assert bres_2022_employees._data.shape == (18, 34753)
assert bres_2022_employment._data.shape == (18, 34753)
assert bres_2022_full_time_employees._data.shape == (18, 34753)
assert bres_2022_part_time_employees._data.shape == (18, 34753)
