from pathlib import Path

import yaml
from caf.core.zoning import TranslationWeighting

from land_use import constants
from land_use import data_processing


def process_bres_2022(config: dict):
    # Define whether to output intermediate outputs, recommended to not output loads if debugging
    generate_summary_outputs = bool(config["output_intermediate_outputs"])

    # read in the data from the config file
    # note this data is only for England and Wales
    bres_2022_employees_2011_lsoa = read_dvector(
        config=config, key="bres_2022_employees"
    )
    bres_2022_employment_2011_lsoa = read_dvector(
        config=config, key="bres_2022_employment"
    )
    bres_2022_full_time_employees_2011_lsoa = read_dvector(
        config=config, key="bres_2022_full_time_employees"
    )
    bres_2022_part_time_employees_2011_lsoa = read_dvector(
        config=config, key="bres_2022_part_time_employees"
    )

    bres_2022_employees_2021_lsao = translate_to_lsoa_2021(
        bres_2022_employees_2011_lsoa
    )
    bres_2022_employment_2021_lsoa = translate_to_lsoa_2021(
        bres_2022_employment_2011_lsoa
    )
    bres_2022_full_time_employees_2021_lsoa = translate_to_lsoa_2021(
        bres_2022_full_time_employees_2011_lsoa
    )
    bres_2022_part_time_employees_2021_lsoa = translate_to_lsoa_2021(
        bres_2022_part_time_employees_2011_lsoa
    )

    print(bres_2022_employees_2021_lsao.data.head())
    print(bres_2022_employment_2021_lsoa.data.head())
    print(bres_2022_full_time_employees_2021_lsoa.data.head())
    print(bres_2022_part_time_employees_2021_lsoa.data.head())


def process_bres_2021(config: dict):
    bres_2021_employees_2011_msoa_sic_2 = read_dvector(
        config=config, key="bres_2021_employees_sic_2"
    )

    bres_2021_employment_2011_lsoa_sic_2 = read_dvector(
        config=config, key="bres_2021_employment_sic_2"
    )

    bres_2021_employees_2021_lsoa_sic_2 = translate_to_lsoa_2021(
        bres_2021_employees_2011_msoa_sic_2
    )

    bres_2021_employement_2021_lsoa_sic_2 = translate_to_lsoa_2021(
        bres_2021_employment_2011_lsoa_sic_2
    )


def read_dvector(config: dict, key: str) -> data_processing.DVector:
    return data_processing.read_dvector_data(
        input_root_directory=config["input_root_directory"],
        **config[key],
    )


def translate_to_lsoa_2021(dvec_in: data_processing.DVector) -> data_processing.DVector:
    return dvec_in.translate_zoning(
        new_zoning=constants.LSOA_ZONING_SYSTEM,
        cache_path=constants.CACHE_FOLDER,
        weighting=TranslationWeighting.SPATIAL,
        check_totals=True,
    )


if __name__ == "__main__":
    # load configuration file
    with open(
        r"scenario_configurations\iteration_5\base_employment_config.yml", "r"
    ) as text_file:
        config = yaml.load(text_file, yaml.SafeLoader)

    # Get output directory for intermediate outputs from config file
    OUTPUT_DIR = Path(config["output_directory"])
    OUTPUT_DIR.mkdir(exist_ok=True)

    process_bres_2021(config=config)
    process_bres_2022(config=config)
