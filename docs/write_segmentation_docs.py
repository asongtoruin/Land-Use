from argparse import ArgumentParser
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Tuple

# Janky, but this gives us the path to the project. 
# Adding it to sys.path lets us import direct from land_use
PROJECT_DIR = Path(Path(__file__).parent.parent)
sys.path.append(str(PROJECT_DIR))

from caf.core.segments import SegmentsSuper
import yaml

from land_use.constants.segments import split_input_segments, _CUSTOM_SEGMENT_CATEGORIES


LIST_TABLE_PREFIX = """
.. list-table::
   :header-rows: 1

"""


def lookup_segments(segments: Iterable[str]) -> Tuple[Dict[str, Dict[int, str]]]:
    """Get segment dictionaries from some iterable of IDs.

    Splits into "standard" and "custom", to allow for separate documentation

    Parameters
    ----------
    segments : Iterable[str]
        string names of segments to look up

    Returns
    -------
    Tuple[Dict[str, Dict[int, str]]]
        two-element tuple - first element is the standard segments, second is 
        the custom. Keys are the names of the segments, witht he values being a 
        dictionary of id: description.
    """
    split_segs = split_input_segments(segments)
    standard_segs = split_segs[True]
    custom_segs = split_segs[False]

    standard_segs = {
        seg: SegmentsSuper(seg).get_segment().values for seg in standard_segs
    }

    custom_segs = {seg: _CUSTOM_SEGMENT_CATEGORIES[seg] for seg in custom_segs}

    return standard_segs, custom_segs


def dict_to_table(title: str, contents: Dict[Any, Any]) -> str:
    """Converts a dictionary (probably of segment lookups) into a table
    
    Specifically this generates a list-table for Sphinx

    Parameters
    ----------
    title : str
        title to assign to the section of the table. This will be given 
        "subsection" level within Sphinx
    contents : Dict[Any, Any]
        Contents of the lookup. Keys will be labelled "Value", and values as 
        "Description"

    Returns
    -------
    str
        A list-table ready to be used in Sphinx docs
    """
    table_records = '   * - Value\n     - Description\n'
    for key, value in contents.items():
        table_records += f'   * - {key}\n     - {value}\n'

    title = f'``{title}``'

    return (
        f'{title}\n' + '-' * len(title) + '\n'
        + LIST_TABLE_PREFIX
        + table_records
    )

def segments_from_yaml(yaml_file: Path) -> set:
    """Read in segments applied from a YAML config file for Land-Use

    Parameters
    ----------
    yaml_file : Path
        Path to the YAML file to be processed

    Returns
    -------
    set
        a set of all the segments referenced in the file
    """
    with yaml_file.open('r') as file_obj:
        contents = yaml.load(file_obj, Loader=yaml.SafeLoader)

    all_segments = set()

    for value in contents.values():
        if isinstance(value, dict):
            for sub_value in value.values():
                if isinstance(sub_value, dict):
                    if segments := sub_value.get('input_segments'):
                        all_segments.update(segments)
                if isinstance(sub_value, list):
                    for item in sub_value:
                        if isinstance(item, dict):
                            if segments := item.get('input_segments'):
                                all_segments.update(segments)
    
    return all_segments


def segmentation_page_from_yaml(yaml_file: Path, title: str = 'Segmentation') -> str:
    """Generate a "Segmentation" page in Sphinx from a given YAML file.

    Parameters
    ----------
    yaml_file : Path
        path to the YAML file to be processed
    title : str, optional
        title for the page itself, by default 'Segmentation'

    Returns
    -------
    str
        string representation of the generated page
    """
    contents = (
        f'{title}\n'
        f'{"#"  * len(title)}\n\n'
        'Source\n======\n'
        f'Generated from ``{yaml_file.relative_to(PROJECT_DIR)}``\n\n'
    )

    segments = segments_from_yaml(yaml_file)

    standard_segs, custom_segs = lookup_segments(sorted(segments))

    if standard_segs:
        contents += 'Standard Segments\n=================\n'
        for seg_name, seg_lookup in standard_segs.items():
            contents += dict_to_table(seg_name, seg_lookup) + '\n\n'
    
    if custom_segs:
        contents += 'Custom Segments\n===============\n'
        for seg_name, seg_lookup in custom_segs.items():
            contents += dict_to_table(seg_name, seg_lookup) + '\n\n'
    
    return contents


if __name__ == '__main__':
    # Set up the argument parser
    parser = ArgumentParser(
        'Process to auto-list segments used in configuration files for '
        'documentation purposes'
    )

    parser.add_argument('-pc', '--pop_config', type=Path)
    parser.add_argument('-ec', '--emp_config', type=Path)

    args = parser.parse_args()

    this_folder = Path(__file__).parent

    with open(this_folder / 'population' / 'segmentation.rst', 'w') as pop_seg_file:
        pop_seg_file.write(segmentation_page_from_yaml(args.pop_config.absolute()))
    
    with open(this_folder / 'employment' / 'segmentation.rst', 'w') as pop_seg_file:
        pop_seg_file.write(segmentation_page_from_yaml(args.emp_config.absolute()))

