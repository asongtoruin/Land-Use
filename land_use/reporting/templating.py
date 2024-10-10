from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


_template_dir = Path(__file__).parent / 'templates'
_environment = Environment(loader=FileSystemLoader(_template_dir))

SCENARIO_TEMPLATE = _environment.get_template('scenario_page.jinja')
DATA_TYPE_TEMPLATE = _environment.get_template('data_type_page.jinja')
SEGMENT_TEMPLATE = _environment.get_template('segment_page.jinja')

# Register "now" in the templates so we can timestamp on creation.
for template in SCENARIO_TEMPLATE, DATA_TYPE_TEMPLATE, SEGMENT_TEMPLATE:
    template.globals['now'] = datetime.now


def render_scenario_page(scenario_name: str) -> str:
    return SCENARIO_TEMPLATE.render(scenario_name=scenario_name)

def render_data_type_page(data_type: str) -> str:
    return DATA_TYPE_TEMPLATE.render(data_type=data_type)

def render_segment_page(segment_name, graph_paths, table_paths) -> str:
    return SEGMENT_TEMPLATE.render(segment_name=segment_name, graph_paths=graph_paths, table_paths=table_paths)
