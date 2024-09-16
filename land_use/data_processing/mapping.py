from collections import OrderedDict
import logging
from pathlib import Path
from typing import List, Optional

from branca.colormap import ColorMap
from caf.core import DVector, ZoningSystem
import folium
import geopandas as gpd

from ..constants import CACHE_FOLDER

LOGGER = logging.getLogger(__name__)

def load_spatial_zoning(dvec: DVector, simplification: Optional[int] = 30) -> gpd.GeoDataFrame:
    """Loads the spatial representation of a DVector's zoning, optionally simplifying

    Parameters
    ----------
    dvec : DVector
        DVector object to load the zoning from
    simplification : Optional[int], optional
        Tolerance, in metres, to apply when simplifying zones. By default 30

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the (maybe simplified) polygonal representation
        of the DVector's ZoningSystem
    """
    # Load the zone system, filter to only relevant columns
    spatial_zones = gpd.read_file(dvec.zoning_system.metadata.shapefile_path)
    id_col = dvec.zoning_system.metadata.shapefile_id_col
    spatial_zones = spatial_zones[[id_col, 'geometry']]

    # Simplify if required
    if simplification:
        spatial_zones["geometry"] = spatial_zones.geometry.simplify(
            tolerance=simplification, preserve_topology=True
        )

    return spatial_zones


def drop_duplicate_cbars(map_obj: folium.Map, unit_label: str) -> None:
    new_children = OrderedDict()
    has_cbar = False

    for name, obj in map_obj._children.items():
        # Keep anything that's not a ColorMap
        if not isinstance(obj, ColorMap):
            new_children[name] = obj
        
        # And the first ColorMap we encounter
        elif not has_cbar:
            new_children[name] = obj
            obj.caption = unit_label
            has_cbar = True
    
    # Update the source map
    map_obj._children = new_children


def render_map(
    spatial_data: gpd.GeoDataFrame,
    cols_to_map: List[int], 
    cmap: str = 'plasma',
    cbar_label: str = 'Person count',
    vmax: Optional[float] = None,
    vmin: Optional[float] = None
) -> folium.Map:
    print(spatial_data)
    output_map = folium.Map(tiles=None)

    # Set up background options
    folium.TileLayer('CartoDB positron', name='Light map', control=False).add_to(output_map)
    # TODO: it would be nice to have multiple mapping options, but this seems fiddly to do with .explore. Try and make this work?
    # folium.TileLayer('OpenStreetMap', name='Detailed map', show=False).add_to(output_map)

    if vmax is None:
        vmax = spatial_data.max(numeric_only=True).max(numeric_only=True)
    if vmin is None:
        vmin = spatial_data.min(numeric_only=True).min(numeric_only=True)

    # Plot the different columns
    for i, col in enumerate(cols_to_map):
        _ = spatial_data.explore(
            column=col,
            vmin=vmin,
            vmax=vmax,
            cmap=cmap,
            m=output_map,
            name=col,
            overlay=False,
            tooltip_kwds={'localize': True},
            style_kwds={'color': 'black', 'weight': 1, 'fillOpacity': 0.3},
            show=not i
        )
    
    folium.LayerControl(collapsed=False).add_to(output_map)
    output_map.fit_bounds(output_map.get_bounds(), padding=(30, 30))

    # We'll have multiple colour bars - drop them
    drop_duplicate_cbars(output_map, cbar_label)

    return output_map


def create_interactive_maps(
    dvec: DVector,
    output_folder: Path,
    desired_zoning_system: Optional[str] = None,
    zone_cache_path: Path = CACHE_FOLDER,
    max_unique_categories: int = 15
) -> List[Path]:
    
    if desired_zoning_system:
        zs = ZoningSystem.get_zoning(
            desired_zoning_system, search_dir=zone_cache_path
        )

        dvec = dvec.translate_zoning(
            zs, cache_path=zone_cache_path
        )

    segmentation = dvec.segmentation
    id_col = dvec.zoning_system.metadata.shapefile_id_col

    spatial_zoning = load_spatial_zoning(dvec=dvec)

    for seg in segmentation.segments:
        if len(seg) > max_unique_categories:
            LOGGER.info(
                f'Skipping {seg.name}, too many categories '
                f'({len(seg)} > {max_unique_categories})'
            )
            continue
        
        # Get values just by that segment, flip from "wide" to "long"
        segment_absolute = dvec.aggregate([seg.name]).data.T

        # Convert to string names for segments
        segment_absolute.columns = [
            f'{c}: {seg.values[c]}' for c in segment_absolute.columns
        ]
        
        # Calculate proportions for mapping
        segment_proportional = segment_absolute.copy()
        zone_totals = segment_proportional.sum(axis=1)
        for col in segment_proportional.columns:
            segment_proportional[col] = segment_proportional[col] / zone_totals
        
        for unit, data in (('absolute', segment_absolute), ('proportional', segment_proportional)):
            label = f'{seg.name} ({unit})'
            segment_spatial = spatial_zoning.merge(
                data, how='left', right_index=True, left_on=id_col
            )

            output_map = render_map(
                segment_spatial, cols_to_map=segment_absolute.columns,
                cmap='viridis' if unit=='proportional' else 'plasma',
                cbar_label=label
            )

            output_map.save(output_folder / f'{label}.html')
