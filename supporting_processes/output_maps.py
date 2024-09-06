from collections import OrderedDict
import math

from caf.core import DVector, ZoningSystem
import folium
import geopandas as gpd
import branca as bc

# Define GOR to process
GOR = 'NW'
# define geopandas simplification tolerance (30 looks like good compromise between file size and boudnary detail)
# (think this relates to metres if the shapefile is in BNG)
tolerance = 30

# Get some example output - convert to MSOA (LSOA files were huge)
example_output = DVector.load(f'Output P11_{GOR}.hdf').translate_zoning(
    ZoningSystem.get_zoning(f'MSOA2021-{GOR}', search_dir=r'F:\Working\Land-Use\CACHE'),
    cache_path=r'F:\Working\Land-Use\CACHE'
)

# get segmentation from DVector
segs = example_output.data.index.names

spatial_zones = gpd.read_file(example_output.zoning_system.metadata.shapefile_path)
id_col = example_output.zoning_system.metadata.shapefile_id_col

spatial_zones = spatial_zones[[id_col, 'geometry']]

# loop through the available segmentations in the data
for seg in segs:
    print(f'Processing segmentation {seg}')

    # Take some example segment
    zonal_summary = example_output.aggregate([seg]).data.T

    # Folium seems to struggle with integer names - convert to strings.
    zonal_summary.columns = [example_output.segmentation.seg_dict[seg].values[c] for c in zonal_summary.columns]

    # Figure out the zone totals, and calculate proportions
    proportional_summary = zonal_summary.copy()
    zone_totals = proportional_summary.sum(axis=1)
    for col in proportional_summary.columns:
        proportional_summary[col] = proportional_summary[col] / zone_totals
    dfs = {'absolute': zonal_summary, 'proportional': proportional_summary}

    for unit, df in dfs.items():
        print(f'Processing {unit} data')

        # get if the dataframe is absolute or proportional data
        proportional = unit == 'proportional'

        # get upper and lower limit across all the categories (columns)
        if proportional:
            vmax = math.floor(df[df.columns].max().max() * 100) / 100.0
            vmin = math.ceil(df[df.columns].min().min() * 100) / 100.0
        else:
            vmax = math.ceil(df[df.columns].max().max())
            vmin = 0

        # Merge the spatial file with the data
        spatial_data = gpd.GeoDataFrame(
            spatial_zones.merge(df, how='left', right_index=True, left_on=id_col)
        )

        # try simplifying the geometry of the shapefile to see if it reduces the size of the outputs
        spatial_simplified = spatial_data.copy()
        spatial_simplified["geometry"] = spatial_data.geometry.simplify(tolerance=tolerance, preserve_topology=True)

        # Make the map, add the different layers and a switcher
        map = folium.Map(tiles=None)
        folium.TileLayer('CartoDB positron', name='Light Map', control=False).add_to(map)
        show = True
        for i, col in enumerate(zonal_summary.columns):
            if i > 0:
                show = False
            _ = spatial_simplified.explore(
                column=col,
                vmin=vmin,
                vmax=vmax,
                cmap='viridis' if proportional else 'plasma',
                m=map,
                name=col,
                overlay=False,
                tooltip_kwds={'localize': True},
                style_kwds={'color': 'black', 'weight': 1, 'fillOpacity': 0.3},
                show=show
            )

        # create layer control legend
        folium.LayerControl(collapsed=False).add_to(map)
        map.fit_bounds(map.get_bounds(), padding=(30, 30))

        # deal with color bar formatting, and only plotting one
        new_children = OrderedDict()
        has_cbar = False

        # remove colour bars
        for name, obj in map._children.items():
            if not isinstance(obj, bc.colormap.ColorMap):
                new_children[name] = obj
            elif not has_cbar:
                new_children[name] = obj
                obj.caption = f'{seg.upper()} ({unit.lower()})'
                has_cbar = True
        # overwrite children
        map._children = new_children

        # save interactive map output
        map.save(fr'C:/Projects/maps/{GOR}-{seg}-{unit}.html')
