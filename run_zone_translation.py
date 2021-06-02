
"""
Translate list of land uses to a zoning system
"""

import pathlib
import os

import pandas as pd

import land_use.utils.translate as trans
import land_use.utils.file_ops as file_ops


def main():

    write = True
    trans_list = ['I:/NorMITs Land Use/base_land_use/iter3b/outputs/land_use_output_safe_msoa.csv']
    msoa_to_noham = 'I:/NorMITs Demand/import/zone_translation/weighted/msoa_noham_pop_weighted_lookup.csv'

    model_zoning = 'noham'

    # Format lookup
    corr = pd.read_csv(msoa_to_noham)
    ri_cols = ['msoa_zone_id', 'noham_zone_id', 'msoa_to_noham']
    corr = corr.reindex(ri_cols, axis=1)

    # TODO: MP
    for translation in trans_list:
        lu_dat = pd.read_csv(translation)

        retain_cols = ['area_type', 'household_composition',
                         'ca', 'traveller_type', 'soc', 'ns']

        trans_out = trans.vector_join_translation(
            lu_data=lu_dat,
            trans_df=corr,
            retain_cols=retain_cols,
            join_id='msoa_zone_id',
            zone_id='noham_zone_id',
            var_col='people',
            weight_col='msoa_to_noham',
        )

        if write:
            path_name = pathlib.Path(translation)
            folder = path_name.parent
            folder = folder / model_zoning
            file = path_name.stem.replace('msoa', model_zoning) + path_name.suffix
            if not os.path.exists(folder):
                file_ops.create_folder(str(folder))
            out_path = folder / file
            trans_out.to_csv(out_path)

    return 0


if __name__ == '__main__':

    main()
