import pandas as pd
import numpy as np
import os
from caf.toolkit import iterative_proportional_fitting as ipfn


def main():
    # ----- Read and Format input data ------

    seed_path = r'C:\Users\BenQ\Documents\NorMITs\Inputs\test1\Seed.csv'
    ctrl_z_path = r'C:\Users\BenQ\Documents\NorMITs\Inputs\test1\Ctrl_z.csv'
    ctrl_dhtn_path = r'C:\Users\BenQ\Documents\NorMITs\Inputs\test1\Ctrl_dhtn.csv'
    ctrl_dage__path = r'C:\Users\BenQ\Documents\NorMITs\Inputs\test1\Ctrl_dage+.csv'
    ctrl_ds_path = r'C:\Users\BenQ\Documents\NorMITs\Inputs\test1\Ctrl_ds.csv'
    seed = pd.read_csv(seed_path)
    ctrl_z = pd.read_csv(ctrl_z_path)
    ctrl_dhtn = pd.read_csv(ctrl_dhtn_path)
    ctrl_dage_ = pd.read_csv(ctrl_dage__path)
    ctrl_ds = pd.read_csv(ctrl_ds_path)
    # --- make sure the data is float---
    seed['Population'] = seed['Population'].astype(float)
    ctrl_dhtn['Population'] = ctrl_dhtn['Population'].astype(float)
    ctrl_dage_['Population'] = ctrl_dage_['Population'].astype(float)
    ctrl_ds['Population'] = ctrl_ds['Population'].astype(float)
    ctrl_z['Population'] = ctrl_z['Population'].astype(float)
    # ----- Define Marginals ------
    ctrl_z = ctrl_z.groupby(['z'])['Population'].sum()
    ctrl_dhtn = ctrl_dhtn.groupby(['d', 'h', 't', 'n'])['Population'].sum()
    ctrl_dage_ = ctrl_dage_.groupby(['d', 'a', 'g', 'e+'])['Population'].sum()
    ctrl_ds = ctrl_ds.groupby(['d', 's'])['Population'].sum()

    marginals = [ctrl_z, ctrl_dhtn, ctrl_dage_, ctrl_ds]
    # ----- Run ipf ------
    done_df, iters, conv = ipfn.ipf_dataframe(
        seed_df=seed,
        target_marginals=marginals,
        value_col="Population",
        max_iterations=5000,
        tol=1e-9,
        min_tol_rate=1e-9,
        show_pbar=True
    )
    output_folder = r'C:\Users\BenQ\Documents\NorMITs\Outputs'
    os.chdir(output_folder)
    output_filename = 'furnessed_pop_test1.csv'
    done_df.to_csv(output_filename, index=False)



if __name__ == "__main__":
    main()
