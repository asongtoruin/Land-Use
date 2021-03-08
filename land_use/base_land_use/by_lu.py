import os

import pandas as pd

import land_use.lu_constants as consts
from land_use import utils as fyu


class BaseYearLandUse:
    def __init__(self,
                 model_folder=consts.LU_FOLDER,
                 iteration=consts.LU_MR_ITER,
                 import_folder=consts.LU_IMPORTS,
                 model_zoning='msoa',
                 base_land_use_path=consts.RESI_LAND_USE_MSOA,
                 base_employment_path=consts.EMPLOYMENT_MSOA,
                 base_soc_mix_path=consts.SOC_2DIGIT_SIC,
                 base_year='2018',
                 pop_segmentation_cols=None):

        # File ops
        self.model_folder = model_folder
        self.iteration = iteration
        self.import_folder = import_folder

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year

        # Segmentation
        self.pop_segmentation_cols = pop_segmentation_cols

        write_folder = os.path.join(
            model_folder,
            iteration,
            'outputs')

        if not os.path.exists(write_folder):
            fyu.create_folder(write_folder)

        pop_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.base_year) + '_pop.csv'))

        emp_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.base_year) + '_emp.csv'))

        # Pathing
        self.in_paths = {
            'base_land_use': base_land_use_path,
            'base_employment': base_employment_path,
            'base_soc_mix': base_soc_mix_path,
            }

        self.out_paths = {
            'write_folder': write_folder,
            'pop_write_path': pop_write_name,
            'emp_write_path': emp_write_name
        }

    def build_by_pop(self):
        """
        """

        return 0

    def build_by_emp(self,
                     export=True,
                     verbose=True):

        emp = self._build_emp(verbose=verbose)

        if export:
            if verbose:
                print('Writing to:')
                print(self.out_paths['emp_write_path'])
            emp.to_csv(self.out_paths['emp_write_path'], index=False)

        return emp

    def _build_emp(self,
                   verbose=False):
        # Init
        zone_col = self._define_zone_col()
        emp_cat_col = 'employment_cat'

        # ## BASE YEAR EMPLOYMENT ## #
        print("Loading the base year employment data...")
        base_year_emp = fyu.get_land_use(
            path=self.in_paths['base_employment'],
            model_zone_col=zone_col,
            segmentation_cols=None,
            add_total=True,
            total_col_name='E01',
            to_long=True,
            long_var_name=emp_cat_col,
            long_value_name=self.base_year)

        # Print employment numbers
        print(base_year_emp[base_year_emp[emp_cat_col] == 'E01'])

        # Add Soc splits into the base year
        base_year_emp = self._split_by_soc(
            df=base_year_emp,
            soc_weights=self._get_soc_weights(),
            unique_col=self.base_year,
            split_cols=[zone_col, emp_cat_col]
            )

        return base_year_emp

    def _define_zone_col(self):
        """
        Work out a sensible column name to use as the model zoning id.

        Returns
        -------
        mz_id: A model zoning ID

        """
        if 'zone_id' not in self.model_zoning:
            zone_col = self.model_zoning.lower() + '_zone_id'
        else:
            zone_col = self.model_zoning.lower()

        return zone_col

    def _get_pop_emp(self,
                     vector_type,
                     retain_cols):
        """
        vector_type = 'pop' or 'emp'
        """

        if vector_type == 'pop':
            dat = pd.read_csv(self.in_paths['pop_growth'], dtype={'soc': str})
        elif vector_type == 'emp':
            dat = pd.read_csv(self.in_paths['emp_growth'], dtype={'soc': str})
        ri_cols = list([self.model_zoning + '_zone_id'])
        for col in retain_cols:
            if col in list(dat):
                ri_cols.append(col)
        ri_cols.append(self.future_year)
        dat = dat.reindex(ri_cols, axis=1)

    @staticmethod
    def _split_by_soc(df: pd.DataFrame,
                      soc_weights: pd.DataFrame,
                      zone_col: str = 'msoa_zone_id',
                      p_col: str = 'p',
                      unique_col: str = 'trips',
                      soc_col: str = 'soc',
                      split_cols: str = None
                      ) -> pd.DataFrame:
        """
        Splits df purposes by the soc_weights given.

        Parameters
        ----------
        df:
            Dataframe to add soc splits too. Must contain the following columns
            [zone_col, p_col, unique_col]

        soc_weights:
            Wide dataframe containing the soc splitting weights. Must have a
            zone_col columns, and all other columns are the soc categories to split
            by.

        zone_col:
            The name of the column in df and soc_weights that contains the
            zone data.

        p_col:
            Name of the column in df that contains purpose data.

        unique_col:
            Name of the column in df that contains the unique data (usually the
            number of trips at that row of segmentation)

        soc_col:
            The name to give to the added soc column in the return dataframe.

        split_cols:
            Which columns are being split by soc. If left as None, only zone_col
            is used.

        Returns
        -------
        soc_split_df:
            df with an added soc_col. Unique_col will be split by the weights
            given
        """
        # Init
        soc_cats = list(soc_weights.columns)
        # Drop zone col if it's made its way in
        soc_cats = [x for x in soc_cats if zone_col not in x]
        split_cols = [zone_col] if split_cols is None else split_cols

        # Figure out which rows need splitting
        if p_col in df:
            mask = (df[p_col].isin(consts.SOC_P))
            split_df = df[mask].copy()
            retain_df = df[~mask].copy()
            id_cols = split_cols + [p_col]
        else:
            # Split on all data
            split_df = df.copy()
            retain_df = None
            id_cols = split_cols

        # Split by soc weights
        split_df = pd.merge(
            split_df,
            soc_weights,
            on=zone_col
        )

        for soc in soc_cats:
            split_df[soc] *= split_df[unique_col]

        # Tidy up the split dataframe ready to re-merge
        split_df = split_df.drop(unique_col, axis='columns')
        # Re melt - get soc back as col
        split_df = split_df.melt(
            id_vars=id_cols,
            value_vars=soc_cats,
            var_name=soc_col,
            value_name=unique_col,
        )

        # Don't need to stick back together
        if retain_df is None:
            return split_df

        # Add the soc col to the retained values to match
        retain_df[soc_col] = 0

        # Finally, stick the two back together
        return pd.concat([split_df, retain_df])

    def _get_soc_weights(self,
                         zone_col: str = 'msoa_zone_id',
                         soc_col: str = 'soc_class',
                         jobs_col: str = 'seg_jobs',
                         str_cols: bool = False
                         ) -> pd.DataFrame:
        """
        Converts the input file into soc weights by zone

        Parameters
        ----------
        soc_weights_path:
            Path to the soc weights file. Must contain at least the following
            column names [zone_col, soc_col, jobs_col]

        zone_col:
            The column name in soc_weights_path that contains the zone data.

        soc_col:
            The column name in soc_weights_path that contains the soc categories.

        jobs_col:
            The column name in soc_weights_path that contains the number of jobs
            data.

        str_cols:
            Whether the return dataframe columns should be as [soc1, soc2, ...]
            (if True), or [1, 2, ...] (if False).

        Returns
        -------
        soc_weights:
            a wide dataframe with zones from zone_col as the column names, and
            soc categories from soc_col as columns. Each row of soc weights will
            sum to 1.
        """
        # Init
        soc_weighted_jobs = pd.read_csv(self.in_paths['base_soc_mix'])

        # Convert soc numbers to names (to differentiate from ns)
        soc_weighted_jobs[soc_col] = soc_weighted_jobs[soc_col].astype(int).astype(str)

        if str_cols:
            soc_weighted_jobs[soc_col] = 'soc' + soc_weighted_jobs[soc_col]

        # Calculate Zonal weights for socs
        # This give us the benefit of model purposes in HSL data
        group_cols = [zone_col, soc_col]
        index_cols = group_cols.copy()
        index_cols.append(jobs_col)

        soc_weights = soc_weighted_jobs.reindex(index_cols, axis='columns')
        soc_weights = soc_weights.groupby(group_cols).sum().reset_index()
        soc_weights = soc_weights.pivot(
            index=zone_col,
            columns=soc_col,
            values=jobs_col
        ).reset_index()

        # Convert to factors
        soc_segments = soc_weighted_jobs[soc_col].unique()
        soc_weights['total'] = soc_weights[soc_segments].sum(axis='columns')

        for soc in soc_segments:
            soc_weights[soc] /= soc_weights['total']

        soc_weights = soc_weights.drop('total', axis='columns')

        return soc_weights