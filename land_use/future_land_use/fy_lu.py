
import os

import pandas as pd

import land_use.lu_constants as consts
from land_use import utils as fyu


class FutureYearLandUse:
    def __init__(self,
                 model_folder=consts.LU_FOLDER,
                 iteration=consts.LU_MR_ITER,
                 import_folder=consts.LU_IMPORTS,
                 model_zoning='msoa',
                 base_land_use_path=consts.RESI_LAND_USE_MSOA,
                 base_employment_path=consts.EMPLOYMENT_MSOA,
                 base_soc_mix_path=consts.SOC_2DIGIT_SIC,
                 base_year='2018',
                 future_year='2033',
                 scenario_name='NTEM',
                 pop_growth_path=None,
                 emp_growth_path=None,
                 ca_growth_path=None,
                 pop_segmentation_cols=None):

        # File ops
        self.model_folder = model_folder
        self.iteration = iteration
        self.import_folder = import_folder

        # Basic config
        self.model_zoning = model_zoning
        self.base_year = base_year
        self.future_year = future_year
        self.scenario_name = scenario_name.upper()

        # If Nones passed in, parse paths
        if pop_growth_path is None:
            pop_growth_path = self._get_scenario_path(
                'pop_growth',
            )
        if emp_growth_path is None:
            emp_growth_path = self._get_scenario_path(
                'emp_growth',
            )
        if ca_growth_path is None:
            ca_growth_path = self._get_scenario_path(
                'ca_growth',
            )

        # Segmentation
        self.pop_segmentation_cols = pop_segmentation_cols

        write_folder = os.path.join(
            model_folder,
            iteration,
            'outputs',
            'scenarios',
            scenario_name)

        if not os.path.exists(write_folder):
            fyu.create_folder(write_folder)

        pop_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.future_year) + '_pop.csv'))

        emp_write_name = os.path.join(
            write_folder,
            ('land_use_' + str(self.future_year) + '_emp.csv'))

        # Pathing
        self.in_paths = {
            'base_land_use': base_land_use_path,
            'base_employment': base_employment_path,
            'base_soc_mix': base_soc_mix_path,
            'pop_growth': pop_growth_path,
            'emp_growth': emp_growth_path,
            'ca_growth': ca_growth_path
            }

        self.out_paths = {
            'write_folder': write_folder,
            'pop_write_path': pop_write_name,
            'emp_write_path': emp_write_name
        }

    def build_fy_pop(self,
                     adjust_ca=True,
                     adjust_soc=True,
                     export=True,
                     verbose=True):
        """
        """
        fy_pop = self._grow_pop(verbose=verbose)

        if adjust_ca:
            fy_pop = self._adjust_ca(fy_pop)

        if adjust_soc:
            print('If this were an FTS youd be adjusting soc by now')

        if export:
            if verbose:
                print('Writing to:')
                print(self.out_paths['pop_write_path'])
            fy_pop.to_csv(self.out_paths['pop_write_path'],
                          index=False)

        return fy_pop

    def build_fy_emp(self,
                     export=True,
                     verbose=True):

        fy_emp = self._grow_emp(verbose=verbose)

        if export:
            if verbose:
                print('Writing to:')
                print(self.out_paths['emp_write_path'])
            fy_emp.to_csv(self.out_paths['emp_write_path'],
                          index=False)

        return fy_emp

    def _get_scenario_path(self,
                           vector):
        """
        Parameters
        ----------
        vector = ['pop_growth', 'emp_growth','ca_growth']

        Returns
        -------
        path : path to required vector
        """

        if vector == 'pop_growth':
            target_folder = 'population',
            target_file = 'future_population_growth.csv'
        elif vector == 'emp_growth':
            target_folder = 'employment',
            target_file = 'future_employment_growth.csv'
        elif vector == 'ca_growth':
            target_folder = 'car ownership'
            target_file = 'ca_future_shares.csv'
        else:
            raise ValueError('Not sure where to look for ' + vector)

        # Run scenario name through consts to get name
        sc_path = os.path.join(
            self.model_folder,
            self.iteration,
            self.import_folder,
            consts.SCENARIO_FOLDERS[self.scenario_name],
            target_folder,
            target_file
        )

        return sc_path


    def _grow_pop(self,
                  verbose=False
                  ):

        # Define zone col name
        zone_col = self._define_zone_col()

        #TODO: Need to refactor the growth if bases are misaligned

        # Get pop growth, filter to target year only
        population_growth = self._get_fy_pop_emp(
            'pop')

        # ## BASE YEAR POPULATION ## #
        # TODO: Fix the segmentation cols in utils
        print("Loading the base year population data...")
        base_year_pop = fyu.get_land_use(
            self.in_paths['base_land_use'],
            model_zone_col=zone_col,
            segmentation_cols=None)
        base_year_pop = base_year_pop.rename(
            columns={'people': self.base_year})

        # Audit population numbers
        print("Base Year Population: %d" % base_year_pop[self.base_year].sum())

        # ## FUTURE YEAR POPULATION ## #
        print("Generating future year population data...")
        # Merge on all possible segmentations - not years
        merge_cols = fyu.intersection(list(base_year_pop), list(population_growth))

        population = self._grow_to_future_year(
            by_vector=base_year_pop,
            fy_vector=population_growth,
            merge_cols=merge_cols
        )

        # ## TIDY UP, WRITE TO DISK ## #
        # Reindex and sum

        # Population Audit
        if verbose:
            print(list(population))
            print('\n', '-' * 15, 'Population Audit', '-' * 15)
            print('Total population for year %s is: %.4f' % (self.future_year, population[self.future_year].sum()))
            print('\n')

        # Write the produced population to file
        # print("Writing population to file...")
        # population_output = os.path.join(out_path, self.pop_fname)
        # population.to_csv(population_output, index=False)

        return population

    def _grow_emp(self,
                  verbose=False):
        # Init
        zone_col = self._define_zone_col()
        emp_cat_col = 'employment_cat'

        employment_growth = pd.read_csv(self.in_paths['emp_growth'])

        # ## BASE YEAR EMPLOYMENT ## #
        print("Loading the base year employment data...")
        base_year_emp = fyu.get_land_use(
            path=self.in_paths['base_employment'],
            model_zone_col=zone_col,
            segmentation_cols=None)
        # Fill in an EO1 Value, if you have to
        if 'E01' not in list(base_year_emp):
            # TODO: Make an EO1
            print('Code filler')

        # Audit employment numbers

        mask = (base_year_emp[emp_cat_col] == 'E01')
        total_base_year_emp = base_year_emp.loc[mask, self.base_year].sum()
        print("Base Year Employment: %d" % total_base_year_emp)

        # ## FUTURE YEAR EMPLOYMENT ## #
        print("Generating future year employment data...")
        # If soc splits in the growth factors, we have a few extra steps
        if 'soc' in employment_growth:
            # Add Soc splits into the base year
            base_year_emp = self._split_by_soc(
                df=base_year_emp,
                soc_weights=self._get_soc_weights(
                    pd.read_csv(self.in_paths['base_soc_mix'])),
                unique_col=self.base_year,
                split_cols=[zone_col, emp_cat_col]
            )

        # Merge on all possible segmentations - not years
        merge_cols = fyu.intersection(list(base_year_emp), list(employment_growth))

        employment = self._grow_to_future_year(
            by_vector=base_year_emp,
            fy_vector=employment_growth)

        return employment

    def _adjust_ca(self,
                   fy_pop_vector,
                   verbose=True) -> pd.DataFrame:
        """

        Parameters
        ----------
        fy_pop_vector: Ready adjusted future year pop vector

        Returns
        -------
        fy_pop_vector: Population vector with adjusted car availability
        ca_adjustment_factors: Report Dataframe

        """
        # Get zone name
        zone_col = self._define_zone_col()

        # Build base year ca totals
        by_ca = fy_pop_vector.copy()
        by_ca = by_ca.reindex(
            [zone_col, 'ca', self.future_year], axis=1).groupby(
            [zone_col, 'ca']).sum().reset_index()
        # Get by ca totals
        by_ca = by_ca.pivot(
            index=zone_col,
            columns='ca',
            values=self.future_year).reset_index()
        by_ca['total'] = by_ca[1] + by_ca[2]
        by_ca[1] /= by_ca['total']
        by_ca[2] /= by_ca['total']
        by_ca = by_ca.drop('total', axis=1)
        by_ca = by_ca.melt(id_vars = zone_col,
                           var_name='ca',
                           value_name=self.future_year)
        by_ca = by_ca.rename(
            columns={self.future_year:'by_ca'})

        # Get ca shares
        ca_shares = pd.read_csv(self.in_paths['ca_growth'])
        # Filter to target_year
        ca_shares = ca_shares.reindex(
            [zone_col, 'ca', self.future_year], axis=1)
        ca_shares = ca_shares.rename(columns={self.future_year: 'fy_ca'})

        # Join
        fy_ca_factors = by_ca.merge(
            ca_shares,
            how='left',
            on=[zone_col, 'ca'])
        fy_ca_factors['ca_adj'] = fy_ca_factors['fy_ca'] / fy_ca_factors['by_ca']
        fy_ca_factors = fy_ca_factors.drop(['by_ca', 'fy_ca'], axis=1)

        before = fy_pop_vector[self.future_year].sum()

        # Adjust CA
        fy_pop_vector = fy_pop_vector.merge(
            fy_ca_factors,
            how='left',
            on=[zone_col, 'ca'])
        fy_pop_vector[self.future_year] *= fy_pop_vector['ca_adj']
        fy_pop_vector = fy_pop_vector.drop('ca_adj', axis=1)

        after = fy_pop_vector[self.future_year].sum()

        if verbose:
            print('*' * 15)
            print('Car availability adjustment')
            print('Total before: ' + str(before))
            print('Total after: ' + str(after))

        return fy_pop_vector

    def _grow_to_future_year(self,
                             by_vector: pd.DataFrame,
                             fy_vector: pd.DataFrame,
                             merge_cols=None,
                             verbose=True) -> pd.DataFrame:
        """
        Parameters
        ----------
        by_vector: Dataframe of a base year pop/emp vector
        fy_vector: Dataframe of growth factors from a given base year
        to a give future year, by year

        Returns
        -------
        fy_vector: Absolute totals from by_vector, grown to fy totals
        """
        if merge_cols is None:
            merge_cols = self._define_zone_col()

        start = by_vector[self.base_year].sum()

        fy_vector = by_vector.merge(fy_vector,
                                    how='left',
                                    on=merge_cols)
        merge = fy_vector[self.base_year].sum()

        fy_vector[self.future_year] *= fy_vector[self.base_year]
        end = fy_vector[self.future_year].sum()
        fy_vector = fy_vector.drop(self.base_year, axis=1)

        if verbose:
            print('Start: ' + str(start))
            print('Merge:' + str(merge))
            print('End:' + str(end))

        return fy_vector

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
        soc_weighted_jobs = pd.read_csv(self.in_paths['soc_weights'])

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
        )

        # Convert to factors
        soc_segments = soc_weighted_jobs[soc_col].unique()
        soc_weights['total'] = soc_weights[soc_segments].sum(axis='columns')

        for soc in soc_segments:
            soc_weights[soc] /= soc_weights['total']

        soc_weights = soc_weights.drop('total', axis='columns')

        return soc_weights

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

    def _get_fy_pop_emp(self,
                        vector_type):
        """
        vector_type = 'pop' or 'emp'
        """

        if vector_type == 'pop':
            dat = pd.read_csv(self.in_paths['pop_growth'])
        elif vector_type == 'emp':
            dat = pd.read_csv(self.in_paths['emp_growth'])
        ri_cols = [
            self.model_zoning + '_zone_id', self.future_year]
        dat = dat.reindex(ri_cols, axis=1)

        return dat

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
