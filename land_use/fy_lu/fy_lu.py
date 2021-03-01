
import os

import pandas as pd

import land_use.lu_constants as consts
from land_use import utils as fyu


class FutureYearLandUse:
    def __init__(self,
                 model_zoning='msoa',
                 base_land_use=consts.LAND_USE_MSOA,
                 base_year='2018',
                 future_years=['2033', '2035', '2050'],
                 scenario_name='NTEM',
                 pop_growth_path=consts.NTEM_POP_GROWTH,
                 base_soc_mix_path=consts.SOC_2DIGIT_SIC,
                 pop_segmentation_cols=None):

        # Basic config
        self.model_zoning = model_zoning
        self.base_land_use = base_land_use
        self.base_year = base_year
        self.future_years = future_years
        self.scenario_name = scenario_name

        # Segmentation
        self.pop_segmentation_cols = pop_segmentation_cols

        # Pathing
        self.paths = {
            'base_land_use': base_land_use,
            'pop_growth': pop_growth_path,
            'base_soc_mix': base_soc_mix_path}


    def main(self):

        return 0

    def grow_pop(self,
                 base_year='2018',
                 future_years=['2033', '2035', '2050'],
                 segmentation_cols=None,
                 population_infill=0.001,
                 verbose=False
                 ):
        # Setup
        all_years = [str(x) for x in [base_year] + future_years]
        # Define zone col name
        if 'zone_id' not in self.model_zoning:
            zone_col = self.model_zoning.lower() + '_zone_id'
        else:
            zone_cols = self.model_zoning.lower()

        # Cars or CA??
        if segmentation_cols is None:
            segmentation_cols = [
                'area_type',
                'traveller_type',
                'soc',
                'ns',
                'cars'
            ]

        population_growth = pd.read_csv(self.paths['pop_growth'])

        # ## BASE YEAR POPULATION ## #
        print("Loading the base year population data...")
        base_year_pop = fyu.get_land_use_data(
            self.paths['base_land_use'],
            segmentation_cols=segmentation_cols,
            apply_ca_model=False)
        base_year_pop = base_year_pop.rename(columns={'people': base_year})

        # Audit population numbers
        print("Base Year Population: %d" % base_year_pop[base_year].sum())

        # ## FUTURE YEAR POPULATION ## #
        print("Generating future year population data...")
        # Merge on all possible segmentations - not years
        merge_cols = fyu.intersection(list(base_year_pop), list(population_growth))
        merge_cols = fyu.list_safe_remove(merge_cols, all_years)

        population = fyu.grow_to_future_years(
            base_year_df=base_year_pop,
            growth_df=population_growth,
            base_year=base_year,
            future_years=future_years,
            infill=population_infill,
            growth_merge_cols=merge_cols
        )

        # ## TIDY UP, WRITE TO DISK ## #
        # Reindex and sum
        group_cols = [zone_col] + segmentation_cols
        index_cols = group_cols.copy() + all_years
        population = population.reindex(index_cols, axis='columns')
        population = population.groupby(group_cols).sum().reset_index()

        # Population Audit
        if verbose:
            print('\n', '-' * 15, 'Population Audit', '-' * 15)
            for year in all_years:
                print('. Total population for year %s is: %.4f'
                      % (year, population[year].sum()))
            print('\n')

        # Write the produced population to file
        # print("Writing population to file...")
        # population_output = os.path.join(out_path, self.pop_fname)
        # population.to_csv(population_output, index=False)

        return population

    def get_soc_weights(self,
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
        soc_weighted_jobs = pd.read_csv(self.paths['soc_weights'])

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

    def split_by_soc(df: pd.DataFrame,
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

    def grow_emp(base_year='2018',
                 future_years=['2033', '2035', '2050'],
                 segmentation_cols=None,
                 employment_infill=0.001,
                 reports=True
                 ):
        # Init
        zoning_system = 'msoa'
        zone_col = '%s_zone_id' % zoning_system
        emp_cat_col = 'employment_cat'

        all_years = [str(x) for x in [base_year] + future_years]

        # Setup
        if segmentation_cols is None:
            segmentation_cols = [emp_cat_col]

        employment_growth = pd.read_csv(EMP_GROWTH_PATH)

        # ## BASE YEAR EMPLOYMENT ## #
        print("Loading the base year employment data...")
        base_year_emp = get_employment_data(
            import_path=imports['base_employment'],
            zone_col=zone_col,
            emp_cat_col=emp_cat_col,
            return_format='long',
            value_col=base_year,
        )

        # Audit employment numbers
        mask = (base_year_emp[emp_cat_col] == 'E01')
        total_base_year_emp = base_year_emp.loc[mask, base_year].sum()
        print("Base Year Employment: %d" % total_base_year_emp)

        # ## FUTURE YEAR EMPLOYMENT ## #
        print("Generating future year employment data...")
        # If soc splits in the growth factors, we have a few extra steps
        if 'soc' in employment_growth:
            # Add Soc splits into the base year
            base_year_emp = split_by_soc(
                df=base_year_emp,
                soc_weights=self.get_soc_weights(pd.read_csv(SOC_WEIGHTS_PATH)),
                unique_col=base_year,
                split_cols=[zone_col, emp_cat_col]
            )

            # Aggregate the growth factors to remove extra segmentation
            group_cols = [zone_col, 'soc']
            index_cols = group_cols.copy() + all_years

            # Make sure both soc columns are the same format
            base_year_emp['soc'] = base_year_emp['soc'].astype('float').astype('int')
            employment_growth['soc'] = employment_growth['soc'].astype('float').astype('int')
        else:
            # We're not using soc, remove it from our segmentations
            emp_segments.remove('soc')
            attr_segments.remove('soc')

        # Make sure our growth factors are at the right segmentation
        group_cols = [zone_col] + emp_segments.copy()
        group_cols.remove(emp_cat_col)
        index_cols = group_cols.copy() + all_years

        employment_growth = employment_growth.reindex(columns=index_cols)
        employment_growth = employment_growth.groupby(group_cols).mean().reset_index()

        # Merge on all possible segmentations - not years
        merge_cols = fyu.intersection(list(base_year_emp), list(employment_growth))
        merge_cols = fyu.list_safe_remove(merge_cols, all_years)

        employment = fyu.grow_to_future_years(
            base_year_df=base_year_emp,
            growth_df=employment_growth,
            base_year=base_year,
            future_years=future_years,
            growth_merge_cols=merge_cols,
            no_neg_growth=no_neg_growth,
            infill=employment_infill
        )

        # ## TIDY UP, WRITE TO DISK ## #
        # Earlier than previously to also save the soc segmentation
        if out_path is None:
            print("WARNING! No output path given. "
                  "Not writing employment to file.")
        else:
            print("Writing employment to file...")
            path = os.path.join(out_path, consts.EMP_FNAME % self.zone_col)
            employment.to_csv(path, index=False)

        # Reindex and sum
        # Removes soc splits - attractions weights can't cope
        group_cols = [zone_col] + emp_segments
        index_cols = group_cols.copy() + all_years
        employment = employment.reindex(index_cols, axis='columns')
        employment = employment.groupby(group_cols).sum().reset_index()

        # Population Audit
        if audits:
            print('\n', '-' * 15, 'Employment Audit', '-' * 15)
            mask = (employment[emp_cat_col] == 'E01')
            for year in all_years:
                total_emp = employment.loc[mask, year].sum()
                print('. Total jobs for year %s is: %.4f'
                      % (str(year), total_emp))
            print('\n')





