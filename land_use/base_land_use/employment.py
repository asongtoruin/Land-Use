
import pandas as pd


def get_emp_data(by_lu_obj):

    """
    Takes base year object and imports employment vectors
    """
    emp_dict = dict()

    emp_dict.update({'e_cat': pd.read_csv(by_lu_obj.e_cat_emp_path)})
    emp_dict.update({'soc_cat': pd.read_csv(by_lu_obj.soc_emp_path)})
    emp_dict.update({'umn': pd.read_csv(by_lu_obj.unemp_path)})

    by_lu_obj.emp_data = emp_dict

    return 0


def skill_weight_e_cats(by_lu_obj,
                        verbose=True):

    """
    Reduce soc to factors and weight out e_cat jobs by skill
    """

    # Soc dat in
    soc_dat = by_lu_obj.emp_data['soc_cat'].copy()

    # Sum by seg and soc total
    soc_factor = soc_dat.groupby(
        ['gor', 'soc_cat'])['seg_jobs'].sum().reset_index()
    soc_total = soc_dat.groupby('gor')['seg_jobs'].sum().reset_index()
    soc_total = soc_total.rename(columns={'seg_jobs': 'total_jobs'})

    # Make factors
    soc_factor = soc_factor.merge(soc_total,
                                  how='left',
                                  on='gor')
    soc_factor['soc_factor'] = soc_factor['seg_jobs']/soc_factor['total_jobs']
    # YZ - fill na with zero
    soc_factor['soc_factor'] = soc_factor['soc_factor'].fillna(0)
    soc_factor = soc_factor.reindex(
        ['gor', 'soc_cat', 'soc_factor'], axis=1)

    # Apply to e_cat dat
    # Splits cats equally by e-cat, which is a bad assumption

    # Bring in GOR to MSOA Lookup
    # TODO: Move to constants
    msoa_to_gor = r'I:\Data\Zone Translations\msoa_to_gor_correspondence.csv'
    # Join region to MSOA in SOCs
    msoa_gor = pd.read_csv(msoa_to_gor)
    msoa_gor = msoa_gor.reindex(['msoa_zone_id', 'gor'], axis=1)
    msoa_gor = msoa_gor.drop_duplicates()

    soc_factor = soc_factor.merge(msoa_gor,
                                  how='left',
                                  on='gor')

    # Pivot ee cats to long
    emp_data = by_lu_obj.emp_data['e_cat'].copy()

    # Build E01
    # TODO: E01 should be removed, weight by individual cats
    # Don't like how it doubles all of the jobs
    emp_data['E01'] = emp_data.sum(axis=1)

    # Define audit total
    in_total = emp_data['E01'].sum()
    emp_data = emp_data.melt(id_vars='msoa_zone_id',
                             var_name='e_cat',
                             value_name='employment')

    # Merge on totals
    emp_out = emp_data.merge(
        soc_factor,
        how='left',
        on='msoa_zone_id')

    # Factor by SOC
    emp_out['employment'] *= emp_out['soc_factor']
    emp_out = emp_out.drop('soc_factor', axis=1)

    # Format
    format_cols = ['msoa_zone_id', 'e_cat', 'soc_cat', 'employment']
    emp_out = emp_out.reindex(format_cols, axis=1)
    emp_out = emp_out.sort_values(format_cols[0:-1]).reset_index(drop=True)

    # Out total
    out_total = emp_out['employment'].sum()

    # print
    if verbose:
        print('In employment %d, out employment (excl. E01) %d' % (in_total, out_total/2))

    by_lu_obj.emp_out = emp_out

    return 0


def unemp_infill(by_lu_obj):

    """
    Takes a 3 skill employment vector, infills SOC4 using proportion of unemployed
    """

    # Get unm data
    unm_data = by_lu_obj.emp_data['umn']
    # get unm total
    unm_total = unm_data['unm'].sum()
    # Factor by 2 because E01 :(
    unm_total *= 2

    # Get emp data
    emp_data = by_lu_obj.emp_out.copy()
    summary_cols = ['msoa_zone_id', 'e_cat', 'employment']
    emp_summary = emp_data.groupby(
        summary_cols[0:-1])['employment'].sum().reset_index()
    emp_summary['employment'] /= emp_summary['employment'].sum()

    unm_in = emp_summary.copy()
    unm_in['employment'] *= unm_total
    unm_in['soc_cat'] = 4

    emp_out = pd.concat([emp_data, unm_in])

    format_cols = ['msoa_zone_id', 'e_cat', 'soc_cat', 'employment']
    emp_out = emp_out.reindex(format_cols, axis=1)
    emp_out = emp_out.sort_values(format_cols[0:-1]).reset_index(drop=True)

    by_lu_obj.emp_out = emp_out

    return 0