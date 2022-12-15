
import os

import land_use.reports.sector_report as sr
import land_use.lu_constants as luc
import land_use.utils.file_ops as fo

if __name__ == '__main__':

    model_name = 'msoa'
    iteration = 'iter3d'
    run_folder = 'I:/NorMITs Land Use/future_land_use/%s/outputs/scenarios' % iteration
    # This should come from constants really
    base_path = 'I:/NorMITs Land Use/base_land_use/iter3d/outputs'
    scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    years = ['2018', '2033', '2035', '2040', '2050']
    out_folder = 'C:/Users/%s/Documents/Sector Reports' % os.getlogin()

    folder_list = list()
    for sc in scenarios:
        for y in years:
            if y == luc.BASE_YEAR:
                if base_path not in folder_list:
                    folder_list.append(
                        os.path.join(
                            base_path
                            )
                        )
            else:
                path = os.path.join(
                    run_folder, '%s' % sc)
                if path not in folder_list:
                    folder_list.append(path)

    for folder in folder_list:

        print('Building sector reports for %s' % folder)
        report = sr.SectorReporter(target_folder=folder,
                                   retain_cols=['ca'])

        out = report.sector_report(ca_report=True,
                                   three_sector_report=False,
                                   ie_sector_report=False,
                                   north_report=False)

        out_folder = os.path.join(folder, 'sector_reports')
        if not os.path.exists(out_folder):
            fo.create_folder(out_folder)
        
        for lu_out, reps in out.items():
            print('.')
            print('unpacking reports for %s' % lu_out)
            for sub_r, dat in reps.items():
                rep_out = lu_out + '_' + sub_r + '.csv'
                print('.')
                print('exporting %s' % rep_out)
                out_path = os.path.join(out_folder, rep_out)
                dat.to_csv(out_path, index=False)



