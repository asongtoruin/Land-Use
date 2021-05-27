
import os

import land_use.reports.sector_report as sr

if __name__ == '__main__':

    model_name = 'msoa'
    iteration = 'iter3b'
    run_folder = 'I:/NorMITs Land Use/future_land_use/%s/scenarios' % iteration
    scenarios = ['SC01_JAM', 'SC02_PP', 'SC03_DD', 'SC04_UZC']
    out_folder = 'C:/Users/%s/Documents/sector_reports' % os.getlogin()

    folder_list = list()
    for sc in scenarios:
        folder_list.append(
            os.path.join(
                run_folder, '%s' % sc))

    for folder in folder_list:

        print('Building sector reports for %s' % folder)
        report = sr.SectorReporter(target_folder=folder,
                                   retain_cols=['ca'])

        out = report.sector_report(ca_report=True,
                                   three_sector_report=False,
                                   ie_sector_report=False,
                                   north_report=False)

        out_folder = os.path.join(folder, 'sector_reports')

        for lu_out, reps in out.items():
            print('.')
            print('unpacking reports for %s' % lu_out)
            for sub_r, dat in reps.items():
                rep_out = lu_out + '_' + sub_r + '.csv'
                print('.')
                print('exporting %s' % rep_out)
                out_path = os.path.join(out_folder, rep_out)
                dat.to_csv(out_path, index=False)



