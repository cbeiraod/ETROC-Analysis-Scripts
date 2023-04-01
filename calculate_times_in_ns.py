#############################################################################
# zlib License
#
# (C) 2023 Cristóvão Beirão da Cruz e Silva <cbeiraod@cern.ch>
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.
#############################################################################

from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import pandas
import numpy
import sqlite3

from utilities import plot_times_in_ns_task


def calculate_times_in_ns_task(
    Fermat: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    fbin_choice:str="mean",
    ):
    if Fermat.task_completed("apply_event_cuts"):
        with Fermat.handle_task("calculate_times_in_ns", drop_old_data=drop_old_data) as Einstein:
            with sqlite3.connect(Einstein.path_directory/"data"/'data.sqlite') as input_sqlite3_connection, \
                 sqlite3.connect(Einstein.task_path/'data.sqlite') as output_sqlite3_connection:
                data_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

                filter_df = pandas.read_feather(Einstein.path_directory/"event_filter.fd")
                filter_df.set_index("event", inplace=True)

                from cut_etroc1_single_run import apply_event_filter
                data_df = apply_event_filter(data_df, filter_df)
                accepted_data_df = data_df.loc[data_df['accepted']==True]
                board_grouped_accepted_data_df = accepted_data_df.groupby(['data_board_id'])

                board_info_df = board_grouped_accepted_data_df[['calibration_code']].mean()
                board_info_df.rename(columns = {'calibration_code':'calibration_code_mean'}, inplace = True)
                board_info_df['calibration_code_median'] = board_grouped_accepted_data_df[['calibration_code']].median()
                board_info_df['fbin_mean'] = 3.125/board_info_df['calibration_code_mean']
                board_info_df['fbin_median'] = 3.125/board_info_df['calibration_code_median']

                #accepted_data_df.set_index("data_board_id", inplace=True)
                #accepted_data_df["fbin"] = board_info_df['fbin_mean']
                #accepted_data_df.reset_index(inplace=True)

                #accepted_data_df["time_of_arrival_ns"] = 12.5 - accepted_data_df['time_of_arrival']*accepted_data_df['fbin']
                #accepted_data_df["time_over_threshold_ns"] = (accepted_data_df["time_over_threshold"]*2 - (accepted_data_df["time_over_threshold"]/32.).apply(numpy.floor))*accepted_data_df['fbin']

                data_df.set_index("data_board_id", inplace=True)
                if fbin_choice == "mean":
                    data_df["fbin"] = board_info_df['fbin_mean']
                elif fbin_choice == "median":
                    data_df["fbin"] = board_info_df['fbin_median']
                elif fbin_choice == "event":
                    data_df["fbin"] = 3.125/data_df['calibration_code']
                data_df.reset_index(inplace=True)

                data_df["time_of_arrival_ns"] = 12.5 - data_df['time_of_arrival']*data_df['fbin']
                data_df["time_over_threshold_ns"] = (data_df["time_over_threshold"]*2 - (data_df["time_over_threshold"]/32.).apply(numpy.floor))*data_df['fbin']

                board_info_df.to_sql('board_info_data',
                                     output_sqlite3_connection,
                                     #index=False,
                                     if_exists='replace')

                data_df.drop(labels=['accepted', 'event_filter'], axis=1, inplace=True)

                data_df.to_sql('etroc1_data',
                               output_sqlite3_connection,
                               index=False,
                               if_exists='replace')

def script_main(
    output_directory:Path,
    make_plots:bool=True,
    max_toa:float=0,
    max_tot:float=0,
    ):

    script_logger = logging.getLogger('apply_event_cuts')

    if max_toa == 0:
        max_toa = None
    if max_tot == 0:
        max_tot = None

    with RM.RunManager(output_directory.resolve()) as Fermat:
        Fermat.create_run(raise_error=False)

        if not Fermat.task_completed("apply_event_cuts"):
            raise RuntimeError("You can only run this script after applying event cuts")

        calculate_times_in_ns_task(Fermat, script_logger=script_logger)

        if make_plots:
            plot_times_in_ns_task(
                Fermat,
                script_logger=script_logger,
                task_name="plot_times_in_ns_before_cuts",
                data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={},
                max_toa=max_toa,
                max_tot=max_tot,
                min_toa=0,
                min_tot=0,
            )

            plot_times_in_ns_task(
                Fermat,
                script_logger=script_logger,
                task_name="plot_times_in_ns_after_cuts",
                data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={"event": Fermat.path_directory/"event_filter.fd"},
                max_toa=max_toa,
                max_tot=max_tot,
                min_toa=0,
                min_tot=0,
            )

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with the KC 705 FPGA development board connected to an ETROC1 into our data format')
    parser.add_argument(
        '-l',
        '--log-level',
        help = 'Set the logging level. Default: WARNING',
        choices = ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"],
        default = "WARNING",
        dest = 'log_level',
    )
    parser.add_argument(
        '--log-file',
        help = 'If set, the full log will be saved to a file (i.e. the log level is ignored)',
        action = 'store_true',
        dest = 'log_file',
    )
    parser.add_argument(
        '-o',
        '--out-directory',
        metavar = 'path',
        help = 'Path to the output directory for the run data. Default: ./out',
        default = "./out",
        dest = 'out_directory',
        type = str,
    )
    parser.add_argument(
        '-a',
        '--max_toa',
        metavar = 'int',
        help = 'Maximum value of the time of arrival (in ns) for plotting. Default: 0 (automatically calculated)',
        default = 0,
        dest = 'max_toa',
        type = float,
    )
    parser.add_argument(
        '-t',
        '--max_tot',
        metavar = 'int',
        help = 'Maximum value of the time over threshold (in ns) for plotting. Default: 0 (automatically calculated)',
        default = 0,
        dest = 'max_tot',
        type = float,
    )

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename='logging.log', filemode='w', encoding='utf-8', level=logging.NOTSET)
    else:
        if args.log_level == "CRITICAL":
            logging.basicConfig(level=50)
        elif args.log_level == "ERROR":
            logging.basicConfig(level=40)
        elif args.log_level == "WARNING":
            logging.basicConfig(level=30)
        elif args.log_level == "INFO":
            logging.basicConfig(level=20)
        elif args.log_level == "DEBUG":
            logging.basicConfig(level=10)
        elif args.log_level == "NOTSET":
            logging.basicConfig(level=0)

    script_main(Path(args.out_directory), max_toa=args.max_toa, max_tot=args.max_tot)