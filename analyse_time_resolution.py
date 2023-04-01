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

from utilities import filter_dataframe
from utilities import make_histogram_plot
from utilities import make_2d_line_plot

import scipy.odr
import plotly.express as px
import plotly.graph_objects as go
from math import sqrt


def calculate_time_delta(
    iteration:int,
    board_list:list[int],
    data_df:pandas.DataFrame,
    pivot_df:pandas.DataFrame,
    ):
    delta_column = "time_delta"
    time_column = "time_of_arrival_twc_iteration_{}".format(iteration)
    for board_id in board_list:
        delta_board_column = (delta_column, board_id)

        columns_to_sum = []
        for other_board in board_list:
            if other_board == board_id:
                continue
            columns_to_sum += [(time_column, other_board)]
        pivot_df[delta_board_column] = pivot_df[columns_to_sum].sum(axis=1)/(len(board_list) - 1) - pivot_df[(time_column, board_id)]
    data_df[delta_column] = pivot_df.stack()[delta_column]

    diff_column = "time_diff"
    for idx_0 in range(len(board_list)):
        # Create the following pairs( for 3 boards): [(0, 1), (1, 2), (2, 0)]
        idx_1 = idx_0 + 1
        if idx_1 == len(board_list):
            idx_1 = 0
        board_id_0 = board_list[idx_0]
        board_id_1 = board_list[idx_1]

        diff_board_column = (diff_column, board_id_0)
        board_column_0 = (time_column, board_id_0)
        board_column_1 = (time_column, board_id_1)

        pivot_df[diff_board_column] = pivot_df[board_column_0] - pivot_df[board_column_1]
    data_df[diff_column] = pivot_df.stack()[diff_column]

def calculate_time_resolution_with_time_filters(
        Jorge: RM.TaskManager,
        script_logger: logging.Logger,
        original_df: pandas.DataFrame,
        time_filters: dict[str, Path],
        max_twc_iterations: int,
    ):
    board_list = sorted(original_df['data_board_id'].unique())
    N = len(board_list)  # N is the number of boards

    if N <= 2:
        script_logger.error("This script requires the data to be taken with at least 3 boards")
        return pandas.DataFrame()

    largest_idx = None
    for step in time_filters:
        if step == 'Final':
            continue
        step_idx = int(step.split('-')[0])
        if largest_idx is None or step_idx > largest_idx:
            largest_idx = step_idx

    timing_info = pandas.DataFrame()
    for step in time_filters:
        if step != 'Final':
            step_idx = int(step.split('-')[0])
        else:
            step_idx = largest_idx + 1

        data_df = filter_dataframe(
            df=original_df,
            filter_files={
                "event": Jorge.path_directory/"event_filter.fd",
                "time": time_filters[step],
            },
            script_logger=script_logger,
        )

        pivot_df = data_df.pivot(
            index = 'event',
            columns = 'data_board_id',
            values = list(set(data_df.columns) - {'data_board_id', 'event'}),
        )

        for twc_iteration in range(max_twc_iterations):
            twcDir = Jorge.task_path/("twc_iteration_{}".format(twc_iteration))
            twcDir.mkdir(exist_ok=True)
            outDir = twcDir/step
            outDir.mkdir(exist_ok=True)

            data_df.set_index(["event", "data_board_id"], inplace=True)

            calculate_time_delta(
                iteration=twc_iteration,
                board_list=board_list,
                data_df=data_df,
                pivot_df=pivot_df,
            )

            data_df.reset_index(inplace=True)

            make_histogram_plot(
                data_df=data_df,
                run_name=Jorge.run_name,
                task_name=Jorge.task_name,
                base_path=outDir,
                column_id="time_delta",
                axis_name="ΔT [ns]",
                variable_name="Time Delta",
                file_name="time_delta",
                extra_title="ΔTi = (∑ tj)/(N-1) - ti; j ≠ i"
            )

            make_histogram_plot(
                data_df=data_df,
                run_name=Jorge.run_name,
                task_name=Jorge.task_name,
                base_path=outDir,
                column_id="time_diff",
                axis_name="Δtij [ns]",
                variable_name="Time Difference",
                file_name="time_diff",
                extra_title="Δtij = ti - tj; j = i + 1"
            )

            grouped_data_df = data_df.query('accepted==True').groupby(["data_board_id"])

            twc_timing_info = pandas.DataFrame()
            twc_timing_info['time_delta_width'] = grouped_data_df['time_delta'].std()
            twc_timing_info['time_diff_width'] = grouped_data_df['time_diff'].std()
            twc_timing_info['time_delta_width_unc'] = twc_timing_info['time_delta_width']/((2*grouped_data_df['time_delta'].count() - 2)**(1/2))
            twc_timing_info['time_diff_width_unc'] = twc_timing_info['time_diff_width']/((2*grouped_data_df['time_diff'].count() - 2)**(1/2))
            twc_timing_info['step_order'] = step_idx
            twc_timing_info['step_name'] = step
            twc_timing_info['twc_iteration'] = twc_iteration

            # From Jongho:
            # Ex) b0 time resolution
            # a = b1_sigma^2 + b3_sigma^2 - b0_sigma^2
            # b = a * 0.5
            # b0_resolution = sqrt(b)
            # It seems this only works for 3 boards, so put a guard:
            if N == 3:
                twc_timing_info['time_resolution'] = 0
                twc_timing_info['time_resolution_unc'] = 0
                for board_idx in range(len(board_list)):
                    next_board_idx = board_idx + 1
                    if next_board_idx == len(board_list):
                        next_board_idx = 0

                    prev_board_idx = board_idx - 1
                    if prev_board_idx < 0:
                        prev_board_idx = len(board_list) - 1

                    sum = twc_timing_info.loc[board_list[board_idx]]['time_diff_width']**2
                    sum += twc_timing_info.loc[board_list[prev_board_idx]]['time_diff_width']**2
                    sum -= twc_timing_info.loc[board_list[next_board_idx]]['time_diff_width']**2
                    twc_timing_info.at[board_list[board_idx], 'time_resolution'] = sqrt(sum/2)

                    sum_unc_2 = (2*twc_timing_info.loc[board_list[board_idx]]['time_diff_width'])**2 * twc_timing_info.loc[board_list[board_idx]]['time_diff_width_unc']**2
                    sum_unc_2 += (2*twc_timing_info.loc[board_list[prev_board_idx]]['time_diff_width'])**2 * twc_timing_info.loc[board_list[prev_board_idx]]['time_diff_width_unc']**2
                    sum_unc_2 += (2*twc_timing_info.loc[board_list[next_board_idx]]['time_diff_width'])**2 * twc_timing_info.loc[board_list[next_board_idx]]['time_diff_width_unc']**2
                    twc_timing_info.at[board_list[board_idx], 'time_resolution_unc'] = sqrt(1/(8*sum) * sum_unc_2)

            # My own calculation
            twc_timing_info['time_resolution_new'] = 0
            twc_timing_info['time_resolution_new_unc'] = 0
            for board_id in board_list:
                sum = 0
                sum_unc_2 = 0
                for other_board in board_list:
                    if other_board == board_id:
                        continue
                    sum += twc_timing_info.loc[other_board]['time_delta_width']**2
                    sum_unc_2 += (2*twc_timing_info.loc[other_board]['time_delta_width'])**2 * twc_timing_info.loc[other_board]['time_delta_width_unc']**2

                sum = float(N**2 - N - 1)*twc_timing_info.loc[board_id]['time_delta_width']**2 - sum
                sum *= float(N-1)/float(N**3 - 2*N**2)
                sum_unc_2 = (2*float(N**2 - N - 1)*twc_timing_info.loc[board_id]['time_delta_width'])**2 * twc_timing_info.loc[board_id]['time_delta_width_unc']**2 + sum_unc_2
                sum_unc_2 *= (float(N-1)/float(N**3 - 2*N**2))**2
                twc_timing_info.at[board_id, 'time_resolution_new'] = sqrt(sum)
                twc_timing_info.at[board_id, 'time_resolution_new_unc'] = sqrt(1/(4*sum) * sum_unc_2)

            twc_timing_info.reset_index(inplace=True)

            #if step == "Final":
            #    print(twc_timing_info)

            # Save the timing info of this pair (cuts, iteration)
            with sqlite3.connect(outDir/'data.sqlite') as output_twc_sqlite3:
                twc_timing_info.to_sql('timing_info',
                                       output_twc_sqlite3,
                                       index=False,
                                       if_exists='replace')

            timing_info = pandas.concat([timing_info, twc_timing_info], ignore_index=True)

    #print(timing_info)
    return timing_info

def analyse_time_resolution_task(
    Linus: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    ):
    if Linus.task_completed("calculate_time_walk_correction"):
        with Linus.handle_task("analyse_time_resolution", drop_old_data=drop_old_data) as Jorge:
            with sqlite3.connect(Jorge.get_task_path("calculate_time_walk_correction")/'data.sqlite') as input_sqlite3_connection, \
                 sqlite3.connect(Jorge.task_path/'time_resolution.sqlite') as output_sqlite3_connection:
                original_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)
                twc_info_df = pandas.read_sql('SELECT * FROM twc_info', input_sqlite3_connection, index_col=None)

                max_twc_iterations = twc_info_df.iloc[0]['max_twc_iterations']
                board_list = sorted(original_df['data_board_id'].unique())

                time_filters = {
                    'Final': Jorge.path_directory/"time_filter.fd",
                }
                if Jorge.task_completed("apply_time_cuts"):
                    for dir in (Jorge.get_task_path("apply_time_cuts")/'CutflowPlots').iterdir():
                        time_filters[dir.name] = dir/"time_filter.fd"

                timing_df = calculate_time_resolution_with_time_filters(
                    Jorge,
                    script_logger=script_logger,
                    original_df=original_df,
                    time_filters=time_filters,
                    max_twc_iterations=max_twc_iterations,
                )

                timing_df.to_sql('timing_info',
                               output_sqlite3_connection,
                               index=False,
                               if_exists='replace')

                timing_df["data_board_id_cat"] = timing_df["data_board_id"].astype(str)

                plotDir = Jorge.task_path/'plots'
                plotDir.mkdir(exist_ok=True)
                twc_iteration = sorted(timing_df["twc_iteration"].unique())
                step_order = sorted(timing_df["step_order"].unique())

                for step_id in step_order:
                    tmp_df = timing_df.query('step_order=={}'.format(step_id)).reset_index()
                    tmp_df.sort_values(by=['data_board_id', 'twc_iteration'], inplace=True)

                    step_name = tmp_df.at[0, 'step_name']
                    if '-' in step_name:
                        step_name = str(step_name).split('-')[1]

                    outDir = plotDir/'Step-{}-{}'.format(step_id, step_name)
                    outDir.mkdir(exist_ok=True)

                    # Convert times from ns into ps
                    tmp_df["time_resolution_new"] = tmp_df["time_resolution_new"]*1000
                    tmp_df["time_resolution_new_unc"] = tmp_df["time_resolution_new_unc"]*1000

                    make_2d_line_plot(
                        data_df=tmp_df,
                        run_name=Jorge.run_name,
                        task_name=Jorge.task_name,
                        base_path=outDir,
                        plot_title="Time Resolution vs TWC Iteration",
                        subtitle="Time cut step: {}".format(step_name),
                        x_var="twc_iteration",
                        y_var="time_resolution_new",
                        y_error="time_resolution_new_unc",
                        file_name="time_resolution_vs_iteration",
                        color_var="data_board_id_cat",
                        labels={
                            'data_board_id_cat': 'Board ID',
                            'time_resolution_new': 'Time Resolution [ps]',
                            'twc_iteration': 'TWC Iteration',
                        },
                    )

                for iteration in twc_iteration:
                    tmp_df = timing_df.query('twc_iteration=={}'.format(iteration)).reset_index()
                    tmp_df.sort_values(by=['data_board_id', 'step_order'], inplace=True)

                    outDir = plotDir/'Iteration-{}'.format(iteration)
                    outDir.mkdir(exist_ok=True)

                    # Convert times from ns into ps
                    tmp_df["time_resolution_new"] = tmp_df["time_resolution_new"]*1000
                    tmp_df["time_resolution_new_unc"] = tmp_df["time_resolution_new_unc"]*1000

                    make_2d_line_plot(
                        data_df=tmp_df,
                        run_name=Jorge.run_name,
                        task_name=Jorge.task_name,
                        base_path=outDir,
                        plot_title="Time Resolution vs Time Cut Step",
                        subtitle="TWC Iteration: {}".format(iteration),
                        x_var="step_order",
                        y_var="time_resolution_new",
                        y_error="time_resolution_new_unc",
                        file_name="time_resolution_vs_step",
                        color_var="data_board_id_cat",
                        labels={
                            'data_board_id_cat': 'Board ID',
                            'time_resolution_new': 'Time Resolution [ps]',
                            'step_order': 'Cut Sequence',
                        },
                        text_var='step_name',
                    )

def script_main(
    output_directory:Path,
    make_plots:bool=True,
    ):

    script_logger = logging.getLogger('analyse_time_resolution')

    with RM.RunManager(output_directory.resolve()) as Linus:
        Linus.create_run(raise_error=False)

        if not Linus.task_completed("calculate_time_walk_correction"):
            raise RuntimeError("You can only run this script after calculating the time walk correction")

        analyse_time_resolution_task(Linus, script_logger=script_logger)

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

    script_main(Path(args.out_directory))