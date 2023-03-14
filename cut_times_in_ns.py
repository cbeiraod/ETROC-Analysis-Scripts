from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import numpy as np
import sqlite3

from utilities import plot_etroc1_task
from utilities import plot_times_in_ns_task
from utilities import build_plots
from utilities import build_time_plots
from utilities import apply_event_filter

from cut_etroc1_single_run import df_apply_cut
from cut_etroc1_single_run import apply_numeric_comparison_to_column

from math import sqrt



def df_apply_diagonal_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_direction:str,
    column_1:tuple,
    column_2:tuple,
    distance_value:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna() | data_df[column_2].isna()
    else:
        extra_rows_to_keep = False

    distance = data_df[column_1] + data_df[column_2]

    accepted_df['accepted'] &= apply_numeric_comparison_to_column(distance, cut_direction, distance_value, "diagonal cut") | extra_rows_to_keep

    return accepted_df

def df_apply_diagonal_distance_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_direction:str,
    column_1:tuple,
    column_2:tuple,
    distance_value:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna() | data_df[column_2].isna()
    else:
        extra_rows_to_keep = False

    distance = (data_df[column_1] - data_df[column_2]).abs()/sqrt(2)

    accepted_df['accepted'] &= apply_numeric_comparison_to_column(distance, cut_direction, distance_value, "diagonal distance") | extra_rows_to_keep

    return accepted_df

def df_apply_fit_distance_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_direction:str,
    column_1:tuple,
    column_2:tuple,
    distance_value:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna() | data_df[column_2].isna()
    else:
        extra_rows_to_keep = False

    import scipy.stats
    fit = scipy.stats.linregress(x=data_df[column_1].astype(float), y=data_df[column_2].astype(float))

    # https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
    distance = (data_df[column_1]*fit.slope - data_df[column_2] + fit.intercept)/(sqrt(fit.slope**2 + 1))

    accepted_df['accepted'] &= apply_numeric_comparison_to_column(distance.abs(), cut_direction, distance_value, "fit distance") | extra_rows_to_keep

    return accepted_df

def df_apply_circle_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_direction:str,
    column_1:tuple,
    column_2:tuple,
    center_1:str,
    center_2:str,
    radius:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna() | data_df[column_2].isna()
    else:
        extra_rows_to_keep = False

    distance = (data_df[column_1] - center_1)**2 + (data_df[column_2] - center_2)**2

    if cut_direction == "inside":
        accepted_df['accepted'] &= (distance < radius**2) | extra_rows_to_keep
    elif cut_direction == "outside":
        accepted_df['accepted'] &= (distance > radius**2) | extra_rows_to_keep
    else:
        raise RuntimeError("Unknown cut direction for circle: {}".format(cut_direction))

    return accepted_df

def df_apply_corner_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    corner_direction:int,
    cut_direction:str,
    column_1:tuple,
    column_2:tuple,
    edge_1:str,
    edge_2:str,
    radius:str,
    keep_nan:bool=False,
    ):
    """
    corner_direction defines the direction of the corner:
      1 - up-right
      2 - up-left
      3 - down-right
      4 - down-left
    """
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna() | data_df[column_2].isna()
    else:
        extra_rows_to_keep = False

    region_1 = (data_df[column_1] < edge_1) & (data_df[column_2] < edge_2 - radius)
    region_2 = (data_df[column_1] < edge_1 - radius) & (data_df[column_2] < edge_2)
    region_3 = ((data_df[column_1] - (edge_1 - radius))**2 + (data_df[column_2] - (edge_2 - radius))**2) < radius**2

    if corner_direction == 1:  # up-right
        region_1 = (data_df[column_1] < edge_1) & (data_df[column_2] <= edge_2 - radius)
        region_2 = (data_df[column_1] <= edge_1 - radius) & (data_df[column_2] < edge_2)
        region_3 = ((data_df[column_1] - (edge_1 - radius))**2 + (data_df[column_2] - (edge_2 - radius))**2) < radius**2
    elif corner_direction == 2:  # up-left
        region_1 = (data_df[column_1] > edge_1) & (data_df[column_2] <= edge_2 - radius)
        region_2 = (data_df[column_1] >= edge_1 + radius) & (data_df[column_2] < edge_2)
        region_3 = ((data_df[column_1] - (edge_1 + radius))**2 + (data_df[column_2] - (edge_2 - radius))**2) < radius**2
    elif corner_direction == 3:  # down-right
        region_1 = (data_df[column_1] < edge_1) & (data_df[column_2] >= edge_2 + radius)
        region_2 = (data_df[column_1] <= edge_1 - radius) & (data_df[column_2] > edge_2)
        region_3 = ((data_df[column_1] - (edge_1 - radius))**2 + (data_df[column_2] - (edge_2 + radius))**2) < radius**2
    elif corner_direction == 4:  # down-left
        region_1 = (data_df[column_1] > edge_1) & (data_df[column_2] >= edge_2 + radius)
        region_2 = (data_df[column_1] >= edge_1 + radius) & (data_df[column_2] > edge_2)
        region_3 = ((data_df[column_1] - (edge_1 + radius))**2 + (data_df[column_2] - (edge_2 + radius))**2) < radius**2
    else:
        raise RuntimeError("Unknown corner direction for corner: {}".format(corner_direction))

    if cut_direction == "inside":
        accepted_df['accepted'] &= (region_1 | region_2 | region_3) | extra_rows_to_keep
    elif cut_direction == "outside":
        accepted_df['accepted'] &= (~(region_1 | region_2 | region_3)) | extra_rows_to_keep
    else:
        raise RuntimeError("Unknown cut direction for corner: {}".format(cut_direction))

    return accepted_df

def df_apply_1d_distance_cut(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_direction:str,
    column_1:tuple,
    center:str,
    limit:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[column_1].isna()
    else:
        extra_rows_to_keep = False

    distance = (data_df[column_1] - center).abs()

    if cut_direction == "inside":
        accepted_df['accepted'] &= (distance < limit) | extra_rows_to_keep
    elif cut_direction == "outside":
        accepted_df['accepted'] &= (distance > limit) | extra_rows_to_keep
    else:
        raise RuntimeError("Unknown cut direction for 1d distance: {}".format(cut_direction))

    return accepted_df

def df_apply_time_cut_governor(
    accepted_df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    cut_type:str,
    cut_direction:str,
    variable_1:str,
    board_id_1:str,
    variable_2:str,
    board_id_2:str,
    value_1:str,
    value_2:str,
    value_3:str,
    keep_nan:bool=False,
    ):
    if cut_type == "simple":
        return df_apply_cut(
            accepted_df,
            data_df,
            board_id=board_id_1,
            variable=variable_1,
            cut_type=cut_direction,
            cut_value=value_1,
            keep_nan=keep_nan,
        )
    elif cut_type == "circle":
        return df_apply_circle_cut(
            accepted_df,
            data_df,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            value_2,
            value_3,
            keep_nan=keep_nan,
        )
    elif cut_type == "corner-ur":
        return df_apply_corner_cut(
            accepted_df,
            data_df,
            1,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            value_2,
            value_3,
            keep_nan=keep_nan,
        )
    elif cut_type == "corner-ul":
        return df_apply_corner_cut(
            accepted_df,
            data_df,
            2,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            value_2,
            value_3,
            keep_nan=keep_nan,
        )
    elif cut_type == "corner-dr":
        return df_apply_corner_cut(
            accepted_df,
            data_df,
            3,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            value_2,
            value_3,
            keep_nan=keep_nan,
        )
    elif cut_type == "corner-dl":
        return df_apply_corner_cut(
            accepted_df,
            data_df,
            4,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            value_2,
            value_3,
            keep_nan=keep_nan,
        )
    elif cut_type == "fit-dist":
        return df_apply_fit_distance_cut(
            accepted_df,
            data_df,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            keep_nan=keep_nan,
        )
    elif cut_type == "diag-dist":
        return df_apply_diagonal_distance_cut(
            accepted_df,
            data_df,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            keep_nan=keep_nan,
        )
    elif cut_type == "diagonal":
        return df_apply_diagonal_cut(
            accepted_df,
            data_df,
            cut_direction,
            (variable_1, board_id_1),
            (variable_2, board_id_2),
            value_1,
            keep_nan=keep_nan,
        )
    elif cut_type == "1d-dist":
        return df_apply_1d_distance_cut(
            accepted_df,
            data_df,
            cut_direction,
            (variable_1, board_id_1),
            value_1,
            value_2,
            keep_nan=keep_nan,
        )
    else:
        raise RuntimeError("Unknown cut type: {}".format(cut_type))

def apply_time_cuts(
    Shinji: RM.TaskManager,
    data_df: pandas.DataFrame,
    time_cuts_df: pandas.DataFrame,
    script_logger: logging.Logger,
    max_toa:float=20,
    max_tot:float=20,
    min_toa:float=-20,
    min_tot:float=-20,
    keep_events_without_data:bool = False,
    ):
    """
    Given a dataframe `time_cuts_df` with one cut per row, e.g.
    ```
    cut_type  cut_direction              variable_1  board_id_1              variable_2  board_id_2  value_1  value_2  value_3
      corner        outside  time_over_threshold_ns           0  time_over_threshold_ns           1      2.6      2.3      0.8
      simple              <  time_over_threshold_ns           0                     NaN         NaN        6      NaN      NaN
    ```
    this function returns a series with the index `event` and the value
    either `True` or `False` stating if the even satisfies ALL the
    cuts at the same time.
    """
    board_id_list = data_df['data_board_id'].unique()
    for board_id in time_cuts_df['board_id_1'].unique():
        if board_id != "*" and board_id != "#":
            if int(board_id) not in board_id_list:
                raise ValueError("The board_id defined in the cuts file ({}) can not be found in the data. The set of board_ids defined in data is: {}".format(board_id, board_id_list))
    for board_id in time_cuts_df['board_id_2'].unique():
        if not np.isnan(board_id) and board_id != "*" and board_id != "#":
            if int(board_id) not in board_id_list:
                raise ValueError("The board_id defined in the cuts file ({}) can not be found in the data. The set of board_ids defined in data is: {}".format(board_id, board_id_list))

    pivot_data_df = data_df.pivot(
        index = 'event',
        columns = 'data_board_id',
        values = list(set(data_df.columns) - {'data_board_id', 'event'}),
    )

    base_path = Shinji.task_path.resolve()/"CutflowPlots"
    base_path.mkdir(exist_ok=True)

    triggers_accepted_df = pandas.DataFrame({'accepted': True}, index=pivot_data_df.index)
    for idx, cut_row in time_cuts_df.iterrows():
        if cut_row['cut_type'][0] == "#":  # If first character is #, then we skip the row
            continue
        triggers_accepted_df = df_apply_time_cut_governor(
            triggers_accepted_df,
            pivot_data_df,
            cut_row['cut_type'],
            cut_row['cut_direction'],
            cut_row['variable_1'],
            cut_row['board_id_1'],
            cut_row['variable_2'],
            cut_row['board_id_2'],
            cut_row['value_1'],
            cut_row['value_2'],
            cut_row['value_3'],
            keep_nan=keep_events_without_data,
        )

        if "output" in cut_row and isinstance(cut_row["output"], str):
            script_logger.info("Making partial cut plots after cut {}:\n{}".format(idx, cut_row))
            base_name = str(idx) + "-" + cut_row["output"]
            (base_path/base_name).mkdir(exist_ok=True)
            (base_path/base_name/"plots").mkdir(exist_ok=True)
            (base_path/base_name/"time_plots").mkdir(exist_ok=True)
            this_data_df = data_df
            if Shinji.task_completed("apply_event_cuts"):
                event_accepted_df = pandas.read_feather(Shinji.get_task_path("apply_event_cuts")/"event_filter.fd")
                event_accepted_df.set_index("event", inplace=True)
                this_data_df = apply_event_filter(this_data_df, event_accepted_df)
            this_data_df = apply_event_filter(this_data_df, triggers_accepted_df, filter_name="time_filter")
            triggers_accepted_df.reset_index().to_feather(base_path/base_name/'time_filter.fd')
            build_plots(this_data_df, Shinji.run_name, Shinji.task_name, base_path/base_name/"plots", extra_title="Partial Cuts")
            build_time_plots(this_data_df, base_path/base_name/"time_plots", Shinji.run_name, Shinji.task_name, extra_title="Partial Cuts", max_toa=max_toa, max_tot=max_tot, min_toa=min_toa, min_tot=min_tot)
            del this_data_df

    return triggers_accepted_df

def apply_time_cuts_task(
    Dexter: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    max_toa:float=20,
    max_tot:float=20,
    min_toa:float=-20,
    min_tot:float=-20,
    keep_events_without_data:bool = False,
):
    if Dexter.task_completed("calculate_times_in_ns"):
        with Dexter.handle_task("apply_time_cuts", drop_old_data=drop_old_data) as Shinji:
            if not (Shinji.path_directory/"time_cuts.csv").is_file():
                script_logger.info("A time cuts file is not defined for run {}".format(Dexter.run_name))
            else:
                with sqlite3.connect(Shinji.get_task_path("calculate_times_in_ns")/'data.sqlite') as input_sqlite3_connection:
                    cuts_df = pandas.read_csv(Shinji.path_directory/"time_cuts.csv")

                    if ("cut_type" not in cuts_df or
                        "cut_direction" not in cuts_df or
                        "variable_1" not in cuts_df or
                        "board_id_1" not in cuts_df or
                        "variable_2" not in cuts_df or
                        "board_id_2" not in cuts_df or
                        "value_1" not in cuts_df or
                        "value_2" not in cuts_df or
                        "value_3" not in cuts_df
                        ):
                        script_logger.error("The time cuts file does not have the correct format")
                        raise RuntimeError("Bad time cuts config file")
                    cuts_df.to_csv(Shinji.task_path/'cuts.backup.csv', index=False)

                    input_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

                    filtered_events_df = apply_time_cuts(
                        Shinji,
                        input_df,
                        cuts_df,
                        script_logger=script_logger,
                        max_toa=max_toa,
                        max_tot=max_tot,
                        min_toa=min_toa,
                        min_tot=min_tot,
                        keep_events_without_data=keep_events_without_data,
                    )
                    filtered_events_df.reset_index(inplace=True)

                    script_logger.info('Saving run event filter metadata...')
                    filtered_events_df.to_feather(Shinji.task_path/'time_filter.fd')
                    filtered_events_df.to_feather(Shinji.path_directory/'time_filter.fd')

def script_main(
    output_directory:Path,
    drop_old_data:bool=True,
    make_plots:bool=False,
    max_toa:float=0,
    max_tot:float=0,
    keep_events_without_data:bool=False,
    ):

    script_logger = logging.getLogger('apply_time_cuts')

    if max_toa == 0:
        max_toa = None
    if max_tot == 0:
        max_tot = None

    with RM.RunManager(output_directory.resolve()) as Dexter:
        Dexter.create_run(raise_error=False)

        if not Dexter.task_completed("calculate_times_in_ns"):
            raise RuntimeError("You can only run this script after calculating the times in ns")

        apply_time_cuts_task(
            Dexter,
            script_logger=script_logger,
            drop_old_data=drop_old_data,
            max_toa=max_toa,
            max_tot=max_tot,
            min_toa=0,
            min_tot=0,
            keep_events_without_data=keep_events_without_data,
        )

        if Dexter.task_completed("apply_time_cuts") and make_plots:
            plot_etroc1_task(
                Dexter,
                "plot_after_time_cuts",
                Dexter.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={
                    "event": Dexter.path_directory/"event_filter.fd",
                    "time": Dexter.path_directory/"time_filter.fd",
                }
            )
            plot_times_in_ns_task(
                Dexter,
                script_logger=script_logger,
                task_name="plot_time_after_time_cuts",
                data_file=Dexter.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={
                    "event": Dexter.path_directory/"event_filter.fd",
                    "time": Dexter.path_directory/"time_filter.fd",
                },
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
    parser.add_argument(
        '-k',
        '--keep-events',
        help = 'Normally, when applying cuts if a certain board does not have data for a given event, the cut will remove that event. If set, these events will be kept instead.',
        action = 'store_true',
        dest = 'keep_events_without_data',
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

    script_main(
        Path(args.out_directory),
        max_toa=args.max_toa,
        max_tot=args.max_tot,
        keep_events_without_data=args.keep_events_without_data
    )