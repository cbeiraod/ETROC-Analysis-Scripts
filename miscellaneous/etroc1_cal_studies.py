from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import numpy as np
import sqlite3
import plotly.graph_objects as go

from utilities import apply_event_filter

def data_df_apply_single_cut(
    data_df: pandas.DataFrame,
    board_id:int,
    variable:str,
    cut_type:str,
    cut_value:str,
    keep_nan:bool=False,
    ):
    if keep_nan:
        extra_rows_to_keep = data_df[(variable, board_id)].isna()
    else:
        extra_rows_to_keep = False

    if cut_type == '<':
        return ((data_df[(variable, board_id)] < cut_value) | extra_rows_to_keep)
    elif cut_type == '<=':
        return ((data_df[(variable, board_id)] <= cut_value) | extra_rows_to_keep)
    elif cut_type == '>':
        return ((data_df[(variable, board_id)] > cut_value) | extra_rows_to_keep)
    elif cut_type == '>=':
        return ((data_df[(variable, board_id)] >= cut_value) | extra_rows_to_keep)
    elif cut_type == '==':
        return ((data_df[(variable, board_id)] == cut_value) | extra_rows_to_keep)
    elif cut_type == '<>':
        return ((data_df[(variable, board_id)] != cut_value) | extra_rows_to_keep)
    else:
        raise ValueError('Received a cut of type `cut_type: {}`, dont know that that is...'.format(cut_type))

def df_apply_cut(
    df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    board_id:str,
    variable:str,
    cut_type:str,
    cut_value:str,
    keep_nan:bool=False,
    ):
    if board_id != "*" and board_id != "#":
        df['accepted'] &= data_df_apply_single_cut(data_df, int(board_id), variable, cut_type, cut_value, keep_nan=keep_nan)
    else:
        full_cut = None
        board_ids = data_df.stack().reset_index(level="data_board_id")["data_board_id"].unique()
        for this_board_id in board_ids:
            cut = data_df_apply_single_cut(data_df, int(this_board_id), variable, cut_type, cut_value, keep_nan=keep_nan)
            if full_cut is None:
                full_cut = cut
            else:
                if board_id == "*":
                    full_cut &= cut
                elif board_id == "#":
                    full_cut |= cut
                else:  # WTF
                    raise RuntimeError("WTF is going on...")
        df['accepted'] &= full_cut

    return df

def apply_event_cuts(
    data_df: pandas.DataFrame,
    cuts_df: pandas.DataFrame,
    script_logger: logging.Logger,
    Johnny: RM.TaskManager,
    keep_events_without_data:bool = False,
    ):
    """
    Given a dataframe `cuts_df` with one cut per row, e.g.
    ```
               variable  board_id  cut_type  cut_value
       calibration_code         1         <        200
       calibration_code         0         >        140
    time_over_threshold         3        >=        300
    ```
    this function returns a series with the index `event` and the value
    either `True` or `False` stating if the even satisfies ALL the
    cuts at the same time.
    """
    board_id_list = data_df['data_board_id'].unique()
    for board_id in cuts_df['board_id'].unique():
        if board_id != "*" and board_id != "#":
            if int(board_id) not in board_id_list:
                raise ValueError("The board_id defined in the cuts file ({}) can not be found in the data. The set of board_ids defined in data is: {}".format(board_id, board_id_list))

    pivot_data_df = data_df.pivot(
        index = 'event',
        columns = 'data_board_id',
        values = list(set(data_df.columns) - {'data_board_id', 'event'}),
    )

    base_path = Johnny.task_path.resolve()/"CutflowPlots"
    base_path.mkdir(exist_ok=True)

    triggers_accepted_df = pandas.DataFrame({'accepted': True}, index=pivot_data_df.index)
    for idx, cut_row in cuts_df.iterrows():
        triggers_accepted_df = df_apply_cut(triggers_accepted_df, pivot_data_df, cut_row['board_id'], cut_row['variable'], cut_row['cut_type'], cut_row['cut_value'], keep_nan=keep_events_without_data)

        if "output" in cut_row and isinstance(cut_row["output"], str):
            script_logger.info("Making partial cut plots after cut {}:\n{}".format(idx, cut_row))
            base_name = str(idx) + "-" + cut_row["output"]
            (base_path/base_name).mkdir(exist_ok=True)
            this_data_df = apply_event_filter(data_df, triggers_accepted_df)
            build_plots(this_data_df, Johnny.run_name, Johnny.task_name, base_path/base_name, extra_title="Partial Cuts")
            del this_data_df

    return triggers_accepted_df

def apply_event_cuts_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    keep_events_without_data:bool = False,
):
    if AdaLovelace.task_completed("proccess_etroc1_data_run") or AdaLovelace.task_completed("proccess_etroc1_data_run_txt"):
        with AdaLovelace.handle_task("apply_event_cuts", drop_old_data=drop_old_data) as Miso:
            if not (Miso.path_directory/"cuts.csv").is_file():
                script_logger.info("A cuts file is not defined for run {}".format(AdaLovelace.run_name))
            else:
                with sqlite3.connect(Miso.path_directory/"data"/'data.sqlite') as input_sqlite3_connection:
                    cuts_df = pandas.read_csv(Miso.path_directory/"cuts.csv")

                    if ("board_id" not in cuts_df or
                        "variable" not in cuts_df or
                        "cut_type" not in cuts_df or
                        "cut_value" not in cuts_df
                        ):
                        script_logger.error("The cuts file does not have the correct format")
                        raise RuntimeError("Bad cuts config file")
                    cuts_df.to_csv(Miso.task_path/'cuts.backup.csv', index=False)

                    input_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

                    filtered_events_df = apply_event_cuts(input_df, cuts_df, script_logger=script_logger, Johnny=Miso, keep_events_without_data=keep_events_without_data)

                    script_logger.info('Saving run event filter metadata...')
                    filtered_events_df.reset_index().to_feather(Miso.task_path/'event_filter.fd')
                    filtered_events_df.reset_index().to_feather(Miso.path_directory/'event_filter.fd')


def filter_dataframe(
    df:pandas.DataFrame,
    filter_files:dict[Path],
    script_logger:logging.Logger,
    ):
    for filter in filter_files:
        if filter_files[filter].is_file():
            filter_df = pandas.read_feather(filter_files[filter])
            filter_df.set_index("event", inplace=True)

            if filter == "event":
                df = apply_event_filter(df, filter_df)
            elif filter == "time":
                df = apply_event_filter(df, filter_df, filter_name="time_filter")
        else:
            script_logger.error("The filter file {} does not exist".format(filter_files[filter]))

    return df

def plot_cal_task(
        Bob_Manager:RM.RunManager,
        task_name:str,
        data_file:Path,
        filter_files:dict[str,Path] = {},
        drop_old_data:bool = True,
        extra_title: str = "",
        ):

    script_logger = logging.getLogger('run_plotter')

    if not data_file.is_file():
        script_logger.info("The data file should be an existing file")
        return

    with Bob_Manager.handle_task(task_name, drop_old_data=drop_old_data) as Picasso:
        with sqlite3.connect(data_file) as sqlite3_connection:
            df = pandas.read_sql('SELECT * FROM etroc1_data', sqlite3_connection, index_col=None)

            df = filter_dataframe(
                df=df,
                filter_files=filter_files,
                script_logger=script_logger,
            )

            build_cal_plots(df, Picasso.run_name, task_name, Picasso.task_path, extra_title=extra_title)

def build_cal_plots(
    original_df: pandas.DataFrame,
    run_name: str,
    task_name: str,
    base_path: Path,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    extra_title: str = ""
):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    if "accepted" in original_df:
        df = original_df.query('accepted==True')
    else:
        df = original_df
    
    # Even - Even
    fig = go.Figure()
    for board_id in sorted(df["data_board_id"].unique()):
        fig.add_trace(go.Histogram(
            x=df.loc[(df["time_of_arrival"]%2 == 0) & (df["calibration_code"]%2 == 0)]["calibration_code"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Calibration Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Calibration Code', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )
    fig.update_yaxes(type="log")

    fig.write_html(
        base_path/'calibration_code_histogram_evenTOA_evenCAL.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    # Even - Odd
    fig = go.Figure()
    for board_id in sorted(df["data_board_id"].unique()):
        fig.add_trace(go.Histogram(
            x=df.loc[(df["time_of_arrival"]%2 == 0) & (df["calibration_code"]%2 == 1)]["calibration_code"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Calibration Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Calibration Code', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )
    fig.update_yaxes(type="log")

    fig.write_html(
        base_path/'calibration_code_histogram_evenTOA_oddCAL.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    # Odd - Odd
    fig = go.Figure()
    for board_id in sorted(df["data_board_id"].unique()):
        fig.add_trace(go.Histogram(
            x=df.loc[(df["time_of_arrival"]%2 == 1) & (df["calibration_code"]%2 == 1)]["calibration_code"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Calibration Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Calibration Code', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )
    fig.update_yaxes(type="log")

    fig.write_html(
        base_path/'calibration_code_histogram_oddTOA_oddCAL.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    # Odd - Even
    fig = go.Figure()
    for board_id in sorted(df["data_board_id"].unique()):
        fig.add_trace(go.Histogram(
            x=df.loc[(df["time_of_arrival"]%2 == 1) & (df["calibration_code"]%2 == 0)]["calibration_code"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Calibration Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Calibration Code', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )
    fig.update_yaxes(type="log")

    fig.write_html(
        base_path/'calibration_code_histogram_oddTOA_evenCAL.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def script_main(
        output_directory:Path,
        drop_old_data:bool=True,
        make_plots:bool=True,
        keep_events_without_data:bool=False,
        ):

    script_logger = logging.getLogger('apply_event_cuts')

    with RM.RunManager(output_directory.resolve()) as Bob:
        Bob.create_run(raise_error=False)

        apply_event_cuts_task(
            Bob,
            script_logger=script_logger,
            drop_old_data=drop_old_data,
            keep_events_without_data=keep_events_without_data,
        )

        if Bob.task_completed("apply_event_cuts") and make_plots:
            plot_cal_task(Bob, "plot_after_cuts", Bob.path_directory/"data"/"data.sqlite", filter_files={"event": Bob.path_directory/"event_filter.fd"})

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

    script_main(Path(args.out_directory), keep_events_without_data=args.keep_events_without_data)
