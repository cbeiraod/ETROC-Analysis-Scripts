from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import numpy as np
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task
from plot_etroc1_single_run import make_plots

def data_df_apply_single_cut(
    data_df: pandas.DataFrame,
    board_id:int,
    variable:str,
    cut_type:str,
    cut_value:str,
    ):
    if cut_type == '<':
        return data_df[(variable, board_id)] < cut_value
    elif cut_type == '<=':
        return data_df[(variable, board_id)] <= cut_value
    elif cut_type == '>':
        return data_df[(variable, board_id)] > cut_value
    elif cut_type == '>=':
        return data_df[(variable, board_id)] >= cut_value
    elif cut_type == '==':
        return data_df[(variable, board_id)] == cut_value
    elif cut_type == '<>':
        return data_df[(variable, board_id)] != cut_value
    else:
        raise ValueError('Received a cut of type `cut_type: {}`, dont know that that is...'.format(cut_type))

def df_apply_cut(
    df: pandas.DataFrame,
    data_df: pandas.DataFrame,
    board_id:str,
    variable:str,
    cut_type:str,
    cut_value:str,
    ):
    if board_id != "*" and board_id != "#":
        df['accepted'] &= data_df_apply_single_cut(data_df, int(board_id), variable, cut_type, cut_value)
    else:
        full_cut = None
        board_ids = data_df.stack().reset_index(level="data_board_id")["data_board_id"].unique()
        for this_board_id in board_ids:
            cut = data_df_apply_single_cut(data_df, int(this_board_id), variable, cut_type, cut_value)
            if full_cut is None:
                full_cut = cut
            else:
                if board_id == "*":
                    full_cut &= cut
                elif board_id == "*":
                    full_cut |= cut
                else:  # WTF
                    raise RuntimeError("WTF is going on...")
        df['accepted'] &= full_cut

    return df

def apply_event_filter(data_df: pandas.DataFrame, filter_df: pandas.DataFrame):
    reindexed_data_df = data_df.set_index('event')
    reindexed_data_df["event_filter"] = filter_df
    if "accepted" not in reindexed_data_df:
        reindexed_data_df["accepted"] = filter_df
    else:
        reindexed_data_df["accepted"] &= filter_df
    return reindexed_data_df.reset_index()

def apply_event_cuts(
    data_df: pandas.DataFrame,
    cuts_df: pandas.DataFrame,
    script_logger: logging.Logger,
    Johnny: RM.TaskManager,
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
        values = list(set(data_df.columns) - {'data_board_id'}),
    )

    base_path = Johnny.task_path.resolve()/"CutflowPlots"
    base_path.mkdir(exist_ok=True)

    triggers_accepted_df = pandas.DataFrame({'accepted': True}, index=pivot_data_df.index)
    for idx, cut_row in cuts_df.iterrows():
        triggers_accepted_df = df_apply_cut(triggers_accepted_df, pivot_data_df, cut_row['board_id'], cut_row['variable'], cut_row['cut_type'], cut_row['cut_value'])

        if "output" in cut_row and isinstance(cut_row["output"], str):
            script_logger.info("Making partial cut plots after cut {}:\n{}".format(idx, cut_row))
            base_name = str(idx) + "-" + cut_row["output"]
            (base_path/base_name).mkdir(exist_ok=True)
            this_data_df = apply_event_filter(data_df, triggers_accepted_df)
            make_plots(this_data_df, Johnny.run_name, Johnny.task_name, base_path/base_name, extra_title="Partial Cuts")
            del this_data_df

    return triggers_accepted_df

def apply_event_cuts_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
):
    if AdaLovelace.task_completed("proccess_etroc1_data_run") or AdaLovelace.task_completed("proccess_etroc1_data_run_txt"):
        with AdaLovelace.handle_task("apply_event_cuts", drop_old_data=drop_old_data) as Miso:
            if not (Miso.path_directory/"cuts.csv").is_file():
                script_logger.info("A cuts file is not defined for run {}".format(AdaLovelace.run_name))
                shutil.copy(Miso.path_directory/"data"/'data.sqlite', Miso.task_path/"data.sqlite")
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

                    filtered_events_df = apply_event_cuts(input_df, cuts_df, script_logger=script_logger, Johnny=Miso)

                    script_logger.info('Saving run event filter metadata...')
                    filtered_events_df.reset_index().to_feather(Miso.task_path/'event_filter.fd')
                    filtered_events_df.reset_index().to_feather(Miso.path_directory/'event_filter.fd')

def script_main(
        output_directory:Path,
        drop_old_data:bool=True,
        make_plots:bool=True
        ):

    script_logger = logging.getLogger('apply_event_cuts')

    with RM.RunManager(output_directory.resolve()) as Bob:
        Bob.create_run(raise_error=False)

        apply_event_cuts_task(
            Bob,
            script_logger=script_logger,
            drop_old_data=drop_old_data,
        )

        if Bob.task_completed("apply_event_cuts") and make_plots:
            plot_etroc1_task(Bob, "plot_after_cuts", Bob.path_directory/"data"/"data.sqlite", filter_files={"event": Bob.path_directory/"event_filter.fd"})



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
