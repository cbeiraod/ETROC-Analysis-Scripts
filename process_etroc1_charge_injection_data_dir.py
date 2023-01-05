from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
from datetime import datetime
import pandas
import sqlite3
from process_etroc1_single_charge_injection_run import script_main as process_single_run
from cut_etroc1_single_run import script_main as cut_single_run

import plotly.express as px

def process_etroc1_data_directory_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    run_files: list[str],
    keep_only_triggers: bool,
    make_plots:bool = False
):
    with AdaLovelace.handle_task("process_etroc1_data_directory", drop_old_data=True) as Turing:
        for file in run_files:
            path = Path(file)
            file_base_name = str(path.name)[:-12]
            file_directory = AdaLovelace.path_directory/"Individual_Runs"/file_base_name

            # Convert from RAW data format to our format
            process_single_run(path, file_directory, keep_only_triggers, add_extra_data=False, drop_old_data=True, make_plots=make_plots)

            # Build a basic cuts.csv file
            with (file_directory/"cuts.csv").open("w") as cuts_file:
                cuts_file.write("board_id,variable,cut_type,cut_value,output\n")
                cuts_file.write("*,calibration_code,<,200")

            # Apply cuts
            cut_single_run(file_directory, drop_old_data=True, make_plots=make_plots)

def merge_etroc1_runs_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    board0_default:int = 535,
    board1_default:int = 720,
    board3_default:int = 720,
    filter_default:bool = True,
    trigger_board:int = 0,
):
    if AdaLovelace.task_completed("process_etroc1_data_directory"):
        with AdaLovelace.handle_task("merge_etroc1_runs", drop_old_data=True) as Bento:
            with sqlite3.connect(Bento.task_path/'data.sqlite') as sqlite3_connection:
                run_paths = [x for x in (AdaLovelace.path_directory/"Individual_Runs").iterdir() if x.is_dir()]
                for run_path in run_paths:
                    with RM.RunManager(run_path) as Goku:
                        if Goku.task_completed("apply_event_cuts"):
                            sqlite_file = Goku.path_directory/"data"/"data.sqlite"
                            with sqlite3.connect(sqlite_file) as sqlite3_connection_run:
                                df = pandas.read_sql('SELECT * FROM etroc1_data', sqlite3_connection_run, index_col=None)

                                from cut_etroc1_single_run import apply_event_filter
                                filter_df = pandas.read_feather(Goku.path_directory/"event_filter.fd")
                                filter_df.set_index("event", inplace=True)

                                df = apply_event_filter(df, filter_df)

                                df.drop(df.index[df['accepted'] == False], inplace=True)  # Drop the False, i.e. keep the True
                                df.reset_index(drop=True, inplace=True)

                                sqlite_file = Goku.path_directory/"data-filtered"/"data.sqlite"
                                (Goku.path_directory/"data-filtered").mkdir(exist_ok=True)
                                with sqlite3.connect(sqlite_file) as sqlite3_out_connection:
                                    df.to_sql('etroc1_data',
                                            sqlite3_out_connection,
                                            index=False,
                                            if_exists='replace')
                                del df
                                del filter_df
                        elif Goku.task_ran_successfully("proccess_etroc1_data_run"):
                            sqlite_file = Goku.path_directory/"data"/"data.sqlite"
                        else:
                            sqlite_file = None
                            script_logger.error("There is no data to process for run {}".format(Goku.run_name))

                        if sqlite_file is not None:
                            info = str(Goku.path_directory.name).split('_')  # For retrieving metadata about the run later
                            board0_threshold = int(info[5][3:])
                            board1_threshold = int(info[8][3:])
                            board3_threshold = int(info[11][3:])

                            with sqlite3.connect(sqlite_file) as sqlite3_connection_run:
                                run_df = pandas.read_sql('SELECT data_board_id, COUNT(*) AS hits FROM etroc1_data GROUP BY data_board_id', sqlite3_connection_run, index_col=None)

                                if filter_default:
                                    if len(run_df) == 0:  # Figure out which board is different from default and add a 0 hit entry
                                        if board0_threshold != board0_default:  # It is board 0
                                            run_df.loc[len(run_df.index)] = [0, 0]
                                        elif board1_threshold != board1_default:  # It is board 1
                                            run_df.loc[len(run_df.index)] = [1, 0]
                                        elif board3_threshold != board3_default:  # It is board 3
                                            run_df.loc[len(run_df.index)] = [3, 0]
                                        else:  # WTF is going on?
                                            script_logger.error("Something weird happened, there is an individual run with no threshold different from default and no data. Make sure the data taking parameters made sense. This is only possible if the threshold of the trigger board was set too high.")
                                    elif len(run_df) > 1:  # There is data from the board of interest and others (probably trigger board and others with badly set threshold)
                                        run_df.drop(run_df.index[run_df['data_board_id'] == trigger_board], inplace=True)
                                        run_df.reset_index(drop=True, inplace=True)
                                        if len(run_df) > 1:
                                            script_logger.error("After removing the extra trigger board, there is still multiple boards in a single run. This is not yet correctly handled. Please consider the data plots with care or fix the code to handle this correctly")
                                    else:  # There is data from only 1 board, but this may be from the trigger board and not the board of interest
                                        if board0_threshold != board0_default:  # Data should be from board 0
                                            if run_df["data_board_id"][0] != 0:  # if not from this board
                                                run_df.drop(run_df.index[run_df['data_board_id'] == trigger_board], inplace=True)
                                                run_df.reset_index(drop=True, inplace=True)
                                                run_df.loc[len(run_df.index)] = [0, 0]
                                        elif board1_threshold != board1_default:  # Data should be from board 1
                                            if run_df["data_board_id"][0] != 1:  # if not from this board
                                                run_df.drop(run_df.index[run_df['data_board_id'] == trigger_board], inplace=True)
                                                run_df.reset_index(drop=True, inplace=True)
                                                run_df.loc[len(run_df.index)] = [1, 0]
                                        elif board3_threshold != board3_default:  # Data should be from board 3
                                            if run_df["data_board_id"][0] != 3:  # if not from this board
                                                run_df.drop(run_df.index[run_df['data_board_id'] == trigger_board], inplace=True)
                                                run_df.reset_index(drop=True, inplace=True)
                                                run_df.loc[len(run_df.index)] = [3, 0]
                                        else:  # This is the data for the trigger board when it equals the default, keep it
                                            if run_df["data_board_id"][0] != trigger_board:
                                                script_logger.error("There is a problem... expecting data from trigger board only, but there was data from another board. Probably the default thresholds are improperly configured")

                                run_df["hits"] = run_df["hits"].astype("int64")
                                run_df["data_board_id"] = run_df["data_board_id"].astype("int8")

                                run_df["phase_adjust"] = info[2][8:]
                                run_df["phase_adjust"] = run_df["phase_adjust"].astype("int8")  # TODO: Check if type is ok

                                run_df["pixel_id"] = None
                                run_df["board_injected_charge"] = None
                                run_df["board_discriminator_threshold"] = None

                                for idx in range(len(run_df["data_board_id"])):
                                    board_id = run_df["data_board_id"][idx]

                                    if board_id is None:
                                        continue
                                    elif board_id == 3:  # Because the board numbering goes 0 - 1 - 3, but indexes are sequential
                                        board_idx = 2
                                    else:
                                        board_idx = board_id

                                    run_df.at[idx, 'pixel_id'] = info[3 + board_idx*3]
                                    run_df.at[idx, 'board_injected_charge'] = info[4 + board_idx*3][4:]
                                    run_df.at[idx, 'board_discriminator_threshold'] = info[5 + board_idx*3][3:]

                                run_df["board_injected_charge"] = run_df["board_injected_charge"].astype("int16")  # TODO: Check if type is ok
                                run_df["board_discriminator_threshold"] = run_df["board_discriminator_threshold"].astype("int16")  # TODO: Check if type is ok

                                run_df = run_df.dropna()

                                script_logger.info('Saving run {} summary data into database...'.format(Goku.run_name))
                                run_df.to_sql('combined_etroc1_data',
                                              sqlite3_connection,
                                              index=False,
                                              if_exists='append')

def plot_etroc1_combined_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    extra_title:str = "",
):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    if AdaLovelace.task_completed("merge_etroc1_runs"):
        with AdaLovelace.handle_task("plot_etroc1_combined", drop_old_data=True) as VanGogh:
            sqlite_file = VanGogh.get_task_path("merge_etroc1_runs")/'data.sqlite'
            with sqlite3.connect(sqlite_file) as sqlite3_connection:
                combined_df = pandas.read_sql('SELECT * FROM combined_etroc1_data', sqlite3_connection, index_col=None)
                combined_df = combined_df.sort_values(by=['data_board_id', 'board_discriminator_threshold'])

                fig = px.line(
                    data_frame = combined_df,
                    x = 'board_discriminator_threshold',
                    y = 'hits',
                    labels = {
                        "board_discriminator_threshold": "Discriminator Threshold [DAC Counts]",
                        "hits": "Hits",
                        "data_board_id": "Board ID",
                        "board_injected_charge": "Injected Charge [fC]"
                    },
                    color="data_board_id",
                    line_dash="board_injected_charge",
                    #line_group="board_injected_charge",
                    title = "Discriminator Threshold DAC Scan<br><sup>Run: {}{}</sup>".format(AdaLovelace.run_name, extra_title),
                    symbol = "board_injected_charge",
                )
                fig.write_html(
                    VanGogh.task_path/"DAC_Scan.html",
                    full_html = False, # For saving a html containing only a div with the plot
                    include_plotlyjs='cdn',
                )

                combined_df["data_board_id_cat"] = combined_df["data_board_id"].astype(str)
                fig = px.scatter_matrix(
                    combined_df,
                    dimensions=["board_discriminator_threshold", "hits", "phase_adjust", "board_injected_charge"],
                    labels = {
                        "board_discriminator_threshold": "Discriminator Threshold [DAC Counts]",
                        "hits": "Hits",
                        "data_board_id_cat": "Board ID",
                        "phase_adjust": "Phase Adjust [?]",
                        "board_injected_charge": "Injected Charge [fC]"
                    },
                    title = "Variable Correlations<br><sup>Run: {}{}</sup>".format(AdaLovelace.run_name, extra_title),
                    color="data_board_id_cat",
                    symbol = "board_injected_charge",
                )
                fig.update_traces(
                    diagonal_visible=False,
                    showupperhalf=False
                )

                fig.write_html(
                    VanGogh.task_path/'multi_scatter.html',
                    full_html = False, # For saving a html containing only a div with the plot
                    include_plotlyjs = 'cdn',
                )

def script_main(
        input_directory:Path,
        output_directory:Path,
        keep_only_triggers:bool,
        make_plots:bool,
        add_date:bool = True,
        board0_default:int = 535,
        board1_default:int = 720,
        board3_default:int = 720,
        filter_default:bool = True,
        trigger_board:int = 0,
        ):
    script_logger = logging.getLogger('process_dir')

    if not input_directory.is_dir():
        script_logger.info("The input directory should be an existing directory")
        return

    if add_date:
        now = datetime.now() # current date and time

        out_dir = output_directory.resolve()
        out_dir = out_dir.parent / (now.strftime("%Y%m%d-") + out_dir.name)
    else:
        out_dir = output_directory.resolve()

    with RM.RunManager(out_dir) as Guilherme:
        Guilherme.create_run(raise_error=True)

        run_files = [x for x in input_directory.iterdir() if x.is_file() and str(x)[-11:-4] == "Split_0"]

        process_etroc1_data_directory_task(
            Guilherme,
            script_logger=script_logger,
            run_files=run_files,
            keep_only_triggers=keep_only_triggers,
            make_plots=make_plots,
        )

        merge_etroc1_runs_task(
            Guilherme,
            script_logger = script_logger,
            board0_default = board0_default,
            board1_default = board1_default,
            board3_default = board3_default,
            filter_default = filter_default,
            trigger_board = trigger_board,
        )

        plot_etroc1_combined_task(
            Guilherme,
            script_logger=script_logger,
        )

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts all individual data file from a directory of data taken with the KC 705 FPGA development board connected to an ETROC1 into our data format')
    parser.add_argument(
        '-d',
        '--directory',
        metavar = 'path',
        help = "Path to the directory with the measurements' data.",
        required = True,
        dest = 'directory',
        type = str,
    )
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
        '--keep-all',
        help = "If set, all lines from the raw data file will be kept in the output, if not, only those corresponding to triggers will be kept",
        action = 'store_true',
        dest = 'keep_all',
    )
    parser.add_argument(
        '-p',
        '--make_plots',
        help = "If set, the plots for the individual runs will be created",
        action = 'store_true',
        dest = 'make_plots',
    )
    parser.add_argument(
        '-i',
        '--inhibit_date',
        help = "If set, the date will not be added to the start of the directory name",
        action = 'store_true',
        dest = 'inhibit_date',
    )
    parser.add_argument(
        '-b0',
        '--board0_default',
        metavar = 'int',
        help = "The default value of the DAC threshold for board with ID 0. Default: 535",
        default = "535",
        dest = 'board0_default',
        type = int,
    )
    parser.add_argument(
        '-b1',
        '--board1_default',
        metavar = 'int',
        help = "The default value of the DAC threshold for board with ID 1. Default: 720",
        default = "720",
        dest = 'board1_default',
        type = int,
    )
    parser.add_argument(
        '-b3',
        '--board3_default',
        metavar = 'int',
        help = "The default value of the DAC threshold for board with ID 3. Default: 720",
        default = "720",
        dest = 'board3_default',
        type = int,
    )
    parser.add_argument(
        '-f',
        '--filter_default',
        help = "If set, the default values for the different boards will be used to filter out the data which is not being scanned",
        action = 'store_true',
        dest = 'filter_default',
    )
    parser.add_argument(
        '-t',
        '--trigger_board',
        help = 'The ID of the board being used to trigger. Default: 0',
        choices = [0, 1, 3],
        default = 0,
        dest = 'trigger_board',
        type = int,
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
        Path(args.directory),
        Path(args.out_directory),
        keep_only_triggers = not args.keep_all,
        make_plots = args.make_plots,
        add_date = not args.inhibit_date,
        board0_default = args.board0_default,
        board1_default = args.board1_default,
        board3_default = args.board3_default,
        filter_default = args.filter_default,
        trigger_board = args.trigger_board,
    )