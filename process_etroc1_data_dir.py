from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
from datetime import datetime
import pandas
import sqlite3
from process_etroc1_single_run import script_main as process_single_run
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
            process_single_run(path, file_directory, keep_only_triggers, add_extra_data=False, drop_old_data=False, make_plots=make_plots)

            # Build a basic cuts.csv file
            with (file_directory/"cuts.csv").open("w") as cuts_file:
                cuts_file.write("calibration_code,<,200")

            # Apply cuts
            cut_single_run(file_directory, drop_old_data=False, make_plots=make_plots)

def merge_runs_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger
):
    if AdaLovelace.task_completed("process_etroc1_data_directory"):
        with AdaLovelace.handle_task("merge_runs", drop_old_data=True) as Bento:
            with sqlite3.connect(Bento.task_path/'data.sqlite') as sqlite3_connection:
                run_paths = [x for x in (AdaLovelace.path_directory/"Individual_Runs").iterdir() if x.is_dir()]
                for run_path in run_paths:
                    with RM.RunManager(run_path) as Goku:
                        if Goku.task_completed("apply_cuts"):
                            sqlite_file = Goku.get_task_path("apply_cuts")/"data.sqlite"
                        elif Goku.task_ran_successfully("proccess_etroc1_data_run"):
                            sqlite_file = Goku.path_directory/"data"/"data.sqlite"
                        else:
                            sqlite_file = None
                            script_logger.error("There is no data to process for run {}".format(Goku.run_name))

                        if sqlite_file is not None:
                            info = str(Goku.path_directory.name).split('_')  # For retrieving metadata about the run later

                            with sqlite3.connect(sqlite_file) as sqlite3_connection_run:
                                run_df = pandas.read_sql('SELECT data_board_id, COUNT(*) AS hits FROM etroc1_data GROUP BY data_board_id', sqlite3_connection_run, index_col=None)
                                # TODO: add a flag to count hits only on one board

                                if len(run_df) > 1:
                                    print(run_df)

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

                                    run_df["pixel_id"][idx] = info[3 + board_idx*3]
                                    run_df["board_injected_charge"][idx] = info[4 + board_idx*3][4:]
                                    run_df["board_discriminator_threshold"][idx] = info[5 + board_idx*3][3:]

                                run_df["board_injected_charge"] = run_df["board_injected_charge"].astype("int16")  # TODO: Check if type is ok
                                run_df["board_discriminator_threshold"] = run_df["board_discriminator_threshold"].astype("int16")  # TODO: Check if type is ok

                                run_df = run_df.dropna()

                                script_logger.info('Saving run {} summary data into database...'.format(Goku.run_name))
                                run_df.to_sql('combined_etroc1_data',
                                              sqlite3_connection,
                                              index=False,
                                              if_exists='append')

def plot_combined_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    extra_title:str = "",
):
    if AdaLovelace.task_completed("merge_runs"):
        with AdaLovelace.handle_task("plot_combined", drop_old_data=True) as VanGogh:
            sqlite_file = VanGogh.get_task_path("merge_runs")/'data.sqlite'
            with sqlite3.connect(sqlite_file) as sqlite3_connection:
                combined_df = pandas.read_sql('SELECT * FROM combined_etroc1_data', sqlite3_connection, index_col=None)
                combined_df = combined_df.sort_values(by=['board_discriminator_threshold', 'data_board_id'])

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

        process_etroc1_data_directory_task(Guilherme, script_logger=script_logger, run_files=run_files, keep_only_triggers=keep_only_triggers, make_plots=make_plots)

        merge_runs_task(Guilherme, script_logger=script_logger)

        plot_combined_task(Guilherme, script_logger=script_logger)




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
        help = 'Set the logging level',
        choices = ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"],
        default = "ERROR",
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
        help = 'Path to the output directory for the run data.',
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
        dest = 'inhibit_data',
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
        add_date = not args.inhibit_date
    )