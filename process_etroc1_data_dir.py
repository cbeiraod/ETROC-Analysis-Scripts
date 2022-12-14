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

def script_main(
        input_directory:Path,
        output_directory:Path,
        keep_only_triggers:bool,
        ):
    script_logger = logging.getLogger('process_dir')

    if not input_directory.is_dir():
        script_logger.info("The input directory should be an existing directory")
        return

    now = datetime.now() # current date and time

    out_dir = output_directory.resolve()
    out_dir = out_dir.parent / (now.strftime("%Y%m%d-") + out_dir.name)

    with RM.RunManager(out_dir) as Guilherme:
        Guilherme.create_run(raise_error=False)

        run_files = [x for x in input_directory.iterdir() if x.is_file() and str(x)[-11:-4] == "Split_0"]

        with Guilherme.handle_task("process_etroc_data_directory", drop_old_data=True) as Turing:
            for file in run_files:
                path = Path(file)
                file_base_name = str(path.name)[:-12]
                file_directory = Guilherme.path_directory/"Individual_Runs"/file_base_name

                # Convert from RAW data format to our format
                process_single_run(path, file_directory, keep_only_triggers, add_extra_data=False, drop_old_data=False)

                # Build a basic cuts.csv file
                with (file_directory/"cuts.csv").open("w") as cuts_file:
                    cuts_file.write("calibration_code,<,200")

                # Apply cuts
                cut_single_run(file_directory, drop_old_data=False)

        if Guilherme.task_completed("process_etroc_data_directory"):
            with Guilherme.handle_task("merge_runs", drop_old_data=False) as Bento:
                with sqlite3.connect(Bento.task_path/'data.sqlite') as sqlite3_connection:
                    for file in run_files:
                        path = Path(file)
                        file_base_name = str(path.name)[:-12]
                        file_directory = Guilherme.path_directory/"Individual_Runs"/file_base_name

                        with RM.RunManager(file_directory) as Goku:
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
                                    combined_df = pandas.read_sql('SELECT data_board_id, COUNT(*) AS hits FROM etroc1_data GROUP BY data_board_id', sqlite3_connection_run, index_col=None)
                                    # TODO: add a flag to count hits only on one board

                                    combined_df["phase_adjust"] = info[2][8:]
                                    combined_df["pixel_id"] = None
                                    combined_df["board_injected_charge"] = None
                                    combined_df["board_discriminator_threshold"] = None

                                    for idx in range(len(combined_df["data_board_id"])):
                                        board_id = combined_df["data_board_id"][idx]
                                        if board_id is None:
                                            continue

                                        #pixel_str = "pixel{}_id".format(board_id)
                                        #inj_charge_str = "board{}_injected_charge".format(board_id)
                                        #discr_str = "board{}_discriminator_threshold".format(board_id)

                                        if board_id == 3:
                                            board_id = 2
                                        combined_df["pixel_id"][idx] = info[3 + board_id*3]
                                        combined_df["board_injected_charge"][idx] = info[4 + board_id*3][4:]
                                        combined_df["board_discriminator_threshold"][idx] = info[5 + board_id*3][3:]

                                    combined_df = combined_df.dropna()

                                    #combined_df["pixel0_id"] = info[3]
                                    #combined_df["board0_injected_charge"] = info[4][4:]
                                    #combined_df["board0_discriminator_threshold"] = info[5][3:]
                                    #combined_df["pixel1_id"] = info[6]
                                    #combined_df["board1_injected_charge"] = info[7][4:]
                                    #combined_df["board1_discriminator_threshold"] = info[8][3:]
                                    #combined_df["pixel3_id"] = info[9]
                                    #combined_df["board3_injected_charge"] = info[10][4:]
                                    #combined_df["board3_discriminator_threshold"] = info[11][3:]

                                    script_logger.info('Saving run {} summary data into database...'.format(file_base_name))
                                    combined_df.to_sql('combined_etroc1_data',
                                              sqlite3_connection,
                                              #index=False,
                                              if_exists='append')

        if Guilherme.task_completed("merge_runs"):
            with Guilherme.handle_task("plot_combined", drop_old_data=False) as VanGogh:
                sqlite_file = VanGogh.get_task_path("merge_runs")/'data.sqlite'
                with sqlite3.connect(sqlite_file) as sqlite3_connection:
                    combined_df = pandas.read_sql('SELECT * FROM combined_etroc1_data', sqlite3_connection, index_col=None)
                    combined_df = combined_df.sort_values(by=['board_discriminator_threshold', 'data_board_id'])

                    fig = px.line(
            			data_frame = combined_df,
            			x = 'board_discriminator_threshold',
		            	y = 'hits',
                        color="data_board_id",
                        line_dash="board_injected_charge",
                        line_group="board_injected_charge",
        			    title = 'DAC Scan',
    		        	markers = '.',
	    	        )
                    fig.write_html(
                        VanGogh.task_path/"DAC_Scan.html",
                        full_html = False, # For saving a html containing only a div with the plot
                        include_plotlyjs='cdn',
                    )

                    fig = px.scatter_matrix(
                        combined_df,
                        dimensions=["board_discriminator_threshold", "hits", "phase_adjust", "board_injected_charge"],
                        color="data_board_id",
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

    script_main(Path(args.directory), Path(args.out_directory), not args.keep_all)