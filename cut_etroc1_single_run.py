from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task

def script_main(
        output_directory:Path,
        drop_old_data:bool=False,
        make_plots:bool=True
        ):

    script_logger = logging.getLogger('cut_run')

    with RM.RunManager(output_directory.resolve()) as Bob:
        Bob.create_run(raise_error=False)

        if Bob.task_completed("proccess_etroc1_data_run") or Bob.task_completed("proccess_etroc1_data_run_txt"):
            with Bob.handle_task("apply_cuts", drop_old_data=drop_old_data) as Miso:
                if not (Bob.path_directory/"cuts.csv").is_file():
                    script_logger.info("A cuts file is not defined for run {}".format(Bob.run_name))
                    shutil.copy(Bob.path_directory/"data"/'data.sqlite', Miso.task_path/"data.sqlite")
                else:
                    with sqlite3.connect(Bob.path_directory/"data"/'data.sqlite') as input_sqlite3_connection, \
                         sqlite3.connect(Miso.task_path/"data.sqlite") as output_sqlite3_connection, \
                         (Bob.path_directory/"cuts.csv").open("r") as cut_file:
                        input_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

                        for cut_line in cut_file.readlines():
                            cut_info = cut_line.split(",")
                            cut_info = [val.strip() for val in cut_info]

                            if cut_info[1] == "<":
                                input_df = input_df.loc[input_df[cut_info[0]] < int(cut_info[2])]
                            elif cut_info[1] == "<=":
                                input_df = input_df.loc[input_df[cut_info[0]] <= int(cut_info[2])]
                            elif cut_info[1] == ">":
                                input_df = input_df.loc[input_df[cut_info[0]] > int(cut_info[2])]
                            elif cut_info[1] == ">=":
                                input_df = input_df.loc[input_df[cut_info[0]] >= int(cut_info[2])]
                            elif cut_info[1] == "=":
                                input_df = input_df.loc[input_df[cut_info[0]] == int(cut_info[2])]
                            elif cut_info[1] == "<>":
                                input_df = input_df.loc[input_df[cut_info[0]] != int(cut_info[2])]
                            elif cut_info[1] == "str=":
                                input_df = input_df.loc[input_df[cut_info[0]] == cut_info[2]]
                            else:
                                script_logger.error("unknown cut in cuts file: {}".format(cut_line))

                        script_logger.info('Saving run metadata into database...')
                        input_df.to_sql('etroc1_data',
                                  output_sqlite3_connection,
                                  #index=False,
                                  if_exists='replace')

        if Bob.task_completed("apply_cuts") and make_plots:
            plot_etroc1_task(Bob, "plot_after_cuts", Bob.get_task_path("apply_cuts")/"data.sqlite")



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with the KC 705 FPGA development board connected to an ETROC1 into our data format')
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
