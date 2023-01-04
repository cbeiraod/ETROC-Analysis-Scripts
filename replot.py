from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task
from process_etroc1_data_dir import plot_etroc1_combined_task

def script_main(
        output_directory:Path,
        extra_title:str = ""
    ):

    script_logger = logging.getLogger('replotter')

    with RM.RunManager(output_directory.resolve()) as Geralt:
        Geralt.create_run(raise_error=False)

        if Geralt.task_completed("merge_etroc1_runs"):
            plot_etroc1_combined_task(Geralt, script_logger=script_logger, extra_title=extra_title)

        if Geralt.task_completed("proccess_etroc1_data_run") or Geralt.task_completed("proccess_etroc1_data_run_txt"):
            plot_etroc1_task(Geralt, "plot_before_cuts", Geralt.path_directory/"data"/"data.sqlite", extra_title=extra_title)
            if Geralt.task_completed("apply_cuts"):
                plot_etroc1_task(Geralt, "plot_after_cuts", Geralt.get_task_path("apply_cuts")/"data.sqlite", extra_title=extra_title)

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