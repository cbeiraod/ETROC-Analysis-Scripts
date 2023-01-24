from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task
from process_etroc1_charge_injection_data_dir import plot_etroc1_combined_task
from analyse_dac_vs_charge import plot_dac_vs_charge_task
from analyse_time_resolution import plot_times_in_ns_task

def script_main(
        output_directory:Path,
        extra_title:str = "",
        max_toa:float=0,
        max_tot:float=0,
    ):

    script_logger = logging.getLogger('replotter')

    if max_toa == 0:
        max_toa = None
    if max_tot == 0:
        max_tot = None

    with RM.RunManager(output_directory.resolve()) as Geralt:
        Geralt.create_run(raise_error=False)

        if Geralt.task_completed("merge_etroc1_runs"):
            plot_etroc1_combined_task(Geralt, script_logger=script_logger, extra_title=extra_title)

        if Geralt.task_completed("proccess_etroc1_data_run") or Geralt.task_completed("proccess_etroc1_data_run_txt"):
            plot_etroc1_task(Geralt, "plot_before_cuts", Geralt.path_directory/"data"/"data.sqlite", extra_title=extra_title)
            if Geralt.task_completed("apply_event_cuts"):
                plot_etroc1_task(Geralt, "plot_after_cuts", Geralt.path_directory/"data"/"data.sqlite", extra_title=extra_title, filter_files={"event": Geralt.path_directory/"event_filter.fd"})

        if Geralt.task_completed("calculate_dac_points"):
            plot_dac_vs_charge_task(Geralt, script_logger=script_logger, extra_title=extra_title)

        if Geralt.task_completed("calculate_times_in_ns"):
            plot_times_in_ns_task(
                Geralt,
                script_logger=script_logger,
                task_name="plot_times_in_ns_before_cuts",
                data_file=Geralt.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={},
                max_toa=max_toa,
                max_tot=max_tot,
                min_toa=0,
                min_tot=0,
                extra_title=extra_title,
            )
            plot_times_in_ns_task(
                Geralt,
                script_logger=script_logger,
                task_name="plot_times_in_ns_after_cuts",
                data_file=Geralt.get_task_path("calculate_times_in_ns")/'data.sqlite',
                filter_files={"event": Geralt.path_directory/"event_filter.fd"},
                max_toa=max_toa,
                max_tot=max_tot,
                min_toa=0,
                min_tot=0,
                extra_title=extra_title,
            )

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