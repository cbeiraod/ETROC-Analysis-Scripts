from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task

def script_main(
        input_file:Path,
        output_directory:Path,
        keep_only_triggers:bool,
        add_extra_data:bool=True,
        drop_old_data:bool=False
        ):

    script_logger = logging.getLogger('process_run')

    if not input_file.is_file():
        script_logger.info("The input file should be an existing file")
        return

    with RM.RunManager(output_directory.resolve()) as Bob:
        Bob.create_run(raise_error=False)

        with Bob.handle_task("proccess_etroc1_data_run", drop_old_data=drop_old_data) as Miso:
            # Copied data location
            backup_data_dir = (Miso.task_path/'original_data').resolve()
            backup_data_dir.mkdir()

            # Copy and save original data
            script_logger.info("Copying original data to backup location")
            shutil.copy(input_file, backup_data_dir)

            # Create data directory
            data_dir = Bob.path_directory/"data"
            data_dir.mkdir()

            info = str(input_file.name).split('_')

            with sqlite3.connect(data_dir/'data.sqlite') as sqlite3_connection:
                df = pandas.read_csv(
                    input_file,
                    header=None,
                    delim_whitespace=True,
                    names=[
                        "data_board_id",
                        "time_of_arrival",
                        "time_over_threshold",
                        "calibration_code",
                        "hit_flag",
                    ]
                )

                # TODO open other splits of file

                if keep_only_triggers:
                    df = df.loc[df["hit_flag"] == 1]

                if add_extra_data:
                    df["phase_adjust"] = info[2][8:]
                    df["pixel0_id"] = info[3]
                    df["board0_injected_charge"] = info[4][4:]
                    df["board0_discriminator_threshold"] = info[5][3:]
                    df["pixel1_id"] = info[6]
                    df["board1_injected_charge"] = info[7][4:]
                    df["board1_discriminator_threshold"] = info[8][3:]
                    df["pixel3_id"] = info[9]
                    df["board3_injected_charge"] = info[10][4:]
                    df["board3_discriminator_threshold"] = info[11][3:]

                script_logger.info('Saving run metadata into database...')
                df.to_sql('etroc1_data',
                          sqlite3_connection,
                          #index=False,
                          if_exists='replace')

        if Bob.task_completed("proccess_etroc1_data_run"):
            plot_etroc1_task(Bob, "plot_before_cuts", Bob.path_directory/"data"/"data.sqlite")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with the KC 705 FPGA development board connected to an ETROC1 into our data format')
    parser.add_argument(
        '--file',
        metavar = 'path',
        help = 'Path to the file with the measurements.',
        required = True,
        dest = 'file',
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

    script_main(Path(args.file), Path(args.out_directory), not args.keep_all)
