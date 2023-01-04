from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

from plot_etroc1_single_run import plot_etroc1_task

def proccess_etroc1_txt_run_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    input_file: Path,
    keep_only_triggers: bool,
    ignore_rows:int=1,
    add_extra_data:bool=True,
    drop_old_data:bool=False,
):
    with AdaLovelace.handle_task("proccess_etroc1_data_run_txt", drop_old_data=drop_old_data) as Miso:
        # Copied data location
        backup_data_dir = Miso.task_path.resolve()/'original_data'
        backup_data_dir.mkdir()

        # Copy and save original data
        script_logger.info("Copying original data to backup location")
        shutil.copy(input_file, backup_data_dir)

        # Create data directory
        data_dir = Miso.path_directory/"data"
        data_dir.mkdir()

        info = str(input_file.name).split('_')

        with sqlite3.connect(data_dir/'data.sqlite') as sqlite3_connection:
            df = pandas.read_csv(
                input_file,
                header=None,
                delim_whitespace=True,
                skiprows=ignore_rows,
                names=[
                    "data_board_id",
                    "time_of_arrival",
                    "time_over_threshold",
                    "calibration_code",
                    "hit_flag",
                    "day",
                    "time",
                ]
            )

            if keep_only_triggers:
                df = df.loc[df["hit_flag"] == 1]

            # Adjust types and sizes
            df["data_board_id"] = df["data_board_id"].astype("int8")
            df["time_of_arrival"] = df["time_of_arrival"].astype("int16")
            df["time_over_threshold"] = df["time_over_threshold"].astype("int16")
            df["calibration_code"] = df["calibration_code"].astype("int16")
            df["hit_flag"] = df["hit_flag"].astype("bool")

            # Combine day and time into datetime
            df["datetime"] = pandas.to_datetime(
                df['day'].astype(str) + " " + df["time"],
                format='%Y-%m-%d %H:%M:%S',
            )
            df.drop('day', axis=1, inplace=True)
            df.drop('time', axis=1, inplace=True)

            # print(df)
            # print(df.dtypes)

            if add_extra_data:  # For now only add pixel names
                df["pixel_id"] = None
                for idx in range(len(df["data_board_id"])):
                    board_id = df["data_board_id"][idx]

                    if board_id is None:
                        continue
                    elif board_id == 3:  # Because the board numbering goes 0 - 1 - 3, but indexes are sequential
                        board_idx = 2
                    else:
                        board_idx = board_id

                    # df["pixel_id"][idx] = info[board_idx]
                    df.at[idx, 'pixel_id'] = info[board_idx]

            script_logger.info('Saving run metadata into database...')
            df.to_sql('etroc1_data',
                      sqlite3_connection,
                      index=False,
                      if_exists='replace')

def script_main(
        input_file:Path,
        output_directory:Path,
        keep_only_triggers:bool,
        ignore_rows:int=1,
        add_extra_data:bool=True,
        drop_old_data:bool=False,
        make_plots:bool=True
        ):

    script_logger = logging.getLogger('process_run')

    if not input_file.is_file():
        script_logger.info("The input file should be an existing file")
        return

    if not isinstance(ignore_rows, int):
        raise TypeError("The number of rows to ignore from the txt file should be an integer")

    if ignore_rows < 0:
        raise RuntimeError("The number of rows to ignore should be greater or equal to 0")

    with RM.RunManager(output_directory.resolve()) as Bob:
        Bob.create_run(raise_error=False)

        proccess_etroc1_txt_run_task(
            Bob,
            script_logger=script_logger,
            input_file=input_file,
            keep_only_triggers=keep_only_triggers,
            ignore_rows=ignore_rows,
            add_extra_data=add_extra_data,
            drop_old_data=drop_old_data,
        )

        if Bob.task_completed("proccess_etroc1_data_run_txt") and make_plots:
            plot_etroc1_task(Bob, "plot_before_cuts", Bob.path_directory/"data"/"data.sqlite")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with the KC 705 FPGA development board connected to an ETROC1 into our analysis data format')
    parser.add_argument(
        '--file',
        metavar = 'path',
        help = 'Path to the txt file with the measurements.',
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
