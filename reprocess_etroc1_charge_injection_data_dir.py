from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
from datetime import datetime
from process_etroc1_charge_injection_data_dir import apply_cuts_to_single_runs_subtask
from process_etroc1_charge_injection_data_dir import merge_etroc1_runs_task
from process_etroc1_charge_injection_data_dir import plot_etroc1_combined_task

def reprocess_etroc1_data_directory_task(
    AdaLovelace: RM.RunManager,
    script_logger: logging.Logger,
    run_dirs: list[Path],
    make_plots:bool = False,
):
    if AdaLovelace.task_completed("process_etroc1_data_directory"):
        with AdaLovelace.handle_task("reprocess_etroc1_data_directory", drop_old_data=True) as Turing:
            run_dirs = []

            apply_cuts_to_single_runs_subtask(
                Turing,
                script_logger=script_logger,
                run_dirs=run_dirs,
                make_plots=make_plots,
            )

def script_main(
        output_directory:Path,
        make_plots:bool,
        board0_default:int = 535,
        board1_default:int = 720,
        board3_default:int = 720,
        filter_default:bool = True,
        trigger_board:int = 0,
        ):
    script_logger = logging.getLogger('reprocess_dir')

    if not output_directory.is_dir():
        script_logger.info("The output directory should be an existing directory")
        return

    out_dir = output_directory.resolve()

    with RM.RunManager(out_dir) as Guilherme:
        Guilherme.create_run(raise_error=False)

        #run_files = [x for x in input_directory.iterdir() if x.is_file() and str(x)[-11:-4] == "Split_0"]
        run_dirs = [x for x in (out_dir/"Individual_Runs").iterdir() if x.is_dir()]

        reprocess_etroc1_data_directory_task(
            Guilherme,
            script_logger=script_logger,
            run_dirs=run_dirs,
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
        '-o',
        '--out-directory',
        metavar = 'path',
        help = 'Path to the output directory for the run data. Default: ./out',
        default = "./out",
        dest = 'out_directory',
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
        '-p',
        '--make_plots',
        help = "If set, the plots for the individual runs will be created",
        action = 'store_true',
        dest = 'make_plots',
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
        Path(args.out_directory),
        make_plots = args.make_plots,
        board0_default = args.board0_default,
        board1_default = args.board1_default,
        board3_default = args.board3_default,
        filter_default = args.filter_default,
        trigger_board = args.trigger_board,
    )