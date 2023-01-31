from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import pandas
import numpy
import sqlite3
import plotly.graph_objects as go

from utilities import apply_event_filter, filter_dataframe

def plot_cal_code_task(
    Fermat: RM.RunManager,
    script_logger: logging.Logger,
    task_name:str,
    data_file:Path,
    filter_files:dict[str,Path] = {},
    drop_old_data:bool=True,
    extra_title: str = "",
    full_html:bool=False,
    ):

    with Fermat.handle_task(task_name, drop_old_data=drop_old_data) as Monet:
        base_path=Monet.task_path
        run_name=Monet.run_name,
        with sqlite3.connect(data_file) as input_sqlite3_connection:
            data_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

            data_df = filter_dataframe(
                df=data_df,
                filter_files=filter_files,
                script_logger=script_logger,
            )

            filter_df = pandas.read_feather(Monet.path_directory/"event_filter.fd")
            filter_df.set_index("event", inplace=True)

            from cut_etroc1_single_run import apply_event_filter
            data_df = apply_event_filter(data_df, filter_df)
            df = data_df.loc[data_df['accepted']==True]

            # Even - Even
            fig = go.Figure()
            for board_id in sorted(df["data_board_id"].unique()):
                condition = (df["data_board_id"] == board_id) & (df["time_of_arrival"]%2 == 0) & (df["calibration_code"]%2 == 0)
                fig.add_trace(go.Histogram(
                    x=df.loc[condition]["calibration_code"],
                    name='Board {}'.format(board_id), # name used in legend and hover labels
                    opacity=0.5,
                    bingroup=1,
                ))
            fig.update_layout(
                barmode='overlay',
                title_text="Histogram of Even Calibration Code when Even TOA Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
                xaxis_title_text='Calibration Code', # xaxis label
                yaxis_title_text='Count', # yaxis label
            )
            fig.update_yaxes(type="log")

            fig.write_html(
                base_path/'calibration_code_histogram_evenTOA_evenCAL.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            # Even - Odd
            fig = go.Figure()
            for board_id in sorted(df["data_board_id"].unique()):
                condition = (df["data_board_id"] == board_id) & (df["time_of_arrival"]%2 == 0) & (df["calibration_code"]%2 == 1)
                fig.add_trace(go.Histogram(
                    x=df.loc[condition]["calibration_code"],
                    name='Board {}'.format(board_id), # name used in legend and hover labels
                    opacity=0.5,
                    bingroup=1,
                ))
            fig.update_layout(
                barmode='overlay',
                title_text="Histogram of Even Calibration Code when Odd TOA Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
                xaxis_title_text='Calibration Code', # xaxis label
                yaxis_title_text='Count', # yaxis label
            )
            fig.update_yaxes(type="log")

            fig.write_html(
                base_path/'calibration_code_histogram_evenTOA_oddCAL.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            # Odd - Even
            fig = go.Figure()
            for board_id in sorted(df["data_board_id"].unique()):
                condition = (df["data_board_id"] == board_id) & (df["time_of_arrival"]%2 == 1) & (df["calibration_code"]%2 == 0)
                fig.add_trace(go.Histogram(
                    x=df.loc[condition]["calibration_code"],
                    name='Board {}'.format(board_id), # name used in legend and hover labels
                    opacity=0.5,
                    bingroup=1,
                ))
            fig.update_layout(
                barmode='overlay',
                title_text="Histogram of Even Calibration Code with Odd TOA Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
                xaxis_title_text='Calibration Code', # xaxis label
                yaxis_title_text='Count', # yaxis label
            )
            fig.update_yaxes(type="log")

            fig.write_html(
                base_path/'calibration_code_histogram_oddTOA_oddCAL.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            # Odd - Odd
            fig = go.Figure()
            for board_id in sorted(df["data_board_id"].unique()):
                condition = (df["data_board_id"] == board_id) & (df["time_of_arrival"]%2 == 1) & (df["calibration_code"]%2 == 1)
                fig.add_trace(go.Histogram(
                    x=df.loc[condition]["calibration_code"],
                    name='Board {}'.format(board_id), # name used in legend and hover labels
                    opacity=0.5,
                    bingroup=1,
                ))
            fig.update_layout(
                barmode='overlay',
                title_text="Histogram of Odd Calibration Code with Odd TOA Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
                xaxis_title_text='Calibration Code', # xaxis label
                yaxis_title_text='Count', # yaxis label
            )
            fig.update_yaxes(type="log")

            fig.write_html(
                base_path/'calibration_code_histogram_oddTOA_evenCAL.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )    

def script_main(
    output_directory:Path,
    make_plots:bool=True,
    max_toa:float=0,
    max_tot:float=0,
    ):

    script_logger = logging.getLogger('apply_event_cuts')

    if max_toa == 0:
        max_toa = None
    if max_tot == 0:
        max_tot = None

    with RM.RunManager(output_directory.resolve()) as Fermat:
        Fermat.create_run(raise_error=False)

        if not Fermat.task_completed("apply_event_cuts"):
            raise RuntimeError("You can only run this script after applying event cuts")

        plot_cal_code_task(
            Fermat,
            script_logger=script_logger,
            task_name="plot_calcode_EvenOddTOA_after_cuts",
            data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
            filter_files={"event": Fermat.path_directory/"event_filter.fd"},
        )

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