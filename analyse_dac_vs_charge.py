from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import numpy
import sqlite3

import plotly.express as px

def calculate_dac_points_task(
    Oberon: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    noise_with_charge: int=20,
    rolling_mean:int=3,
    edge_detect_difference_from_mean:int=50,
    edge_detect_hit_center:int=4450,
    edge_detect_hit_window:int=150,
    noise_edge_offset:int=4,
    trigger_board:int=None,
    trigger_board_edge_detect_hit_center:int=None,
    trigger_board_edge_detect_hit_window:int=None,
    ):
    if Oberon.task_completed("merge_etroc1_runs"):
        with Oberon.handle_task("calculate_dac_points", drop_old_data=drop_old_data) as Artemis:
            with sqlite3.connect(Artemis.get_task_path("merge_etroc1_runs")/'data.sqlite') as input_sqlite3_connection, \
                 sqlite3.connect(Artemis.task_path/'data.sqlite') as output_sqlite3_connection:
                data_df = pandas.read_sql('SELECT * FROM combined_etroc1_data', input_sqlite3_connection, index_col=None)
                board_list = sorted(data_df['data_board_id'].unique())

                hit_df = data_df.query("hits>0")
                grouped_hit_df = hit_df.groupby(["data_board_id", "board_injected_charge"])
                min_df = grouped_hit_df[['board_discriminator_threshold']].min()
                max_df = grouped_hit_df[['board_discriminator_threshold']].max()

                noise_limit_df = min_df.groupby(["data_board_id"]).mean()
                noise_limit_df.rename(columns = {'board_discriminator_threshold':'noise_min_dac'}, inplace = True)
                if noise_with_charge is not None and noise_with_charge in hit_df["board_injected_charge"].unique():
                    for board_id in noise_limit_df.index:
                        noise_limit_df.at[board_id, 'noise_min_dac'] = min_df.at[(board_id, noise_with_charge), 'board_discriminator_threshold']

                # Sort data so it is possible to find min/max DAC for each board and injected charge
                sorted_data_df = data_df.sort_values(["data_board_id", "board_injected_charge", "board_discriminator_threshold"]).reset_index(drop=True)
                # Then group by board and injected charge so that further operations apply to the respective group
                grouped_sorted_data_df = sorted_data_df.groupby(["data_board_id", "board_injected_charge"])

                print_board = 0
                print_charge = 20
                print_group = (print_board, print_charge)

                #print(grouped_sorted_data_df.get_group(print_group))
                # Calculate rolling mean of the hits over 3 rows
                sorted_data_df["hit_mean"] = grouped_sorted_data_df.rolling(rolling_mean, center=True)[["hits"]].mean().reset_index().set_index("level_2")["hits"]
                # Calculate difference of hits to the previously computed mean
                sorted_data_df["hit_mean_diff"] = sorted_data_df["hit_mean"] - sorted_data_df["hits"]
                # Calculate difference between sequential rows
                sorted_data_df["hit_diff"] = grouped_sorted_data_df['hits'].diff()
                #print(grouped_sorted_data_df.get_group(print_group))
                #print(grouped_sorted_data_df.get_group(print_group)[["board_discriminator_threshold", "hits", "hit_diff", "hit_mean", "hit_mean_diff", "noise_limit"]].to_string())

                # Calculate the centers for each board, for the edge detect algorithm
                center_df = pandas.DataFrame()
                center_df.index = board_list
                center_df.index.name='data_board_id'
                center_df["center"] = edge_detect_hit_center

                # Calculate the windows for each board, for the edge detect algorithm
                window_df = pandas.DataFrame()
                window_df.index = board_list
                window_df.index.name='data_board_id'
                window_df["window"] = edge_detect_hit_window

                if trigger_board is not None and trigger_board in board_list:
                    if trigger_board_edge_detect_hit_center is not None:
                        center_df.at[trigger_board, "center"] = trigger_board_edge_detect_hit_center

                    if trigger_board_edge_detect_hit_window is not None:
                        window_df.at[trigger_board, "window"] = trigger_board_edge_detect_hit_window


                sorted_data_df.set_index('data_board_id', inplace=True)
                sorted_data_df['center'] = center_df
                sorted_data_df['window'] = window_df
                sorted_data_df.reset_index(inplace=True)

                # Run the edge detect algorithm and keep only data which triggers the edge detection
                sorted_data_df["edge_detect"] = (sorted_data_df["hit_mean_diff"].abs() < edge_detect_difference_from_mean) * ((sorted_data_df['hits'] - sorted_data_df['center']).abs() < sorted_data_df['window'])
                filtered_edge_df = sorted_data_df.loc[sorted_data_df["edge_detect"]]
                grouped_filtered_edge_df = filtered_edge_df.groupby(["data_board_id", "board_injected_charge"])
                #print(grouped_filtered_edge_df.get_group(print_group))
                noise_max_df = grouped_filtered_edge_df[["board_discriminator_threshold"]].min()
                #print(noise_max_df)

                noise_limit_df['noise_max_dac'] = noise_max_df.groupby(["data_board_id"]).median()
                if noise_with_charge is not None:
                    for board_id in noise_limit_df.index:
                        if (board_id, noise_with_charge) in noise_max_df.index:
                            noise_limit_df.at[board_id, 'noise_max_dac'] = noise_max_df.at[(board_id, noise_with_charge), "board_discriminator_threshold"]
                #print(noise_limit_df)

                noise_limit_df['noise_max_dac'] = noise_limit_df['noise_max_dac'] - noise_edge_offset

                max_df.to_sql('dac_charge_data',
                              output_sqlite3_connection,
                              #index=False,
                              if_exists='replace')

                noise_limit_df.to_sql('noise_limit_data',
                                      output_sqlite3_connection,
                                      #index=False,
                                      if_exists='replace')

def plot_dac_vs_charge_task(
    Oberon: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    extra_title: str = "",
    ):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    from math import ceil
    from math import floor

    if Oberon.task_completed("calculate_dac_points"):
        with Oberon.handle_task("plot_dac_vs_charge", drop_old_data=drop_old_data) as Matisse:
            with sqlite3.connect(Matisse.get_task_path("calculate_dac_points")/'data.sqlite') as sqlite3_connection:
                data_df = pandas.read_sql('SELECT * FROM dac_charge_data', sqlite3_connection, index_col=None)
                noise_edges_df = pandas.read_sql('SELECT * FROM noise_limit_data', sqlite3_connection, index_col=None)
                noise_edges_df.set_index("data_board_id", inplace=True)

                for board_id in data_df["data_board_id"].unique():
                    board_data_df = data_df.loc[data_df["data_board_id"] == board_id]
                    noise_edges = noise_edges_df.loc[board_id]

                    board_data_df["board_injected_charge"] = board_data_df["board_injected_charge"].astype(float)

                    fig = px.scatter(
                        board_data_df,
                        x="board_injected_charge",
                        y="board_discriminator_threshold",
                        labels = {
                            "board_discriminator_threshold": "Discriminator Threshold [DAC Counts]",
                            "hits": "Hits",
                            "data_board_id": "Board ID",
                            "board_injected_charge": "Injected Charge [fC]",
                        },
                        title = "Discriminator Threshold vs Injected Charge<br><sup>Board {}; Run: {}{}</sup>".format(board_id, Matisse.run_name, extra_title),
                        trendline="ols",
                    )

                    model = px.get_trendline_results(fig)
                    alpha = model.iloc[0]["px_fit_results"].params[0]
                    beta = model.iloc[0]["px_fit_results"].params[1]
                    rsq = model.iloc[0]["px_fit_results"].rsquared

                    min_charge = (float(noise_edges["noise_max_dac"]) - alpha)/beta
                    extra_charge = min(5,floor(min_charge))

                    fig.data[0].name = 'measurements'
                    fig.data[0].showlegend = True
                    fig.data[1].name = fig.data[1].name  + 'fit: y = ' + str(round(alpha, 2)) + ' + ' + str(round(beta, 2)) + 'x'
                    fig.data[1].showlegend = True
                    fig.data[1].line.color = 'green'
                    fig.data[1].line.dash = 'dash'

                    # Add extra points to the fit so it extends to the noise region
                    fig.data[1].y = numpy.insert(fig.data[1].y, 0, noise_edges["noise_max_dac"], axis=0)
                    fig.data[1].x = numpy.insert(fig.data[1].x, 0, min_charge, axis=0)
                    fig.data[1].y = numpy.insert(fig.data[1].y, 0, extra_charge*beta + alpha, axis=0)
                    fig.data[1].x = numpy.insert(fig.data[1].x, 0, extra_charge, axis=0)

                    min_charge = ceil(min_charge)

                    fig.add_hrect(
                        y0=noise_edges["noise_min_dac"],
                        y1=noise_edges["noise_max_dac"],
                        line_width=0,
                        fillcolor="red",
                        opacity=0.2,
                        annotation_text="Noise: {}-{}".format(floor(noise_edges["noise_min_dac"]), ceil(noise_edges["noise_max_dac"])),
                        annotation_position="top right",
                        annotation_font_size=20,
                    )

                    threshold_str = ""
                    charges = [min_charge + i for i in range(3)]
                    for charge in charges:
                        threshold_str += "<br>{} fC: {}".format(charge, ceil(alpha + beta*charge))

                    fig.add_annotation(
                        text="Suggested thresholds:{}".format(threshold_str),
                        xref="paper",
                        yref="paper",
                        x=0.02,
                        y=0.8,
                        showarrow=False,
                        font=dict(
                            #family="Courier New, monospace",
                            size=16,
                            #color="#ffffff"
                        ),
                    )

                    fig.write_html(
                        Matisse.task_path/'Board{}_DAC_vs_Injected_Charge.html'.format(board_id),
                        full_html = False,
                        include_plotlyjs = 'cdn',
                    )

def script_main(
    output_directory:Path,
    noise_with_charge: int=20,
    rolling_mean:int=3,
    edge_detect_difference_from_mean:int=50,
    edge_detect_hit_center:int=4450,
    edge_detect_hit_window:int=150,
    noise_edge_offset:int=4,
    trigger_board:int=None,
    trigger_board_edge_detect_hit_center:int=None,
    trigger_board_edge_detect_hit_window:int=None,
    ):

    script_logger = logging.getLogger('dac_vs_charge')

    with RM.RunManager(output_directory.resolve()) as Oberon:
        Oberon.create_run(raise_error=False)

        if not Oberon.task_completed("merge_etroc1_runs"):
            raise RuntimeError("You can only run this script after mergeing the etroc1 charge injection runs")

        if noise_with_charge == 0:
            noise_with_charge = None
        calculate_dac_points_task(
            Oberon,
            script_logger=script_logger,
            noise_with_charge=noise_with_charge,
            rolling_mean=rolling_mean,
            edge_detect_difference_from_mean=edge_detect_difference_from_mean,
            edge_detect_hit_center=edge_detect_hit_center,
            edge_detect_hit_window=edge_detect_hit_window,
            noise_edge_offset=noise_edge_offset,
            trigger_board=trigger_board,
            trigger_board_edge_detect_hit_center=trigger_board_edge_detect_hit_center,
            trigger_board_edge_detect_hit_window=trigger_board_edge_detect_hit_window,
        )

        plot_dac_vs_charge_task(
            Oberon,
            script_logger=script_logger,
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
    parser.add_argument(
        '-n',
        '--noise_with_charge',
        metavar = 'int',
        help = 'Injected charge value used to calculate the minimum and maximum values of DAC corresponding to the noise region. Default: 0 (it will do the mean)',
        default = 0,
        dest = 'noise_with_charge',
        type = int,
    )
    parser.add_argument(
        '-r',
        '--rolling_mean',
        metavar = 'int',
        help = 'Window of rows over which to calculate the hit rolling mean for the edge detect algorithm. Default: 3',
        default = 3,
        dest = 'rolling_mean',
        type = int,
    )
    parser.add_argument(
        '-d',
        '--difference_from_mean',
        metavar = 'int',
        help = 'Maximum value the hits is allowed to be away from the mean for the edge detect algorithm. Default: 50',
        default = 50,
        dest = 'edge_detect_difference_from_mean',
        type = int,
    )
    parser.add_argument(
        '-c',
        '--hit_center',
        metavar = 'int',
        help = 'Center of the window on the hit value for the edge detect algorithm. Default: 4450',
        default = 4450,
        dest = 'edge_detect_hit_center',
        type = int,
    )
    parser.add_argument(
        '-w',
        '--hit_window',
        metavar = 'int',
        help = 'Half-size of the window on the hit value for the edge detect algorithm. Default: 150',
        default = 150,
        dest = 'edge_detect_hit_window',
        type = int,
    )
    parser.add_argument(
        '--trigger_board',
        metavar = 'int',
        help = 'The board used to trigger the system, only used if the edge_detect_hit_center_trigger_board is set. Default: no default value',
        dest = 'trigger_board',
        type = int,
    )
    parser.add_argument(
        '--trigger_board_hit_center',
        metavar = 'int',
        help = 'Center of the window on the hit value for the edge detect algorithm for the trigger board. Only used if the trigger board is defined. If this value is not defined, the hit_center is used instead. Default: no default value',
        dest = 'edge_detect_hit_center_trigger_board',
        type = int,
    )
    parser.add_argument(
        '--trigger_board_hit_window',
        metavar = 'int',
        help = 'Half-size of the window on the hit value for the edge detect algorithm for the trigger board. Only used if the trigger board is defined. If this value is not defined, the hit_window is used instead. Default: no default value',
        dest = 'edge_detect_hit_window_trigger_board',
        type = int,
    )
    parser.add_argument(
        '-f',
        '--noise_edge_offset',
        metavar = 'int',
        help = 'Offset to apply to the calculated noise edge after applying the edge detect algorithm. Default: 4',
        default = 4,
        dest = 'noise_edge_offset',
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

    trigger_board = None
    if hasattr(args, 'trigger_board'):
        trigger_board = args.trigger_board

    edge_detect_hit_center_trigger_board = None
    if hasattr(args, 'edge_detect_hit_center_trigger_board'):
        edge_detect_hit_center_trigger_board = args.edge_detect_hit_center_trigger_board

    edge_detect_hit_window_trigger_board = None
    if hasattr(args, 'edge_detect_hit_window_trigger_board'):
        edge_detect_hit_window_trigger_board = args.edge_detect_hit_window_trigger_board

    script_main(
        Path(args.out_directory),
        noise_with_charge=args.noise_with_charge,
        rolling_mean=args.rolling_mean,
        edge_detect_difference_from_mean=args.edge_detect_difference_from_mean,
        edge_detect_hit_center=args.edge_detect_hit_center,
        edge_detect_hit_window=args.edge_detect_hit_window,
        noise_edge_offset=args.noise_edge_offset,
        trigger_board=trigger_board,
        trigger_board_edge_detect_hit_center=edge_detect_hit_center_trigger_board,
        trigger_board_edge_detect_hit_window=edge_detect_hit_window_trigger_board,
    )