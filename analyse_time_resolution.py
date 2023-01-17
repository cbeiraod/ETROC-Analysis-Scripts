from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import numpy
import sqlite3

import plotly.express as px
import plotly.graph_objects as go

from math import ceil
from math import floor

def calculate_times_in_ns_task(
    Fermat: RM.RunManager,
    script_logger: logging.Logger,
    drop_old_data:bool=True,
    ):
    if Fermat.task_completed("apply_event_cuts"):
        with Fermat.handle_task("calculate_times_in_ns", drop_old_data=drop_old_data) as Einstein:
            with sqlite3.connect(Einstein.path_directory/"data"/'data.sqlite') as input_sqlite3_connection, \
                 sqlite3.connect(Einstein.task_path/'data.sqlite') as output_sqlite3_connection:
                data_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

                filter_df = pandas.read_feather(Einstein.path_directory/"event_filter.fd")
                filter_df.set_index("event", inplace=True)

                from cut_etroc1_single_run import apply_event_filter
                data_df = apply_event_filter(data_df, filter_df)
                accepted_data_df = data_df.loc[data_df['accepted']==True]
                board_grouped_accepted_data_df = accepted_data_df.groupby(['data_board_id'])

                board_info_df = board_grouped_accepted_data_df[['calibration_code']].mean()
                board_info_df.rename(columns = {'calibration_code':'calibration_code_mean'}, inplace = True)
                board_info_df['calibration_code_median'] = board_grouped_accepted_data_df[['calibration_code']].median()
                board_info_df['fbin_mean'] = 3.125/board_info_df['calibration_code_mean']
                board_info_df['fbin_median'] = 3.125/board_info_df['calibration_code_median']

                #accepted_data_df.set_index("data_board_id", inplace=True)
                #accepted_data_df["fbin"] = board_info_df['fbin_mean']
                #accepted_data_df.reset_index(inplace=True)

                #accepted_data_df["time_of_arrival_ns"] = 12.5 - accepted_data_df['time_of_arrival']*accepted_data_df['fbin']
                #accepted_data_df["time_over_threshold_ns"] = (accepted_data_df["time_over_threshold"]*2 - (accepted_data_df["time_over_threshold"]/32.).apply(numpy.floor))*accepted_data_df['fbin']

                data_df.set_index("data_board_id", inplace=True)
                data_df["fbin"] = board_info_df['fbin_mean']
                data_df.reset_index(inplace=True)

                data_df["time_of_arrival_ns"] = 12.5 - data_df['time_of_arrival']*data_df['fbin']
                data_df["time_over_threshold_ns"] = (data_df["time_over_threshold"]*2 - (data_df["time_over_threshold"]/32.).apply(numpy.floor))*data_df['fbin']

                board_info_df.to_sql('board_info_data',
                                     output_sqlite3_connection,
                                     #index=False,
                                     if_exists='replace')

                data_df.drop(labels=['accepted', 'event_filter'], axis=1, inplace=True)

                data_df.to_sql('etroc1_data',
                               output_sqlite3_connection,
                               index=False,
                               if_exists='replace')

def make_toa_correlation(
    board_a:int,
    board_b:int,
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    extra_title: str = "",
    full_html:bool=False,
    range_x = None,
    range_y = None,
    ):
    min_x = data_df["time_of_arrival_ns_{}".format(board_a)].min()
    max_x = data_df["time_of_arrival_ns_{}".format(board_a)].max()
    min_y = data_df["time_of_arrival_ns_{}".format(board_b)].min()
    max_y = data_df["time_of_arrival_ns_{}".format(board_b)].max()

    if range_x is None:
        range_x = [min_x, max_x]
    else:
        range_x = [
            max(min_x, range_x[0]),
            min(max_x, range_x[1])
        ]
    if range_y is None:
        range_y = [min_y, max_y]
    else:
        range_y = [
            max(min_y, range_y[0]),
            min(max_y, range_y[1])
        ]

    nbinsx = ceil((range_x[1] - range_x[0]) * 40)
    nbinsy = ceil((range_y[1] - range_y[0]) * 40)

    fig = px.scatter(
        data_df,
        x="time_of_arrival_ns_{}".format(board_a),
        y="time_of_arrival_ns_{}".format(board_b),
        labels = {
            "time_of_arrival_ns_{}".format(board_a): "Board {} Time of Arrival [ns]".format(board_a),
            "time_of_arrival_ns_{}".format(board_b): "Board {} Time of Arrival [ns]".format(board_b),
        },
        title = "Time of Arrival correlation between board {} and board {}<br><sup>Run: {}{}</sup>".format(board_a, board_b, run_name, extra_title),
        opacity = 0.1,
        trendline="ols",
        range_x=range_x,
        range_y=range_y,
    )

    model = px.get_trendline_results(fig)
    alpha = model.iloc[0]["px_fit_results"].params[0]
    beta = model.iloc[0]["px_fit_results"].params[1]
    rsq = model.iloc[0]["px_fit_results"].rsquared

    fig.data[0].name = 'data'
    fig.data[0].showlegend = True
    fig.data[1].name = fig.data[1].name  + 'fit: y = ' + str(round(alpha, 2)) + ' + ' + str(round(beta, 2)) + 'x'
    fig.data[1].showlegend = True
    fig.data[1].line.color = 'red'
    #fig.data[1].line.dash = 'dash'
    trendline = fig.data[1]

    fig.write_html(
        base_path/'toa_board{}_vs_board{}_scatter.html'.format(board_a, board_b),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = px.density_heatmap(
        data_df,
        x="time_of_arrival_ns_{}".format(board_a),
        y="time_of_arrival_ns_{}".format(board_b),
        labels = {
            "time_of_arrival_ns_{}".format(board_a): "Board {} Time of Arrival [ns]".format(board_a),
            "time_of_arrival_ns_{}".format(board_b): "Board {} Time of Arrival [ns]".format(board_b),
        },
        color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
        title = "Time of Arrival correlation between board {} and board {}<br><sup>Run: {}{}</sup>".format(board_a, board_b, run_name, extra_title),
        range_x=range_x,
        range_y=range_y,
        nbinsx=nbinsx,
        nbinsy=nbinsy,
    )

    #trendline.showlegend = False
    fig.add_trace(trendline)

    fig.write_html(
        base_path/'toa_board{}_vs_board{}.html'.format(board_a, board_b),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

2+2

def plot_times_in_ns_task(
    Fermat: RM.RunManager,
    script_logger: logging.Logger,
    task_name:str,
    data_file:Path,
    filter_files:dict[Path] = {},
    drop_old_data:bool=True,
    extra_title: str = "",
    full_html:bool=False,
    max_toa:float=20,
    max_tot:float=20,
    min_toa:float=-20,
    min_tot:float=-20,
    ):
    with Fermat.handle_task(task_name, drop_old_data=drop_old_data) as Monet:
        with sqlite3.connect(data_file) as input_sqlite3_connection:
            data_df = pandas.read_sql('SELECT * FROM etroc1_data', input_sqlite3_connection, index_col=None)

            for filter in filter_files:
                if filter_files[filter].is_file():
                    filter_df = pandas.read_feather(filter_files[filter])
                    filter_df.set_index("event", inplace=True)

                    if filter == "event":
                        from cut_etroc1_single_run import apply_event_filter
                        data_df = apply_event_filter(data_df, filter_df)
                else:
                    script_logger.error("The filter file {} does not exist".format(filter_files[filter]))

            full_df = data_df
            if 'accepted' in data_df.columns:
                data_df = full_df.loc[full_df['accepted']==True]
            #board_grouped_accepted_data_df = accepted_data_df.groupby(['data_board_id'])

            extra_title = ""
            tot_dimensions = []
            toa_dimensions = []
            toa_labels = {}

            for board_id in data_df["data_board_id"].unique():
                toa_dimensions += ["time_of_arrival_ns_{}".format(board_id)]
                tot_dimensions += ["time_over_threshold_ns_{}".format(board_id)]
                toa_labels["time_of_arrival_ns_{}".format(board_id)] = "Board {} TOA [ns]".format(board_id)
                toa_labels["time_over_threshold_ns_{}".format(board_id)] = "Board {} TOT [ns]".format(board_id)

                board_df = data_df.loc[data_df["data_board_id"] == board_id]

                min_toa_df = board_df["time_of_arrival_ns"].min()
                max_toa_df = board_df["time_of_arrival_ns"].max()
                min_tot_df = board_df["time_over_threshold_ns"].min()
                max_tot_df = board_df["time_over_threshold_ns"].max()

                if min_toa is not None and max_toa is not None and max_toa > min_toa:
                    range_toa = [
                        max(min_toa, min_toa_df),
                        min(max_toa, max_toa_df)
                    ]
                else:
                    range_toa = [min_toa_df, max_toa_df]
                if min_tot is not None and max_tot is not None and max_tot > min_tot:
                    range_tot = [
                        max(min_tot, min_tot_df),
                        min(max_tot, max_tot_df)
                    ]
                else:
                    range_tot = [min_tot_df, max_tot_df]

                nbins_toa = ceil((range_toa[1] - range_toa[0]) * 20)
                nbins_tot = ceil((range_tot[1] - range_tot[0]) * 20)

                fig = px.density_heatmap(
                    board_df,
                    x="time_over_threshold_ns",
                    y="time_of_arrival_ns",
                    labels = {
                        "time_over_threshold_ns": "Time over Threshold [ns]",
                        "time_of_arrival_ns": "Time of Arrival [ns]",
                        "data_board_id": "Board ID",
                    },
                    color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
                    title = "Histogram of TOT vs TOA in ns<br><sup>Board {}; Run: {}{}</sup>".format(board_id, Monet.run_name, extra_title),
                    range_x=range_tot,
                    range_y=range_toa,
                    nbinsx=nbins_tot,
                    nbinsy=nbins_toa,
                )

                fig.write_html(
                    Monet.task_path/'Board{}_TOT_vs_TOA_ns.html'.format(board_id),
                    full_html = full_html,
                    include_plotlyjs = 'cdn',
                )

                fig = px.scatter(
                    board_df,
                    x="time_over_threshold_ns",
                    y="time_of_arrival_ns",
                    labels = {
                        "time_over_threshold_ns": "Time over Threshold [ns]",
                        "time_of_arrival_ns": "Time of Arrival [ns]",
                        "data_board_id": "Board ID",
                    },
                    title = "Scatter of TOT vs TOA in ns<br><sup>Board {}; Run: {}{}</sup>".format(board_id, Monet.run_name, extra_title),
                    range_x=range_tot,
                    range_y=range_toa,
                    opacity=0.2,
                )

                fig.write_html(
                    Monet.task_path/'Board{}_TOT_vs_TOA_ns_scatter.html'.format(board_id),
                    full_html = full_html,
                    include_plotlyjs = 'cdn',
                )

            min_toa_df = data_df["time_of_arrival_ns"].min()
            max_toa_df = data_df["time_of_arrival_ns"].max()
            min_tot_df = data_df["time_over_threshold_ns"].min()
            max_tot_df = data_df["time_over_threshold_ns"].max()

            if min_toa is not None and max_toa is not None and max_toa > min_toa:
                range_toa = [
                    max(min_toa, min_toa_df),
                    min(max_toa, max_toa_df)
                ]
            else:
                range_toa = [min_toa_df, max_toa_df]
            if min_tot is not None and max_tot is not None and max_tot > min_tot:
                range_tot = [
                    max(min_tot, min_tot_df),
                    min(max_tot, max_tot_df)
                ]
            else:
                range_tot = [min_tot_df, max_tot_df]

            nbins_toa = ceil((range_toa[1] - range_toa[0]) * 30)
            nbins_tot = ceil((range_tot[1] - range_tot[0]) * 30)

            fig = px.density_heatmap(
                data_df,
                x="time_over_threshold_ns",
                y="time_of_arrival_ns",
                labels = {
                    "time_over_threshold_ns": "Time over Threshold [ns]",
                    "time_of_arrival_ns": "Time of Arrival [ns]",
                    "data_board_id": "Board ID",
                },
                color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
                facet_col='data_board_id',
                facet_col_wrap=2,
                title = "Histogram of TOT vs TOA in ns<br><sup>Run: {}{}</sup>".format(Monet.run_name, extra_title),
                range_x=range_tot,
                range_y=range_toa,
                nbinsx=nbins_tot,
                nbinsy=nbins_toa,
            )
            fig.write_html(
                Monet.task_path/'TOT_vs_TOA_ns.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            data_df["data_board_id_cat"] = data_df["data_board_id"].astype(str)
            fig = px.scatter_matrix(
                data_df,
                dimensions=["time_of_arrival", "time_over_threshold", "calibration_code", "time_of_arrival_ns", "time_over_threshold_ns"],
                labels = {
                    "time_over_threshold": "Time over Threshold [code]",
                    "time_of_arrival": "Time of Arrival [code]",
                    "time_over_threshold_ns": "Time over Threshold [ns]",
                    "time_of_arrival_ns": "Time of Arrival [ns]",
                    "calibration_code": "Calibration Code",
                    "data_board_id_cat": "Board ID",
                },
                color='data_board_id_cat',
                title = "Scatter plot comparing variables for each board<br><sup>Run: {}{}</sup>".format(Monet.run_name, extra_title),
                opacity = 0.2,
            )
            fig.update_traces(
                diagonal_visible=False,
                showupperhalf=False,
                marker = {'size': 3},
            )
            for k in range(len(fig.data)):
                fig.data[k].update(
                    selected = dict(
                        marker = dict(
                            #opacity = 1,
                            #color = 'blue',
                        )
                    ),
                    unselected = dict(
                        marker = dict(
                            #opacity = 0.1,
                            color="grey"
                        )
                    ),
                )
            fig.write_html(
                Monet.task_path/'multi_scatter.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            pivot_data_df = data_df.pivot(
                index = 'event',
                columns = 'data_board_id',
                values = list(set(data_df.columns) - {'data_board_id', 'event'}),
            )
            pivot_data_df.columns = ["{}_{}".format(x, y) for x, y in pivot_data_df.columns]

            fig = px.scatter_matrix(
                pivot_data_df,
                dimensions = sorted(toa_dimensions),
                labels = toa_labels,
				title = 'TOA Correlation Matrix<br><sup>Run: {}{}</sup>'.format(Monet.run_name, extra_title),
                opacity = 0.15,
            )
            fig.update_traces(
                diagonal_visible = False,
                showupperhalf = False,
                marker = {'size': 3},
            )
            for k in range(len(fig.data)):
                fig.data[k].update(
                    selected = dict(
                        marker = dict(
                            #opacity = 1,
                            #color = 'blue',
                        )
                    ),
                    unselected = dict(
                        marker = dict(
                            #opacity = 0.1,
                            color="grey"
                        )
                    ),
                )
            fig.write_html(
                Monet.task_path/'toa_correlation_matrix.html',
                full_html = full_html,
                include_plotlyjs = 'cdn',
            )

            board_ids = sorted(data_df["data_board_id"].unique())
            for idx_a in range(len(board_ids)):
                board_a = board_ids[idx_a]
                for idx_b in range(len(board_ids)):
                    board_b = board_ids[idx_b]
                    if idx_a >= idx_b:
                        continue

                    make_toa_correlation(board_a=board_a, board_b=board_b, data_df=pivot_data_df, base_path=Monet.task_path, run_name=Monet.run_name, extra_title=extra_title, full_html=full_html, range_x=range_toa, range_y=range_toa)

            fig = px.scatter_matrix(
                pivot_data_df,
                dimensions=toa_dimensions + tot_dimensions,
                labels = toa_labels,
                title = "Scatter plot correlating time variables between boards<br><sup>Run: {}{}</sup>".format(Monet.run_name, extra_title),
                opacity = 0.2,
            )
            fig.update_traces(
                diagonal_visible=False,
                showupperhalf=False,
                marker = {'size': 3},
            )
            for k in range(len(fig.data)):
                fig.data[k].update(
                    selected = dict(
                        marker = dict(
                            #opacity = 1,
                            #color = 'blue',
                        )
                    ),
                    unselected = dict(
                        marker = dict(
                            #opacity = 0.1,
                            color="grey"
                        )
                    ),
                )
            fig.write_html(
                Monet.task_path/'time_scatter.html',
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

        calculate_times_in_ns_task(Fermat, script_logger=script_logger)

        plot_times_in_ns_task(
            Fermat,
            script_logger=script_logger,
            task_name="plot_times_in_ns_before_cuts",
            data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
            filter_files={},
            max_toa=max_toa,
            max_tot=max_tot,
            min_toa=0,
            min_tot=0,
        )

        plot_times_in_ns_task(
            Fermat,
            script_logger=script_logger,
            task_name="plot_times_in_ns_after_cuts",
            data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
            filter_files={"event": Fermat.path_directory/"event_filter.fd"},
            max_toa=max_toa,
            max_tot=max_tot,
            min_toa=0,
            min_tot=0,
        )

        # TODO: Add here cuts for cleaning TOA and other stuff

        plot_times_in_ns_task(
            Fermat,
            script_logger=script_logger,
            task_name="plot_times_in_ns_final",
            data_file=Fermat.get_task_path("calculate_times_in_ns")/'data.sqlite',
            filter_files={"event": Fermat.path_directory/"event_filter.fd"},
            max_toa=max_toa,
            max_tot=max_tot,
            min_toa=0,
            min_tot=0,
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