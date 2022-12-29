from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

import plotly.express as px

def make_plots(
    df: pandas.DataFrame,
    run_name: str,
    task_name: str,
    base_path: Path,
    full_html: bool = False,  # For saving a html containing only a div with the plot
):
    fig = px.histogram(
        df,
        x = 'calibration_code',
        labels = {
            "calibration_code": "Calibration Code",
            "count": "Counts",
        },
        color='data_board_id',
        title = "Histogram of Calibration Code<br><sup>Run: {}</sup>".format(run_name),
    )

    fig.write_html(
        str(base_path/'calibration_code_histogram.html'),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = px.histogram(
        df,
        x = 'time_of_arrival',
        labels = {
            "time_of_arrival": "Time of Arrival",
            "count": "Counts",
        },
        color='data_board_id',
        title = "Histogram of Time of Arrival<br><sup>Run: {}</sup>".format(run_name),
    )

    fig.write_html(
        str(base_path/'time_of_arrival_histogram.html'),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = px.histogram(
        df,
        x = 'time_over_threshold',
        labels = {
            "time_over_threshold": "Time over Threshold",
            "count": "Counts",
        },
        color='data_board_id',
        title = "Histogram of Time over Threshold<br><sup>Run: {}</sup>".format(run_name),
    )

    fig.write_html(
        str(base_path/'time_over_threshold_histogram.html'),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    df["data_board_id_cat"] = df["data_board_id"].astype(str)
    fig = px.scatter_matrix(
        df,
        dimensions=["time_of_arrival", "time_over_threshold", "calibration_code"],
        labels = {
            "time_over_threshold": "Time over Threshold",
            "time_of_arrival": "Time of Arrival",
            "calibration_code": "Calibration Code",
            "data_board_id_cat": "Board ID",
        },
        color='data_board_id_cat',
    )

    fig.update_traces(
        diagonal_visible=False,
        showupperhalf=False
    )

    fig.write_html(
        base_path/'multi_scatter.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    for board_id in df["data_board_id"].unique():
        board_df = df.loc[df["data_board_id"] == board_id]

        fig = px.density_heatmap(
            board_df,
            x="time_over_threshold",
            y="time_of_arrival",
            labels = {
                "time_over_threshold": "Time over Threshold",
                "time_of_arrival": "Time of Arrival",
                "data_board_id": "Board ID",
            },
            # marginal_x="histogram",
            # marginal_y="histogram",
            # color_continuous_scale="Viridis",  # https://plotly.com/python/builtin-colorscales/
            # facet_col='data_board_id',
            # facet_col_wrap=2,
        )

        fig.write_html(
            base_path/'Board{}_TOT_vs_TOA.html'.format(board_id),
            full_html = full_html,
            include_plotlyjs = 'cdn',
        )

    fig = px.density_heatmap(
        df,
        x="time_over_threshold",
        y="time_of_arrival",
        labels = {
            "time_over_threshold": "Time over Threshold",
            "time_of_arrival": "Time of Arrival",
            "data_board_id": "Board ID",
        },
        # marginal_x="histogram",
        # marginal_y="histogram",
        # color_continuous_scale="Viridis",  # https://plotly.com/python/builtin-colorscales/
        facet_col='data_board_id',
        facet_col_wrap=2,
    )
    fig.write_html(
        base_path/'TOT_vs_TOA.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def plot_etroc1_task(
        Bob_Manager:RM.RunManager,
        task_name:str,
        data_file:Path,
        drop_old_data:bool = False
        ):

    script_logger = logging.getLogger('run_plotter')

    if not data_file.is_file():
        script_logger.info("The data file should be an existing file")
        return

    with Bob_Manager.handle_task(task_name, drop_old_data=drop_old_data) as Picasso:
        with sqlite3.connect(data_file) as sqlite3_connection:
            df = pandas.read_sql('SELECT * FROM etroc1_data', sqlite3_connection, index_col=None)

            make_plots(df, Picasso.run_name, task_name, Picasso.task_path)



if __name__ == '__main__':
    print("This is not a standalone script to run, it is run automatically as a part of the other scripts")
