from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import shutil
import pandas
import sqlite3

import plotly.express as px
import plotly.graph_objects as go

def make_plots(
    original_df: pandas.DataFrame,
    run_name: str,
    task_name: str,
    base_path: Path,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    extra_title: str = ""
):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    if "accepted" in original_df:
        df = original_df.query('accepted==True')
    else:
        df = original_df

    fig = go.Figure()
    for board_id in df["data_board_id"].unique():
        fig.add_trace(go.Histogram(
            x=df.loc[df["data_board_id"] == board_id]["calibration_code"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Calibration Code<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Calibration Code', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )
    fig.update_yaxes(type="log")

    fig.write_html(
        base_path/'calibration_code_histogram.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )
    fig.update_traces(
        histnorm="probability"
    )
    fig.update_layout(
        yaxis_title_text='Probability', # yaxis label
    )
    fig.write_html(
        base_path/'calibration_code_pdf.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = go.Figure()
    for board_id in df["data_board_id"].unique():
        fig.add_trace(go.Histogram(
            x=df.loc[df["data_board_id"] == board_id]["time_of_arrival"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Time of Arrival<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Time of Arrival', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )

    fig.write_html(
        base_path/'time_of_arrival_histogram.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )
    #fig.write_image(
    #    file=base_path/'time_of_arrival_histogram.pdf',
    #    format="pdf"
    #)
    fig.update_traces(
        histnorm="probability"
    )
    fig.update_layout(
        yaxis_title_text='Probability', # yaxis label
    )
    fig.write_html(
        base_path/'time_of_arrival_pdf.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = go.Figure()
    for board_id in df["data_board_id"].unique():
        fig.add_trace(go.Histogram(
            x=df.loc[df["data_board_id"] == board_id]["time_over_threshold"],
            name='Board {}'.format(board_id), # name used in legend and hover labels
            opacity=0.5,
            bingroup=1,
        ))
    fig.update_layout(
        barmode='overlay',
        title_text="Histogram of Time over Threshold<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
        xaxis_title_text='Time over Threshold', # xaxis label
        yaxis_title_text='Count', # yaxis label
    )

    fig.write_html(
        base_path/'time_over_threshold_histogram.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )
    fig.update_traces(
        histnorm="probability"
    )
    fig.update_layout(
        yaxis_title_text='Probability', # yaxis label
    )
    fig.write_html(
        base_path/'time_over_threshold_pdf.html',
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
        title = "Scatter plot comparing variables for each board<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
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

    if len(df) == 0:  # The heatmaps (2D Histograms) seem to break when the dataframe has no data
        return

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
            color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
            # facet_col='data_board_id',
            # facet_col_wrap=2,
            title = "Histogram of TOT vs TOA<br><sup>Board {}; Run: {}{}</sup>".format(board_id, run_name, extra_title),
            # marginal_x='box',  # One of 'rug', 'box', 'violin', or 'histogram'
            # marginal_y='box',
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
        color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
        facet_col='data_board_id',
        facet_col_wrap=2,
        title = "Histogram of TOT vs TOA<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
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
        filter_files:dict[Path] = {},
        drop_old_data:bool = True,
        extra_title: str = "",
        ):

    script_logger = logging.getLogger('run_plotter')

    if not data_file.is_file():
        script_logger.info("The data file should be an existing file")
        return

    with Bob_Manager.handle_task(task_name, drop_old_data=drop_old_data) as Picasso:
        with sqlite3.connect(data_file) as sqlite3_connection:
            df = pandas.read_sql('SELECT * FROM etroc1_data', sqlite3_connection, index_col=None)

            for filter in filter_files:
                if filter_files[filter].is_file():
                    filter_df = pandas.read_feather(filter_files[filter])
                    filter_df.set_index("event", inplace=True)

                    if filter == "event":
                        from cut_etroc1_single_run import apply_event_filter
                        df = apply_event_filter(df, filter_df)
                else:
                    script_logger.error("The filter file {} does not exist".format(filter_files[filter]))

            make_plots(df, Picasso.run_name, task_name, Picasso.task_path, extra_title=extra_title)

if __name__ == '__main__':
    print("This is not a standalone script to run, it is run automatically as a part of the other scripts")
