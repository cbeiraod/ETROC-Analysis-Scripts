from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html

import lip_pps_run_manager as RM

import logging
import pandas
import numpy
import sympy
import sqlite3

import plotly.express as px
import plotly.graph_objects as go

from math import ceil
from math import floor

def make_multi_scatter_plot(
    data_df:pandas.DataFrame,
    run_name: str,
    task_name: str,
    base_path: Path,
    color_column: str,
    full_html: bool = False,  # For saving a html containing only a div with the plot
    extra_title: str = "",
    additional_dimensions: list[str] = [],
    additional_labels: dict[str] = {},
    use_base_dimensions: bool = True,
    ):
    if use_base_dimensions:
        labels = {
            "time_over_threshold": "Time over Threshold",
            "time_of_arrival": "Time of Arrival",
            "calibration_code": "Calibration Code",
            "data_board_id_cat": "Board ID"
        }
        dimensions = ["time_of_arrival", "time_over_threshold", "calibration_code"]
    else:
        labels = {"data_board_id_cat": "Board ID"}
        dimensions = []
    labels.update(additional_labels)

    fig = px.scatter_matrix(
        data_df,
        dimensions=sorted(dimensions + additional_dimensions),
        labels = labels,
        color=color_column,
        title = "Scatter plot comparing variables for each board<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
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
        base_path/'multi_scatter.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_tot_vs_toa_plots(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    full_html:bool=False,
    max_toa:float=20,
    max_tot:float=20,
    min_toa:float=-20,
    min_tot:float=-20,
    extra_title: str = "",
    file_prepend: str = "",
    subtitle: str = "",
    facet_col=None,
    facet_col_wrap=None,
    ):
    if file_prepend != "":
        file_prepend += "_"

    if subtitle != "":
        subtitle += "; "

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

    nbins_toa = ceil((range_toa[1] - range_toa[0]) * 20)
    nbins_tot = ceil((range_tot[1] - range_tot[0]) * 20)

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
        title = "Histogram of TOT vs TOA in ns<br><sup>{}Run: {}{}</sup>".format(subtitle, run_name, extra_title),
        range_x=range_tot,
        range_y=range_toa,
        nbinsx=nbins_tot,
        nbinsy=nbins_toa,
        facet_col=facet_col,
        facet_col_wrap=facet_col_wrap,
    )

    fig.write_html(
        base_path/'{}TOT_vs_TOA_ns_histogram.html'.format(file_prepend),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = px.scatter(
        data_df,
        x="time_over_threshold_ns",
        y="time_of_arrival_ns",
        labels = {
            "time_over_threshold_ns": "Time over Threshold [ns]",
            "time_of_arrival_ns": "Time of Arrival [ns]",
            "data_board_id": "Board ID",
        },
        title = "Scatter of TOT vs TOA in ns<br><sup>{}Run: {}{}</sup>".format(subtitle, run_name, extra_title),
        range_x=range_tot,
        range_y=range_toa,
        opacity=0.2,
        facet_col=facet_col,
        facet_col_wrap=facet_col_wrap,
    )

    fig.write_html(
        base_path/'{}TOT_vs_TOA_ns_scatter.html'.format(file_prepend),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_boards_toa_correlation_plot(
    board_a:int,
    board_b:int,
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    extra_title: str = "",
    full_html:bool=False,
    range_toa = None,
    ):
    min_x = data_df["time_of_arrival_ns_{}".format(board_a)].min()
    max_x = data_df["time_of_arrival_ns_{}".format(board_a)].max()
    min_y = data_df["time_of_arrival_ns_{}".format(board_b)].min()
    max_y = data_df["time_of_arrival_ns_{}".format(board_b)].max()

    if range_toa is None:
        range_x = [min_x, max_x]
        range_y = [min_y, max_y]
    else:
        range_x = [
            max(min_x, range_toa[0]),
            min(max_x, range_toa[1])
        ]
        range_y = [
            max(min_y, range_toa[0]),
            min(max_y, range_toa[1])
        ]

    nbinsx = ceil((range_x[1] - range_x[0]) * 40)  # 40 bins per unit (but it seems plotly uses this more as a suggestion)
    nbinsy = ceil((range_y[1] - range_y[0]) * 40)  # 40 bins per unit (but it seems plotly uses this more as a suggestion)

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

def make_toa_correlation_plot(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_ids: list[int],
    full_html:bool=False,
    extra_title: str = "",
    ):

    toa_dimensions = []
    toa_labels = {}

    for board_id in board_ids:
        toa_dimensions += ["time_of_arrival_ns_{}".format(board_id)]
        toa_labels["time_of_arrival_ns_{}".format(board_id)] = "Board {} Time of Arrival [ns]".format(board_id)

    fig = px.scatter_matrix(
        data_df,
        dimensions = sorted(toa_dimensions),
        labels = toa_labels,
        title = 'Time of Arrival Correlation Matrix<br><sup>Run: {}{}</sup>'.format(run_name, extra_title),
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
        base_path/'toa_correlation_matrix.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_toa_correlation_plots(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_ids: list[int],
    range_toa:list[float]=[-20,20],
    full_html:bool=False,
    extra_title: str = "",
    ):

    make_toa_correlation_plot(
        data_df,
        base_path=base_path,
        run_name=run_name,
        board_ids=board_ids,
        full_html=full_html,
        extra_title=extra_title,
    )

    for idx_a in range(len(board_ids)):
        board_a = board_ids[idx_a]
        for idx_b in range(len(board_ids)):
            board_b = board_ids[idx_b]
            if idx_a >= idx_b:
                continue

            make_boards_toa_correlation_plot(
                board_a=board_a,
                board_b=board_b,
                data_df=data_df,
                base_path=base_path,
                run_name=run_name,
                extra_title=extra_title,
                full_html=full_html,
                range_toa=range_toa,
            )

def make_boards_tot_correlation_plot(
    board_a:int,
    board_b:int,
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    extra_title: str = "",
    full_html:bool=False,
    range_tot = None,
    ):
    min_x = data_df["time_over_threshold_ns_{}".format(board_a)].min()
    max_x = data_df["time_over_threshold_ns_{}".format(board_a)].max()
    min_y = data_df["time_over_threshold_ns_{}".format(board_b)].min()
    max_y = data_df["time_over_threshold_ns_{}".format(board_b)].max()

    if range_tot is None:
        range_x = [min_x, max_x]
        range_y = [min_y, max_y]
    else:
        range_x = [
            max(min_x, range_tot[0]),
            min(max_x, range_tot[1])
        ]
        range_y = [
            max(min_y, range_tot[0]),
            min(max_y, range_tot[1])
        ]

    nbinsx = ceil((range_x[1] - range_x[0]) * 40)  # 40 bins per unit (but it seems plotly uses this more as a suggestion)
    nbinsy = ceil((range_y[1] - range_y[0]) * 40)  # 40 bins per unit (but it seems plotly uses this more as a suggestion)

    fig = px.scatter(
        data_df,
        x="time_over_threshold_ns_{}".format(board_a),
        y="time_over_threshold_ns_{}".format(board_b),
        labels = {
            "time_over_threshold_ns_{}".format(board_a): "Board {} Time over Threshold [ns]".format(board_a),
            "time_over_threshold_ns_{}".format(board_b): "Board {} Time over Threshold [ns]".format(board_b),
        },
        title = "Time over Threshold correlation between board {} and board {}<br><sup>Run: {}{}</sup>".format(board_a, board_b, run_name, extra_title),
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
        base_path/'tot_board{}_vs_board{}_scatter.html'.format(board_a, board_b),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    fig = px.density_heatmap(
        data_df,
        x="time_over_threshold_ns_{}".format(board_a),
        y="time_over_threshold_ns_{}".format(board_b),
        labels = {
            "time_over_threshold_ns_{}".format(board_a): "Board {} Time over Threshold [ns]".format(board_a),
            "time_over_threshold_ns_{}".format(board_b): "Board {} Time over Threshold [ns]".format(board_b),
        },
        color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
        title = "Time over Threshold correlation between board {} and board {}<br><sup>Run: {}{}</sup>".format(board_a, board_b, run_name, extra_title),
        range_x=range_x,
        range_y=range_y,
        nbinsx=nbinsx,
        nbinsy=nbinsy,
    )

    #trendline.showlegend = False
    fig.add_trace(trendline)

    fig.write_html(
        base_path/'tot_board{}_vs_board{}.html'.format(board_a, board_b),
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_tot_correlation_plot(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_ids: list[int],
    full_html:bool=False,
    extra_title: str = "",
    ):

    tot_dimensions = []
    tot_labels = {}

    for board_id in board_ids:
        tot_dimensions += ["time_over_threshold_ns_{}".format(board_id)]
        tot_labels["time_over_threshold_ns_{}".format(board_id)] = "Board {} Time over Threshold [ns]".format(board_id)

    fig = px.scatter_matrix(
        data_df,
        dimensions = sorted(tot_dimensions),
        labels = tot_labels,
        title = 'Time over Threshold Correlation Matrix<br><sup>Run: {}{}</sup>'.format(run_name, extra_title),
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
        base_path/'tot_correlation_matrix.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_tot_correlation_plots(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_ids: list[int],
    range_tot:list[float]=[-20,20],
    full_html:bool=False,
    extra_title: str = "",
    ):

    make_tot_correlation_plot(
        data_df,
        base_path=base_path,
        run_name=run_name,
        board_ids=board_ids,
        full_html=full_html,
        extra_title=extra_title,
    )

    for idx_a in range(len(board_ids)):
        board_a = board_ids[idx_a]
        for idx_b in range(len(board_ids)):
            board_b = board_ids[idx_b]
            if idx_a >= idx_b:
                continue

            make_boards_tot_correlation_plot(
                board_a=board_a,
                board_b=board_b,
                data_df=data_df,
                base_path=base_path,
                run_name=run_name,
                extra_title=extra_title,
                full_html=full_html,
                range_tot=range_tot,
            )

def make_time_correlation_plot(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_ids: list[int],
    full_html:bool=False,
    extra_title: str = "",
    do_toa: bool = True,
    do_tot: bool = True,
    additional_dimensions: list[str] = [],
    additional_labels: dict[str] = {},
    ):
    dimensions = []
    labels = {}

    # The two loops are separated so that the dimensions are in the order we choose, i.e. toa before tot
    if do_toa:
        for board_id in board_ids:
            dimension = "time_of_arrival_ns_{}".format(board_id)
            label = "Board {} TOA [ns]".format(board_id)
            dimensions += [dimension]
            labels[dimension] = label
    labels.update(additional_labels)

    if do_tot:
        for board_id in board_ids:
            dimension = "time_over_threshold_ns_{}".format(board_id)
            label = "Board {} TOT [ns]".format(board_id)
            dimensions += [dimension]
            labels[dimension] = label

    fig = px.scatter_matrix(
        data_df,
        dimensions=dimensions + additional_dimensions,
        labels = labels,
        title = "Scatter plot correlating time variables between boards<br><sup>Run: {}{}</sup>".format(run_name, extra_title),
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
        base_path/'time_scatter.html',
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_board_scatter_with_fit_plot(
    data_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    board_id: int,
    x_axis_col: str,
    y_axis_col: str,
    x_axis_label: str,
    y_axis_label: str,
    title: str,
    file_name: str,
    poly: numpy.poly1d = None,
    make_hist: bool = True,
    full_html: bool = False,
    extra_title: str = "",
    rounding_digits: int = 3,
    annotation_distance: float = 10,
    ):
    accepted = data_df[("accepted", board_id)]
    x_column = data_df.loc[accepted][(x_axis_col, board_id)].astype(float)
    y_column = data_df.loc[accepted][(y_axis_col, board_id)].astype(float)
    min_x = x_column.min()
    max_x = x_column.max()

    if poly is not None:
        poly_expr = sympy.Poly(reversed(poly.coef.round(rounding_digits)), sympy.symbols('x')).as_expr()
        poly_eq = sympy.printing.latex(poly_expr)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x = x_column,
            y = y_column,
            mode = "markers",
            name = "Data",
        )
    )
    if poly is not None:
        range_x = max_x - min_x
        extra_x = range_x * 0.1
        poly_x = numpy.linspace(min_x - extra_x, max_x + extra_x)
        poly_y = poly(poly_x)
        fig.add_trace(
            go.Scatter(
                x = poly_x,
                y = poly_y,
                mode = "lines",
                name = "Fit"
            )
        )
        fig.add_annotation(
            text="${}$".format(poly_eq),
            #xref="paper",
            #yref="paper",
            x=poly_x[-5],
            y=poly_y[-5],
            ax=-5*abs(annotation_distance),
            ay=-8*annotation_distance,
            showarrow=True,
            font=dict(
                family="Courier New, monospace",
                size=16,
                color="#ff0000"
            ),
            arrowcolor='#ff0000',
            align="right",
        )
    fig.update_layout(
        title_text="Board {} {}<br><sup>Run: {}{}</sup>".format(board_id, title, run_name, extra_title),
        xaxis_title_text=x_axis_label, # xaxis label
        yaxis_title_text=y_axis_label, # yaxis label
    )
    fig.write_html(
        base_path/'Board{}_{}.html'.format(board_id, file_name),
        full_html = full_html,
        include_plotlyjs = 'cdn',
        include_mathjax = 'cdn',
    )

    if make_hist:
        fig = px.density_heatmap(
            x=x_column,
            y=y_column,
            #color_continuous_scale=[
            #    [0, colorscale[0]],
            #    [1./1000000, colorscale[2]],
            #    [1./10000, colorscale[4]],
            #    [1./100, colorscale[7]],
            #    [1., colorscale[8]],
            #],
            color_continuous_scale="Blues",  # https://plotly.com/python/builtin-colorscales/
        )
        if poly is not None:
            poly_x = numpy.linspace(min_x, max_x)
            poly_y = poly(poly_x)
            fig.add_trace(
                go.Scatter(
                    x = poly_x,
                    y = poly_y,
                    mode = "lines",
                    name = "Fit"
                )
            )
            fig.add_annotation(
                text="${}$".format(poly_eq),
                #xref="paper",
                #yref="paper",
                x=poly_x[-5],
                y=poly_y[-5],
                ax=-5*abs(annotation_distance),
                ay=-8*annotation_distance,
                showarrow=True,
                font=dict(
                    family="Courier New, monospace",
                    size=16,
                    color="#ff0000"
                ),
                arrowcolor='#ff0000',
                align="right",
            )
        fig.update_layout(
            title_text="Board {} {}<br><sup>Run: {}{}</sup>".format(board_id, title, run_name, extra_title),
            xaxis_title_text=x_axis_label, # xaxis label
            yaxis_title_text=y_axis_label, # yaxis label
        )
        fig.write_html(
            base_path/'Board{}_{}_Heatmap.html'.format(board_id, file_name),
            full_html = full_html,
            include_plotlyjs = 'cdn',
            include_mathjax = 'cdn',
        )

def build_plots(
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
    for board_id in sorted(df["data_board_id"].unique()):
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
    for board_id in sorted(df["data_board_id"].unique()):
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
    for board_id in sorted(df["data_board_id"].unique()):
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

    sorted_df = df.sort_values(["data_board_id", "event"])
    sorted_df["data_board_id_cat"] = sorted_df["data_board_id"].astype(str)
    make_multi_scatter_plot(
        data_df=sorted_df,
        run_name=run_name,
        task_name=task_name,
        base_path=base_path,
        color_column='data_board_id_cat',
        full_html=full_html,
        extra_title=extra_title,
    )

    if len(df) == 0:  # The heatmaps (2D Histograms) seem to break when the dataframe has no data
        return

    for board_id in sorted(df["data_board_id"].unique()):
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
        sorted_df,
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

def build_time_plots(
    original_df: pandas.DataFrame,
    base_path: Path,
    run_name: str,
    task_name: str,
    full_html:bool=False,
    max_toa:float=20,
    max_tot:float=20,
    min_toa:float=-20,
    min_tot:float=-20,
    extra_title:str="",
    ):
    if extra_title != "":
        extra_title = "<br>" + extra_title

    if "accepted" in original_df:
        df = original_df.query('accepted==True')
    else:
        df = original_df
    #board_grouped_data_df = df.groupby(['data_board_id'])

    df.sort_values(["event", "data_board_id"], inplace=True)

    # Build non-numeric categories for labels in plotly
    df["data_board_id_cat"] = df["data_board_id"].astype(str)

    # Get list of all board ids
    board_ids = sorted(df["data_board_id"].unique())

    # Create the pivot table with a column for each board
    pivot_df = df.pivot(
        index = 'event',
        columns = 'data_board_id',
        values = list(set(df.columns) - {'data_board_id', 'event'}),
    )
    # Rename columns so they are no longer hierarchical
    pivot_df.columns = ["{}_{}".format(x, y) for x, y in pivot_df.columns]

    # Calculate plot ranges
    #   TOA
    range_toa = None
    min_toa_df = df["time_of_arrival_ns"].min()
    max_toa_df = df["time_of_arrival_ns"].max()
    if min_toa is None and max_toa is None:
        range_toa = [min_toa_df, max_toa_df]
    elif min_toa is not None and max_toa is not None:
        range_toa = [min_toa, max_toa]
    elif min_toa is None:
        range_toa = [min_toa_df, max_toa]
    else:
        range_toa = [min_toa, max_toa_df]
    #   TOT
    range_tot = None
    min_tot_df = df["time_over_threshold_ns"].min()
    max_tot_df = df["time_over_threshold_ns"].max()
    if min_tot is None and max_tot is None:
        range_tot = [min_tot_df, max_tot_df]
    elif min_tot is not None and max_tot is not None:
        range_tot = [min_tot, max_tot]
    elif min_tot is None:
        range_tot = [min_tot_df, max_tot]
    else:
        range_tot = [min_tot, max_tot_df]

    for board_id in board_ids:
        board_df = df.loc[df["data_board_id"] == board_id]

        make_tot_vs_toa_plots(
            data_df=board_df,
            base_path=base_path,
            run_name=run_name,
            full_html=full_html,
            max_toa=max_toa,
            max_tot=max_tot,
            min_toa=min_toa,
            min_tot=min_tot,
            extra_title=extra_title,
            file_prepend="Board{}".format(board_id),
            subtitle="Board {}".format(board_id),
        )

    make_tot_vs_toa_plots(
        data_df=df,
        base_path=base_path,
        run_name=run_name,
        full_html=full_html,
        max_toa=max_toa,
        max_tot=max_tot,
        min_toa=min_toa,
        min_tot=min_tot,
        extra_title=extra_title,
        facet_col='data_board_id',
        facet_col_wrap=2,
    )

    make_multi_scatter_plot(
        data_df=df,
        run_name=run_name,
        task_name=task_name,
        base_path=base_path,
        color_column="data_board_id_cat",
        full_html=full_html,
        extra_title=extra_title,
        additional_dimensions=["time_of_arrival_ns", "time_over_threshold_ns"],
        additional_labels={
            "time_over_threshold_ns": "Time over Threshold [ns]",
            "time_of_arrival_ns": "Time of Arrival [ns]",
        },
    )


    make_toa_correlation_plots(
        data_df=pivot_df,
        base_path=base_path,
        run_name=run_name,
        board_ids=board_ids,
        range_toa=range_toa,
        full_html=full_html,
        extra_title=extra_title,
    )

    make_tot_correlation_plots(
        data_df=pivot_df,
        base_path=base_path,
        run_name=run_name,
        board_ids=board_ids,
        range_tot=range_tot,
        full_html=full_html,
        extra_title=extra_title,
    )

    make_time_correlation_plot(
        data_df=pivot_df,
        base_path=base_path,
        run_name=run_name,
        board_ids=board_ids,
        full_html=full_html,
        extra_title=extra_title,
    )

def apply_event_filter(data_df: pandas.DataFrame, filter_df: pandas.DataFrame, filter_name: str = "event_filter"):
    reindexed_data_df = data_df.set_index('event')
    reindexed_data_df[filter_name] = filter_df
    if "accepted" not in reindexed_data_df:
        reindexed_data_df["accepted"] = reindexed_data_df[filter_name]
    else:
        reindexed_data_df["accepted"] &= reindexed_data_df[filter_name]
    return reindexed_data_df.reset_index()

def filter_dataframe(
    df:pandas.DataFrame,
    filter_files:dict[Path],
    script_logger:logging.Logger,
    ):
    for filter in filter_files:
        if filter_files[filter].is_file():
            filter_df = pandas.read_feather(filter_files[filter])
            filter_df.set_index("event", inplace=True)

            if filter == "event":
                df = apply_event_filter(df, filter_df)
            elif filter == "time":
                df = apply_event_filter(df, filter_df, filter_name="time_filter")
        else:
            script_logger.error("The filter file {} does not exist".format(filter_files[filter]))

    return df

def plot_etroc1_task(
        Bob_Manager:RM.RunManager,
        task_name:str,
        data_file:Path,
        filter_files:dict[str,Path] = {},
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

            df = filter_dataframe(
                df=df,
                filter_files=filter_files,
                script_logger=script_logger,
            )

            build_plots(df, Picasso.run_name, task_name, Picasso.task_path, extra_title=extra_title)

def plot_times_in_ns_task(
    Fermat: RM.RunManager,
    script_logger: logging.Logger,
    task_name:str,
    data_file:Path,
    filter_files:dict[str,Path] = {},
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

            data_df = filter_dataframe(
                df=data_df,
                filter_files=filter_files,
                script_logger=script_logger,
            )

            build_time_plots(
                data_df,
                base_path=Monet.task_path,
                run_name=Monet.run_name,
                task_name=Monet.task_name,
                full_html=full_html,
                max_toa=max_toa,
                max_tot=max_tot,
                min_toa=min_toa,
                min_tot=min_tot,
                extra_title=extra_title,
            )

if __name__ == '__main__':
    print("This is not a standalone script to run, it provides utilities which are run automatically as a part of the other scripts")
