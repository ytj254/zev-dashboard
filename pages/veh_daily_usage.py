import dash
from dash import register_page, html, dcc, callback, Output, Input
import pandas as pd
import plotly.graph_objects as go
from db import engine

register_page(__name__, path="/veh_daily_usage")

DARK_BG = "#303030"
TEXT_COLOR = "white"
BAR_COLOR = "#5bc0de"

metric_options = [
    {'label': 'Total Distance (mi)', 'value': 'tot_dist'},
    {'label': 'Total Energy (kWh)', 'value': 'tot_energy'},
    {'label': 'Initial SOC', 'value': 'init_soc'},
    {'label': 'Final SOC', 'value': 'final_soc'},
    {'label': 'SOC Used', 'value': 'tot_soc_used'},
    {'label': 'Idle Time (h)', 'value': 'idle_time'},
    {'label': 'Peak Payload (lbs)', 'value': 'peak_payload'},
]

layout = html.Div([
    html.Div([
        dcc.Dropdown(id='fleet-dropdown', placeholder='Select Fleet', style={"width": "250px", "color": "black", "backgroundColor": "white"}),
        dcc.Dropdown(id='make-dropdown', placeholder='Select Make', style={"width": "250px", "color": "black", "backgroundColor": "white"}),
        dcc.Dropdown(id='model-dropdown', placeholder='Select Model', style={"width": "250px", "color": "black", "backgroundColor": "white"}),
        dcc.Dropdown(id='vehicle-class-dropdown', placeholder='Select Vehicle Class', style={"width": "250px", "color": "black", "backgroundColor": "white"}),
        dcc.Dropdown(id='fleet-veh-id-dropdown', placeholder='Select Fleet Vehicle ID', style={"width": "250px", "color": "black", "backgroundColor": "white"}),
        dcc.DatePickerRange(id='date-range-picker'),
        dcc.Dropdown(id='metric-dropdown', options=metric_options, value='tot_dist', placeholder='Select Metric', style={"width": "250px", "color": "black", "backgroundColor": "white"})
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'gap': '10px', 'marginBottom': '20px'}),

    html.Div([
        html.H5("Daily Metric by Date", style={"color": TEXT_COLOR}),
        dcc.Graph(id='daily-usage-graph')
    ], style={"marginBottom": "2rem"}),

    html.Div([
        html.Div([
            html.H5("Metric Summary", style={"color": TEXT_COLOR}),
            dcc.Graph(id="summary-chart-1", figure=go.Figure(layout=go.Layout(paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG, xaxis=dict(tickfont=dict(color=TEXT_COLOR)),
            yaxis=dict(tickfont=dict(color=TEXT_COLOR)),
            font=dict(color=TEXT_COLOR))))
        ], style={"width": "49%", "display": "inline-block"}),

        html.Div([
            html.H5("Comparison Chart", style={"color": TEXT_COLOR}),
            dcc.Graph(id="summary-chart-2", figure=go.Figure(layout=go.Layout(paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG, xaxis=dict(tickfont=dict(color=TEXT_COLOR)),
            yaxis=dict(tickfont=dict(color=TEXT_COLOR)),
            font=dict(color=TEXT_COLOR))))
        ], style={"width": "49%", "display": "inline-block", "marginLeft": "2%"})
    ])
])

@callback(Output('fleet-dropdown', 'options'), Input('fleet-dropdown', 'id'))
def load_fleet_options(_):
    df = pd.read_sql("SELECT DISTINCT fleet_name FROM fleet", engine)
    return [{'label': f, 'value': f} for f in df['fleet_name']]

@callback(
    Output('make-dropdown', 'options'),
    Output('model-dropdown', 'options'),
    Output('vehicle-class-dropdown', 'options'),
    Output('fleet-veh-id-dropdown', 'options'),
    Input('fleet-dropdown', 'value')
)
def update_vehicle_filters(fleet_name):
    if not fleet_name:
        return [], [], [], []
    query = """
        SELECT DISTINCT v.make, v.model, v.class, v.fleet_vehicle_id
        FROM vehicle v JOIN fleet f ON v.fleet_id = f.id
        WHERE f.fleet_name = %s
    """
    df = pd.read_sql(query, engine, params=(fleet_name,))
    return (
        [{'label': m, 'value': m} for m in df['make'].dropna().unique()],
        [{'label': m, 'value': m} for m in df['model'].dropna().unique()],
        [{'label': c, 'value': c} for c in df['class'].dropna().unique()],
        [{'label': i, 'value': i} for i in df['fleet_vehicle_id'].dropna().unique()],
    )

@callback(
    Output('daily-usage-graph', 'figure'),
    Input('fleet-dropdown', 'value'),
    Input('make-dropdown', 'value'),
    Input('model-dropdown', 'value'),
    Input('vehicle-class-dropdown', 'value'),
    Input('fleet-veh-id-dropdown', 'value'),
    Input('date-range-picker', 'start_date'),
    Input('date-range-picker', 'end_date'),
    Input('metric-dropdown', 'value')
)
def update_daily_usage(fleet, make, model, vehicle_class, fleet_vehicle_id, start_date, end_date, metric):
    def empty_fig(text):
        return go.Figure(layout=go.Layout(
            paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
            xaxis=dict(visible=False), yaxis=dict(visible=False),
            annotations=[dict(text=text, x=0.5, y=0.5, showarrow=False,
                              font=dict(color="white", size=16), xref="paper", yref="paper")]
        ))

    if not start_date or not end_date or metric is None:
        return empty_fig("Please select a date range and metric")

    query = """
        SELECT vd.date, vd.tot_dist, vd.tot_energy, vd.init_soc, vd.final_soc,
               vd.tot_soc_used, vd.idle_time, vd.peak_payload
        FROM veh_daily vd
        JOIN vehicle v ON vd.veh_id = v.fleet_vehicle_id
        JOIN fleet f ON v.fleet_id = f.id
        WHERE vd.date BETWEEN %s AND %s
    """
    params = [start_date, end_date]
    filters = [
        (fleet, " AND f.fleet_name = %s"),
        (make, " AND v.make = %s"),
        (model, " AND v.model = %s"),
        (vehicle_class, " AND v.class = %s"),
        (fleet_vehicle_id, " AND vd.veh_id = %s")
    ]
    for value, clause in filters:
        if value: query += clause; params.append(value)

    df = pd.read_sql(query + " ORDER BY vd.date", engine, params=tuple(params))
    if df.empty or metric not in df.columns:
        return empty_fig("No data available")

    y_label = next((opt['label'] for opt in metric_options if opt['value'] == metric), metric)
    fig = go.Figure(data=[go.Bar(x=df["date"], y=df[metric], marker_color=BAR_COLOR)])
    fig.update_layout(
        title=dict(text=f"{y_label} per Day", font=dict(color=TEXT_COLOR)),
        xaxis=dict(title="Date", color=TEXT_COLOR, tickfont=dict(color=TEXT_COLOR)),
        yaxis=dict(title=y_label, color=TEXT_COLOR, tickfont=dict(color=TEXT_COLOR)),
        paper_bgcolor=DARK_BG,
        plot_bgcolor=DARK_BG,
        font=dict(color=TEXT_COLOR)
    )
    return fig
