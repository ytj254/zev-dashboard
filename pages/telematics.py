from dash import register_page, html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig

register_page(__name__, path="/telematics")

# ---------- Layout ----------
layout = html.Div([
    # KPI Row
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Avg Speed (mph)"), html.H4(id="kpi-avg-speed")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Max Speed (mph)"), html.H4(id="kpi-max-speed")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Distance (mi)"), html.H4(id="kpi-distance")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("SOC Change (%)"), html.H4(id="kpi-soc-change")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("# Data Points"), html.H4(id="kpi-points")]))),
    ], className="mb-4"),

    dbc.Row([
        # Left Filters
        dbc.Col([
            html.H5("Filters"),
            dcc.Dropdown(id="fleet-dropdown-telematics", placeholder="Select Fleet", style=DROPDOWN_STYLE),
            dcc.Dropdown(id="vehicle-dropdown-telematics", placeholder="Select Vehicle", style=DROPDOWN_STYLE),
            dcc.DatePickerRange(id="date-picker", display_format="YYYY-MM-DD"),
            dbc.Checklist(
                id="layer-toggle",
                options=[
                    {"label": "Speed", "value": "speed"},
                    {"label": "SOC", "value": "soc"},
                    {"label": "Elevation", "value": "elevation"}
                ],
                inline=False
            ),
            html.Br(),
            html.Div("Replay Speed:"),
            dcc.Slider(
                id="replay-speed", min=0.5, max=3, step=0.5, value=1,
                marks={i: f"{i}x" for i in [0.5, 1, 2, 3]}
            )
        ], width=3, style={"background": "#f8f9fa", "padding": "1rem"}),

        # Right Visualization
        dbc.Col([
            dcc.Loading([
                dcc.Graph(id="telematics-map", style={"height": "60vh"}),
                dcc.Graph(id="telematics-timeseries", style={"height": "30vh"})
            ])
        ], width=9)
    ])
])

# ---------- Callbacks ----------
@callback(
    Output("fleet-dropdown-telematics", "options"),
    Input("fleet-dropdown-telematics", "id")
)
def load_fleet_options(_):
    df = pd.read_sql("SELECT fleet_name FROM fleet ORDER BY fleet_name", engine)
    return [{"label": f, "value": f} for f in df["fleet_name"]]

@callback(
    Output("vehicle-dropdown-telematics", "options"),
    Input("fleet-dropdown-telematics", "value")
)
def load_vehicle_options(fleet_name):
    if not fleet_name:
        return []
    query = """
        SELECT v.fleet_vehicle_id
        FROM vehicle v
        JOIN fleet f ON v.fleet_id = f.id
        WHERE f.fleet_name = %s
        ORDER BY v.fleet_vehicle_id
    """
    df = pd.read_sql(query, engine, params=[fleet_name])
    return [{"label": vid, "value": vid} for vid in df["fleet_vehicle_id"]]

@callback(
    Output("kpi-avg-speed", "children"),
    Output("kpi-max-speed", "children"),
    Output("kpi-distance", "children"),
    Output("kpi-soc-change", "children"),
    Output("kpi-points", "children"),
    Output("telematics-map", "figure"),
    Output("telematics-timeseries", "figure"),
    Input("fleet-dropdown-telematics", "value"),
    Input("vehicle-dropdown-telematics", "value"),
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date"),
    Input("layer-toggle", "value")
)
def update_telematics(fleet_name, vehicle_id, start_date, end_date, layers):
    if not fleet_name or not start_date or not end_date:
        return "0", "0", "0", "0", "0", empty_fig("No data"), empty_fig("No data")

    # Build query to use indexes efficiently
    query = """
        SELECT t.timestamp, t.latitude, t.longitude, t.speed, t.soc, t.elevation, t.mileage
        FROM veh_tel t
        JOIN vehicle v ON t.veh_id = v.id
        JOIN fleet f ON v.fleet_id = f.id
        WHERE f.fleet_name = %s
          AND (%s IS NULL OR v.fleet_vehicle_id = %s)
          AND t.timestamp >= %s
          AND t.timestamp <= %s
        ORDER BY t.timestamp
    """
    params = [fleet_name, vehicle_id, vehicle_id, start_date, end_date]
    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return "0", "0", "0", "0", "0", empty_fig("No data"), empty_fig("No data")

    # KPIs
    avg_speed = round(df["speed"].mean(), 2) if "speed" in df else 0
    max_speed = round(df["speed"].max(), 2) if "speed" in df else 0
    distance = round((df["mileage"].max() - df["mileage"].min()) / 1609.34, 2) if "mileage" in df else 0
    soc_change = round(df["soc"].iloc[-1] - df["soc"].iloc[0], 2) if "soc" in df else 0
    points = len(df)

    # Map
    fig_map = px.scatter_mapbox(
        df, lat="latitude", lon="longitude",
        color="speed" if layers and "speed" in layers else None,
        hover_data={"timestamp": True, "soc": True, "speed": True},
        zoom=8
    )
    fig_map.update_layout(mapbox_style="open-street-map", paper_bgcolor=DARK_BG)

    # Time-series
    fig_time = empty_fig("No data")
    if layers:
        melted = df.melt(id_vars=["timestamp"], value_vars=layers, var_name="Metric", value_name="Value")
        fig_time = px.line(melted, x="timestamp", y="Value", color="Metric")
        fig_time.update_layout(
            paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
            font_color=TEXT_COLOR,
            xaxis=dict(gridcolor=GRID_COLOR), yaxis=dict(gridcolor=GRID_COLOR)
        )

    return avg_speed, max_speed, distance, soc_change, points, fig_map, fig_time
