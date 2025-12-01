from dash import register_page, html, dcc, callback, Input, Output, dash_table
import dash_leaflet as dl
import dash_bootstrap_components as dbc
import pandas as pd
from db import engine
from sqlalchemy import text
from styles import DROPDOWN_STYLE, DARK_BG, TEXT_COLOR
import json
import plotly.express as px
import geopandas as gpd


def latest_month_bounds(engine):
    sql = text("""
        WITH m AS (
          SELECT max("timestamp")::date AS end_d
          FROM veh_tel
        )
        SELECT (end_d - INTERVAL '30 days')::date AS start_date,
               end_d::date                         AS end_date
        FROM m;
    """)
    df = pd.read_sql(sql, engine)
    if df.empty or pd.isna(df.iloc[0]['end_date']):
        return None, None
    r = df.iloc[0]
    return str(r.start_date), str(r.end_date)

register_page(__name__, path="/telematics", name="Telematics")

# ---------- Fleet Color Mapping ----------
COLOR_SERIES = px.colors.qualitative.Dark24

# Load all fleet names from DB and assign colors
fleet_list = pd.read_sql("SELECT fleet_name FROM fleet ORDER BY fleet_name", engine)["fleet_name"].tolist()
FLEET_COLORS = {fleet: COLOR_SERIES[i % len(COLOR_SERIES)] for i, fleet in enumerate(fleet_list)}

# Load PA boundary
with open("assets/pa_boundary.geojson") as f:
    pa_geojson = json.load(f)

pa_border = dl.GeoJSON(
    data=pa_geojson,
    options=dict(style=dict(color="red", weight=2, fill=False)),
    hoverStyle=dict(weight=2, color="darkblue")
)

# Load EJ areas from PostGIS
ej_gdf = gpd.read_postgis(
    "SELECT id, ejarea, geometry FROM ej_area WHERE ejarea = true",
    engine,
    geom_col="geometry"
)
ej_geojson = json.loads(ej_gdf.to_json())

# Style EJ areas: all red
ej_layer = dl.GeoJSON(
    data=ej_geojson,
    options=dict(style=dict(color="gray", weight=1, fillOpacity=0.3)),
    id="ej-layer"
)

start_d, end_d = latest_month_bounds(engine)

# ---------- Layout ----------
layout = html.Div([
    # KPI Row (always full dataset)
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Avg Speed (mph)"), html.H4(id="kpi-avg-speed")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Max Speed (mph)"), html.H4(id="kpi-max-speed")]))),
        # dbc.Col(dbc.Card(dbc.CardBody([html.H6("Total Distance (mi)"), html.H4(id="kpi-distance")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("# Data Points"), html.H4(id="kpi-points")]))),
    ], className="mb-4"),

    dbc.Row([
        # Left Filters + Summary Table
        dbc.Col([
            html.H5("Filters", style={"color": TEXT_COLOR}),
            dcc.Dropdown(id="fleet-dropdown-telematics", placeholder="Select Fleet", style=DROPDOWN_STYLE),
            dcc.Dropdown(id="vehicle-dropdown-telematics", placeholder="Select Vehicle", style=DROPDOWN_STYLE),
            dcc.DatePickerRange(id="date-picker", start_date=start_d, end_date=end_d,),
            
            # add spacing
            html.Div(style={"height": "25px"}),
            
            html.H5("Filtered Summary", style={"color": TEXT_COLOR}),
            
            html.Div(
                id="summary-table-telematics",
                style={
                    "width": "100%",          # take full available width
                    "maxWidth": "100%",       # prevent shrinking
                    "overflowX": "auto",      # horizontal scroll if needed
                    "marginBottom": "20px"    # space below
                }
            )
        ], width=3, style={
            "backgroundColor": DARK_BG, 
            "padding": "1rem",
            "display": "flex",
            "flexDirection": "column",
            "gap": "10px"  # space between filter elements
        }),

        # Right Map
        dbc.Col([
            dl.Map(
                id="telematics-map",
                children=[
                    dl.TileLayer(),
                    pa_border,
                    ej_layer,
                ],
                bounds=[[39.7198, -80.5199], [42.2695, -74.6895]],
                style={'height': '90vh'}
            )
        ], width=9)
    ])
])

# ---------- Callbacks ----------

# Fleet options
@callback(
    Output("fleet-dropdown-telematics", "options"),
    Input("fleet-dropdown-telematics", "id")
)
def load_fleet_options(_):
    df = pd.read_sql("SELECT fleet_name FROM fleet ORDER BY fleet_name", engine)
    return [{"label": f, "value": f} for f in df["fleet_name"]]

# Vehicle options
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
    df = pd.read_sql(query, engine, params=(fleet_name,))
    return [{"label": vid, "value": vid} for vid in df["fleet_vehicle_id"]]

# KPIs (always all data)
@callback(
    Output("kpi-avg-speed", "children"),
    Output("kpi-max-speed", "children"),
    # Output("kpi-distance", "children"),
    Output("kpi-points", "children"),
    Input("fleet-dropdown-telematics", "id")  # dummy trigger
)
def update_kpis(_):
    df = pd.read_sql("SELECT speed, mileage FROM veh_tel", engine)
    if df.empty:
        return "0", "0", "0"
    
    avg_speed = round(df["speed"].mean(), 2)
    max_speed = round(df["speed"].max(), 2)
    points = len(df)
    return avg_speed, max_speed, points

# Map + Filtered Summary
@callback(
    Output("telematics-map", "children", allow_duplicate=True),
    Output("summary-table-telematics", "children"),
    Input("fleet-dropdown-telematics", "value"),
    Input("vehicle-dropdown-telematics", "value"),
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date"),
    prevent_initial_call="initial_duplicate"
)
def update_map_and_summary(fleet_name, vehicle_id, start_date, end_date):
    query = """
        SELECT t.timestamp, t.latitude, t.longitude, t.speed,
               f.fleet_name, v.fleet_vehicle_id
        FROM veh_tel t
        JOIN vehicle v ON t.veh_id = v.id
        JOIN fleet f ON v.fleet_id = f.id
        WHERE 1=1
    """
    params = ()
    if fleet_name:
        query += " AND f.fleet_name = %s"
        params += (fleet_name,)
    if vehicle_id:
        query += " AND v.fleet_vehicle_id = %s"
        params += (vehicle_id,)
    if start_date:
        query += " AND t.timestamp >= %s"
        params += (start_date,)
    if end_date:
        query += " AND t.timestamp <= %s"
        params += (end_date,)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return [dl.TileLayer(), pa_border], html.Div("No data", style={"color": TEXT_COLOR})
    
    # Sort points in time and split trajectories by vehicle to avoid cross-vehicle connections
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    # Summary Table
    summary_data = [
        ["Fleet", fleet_name or "All"],
        ["Vehicle", vehicle_id or "All"],
        ["Date Range", f"{start_date or 'Start'} to {end_date or 'End'}"],
        ["# Data Points", len(df)],
        # ["Total Distance (mi)", total_distance],
        ["Avg Speed (mph)", round(df["speed"].mean(), 2)],
        ["Max Speed (mph)", round(df["speed"].max(), 2)]
    ]
    summary_table = dbc.Table(
        [html.Tr([html.Td(k), html.Td(v)]) for k, v in summary_data],
        bordered=True,
        color="dark",
        hover=True,
        responsive=True,
        striped=True
    )

    # Build polylines with fixed fleet colors
    polylines = []
    for (fleet, veh), fleet_df in df.groupby(["fleet_name", "fleet_vehicle_id"]):
        coords = list(zip(fleet_df["latitude"], fleet_df["longitude"]))
        color = FLEET_COLORS.get(fleet, "gray")
        tooltip = f"{fleet} | {veh}"
        polylines.append(dl.Polyline(
            positions=coords,
            color=color,
            weight=3,
            opacity=0.9,
            children=[dl.Tooltip(tooltip)]
        ))

    return [dl.TileLayer(), pa_border, ej_layer, *polylines], summary_table
