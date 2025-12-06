from dash import register_page, html, dcc, callback, Input, Output
import dash_leaflet as dl
import dash_bootstrap_components as dbc
import pandas as pd
from db import engine
from sqlalchemy import text
from styles import DROPDOWN_STYLE, DARK_BG, TEXT_COLOR
import json
import plotly.express as px
import geopandas as gpd

register_page(__name__, path="/telematics", name="Telematics")

# ---------- Fleet Color Mapping ----------
# Pre-assign fixed colors to all fleets in database (sorted alphabetically)
COLOR_PALETTE = px.colors.qualitative.Dark24

def get_fleet_color_mapping():
    """Create a fixed fleet-to-color mapping based on all fleets in database."""
    try:
        fleet_df = pd.read_sql("SELECT fleet_name FROM fleet ORDER BY fleet_name", engine)
        color_map = {}
        for idx, fleet_name in enumerate(fleet_df["fleet_name"]):
            color_map[fleet_name] = COLOR_PALETTE[idx % len(COLOR_PALETTE)]
        return color_map
    except Exception as e:
        print(f"Error loading fleet colors: {e}")
        return {}

FLEET_COLOR_MAP = get_fleet_color_mapping()


def latest_month_bounds(engine):
    """Get the date range for the latest month of telematics data."""
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


start_d, end_d = latest_month_bounds(engine)

# ---------- Load Map Layers ----------
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

ej_layer = dl.GeoJSON(
    data=ej_geojson,
    options=dict(style=dict(color="gray", weight=1, fillOpacity=0.3)),
    id="ej-layer"
)

traj_layer = dl.LayerGroup(id="traj-layer")

# ---------- Page Layout ----------
layout = html.Div([
    # KPI Row - always shows stats for ALL data
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Avg Speed (mph)"), 
            html.H4(id="kpi-avg-speed")
        ]))),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Max Speed (mph)"), 
            html.H4(id="kpi-max-speed")
        ]))),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("# Data Points"), 
            html.H4(id="kpi-points")
        ]))),
    ], className="mb-4"),

    dbc.Row([
        # Left column: Filters + Summary Table
        dbc.Col([
            html.H5("Filters", style={"color": TEXT_COLOR}),
            dcc.Dropdown(
                id="fleet-dropdown-telematics", 
                placeholder="Select Fleet (optional)", 
                style=DROPDOWN_STYLE,
                clearable=True
            ),
            dcc.Dropdown(
                id="vehicle-dropdown-telematics", 
                placeholder="Select Vehicle (optional)", 
                style=DROPDOWN_STYLE,
                clearable=True
            ),
            dcc.DatePickerRange(
                id="date-picker-telematics", 
                start_date=start_d, 
                end_date=end_d,
                display_format='YYYY-MM-DD'
            ),
            
            html.Div(style={"height": "25px"}),
            
            html.H5("Filtered Summary", style={"color": TEXT_COLOR}),
            html.Div(
                id="summary-table-telematics",
                style={
                    "width": "100%",
                    "maxWidth": "100%",
                    "overflowX": "auto",
                    "marginBottom": "20px"
                }
            )
        ], width=3, style={
            "backgroundColor": DARK_BG, 
            "padding": "1rem",
            "display": "flex",
            "flexDirection": "column",
            "gap": "10px"
        }),

        # Right column: Map
        dbc.Col([
            dl.Map(
                id="telematics-map",
                children=[
                    dl.TileLayer(),
                    pa_border,
                    ej_layer,
                    traj_layer,
                ],
                center=[40.9, -77.5],
                zoom=7,
                style={"height": "90vh"},
            )
        ], width=9)
    ])
])

# ---------- Callbacks ----------

@callback(
    Output("fleet-dropdown-telematics", "options"),
    Input("fleet-dropdown-telematics", "id")
)
def populate_fleet_dropdown(_):
    """Load all fleet names for the dropdown."""
    try:
        df = pd.read_sql("SELECT fleet_name FROM fleet ORDER BY fleet_name", engine)
        return [{"label": f, "value": f} for f in df["fleet_name"]]
    except Exception as e:
        print(f"Error loading fleets: {e}")
        return []


@callback(
    Output("vehicle-dropdown-telematics", "options"),
    Output("vehicle-dropdown-telematics", "value"),
    Input("fleet-dropdown-telematics", "value")
)
def populate_vehicle_dropdown(fleet_name):
    """Load vehicles for selected fleet. Clear vehicle selection when fleet changes."""
    if not fleet_name:
        return [], None
    
    try:
        query = """
            SELECT v.fleet_vehicle_id
            FROM vehicle v
            JOIN fleet f ON v.fleet_id = f.id
            WHERE f.fleet_name = %s
            ORDER BY v.fleet_vehicle_id
        """
        df = pd.read_sql(query, engine, params=(fleet_name,))
        options = [{"label": vid, "value": vid} for vid in df["fleet_vehicle_id"]]
        return options, None  # Reset vehicle selection
    except Exception as e:
        print(f"Error loading vehicles: {e}")
        return [], None


@callback(
    Output("kpi-avg-speed", "children"),
    Output("kpi-max-speed", "children"),
    Output("kpi-points", "children"),
    Input("fleet-dropdown-telematics", "id")
)
def update_kpis(_):
    """Update KPIs based on ALL telematics data (not filtered)."""
    try:
        df = pd.read_sql("SELECT speed FROM veh_tel", engine)
        if df.empty:
            return "0", "0", "0"
        
        avg_speed = round(df["speed"].mean(), 2)
        max_speed = round(df["speed"].max(), 2)
        points = len(df)
        return f"{avg_speed:,.2f}", f"{max_speed:,.2f}", f"{points:,}"
    except Exception as e:
        print(f"Error updating KPIs: {e}")
        return "Error", "Error", "Error"


@callback(
    Output("traj-layer", "children"),
    Output("summary-table-telematics", "children"),
    Input("fleet-dropdown-telematics", "value"),
    Input("vehicle-dropdown-telematics", "value"),
    Input("date-picker-telematics", "start_date"),
    Input("date-picker-telematics", "end_date"),
)
def update_map_and_summary(fleet_name, vehicle_id, start_date, end_date):
    """
    Update map trajectories and summary table based on filters.
    Default: show all fleets for latest one month.
    Each fleet uses its pre-assigned color from FLEET_COLOR_MAP.
    """
    
    # Build query with filters
    query = """
        SELECT t.timestamp, t.latitude, t.longitude, t.speed,
               f.fleet_name, v.fleet_vehicle_id
        FROM veh_tel t
        JOIN vehicle v ON t.veh_id = v.id
        JOIN fleet f ON v.fleet_id = f.id
        WHERE 1=1
    """
    params = []
    
    # Apply fleet filter if selected
    if fleet_name:
        query += " AND f.fleet_name = %s"
        params.append(fleet_name)
    
    # Apply vehicle filter if selected
    if vehicle_id:
        query += " AND v.fleet_vehicle_id = %s"
        params.append(vehicle_id)
    
    # Apply date filters (default to latest month if not specified)
    if start_date:
        query += " AND t.timestamp >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND t.timestamp <= %s"
        params.append(end_date)
    
    query += " ORDER BY t.timestamp"
    
    try:
        df = pd.read_sql(query, engine, params=tuple(params) if params else None)
    except Exception as e:
        print(f"Error querying telematics data: {e}")
        error_msg = html.Div(f"Error loading data: {str(e)}", style={"color": "red"})
        return [dl.TileLayer(), pa_border, ej_layer], error_msg
    
    # ---- drop bad coordinates: critical for the 'equals' error ----
    df = df.dropna(subset=["latitude", "longitude"])
    df = df[
        df["latitude"].between(-90, 90) &
        df["longitude"].between(-180, 180)
    ]
    if df.empty:
        no_data_msg = html.Div("No valid coordinates for selected filters", style={"color": TEXT_COLOR})
        return [], no_data_msg

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # Build summary table
    summary_data = [
        ["Fleet", fleet_name or "All Fleets"],
        ["Vehicle", vehicle_id or "All Vehicles"],
        ["Date Range", f"{start_date or 'Start'} to {end_date or 'End'}"],
        ["# Data Points", f"{len(df):,}"],
        ["Avg Speed (mph)", f"{df['speed'].mean():.2f}"],
        ["Max Speed (mph)", f"{df['speed'].max():.2f}"]
    ]
    summary_table = dbc.Table(
        html.Tbody(
            [html.Tr([html.Td(k, style={"fontWeight": "bold"}), html.Td(v)])
            for k, v in summary_data]
        ),
        bordered=True,
        color="dark",
        hover=True,
        responsive=True,
        striped=True,
        size="sm"
    )
    
    # Build trajectory polylines - group by fleet and vehicle
    polylines = []
    for (fleet, vehicle), group_df in df.groupby(["fleet_name", "fleet_vehicle_id"], sort=False):
        group_df = group_df.sort_values("timestamp")
        if len(group_df) < 2:
            continue

        coords = list(zip(group_df["latitude"], group_df["longitude"]))
        fleet_color = FLEET_COLOR_MAP.get(fleet, "#808080")

        tooltip_text = f"{fleet} | {vehicle}"

        polylines.append(
            dl.Polyline(
                id={"type": "traj", "fleet": fleet, "veh": vehicle},
                positions=coords,
                pathOptions=dict(color=fleet_color, weight=3, opacity=0.8),
                children=[dl.Tooltip(tooltip_text)],
            )
        )
    
    return polylines, summary_table
