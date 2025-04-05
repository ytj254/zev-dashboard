from dash import register_page, html, Output, Input, callback, ALL
import dash_leaflet as dl
import dash_bootstrap_components as dbc
from dash import ctx
from db import get_fleet_data
from utils import format_if_notna

register_page(__name__, path="/fleet_info")

# Load fleet data
df = get_fleet_data()
df = df[df["latitude"].notnull() & df["longitude"].notnull()]

# Create markers with pattern-matching IDs
markers = [
    dl.Marker(
        children=dl.Tooltip(row["fleet_name"]),
        id={"type": "fleet-marker", "index": int(row["id"])},
        position=[row["latitude"], row["longitude"]],
        )
    for _, row in df.iterrows()
]

layout = dbc.Row([
    dbc.Col([
        html.H4("Fleet Details"),
        html.Div("Click a marker to view fleet information.", id="fleet-detail")
    ], width=4, style={"padding": "1rem", "height": "90vh", "overflowY": "auto", "background": "#f8f9fa"}),

    dbc.Col([
        dl.Map(children=[
            dl.TileLayer(),
            dl.LayerGroup(markers)
            ], 
               center=[40.8, -77.8], zoom=7, style={'height': '90vh'}, 
               )
    ], width=8)
])

# Pattern-matching callback
@callback(
    Output("fleet-detail", "children"),
    Input({"type": "fleet-marker", "index": ALL}, "n_clicks")
)
def update_fleet_info(n_clicks):
    triggered = ctx.triggered_id
    if not triggered:
        return "Click a marker to view fleet information."

    fleet_id = triggered["index"]
    fleet_row = df[df["id"] == fleet_id].iloc[0]

    return html.Div([
        html.H5(fleet_row["fleet_name"]),
        html.P(f"Fleet size: {format_if_notna(fleet_row['fleet_size'])}"),
        html.P(f"ZEVs total: {format_if_notna(fleet_row['zev_tot'])}"),
        html.P(f"ZEVs grant: {format_if_notna(fleet_row['zev_grant'])}"),
        html.P(f"Charger grant: {format_if_notna(fleet_row['charger_grant'])}"),
        html.P(f"Vendor: {fleet_row['vendor_name']}"),
        html.P(f"Depot address: {fleet_row['depot_adr']}"),
    ])
