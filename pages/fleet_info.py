from dash import register_page, html, Output, Input, callback, ALL
import dash_leaflet as dl
import dash_bootstrap_components as dbc
from dash import ctx
from db import get_fleet_data, get_veh_data, get_charger_data
from utils import format_if_notna, battery_chem_map, charger_type_map, connector_type_map, map_multi_labels
import json

register_page(__name__, path="/fleet_info")

# Load PA boundary GeoJSON (replace with your local path or URL fetch if needed)
with open("assets/pa_boundary.geojson") as f:
    pa_geojson = json.load(f)
    
pa_border = dl.GeoJSON(data=pa_geojson, 
                       options=dict(style=dict(color="red", weight=2, fill=False)), 
                       hoverStyle=dict(weight=2, color="darkblue"))

# Load fleet data
df_fleet = get_fleet_data()
df_fleet = df_fleet[df_fleet["latitude"].notnull() & df_fleet["longitude"].notnull()]

df_veh = get_veh_data()
# df_veh["battery_chem_label"] = df_veh["battery_chem"].map(battery_chem_map)

df_charger = get_charger_data()
df_charger["charger_type_label"] = map_multi_labels(df_charger["charger_type"], charger_type_map)
df_charger["connector_type_label"] = map_multi_labels(df_charger["connector_type"], connector_type_map)

# print(df_veh.head())
# print(df_charger.head())

# Create markers with pattern-matching IDs
markers = [
    dl.Marker(
        children=dl.Tooltip(row["fleet_name"]),
        id={"type": "fleet-marker", "index": int(row["id"])},
        position=[row["latitude"], row["longitude"]],
        )
    for _, row in df_fleet.iterrows()
]

layout = dbc.Row([
    dbc.Col([
        html.H4("Fleet Details", className="fw-bold"),
        html.Div("Click a marker to view fleet information.", id="fleet-detail")
    ], width=3, className="bg-dark text-white", style={
        "padding": "1rem",
        "height": "90vh",
        "overflowY": "auto"
    }),

    dbc.Col([
        dl.Map(children=[
            dl.TileLayer(),
            dl.LayerGroup(markers),
            pa_border
            ], 
               bounds=[[39.7198, -80.5199], [42.2695, -74.6895]],
               style={'height': '90vh'}, 
               )
    ], width=9)
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
    fleet_row = df_fleet[df_fleet["id"] == fleet_id].iloc[0]
    # print(fleet_id, type(fleet_id))
    df_veh_sub = df_veh[df_veh["fleet_id"] == fleet_id]
    # print(df_veh_sub.groupby(["make", "model", "year"]).size())
    df_charger_sub = df_charger[df_charger["fleet_id"] == fleet_id]
    # print(df_charger_sub.groupby(["charger_type_label", "connector_type_label"]).size())

    return dbc.Card([
        dbc.CardHeader(html.H4(fleet_row["fleet_name"], className="mb-0")),

        dbc.CardBody([
            html.H6("Fleet Summary", style={"fontWeight": "bold", "marginTop": "1rem"}),
            html.P(f"Fleet size: {format_if_notna(fleet_row['fleet_size'])}", className="mb-1"),
            html.P(f"ZEVs total: {format_if_notna(fleet_row['zev_tot'])}", className="mb-1"),
            html.P(f"ZEVs grant: {format_if_notna(fleet_row['zev_grant'])}", className="mb-1"),
            html.P(f"Charger grant: {format_if_notna(fleet_row['charger_grant'])}", className="mb-1"),
            html.P(f"Vendor: {fleet_row['vendor_name']}", className="mb-1"),
            html.P(f"Depot address: {fleet_row['depot_adr']}", className="mb-3"),

            html.Hr(),

            html.H6("Vehicle Details", style={"fontWeight": "bold", "marginTop": "1rem"}),
            html.P(f"Total vehicles: {len(df_veh_sub)}", className="mb-2"),
            dbc.ListGroup([
                dbc.ListGroupItem(f"{make} {model} {year} – {count} vehicle(s)")
                for (make, model, year), count in df_veh_sub.groupby(["make", "model", "year"]).size().items()
            ]) if not df_veh_sub.empty else html.P("No vehicle data available.", className="text-muted"),

            html.Hr(),

            html.H6("Charger Details", style={"fontWeight": "bold", "marginTop": "1rem"}),
            html.P(f"Total chargers: {len(df_charger_sub)}", className="mb-2"),
            dbc.ListGroup([
                dbc.ListGroupItem(f"{charger_type_label} / {connector_type_label} – {count} charger(s)")
                for (charger_type_label, connector_type_label), count in df_charger_sub.groupby(["charger_type_label", "connector_type_label"]).size().items()
            ]) if not df_charger_sub.empty else html.P("No charger data available.", className="text-muted"),
        ])
    ], className="mb-3 shadow-sm")
