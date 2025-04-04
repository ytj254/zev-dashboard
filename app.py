from dash import Dash, html
import dash_leaflet as dl
from db import get_fleet_data

app = Dash(__name__)
app.title = "ZEV Fleet Map"

df = get_fleet_data()

app.layout = html.Div([
    html.H1("ZEV Fleet Map"),
    dl.Map(center=[40.8, -77.8], zoom=7, style={'height': '90vh'}, children=[
        dl.TileLayer(),
        dl.LayerGroup([
            dl.Marker(
                position=[row["latitude"], row["longitude"]],
                children=dl.Popup([
                    html.B(row["fleet"]),
                    html.Br(),
                    f"ZEVs: {row['zev_tot']}",
                    html.Br(),
                    f"Vendor: {row['vendor_name']}",
                    html.Br(),
                    row["depot_adr"]
                ])
            ) for _, row in df.iterrows()
        ])
    ])
])

if __name__ == "__main__":
    app.run_server(debug=True)
