# app.py
from dash import Dash, html, dcc, page_container
import dash_bootstrap_components as dbc

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Nav([
        dbc.NavLink("Overview", href="/", active="exact"),
        dbc.NavLink("Fleet", href="/fleet_info", active="exact"),
        dbc.NavLink("Vehicle", href="/vehicle_infor", active="exact"),
        dbc.NavLink("Charger", href="/charger_info", active="exact"),
        dbc.NavLink("Maintenance", href="/maintenance", active="exact"),
        dbc.NavLink("Charging Events", href="/charging", active="exact"),
        dbc.NavLink("Telematics", href="/telematics", active="exact"),
        dbc.NavLink("Vehicle Daily Usage", href="/veh_daily_usage", active="exact"),
        dbc.NavLink("Analysis", href="/analysis", active="exact"),
    ], pills=True),
    html.Hr(),
    page_container
], fluid=True)

if __name__ == "__main__":
    app.run(debug=True)
