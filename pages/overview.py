# pages/overview.py
from dash import register_page, html

register_page(__name__, path="/")

layout = html.Div([
    html.H2("Welcome to the Pennsylvania Medium and Heavy-Duty Zero-Emission Vehicle Performance Monitoring Dashboard"),
    html.P(
        "The Driving PA Forward grant and rebate programs were developed to improve air quality "
        "statewide by driving transformation from older, high-polluting diesel engines to clean transportation technologies."
    ),
    html.P(
        "The Pennsylvania Department of Environmental Protection (DEP) is offering a grant program as part of the "
        "Driving PA Forward program to PA fleets for the purchase of modern, zero-emission vehicles (ZEV) and charging/fueling infrastructure. "
        "The objective is to develop a small number of ZEV fleet pilot projects to demonstrate ZEV vehicle performance "
        "in long-term, real-world applications."
    ),
    html.Hr(),
    html.P("Use the navigation bar above to explore fleet, vehicle, charger, telematics, and performance data.")
])
