from dash import Dash, html, page_container
import dash_bootstrap_components as dbc

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

nav_items = [
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-house me-2"), "OVERVIEW"], href="/", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-truck me-2"), "FLEET"], href="/fleet_info", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-car-front me-2"), "VEHICLE"], href="/vehicle_infor", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-battery-charging me-2"), "CHARGER"], href="/charger_info", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-wrench me-2"), "MAINTENANCE"], href="/maintenance", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-bolt me-2"), "CHARGING"], href="/charging", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-satellite-dish me-2"), "TELEMATICS"], href="/telematics", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-calendar-day me-2"), "DAILY USAGE"], href="/veh_daily_usage", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-bar-chart me-2"), "ANALYSIS"], href="/analysis", active="exact")),
]

navbar = dbc.NavbarSimple(
    brand="ZEV Performance Dashboard",
    brand_style={"textTransform": "uppercase", "fontWeight": "bold", "fontSize": "1.25rem"},
    color="dark",
    dark=True,
    children=nav_items,
    expand="lg",
    className="mb-4"
)

app.layout = dbc.Container([
    navbar,
    page_container
], fluid=True)

if __name__ == "__main__":
    app.run(debug=True)
