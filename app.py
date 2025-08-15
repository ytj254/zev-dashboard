from dash import Dash, html, page_container
import dash_bootstrap_components as dbc

app = Dash(
    __name__, 
    use_pages=True, 
    external_stylesheets=[dbc.themes.DARKLY], 
    suppress_callback_exceptions=True
)
server = app.server  # This is what gunicorn needs

nav_items = [
    dbc.NavItem(dbc.NavLink("OVERVIEW", href="/", active="exact")),
    dbc.NavItem(dbc.NavLink("FLEET", href="/fleet_info", active="exact")),
    # dbc.NavItem(dbc.NavLink("VEHICLE", href="/vehicle_infor", active="exact")),
    # dbc.NavItem(dbc.NavLink("CHARGER", href="/charger_info", active="exact")),
    dbc.NavItem(dbc.NavLink("DAILY USAGE", href="/veh_daily_usage", active="exact")),
    dbc.NavItem(dbc.NavLink("TELEMATICS", href="/telematics", active="exact")),
    dbc.NavItem(dbc.NavLink("CHARGING", href="/charging", active="exact")),
    dbc.NavItem(dbc.NavLink("MAINTENANCE", href="/maintenance", active="exact")),
    dbc.NavItem(dbc.NavLink("ANALYSIS", href="/analysis", active="exact")),
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
    app.run(host="0.0.0.0", port=8050, debug=True)
