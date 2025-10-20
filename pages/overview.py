# pages/overview.py
from dash import register_page, html

register_page(__name__, path="/", name="Overview")

background_url = "/assets/background.png"  # Place the image in the assets folder

layout = html.Div(
    style={
        "backgroundImage": f"url('{background_url}')",
        "backgroundSize": "cover",
        "backgroundPosition": "center",
        "minHeight": "100vh",
        "padding": "5rem 3rem",
        "color": "white",
        "textShadow": "2px 2px 6px black",
        "backgroundRepeat": "no-repeat"
    },
    children=[
        html.Div(
            style={
                "backgroundColor": "rgba(0, 0, 0, 0.6)", 
                "padding": "2.5rem", 
                "borderRadius": "1.5rem"
                },
            children=[
                html.H2(
                    "Welcome to the Pennsylvania Medium and Heavy-Duty Zero-Emission Vehicle Performance Monitoring Dashboard",
                    style={"fontWeight": "bold", "fontSize": "3rem", "lineHeight": "2"}
                ),
                html.Hr(style={"borderColor": "white"}),
                html.P(
                    "The Driving PA Forward grant and rebate programs aim to enhance air quality across Pennsylvania "
                    "by replacing older diesel engines with clean, zero-emission transportation technologies.",
                    style={"fontSize": "2rem", "marginBottom": "1.5rem"}
                ),
                html.P(
                    "As part of this initiative, the Pennsylvania Department of Environmental Protection (DEP) is providing "
                    "grants to support the purchase of zero-emission vehicles (ZEVs) and related infrastructure for fleet operations. "
                    "These pilot projects demonstrate the real-world performance of ZEVs over extended periods.",
                    style={"fontSize": "2rem", "marginBottom": "1.5rem"}
                ),
                html.P(
                    "Use the navigation bar above to explore detailed data on fleets, vehicles, chargers, telematics, and energy performance.",
                    style={"fontSize": "2rem"}
                )
            ]
        )
    ]
)
