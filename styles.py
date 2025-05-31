# styles.py
import plotly.graph_objects as go

DROPDOWN_STYLE = {
    "width": "250px",
    "color": "black",
    "backgroundColor": "white"
}

CARD_STYLE = {
    "padding": "1rem",
    "marginBottom": "1rem"
}

DARK_BG = "#303030"
TEXT_COLOR = "white"
BAR_COLOR = "#5bc0de"
GRID_COLOR="#444444"

def empty_fig(text="No data available"):
    return go.Figure(layout=go.Layout(
        paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        annotations=[dict(text=text, x=0.5, y=0.5, showarrow=False,
                          font=dict(color="white", size=16), xref="paper", yref="paper")],
        font=dict(color="white")
    ))