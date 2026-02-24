from dash import register_page, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from utils import charger_type_map
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig


register_page(__name__, path="/analysis", name="Analysis")
LOCAL_TZ = "America/New_York"
WEEKDAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def load_charging_analysis_data():
    query = """
        SELECT r.connect_time, r.disconnect_time, r.refuel_start, r.refuel_end, c.charger_type, f.fleet_name
        FROM refuel_inf r
        JOIN charger c ON r.charger_id = c.id
        JOIN fleet f ON c.fleet_id = f.id
    """
    df = pd.read_sql(query, engine)
    df["charger_type"] = df["charger_type"].map(charger_type_map).fillna(df["charger_type"])

    # Prioritize charging timestamps; fallback to connect/disconnect.
    df["charge_start_time"] = df["refuel_start"].fillna(df["connect_time"])
    df["charge_end_time"] = df["refuel_end"].fillna(df["disconnect_time"])

    date_series = df["charge_start_time"].fillna(df["charge_end_time"])
    df["date"] = pd.to_datetime(date_series, errors="coerce").dt.date
    return df


def _to_local_time(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
    return dt.dt.tz_convert(LOCAL_TZ)


def _weekday_hour_duration_matrix(df: pd.DataFrame):
    if df.empty:
        return None

    d = df[["charge_start_time", "charge_end_time"]].copy()
    d["start_local"] = _to_local_time(d["charge_start_time"])
    d["end_local"] = _to_local_time(d["charge_end_time"])
    d = d.dropna(subset=["start_local", "end_local"])
    d = d[d["end_local"] > d["start_local"]]
    if d.empty:
        return None

    minutes = pd.DataFrame(0.0, index=WEEKDAY_ORDER, columns=range(24))
    for start_ts, end_ts in d[["start_local", "end_local"]].itertuples(index=False):
        cursor = start_ts.floor("h")
        while cursor < end_ts:
            next_hour = cursor + pd.Timedelta(hours=1)
            overlap_start = max(start_ts, cursor)
            overlap_end = min(end_ts, next_hour)
            overlap_min = (overlap_end - overlap_start).total_seconds() / 60.0
            minutes.iat[cursor.weekday(), cursor.hour] += overlap_min
            cursor = next_hour

    return minutes


layout = html.Div([
    html.H4("Analysis Workspace"),
    html.P("Cross-cutting analysis views and placeholders for upcoming insights.", style={"color": TEXT_COLOR}),

    html.Div([
        dcc.Dropdown(id="analysis-fleet-filter", placeholder="Select Fleet", style=DROPDOWN_STYLE),
        dcc.Dropdown(id="analysis-charger-filter", placeholder="Select Charger Type", style=DROPDOWN_STYLE),
        dcc.DatePickerRange(id="analysis-date-range"),
    ], style={"display": "flex", "flexWrap": "wrap", "gap": "10px", "margin": "20px 0"}),

    dbc.Row([
        dbc.Col(dcc.Graph(id="analysis-charging-heatmap"), width=8),
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H6("Telematics"),
            ]), className="mb-3"),
            dbc.Card(dbc.CardBody([
                html.H6("Maintenance"),
            ])),
        ], width=4),
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(figure=empty_fig("Telematics")), width=6),
        dbc.Col(dcc.Graph(figure=empty_fig("Daily Usage")), width=6),
    ]),
])


@callback(
    Output("analysis-fleet-filter", "options"),
    Input("analysis-fleet-filter", "id"),
)
def populate_analysis_fleet_options(_):
    df = load_charging_analysis_data()
    fleets = sorted(df["fleet_name"].dropna().unique())
    return [{"label": f, "value": f} for f in fleets]


@callback(
    Output("analysis-charger-filter", "options"),
    Input("analysis-charger-filter", "id"),
)
def populate_analysis_charger_options(_):
    df = load_charging_analysis_data()
    types = sorted(df["charger_type"].dropna().unique())
    return [{"label": t, "value": t} for t in types]


@callback(
    Output("analysis-charging-heatmap", "figure"),
    Input("analysis-fleet-filter", "value"),
    Input("analysis-charger-filter", "value"),
    Input("analysis-date-range", "start_date"),
    Input("analysis-date-range", "end_date"),
)
def update_analysis_heatmap(fleet_val, charger_val, start_date, end_date):
    df = load_charging_analysis_data()

    if not any([fleet_val, charger_val, start_date, end_date]) and not df.empty:
        latest = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.notna(latest):
            earliest = latest - pd.Timedelta(days=29)
            df = df[(pd.to_datetime(df["date"]) >= earliest) & (pd.to_datetime(df["date"]) <= latest)]

    if fleet_val:
        df = df[df["fleet_name"] == fleet_val]
    if charger_val:
        df = df[df["charger_type"] == charger_val]
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date).date()]

    matrix = _weekday_hour_duration_matrix(df)
    if matrix is None or matrix.empty:
        fig = empty_fig("No data available")
    else:
        fig = px.imshow(
            matrix,
            labels={"x": "Hour of Day", "y": "Day of Week", "color": "Charging Minutes"},
            aspect="auto",
            color_continuous_scale="YlOrRd",
            title="Charging Duration Heatmap (Day of Week x Hour, Local Time)",
        )
        fig.update_xaxes(tickmode="array", tickvals=list(range(24)))
        fig.update_layout(coloraxis_colorbar_title="Minutes")

    fig.update_layout(
        plot_bgcolor=DARK_BG,
        paper_bgcolor=DARK_BG,
        font_color=TEXT_COLOR,
        xaxis=dict(gridcolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR),
    )
    return fig
