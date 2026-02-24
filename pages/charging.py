from dash import register_page, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from utils import charger_type_map
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig


register_page(__name__, path="/charging", name="Charging")
LOCAL_TZ = "America/New_York"
TIMESTAMP_COLS = ["connect_time", "disconnect_time", "refuel_start", "refuel_end"]


def load_charging_data():
    query = """
        SELECT r.*, c.charger_type, f.fleet_name
        FROM refuel_inf r
        JOIN charger c ON r.charger_id = c.id
        JOIN fleet f ON c.fleet_id = f.id
    """
    df = pd.read_sql(query, engine)

    for col in TIMESTAMP_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["start_soc"] = df["start_soc"] * 100
    df["end_soc"] = df["end_soc"] * 100
    df["soc_gain"] = df["end_soc"] - df["start_soc"]

    # Charging duration: prefer tot_ref_dura, fallback to refuel_start/refuel_end.
    duration_from_times = (df["refuel_end"] - df["refuel_start"]).dt.total_seconds() / 60
    df["charging_duration"] = df["tot_ref_dura"].fillna(duration_from_times)

    df["connecting_duration"] = (df["disconnect_time"] - df["connect_time"]).dt.total_seconds() / 60
    df["charge_start_time"] = df["refuel_start"].fillna(df["connect_time"])
    df["charge_end_time"] = df["refuel_end"].fillna(df["disconnect_time"])

    df["charger_type"] = df["charger_type"].map(charger_type_map).fillna(df["charger_type"])

    # Robust event date: charge_start_time fallback to charge_end_time.
    date_series = df["charge_start_time"].copy().fillna(df["charge_end_time"])
    df["date"] = date_series.dt.date
    return df


def _daily_mean(df, col):
    if df.empty or col not in df.columns:
        return None
    d = df[["date", col]].dropna()
    if d.empty:
        return None
    return d.groupby("date", as_index=False)[col].mean()


def _hourly_start_end_distribution(df):
    if df.empty:
        return None

    s = pd.to_datetime(df["charge_start_time"], errors="coerce").dropna()
    e = pd.to_datetime(df["charge_end_time"], errors="coerce").dropna()
    if s.empty and e.empty:
        return None

    # Stored timestamps are UTC-naive; convert to local time for 24h distribution.
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ)
    else:
        s = s.dt.tz_convert(LOCAL_TZ)

    if getattr(e.dt, "tz", None) is None:
        e = e.dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ)
    else:
        e = e.dt.tz_convert(LOCAL_TZ)

    out = []
    if not s.empty:
        out.append(pd.DataFrame({"hour": s.dt.hour, "event": "Start"}))
    if not e.empty:
        out.append(pd.DataFrame({"hour": e.dt.hour, "event": "End"}))
    return pd.concat(out, ignore_index=True) if out else None


def _apply_filters(df, fleet_val, charger_val, start_date, end_date):
    if fleet_val:
        df = df[df["fleet_name"] == fleet_val]
    if charger_val:
        df = df[df["charger_type"] == charger_val]
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date).date()]
    return df


def _default_last_30_days(df):
    if df.empty:
        return df

    date_as_dt = pd.to_datetime(df["date"], errors="coerce")
    latest = date_as_dt.max()
    if pd.isna(latest):
        return df

    earliest = latest - pd.Timedelta(days=29)
    return df[(date_as_dt >= earliest) & (date_as_dt <= latest)]


def _hourly_start_end_counts(hourly_dist):
    hourly_counts = (
        hourly_dist.groupby(["event", "hour"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    scaffold = pd.MultiIndex.from_product(
        [["Start", "End"], range(24)],
        names=["event", "hour"],
    ).to_frame(index=False)

    hourly_counts = scaffold.merge(hourly_counts, on=["event", "hour"], how="left")
    hourly_counts["count"] = hourly_counts["count"].fillna(0).astype(int)
    return hourly_counts


def _style_fig(fig):
    fig.update_layout(
        plot_bgcolor=DARK_BG,
        paper_bgcolor=DARK_BG,
        font_color=TEXT_COLOR,
        xaxis=dict(gridcolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR),
    )
    return fig


layout = html.Div([
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Charging Events", className="card-title"),
            html.H4(id="kpi-events", className="card-text")
        ])), width=2),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Average Energy Delivered (kWh)", className="card-title"),
            html.H4(id="kpi-avg-energy", className="card-text")
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Average Charging Duration (min)", className="card-title"),
            html.H4(id="kpi-avg-chg-time", className="card-text")
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Average Connecting Duration (min)", className="card-title"),
            html.H4(id="kpi-avg-conn-time", className="card-text")
        ])), width=4),
    ], className="mb-4"),

    html.H4("Charging Events Summary by Fleet and Charger Type"),
    html.Div(id="summary-table-charging"),

    html.Div([
        dcc.Dropdown(id="fleet-filter", placeholder="Select Fleet", style=DROPDOWN_STYLE),
        dcc.Dropdown(id="charger-filter", placeholder="Select Charger Type", style=DROPDOWN_STYLE),
        dcc.DatePickerRange(id="date-range-picker"),
    ], style={"display": "flex", "flexWrap": "wrap", "gap": "10px", "margin": "30px 0 20px 0"}),

    dbc.Row([
        dbc.Col(dcc.Graph(id="fig-power"), width=6),
        dbc.Col(dcc.Graph(id="fig-soc"), width=6),
    ]),
    dbc.Row([
        dbc.Col(dcc.Graph(id="fig-chg-time"), width=6),
        dbc.Col(dcc.Graph(id="fig-conn-time"), width=6),
    ]),
])


@callback(
    Output("kpi-events", "children"),
    Output("kpi-avg-energy", "children"),
    Output("kpi-avg-chg-time", "children"),
    Output("kpi-avg-conn-time", "children"),
    Output("summary-table-charging", "children"),
    Input("summary-table-charging", "id"),
)
def update_summary(_):
    header_style = {"padding": "0.3rem 0.45rem", "fontSize": "0.82rem", "whiteSpace": "nowrap"}
    cell_style = {"padding": "0.22rem 0.45rem", "fontSize": "0.82rem", "lineHeight": "1.15"}

    df = load_charging_data()
    kpi1 = len(df)
    kpi2 = round(df["tot_energy"].mean(), 2) if not df.empty else 0
    kpi3 = round(df["charging_duration"].mean(), 2) if not df.empty else 0
    kpi4 = round(df["connecting_duration"].mean(), 2) if not df.empty else 0

    summary = df.groupby(["fleet_name", "charger_type"]).agg(
        Events=("id", "count"),
        Energy_kWh=("tot_energy", "sum"),
        Avg_Charging_Min=("charging_duration", "mean"),
        Avg_Connecting_Min=("connecting_duration", "mean"),
        Avg_Power_kW=("avg_power", "mean"),
        Avg_SOC_Gain=("soc_gain", "mean"),
    ).reset_index().round(2)

    col_name_map = {
        "fleet_name": "Fleet Name",
        "charger_type": "Charger Type",
        "Events": "# of Events",
        "Energy_kWh": "Total Energy (kWh)",
        "Avg_Charging_Min": "Avg Charging (min)",
        "Avg_Connecting_Min": "Avg Connecting (min)",
        "Avg_Power_kW": "Avg Power Output (kW)",
        "Avg_SOC_Gain": "Avg SOC Gain (%)",
    }

    summary.rename(columns=col_name_map, inplace=True)
    summary.columns = [col_name_map.get(col, col) for col in summary.columns]
    summary = summary.where(pd.notna(summary), "n/a")

    header = html.Thead(html.Tr([html.Th(col, style=header_style) for col in summary.columns]))
    body = html.Tbody([
        html.Tr([html.Td(row[col], style=cell_style) for col in summary.columns])
        for _, row in summary.iterrows()
    ])

    table_ui = dbc.Table(
        [header, body],
        bordered=True,
        hover=True,
        responsive=True,
        className="table table-dark mb-0",
        size="sm",
    )

    return kpi1, kpi2, kpi3, kpi4, table_ui


@callback(
    Output("fleet-filter", "options"),
    Input("fleet-filter", "id"),
)
def populate_fleet_options(_):
    df = load_charging_data()
    fleets = sorted(df["fleet_name"].dropna().unique())
    return [{"label": f, "value": f} for f in fleets]


@callback(
    Output("charger-filter", "options"),
    Input("charger-filter", "id"),
)
def populate_charger_options(_):
    df = load_charging_data()
    types = sorted(df["charger_type"].dropna().unique())
    return [{"label": t, "value": t} for t in types]


@callback(
    Output("fig-power", "figure"),
    Output("fig-soc", "figure"),
    Output("fig-chg-time", "figure"),
    Output("fig-conn-time", "figure"),
    Input("fleet-filter", "value"),
    Input("charger-filter", "value"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
)
def update_figures(fleet_val, charger_val, start_date, end_date):
    df = load_charging_data()

    # Auto-limit to latest 30 days if nothing is selected.
    if not any([fleet_val, charger_val, start_date, end_date]) and not df.empty:
        df = _default_last_30_days(df)

    df = _apply_filters(df, fleet_val, charger_val, start_date, end_date)

    figs = []

    daily_power = _daily_mean(df, "avg_power")
    if daily_power is None or daily_power.empty:
        figs.append(empty_fig("No data available"))
    else:
        f = px.bar(daily_power, x="date", y="avg_power", title="Average Power Output (kW)")
        f.update_layout(yaxis_title="Power (kW)")
        figs.append(f)

    hourly_dist = _hourly_start_end_distribution(df)
    if hourly_dist is None or hourly_dist.empty:
        figs.append(empty_fig("No data available"))
    else:
        hourly_counts = _hourly_start_end_counts(hourly_dist)
        f = px.bar(
            hourly_counts,
            x="hour",
            y="count",
            color="event",
            barmode="group",
            category_orders={"event": ["Start", "End"]},
            title="Charging Event Time Distribution (24h, Local Time)",
        )
        f.update_layout(
            xaxis_title="Hour of Day",
            yaxis_title="Event Count",
            legend_title="Event",
        )
        f.update_xaxes(tickmode="array", tickvals=list(range(24)))
        figs.append(f)

    daily_chg = _daily_mean(df, "charging_duration")
    if daily_chg is None or daily_chg.empty:
        figs.append(empty_fig("No data available"))
    else:
        f = px.bar(daily_chg, x="date", y="charging_duration", title="Average Charging Duration (min)")
        f.update_layout(yaxis_title="Duration (min)")
        figs.append(f)

    daily_conn = _daily_mean(df, "connecting_duration")
    if daily_conn is None or daily_conn.empty:
        figs.append(empty_fig("No data available"))
    else:
        f = px.bar(daily_conn, x="date", y="connecting_duration", title="Average Connecting Duration (min)")
        f.update_layout(yaxis_title="Duration (min)")
        figs.append(f)

    for fig in figs:
        _style_fig(fig)

    return tuple(figs)
