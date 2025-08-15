from dash import register_page, html, dcc, dash_table, Input, Output, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from utils import charger_type_map
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig


register_page(__name__, path="/charging")

def load_charging_data():
    query = """
        SELECT r.*, c.charger_type, f.fleet_name
        FROM refuel_inf r
        JOIN charger c ON r.charger_id = c.id
        JOIN fleet f ON c.fleet_id = f.id
    """
    df = pd.read_sql(query, engine)
    df["start_soc"] = df["start_soc"] * 100
    df["end_soc"] = df["end_soc"] * 100
    df["soc_gain"] = df["end_soc"] - df["start_soc"]
    df["charging_duration"] = (df["refuel_end"] - df["refuel_start"]).dt.total_seconds() / 60
    df["connecting_duration"] = (df["disconnect_time"] - df["connect_time"]).dt.total_seconds() / 60
    df["charger_type"] = df["charger_type"].map(charger_type_map).fillna(df["charger_type"])
    df["date"] = pd.to_datetime(df["connect_time"]).dt.date
    return df

# === Layout ===

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
    dash_table.DataTable(
        id='summary-table',
        style_table={'overflowX': 'auto'},
        style_cell={'color': 'white', 'backgroundColor': '#303030'},
        style_header={'backgroundColor': '#1f1f1f', 'color': 'white', 'fontWeight': 'bold'}
    ),
    
    html.Div([
        dcc.Dropdown(id="fleet-filter", placeholder="Select Fleet", style=DROPDOWN_STYLE),
        dcc.Dropdown(id="charger-filter", placeholder="Select Charger Type", style=DROPDOWN_STYLE),
        dcc.DatePickerRange(id='date-range-picker'),
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'gap': '10px', 'margin': '30px 0 20px 0'}),

    dbc.Row([
        dbc.Col(dcc.Graph(id="fig-power"), width=6),
        dbc.Col(dcc.Graph(id="fig-soc"), width=6),
    ]),
    dbc.Row([
        dbc.Col(dcc.Graph(id="fig-chg-time"), width=6),
        dbc.Col(dcc.Graph(id="fig-conn-time"), width=6),
    ])
])

# === KPI and Summary Table Callback ===
@callback(
    Output("kpi-events", "children"),
    Output("kpi-avg-energy", "children"),
    Output("kpi-avg-chg-time", "children"),
    Output("kpi-avg-conn-time", "children"),
    Output("summary-table", "data"),
    Output("summary-table", "columns"),
    Input("summary-table", "id")  # dummy input to trigger once
)
def update_summary(_):
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
        Avg_SOC_Gain=("soc_gain", "mean")
    ).reset_index().round(2)

    col_name_map = {
        "fleet_name": "Fleet Name",
        "charger_type": "Charger Type",
        "Events": "# of Events",
        "Energy_kWh": "Total Energy (kWh)",
        "Avg_Charging_Min": "Avg Charging (min)",
        "Avg_Connecting_Min": "Avg Connecting (min)",
        "Avg_Power_kW": "Avg Power Output (kW)",
        "Avg_SOC_Gain": "Avg SOC Gain (%)"
    }

    summary.rename(columns=col_name_map, inplace=True)
    columns = [{"name": col_name_map.get(col, col), "id": col} for col in summary.columns]
    data = summary.to_dict("records")

    return kpi1, kpi2, kpi3, kpi4, data, columns

# === Dropdown Options Callbacks ===

@callback(
    Output("fleet-filter", "options"),
    Input("fleet-filter", "id")
)
def populate_fleet_options(_):
    df = load_charging_data()
    fleets = sorted(df["fleet_name"].dropna().unique())
    return [{"label": f, "value": f} for f in fleets]

@callback(
    Output("charger-filter", "options"),
    Input("charger-filter", "id")
)
def populate_charger_options(_):
    df = load_charging_data()
    types = sorted(df["charger_type"].dropna().unique())
    return [{"label": t, "value": t} for t in types]

# === Figure Update Callback ===
@callback(
    Output("fig-power", "figure"),
    Output("fig-soc", "figure"),
    Output("fig-chg-time", "figure"),
    Output("fig-conn-time", "figure"),
    Input("fleet-filter", "value"),
    Input("charger-filter", "value"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date")
)
def update_figures(fleet_val, charger_val, start_date, end_date):
    df = load_charging_data()
    
    # Auto-limit to latest 30 days if nothing is selected
    if not any([fleet_val, charger_val, start_date, end_date]) and not df.empty:
        latest = df["date"].max()
        earliest = latest - pd.Timedelta(days=29)
        df = df[df["date"].between(earliest, latest)]
        
    if fleet_val:
        df = df[df["fleet_name"] == fleet_val]
    if charger_val:
        df = df[df["charger_type"] == charger_val]
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date).date()]

    if df.empty:
        return (
            empty_fig("No data available"),
            empty_fig("No data available"),
            empty_fig("No data available"),
            empty_fig("No data available")
        )

    daily = df.groupby("date").agg({
        "avg_power": "mean",
        "soc_gain": "mean",
        "charging_duration": "mean",
        "connecting_duration": "mean"
    }).reset_index()

    fig1 = px.bar(daily, x="date", y="avg_power", title="Average Power Output (kW)")
    fig1.update_layout(yaxis_title="Power (kW)")
    
    fig2 = px.bar(daily, x="date", y="soc_gain", title="Average SOC Gain (%)")
    fig2.update_layout(yaxis_title="SOC (%)")

    fig3 = px.bar(daily, x="date", y="charging_duration", title="Average Charging Duration (min)")
    fig3.update_layout(yaxis_title="Duration (min)")

    fig4 = px.bar(daily, x="date", y="connecting_duration", title="Average Connecting Duration (min)")
    fig4.update_layout(yaxis_title="Duration (min)")

    for fig in [fig1, fig2, fig3, fig4]:
        fig.update_layout(
            yaxis_title=None,
            plot_bgcolor=DARK_BG,
            paper_bgcolor=DARK_BG,
            font_color=TEXT_COLOR,
            xaxis=dict(gridcolor=GRID_COLOR),
            yaxis=dict(gridcolor=GRID_COLOR)
        )

    return fig1, fig2, fig3, fig4