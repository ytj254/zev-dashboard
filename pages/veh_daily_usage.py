from dash import register_page, html, dcc, dash_table, Input, Output, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from utils import empty_fig

register_page(__name__, path="/veh_daily_usage")

DARK_BG = "#303030"
TEXT_COLOR = "white"
BAR_COLOR = "#5bc0de"

metric_options = [
    {'label': 'Total Distance (mi)', 'value': 'tot_dist'},
    {'label': 'Total Energy (kWh)', 'value': 'tot_energy'},
    {'label': 'Initial SOC', 'value': 'init_soc'},
    {'label': 'Final SOC', 'value': 'final_soc'},
    {'label': 'SOC Used', 'value': 'tot_soc_used'},
    {'label': 'Idle Time (h)', 'value': 'idle_time'},
    {'label': 'Peak Payload (lbs)', 'value': 'peak_payload'},
]

DROPDOWN_STYLE = {"width": "250px", "color": "black", "backgroundColor": "white"}

def load_daily_usage_data():
    query = """
        SELECT f.fleet_name AS fleet, vd.*
        FROM veh_daily vd
        JOIN vehicle v ON vd.veh_id = v.fleet_vehicle_id
        JOIN fleet f ON v.fleet_id = f.id
    """
    df = pd.read_sql(query, engine)
    df["tot_soc_used"] = df["tot_soc_used"] * 100
    return df

layout = html.Div([
    # KPI cards
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Total Distance (mi)"), html.H4(id="kpi-total-distance")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Daily Distance (mi)"), html.H4(id="kpi-daily-distance")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Daily Energy (kWh)"), html.H4(id="kpi-daily-energy")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Daily Driving Time (hr)"), html.H4(id="kpi-daily-driving")]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Daily SOC Used (%)"), html.H4(id="kpi-daily-soc")]))),
    ], className="mb-4"),

    html.Div([
        html.H4("Daily Usage Summary by Fleet", style={"color": TEXT_COLOR}),
        dash_table.DataTable(
            id='fleet-summary-table',
            style_table={'overflowX': 'auto'},
            style_cell={'color': 'white', 'backgroundColor': DARK_BG},
            style_header={'backgroundColor': '#1f1f1f', 'color': 'white', 'fontWeight': 'bold'}
        )
    ], style={"marginBottom": "2rem"}),

    # Filters
    html.Div([
        dcc.Dropdown(id='fleet-dropdown', placeholder='Select Fleet', style=DROPDOWN_STYLE),
        dcc.Dropdown(id='make-dropdown', placeholder='Select Make', style=DROPDOWN_STYLE),
        dcc.Dropdown(id='model-dropdown', placeholder='Select Model', style=DROPDOWN_STYLE),
        dcc.Dropdown(id='vehicle-class-dropdown', placeholder='Select Vehicle Class', style=DROPDOWN_STYLE),
        dcc.Dropdown(id='fleet-veh-id-dropdown', placeholder='Select Fleet Vehicle ID', style=DROPDOWN_STYLE),
        dcc.DatePickerRange(id='date-range-picker'),
        dcc.Dropdown(id='metric-dropdown', options=metric_options, value='tot_dist',
                     placeholder='Select Metric', style=DROPDOWN_STYLE)
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'gap': '10px', 'marginBottom': '20px'}),

    html.Div([
        html.H5("Daily Metric by Date", style={"color": TEXT_COLOR}),
        dcc.Graph(id='daily-usage-graph')
    ])
])

@callback(Output('fleet-dropdown', 'options'), Input('fleet-dropdown', 'id'))
def load_fleet_options(_):
    df = pd.read_sql("SELECT DISTINCT fleet_name FROM fleet", engine)
    return [{'label': f, 'value': f} for f in df['fleet_name']]

@callback(
    Output('make-dropdown', 'options'),
    Output('model-dropdown', 'options'),
    Output('vehicle-class-dropdown', 'options'),
    Output('fleet-veh-id-dropdown', 'options'),
    Input('fleet-dropdown', 'value')
)
def update_vehicle_filters(fleet_name):
    if not fleet_name:
        return [], [], [], []
    query = """
        SELECT DISTINCT v.make, v.model, v.class, v.fleet_vehicle_id
        FROM vehicle v JOIN fleet f ON v.fleet_id = f.id
        WHERE f.fleet_name = %s
    """
    df = pd.read_sql(query, engine, params=(fleet_name,))
    return (
        [{'label': m, 'value': m} for m in df['make'].dropna().unique()],
        [{'label': m, 'value': m} for m in df['model'].dropna().unique()],
        [{'label': c, 'value': c} for c in df['class'].dropna().unique()],
        [{'label': i, 'value': i} for i in df['fleet_vehicle_id'].dropna().unique()],
    )

@callback(
    Output('daily-usage-graph', 'figure'),
    Input('fleet-dropdown', 'value'),
    Input('make-dropdown', 'value'),
    Input('model-dropdown', 'value'),
    Input('vehicle-class-dropdown', 'value'),
    Input('fleet-veh-id-dropdown', 'value'),
    Input('date-range-picker', 'start_date'),
    Input('date-range-picker', 'end_date'),
    Input('metric-dropdown', 'value')
)
def update_daily_usage(fleet, make, model, vehicle_class, fleet_vehicle_id, start_date, end_date, metric):
    df = load_daily_usage_data()
    if metric is None or df.empty:
        return empty_fig("Please select a metric")

    filters = []
    if fleet: df = df[df["fleet"] == fleet]
    if make: df = df[df["make"] == make]
    if model: df = df[df["model"] == model]
    if vehicle_class: df = df[df["class"] == vehicle_class]
    if fleet_vehicle_id: df = df[df["veh_id"] == fleet_vehicle_id]
    if start_date: df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date: df = df[df["date"] <= pd.to_datetime(end_date)]

    if df.empty or metric not in df.columns:
        return empty_fig("No data available")

    y_label = next((opt['label'] for opt in metric_options if opt['value'] == metric), metric)
    fig = px.bar(df, x="date", y=metric, title=f"{y_label} per Day")
    fig.update_layout(
        yaxis_title=None,
        xaxis_title="Date",
        paper_bgcolor=DARK_BG,
        plot_bgcolor=DARK_BG,
        font_color=TEXT_COLOR,
        xaxis=dict(gridcolor="#444444"),
        yaxis=dict(gridcolor="#444444")
    )
    return fig

@callback(
    Output("kpi-total-distance", "children"),
    Output("kpi-daily-distance", "children"),
    Output("kpi-daily-energy", "children"),
    Output("kpi-daily-driving", "children"),
    Output("kpi-daily-soc", "children"),
    Output("fleet-summary-table", "data"),
    Output("fleet-summary-table", "columns"),
    Input("fleet-summary-table", "id")
)
def update_summary_table_and_kpis(_):
    df = load_daily_usage_data()
    if df.empty:
        return "0", "0", "0", "0", "0", [], []

    summary = []
    for fleet_name, group in df.groupby("fleet"):
        total_distance = group["tot_dist"].sum()
        avg_distance = group[group["tot_dist"] > 0]["tot_dist"].mean()
        avg_energy = group[group["tot_energy"] > 0]["tot_energy"].mean()
        avg_soc = group[group["tot_soc_used"] > 0]["tot_soc_used"].mean()
        avg_drive = group[group["tot_dura"] > 0]["tot_dura"].mean()
        avg_idle = group["idle_time"].mean()
        avg_payload = group["peak_payload"].mean()
        summary.append({
            "Fleet": fleet_name,
            "Total Distance (mi)": round(total_distance, 2),
            "Daily Distance (mi)": round(avg_distance, 2),
            "Daily Energy (kWh)": round(avg_energy, 2),
            "Daily SOC Used (%)": round(avg_soc, 2),
            "Daily Driving Time (hr)": round(avg_drive, 2),
            "Daily Idle Time (hr)": round(avg_idle, 2),
            "Daily Payload (lbs)": round(avg_payload, 2),
        })

    df_summary = pd.DataFrame(summary)
    kpi1 = df_summary["Total Distance (mi)"].sum()
    kpi2 = df_summary["Daily Distance (mi)"].mean()
    kpi3 = df_summary["Daily Energy (kWh)"].mean()
    kpi4 = df_summary["Daily Driving Time (hr)"].mean()
    kpi5 = df_summary["Daily SOC Used (%)"].mean()
    columns = [{"name": col, "id": col} for col in df_summary.columns]
    return (
        f"{kpi1:.2f}", f"{kpi2:.2f}", f"{kpi3:.2f}", f"{kpi4:.2f}", f"{kpi5:.2f}",
        df_summary.to_dict("records"), columns
    )