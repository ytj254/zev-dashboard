from dash import register_page, html, dcc, dash_table, Input, Output, callback, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig

register_page(__name__, path="/veh_daily_usage")

metric_options = [
    {'label': 'Total Distance (mi)', 'value': 'tot_dist'},
    {'label': 'Total Energy (kWh)', 'value': 'tot_energy'},
    {'label': 'Initial SOC', 'value': 'init_soc'},
    {'label': 'Final SOC', 'value': 'final_soc'},
    {'label': 'SOC Used', 'value': 'tot_soc_used'},
    {'label': 'Idle Time (h)', 'value': 'idle_time'},
    {'label': 'Peak Payload (lbs)', 'value': 'peak_payload'},
]

def load_daily_usage_data():
    query = """
        SELECT f.fleet_name AS fleet, v.make, v.model, v.class, vd.*
        FROM veh_daily vd
        JOIN vehicle v ON vd.veh_id = v.fleet_vehicle_id
        JOIN fleet f ON v.fleet_id = f.id
    """
    df = pd.read_sql(query, engine)
    df["tot_soc_used"] = df["tot_soc_used"] * 100
    return df

layout = html.Div([
    dcc.Store(id="daily-usage-store", data=load_daily_usage_data().to_dict("records")),
    
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

@callback(Output('daily-usage-store', 'data'), Input('daily-usage-graph', 'id'))
def populate_store(_):
    df = load_daily_usage_data()
    return df.to_dict('records')

@callback(Output('fleet-dropdown', 'options'), Input('daily-usage-store', 'data'))
def load_fleet_options(records):
    df = pd.DataFrame(records)
    fleets = sorted(df["fleet"].dropna().unique())
    return [{'label': f, 'value': f} for f in fleets]

@callback(
    Output('make-dropdown', 'options'),
    Output('model-dropdown', 'options'),
    Output('vehicle-class-dropdown', 'options'),
    Output('fleet-veh-id-dropdown', 'options'),
    Input('fleet-dropdown', 'value'),
    State('daily-usage-store', 'data')
)

def update_filters(fleets, records):
    df = pd.DataFrame(records)
    if fleets:
        df = df[df["fleet"] == fleets]

    return (
        [{'label': x, 'value': x} for x in sorted(df['make'].dropna().unique())],
        [{'label': x, 'value': x} for x in sorted(df['model'].dropna().unique())],
        [{'label': x, 'value': x} for x in sorted(df['class'].dropna().unique())],
        [{'label': x, 'value': x} for x in sorted(df['veh_id'].dropna().unique())],
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
    Input('metric-dropdown', 'value'),
    State('daily-usage-store', 'data')
)

def update_figure(fleets, makes, models, classes, veh_ids, start_date, end_date, metric, records):
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date  # Ensure proper type

    if fleets: df = df[df["fleet"] == fleets]
    if makes: df = df[df["make"] == makes]
    if models: df = df[df["model"] == models]
    if classes: df = df[df["class"] == classes]
    if veh_ids: df = df[df["veh_id"] == veh_ids]
    if start_date: df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date: df = df[df["date"] <= pd.to_datetime(end_date).date()]

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
        xaxis=dict(gridcolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR)
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
    Input("fleet-summary-table", "id"),
    State("daily-usage-store", "data")   # <-- Add this line
)

def update_kpis_and_table(_, records):
    df = pd.DataFrame(records)
    if df.empty:
        return "0", "0", "0", "0", "0", [], []

    summary = []
    for fleet_name, group in df.groupby("fleet"):
        summary.append({
            "Fleet": fleet_name,
            "Total Distance (mi)": round(group["tot_dist"].sum(), 2),
            "Daily Distance (mi)": round(group[group["tot_dist"] > 0]["tot_dist"].mean(), 2),
            "Daily Energy (kWh)": round(group[group["tot_energy"] > 0]["tot_energy"].mean(), 2),
            "Daily SOC Used (%)": round(group[group["tot_soc_used"] > 0]["tot_soc_used"].mean(), 2),
            "Daily Driving Time (hr)": round(group[group["tot_dura"] > 0]["tot_dura"].mean(), 2),
            "Daily Idle Time (hr)": round(group["idle_time"].mean(), 2),
            "Daily Payload (lbs)": round(group["peak_payload"].mean(), 2),
        })

    df_summary = pd.DataFrame(summary)
    return (
        f"{df_summary['Total Distance (mi)'].sum():.2f}",
        f"{df_summary['Daily Distance (mi)'].mean():.2f}",
        f"{df_summary['Daily Energy (kWh)'].mean():.2f}",
        f"{df_summary['Daily Driving Time (hr)'].mean():.2f}",
        f"{df_summary['Daily SOC Used (%)'].mean():.2f}",
        df_summary.to_dict("records"),
        [{"name": col, "id": col} for col in df_summary.columns]
    )