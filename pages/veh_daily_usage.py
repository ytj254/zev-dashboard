from dash import register_page, html, dcc, dash_table, Input, Output, callback, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db import engine
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig

register_page(__name__, path="/veh_daily_usage", name="Vehicle Daily Usage")

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
        SELECT f.fleet_name AS fleet, v.make, v.model, v.class, v.fleet_vehicle_id, vd.*
        FROM veh_daily vd
        JOIN vehicle v ON vd.veh_id = v.id
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
        dcc.Graph(id='daily-usage-graph'),
        html.Div(style={"height": "20px"}),
        dcc.Graph(id='efficiency-graph')
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
        [{'label': x, 'value': x} for x in sorted(df['fleet_vehicle_id'].dropna().unique())],
    )

def _filter_daily(records, fleets, makes, models, classes, veh_ids, start_date, end_date):
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    if fleets: df = df[df["fleet"] == fleets]
    if makes: df = df[df["make"] == makes]
    if models: df = df[df["model"] == models]
    if classes: df = df[df["class"] == classes]
    if veh_ids: df = df[df["fleet_vehicle_id"] == veh_ids]
    if start_date: df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date: df = df[df["date"] <= pd.to_datetime(end_date).date()]
    return df


@callback(
    Output('daily-usage-graph', 'figure'),
    Output('efficiency-graph', 'figure'),
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
def update_figures(fleets, makes, models, classes, veh_ids, start_date, end_date, metric, records):
    df = _filter_daily(records, fleets, makes, models, classes, veh_ids, start_date, end_date)

    if df.empty or metric not in df.columns:
        return empty_fig("No data available"), empty_fig("No data available")

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

    eff_source = df[(df["tot_dist"] >= 10) & (df["tot_energy"] > 0)]
    eff_df = (
        eff_source.groupby("date")
        .agg({"tot_energy": "sum", "tot_dist": "sum"})
        .reset_index()
    )
    eff_df["efficiency"] = eff_df.apply(
        lambda r: r.tot_energy / r.tot_dist if pd.notna(r.tot_energy) and pd.notna(r.tot_dist) and r.tot_dist != 0 else None,
        axis=1
    )
    eff_df = eff_df[pd.notna(eff_df["efficiency"])]

    if eff_df.empty:
        eff_fig = empty_fig("No efficiency data")
    else:
        eff_fig = px.bar(eff_df, x="date", y="efficiency", title="Energy Efficiency (kWh/mi)")
        eff_fig.update_layout(
            yaxis_title="kWh/mi",
            xaxis_title="Date",
            paper_bgcolor=DARK_BG,
            plot_bgcolor=DARK_BG,
            font_color=TEXT_COLOR,
            xaxis=dict(gridcolor=GRID_COLOR),
            yaxis=dict(gridcolor=GRID_COLOR)
        )

    return fig, eff_fig

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

    def safe_mean(series):
        s = series.dropna()
        return s.mean() if not s.empty else None

    def safe_eff(group):
        eff_src = group[(group["tot_dist"] > 0) & (group["tot_energy"] > 0)]
        dist_sum = eff_src["tot_dist"].sum(skipna=True)
        if dist_sum and dist_sum > 0:
            energy_sum = eff_src["tot_energy"].sum(skipna=True)
            return energy_sum / dist_sum
        return None

    summary = []
    for fleet_name, group in df.groupby("fleet"):
        dist_positive = group[group["tot_dist"] > 0]["tot_dist"]
        energy_positive = group[group["tot_energy"] > 0]["tot_energy"]
        dura_positive = group[group["tot_dura"] > 0]["tot_dura"]
        summary.append({
            "Fleet": fleet_name,
            "Total Distance (mi)": round(group["tot_dist"].sum(), 2),
            "Daily Distance (mi)": safe_mean(dist_positive),
            "Daily Energy (kWh)": safe_mean(energy_positive),
            "Energy Efficiency (kWh/mi)": safe_eff(group),
            "Daily SOC Used (%)": safe_mean(group[group["tot_soc_used"] > 0]["tot_soc_used"]),
            "Daily Driving Time (hr)": safe_mean(dura_positive),
            "Daily Idle Time (hr)": safe_mean(group["idle_time"]),
        })

    df_summary = pd.DataFrame(summary)

    # Prepare table display with placeholders
    def fmt(val, digits=2):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "n/a"
        try:
            return f"{val:.{digits}f}"
        except Exception:
            return val

    table_df = df_summary.copy()
    for col in table_df.columns:
        if col == "Fleet":
            table_df[col] = table_df[col].fillna("-")
        else:
            table_df[col] = table_df[col].apply(fmt)

    df_summary_kpi = df_summary.fillna(0)
    return (
        f"{df_summary_kpi['Total Distance (mi)'].sum():.2f}",
        f"{df_summary_kpi['Daily Distance (mi)'].mean():.2f}",
        f"{df_summary_kpi['Daily Energy (kWh)'].mean():.2f}",
        f"{df_summary_kpi['Daily Driving Time (hr)'].mean():.2f}",
        f"{df_summary_kpi['Daily SOC Used (%)'].mean():.2f}",
        table_df.to_dict("records"),
        [{"name": col, "id": col} for col in table_df.columns]
    )
