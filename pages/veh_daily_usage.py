from dash import register_page, html, dcc, Input, Output, callback, State
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
    {'label': 'Energy Efficiency (kWh/mi)', 'value': 'efficiency'},
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
        html.Div(id="fleet-summary-table")
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


def _resolve_efficiency_rows(df):
    out = df.copy()
    dist = pd.to_numeric(out.get("tot_dist"), errors="coerce")
    energy = pd.to_numeric(out.get("tot_energy"), errors="coerce")
    reported = (
        pd.to_numeric(out["efficiency"], errors="coerce")
        if "efficiency" in out.columns
        else pd.Series(index=out.index, dtype="float64")
    )
    # Reference rule: apply distance filter first for all efficiency paths.
    # Only rows with tot_dist >= 10 are eligible for efficiency.
    dist_ok = dist >= 10
    computed = pd.Series(index=out.index, dtype="float64")
    # Fallback rule: compute efficiency only when not reported and inputs are valid.
    # efficiency = tot_energy / tot_dist, requiring positive energy and valid distance.
    compute_mask = dist_ok & (energy > 0) & pd.notna(energy) & pd.notna(dist)
    computed.loc[compute_mask] = energy.loc[compute_mask] / dist.loc[compute_mask]
    # Preferred source rule: use reported efficiency when present; else use computed.
    out["efficiency_resolved"] = reported.where(dist_ok & pd.notna(reported), computed)
    out["tot_dist_num"] = dist
    # Shared validity flag used by daily chart and fleet summary table.
    out["efficiency_valid"] = dist_ok & pd.notna(out["efficiency_resolved"])
    return out


def _build_daily_efficiency(df):
    eff_rows = _resolve_efficiency_rows(df)
    eff_rows = eff_rows[eff_rows["efficiency_valid"]].copy()
    if eff_rows.empty:
        return pd.DataFrame(columns=["date", "efficiency"])

    # Aggregate rule: distance-weighted daily efficiency.
    eff_rows["weighted_eff_num"] = eff_rows["efficiency_resolved"] * eff_rows["tot_dist_num"]
    eff_df = (
        eff_rows.groupby("date", as_index=False)
        .agg({"weighted_eff_num": "sum", "tot_dist_num": "sum"})
    )
    eff_df["efficiency"] = eff_df["weighted_eff_num"] / eff_df["tot_dist_num"]
    return eff_df[["date", "efficiency"]]


def _safe_eff(group):
    eff_rows = _resolve_efficiency_rows(group)
    eff_rows = eff_rows[eff_rows["efficiency_valid"]]
    if eff_rows.empty:
        return None
    dist_sum = eff_rows["tot_dist_num"].sum(skipna=True)
    if dist_sum and dist_sum > 0:
        # Same aggregation rule as daily efficiency chart (distance-weighted).
        return (eff_rows["efficiency_resolved"] * eff_rows["tot_dist_num"]).sum(skipna=True) / dist_sum
    return None


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

    if df.empty or (metric != "efficiency" and metric not in df.columns):
        return empty_fig("No data available"), empty_fig("No data available")

    y_label = next((opt['label'] for opt in metric_options if opt['value'] == metric), metric)
    eff_df = _build_daily_efficiency(df)

    if metric == "efficiency":
        if eff_df.empty:
            return empty_fig("No efficiency data"), empty_fig("No efficiency data")
        fig_source = eff_df
    else:
        fig_source = df

    fig = px.bar(fig_source, x="date", y=metric, title=f"{y_label} per Day")
    fig.update_layout(
        yaxis_title=None,
        xaxis_title="Date",
        paper_bgcolor=DARK_BG,
        plot_bgcolor=DARK_BG,
        font_color=TEXT_COLOR,
        xaxis=dict(gridcolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR)
    )

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
    Output("fleet-summary-table", "children"),
    Input("fleet-summary-table", "id"),
    State("daily-usage-store", "data")   # <-- Add this line
)

def update_kpis_and_table(_, records):
    header_style = {"padding": "0.3rem 0.45rem", "fontSize": "0.82rem", "whiteSpace": "nowrap"}
    cell_style = {"padding": "0.22rem 0.45rem", "fontSize": "0.82rem", "lineHeight": "1.15"}
    df = pd.DataFrame(records)
    if df.empty:
        return "0", "0", "0", "0", "0", dbc.Table(
            [html.Tbody([html.Tr([html.Td("No data available", colSpan=8, style={**cell_style, "textAlign": "center"})])])],
            bordered=True,
            hover=True,
            responsive=True,
            className="table table-dark mb-0",
            size="sm",
        )

    def safe_mean(series):
        s = series.dropna()
        return s.mean() if not s.empty else None

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
            "Energy Efficiency (kWh/mi)": _safe_eff(group),
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
            table_df[col] = table_df[col].fillna("n/a")
        else:
            table_df[col] = table_df[col].apply(fmt)

    header = html.Thead(html.Tr([html.Th(col, style=header_style) for col in table_df.columns]))
    body = html.Tbody([
        html.Tr([html.Td(row[col], style=cell_style) for col in table_df.columns])
        for _, row in table_df.iterrows()
    ])
    table_ui = dbc.Table(
        [header, body],
        bordered=True,
        hover=True,
        responsive=True,
        className="table table-dark mb-0",
        size="sm",
    )

    df_summary_kpi = df_summary.fillna(0)
    return (
        f"{df_summary_kpi['Total Distance (mi)'].sum():.2f}",
        f"{df_summary_kpi['Daily Distance (mi)'].mean():.2f}",
        f"{df_summary_kpi['Daily Energy (kWh)'].mean():.2f}",
        f"{df_summary_kpi['Daily Driving Time (hr)'].mean():.2f}",
        f"{df_summary_kpi['Daily SOC Used (%)'].mean():.2f}",
        table_ui,
    )
