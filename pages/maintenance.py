# maintenance.py
from dash import register_page, html, dcc, dash_table, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.express as px
from db import engine
from styles import DROPDOWN_STYLE, DARK_BG, GRID_COLOR, TEXT_COLOR, empty_fig

register_page(__name__, path="/maintenance", name="Maintenance")

# ---------- Data load ----------
def load_maintenance():
    query = """
        SELECT
            m.*,
            v.fleet_id AS v_fleet_id,
            c.fleet_id AS c_fleet_id,
            f1.fleet_name AS v_fleet_name,
            f2.fleet_name AS c_fleet_name,
            v.fleet_vehicle_id,
            c.charger
        FROM maintenance m
        LEFT JOIN vehicle v ON m.vehicle_id = v.id
        LEFT JOIN fleet   f1 ON v.fleet_id = f1.id
        LEFT JOIN charger c ON m.charger_id = c.id
        LEFT JOIN fleet   f2 ON c.fleet_id = f2.id
    """
    df = pd.read_sql(query, engine)

    # Resolve fleet info (maintenance can come via vehicle or charger)
    df["fleet_id"] = df["v_fleet_id"].fillna(df["c_fleet_id"])
    df["fleet_name"] = df["v_fleet_name"].fillna(df["c_fleet_name"])

    # Labels for selectors
    df["vehicle_label"] = df["fleet_vehicle_id"]
    df["charger_label"] = df["charger"]

    # Dates (filters use m.date only, per spec)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Total cost only where ALL three costs present
    for col in ["parts_cost", "labor_cost", "add_cost"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    all_costs_present = df[["parts_cost", "labor_cost", "add_cost"]].notna().all(axis=1)
    df["total_cost"] = np.where(all_costs_present,
                                df["parts_cost"] + df["labor_cost"] + df["add_cost"],
                                np.nan)

    # Normalize location buckets: in-house vs outsourced; NULL -> Unknown
    def map_loc(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "Unknown"
        s = str(x).strip().lower()
        return "In-house" if s == "in-house" else "Outsourced"
    df["maint_loc_bucket"] = df["maint_loc"].apply(map_loc)

    # Warranty bucket
    df["warranty_bucket"] = df["warranty"].map({True: "Yes", False: "No"}).fillna("No")

    # Category (stringify for safety)
    df["maint_categ"] = df["maint_categ"].astype(str).replace({"None": np.nan}).fillna("Unspecified")

    # Odometer for KPI
    for col in ["enter_odo", "exit_odo"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


_df = load_maintenance()

# ---------- KPI helpers ----------
def avg_miles_between_services(df_scope: pd.DataFrame) -> float:
    """
    Per-vehicle: sort by date, diff positive enter_odo; mean per vehicle; then global mean of those means.
    """
    if df_scope.empty:
        return float("nan")
    # Only rows with vehicle_id and enter_odo + date
    d = df_scope.dropna(subset=["vehicle_id", "enter_odo", "date"]).copy()
    if d.empty:
        return float("nan")

    def per_vehicle_avg(g):
        g = g.sort_values("date")
        deltas = g["enter_odo"].diff()
        deltas = deltas[deltas > 0]
        return deltas.mean() if len(deltas) else np.nan

    per_veh = d.groupby("vehicle_id", dropna=True).apply(per_vehicle_avg)
    per_veh = per_veh.dropna()
    return float(per_veh.mean()) if len(per_veh) else float("nan")


# ---------- Block 1 (GLOBAL, not filter-aware) ----------
def kpi_block_global(df_all: pd.DataFrame):
    total_events = len(df_all)
    veh_events = int(df_all["vehicle_id"].notna().sum())
    chg_events = int(df_all["charger_id"].notna().sum())
    avg_total_cost = float(df_all["total_cost"].mean(skipna=True))
    avg_miles_between = avg_miles_between_services(df_all)

    def kpi_card(title, value, fmt=None):
        if fmt == "money" and pd.notna(value):
            val = f"${value:,.0f}"
        elif fmt == "miles" and pd.notna(value):
            val = f"{value:,.0f} mi"
        else:
            val = "—" if pd.isna(value) else f"{value:,}"
        return dbc.Card(
            dbc.CardBody([
                html.Div(title, className="text-muted"),
                html.H3(val, className="mb-0"),
            ]),
            className="shadow-sm rounded-2xl",
            style={"backgroundColor": DARK_BG}
        )

    return dbc.Row([
        dbc.Col(kpi_card("Total Maintenance Events", total_events)),
        dbc.Col(kpi_card("Vehicle Maintenance Events", veh_events)),
        dbc.Col(kpi_card("Charger Maintenance Events", chg_events)),
        dbc.Col(kpi_card("Average Total Cost (per event)", avg_total_cost, fmt="money")),
        dbc.Col(kpi_card("Avg Miles Between Services", avg_miles_between, fmt="miles")),
    ], className="row row-cols-1 row-cols-md-5 g-3")


# ---------- Block 2 (Fleet table – filter-aware) ----------
def compute_fleet_table(df_scope: pd.DataFrame) -> pd.DataFrame:
    if df_scope.empty:
        return pd.DataFrame(columns=[
            "Fleet", "Events", "Vehicle events", "Charger events",
            "Total cost", "Avg total cost", "Avg parts cost", "Avg labor cost", "Avg added cost",
            "Avg miles between services"
        ])

    # Cost-valid subset: all three costs present
    valid_cost_mask = df_scope[["parts_cost", "labor_cost", "add_cost"]].notna().all(axis=1)
    df_cost = df_scope[valid_cost_mask].copy()
    df_cost["total_cost"] = df_cost["parts_cost"] + df_cost["labor_cost"] + df_cost["add_cost"]

    # Avg miles between services per fleet
    def fleet_avg_miles(g):
        return avg_miles_between_services(g)

    grp = df_scope.groupby("fleet_name", dropna=False)

    rows = []
    for fleet, g in grp:
        fleet_name = fleet if pd.notna(fleet) else "Unspecified"
        g_cost = df_cost[df_cost["fleet_name"] == fleet]
        events = len(g)
        veh_events = int(g["vehicle_id"].notna().sum())
        chg_events = int(g["charger_id"].notna().sum())

        total_cost = float(g_cost["total_cost"].sum()) if not g_cost.empty else np.nan
        avg_total = float(g_cost["total_cost"].mean()) if not g_cost.empty else np.nan
        avg_parts = float(g_cost["parts_cost"].mean()) if not g_cost.empty else np.nan
        avg_labor = float(g_cost["labor_cost"].mean()) if not g_cost.empty else np.nan
        avg_added = float(g_cost["add_cost"].mean()) if not g_cost.empty else np.nan

        avg_miles = fleet_avg_miles(g)

        rows.append({
            "Fleet": fleet_name,
            "Events": events,
            "Vehicle events": veh_events,
            "Charger events": chg_events,
            "Total cost": total_cost,
            "Avg total cost": avg_total,
            "Avg parts cost": avg_parts,
            "Avg labor cost": avg_labor,
            "Avg added cost": avg_added,
            "Avg miles between services": avg_miles
        })

    out = pd.DataFrame(rows)
    # Sort by Fleet for consistency
    out = out.sort_values("Fleet", na_position="last")
    return out


def fleet_table_component():
    return dash_table.DataTable(
        id="maint-fleet-table",
        columns=[
            {"name": "Fleet", "id": "Fleet"},
            {"name": "Events", "id": "Events", "type": "numeric"},
            {"name": "Vehicle events", "id": "Vehicle events", "type": "numeric"},
            {"name": "Charger events", "id": "Charger events", "type": "numeric"},
            {"name": "Total cost", "id": "Total cost", "type": "numeric", "format": {"locale": {"symbol": ["$", ""]}}},
            {"name": "Avg total cost", "id": "Avg total cost", "type": "numeric", "format": {"locale": {"symbol": ["$", ""]}}},
            {"name": "Avg parts cost", "id": "Avg parts cost", "type": "numeric", "format": {"locale": {"symbol": ["$", ""]}}},
            {"name": "Avg labor cost", "id": "Avg labor cost", "type": "numeric", "format": {"locale": {"symbol": ["$", ""]}}},
            {"name": "Avg added cost", "id": "Avg added cost", "type": "numeric", "format": {"locale": {"symbol": ["$", ""]}}},
            {"name": "Avg miles between services", "id": "Avg miles between services", "type": "numeric"},
        ],
        data=[],
        style_table={"overflowX": "auto"},
        style_cell={"backgroundColor": DARK_BG, "color": TEXT_COLOR, "border": f"1px solid {GRID_COLOR}"},
        style_header={"fontWeight": "600"},
        sort_action="native",
        page_size=20,
    )


# ---------- Block 3 (Pies + Filters) ----------
def group_small_slices(df_counts: pd.DataFrame, label_col: str, count_col: str, threshold=0.01):
    """
    Group categories whose share < threshold into 'Other'
    """
    if df_counts.empty:
        return df_counts
    total = df_counts[count_col].sum()
    if total <= 0:
        return df_counts
    df = df_counts.copy()
    df["share"] = df[count_col] / total
    small_mask = df["share"] < threshold
    if small_mask.any():
        other_sum = df.loc[small_mask, count_col].sum()
        df = df.loc[~small_mask, [label_col, count_col]]
        df = pd.concat([df, pd.DataFrame([{label_col: "Other", count_col: other_sum}])], ignore_index=True)
    else:
        df = df[[label_col, count_col]]
    return df


def make_pie(fig_title, labels, values):
    if values.sum() == 0:
        return empty_fig(fig_title)
    fig = px.pie(names=labels, values=values, title=fig_title, hole=0.35)
    fig.update_layout(
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG, font_color=TEXT_COLOR,
        legend_title=None, margin=dict(l=10, r=10, t=40, b=10)
    )
    return fig


# ---------- Layout ----------
def layout():
    # Top-level filters (apply to Block 2 + Block 3; Block 1 stays global)
    fleets = (
        _df[["fleet_name"]].dropna().drop_duplicates().sort_values("fleet_name")["fleet_name"].tolist()
    )

    return dbc.Container([
        # html.H2("Maintenance"),
        html.Div(kpi_block_global(_df), className="mb-4"),

        # Block 2: Fleet table
        html.H4("Fleet Summary"),
        fleet_table_component(),
        html.Hr(),

        # Filters
        dbc.Card(dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Fleet", className="fw-semibold d-block"),
                    dcc.Dropdown(
                        id="maint-filter-fleet",
                        options=[{"label": f, "value": f} for f in fleets],
                        placeholder="All fleets",
                        style=DROPDOWN_STYLE,
                        multi=True,
                        value=None
                    )
                ], md=3),

                dbc.Col([
                    html.Label("Asset type", className="fw-semibold d-block"),
                    dcc.RadioItems(
                        id="maint-filter-asset-type",
                        options=[{"label": "Vehicle", "value": "vehicle"},
                                {"label": "Charger", "value": "charger"}],
                        value="vehicle",
                        labelStyle={"display": "block", "margin-bottom": "4px"}  # vertical layout with spacing
                    )
                ], md=2),

                dbc.Col([
                    html.Label("Asset ID", className="fw-semibold d-block"),
                    dcc.Dropdown(
                        id="maint-filter-asset-id",
                        options=[],
                        placeholder="All IDs",
                        style=DROPDOWN_STYLE,
                        multi=True,
                        value=None
                    )
                ], md=4),

                dbc.Col([
                    html.Label("Date range", className="fw-semibold d-block"),
                    html.Div(
                        dcc.DatePickerRange(
                            id="maint-filter-daterange",
                            min_date_allowed=_df["date"].min(),
                            max_date_allowed=_df["date"].max(),
                            start_date=None,
                            end_date=None,
                            clearable=True,
                            display_format="YYYY-MM-DD"
                        ),
                        style={"display": "flex", "flexDirection": "column"}
                    )
                ], md=3),
            ], className="g-3"),
        ]), className="mb-4 shadow-sm rounded-2xl", style={"backgroundColor": DARK_BG}),
        
        # Block 3: Pies
        html.H4("Maintenance Composition"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="maint-pie-category", figure=empty_fig("By Category")), md=4),
            dbc.Col(dcc.Graph(id="maint-pie-warranty", figure=empty_fig("By Warranty")), md=4),
            dbc.Col(dcc.Graph(id="maint-pie-location", figure=empty_fig("By Maintenance Location")), md=4),
        ], className="g-3 mb-5"),
    ], fluid=True)


# ---------- Callbacks ----------
@callback(
    Output("maint-filter-asset-id", "options"),
    Input("maint-filter-asset-type", "value"),
    Input("maint-filter-fleet", "value"),
)
def populate_asset_ids(asset_type, fleets_sel):
    df = _df
    # Filter to selected fleets (optional). If empty/None, show ALL IDs across fleets (per spec).
    if fleets_sel:
        if isinstance(fleets_sel, list):
            df = df[df["fleet_name"].isin(fleets_sel)]
        else:
            df = df[df["fleet_name"] == fleets_sel]

    if asset_type == "vehicle":
        opts = (
            df.dropna(subset=["vehicle_id"])
              .drop_duplicates(subset=["vehicle_id"])
              .assign(label=lambda x: x["vehicle_label"].fillna(x["vehicle_id"].astype(str)))
        )
        opts = [{"label": str(row["label"]), "value": int(row["vehicle_id"])} for _, row in opts.iterrows()]
    else:
        opts = (
            df.dropna(subset=["charger_id"])
              .drop_duplicates(subset=["charger_id"])
              .assign(label=lambda x: x["charger_label"].fillna(x["charger_id"].astype(str)))
        )
        opts = [{"label": str(row["label"]), "value": int(row["charger_id"])} for _, row in opts.iterrows()]
    # If none found, return empty list
    return opts


def apply_filters(df, fleets_sel, asset_type, asset_ids, start_date, end_date):
    d = df.copy()

    # Fleet filter (optional)
    if fleets_sel:
        if isinstance(fleets_sel, list):
            d = d[d["fleet_name"].isin(fleets_sel)]
        else:
            d = d[d["fleet_name"] == fleets_sel]

    # Asset type + IDs (optional)
    if asset_type == "vehicle":
        if asset_ids:
            d = d[d["vehicle_id"].isin(asset_ids)]
    elif asset_type == "charger":
        if asset_ids:
            d = d[d["charger_id"].isin(asset_ids)]

    # Date range (uses m.date)
    if start_date:
        d = d[d["date"] >= pd.to_datetime(start_date)]
    if end_date:
        d = d[d["date"] <= pd.to_datetime(end_date)]

    return d


@callback(
    Output("maint-fleet-table", "data"),
    Output("maint-pie-category", "figure"),
    Output("maint-pie-warranty", "figure"),
    Output("maint-pie-location", "figure"),
    Input("maint-filter-fleet", "value"),
    Input("maint-filter-asset-type", "value"),
    Input("maint-filter-asset-id", "value"),
    Input("maint-filter-daterange", "start_date"),
    Input("maint-filter-daterange", "end_date"),
)
def update_block2_block3(fleets_sel, asset_type, asset_ids, start_date, end_date):
    # Normalize asset_ids to list
    if asset_ids and not isinstance(asset_ids, list):
        asset_ids = [asset_ids]

    d = apply_filters(_df, fleets_sel, asset_type, asset_ids, start_date, end_date)

    # ---- Block 2: Fleet table ----
    tbl = compute_fleet_table(d)
    # Format money columns for display
    money_cols = ["Total cost", "Avg total cost", "Avg parts cost", "Avg labor cost", "Avg added cost"]
    for c in money_cols:
        if c in tbl:
            tbl[c] = tbl[c].apply(lambda x: None if pd.isna(x) else round(float(x), 0))
    if "Avg miles between services" in tbl:
        tbl["Avg miles between services"] = tbl["Avg miles between services"].apply(
            lambda x: None if pd.isna(x) else round(float(x), 0)
        )

    # ---- Block 3: Pies ----
    # Category
    cat_counts = d.groupby("maint_categ", dropna=False).size().reset_index(name="count").rename(columns={"maint_categ": "label"})
    cat_counts = group_small_slices(cat_counts, "label", "count", threshold=0.01)
    fig_cat = make_pie("By Category", labels=cat_counts["label"], values=cat_counts["count"]) if not cat_counts.empty else empty_fig("By Category")

    # Warranty
    war_counts = d.groupby("warranty_bucket", dropna=False).size().reset_index(name="count").rename(columns={"warranty_bucket": "label"})
    fig_war = make_pie("By Warranty", labels=war_counts["label"], values=war_counts["count"]) if not war_counts.empty else empty_fig("By Warranty")

    # Maintenance location
    loc_counts = d.groupby("maint_loc_bucket", dropna=False).size().reset_index(name="count").rename(columns={"maint_loc_bucket": "label"})
    fig_loc = make_pie("By Maintenance Location", labels=loc_counts["label"], values=loc_counts["count"]) if not loc_counts.empty else empty_fig("By Maintenance Location")

    return tbl.to_dict("records"), fig_cat, fig_war, fig_loc
