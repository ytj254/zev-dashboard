# maintenance.py
from dash import register_page, html, dcc, Input, Output, State, callback, no_update
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
        LEFT JOIN vehicle v ON m.veh_id = v.id
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
    d = df_scope.dropna(subset=["veh_id", "enter_odo", "date"]).copy()
    if d.empty:
        return float("nan")

    def per_vehicle_avg(g):
        g = g.sort_values("date")
        deltas = g["enter_odo"].diff()
        deltas = deltas[deltas > 0]
        return deltas.mean() if len(deltas) else np.nan

    per_veh = d.groupby("veh_id", dropna=True).apply(per_vehicle_avg)
    per_veh = per_veh.dropna()
    return float(per_veh.mean()) if len(per_veh) else float("nan")


# ---------- Block 1 (GLOBAL, not filter-aware) ----------
def kpi_block_global(df_all: pd.DataFrame):
    total_events = len(df_all)
    veh_events = int(df_all["veh_id"].notna().sum())
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
            "Fleet", "Asset type", "Events",
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
        # Vehicle row
        g_vehicle = g[g["veh_id"].notna()]
        g_vehicle_cost = df_cost[(df_cost["fleet_name"] == fleet) & (df_cost["veh_id"].notna())]
        rows.append({
            "Fleet": fleet_name,
            "Asset type": "Vehicle",
            "Events": len(g_vehicle),
            "Total cost": float(g_vehicle_cost["total_cost"].sum()) if not g_vehicle_cost.empty else np.nan,
            "Avg total cost": float(g_vehicle_cost["total_cost"].mean()) if not g_vehicle_cost.empty else np.nan,
            "Avg parts cost": float(g_vehicle_cost["parts_cost"].mean()) if not g_vehicle_cost.empty else np.nan,
            "Avg labor cost": float(g_vehicle_cost["labor_cost"].mean()) if not g_vehicle_cost.empty else np.nan,
            "Avg added cost": float(g_vehicle_cost["add_cost"].mean()) if not g_vehicle_cost.empty else np.nan,
            "Avg miles between services": fleet_avg_miles(g_vehicle),
        })

        # Charger row (includes station-level charger rows tagged by maint_ob == 2)
        g_charger = g[(g["charger_id"].notna()) | (g["maint_ob"] == 2)]
        g_charger_cost = df_cost[(df_cost["fleet_name"] == fleet) & ((df_cost["charger_id"].notna()) | (df_cost["maint_ob"] == 2))]
        rows.append({
            "Fleet": fleet_name,
            "Asset type": "Charger",
            "Events": len(g_charger),
            "Total cost": float(g_charger_cost["total_cost"].sum()) if not g_charger_cost.empty else np.nan,
            "Avg total cost": float(g_charger_cost["total_cost"].mean()) if not g_charger_cost.empty else np.nan,
            "Avg parts cost": float(g_charger_cost["parts_cost"].mean()) if not g_charger_cost.empty else np.nan,
            "Avg labor cost": float(g_charger_cost["labor_cost"].mean()) if not g_charger_cost.empty else np.nan,
            "Avg added cost": float(g_charger_cost["add_cost"].mean()) if not g_charger_cost.empty else np.nan,
            "Avg miles between services": np.nan,
        })

    out = pd.DataFrame(rows)
    out["asset_order"] = out["Asset type"].map({"Vehicle": 0, "Charger": 1}).fillna(99)
    out = out.sort_values(["Fleet", "asset_order"], na_position="last").drop(columns=["asset_order"])
    return out


def _fmt_int(val):
    return "n/a" if pd.isna(val) else f"{int(round(float(val))):,}"


def _fmt_money(val):
    return "n/a" if pd.isna(val) else f"${int(round(float(val))):,}"


def render_fleet_table(tbl: pd.DataFrame):
    header_style = {"padding": "0.3rem 0.45rem", "fontSize": "0.82rem", "whiteSpace": "nowrap"}
    cell_style = {"padding": "0.22rem 0.45rem", "fontSize": "0.82rem", "lineHeight": "1.15"}
    headers = [
        "Fleet",
        "Asset type",
        "Events",
        "Total cost",
        "Avg total cost",
        "Avg parts cost",
        "Avg labor cost",
        "Avg added cost",
        "Avg miles between services",
    ]
    if tbl.empty:
        return dbc.Table(
            [
                html.Thead(html.Tr([html.Th(h, style=header_style) for h in headers])),
                html.Tbody([html.Tr([html.Td("No data", colSpan=len(headers), style=cell_style)])]),
            ],
            bordered=True,
            hover=True,
            responsive=True,
            striped=True,
            size="sm",
            color="dark",
            className="mb-0",
        )

    body_rows = []
    for fleet, g in tbl.groupby("Fleet", sort=False, dropna=False):
        fleet_name = "Unspecified" if pd.isna(fleet) else str(fleet)
        group_rows = g.to_dict("records")
        for i, row in enumerate(group_rows):
            cells = []
            if i == 0:
                cells.append(
                    html.Td(
                        fleet_name,
                        rowSpan=len(group_rows),
                        style={**cell_style, "verticalAlign": "middle", "fontWeight": "600"},
                    )
                )
            cells.extend(
                [
                    html.Td(row.get("Asset type", "n/a"), style=cell_style),
                    html.Td(_fmt_int(row.get("Events")), style=cell_style),
                    html.Td(_fmt_money(row.get("Total cost")), style=cell_style),
                    html.Td(_fmt_money(row.get("Avg total cost")), style=cell_style),
                    html.Td(_fmt_money(row.get("Avg parts cost")), style=cell_style),
                    html.Td(_fmt_money(row.get("Avg labor cost")), style=cell_style),
                    html.Td(_fmt_money(row.get("Avg added cost")), style=cell_style),
                    html.Td(_fmt_int(row.get("Avg miles between services")), style=cell_style),
                ]
            )
            body_rows.append(html.Tr(cells))

    return dbc.Table(
        [
            html.Thead(html.Tr([html.Th(h, style=header_style) for h in headers])),
            html.Tbody(body_rows),
        ],
        bordered=True,
        hover=True,
        responsive=True,
        striped=True,
        size="sm",
        color="dark",
        className="mb-0",
    )


def fleet_table_component():
    initial_tbl = compute_fleet_table(_df)
    return html.Div(id="maint-fleet-table", children=render_fleet_table(initial_tbl))


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
            df.dropna(subset=["veh_id"])
              .drop_duplicates(subset=["veh_id"])
              .assign(label=lambda x: x["vehicle_label"].fillna(x["veh_id"].astype(str)))
        )
        opts = [{"label": str(row["label"]), "value": int(row["veh_id"])} for _, row in opts.iterrows()]
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
        d = d[d["veh_id"].notna()]
        if asset_ids:
            d = d[d["veh_id"].isin(asset_ids)]
    elif asset_type == "charger":
        d = d[(d["charger_id"].notna()) | (d["maint_ob"] == 2)]
        if asset_ids:
            d = d[d["charger_id"].isin(asset_ids)]

    # Date range (uses m.date)
    if start_date:
        d = d[d["date"] >= pd.to_datetime(start_date)]
    if end_date:
        d = d[d["date"] <= pd.to_datetime(end_date)]

    return d


@callback(
    Output("maint-fleet-table", "children"),
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
    tbl = compute_fleet_table(_df)
    fleet_table_ui = render_fleet_table(tbl)

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

    return fleet_table_ui, fig_cat, fig_war, fig_loc
