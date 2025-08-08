import os, sys, datetime, textwrap
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# 1) Connect
load_dotenv(dotenv_path=r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\aws\.env")
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("Set DATABASE_URL in .env (e.g., postgresql://user:pass@host:5432/zevperf)")
    sys.exit(1)
engine = create_engine(DB_URL, pool_pre_ping=True)
ins = inspect(engine)

# 2) Helpers
PUBLIC = "public"
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
out_path = r"data_update\db_cheatsheet.md"

def pg_version(conn):
    return conn.execute(text("select version(), current_database()")).fetchone()

def get_tables():
    # Only public schema, sorted for stability
    return sorted([t for t in ins.get_table_names(schema=PUBLIC)])

def get_columns(t):
    cols = ins.get_columns(t, schema=PUBLIC)
    # add default + is_nullable from information_schema for extra clarity
    with engine.begin() as conn:
        rows = conn.execute(text("""
        SELECT column_name, is_nullable, column_default, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
        ORDER BY ordinal_position
        """), {"s": PUBLIC, "t": t}).mappings().all()
    meta = {r["column_name"]: r for r in rows}
    for c in cols:
        m = meta.get(c["name"], {})
        c["is_nullable"] = (m.get("is_nullable") == "YES")
        c["default"] = m.get("column_default")
        c["data_type"] = m.get("data_type")
        c["udt_name"] = m.get("udt_name")
    return cols

def get_pk(t):
    pk = ins.get_pk_constraint(t, schema=PUBLIC)
    return pk.get("constrained_columns", []) or []

def get_uniques(t):
    uqs = []
    for idx in ins.get_indexes(t, schema=PUBLIC):
        if idx.get("unique"):
            uqs.append(idx["column_names"])
    return uqs

def get_fks(t):
    # [{'name':..., 'constrained_columns': [...], 'referred_schema':'public',
    #   'referred_table': 'vehicle', 'referred_columns': [...]}]
    return ins.get_foreign_keys(t, schema=PUBLIC)

def detect_postgis(col):
    # simple mark if geometry/geography
    return (col.get("udt_name") or "").lower() in ("geometry", "geography")

def format_cols(cols):
    lines = []
    for c in cols:
        typ = c.get("type")
        dt = (c.get("data_type") or str(typ)).replace("timestamp without time zone","timestamp")
        mark = " (PostGIS)" if detect_postgis(c) else ""
        nn = "" if c["is_nullable"] else " NOT NULL"
        dflt = f" DEFAULT {c['default']}" if c.get("default") else ""
        lines.append(f"- `{c['name']}` {dt}{mark}{nn}{dflt}")
    return "\n".join(lines)

def format_uqs(uqs):
    if not uqs: return ""
    return "\n".join([f"- UNIQUE({', '.join(cols)})" for cols in uqs])

def format_fks(fks):
    if not fks: return ""
    lines = []
    for fk in fks:
        cc = ", ".join(fk["constrained_columns"])
        rc = ", ".join(fk["referred_columns"])
        lines.append(f"- FK ({cc}) → {fk['referred_table']}({rc})")
    return "\n".join(lines)

# 3) Build ER map (for quick join hints)
edges = defaultdict(list)  # table -> [ (cols -> ref_table.ref_cols) ]
for t in get_tables():
    for fk in get_fks(t):
        edges[t].append((tuple(fk["constrained_columns"]), fk["referred_table"], tuple(fk["referred_columns"])))

# 4) Generate Markdown
with engine.begin() as conn:
    ver, dbname = pg_version(conn)
md = []
md.append(f"# Database Cheat Sheet — `{dbname}`")
md.append(f"_Generated: {now}_  \n_Engine: {ver.split(' on ')[0]}_")
md.append("\n---\n")
md.append("## Tables (public)\n")

for t in get_tables():
    cols = get_columns(t)
    pk = get_pk(t)
    uqs = get_uniques(t)
    fks = get_fks(t)

    md.append(f"### `{t}`")
    if pk: md.append(f"**Primary key**: {', '.join(pk)}\n")
    md.append("**Columns**:\n" + format_cols(cols) + "\n")
    if uqs:
        md.append("**Uniques**:\n" + format_uqs(uqs) + "\n")
    if fks:
        md.append("**Foreign keys**:\n" + format_fks(fks) + "\n")

# Quick join hints from FK graph
md.append("---\n## Quick Join Hints\n")
for t, lst in edges.items():
    for cols, rt, rcols in lst:
        md.append(f"- `{t}`.{', '.join(cols)} → `{rt}`.{', '.join(rcols)}")

# Common copy-paste snippets for you
md.append(r"""
---
## Handy Snippets

```sql
-- Recent telematics
SELECT t."timestamp", t.speed, v.fleet_vehicle_id, f.fleet_name
FROM veh_tel t
JOIN vehicle v ON t.veh_id = v.id
JOIN fleet   f ON v.fleet_id = f.id
ORDER BY t."timestamp" DESC
LIMIT 100;

-- Daily usage with vehicle
SELECT d.date, d.tot_dist, v.make, v.model
FROM veh_daily d
JOIN vehicle v ON d.veh_id = v.fleet_vehicle_id;

-- Charging with charger & fleet
SELECT r.refuel_start, r.tot_energy, c.charger, f.fleet_name
FROM refuel_inf r
JOIN charger c ON r.charger_id = c.charger
JOIN fleet   f ON c.fleet_id = f.id;
```
---
## Operational Instructions

1. **Start the SSH tunnel to RDS**
   - Double-click `start-tunnel.bat` (in the `aws` folder).
   - Keep this window open while you work; it maintains the tunnel to the database.

2. **Run scripts to interact with the database**
   - Example: `python aws/gen_db_cheatsheet.py` to update this cheat sheet.
   - Use scripts in `data_update` subfolder to upload data.

3. **Close the tunnel**
   - When finished, close the SSH tunnel window or press `Ctrl+C` inside it.
""")

# 5) WRITE THE FILE
content = "\n".join(md)
with open(out_path, "w", encoding="utf-8") as f:
    f.write(content)
print(f"[done] Wrote Markdown to: {out_path}")