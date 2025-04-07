# ZEV Fleet Dashboard

This project is a web-based dashboard for exploring fleet data related to zero-emission vehicles (ZEVs) in Pennsylvania. It visualizes fleet locations on a map and shows detailed information about each fleet’s vehicles and chargers. The goal is to help users, such as DEP staff or researchers, understand the makeup and distribution of ZEV fleets across the state.

---
## Deployment

- **Frontend**: Deployed using [Render](https://render.com)
- **Database**: Powered by [Supabase](https://supabase.com) PostgreSQL

The app is publicly accessible and supports interactive maps, real-time queries, and detailed fleet insights.

---
## Project Structured

```text
zev-dashboard/
├── app.py              # Entry point for Dash multi-page app
├── db.py               # Database connection and query functions
├── utils.py            # Helper functions and mappings
├── render.yaml         # Render auto-deploy configuration
├── requirements.txt
├── README.md
├── assets/
│   ├── background.png        # Background for overview page
│   └── pa_boundary.geojson   # GeoJSON for PA boundary map
└── pages/
    ├── overview.py           # Landing page
    ├── fleet_info.py         # Fleet map and details
    ├── vehicle_info.py       # Vehicle details (Keep for possible extension)
    ├── charger_info.py       # Charger details (Keep for possible extension)
    ├── charging.py           # Charger usage and types
    ├── telematics.py         # Telematics and trip summaries
    ├── veh_daily_usage.py    # Daily usage per vehicle
    └── analysis.py           # Summary analysis and visuals
```
> This project uses `dash`, `dash-bootstrap-components`, `dash-leaflet`, `pandas`, `psycopg2`, `SQLAlchemy`, and `gunicorn`.

---
## Deployment Setup (Render + Supabase)

1. **Create a Supabase project**
   - Sync your data with PostgreSQL
   - Get your database connection string (`project-ref` and password)

2. **Deploy on Render**
   - Choose a **Web Service**
   - Add `DATABASE_URL` as an environment variable
   - Use `gunicorn app:server` as the start command

3. **Optional**: Add a `render.yaml` file for auto-deploy config.