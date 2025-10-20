# ZEV Fleet Dashboard

This project is a web-based dashboard for exploring fleet data related to zero-emission vehicles (ZEVs) in Pennsylvania. It visualizes fleet locations on a map and shows detailed information about each fleet’s vehicles and chargers. The goal is to help users, such as DEP staff or researchers, understand the makeup and distribution of ZEV fleets across the state.

---
## Deployment

- **Hosting**: Self-hosted on AWS EC2 with Ubuntu
- **Web Server**: Gunicorn + Nginx
- **Startup**: Managed via systemd (`zev.service`)
- **Domain**: https://pazevperf.dedyn.io (via deSEC DNS)
- **Database**: PostgreSQL running on AWS RDS (with PostGIS enabled)

The app is publicly accessible and supports interactive maps, real-time queries, and detailed fleet insights.

---
## Project Structure

```text
zev-dashboard/
├── app.py                    # Main entry for Dash app with layout and routing
├── db.py                     # PostgreSQL connection utilities
├── utils.py                  # Helper functions and mappings
├── styles.py                 # Centralized styling definitions for consistency
├── requirements.txt          # Python dependencies
├── render.yaml               # (legacy) Render deployment config.
├── .gitignore
├── README.md
├── assets/
│   └── pa_boundary.geojson   # GeoJSON map data for PA boundary
├── aws/
│   └── (AWS files)           # systemd unit file for AWS
├── data_update/              # Scripts for importing or updating data
│   └── (custom scripts)
├── pages/                    # Dash pages (multi-page layout)
   ├── overview.py
   ├── fleet_info.py
   ├── vehicle_info.py        # Vehicle details (Keep for possible extension)    
   ├── charger_info.py        # Charger details (Keep for possible extension)
   ├── veh_daily_usage.py     # Daily usage per vehicle
   ├── telematics.py          # Telematics summaries
   ├── charging.py            # Charger usage
   ├── maintenance.py         # Maintenance event summaries
   └── analysis.py            # Summary analysis and visuals
```
> This project uses `dash`, `dash-bootstrap-components`, `dash-leaflet`, `pandas`, `psycopg2`, `SQLAlchemy`, and `gunicorn`.

---
## Updating the App on EC2

```bash
# SSH into the instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# Pull new code and restart the server
source venv-zev/bin/activate
cd ~/zev-dashboard
git pull origin main
pip install -r requirements.txt  # Optional
sudo systemctl restart zev
```

---
## Testing the App locally

python app.py  
Open http://127.0.0.1:8050/