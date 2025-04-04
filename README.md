# ZEV Dashboard Prototype

This dashboard visualizes fleet and telematics data from PostgreSQL using Plotly Dash.

## Features
- Live map of ZEV fleet depot locations
- Interactive popups with vendor and grant info
- Easy to extend with telematics, charging, and maintenance data

## Setup
```bash
git clone https://github.com/ytj254/zev-dashboard.git
cd zev-dashboard
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
