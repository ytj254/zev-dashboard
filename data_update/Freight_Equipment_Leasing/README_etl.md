## Freight Equipment Leasing ETL

### Prereqs
- Python: pandas, openpyxl, psycopg2, python-dotenv
- DB schema as in `zevperf.sql` plus `sql/setup_ingestion.sql` applied.

### Config
- `.env` loaded from:
  `D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\aws\.env`
  and must contain `DATABASE_URL=postgresql://...`

- Data root:
  `D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\aws_download`

### One-time DB setup
```bash
psql "$DATABASE_URL" -f sql/setup_ingestion.sql
