## Freight Equipment Leasing ETL

### Prereqs
- Python: pandas, openpyxl, psycopg2, python-dotenv
- DB schema as in `zevperf.sql` plus `sql/setup_ingestion.sql` applied.

### Config
- `.env` loaded from:
  `<project root>\aws\.env`
  and must contain `DATABASE_URL=postgresql://...`

- Data root:
  `<project parent>\Incoming fleet data\Freight Equipment Leasing\aws_download`

### One-time DB setup
```bash
psql "$DATABASE_URL" -f sql/setup_ingestion.sql
