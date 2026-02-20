# RUNBOOK

Operational checklist for `global-funds-data-pipeline`.

## 1) First-Time Setup

- [ ] Clone repository
```bash
git clone https://github.com/JIDAPA0/global-funds-data-pipeline.git
cd global-funds-data-pipeline
```

- [ ] Start Docker services
```bash
docker-compose up -d
```

- [ ] Verify containers are up
```bash
docker ps
```

Expected:
- `funds_mysql_container` (healthy)
- `funds_phpmyadmin_container`
- `funds_pipeline_runner`

- [ ] Install Python dependencies inside runner
```bash
docker exec -it funds_pipeline_runner bash -lc "pip install -r requirements.txt"
docker exec -it funds_pipeline_runner bash -lc "python -m playwright install chromium"
```

- [ ] Verify DB access
- MySQL: `localhost:3308`
- phpMyAdmin: `http://localhost:8080`

## 2) Initial Data Load (First Run)

- [ ] Run full pipeline once
```bash
docker exec -it funds_pipeline_runner bash -lc "PYTHONPATH=. python -m src.maintenance.prefect_pipeline"
```

## 2.1) Prefect Schedule Setup (Daily + Weekly)

- [ ] Install dependencies in runner
```bash
docker exec -it funds_pipeline_runner bash -lc "pip install -r requirements.txt"
```

- [ ] Start Prefect API server
```bash
docker exec -d funds_pipeline_runner bash -lc "prefect server start --host 0.0.0.0 --port 4200"
```

- [ ] Register deployments (one command for daily + weekly)
```bash
docker exec -it funds_pipeline_runner bash -lc "export PREFECT_API_URL=http://127.0.0.1:4200/api && PYTHONPATH=. python -m src.maintenance.register_prefect_deployments"
```

- [ ] Start Prefect agent for queue `default`
```bash
docker exec -d funds_pipeline_runner bash -lc "export PREFECT_API_URL=http://127.0.0.1:4200/api && prefect agent start -q default"
```

Default schedule:
- `global-funds-daily`: `30 6 * * 1-5` (`America/New_York`)
- `global-funds-weekly-holdings`: `0 9 * * 6` (`America/New_York`)

## 3) Daily Operations (Market Open Days)

- [ ] Run:
  - Financial Times Master Ticker
  - Financial Times Daily NAV/Price
  - Yahoo Finance Master Ticker

Command:
```bash
docker exec -it funds_pipeline_runner bash -lc "PYTHONPATH=. python -m src.maintenance.prefect_pipeline"
```

Note:
- Holdings + Sector/Region is configured to run only on market-closed days by flow logic.

## 4) Weekly Operations (Market Closed Day)

- [ ] Run weekly refresh:
  - Financial Times Holdings
  - Financial Times Sector/Region
  - Financial Times Static Detail (recommended weekly)

Force holdings run any day:
```bash
docker exec -it funds_pipeline_runner bash -lc "PYTHONPATH=. python - <<'PY'
from src.maintenance.prefect_pipeline import run_data_pipeline
run_data_pipeline(force_run_holdings=True)
PY"
```

## 5) Schema Management

- [ ] Source of truth schema file:
  - `db/init/001_schema.sql`

Important:
- MySQL init scripts run only on first volume initialization.

Recreate DB from scratch:
```bash
docker-compose down -v
docker-compose up -d
```

## 6) Common Troubleshooting

- [ ] Docker command not found
  - Install Docker Desktop or Colima

- [ ] Port conflict (MySQL)
  - Current mapped port is `3308` in `docker-compose.yml`
  - Change host port if needed, then `docker-compose up -d`

- [ ] Container unhealthy
```bash
docker logs funds_mysql_container --tail 200
```

- [ ] Python import errors (`No module named src`)
  - Always run with `PYTHONPATH=.`

## 7) Quick Health Checks

```bash
docker ps
docker logs funds_mysql_container --tail 50
docker logs funds_pipeline_runner --tail 50
```

## 8) Shutdown

```bash
docker-compose down
```
