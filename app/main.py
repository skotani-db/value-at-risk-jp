import os
os.environ["MLFLOW_ENABLE_DB_SDK"] = "true"
os.environ["MLFLOW_TRACKING_URI"] = "databricks"

import json
import yaml
import threading
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from databricks.sdk import WorkspaceClient

app = FastAPI(title="RiskLens VaR")
w = WorkspaceClient()

# In-memory progress tracking
etl_progress = {"running": False, "current": 0, "total": 0, "current_ticker": "", "done": False, "error": None}
model_progress = {"running": False, "current": 0, "total": 4, "step": "", "done": False, "error": None}

# Resolve workspace host from SDK
try:
    RESOLVED_HOST = w.config.host.rstrip("/") if w.config.host else ""
except Exception:
    RESOLVED_HOST = ""

# ── Config ──
CONFIG_PATH = Path(__file__).parent / "config" / "application.yaml"
with open(CONFIG_PATH, "r") as f:
    APP_CONFIG = yaml.safe_load(f)

CATALOG = APP_CONFIG["database"]["catalog"]
SCHEMA = APP_CONFIG["database"]["schema"]
TABLES = APP_CONFIG["database"]["tables"]
MC_CONFIG = APP_CONFIG["monte-carlo"]
MODEL_CONFIG = APP_CONFIG["model"]
YF_CONFIG = APP_CONFIG["yfinance"]

PORTFOLIO_PATH = Path(__file__).parent / "config" / "portfolio.json"
with open(PORTFOLIO_PATH, "r") as f:
    PORTFOLIO = json.load(f)

INDICATORS_PATH = Path(__file__).parent / "config" / "indicators.json"
with open(INDICATORS_PATH, "r") as f:
    INDICATORS = json.load(f)

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
WORKSPACE_HOST = os.environ.get("DATABRICKS_HOST", "")

FQN = lambda t: f"{CATALOG}.{SCHEMA}.{TABLES[t]}"


def run_sql(query: str):
    import time
    if not WAREHOUSE_ID:
        raise HTTPException(500, "DATABRICKS_WAREHOUSE_ID not set")
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="50s",
    )
    # Poll if still running
    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status and resp.status.state and resp.status.state.value == "FAILED":
        msg = resp.status.error.message if resp.status.error else "Unknown"
        raise HTTPException(500, f"SQL: {msg}")
    cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest else []
    rows = []
    if resp.result and resp.result.data_array:
        rows = [dict(zip(cols, r)) for r in resp.result.data_array]
    return {"columns": cols, "rows": rows}


# ── Ticker params for dummy data (from original notebook) ──
TICKER_PARAMS = {
    'BCH': (22.5, 0.00015, 0.018), 'BSAC': (19.8, 0.0001, 0.020),
    'CCU': (12.3, 0.00005, 0.016), 'ITCB': (4.8, -0.0001, 0.025),
    'ENIC': (3.2, 0.00008, 0.015), 'SQM': (52.0, -0.0002, 0.028),
    'CIB': (32.5, 0.00012, 0.022), 'EC': (11.2, 0.00005, 0.026),
    'AVAL': (2.4, -0.0001, 0.020), 'AMX': (17.8, 0.00008, 0.017),
    'AMOV': (18.2, 0.00008, 0.017), 'CX': (7.3, 0.0001, 0.024),
    'KOF': (92.0, 0.00012, 0.014), 'VLRS': (8.5, 0.00005, 0.032),
    'FMX': (132.0, 0.0001, 0.015), 'PAC': (172.0, 0.00015, 0.018),
    'ASR': (285.0, 0.00012, 0.019), 'BSMX': (8.1, 0.0001, 0.021),
    'SIM': (28.5, 0.00008, 0.023), 'TV': (3.1, -0.0003, 0.030),
    'IBA': (48.0, 0.00005, 0.016), 'BLX': (32.0, 0.00015, 0.019),
    'CPA': (98.0, 0.00012, 0.022), 'CPAC': (5.8, 0.00005, 0.018),
    'SCCO': (108.0, 0.00018, 0.025), 'FSM': (4.5, 0.0001, 0.030),
    'BAP': (168.0, 0.00015, 0.020),
}

INDICATOR_PARAMS = {
    'SP500': (5205.0, 0.00035, 0.011),
    'NYSE': (18120.0, 0.00025, 0.009),
    'OIL': (78.5, 0.00005, 0.022),
    'TREASURY': (4.32, 0.0, 0.008),
    'DOWJONES': (39170.0, 0.00030, 0.010),
}


# ════════════════════════════════════════════
# API: Config
# ════════════════════════════════════════════

@app.get("/api/config")
def get_config():
    return {
        "catalog": CATALOG, "schema": SCHEMA, "tables": TABLES,
        "monte_carlo": MC_CONFIG, "model": MODEL_CONFIG,
        "yfinance": YF_CONFIG,
        "portfolio_count": len(PORTFOLIO), "indicator_count": len(INDICATORS),
        "workspace_host": WORKSPACE_HOST,
    }


@app.get("/api/portfolio")
def get_portfolio():
    return PORTFOLIO


# ════════════════════════════════════════════
# API: 01 - ETL
# ════════════════════════════════════════════

class ETLRequest(BaseModel):
    tickers: List[str] = []
    start_date: str = "2024-05-01"
    end_date: str = "2026-05-01"


def _generate_data_bg(tickers, start, end):
    """Background task: generate dummy stock + indicator data."""
    global etl_progress
    total_steps = len(tickers) + 3  # schema + portfolio + stocks_table + per-ticker + indicators
    etl_progress = {"running": True, "current": 0, "total": total_steps, "current_ticker": "Initializing...", "done": False, "error": None}

    try:
        # Step: Schema
        etl_progress["current_ticker"] = "Creating schema..."
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

        # Step: Portfolio table
        etl_progress["current"] = 1
        etl_progress["current_ticker"] = "Creating portfolio table..."
        portfolio_values = ", ".join([
            f"('{p['ticker']}', '{p['country']}', '{p['company'].replace(chr(39), chr(39)+chr(39))}', '{p['industry'].replace(chr(39), chr(39)+chr(39))}', {p['weight']})"
            for p in PORTFOLIO
        ])
        run_sql(f"CREATE OR REPLACE TABLE {FQN('portfolio')} (ticker STRING, country STRING, company STRING, industry STRING, weight DOUBLE)")
        run_sql(f"INSERT INTO {FQN('portfolio')} VALUES {portfolio_values}")

        # Step: Create stocks table
        etl_progress["current"] = 2
        etl_progress["current_ticker"] = "Creating stocks table..."
        run_sql(f"CREATE OR REPLACE TABLE {FQN('stocks')} (ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT)")

        # Per-ticker INSERT
        for i, t in enumerate(tickers):
            etl_progress["current"] = 3 + i
            etl_progress["current_ticker"] = t
            p = TICKER_PARAMS.get(t, (50.0, 0.0001, 0.02))
            run_sql(f"""
                INSERT INTO {FQN('stocks')}
                WITH dates AS (
                    SELECT EXPLODE(SEQUENCE(TO_DATE('{start}'), TO_DATE('{end}'), INTERVAL 1 DAY)) as date
                ),
                bdays AS (
                    SELECT date FROM dates WHERE DAYOFWEEK(date) BETWEEN 2 AND 6
                ),
                with_rand AS (
                    SELECT date, RAND() as r1, RAND() as r2, RAND() as r3, RAND() as r4
                    FROM bdays
                ),
                with_return AS (
                    SELECT date, r1, r2, r3, r4,
                        {p[1]} + {p[2]} * (r1 - 0.5) * 3.46 as daily_ret
                    FROM with_rand
                ),
                cum AS (
                    SELECT date, r2, r3, r4,
                        {p[0]} * EXP(SUM(daily_ret) OVER (ORDER BY date)) as close_price
                    FROM with_return
                )
                SELECT
                    '{t}' as ticker, date,
                    ROUND(close_price * (1 + (r2 - 0.5) * {p[2]} * 0.6), 4) as open,
                    ROUND(close_price * (1 + r3 * {p[2]} * 0.3), 4) as high,
                    ROUND(close_price * (1 - r4 * {p[2]} * 0.3), 4) as low,
                    ROUND(close_price, 4) as close,
                    CAST(GREATEST(10000, {p[0]} * 15000 * (0.3 + r2)) AS BIGINT) as volume
                FROM cum
            """)

        # Market indicators
        etl_progress["current"] = total_steps - 1
        etl_progress["current_ticker"] = "Market Indicators"
        run_sql(f"CREATE OR REPLACE TABLE {FQN('indicators')} (date DATE, SP500 DOUBLE, NYSE DOUBLE, OIL DOUBLE, TREASURY DOUBLE, DOWJONES DOUBLE)")
        run_sql(f"""
            INSERT INTO {FQN('indicators')}
            WITH dates AS (
                SELECT EXPLODE(SEQUENCE(TO_DATE('{start}'), TO_DATE('{end}'), INTERVAL 1 DAY)) as date
            ),
            bdays AS (
                SELECT date FROM dates WHERE DAYOFWEEK(date) BETWEEN 2 AND 6
            ),
            with_rand AS (
                SELECT date, RAND() as r1, RAND() as r2, RAND() as r3, RAND() as r4, RAND() as r5
                FROM bdays
            ),
            cum AS (
                SELECT date,
                    {INDICATOR_PARAMS['SP500'][0]} * EXP(SUM({INDICATOR_PARAMS['SP500'][1]} + {INDICATOR_PARAMS['SP500'][2]} * (r1-0.5)*3.46) OVER (ORDER BY date)) as SP500,
                    {INDICATOR_PARAMS['NYSE'][0]} * EXP(SUM({INDICATOR_PARAMS['NYSE'][1]} + {INDICATOR_PARAMS['NYSE'][2]} * (r2-0.5)*3.46) OVER (ORDER BY date)) as NYSE,
                    {INDICATOR_PARAMS['OIL'][0]} * EXP(SUM({INDICATOR_PARAMS['OIL'][1]} + {INDICATOR_PARAMS['OIL'][2]} * (r3-0.5)*3.46) OVER (ORDER BY date)) as OIL,
                    {INDICATOR_PARAMS['TREASURY'][0]} + SUM({INDICATOR_PARAMS['TREASURY'][2]} * (r4-0.5)*3.46) OVER (ORDER BY date) as TREASURY,
                    {INDICATOR_PARAMS['DOWJONES'][0]} * EXP(SUM({INDICATOR_PARAMS['DOWJONES'][1]} + {INDICATOR_PARAMS['DOWJONES'][2]} * (r5-0.5)*3.46) OVER (ORDER BY date)) as DOWJONES
                FROM with_rand
            )
            SELECT date, ROUND(SP500,2), ROUND(NYSE,2), ROUND(OIL,2),
                   ROUND(GREATEST(1.0, LEAST(7.0, TREASURY)),2), ROUND(DOWJONES,2)
            FROM cum
        """)

        etl_progress["current"] = total_steps
        etl_progress["current_ticker"] = "Done"
        etl_progress["done"] = True
        etl_progress["running"] = False

    except Exception as e:
        etl_progress["error"] = str(e)
        etl_progress["running"] = False


@app.post("/api/etl/generate")
def generate_data(req: ETLRequest):
    """Start dummy data generation in background."""
    if etl_progress["running"]:
        raise HTTPException(409, "Generation already in progress")
    tickers = req.tickers if req.tickers else [p["ticker"] for p in PORTFOLIO]
    thread = threading.Thread(target=_generate_data_bg, args=(tickers, req.start_date, req.end_date))
    thread.start()
    return {"status": "started", "tickers": len(tickers)}


@app.get("/api/etl/progress")
def get_etl_progress():
    return etl_progress


@app.get("/api/etl/stocks")
def get_stocks(ticker: Optional[str] = None, start_date: Optional[str] = None,
               end_date: Optional[str] = None, limit: int = Query(default=1000, le=10000)):
    where = []
    if ticker:
        where.append(f"ticker = '{ticker}'")
    if start_date:
        where.append(f"date >= '{start_date}'")
    if end_date:
        where.append(f"date <= '{end_date}'")
    w_clause = "WHERE " + " AND ".join(where) if where else ""
    return run_sql(f"SELECT * FROM {FQN('stocks')} {w_clause} ORDER BY date LIMIT {limit}")


@app.get("/api/etl/tickers")
def get_tickers():
    return run_sql(f"""
        SELECT ticker, COUNT(*) as cnt, MIN(date) as min_date, MAX(date) as max_date,
               ROUND(AVG(close), 2) as avg_close
        FROM {FQN('stocks')} GROUP BY ticker ORDER BY ticker
    """)


# ════════════════════════════════════════════
# API: 02 - Model Training
# ════════════════════════════════════════════

def non_linear_features(xs):
    """非線形特徴量を生成（x, x^2, x^3, sqrt(|x|)）- 元ノートブック準拠"""
    import numpy as np
    fs = []
    for x in xs:
        fs.append(x)
        fs.append(np.sign(x) * x ** 2)
        fs.append(x ** 3)
        fs.append(np.sign(x) * np.sqrt(abs(x)))
    return fs


def predict_non_linears(ps, fs):
    """非線形特徴量と重みから予測値を計算 - 元ノートブック準拠"""
    s = ps[0]
    for i, f in enumerate(fs):
        s = s + ps[i + 1] * f
    return float(s)


NOTEBOOK_PATH = "/Workspace/Users/shotaro.kotani@databricks.com/risklens-var/notebooks/02_train_model.py"

def _train_model_bg():
    """Background task: kick off training notebook as a serverless job."""
    global model_progress
    model_progress = {"running": True, "current": 0, "total": 3, "step": "Submitting job...", "done": False, "error": None, "run_url": None, "model_url": None}
    import time
    try:
        # Step 1: Submit notebook job via REST API
        model_progress.update({"current": 1, "step": "Submitting training job..."})
        notebook_params = {
            "catalog": CATALOG,
            "schema": SCHEMA,
            "stocks_table": TABLES["stocks"],
            "indicators_table": TABLES["indicators"],
            "model_name": MODEL_CONFIG["name"],
            "model_date": MODEL_CONFIG["date"],
        }
        run_resp = w.api_client.do("POST", "/api/2.1/jobs/runs/submit", body={
            "run_name": "risklens-var-model-training",
            "tasks": [{
                "task_key": "train_model",
                "notebook_task": {
                    "notebook_path": NOTEBOOK_PATH,
                    "base_parameters": notebook_params,
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            }],
            "environments": [{
                "environment_key": "Default",
                "spec": {
                    "client": "1",
                },
            }],
        })
        job_run_id = run_resp.get("run_id")
        run_page_url = f"{RESOLVED_HOST}/#job/{job_run_id}"
        model_progress["run_url"] = run_page_url
        model_progress["model_url"] = f"{RESOLVED_HOST}/explore/data/models/{CATALOG}/{SCHEMA}/{MODEL_CONFIG['name']}"
        print(f"[Model] Job submitted: run_id={job_run_id}")

        # Step 2: Poll job status
        model_progress.update({"current": 2, "step": "Training in progress..."})
        while True:
            time.sleep(10)
            run_info = w.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": str(job_run_id)})
            state = run_info.get("state", {})
            life_cycle = state.get("life_cycle_state", "UNKNOWN")
            result = state.get("result_state")
            model_progress["step"] = f"Job: {life_cycle}"
            model_progress["run_url"] = run_info.get("run_page_url", run_page_url)
            print(f"[Model] Job status: {life_cycle} / {result}")

            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if result == "SUCCESS":
                    break
                else:
                    error_msg = state.get("state_message", "Unknown error")
                    raise Exception(f"Job failed: {result} - {error_msg}")

        # Step 3: Done
        model_progress.update({"current": 3, "step": "Done"})
        model_progress["done"] = True
        model_progress["running"] = False

    except Exception as e:
        print(f"[Model] ERROR: {e}")
        model_progress["error"] = str(e)
        model_progress["running"] = False


@app.post("/api/model/train")
def train_model():
    if model_progress["running"]:
        raise HTTPException(409, "Training already in progress")
    thread = threading.Thread(target=_train_model_bg)
    thread.start()
    return {"status": "started"}


@app.get("/api/model/progress")
def get_model_progress():
    return model_progress


@app.get("/api/model/weights")
def get_model_weights():
    return run_sql(f"SELECT * FROM {FQN('model_weights')} ORDER BY ticker")


@app.get("/api/model/factor_corr")
def get_factor_correlation():
    """Get factor correlation matrix from indicator returns."""
    return run_sql(f"""
        SELECT
            ROUND(CORR(sp500_ret, nyse_ret), 3) as sp500_nyse,
            ROUND(CORR(sp500_ret, oil_ret), 3) as sp500_oil,
            ROUND(CORR(sp500_ret, treasury_ret), 3) as sp500_treasury,
            ROUND(CORR(sp500_ret, dowjones_ret), 3) as sp500_dowjones,
            ROUND(CORR(nyse_ret, oil_ret), 3) as nyse_oil,
            ROUND(CORR(nyse_ret, treasury_ret), 3) as nyse_treasury,
            ROUND(CORR(nyse_ret, dowjones_ret), 3) as nyse_dowjones,
            ROUND(CORR(oil_ret, treasury_ret), 3) as oil_treasury,
            ROUND(CORR(oil_ret, dowjones_ret), 3) as oil_dowjones,
            ROUND(CORR(treasury_ret, dowjones_ret), 3) as treasury_dowjones
        FROM {FQN('indicator_returns')}
        WHERE sp500_ret IS NOT NULL
    """)


@app.get("/api/model/urls")
def get_model_urls():
    host = RESOLVED_HOST
    return {
        "experiment_url": model_progress.get("run_url") or (f"{host}/#mlflow/experiments" if host else None),
        "model_registry_url": model_progress.get("model_url") or (f"{host}/explore/data/models/{CATALOG}/{SCHEMA}/{MODEL_CONFIG['name']}" if host else None),
        "catalog_url": f"{host}/explore/data/{CATALOG}/{SCHEMA}" if host else None,
    }


# ════════════════════════════════════════════
# API: 03 - Monte Carlo Simulation
# ════════════════════════════════════════════

class MCRequest(BaseModel):
    num_trials: int = 32000
    confidence: int = 99


@app.post("/api/montecarlo/step")
def mc_simulation_step(num_trials: int = Query(default=1000), confidence: int = Query(default=99)):
    """Run MC simulation with given # of trials and return histogram + VaR."""
    result = run_sql(f"""
        WITH factor_stats AS (
            SELECT
                STDDEV(sp500_ret) as std_sp500,
                STDDEV(nyse_ret) as std_nyse,
                STDDEV(oil_ret) as std_oil,
                STDDEV(treasury_ret) as std_treasury,
                STDDEV(dowjones_ret) as std_dowjones
            FROM {FQN('indicator_returns')}
            WHERE sp500_ret IS NOT NULL
        ),
        trials AS (
            SELECT EXPLODE(SEQUENCE(1, {num_trials})) as trial_id
        ),
        sim AS (
            SELECT
                t.trial_id,
                p.ticker,
                p.weight,
                m.alpha
                + m.beta_sp500 * fs.std_sp500 * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_oil * fs.std_oil * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_treasury * fs.std_treasury * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                as sim_return
            FROM trials t
            CROSS JOIN {FQN('portfolio')} p
            JOIN {FQN('model_weights')} m ON p.ticker = m.ticker
            CROSS JOIN factor_stats fs
        ),
        portfolio_returns AS (
            SELECT trial_id, SUM(sim_return * weight) as total_return
            FROM sim GROUP BY trial_id
        )
        SELECT
            ROUND(total_return * 500) / 500 as bucket,
            COUNT(*) as frequency
        FROM portfolio_returns
        GROUP BY 1 ORDER BY 1
    """)

    var_result = run_sql(f"""
        WITH factor_stats AS (
            SELECT STDDEV(sp500_ret) as std_sp500, STDDEV(nyse_ret) as std_nyse,
                   STDDEV(oil_ret) as std_oil, STDDEV(treasury_ret) as std_treasury,
                   STDDEV(dowjones_ret) as std_dowjones
            FROM {FQN('indicator_returns')} WHERE sp500_ret IS NOT NULL
        ),
        trials AS (SELECT EXPLODE(SEQUENCE(1, {num_trials})) as trial_id),
        sim AS (
            SELECT t.trial_id, p.weight,
                m.alpha
                + m.beta_sp500 * fs.std_sp500 * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_oil * fs.std_oil * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_treasury * fs.std_treasury * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                as sim_return
            FROM trials t CROSS JOIN {FQN('portfolio')} p
            JOIN {FQN('model_weights')} m ON p.ticker = m.ticker
            CROSS JOIN factor_stats fs
        ),
        pr AS (SELECT trial_id, SUM(sim_return * weight) as total_return FROM sim GROUP BY trial_id)
        SELECT
            ROUND(PERCENTILE(total_return, {(100-confidence)/100.0}), 6) as var_value,
            ROUND(AVG(CASE WHEN total_return <= PERCENTILE(total_return, {(100-confidence)/100.0}) THEN total_return END), 6) as expected_shortfall,
            ROUND(AVG(total_return), 6) as mean_return,
            ROUND(STDDEV(total_return), 6) as std_return,
            COUNT(*) as num_trials
        FROM pr
    """)

    return {"histogram": result, "stats": var_result}


@app.post("/api/montecarlo/persist")
def mc_persist(req: MCRequest):
    """Run full MC simulation and persist results for aggregation."""

    # Portfolio returns per date
    run_sql(f"""
        CREATE OR REPLACE TABLE {FQN('mc_portfolio_returns')} AS
        WITH dates AS (
            SELECT DISTINCT date FROM {FQN('stocks')}
            WHERE date >= '{MODEL_CONFIG["date"]}'
        ),
        factor_stats AS (
            SELECT STDDEV(sp500_ret) as std_sp500, STDDEV(oil_ret) as std_oil,
                   STDDEV(treasury_ret) as std_treasury
            FROM {FQN('indicator_returns')} WHERE sp500_ret IS NOT NULL
        ),
        trials AS (SELECT EXPLODE(SEQUENCE(1, {req.num_trials})) as trial_id),
        sim AS (
            SELECT d.date, t.trial_id, p.ticker, p.weight, p.country,
                m.alpha
                + m.beta_sp500 * fs.std_sp500 * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_oil * fs.std_oil * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                + m.beta_treasury * fs.std_treasury * (SQRT(-2*LN(GREATEST(RAND(),1e-10)))*COS(2*3.14159265359*RAND()))
                as sim_return
            FROM dates d CROSS JOIN trials t
            CROSS JOIN {FQN('portfolio')} p
            JOIN {FQN('model_weights')} m ON p.ticker = m.ticker
            CROSS JOIN factor_stats fs
        )
        SELECT date, trial_id, country,
            SUM(sim_return * weight) as portfolio_return
        FROM sim GROUP BY date, trial_id, country
    """)

    # VaR timeseries
    run_sql(f"""
        CREATE OR REPLACE TABLE {FQN('var_timeseries')} AS
        SELECT date,
            ROUND(PERCENTILE(portfolio_return, 0.01), 6) as var_99,
            ROUND(AVG(CASE WHEN portfolio_return <= PERCENTILE(portfolio_return, 0.01) THEN portfolio_return END), 6) as es_99,
            COUNT(*) as num_trials
        FROM {FQN('mc_portfolio_returns')}
        GROUP BY date ORDER BY date
    """)

    # VaR by country
    run_sql(f"""
        CREATE OR REPLACE TABLE {FQN('var_by_country')} AS
        SELECT date, country,
            ROUND(PERCENTILE(portfolio_return, 0.01), 6) as var_99
        FROM {FQN('mc_portfolio_returns')}
        GROUP BY date, country ORDER BY date, country
    """)

    return {"status": "ok"}


# ════════════════════════════════════════════
# API: 04 - Aggregation
# ════════════════════════════════════════════

@app.get("/api/aggregation/var_timeseries")
def get_var_timeseries():
    return run_sql(f"SELECT * FROM {FQN('var_timeseries')} ORDER BY date")


@app.get("/api/aggregation/var_by_country")
def get_var_by_country():
    return run_sql(f"SELECT * FROM {FQN('var_by_country')} ORDER BY date, country")


# ════════════════════════════════════════════
# API: 05 - Compliance
# ════════════════════════════════════════════

@app.post("/api/compliance/compute")
def compute_compliance():
    """Compute compliance backtest data."""
    run_sql(f"""
        CREATE OR REPLACE TABLE {FQN('compliance')} AS
        WITH daily_returns AS (
            SELECT date,
                SUM(LN(close / LAG(close) OVER (PARTITION BY s.ticker ORDER BY s.date)) * p.weight) as portfolio_return
            FROM {FQN('stocks')} s
            JOIN {FQN('portfolio')} p ON s.ticker = p.ticker
            WHERE s.close IS NOT NULL
            GROUP BY date
        ),
        with_var AS (
            SELECT d.date, d.portfolio_return, v.var_99
            FROM daily_returns d
            LEFT JOIN {FQN('var_timeseries')} v ON d.date = v.date
        )
        SELECT date, portfolio_return, var_99,
            CASE
                WHEN portfolio_return < var_99 THEN 1 ELSE 0
            END as is_breach
        FROM with_var
        WHERE portfolio_return IS NOT NULL
        ORDER BY date
    """)
    return {"status": "ok"}


@app.get("/api/compliance/data")
def get_compliance_data():
    return run_sql(f"SELECT * FROM {FQN('compliance')} ORDER BY date")


@app.get("/api/compliance/summary")
def get_compliance_summary():
    return run_sql(f"""
        SELECT
            SUM(is_breach) as total_breaches,
            COUNT(*) as total_days,
            ROUND(SUM(is_breach) * 100.0 / COUNT(*), 2) as breach_pct,
            CASE
                WHEN SUM(is_breach) <= 4 THEN 'GREEN'
                WHEN SUM(is_breach) <= 9 THEN 'YELLOW'
                ELSE 'RED'
            END as basel_zone
        FROM {FQN('compliance')}
    """)


# ════════════════════════════════════════════
# API: Genie
# ════════════════════════════════════════════

class GenieRequest(BaseModel):
    space_id: str
    question: str


@app.post("/api/genie/ask")
def ask_genie(req: GenieRequest):
    try:
        conv = w.genie.start_conversation(space_id=req.space_id, content=req.question)
        return {"conversation_id": conv.conversation_id, "message_id": conv.message_id}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/genie/result/{space_id}/{conversation_id}/{message_id}")
def get_genie_result(space_id: str, conversation_id: str, message_id: str):
    try:
        msg = w.genie.get_message(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
        result = {}
        if msg.attachments:
            for att in msg.attachments:
                if att.text:
                    result["text"] = att.text.content
                if att.query:
                    result["query"] = att.query.query
                    result["description"] = att.query.description
        return {"status": msg.status.value if msg.status else None, "result": result}
    except Exception as e:
        raise HTTPException(500, str(e))


# ════════════════════════════════════════════
# Static files
# ════════════════════════════════════════════

STATIC_DIR = Path(__file__).parent / "frontend" / "dist"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        fp = STATIC_DIR / full_path
        if fp.exists() and fp.is_file():
            return FileResponse(fp)
        return FileResponse(STATIC_DIR / "index.html")
