import os
import json
import yaml
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from databricks.sdk import WorkspaceClient

app = FastAPI(title="Value at Risk - Dashboard")

# Databricks client (uses DATABRICKS_HOST / DATABRICKS_TOKEN from env or Databricks Apps service principal)
w = WorkspaceClient()

# Load application config
CONFIG_PATH = Path(__file__).parent / "config" / "application.yaml"
with open(CONFIG_PATH, "r") as f:
    APP_CONFIG = yaml.safe_load(f)

CATALOG = APP_CONFIG["database"]["catalog"]
SCHEMA = APP_CONFIG["database"]["schema"]
TABLES = APP_CONFIG["database"]["tables"]
MC_CONFIG = APP_CONFIG["monte-carlo"]
MODEL_CONFIG = APP_CONFIG["model"]

# Load portfolio
PORTFOLIO_PATH = Path(__file__).parent / "config" / "portfolio.json"
with open(PORTFOLIO_PATH, "r") as f:
    PORTFOLIO = json.load(f)

# Load indicators
INDICATORS_PATH = Path(__file__).parent / "config" / "indicators.json"
with open(INDICATORS_PATH, "r") as f:
    INDICATORS = json.load(f)

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def execute_sql(query: str, warehouse_id: str = None):
    """Execute SQL via Statement Execution API and return results."""
    wh = warehouse_id or WAREHOUSE_ID
    if not wh:
        raise HTTPException(status_code=500, detail="DATABRICKS_WAREHOUSE_ID not set")

    response = w.statement_execution.execute_statement(
        warehouse_id=wh,
        statement=query,
        wait_timeout="60s",
    )
    if response.status and response.status.state and response.status.state.value == "FAILED":
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        raise HTTPException(status_code=500, detail=f"SQL error: {error_msg}")

    columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
    rows = []
    if response.result and response.result.data_array:
        rows = [dict(zip(columns, row)) for row in response.result.data_array]
    return {"columns": columns, "rows": rows}


# ──────────────────────────────────────────────
# API: General
# ──────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    return {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "tables": TABLES,
        "monte_carlo": MC_CONFIG,
        "model": MODEL_CONFIG,
        "portfolio_count": len(PORTFOLIO),
        "indicator_count": len(INDICATORS),
    }


@app.get("/api/portfolio")
def get_portfolio():
    return PORTFOLIO


@app.get("/api/indicators")
def get_indicators():
    return INDICATORS


# ──────────────────────────────────────────────
# API: 1. Market ETL - Data Lineage
# ──────────────────────────────────────────────

@app.get("/api/etl/stocks")
def get_stocks(ticker: Optional[str] = None, limit: int = Query(default=500, le=5000)):
    table = f"{CATALOG}.{SCHEMA}.{TABLES['stocks']}"
    where = f"WHERE ticker = '{ticker}'" if ticker else ""
    query = f"SELECT * FROM {table} {where} ORDER BY date DESC LIMIT {limit}"
    return execute_sql(query)


@app.get("/api/etl/stocks/summary")
def get_stocks_summary():
    table = f"{CATALOG}.{SCHEMA}.{TABLES['stocks']}"
    query = f"""
        SELECT ticker,
               COUNT(*) as record_count,
               MIN(date) as min_date,
               MAX(date) as max_date,
               ROUND(AVG(close), 2) as avg_close,
               ROUND(STDDEV(close), 4) as std_close
        FROM {table}
        GROUP BY ticker
        ORDER BY ticker
    """
    return execute_sql(query)


@app.get("/api/etl/indicators")
def get_indicators_data(limit: int = Query(default=500, le=5000)):
    table = f"{CATALOG}.{SCHEMA}.{TABLES['indicators']}"
    query = f"SELECT * FROM {table} ORDER BY date DESC LIMIT {limit}"
    return execute_sql(query)


@app.get("/api/etl/lineage")
def get_lineage():
    """Return data lineage information for the ETL pipeline."""
    return {
        "nodes": [
            {"id": "yfinance", "label": "Yahoo Finance API", "type": "source"},
            {"id": "portfolio", "label": "Portfolio Config (27 stocks)", "type": "config"},
            {"id": "indicators_src", "label": "Market Indicators Config", "type": "config"},
            {"id": "stocks_raw", "label": "Raw Stock Data", "type": "transform"},
            {"id": "dummy_check", "label": "Data Validation & Dummy Generation", "type": "transform"},
            {"id": "stocks_table", "label": f"{TABLES['stocks']}", "type": "table"},
            {"id": "indicators_raw", "label": "Raw Indicator Data", "type": "transform"},
            {"id": "indicators_table", "label": f"{TABLES['indicators']}", "type": "table"},
            {"id": "log_returns", "label": "Log Returns Calculation", "type": "transform"},
            {"id": "volatility_window", "label": f"Sliding Window ({MC_CONFIG['volatility']} days)", "type": "transform"},
            {"id": "volatility_table", "label": f"{TABLES['volatility']}", "type": "table"},
        ],
        "edges": [
            {"from": "yfinance", "to": "stocks_raw"},
            {"from": "portfolio", "to": "stocks_raw"},
            {"from": "stocks_raw", "to": "dummy_check"},
            {"from": "dummy_check", "to": "stocks_table"},
            {"from": "yfinance", "to": "indicators_raw"},
            {"from": "indicators_src", "to": "indicators_raw"},
            {"from": "indicators_raw", "to": "indicators_table"},
            {"from": "indicators_table", "to": "log_returns"},
            {"from": "log_returns", "to": "volatility_window"},
            {"from": "volatility_window", "to": "volatility_table"},
        ],
    }


# ──────────────────────────────────────────────
# API: 2. Model - MLflow metadata
# ──────────────────────────────────────────────

@app.get("/api/model/info")
def get_model_info():
    """Get model metadata from Unity Catalog."""
    uc_model_name = f"{CATALOG}.{SCHEMA}.{MODEL_CONFIG['name']}"
    try:
        model = w.registered_models.get(full_name=uc_model_name)
        return {
            "name": uc_model_name,
            "description": model.comment or "VaR prediction model using non-linear features + OLS",
            "created_at": str(model.created_at) if model.created_at else None,
            "updated_at": str(model.updated_at) if model.updated_at else None,
            "owner": model.owner,
        }
    except Exception as e:
        return {"name": uc_model_name, "error": str(e)}


@app.get("/api/model/versions")
def get_model_versions():
    """Get model versions from Unity Catalog."""
    uc_model_name = f"{CATALOG}.{SCHEMA}.{MODEL_CONFIG['name']}"
    try:
        versions = list(w.model_versions.list(full_name=uc_model_name))
        return [
            {
                "version": v.version,
                "created_at": str(v.created_at) if v.created_at else None,
                "status": v.status.value if v.status else None,
                "aliases": [a.alias_name for a in (v.aliases or [])],
                "run_id": v.run_id,
            }
            for v in versions
        ]
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/model/accuracy")
def get_model_accuracy():
    """Get per-ticker model accuracy (WSSE) from precomputed data."""
    table = f"{CATALOG}.{SCHEMA}.{TABLES['stocks']}"
    query = f"""
        SELECT ticker, COUNT(*) as data_points
        FROM {table}
        GROUP BY ticker
        ORDER BY ticker
    """
    return execute_sql(query)


# ──────────────────────────────────────────────
# API: 3. Monte Carlo Simulation
# ──────────────────────────────────────────────

class MonteCarloParams(BaseModel):
    runs: int = 32000
    volatility_window: int = 90
    confidence_level: int = 99


@app.get("/api/montecarlo/market")
def get_mc_market(limit: int = Query(default=100, le=5000)):
    table = f"{CATALOG}.{SCHEMA}.{TABLES['mc_market']}"
    query = f"SELECT date, trial_id FROM {table} LIMIT {limit}"
    return execute_sql(query)


@app.get("/api/montecarlo/trials/summary")
def get_mc_trials_summary():
    table = f"{CATALOG}.{SCHEMA}.{TABLES['mc_trials']}"
    query = f"""
        SELECT date, ticker, size(returns) as num_trials
        FROM {table}
        ORDER BY date, ticker
        LIMIT 500
    """
    return execute_sql(query)


@app.get("/api/montecarlo/var")
def get_mc_var(confidence: int = Query(default=99, ge=90, le=99)):
    """Calculate VaR from monte carlo results for each date."""
    trials_table = f"{CATALOG}.{SCHEMA}.{TABLES['mc_trials']}"
    query = f"""
        WITH portfolio_returns AS (
            SELECT t.date,
                   aggregate(
                       transform(
                           sequence(0, size(t.returns)-1),
                           i -> t.returns[i] * p.weight
                       ),
                       DOUBLE(0),
                       (acc, x) -> acc + x
                   ) as weighted_return
            FROM {trials_table} t
            JOIN (
                SELECT ticker, weight FROM VALUES
                {', '.join([f"('{s['ticker']}', {s['weight']})" for s in PORTFOLIO])}
                AS p(ticker, weight)
            ) p ON t.ticker = p.ticker
        )
        SELECT date,
               ROUND(percentile_approx(weighted_return, {(100-confidence)/100.0}), 6) as var_value,
               COUNT(*) as num_simulations
        FROM portfolio_returns
        GROUP BY date
        ORDER BY date
    """
    return execute_sql(query)


@app.get("/api/montecarlo/params")
def get_mc_params():
    return {
        "runs": MC_CONFIG["runs"],
        "volatility_window": MC_CONFIG["volatility"],
        "executors": MC_CONFIG["executors"],
        "model_date": MODEL_CONFIG["date"],
        "data_range": {
            "min": APP_CONFIG["yfinance"]["mindate"],
            "max": APP_CONFIG["yfinance"]["maxdate"],
        },
    }


# ──────────────────────────────────────────────
# API: 4. Aggregation (Dashboard Embed)
# ──────────────────────────────────────────────

@app.get("/api/aggregation/var_by_date")
def get_var_by_date():
    trials_table = f"{CATALOG}.{SCHEMA}.{TABLES['mc_trials']}"
    query = f"""
        WITH portfolio_returns AS (
            SELECT t.date,
                   aggregate(
                       transform(
                           sequence(0, size(t.returns)-1),
                           i -> t.returns[i] * p.weight
                       ),
                       DOUBLE(0),
                       (acc, x) -> acc + x
                   ) as weighted_return
            FROM {trials_table} t
            JOIN (
                SELECT ticker, weight FROM VALUES
                {', '.join([f"('{s['ticker']}', {s['weight']})" for s in PORTFOLIO])}
                AS p(ticker, weight)
            ) p ON t.ticker = p.ticker
        )
        SELECT date,
               ROUND(percentile_approx(weighted_return, 0.01), 6) as var_99
        FROM portfolio_returns
        GROUP BY date
        ORDER BY date
    """
    return execute_sql(query)


@app.get("/api/aggregation/var_by_country")
def get_var_by_country():
    trials_table = f"{CATALOG}.{SCHEMA}.{TABLES['mc_trials']}"
    query = f"""
        WITH portfolio_returns AS (
            SELECT t.date, p.country,
                   aggregate(
                       transform(
                           sequence(0, size(t.returns)-1),
                           i -> t.returns[i] * p.weight
                       ),
                       DOUBLE(0),
                       (acc, x) -> acc + x
                   ) as weighted_return
            FROM {trials_table} t
            JOIN (
                SELECT ticker, country, weight FROM VALUES
                {', '.join([f"('{s['ticker']}', '{s['country']}', {s['weight']})" for s in PORTFOLIO])}
                AS p(ticker, country, weight)
            ) p ON t.ticker = p.ticker
        )
        SELECT date, country,
               ROUND(percentile_approx(weighted_return, 0.01), 6) as var_99
        FROM portfolio_returns
        GROUP BY date, country
        ORDER BY date, country
    """
    return execute_sql(query)


# ──────────────────────────────────────────────
# API: 5. Compliance
# ──────────────────────────────────────────────

@app.get("/api/compliance/backtest")
def get_backtest_data():
    stocks_table = f"{CATALOG}.{SCHEMA}.{TABLES['stocks']}"
    query = f"""
        WITH daily_returns AS (
            SELECT date,
                   SUM(log_return * weight) as portfolio_return
            FROM (
                SELECT s.date, s.ticker, p.weight,
                       LN(s.close / LAG(s.close) OVER (PARTITION BY s.ticker ORDER BY s.date)) as log_return
                FROM {stocks_table} s
                JOIN (
                    SELECT ticker, weight FROM VALUES
                    {', '.join([f"('{s['ticker']}', {s['weight']})" for s in PORTFOLIO])}
                    AS p(ticker, weight)
                ) p ON s.ticker = p.ticker
                WHERE s.close IS NOT NULL
            )
            WHERE log_return IS NOT NULL
            GROUP BY date
        )
        SELECT date, ROUND(portfolio_return, 6) as portfolio_return
        FROM daily_returns
        ORDER BY date
    """
    return execute_sql(query)


@app.get("/api/compliance/breaches")
def get_breach_summary():
    """Get Basel compliance zone summary."""
    return {
        "zones": [
            {"level": "Green", "threshold": "4 breaches or less", "result": "No concern", "color": "#22c55e"},
            {"level": "Yellow", "threshold": "9 breaches or less", "result": "Monitoring required", "color": "#eab308"},
            {"level": "Red", "threshold": "10+ breaches", "result": "VaR improvement needed", "color": "#ef4444"},
        ]
    }


# ──────────────────────────────────────────────
# API: Jobs
# ──────────────────────────────────────────────

@app.get("/api/jobs/list")
def list_jobs():
    """List VaR-related jobs."""
    try:
        jobs = w.jobs.list(name="var")
        return [
            {
                "job_id": j.job_id,
                "name": j.settings.name if j.settings else None,
                "created_time": str(j.created_time) if j.created_time else None,
            }
            for j in jobs
        ]
    except Exception as e:
        return {"error": str(e)}


class JobRunRequest(BaseModel):
    job_id: int
    notebook_params: dict = {}


@app.post("/api/jobs/run")
def run_job(req: JobRunRequest):
    """Trigger a job run."""
    try:
        run = w.jobs.run_now(job_id=req.job_id, notebook_params=req.notebook_params)
        return {"run_id": run.run_id, "status": "triggered"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/runs/{job_id}")
def get_job_runs(job_id: int, limit: int = Query(default=10, le=50)):
    """Get recent runs for a job."""
    try:
        runs = w.jobs.list_runs(job_id=job_id, limit=limit)
        return [
            {
                "run_id": r.run_id,
                "state": r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else None,
                "result_state": r.state.result_state.value if r.state and r.state.result_state else None,
                "start_time": str(r.start_time) if r.start_time else None,
                "end_time": str(r.end_time) if r.end_time else None,
                "run_page_url": r.run_page_url,
            }
            for r in runs
        ]
    except Exception as e:
        return {"error": str(e)}


# ──────────────────────────────────────────────
# API: Genie
# ──────────────────────────────────────────────

class GenieRequest(BaseModel):
    space_id: str
    question: str


@app.post("/api/genie/ask")
def ask_genie(req: GenieRequest):
    """Start a Genie conversation with a question."""
    try:
        conversation = w.genie.start_conversation(
            space_id=req.space_id,
            content=req.question,
        )
        return {
            "conversation_id": conversation.conversation_id,
            "message_id": conversation.message_id,
            "status": "started",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/genie/result/{space_id}/{conversation_id}/{message_id}")
def get_genie_result(space_id: str, conversation_id: str, message_id: str):
    """Get Genie conversation result."""
    try:
        message = w.genie.get_message(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )
        result = {}
        if message.attachments:
            for att in message.attachments:
                if att.text:
                    result["text"] = att.text.content
                if att.query:
                    result["query"] = att.query.query
                    result["description"] = att.query.description
        return {
            "status": message.status.value if message.status else None,
            "result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────────
# Static files (React build)
# ──────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent / "frontend" / "dist"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")
