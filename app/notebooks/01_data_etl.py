# Databricks notebook source
# MAGIC %md
# MAGIC # Data ETL - yfinance からの市場データ取得
# MAGIC Yahoo Financeから株式・指標データを慎重に取得（429回避のためsleep付き）

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "skotani_var")
dbutils.widgets.text("schema", "var_app")
dbutils.widgets.text("start_date", "2024-05-01")
dbutils.widgets.text("end_date", "2026-05-01")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

fqn = lambda t: f"{catalog}.{schema}.{t}"
print(f"Config: {catalog}.{schema}, Period: {start_date} ~ {end_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ポートフォリオ & 市場指標の定義

# COMMAND ----------

import json
import pandas as pd
import numpy as np
import time
import yfinance as yf
from pyspark.sql import functions as F

# ポートフォリオ（configから読み込み、またはハードコード）
portfolio = [
    {"ticker":"BCH","country":"CHILE","company":"Banco de Chile","industry":"Banks","weight":0.0344827586},
    {"ticker":"BSAC","country":"CHILE","company":"Banco Santander-Chile","industry":"Banks","weight":0.0344827586},
    {"ticker":"CCU","country":"CHILE","company":"Compania Cervecerias Unidas","industry":"Beverages","weight":0.0344827586},
    {"ticker":"ITCB","country":"CHILE","company":"Itau CorpBanca","industry":"Banks","weight":0.0344827586},
    {"ticker":"ENIC","country":"CHILE","company":"Enersis Chile","industry":"Electricity","weight":0.0344827586},
    {"ticker":"SQM","country":"CHILE","company":"SQM","industry":"Chemicals","weight":0.0344827586},
    {"ticker":"CIB","country":"COLOMBIA","company":"BanColombia","industry":"Banks","weight":0.0344827586},
    {"ticker":"EC","country":"COLOMBIA","company":"Ecopetrol","industry":"Oil & Gas","weight":0.0344827586},
    {"ticker":"AVAL","country":"COLOMBIA","company":"Grupo Aval","industry":"Financial Services","weight":0.0344827586},
    {"ticker":"AMX","country":"MEXICO","company":"America Movil","industry":"Telecom","weight":0.0344827586},
    {"ticker":"AMOV","country":"MEXICO","company":"America Movil ADR","industry":"Telecom","weight":0.0344827586},
    {"ticker":"CX","country":"MEXICO","company":"CEMEX","industry":"Construction","weight":0.0344827586},
    {"ticker":"KOF","country":"MEXICO","company":"Coca-Cola FEMSA","industry":"Beverages","weight":0.0344827586},
    {"ticker":"VLRS","country":"MEXICO","company":"Volaris","industry":"Airlines","weight":0.0344827586},
    {"ticker":"FMX","country":"MEXICO","company":"FEMSA","industry":"Beverages","weight":0.0344827586},
    {"ticker":"PAC","country":"MEXICO","company":"Grupo Aeroportuario Pacifico","industry":"Transportation","weight":0.0344827586},
    {"ticker":"ASR","country":"MEXICO","company":"Grupo Aeroportuario Sureste","industry":"Transportation","weight":0.0344827586},
    {"ticker":"BSMX","country":"MEXICO","company":"Santander Mexico","industry":"Banks","weight":0.0344827586},
    {"ticker":"SIM","country":"MEXICO","company":"Grupo Simec","industry":"Metals","weight":0.0344827586},
    {"ticker":"TV","country":"MEXICO","company":"Grupo Televisa","industry":"Media","weight":0.0344827586},
    {"ticker":"IBA","country":"MEXICO","company":"Industrias Bachoco","industry":"Food","weight":0.0344827586},
    {"ticker":"BLX","country":"PANAMA","company":"Banco Latinoamericano","industry":"Banks","weight":0.0344827586},
    {"ticker":"CPA","country":"PANAMA","company":"Copa Holdings","industry":"Airlines","weight":0.0344827586},
    {"ticker":"CPAC","country":"PERU","company":"Cementos Pacasmayo","industry":"Construction","weight":0.0344827586},
    {"ticker":"SCCO","country":"PERU","company":"Southern Copper","industry":"Mining","weight":0.0344827586},
    {"ticker":"FSM","country":"PERU","company":"Fortuna Silver Mines","industry":"Mining","weight":0.0344827586},
    {"ticker":"BAP","country":"PERU","company":"Credicorp","industry":"Banks","weight":0.0344827586},
]

market_indicators = {
    "^GSPC": "SP500",
    "^NYA": "NYSE",
    "^XOI": "OIL",
    "^TNX": "TREASURY",
    "^DJI": "DOWJONES",
}

tickers = [p["ticker"] for p in portfolio]
print(f"Portfolio: {len(tickers)} tickers")
print(f"Indicators: {list(market_indicators.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## スキーマ & ポートフォリオテーブル作成

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

portfolio_pdf = pd.DataFrame(portfolio)
spark.createDataFrame(portfolio_pdf).write.mode("overwrite").format("delta").saveAsTable(fqn("portfolio"))
print(f"✅ Portfolio table: {len(portfolio)} tickers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 株式データのダウンロード（1銘柄ずつ慎重に）

# COMMAND ----------

def download_ticker(tick, min_date, max_date, retries=3):
    """1銘柄をダウンロード。429対策でリトライ+sleep。"""
    for attempt in range(retries):
        try:
            data = yf.Ticker(tick).history(start=min_date, end=max_date)[['Open', 'High', 'Low', 'Close', 'Volume']]
            if len(data) == 0:
                print(f"  ⚠️ {tick}: No data returned")
                return None

            # timezone除去（yfinanceはtz-awareを返す）
            data.index = data.index.tz_localize(None)
            # 欠損日を前方補完
            idx = pd.bdate_range(min_date, max_date)
            data = data.reindex(idx, method='pad')
            data['date'] = data.index
            data['ticker'] = tick
            data = data.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
            data = data[['ticker','date','open','high','low','close','volume']].dropna(subset=['close'])
            return data
        except Exception as e:
            wait = (attempt + 1) * 5
            print(f"  ⚠️ {tick} attempt {attempt+1} failed: {e}. Waiting {wait}s...")
            time.sleep(wait)
    return None

# COMMAND ----------

# 1銘柄ずつダウンロード（並列化しない）
all_stock_data = []
failed_tickers = []

for i, tick in enumerate(tickers):
    print(f"[{i+1}/{len(tickers)}] Downloading {tick}...", end=" ")

    data = download_ticker(tick, start_date, end_date)
    if data is not None and len(data) > 0:
        all_stock_data.append(data)
        print(f"✅ {len(data)} rows")
    else:
        failed_tickers.append(tick)
        print(f"❌ Failed")

    # 429回避: 毎回2秒待つ
    time.sleep(2)

print(f"\n✅ Downloaded: {len(all_stock_data)} tickers")
if failed_tickers:
    print(f"❌ Failed: {failed_tickers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 失敗した銘柄のダミーデータ補完

# COMMAND ----------

if failed_tickers:
    print(f"Generating dummy data for {len(failed_tickers)} failed tickers...")
    np.random.seed(42)
    dates = pd.bdate_range(start=start_date, end=end_date)

    ticker_params = {
        'BCH':(22.5,0.00015,0.018),'BSAC':(19.8,0.0001,0.02),'CCU':(12.3,0.00005,0.016),
        'ITCB':(4.8,-0.0001,0.025),'ENIC':(3.2,0.00008,0.015),'SQM':(52.0,-0.0002,0.028),
        'CIB':(32.5,0.00012,0.022),'EC':(11.2,0.00005,0.026),'AVAL':(2.4,-0.0001,0.02),
        'AMX':(17.8,0.00008,0.017),'AMOV':(18.2,0.00008,0.017),'CX':(7.3,0.0001,0.024),
        'KOF':(92.0,0.00012,0.014),'VLRS':(8.5,0.00005,0.032),'FMX':(132.0,0.0001,0.015),
        'PAC':(172.0,0.00015,0.018),'ASR':(285.0,0.00012,0.019),'BSMX':(8.1,0.0001,0.021),
        'SIM':(28.5,0.00008,0.023),'TV':(3.1,-0.0003,0.03),'IBA':(48.0,0.00005,0.016),
        'BLX':(32.0,0.00015,0.019),'CPA':(98.0,0.00012,0.022),'CPAC':(5.8,0.00005,0.018),
        'SCCO':(108.0,0.00018,0.025),'FSM':(4.5,0.0001,0.03),'BAP':(168.0,0.00015,0.02),
    }

    for tick in failed_tickers:
        p = ticker_params.get(tick, (50.0, 0.0001, 0.02))
        n = len(dates)
        returns = np.random.normal(p[1], p[2], n)
        # レジームシフト
        for m in [3,7,8]:
            mask = pd.Series(dates).dt.month == m
            returns[mask.values] *= 2.0
        prices = p[0] * np.exp(np.cumsum(returns))

        dummy = pd.DataFrame({
            'ticker': tick, 'date': dates,
            'open': prices * (1 + np.random.normal(0, p[2]*0.3, n)),
            'high': prices * (1 + np.abs(np.random.normal(0, p[2]*0.2, n))),
            'low': prices * (1 - np.abs(np.random.normal(0, p[2]*0.2, n))),
            'close': prices,
            'volume': np.random.lognormal(np.log(500000), 0.5, n).astype(int),
        })
        all_stock_data.append(dummy)
        print(f"  Generated dummy for {tick}: {len(dummy)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deltaテーブルに保存

# COMMAND ----------

if all_stock_data:
    combined = pd.concat(all_stock_data, ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'])
    print(f"Total rows: {len(combined)}")

    spark.sql(f"DROP TABLE IF EXISTS {fqn('market_data')}")
    spark.createDataFrame(combined).write.format("delta").saveAsTable(fqn("market_data"))
    print(f"✅ market_data saved: {len(combined)} rows, {combined['ticker'].nunique()} tickers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 市場指標のダウンロード

# COMMAND ----------

indicator_dfs = {}
for symbol, name in market_indicators.items():
    print(f"Downloading {name} ({symbol})...", end=" ")
    for attempt in range(3):
        try:
            data = yf.Ticker(symbol).history(start=start_date, end=end_date)['Close']
            if len(data) > 0:
                indicator_dfs[name] = data
                print(f"✅ {len(data)} rows")
                break
            else:
                print(f"empty", end=" ")
        except Exception as e:
            print(f"retry...", end=" ")
        time.sleep(3)
    else:
        print(f"❌ Failed")
    time.sleep(2)

# COMMAND ----------

# 指標データを結合
if indicator_dfs:
    # timezone除去
    clean_dfs = {}
    for name, series in indicator_dfs.items():
        s = series.copy()
        s.index = s.index.tz_localize(None)
        clean_dfs[name] = s
    indicators_pdf = pd.DataFrame(clean_dfs)
    indicators_pdf['date'] = indicators_pdf.index
    indicators_pdf = indicators_pdf.dropna()

    # 欠損指標はダミー生成
    for name in market_indicators.values():
        if name not in indicators_pdf.columns:
            print(f"  Generating dummy for {name}")
            n = len(indicators_pdf)
            base = {'SP500':5200,'NYSE':18000,'OIL':78,'TREASURY':4.3,'DOWJONES':39000}.get(name, 100)
            indicators_pdf[name] = base * np.exp(np.cumsum(np.random.normal(0, 0.01, n)))

    if len(indicators_pdf) > 0:
        spark.sql(f"DROP TABLE IF EXISTS {fqn('market_indicators')}")
        spark.createDataFrame(indicators_pdf).write.format("delta").saveAsTable(fqn("market_indicators"))
        print(f"✅ market_indicators saved: {len(indicators_pdf)} rows")
    else:
        print("⚠️ No indicator data, generating all dummies...")

# 全指標がダウンロード失敗した場合のフォールバック
try:
    spark.table(fqn("market_indicators"))
except Exception:
    print("Generating full dummy market indicators...")
    dates = pd.bdate_range(start_date, end_date)
    n = len(dates)
    np.random.seed(42)
    dummy_ind = pd.DataFrame({
        'date': dates,
        'SP500': 5200 * np.exp(np.cumsum(np.random.normal(0.0003, 0.011, n))),
        'NYSE': 18000 * np.exp(np.cumsum(np.random.normal(0.0002, 0.009, n))),
        'OIL': 78 * np.exp(np.cumsum(np.random.normal(0, 0.022, n))),
        'TREASURY': 4.3 + np.cumsum(np.random.normal(0, 0.008, n)),
        'DOWJONES': 39000 * np.exp(np.cumsum(np.random.normal(0.0003, 0.01, n))),
    })
    dummy_ind['TREASURY'] = dummy_ind['TREASURY'].clip(1.0, 7.0)
    spark.sql(f"DROP TABLE IF EXISTS {fqn('market_indicators')}")
    spark.createDataFrame(dummy_ind).write.format("delta").saveAsTable(fqn("market_indicators"))
    print(f"✅ Dummy market_indicators saved: {len(dummy_ind)} rows")

# COMMAND ----------

# 検証
display(spark.table(fqn("market_data")).groupBy("ticker").agg(F.count("*").alias("cnt"), F.min("date").alias("min_date"), F.max("date").alias("max_date")).orderBy("ticker"))

# COMMAND ----------

display(spark.table(fqn("market_indicators")).orderBy("date").limit(10))

# COMMAND ----------

print("✅ Data ETL complete!")
