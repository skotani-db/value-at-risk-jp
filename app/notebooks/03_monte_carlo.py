# Databricks notebook source
# MAGIC %md
# MAGIC # モンテカルロシミュレーション (RiskLens VaR App用)
# MAGIC 03_var_monte_carlo.py 準拠。日付ごとのスライディングウィンドウボラティリティ + チェックポイント付き。

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "skotani_var")
dbutils.widgets.text("schema", "var_app")
dbutils.widgets.text("model_name", "value_at_risk")
dbutils.widgets.text("model_date", "2026-04-01")
dbutils.widgets.text("max_date", "2026-05-01")
dbutils.widgets.text("mc_runs", "5000")
dbutils.widgets.text("vol_window", "90")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_name = dbutils.widgets.get("model_name")
model_date = dbutils.widgets.get("model_date")
max_date = dbutils.widgets.get("max_date")
mc_runs = int(dbutils.widgets.get("mc_runs"))
vol_window = int(dbutils.widgets.get("vol_window"))

fqn = lambda t: f"{catalog}.{schema}.{t}"
uc_model_name = f"{catalog}.{schema}.{model_name}"
print(f"Config: {catalog}.{schema}, Model: {uc_model_name}")
print(f"Runs: {mc_runs}, Vol window: {vol_window} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 日付ごとのボラティリティ統計量 (元ノートブック準拠)
# MAGIC 各シミュレーション日について直近N日のスライディングウィンドウで平均・共分散を計算

# COMMAND ----------

import datetime
import pandas as pd
import numpy as np
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import *

indicators_df = spark.table(fqn("market_indicators")).orderBy("date")
w_ord = Window.orderBy("date")

indicator_returns = indicators_df.select(
    F.col("date"),
    F.log(F.col("SP500") / F.lag("SP500").over(w_ord)).alias("sp500"),
    F.log(F.col("NYSE") / F.lag("NYSE").over(w_ord)).alias("nyse"),
    F.log(F.col("OIL") / F.lag("OIL").over(w_ord)).alias("oil"),
    F.log(F.col("TREASURY") / F.lag("TREASURY").over(w_ord)).alias("treasury"),
    F.log(F.col("DOWJONES") / F.lag("DOWJONES").over(w_ord)).alias("dowjones"),
).filter(F.col("sp500").isNotNull())

ret_pdf = indicator_returns.orderBy("date").toPandas()
ret_pdf['date'] = pd.to_datetime(ret_pdf['date'])
ret_pdf = ret_pdf.set_index('date')
factor_cols = ['sp500', 'nyse', 'oil', 'treasury', 'dowjones']
ret_pdf[factor_cols] = ret_pdf[factor_cols].astype(float)

# シミュレーション対象日（毎週）
first = datetime.datetime.strptime(model_date, '%Y-%m-%d')
today = datetime.datetime.strptime(max_date, '%Y-%m-%d')
run_dates = pd.date_range(first, today, freq='w')

# 日付ごとに直近vol_window日の平均・共分散を計算
date_vol_stats = {}
for rd in run_dates:
    window_data = ret_pdf[ret_pdf.index <= rd].tail(vol_window)[factor_cols]
    if len(window_data) > 5:
        date_vol_stats[rd] = {
            'avg': window_data.mean().values,
            'cov': window_data.cov().values,
        }

print(f"Computed volatility for {len(date_vol_stats)} / {len(run_dates)} dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

portfolio_df = spark.table(fqn("portfolio"))
print(f"Simulation: {len(date_vol_stats)} weeks x {mc_runs} trials x {portfolio_df.count()} tickers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルロード

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

model_udf = mlflow.pyfunc.spark_udf(
    model_uri=f'models:/{uc_model_name}@champion',
    result_type='float',
    spark=spark,
    env_manager="local"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## チェックポイント付きシミュレーション
# MAGIC 1000 trial ごとにヒストグラムをDeltaに書き出し。日付ごとに異なるボラティリティ分布を使用。

# COMMAND ----------

# チェックポイントテーブル初期化
spark.sql(f"DROP TABLE IF EXISTS {fqn('mc_checkpoint')}")
spark.sql(f"CREATE TABLE {fqn('mc_checkpoint')} (bucket DOUBLE, frequency LONG, total_trials LONG, checkpoint_id LONG)")

checkpoint_interval = 1000
checkpoints = list(range(checkpoint_interval, mc_runs + 1, checkpoint_interval))
if not checkpoints or checkpoints[-1] != mc_runs:
    checkpoints.append(mc_runs)

# 全日付×全trialのポートフォリオリターンを蓄積（ヒストグラム用はdate集約）
all_portfolio_returns = []  # (date, trial_id, portfolio_return)
sorted_dates = sorted(date_vol_stats.keys())

print(f"Checkpoints: {checkpoints}")

for cp_idx, cp_end in enumerate(checkpoints):
    cp_start = checkpoints[cp_idx - 1] if cp_idx > 0 else 0
    trial_ids = list(range(cp_start, cp_end))

    # 日付ごとに異なる分布からサンプリング
    batch_rows = []
    for rd in sorted_dates:
        stats = date_vol_stats[rd]
        for tid in trial_ids:
            rng = np.random.default_rng(hash((str(rd), tid)) % (2**31))
            features = rng.multivariate_normal(stats['avg'], stats['cov']).tolist()
            batch_rows.append((rd.to_pydatetime(), int(tid), features))

    # Sparkで予測
    batch_schema = StructType([
        StructField('date', TimestampType()),
        StructField('trial_id', IntegerType()),
        StructField('features', ArrayType(FloatType())),
    ])
    batch_sdf = spark.createDataFrame(batch_rows, schema=batch_schema)

    batch_returns = (
        batch_sdf
        .join(portfolio_df.select("ticker", "weight"))
        .withColumn('pred_return', model_udf(F.struct('ticker', 'features')))
        .groupBy('date', 'trial_id')
        .agg(F.sum(F.col('pred_return') * F.col('weight')).alias('portfolio_return'))
    ).toPandas()

    all_portfolio_returns.append(batch_returns)

    # ヒストグラム: 全蓄積分を日付集約してポートフォリオリターンの分布を描画
    all_pdf = pd.concat(all_portfolio_returns, ignore_index=True)
    returns_arr = all_pdf['portfolio_return'].astype(float).values
    n_bins = 60
    hist_counts, bin_edges = np.histogram(returns_arr, bins=n_bins)
    bin_centers = ((bin_edges[:-1] + bin_edges[1:]) / 2).tolist()

    checkpoint_pdf = pd.DataFrame({
        'bucket': [float(x) for x in bin_centers],
        'frequency': [int(x) for x in hist_counts.tolist()],
        'total_trials': [int(cp_end)] * n_bins,
        'checkpoint_id': [int(cp_idx + 1)] * n_bins,
    })
    cp_schema = StructType([
        StructField('bucket', DoubleType()),
        StructField('frequency', LongType()),
        StructField('total_trials', LongType()),
        StructField('checkpoint_id', LongType()),
    ])
    spark.createDataFrame(checkpoint_pdf, schema=cp_schema).write.mode("overwrite").format("delta").saveAsTable(fqn("mc_checkpoint"))

    var99 = np.percentile(returns_arr, 1)
    print(f"  Checkpoint {cp_idx+1}/{len(checkpoints)}: {cp_end}/{mc_runs} trials, VaR99={var99:.6f}")

print(f"✅ Simulation done: {len(all_pdf)} total date x trial returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ベクトル化 & 永続化

# COMMAND ----------

from pyspark.ml.linalg import Vectors, VectorUDT

all_pdf = pd.concat(all_portfolio_returns, ignore_index=True)

# 全シミュレーション結果をSparkに戻す
sim_schema = StructType([
    StructField('date', TimestampType()),
    StructField('trial_id', IntegerType()),
    StructField('portfolio_return', DoubleType()),
])
all_pdf['portfolio_return'] = all_pdf['portfolio_return'].astype(float)
sim_sdf = spark.createDataFrame(all_pdf, schema=sim_schema)

# 日付ごとに集約してDelta保存（軽量形式）
sim_sdf.write.mode("overwrite").format("delta").saveAsTable(fqn("mc_sim_results"))
print("✅ mc_sim_results saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation テーブル事前計算

# COMMAND ----------

sim_df = spark.table(fqn("mc_sim_results"))

# VaR timeseries (日付ごとのVaR99)
var_ts = sim_df.groupBy("date").agg(
    F.expr("CAST(percentile(portfolio_return, 0.01) AS DOUBLE)").alias("var_99"),
    F.count("*").alias("num_trials"),
)
spark.sql(f"DROP TABLE IF EXISTS {fqn('var_timeseries')}")
var_ts.orderBy("date").write.format("delta").saveAsTable(fqn("var_timeseries"))
print("✅ var_timeseries")

# 日付ごと・国ごとのVaRは、ポートフォリオリターン全体からは直接出せないので
# ticker レベルのシミュレーション結果が必要。簡易版として、全体VaRのみ。
# 国別は全体のウェイト比率で按分
portfolio_pd = portfolio_df.toPandas()
country_weights = portfolio_pd.groupby('country')['weight'].sum()

var_ts_pd = var_ts.toPandas()
country_rows = []
for _, row in var_ts_pd.iterrows():
    for country, cw in country_weights.items():
        country_rows.append({
            'date': row['date'],
            'country': country,
            'var_99': float(row['var_99']) * cw / country_weights.sum(),
        })

spark.sql(f"DROP TABLE IF EXISTS {fqn('var_by_country')}")
spark.createDataFrame(pd.DataFrame(country_rows)).write.format("delta").saveAsTable(fqn("var_by_country"))
print("✅ var_by_country")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance backtest

# COMMAND ----------

w_part = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)
inv_returns = (
    spark.table(fqn("market_data")).filter(F.col('close').isNotNull())
    .join(portfolio_df, ['ticker'])
    .withColumn("first", F.first('close').over(w_part))
    .withColumn("return", F.log(F.col("close") / F.col("first")))
    .withColumn("weighted_return", F.col('return') * F.col('weight'))
    .groupBy('date').agg(F.sum('weighted_return').alias('portfolio_return'))
)

var_ts_df = spark.table(fqn("var_timeseries"))
spark.sql(f"DROP TABLE IF EXISTS {fqn('compliance_backtest')}")
(inv_returns.join(var_ts_df, "date", "left")
 .withColumn("is_breach", F.when(F.col("portfolio_return") < F.col("var_99"), 1).otherwise(0))
 .filter(F.col("portfolio_return").isNotNull()).orderBy("date")
 .write.format("delta").saveAsTable(fqn("compliance_backtest")))
print("✅ compliance_backtest")

# COMMAND ----------

print(f"✅ All done! {mc_runs} trials x {len(sorted_dates)} dates")
