# Databricks notebook source
# MAGIC %md
# MAGIC # モンテカルロシミュレーション (RiskLens VaR App用)
# MAGIC チェックポイント付き。途中結果をDeltaテーブルに書き出し、Appがポーリングでヒストグラム更新。

# COMMAND ----------

# MAGIC %pip install tempo mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "skotani_var")
dbutils.widgets.text("schema", "var_app")
dbutils.widgets.text("model_name", "value_at_risk")
dbutils.widgets.text("model_date", "2026-04-01")
dbutils.widgets.text("max_date", "2026-05-01")
dbutils.widgets.text("mc_runs", "5000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_name = dbutils.widgets.get("model_name")
model_date = dbutils.widgets.get("model_date")
max_date = dbutils.widgets.get("max_date")
mc_runs = int(dbutils.widgets.get("mc_runs"))

fqn = lambda t: f"{catalog}.{schema}.{t}"
uc_model_name = f"{catalog}.{schema}.{model_name}"
print(f"Config: {catalog}.{schema}, Model: {uc_model_name}, Runs: {mc_runs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ボラティリティ統計量

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
ret_pdf.index = ret_pdf['date']
factor_cols = ['sp500', 'nyse', 'oil', 'treasury', 'dowjones']
ret_pdf = ret_pdf[factor_cols].astype(float)

vol_avg = ret_pdf.mean().values
vol_cov = ret_pdf.cov().values
print(f"Vol avg: {vol_avg}, Cov shape: {vol_cov.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

first = datetime.datetime.strptime(model_date, '%Y-%m-%d')
today = datetime.datetime.strptime(max_date, '%Y-%m-%d')
run_dates = pd.date_range(first, today, freq='w')

portfolio_df = spark.table(fqn("portfolio"))
dates_sdf = spark.createDataFrame(pd.DataFrame(run_dates, columns=['date']))

print(f"Simulation: {len(run_dates)} weeks x {mc_runs} trials")

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルロード & UDF

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

model_udf = mlflow.pyfunc.spark_udf(
    model_uri=f'models:/{uc_model_name}@champion',
    result_type='float',
    spark=spark,
    env_manager="conda"
)

@udf('array<float>')
def simulate_market(seed):
    rng = np.random.default_rng(int(seed))
    return rng.multivariate_normal(vol_avg, vol_cov).tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## チェックポイント付きシミュレーション
# MAGIC 1000 trial ごとにヒストグラムをDeltaに書き出し

# COMMAND ----------

# チェックポイントテーブル初期化
spark.sql(f"CREATE OR REPLACE TABLE {fqn('mc_checkpoint')} (bucket DOUBLE, frequency BIGINT, total_trials INT, checkpoint_id INT)")

# 1000回ごとにチェックポイント
checkpoint_interval = 1000
checkpoints = list(range(checkpoint_interval, mc_runs + 1, checkpoint_interval))
if not checkpoints or checkpoints[-1] != mc_runs:
    checkpoints.append(mc_runs)

all_returns = []
print(f"Checkpoints: {checkpoints}")

for cp_idx, cp_end in enumerate(checkpoints):
    cp_start = checkpoints[cp_idx - 1] if cp_idx > 0 else 0

    seed_pdf = pd.DataFrame(list(np.arange(cp_start, cp_end)), columns=['trial_id'])

    # 市場条件生成 → モデル予測 → ポートフォリオリターン
    batch_returns_df = (
        dates_sdf
        .join(spark.createDataFrame(seed_pdf))
        .withColumn('features', simulate_market('trial_id'))
        .join(portfolio_df.select("ticker", "weight"))
        .withColumn('pred_return', model_udf(F.struct('ticker', 'features')))
        .groupBy('trial_id')
        .agg(F.sum(F.col('pred_return') * F.col('weight')).alias('portfolio_return'))
        .select('portfolio_return')
    )

    # toPandas で取得（collectより安定）
    batch_pdf = batch_returns_df.toPandas()
    batch_returns = batch_pdf['portfolio_return'].astype(float).tolist()
    all_returns.extend(batch_returns)

    # チェックポイント: ヒストグラム書き出し
    returns_arr = np.array(all_returns)
    n_bins = 60
    hist_counts, bin_edges = np.histogram(returns_arr, bins=n_bins)
    bin_centers = ((bin_edges[:-1] + bin_edges[1:]) / 2).tolist()

    checkpoint_pdf = pd.DataFrame({
        'bucket': bin_centers,
        'frequency': hist_counts.tolist(),
        'total_trials': [cp_end] * n_bins,
        'checkpoint_id': [cp_idx + 1] * n_bins,
    })
    spark.createDataFrame(checkpoint_pdf).write.mode("overwrite").format("delta").saveAsTable(fqn("mc_checkpoint"))

    var99 = np.percentile(returns_arr, 1)
    print(f"  Checkpoint {cp_idx+1}/{len(checkpoints)}: {cp_end}/{mc_runs} trials, VaR99={var99:.6f}")

print(f"✅ Simulation done: {len(all_returns)} portfolio returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ベクトル化 & 永続化

# COMMAND ----------

from pyspark.ml.linalg import Vectors, VectorUDT

full_seed = pd.DataFrame(list(np.arange(0, mc_runs)), columns=['trial_id'])
full_conditions = (
    dates_sdf
    .join(spark.createDataFrame(full_seed))
    .withColumn('features', simulate_market('trial_id'))
    .select('date', 'features', 'trial_id')
)

simulations = (
    full_conditions
    .join(portfolio_df.select("ticker"))
    .withColumn('return', model_udf(F.struct('ticker', 'features')))
    .drop('features')
)

@udf(VectorUDT())
def to_vector(xs, ys):
    v = Vectors.sparse(mc_runs, zip(xs, ys)).toArray()
    return Vectors.dense(v)

(simulations
 .groupBy('date', 'ticker')
 .agg(F.collect_list('trial_id').alias('xs'), F.collect_list('return').alias('ys'))
 .select('date', 'ticker', to_vector('xs', 'ys').alias('returns'))
 .write.mode("overwrite").format("delta").saveAsTable(fqn("monte_carlo_trials")))

spark.sql(f"ALTER TABLE {fqn('monte_carlo_trials')} CLUSTER BY (`date`, `ticker`)")
spark.sql(f"OPTIMIZE {fqn('monte_carlo_trials')}")
print("✅ monte_carlo_trials saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation & Compliance 事前計算

# COMMAND ----------

from pyspark.ml.stat import Summarizer

@udf(VectorUDT())
def weighted_returns(returns, weight):
    return Vectors.dense(returns.toArray() * weight)

@udf('float')
def get_var_udf(simulations, var):
    return float(np.percentile(simulations, 100 - var))

trials_df = spark.read.table(fqn("monte_carlo_trials"))
simulation_df = trials_df.join(portfolio_df, ['ticker']).withColumn('weighted_returns', weighted_returns('returns', 'weight'))

# VaR timeseries
(simulation_df.groupBy('date')
 .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
 .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
 .drop('returns').orderBy('date')
 .write.mode("overwrite").format("delta").saveAsTable(fqn("var_timeseries")))
print("✅ var_timeseries")

# VaR by country
(simulation_df.groupBy('date', 'country')
 .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
 .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
 .drop('returns').orderBy('date')
 .write.mode("overwrite").format("delta").saveAsTable(fqn("var_by_country")))
print("✅ var_by_country")

# Compliance
w_part = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)
inv_returns = (
    spark.table(fqn("market_data")).filter(F.col('close').isNotNull())
    .join(portfolio_df, ['ticker'])
    .withColumn("first", F.first('close').over(w_part))
    .withColumn("return", F.log(F.col("close") / F.col("first")))
    .withColumn("weighted_return", F.col('return') * F.col('weight'))
    .groupBy('date').agg(F.sum('weighted_return').alias('portfolio_return'))
)
(inv_returns.join(spark.table(fqn("var_timeseries")), "date", "left")
 .withColumn("is_breach", F.when(F.col("portfolio_return") < F.col("var_99"), 1).otherwise(0))
 .filter(F.col("portfolio_return").isNotNull()).orderBy("date")
 .write.mode("overwrite").format("delta").saveAsTable(fqn("compliance_backtest")))
print("✅ compliance_backtest")

# COMMAND ----------

print(f"✅ All done! {mc_runs} trials x {len(run_dates)} dates")
