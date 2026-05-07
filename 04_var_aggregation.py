# Databricks notebook source
# MAGIC %md
# MAGIC # VaR集計
# MAGIC 本ノートブックでは、**Delta Lake**上のモンテカルロシミュレーションの多用途な性質を実演します。最も細かい粒度で保存されたデータにより、アナリストは**Spark ML**の集約ベクトル関数を使用して、データを自在にスライス＆ダイスし、オンデマンドでバリュー・アット・リスクを集計する柔軟性を持ちます。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

from utils.var_udf import weighted_returns
trials_df = spark.read.table(config['database']['tables']['mc_trials'])
simulation_df = (
  trials_df
    .join(spark.createDataFrame(portfolio_df), ['ticker'])
    .withColumn('weighted_returns', weighted_returns('returns', 'weight'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ポイント・イン・タイムVaR
# MAGIC すべてのシミュレーションが最細粒度で保存されているため、特定の日のスライスにアクセスし、シンプルな分位関数として関連するバリュー・アット・リスクを取得できます。Spark MLの組み込み関数`Summarizer`を使用して、ポートフォリオ全体の試行ベクトルを集約します。

# COMMAND ----------

from pyspark.sql import functions as F
min_date = trials_df.select(F.min('date').alias('date')).toPandas().iloc[0].date

# COMMAND ----------

from pyspark.ml.stat import Summarizer

point_in_time_vector = (
  simulation_df
    .filter(F.col('date') == min_date)
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .toPandas().iloc[0].returns.toArray()
)

# COMMAND ----------

from utils.var_viz import plot_var
plot_var(point_in_time_vector, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC ## リスクエクスポージャーの推移
# MAGIC 同様のことを、取引履歴全体にわたってスケールで実現できます。各日付において、すべての試行ベクトルを集約し、最悪の1%のイベントを抽出します。

# COMMAND ----------

from utils.var_udf import get_var_udf

risk_exposure = (
  simulation_df
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

# COMMAND ----------

import matplotlib.pyplot as plt
plt.figure(figsize=(20,8))
plt.plot(risk_exposure['date'], risk_exposure['var_99'])
plt.title('ポートフォリオ全体のVaR')
plt.ylabel('バリュー・アット・リスク')
plt.xlabel('日付')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## スライス＆ダイス
# MAGIC モンテカルロデータを最細粒度で保持する主な利点は、異なるセグメント、業種、国別にスライスおよびダイスして可視化できることです。最適化されたDeltaテーブルを使用することで、ポートフォリオマネージャーやリスクアナリストは、What-Ifシナリオやアドホック分析（例：運用国別のバリュー・アット・リスク集計）を効率的に実行できます。

# COMMAND ----------

risk_exposure_country = (
  simulation_df
    .groupBy('date', 'country')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(20,8))
for label, df in risk_exposure_country.groupby('country'):
    df.plot.line(x='date', y='var_99', ax=ax, label=label)

plt.title('国別VaR')
plt.ylabel('バリュー・アット・リスク')
plt.xlabel('日付')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 同様に、特定の国における業種別リスク寄与度に変換できます。全体的なリスクのうち、鉱業への投資にどの程度関連しているか？ポートフォリオのリバランスによってこのエクスポージャーをどのように削減できるか？

# COMMAND ----------

risk_exposure_industry = (
  simulation_df
    .filter(F.col('country') == 'PERU')
    .groupBy('date', 'industry')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
    .toPandas()
)

# COMMAND ----------

import pandas as pd
import numpy as np
risk_contribution_country = pd.crosstab(risk_exposure_industry['date'], risk_exposure_industry['industry'], values=risk_exposure_industry['var_99'], aggfunc=np.sum)
risk_contribution_country = risk_contribution_country.div(risk_contribution_country.sum(axis=1), axis=0)
risk_contribution_country.plot.bar(figsize=(20,8), colormap="Pastel1", stacked=True, width=0.9)

# COMMAND ----------


