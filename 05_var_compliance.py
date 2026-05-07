# Databricks notebook source
# MAGIC %md
# MAGIC # コンプライアンス
# MAGIC バーゼル委員会はVaRのバックテスト手法を規定しています。1日VaR99の結果は
# MAGIC 日次P&L（損益）と比較されます。バックテストは四半期ごとに直近250日間の
# MAGIC データを使用して実施されます。その期間中の超過回数に基づき、VaR
# MAGIC 指標は以下の3つのカラーゾーンのいずれかに分類されます：
# MAGIC
# MAGIC | レベル   | 閾値                 | 結果                       |
# MAGIC |---------|---------------------------|-------------------------------|
# MAGIC | グリーン   | 超過4回以下       | 特段の懸念なし |
# MAGIC | イエロー  | 超過9回以下       | モニタリングが必要           |
# MAGIC | レッド     | 超過10回以上  | VaR指標の改善が必要    |

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
# MAGIC ## 投資リターンの取得
# MAGIC 投資の閾値超過を検出するには、既存の投資を最新のバリュー・アット・リスク計算に重ね合わせる必要があります。ウィンドウパーティショニング関数を使用して投資リターンを計算します。

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.var_udf import compute_return

# 各銘柄に1日のタンブリングウィンドウを適用
window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)

# スライディングウィンドウを適用し、最初の要素を取得
inv_returns_df = spark.table(config['database']['tables']['stocks']) \
  .filter(F.col('close').isNotNull()) \
  .join(spark.createDataFrame(portfolio_df), ['ticker']) \
  .withColumn("first", F.first('close').over(window)) \
  .withColumn("return", compute_return('first', 'close')) \
  .withColumn("weighted_return", F.col('return') * F.col('weight')) \
  .groupBy('date') \
  .agg(F.sum('weighted_return').alias('return'))

display(inv_returns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## バリュー・アット・リスクの取得
# MAGIC 前のセクションで説明したように、試行ベクトルを集約し99パーセンタイルを求めることで、全履歴に対するバリュー・アット・リスクを容易に計算できます。

# COMMAND ----------

from pyspark.ml.stat import Summarizer
from utils.var_udf import get_var_udf

risk_exposure = (
  simulation_df
    .groupBy('date')
    .agg(Summarizer.sum(F.col('weighted_returns')).alias('returns'))
    .withColumn('var_99', get_var_udf(F.col('returns'), F.lit(99)))
    .drop('returns')
    .orderBy('date')
)

# COMMAND ----------

# MAGIC %md
# MAGIC 前回と同様に、`tempo`を使用してこれら2つの系列（投資とリスクエクスポージャー）を効率的に結合します。

# COMMAND ----------

from tempo import *
risk_exposure_tsdf = TSDF(risk_exposure, ts_col="date")
inv_returns_tsdf = TSDF(inv_returns_df, ts_col="date")

# COMMAND ----------

asof_df = (
  inv_returns_tsdf.asofJoin(risk_exposure_tsdf).df
    .na.drop()
    .orderBy('date')
    .select(
      F.col('date'),
      F.col('return'),
      F.col('right_var_99').alias('var_99')
    )
)

display(asof_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 閾値超過の抽出
# MAGIC 250日間の期間内でVaR99の閾値を超えたすべての投資を取得します。

# COMMAND ----------

# タイムスタンプはUNIXタイムスタンプ（秒）として解釈される
days = lambda i: i * 86400
compliance_window = Window.orderBy(F.col("date").cast("long")).rangeBetween(-days(250), 0)

# COMMAND ----------

from utils.var_udf import count_breaches
compliance_df = (
  asof_df
    .withColumn('previous_return', F.collect_list('return').over(compliance_window))
    .withColumn('basel', count_breaches('previous_return', 'var_99'))
    .drop('previous_return')
    .toPandas()
    .set_index('date')
)

# COMMAND ----------

import pandas as pd
import numpy as np
idx = pd.date_range(np.min(compliance_df.index), np.max(compliance_df.index), freq='d')
compliance_df = compliance_df.reindex(idx, method='pad')

# COMMAND ----------

# MAGIC %md
# MAGIC 最後に、投資をバリュー・アット・リスクに対して可視化します。

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

f, (a0, a1) = plt.subplots(2, 1, figsize=(20,8), gridspec_kw={'height_ratios': [10,1]})

a0.plot(compliance_df.index, compliance_df['return'], color='#86bf91', label='リターン')
a0.plot(compliance_df.index, compliance_df['var_99'], label="VaR99", c='red', linestyle='--')
a0.axhline(y=0, linestyle='--', alpha=0.4, color='#86bf91', zorder=1)
a0.title.set_text('VaR99 コンプライアンス')
a0.set_ylabel('日次対数リターン')
a0.legend(loc="upper left")

colors = ['green', 'orange', 'red']
a1.bar(compliance_df.index, 1, color=[colors[i] for i in compliance_df['basel']], label='超過', alpha=0.5, align='edge', width=1.0)
a1.get_yaxis().set_ticks([])
a1.set_xlabel('日付')

plt.subplots_adjust(wspace=0, hspace=0)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ウォール街の銀行、トレーディングリスクが2011年以来の最高水準に急騰
# MAGIC
# MAGIC [...] ウォール街の大手5行の「バリュー・アット・リスク」（潜在的な日次トレーディング損失を測定する指標）の合計が、今年第1四半期に34四半期ぶりの最高水準に急騰したことが、銀行の規制報告書に開示された四半期VaR最高値に関するFinancial Timesの分析で明らかになった。
# MAGIC
# MAGIC [https://on.ft.com/2SSqu8Q](https://on.ft.com/2SSqu8Q)
