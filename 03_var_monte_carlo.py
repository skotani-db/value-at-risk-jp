# Databricks notebook source
# MAGIC %md
# MAGIC # モンテカルロシミュレーション
# MAGIC 本ノートブックでは、前のステージで作成したモデルを使用し、**Apache Spark**を活用してモンテカルロシミュレーションを並列実行します。多変量分布からサンプリングされた各シミュレーション市場条件に対して、仮想的な銘柄リターンを予測します。すべてのデータを**Delta Lake**に保存することで、複数の下流ユースケースでオンデマンドにクエリ可能なデータアセットを作成します。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

import datetime
from datetime import timedelta
import pandas as pd
import datetime

# モデル構築以降の毎週に対してモンテカルロシミュレーションを生成
today = datetime.datetime.strptime(config['yfinance']['maxdate'], '%Y-%m-%d')
first = datetime.datetime.strptime(config['model']['date'], '%Y-%m-%d')
run_dates = pd.date_range(first, today, freq='w')

# COMMAND ----------

# MAGIC %md
# MAGIC ## マーケットボラティリティ
# MAGIC データ取り込み時にすべての統計量を事前計算しているため、モンテカルロシミュレーションを実行したい各日付のマーケット指標の最新の統計分布を容易に取得できます。[`tempo`](https://databrickslabs.github.io/tempo/)ライブラリのas-of結合を使用して時系列情報にアクセスできます。

# COMMAND ----------

from tempo import *
market_tsdf = TSDF(spark.read.table(config['database']['tables']['volatility']), ts_col='date')
rdates_tsdf = TSDF(spark.createDataFrame(pd.DataFrame(run_dates, columns=['date'])), ts_col='date')

# COMMAND ----------

from pyspark.sql import functions as F

volatility_df = rdates_tsdf.asofJoin(market_tsdf).df.select(
  F.col('date'),
  F.col('right_vol_cov').alias('vol_cov'),
  F.col('right_vol_avg').alias('vol_avg')
)

display(volatility_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 試行の分散処理
# MAGIC シード戦略を固定することで、各試行が独立であること（同じ乱数が生成されないこと）を保証し、同じ実験を2回処理する必要がある場合の完全な再現性を確保します。

# COMMAND ----------

from utils.var_utils import create_seed_df
seed_df = create_seed_df(config['monte-carlo']['runs'])
display(seed_df)

# COMMAND ----------

from utils.var_udf import simulate_market

market_conditions = (
  volatility_df
    .join(spark.createDataFrame(seed_df))
    .withColumn('features', simulate_market('vol_avg', 'vol_cov', 'trial_id'))
    .select('date', 'features', 'trial_id')
)

# COMMAND ----------

display(market_conditions)

# COMMAND ----------

# MAGIC %md
# MAGIC 各試行IDとシミュレーション市場条件のクロス結合は計算コストが高いため、このテーブルをDeltaテーブルとして保存し、下流で処理できるようにします。さらに、このテーブルは既知のマーケットボラティリティからポイントをサンプリングしただけで、投資リターンは考慮していないため汎用的です。新しいモデルや新しいトレーディング戦略を、この高コストなプロセスを再実行することなく、同じデータに基づいて実行できます。

# COMMAND ----------

_ = (
  market_conditions
    .repartition(config['monte-carlo']['executors'], 'date')
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(config['database']['tables']['mc_market'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## リターンの計算
# MAGIC 最後に、先ほど作成したモデルを活用して、生成されたマーケット指標に対する各銘柄の投資リターンを予測します。

# COMMAND ----------

import mlflow
# Unity Catalog: catalog.schema.model_name@alias 形式でモデルをロード
uc_model_name = "{}.{}.{}".format(
  config['database']['catalog'],
  config['database']['schema'],
  config['model']['name']
)
model_udf = mlflow.pyfunc.spark_udf(
  model_uri='models:/{}/champion'.format(uc_model_name),
  result_type='float',
  spark=spark
)

# COMMAND ----------

simulations = (
  spark.read.table(config['database']['tables']['mc_market'])
    .join(spark.createDataFrame(portfolio_df[['ticker']]))
    .withColumn('return', model_udf(F.struct('ticker', 'features')))
    .drop('features')
)

display(simulations)

# COMMAND ----------

# MAGIC %md
# MAGIC シミュレーション市場条件を非常に少ない列数の大きなテーブルとして処理しましたが、すべての試行を明確に定義されたベクトルにラップすることで、より良いデータアセットを作成できます。このアセットにより、`pyspark.ml.stat`の`Summarizer`クラスを使用したシンプルな集約関数でベクトルを操作できます（次のノートブック参照）。

# COMMAND ----------

from pyspark.ml.linalg import Vectors, VectorUDT

@udf(VectorUDT())
def to_vector(xs, ys):
  v = Vectors.sparse(config['monte-carlo']['runs'], zip(xs, ys)).toArray()
  return Vectors.dense(v)

# COMMAND ----------

simulations_vectors = (
  simulations
    .groupBy('date', 'ticker')
    .agg(
      F.collect_list('trial_id').alias('xs'),
      F.collect_list('return').alias('ys')
    )
    .select(
      F.col('date'),
      F.col('ticker'),
      to_vector(F.col('xs'), F.col('ys')).alias('returns')
    )
)

# COMMAND ----------

_ = (
  simulations_vectors
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(config['database']['tables']['mc_trials'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC 最後に、テーブルを高速読み取り用に最適化することで、データアセットの特定のスライスを容易に抽出できるようにします。これはDeltaの`OPTIMIZE`コマンドによって実現されます。

# COMMAND ----------

_ = sql('OPTIMIZE {} ZORDER BY (`date`, `ticker`)'.format(config['database']['tables']['mc_trials']))

# COMMAND ----------


