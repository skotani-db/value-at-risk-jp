# Databricks notebook source
# MAGIC %md
# MAGIC # ポートフォリオの作成
# MAGIC 本ノートブックでは、`yfinance`を使用して、均等加重の仮想ラテンアメリカポートフォリオに含まれる40銘柄の株式データをダウンロードします。pandas UDFを使用してこのプロセスを効率的に分散処理し、すべての出力データをDeltaテーブルとして保存する方法を示します。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC この演習では、configフォルダにある均等加重ポートフォリオを使用します。基盤の準備ができたら、リスクエクスポージャーを最小化するためにウェイトを調整することができます。

# COMMAND ----------

display(portfolio_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 株式データのダウンロード
# MAGIC Yahoo Financeから株式市場データ（終値）をダウンロードし、時系列が適切にインデックス付けされ、完全であることを確認します。

# COMMAND ----------

import datetime as dt
y_min_date = dt.datetime.strptime(config['yfinance']['mindate'], "%Y-%m-%d").date()
y_max_date = dt.datetime.strptime(config['yfinance']['maxdate'], "%Y-%m-%d").date()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from utils.var_utils import download_market_data

schema = StructType(
  [
    StructField('ticker', StringType(), True),
    StructField('date', TimestampType(), True),
    StructField('open', DoubleType(), True),
    StructField('high', DoubleType(), True),
    StructField('low', DoubleType(), True),
    StructField('close', DoubleType(), True),
    StructField('volume', DoubleType(), True),
  ]
)

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def download_market_data_udf(group, pdf):
  tick = group[0]
  return download_market_data(tick, y_min_date, y_max_date)

# COMMAND ----------

_ = (
  spark.createDataFrame(portfolio_df)
    .groupBy('ticker')
    .apply(download_market_data_udf)
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(config['database']['tables']['stocks'])
)

# COMMAND ----------

display(spark.read.table(config['database']['tables']['stocks']))

# COMMAND ----------

# MAGIC %md
# MAGIC Databricksランタイムにはplotlyなど多くのPythonライブラリがプリインストールされています。ローソク足チャートで特定の銘柄を可視化できます。

# COMMAND ----------

from pyspark.sql import functions as F

stock_df = (
  spark
    .read
    .table(config['database']['tables']['stocks'])
    .filter(F.col('ticker') == portfolio_df.iloc[0].ticker)
    .orderBy(F.asc('date'))
    .toPandas()
)

# COMMAND ----------

from utils.var_viz import plot_candlesticks
plot_candlesticks(stock_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## マーケットファクターのダウンロード
# MAGIC 各資産はS&P500、原油、国債、ダウ平均などの市場指標や各種インデックスでより適切に説明できると仮定します。これらの指標は、後ほどリスクモデルの入力特徴量として使用されます。

# COMMAND ----------

# 各列に終値インデックスを含むpandasデータフレームを作成
market_indicators_df = pd.DataFrame()
for indicator in market_indicators.keys():
    close_df = download_market_data(indicator, y_min_date, y_max_date)['close'].copy()
    market_indicators_df[market_indicators[indicator]] = close_df

# pandasはSparkデータフレームに変換する際にインデックス（日付）を保持しない
market_indicators_df['date'] = market_indicators_df.index

# COMMAND ----------

_ = (
  spark
    .createDataFrame(market_indicators_df)
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(config['database']['tables']['indicators'])
)

# COMMAND ----------

display(spark.read.table(config['database']['tables']['indicators']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## マーケットボラティリティの計算
# MAGIC 導入部で述べたように、パラメトリックVaRの核心は過去のボラティリティから学習することです。各日を直近の履歴と順次処理する代わりに、シンプルなウィンドウ関数を適用して、すべての時点で過去X日間分のマーケットボラティリティを計算し、多変量分布の背後にある統計量を学習します。

# COMMAND ----------

import numpy as np

def get_market_returns():

  f_ret_pdf = spark.table(config['database']['tables']['indicators']).orderBy('date').toPandas()

  # スライディングウィンドウ用にpandasインデックスとして日付列を追加
  f_ret_pdf.index = f_ret_pdf['date']
  f_ret_pdf = f_ret_pdf.drop(columns = ['date'])

  # 日次対数リターンを計算
  f_ret_pdf = np.log(f_ret_pdf.shift(1)/f_ret_pdf)

  # 日付列を追加
  f_ret_pdf['date'] = f_ret_pdf.index
  f_ret_pdf = f_ret_pdf.dropna()

  return (
    spark
      .createDataFrame(f_ret_pdf)
      .select(F.array(list(market_indicators.values())).alias('features'), F.col('date'))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC データを再帰的にクエリする代わりに、ウィンドウ関数を適用することで、テーブルの各レコードが過去X日間分の観測値と「結合」されます。シンプルなUDFを使用して、各ウィンドウのマーケットボラティリティの統計量を計算できます。

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.var_udf import *

days = lambda i: i * 86400
volatility_window = Window.orderBy(F.col('date').cast('long')).rangeBetween(-days(config['monte-carlo']['volatility']), 0)

volatility_df = (
  get_market_returns()
    .select(
      F.col('date'),
      F.col('features'),
      F.collect_list('features').over(volatility_window).alias('volatility')
    )
    .filter(F.size('volatility') > 1)
    .select(
      F.col('date'),
      F.col('features'),
      compute_avg(F.col('volatility')).alias('vol_avg'),
      compute_cov(F.col('volatility')).alias('vol_cov')
    )
)

# COMMAND ----------

volatility_df.write.format('delta').mode('overwrite').saveAsTable(config['database']['tables']['volatility'])

# COMMAND ----------

# MAGIC %md
# MAGIC これで、すべての時点における最新の指標にアクセスできるようになりました。各日について、リターンの平均と共分散行列がわかります。これらの統計量は、次のノートブックでランダムな市場条件を生成するために使用されます。

# COMMAND ----------

display(spark.read.table(config['database']['tables']['volatility']))
