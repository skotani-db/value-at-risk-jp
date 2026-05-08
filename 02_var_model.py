# Databricks notebook source
# MAGIC %md
# MAGIC # モデル構築
# MAGIC 本ノートブックでは、過去2年分のマーケット指標データを取得し、各銘柄のリターンを予測するモデルを訓練します。ポートフォリオは40銘柄で構成されているため、40個の予測モデルを並列で訓練し、すべての重みを単一の係数行列に集約してモンテカルロシミュレーションに使用します。**MLFlow**の機能を活用することで、より規律あるモデル開発アプローチを実現する方法を示します。

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

import datetime
model_date = datetime.datetime.strptime(config['model']['date'], '%Y-%m-%d')

# COMMAND ----------

# MAGIC %md
# MAGIC `mlflow.log_figure()`を使用して、matplotlibのfigureを直接MLflowに記録します。一時ディレクトリへの保存は不要です。

# COMMAND ----------

# mlflow.log_figure() を使用するため、一時ディレクトリは不要

# COMMAND ----------

# MAGIC %md
# MAGIC ## リターンの計算
# MAGIC 前のノートブックで、各マーケット指標の日次リターンを既に計算しました。これらをモデルの特徴量として使用し、各銘柄の投資リターンを予測します。

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import datetime
market_df = spark.read.table(config['database']['tables']['volatility']).filter(F.col('date') < model_date).select('date', 'features')
market_pd = pd.DataFrame(market_df.toPandas()['features'].to_list(), columns=list(market_indicators.values()))
display(market_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC 投資の日次リターンを計算しましょう。典型的なポートフォリオのサイズを考慮すると、Sparkのウィンドウ関数を活用できます。

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from utils.var_udf import compute_return

def get_stock_returns():

  # 各銘柄に1日のタンブリングウィンドウを適用
  window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 0)

  # スライディングウィンドウを適用し、最初の要素を取得
  stocks_df = spark.table(config['database']['tables']['stocks']) \
    .filter(F.col('close').isNotNull()) \
    .withColumn("first", F.first('close').over(window)) \
    .withColumn("return", compute_return('first', 'close')) \
    .select('date', 'ticker', 'return')

  return stocks_df

# COMMAND ----------

stocks_df = get_stock_returns().filter(F.col('date') < model_date)
display(stocks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 特徴量の作成
# MAGIC リスクモデルは複雑であり、ノートブックの形式だけでは十分に表現できません。本ソリューションアクセラレータは最良の金融モデルを構築することを目的とせず、そのプロセス全体を説明することを目指しています。優れたリスクモデルの出発点は、すべての指標間の相関関係を入念に研究することです（ここではプレゼンテーションのため5つに限定）。

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# pandasでシンプルに相関行列をプロット（マーケットファクターはメモリに収まる）
# マーケットファクターは相関がないと仮定（NASDAQとSP500は相関あり、原油と国債も同様）
f_cor_pdf = market_pd.corr(method='spearman', min_periods=12)
fig_corr, ax_corr = plt.subplots(figsize=(11, 8))
sns.heatmap(f_cor_pdf, annot=True, ax=ax_corr)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC マーケット指標データと株式リターンを結合して、機械学習用の入力データセットを構築します。実際のシナリオではタイムスタンプが異なる可能性があるため（日中のティックデータなど）、このAS-OF結合には[`tempo`](https://databrickslabs.github.io/tempo/)を使用します。

# COMMAND ----------

from tempo import *
market_tsdf = TSDF(market_df.join(stocks_df.select('ticker').distinct()), ts_col="date", partition_cols=['ticker'])
stocks_tsdf = TSDF(stocks_df, ts_col="date", partition_cols=['ticker'])

# COMMAND ----------

features_df = (
  stocks_tsdf.asofJoin(market_tsdf).df
    .select(
      F.col('date'),
      F.col('ticker'),
      F.col('right_features').alias('features'),
      F.col('return')
    )
    .filter(F.col('features').isNotNull())
)

display(features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルの構築
# MAGIC 任意の関数やモデルを`mlflow.pyfunc`モデルとして簡単にラップし、MLレジストリに登録できることを示します。実際のVaRモデルは、ここで説明するシンプルな線形回帰よりも明らかに複雑であり、必ずしもsklearnやkerasの既成モデルではありません。それでも、同じML開発標準に従うべきであり、モデルのI/Oを`pd.Series`、`pd.DataFrame`、`np.array`の形式で表現できれば、MLFlowの機能から簡単に恩恵を受けることができます。

# COMMAND ----------

import statsmodels.api as sm
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from utils.var_utils import non_linear_features

# pandas UDFを使用して複数のモデル（各銘柄ごとに1つ）を並列で訓練
# 結果のデータフレームは各銘柄の線形回帰の重みとなる
train_model_schema = StructType([
  StructField('ticker', StringType(), True),
  StructField('weights', ArrayType(FloatType()), True)
])

# 実際のモデルは以下よりもはるかに複雑になる
@pandas_udf(train_model_schema, PandasUDFType.GROUPED_MAP)
def train_model(group, pdf):
  import pandas as pd
  import numpy as np
  # マーケットファクターベクトルを構築
  # 各銘柄iの切片項として定数を追加
  X = [non_linear_features(row) for row in np.array(pdf['features'])]
  X = sm.add_constant(X, prepend=True)
  y = np.array(pdf['return'])
  model = sm.OLS(y, X).fit()
  w_df = pd.DataFrame(data=[[model.params]], columns=['weights'])
  w_df['ticker'] = group[0]
  return w_df

# COMMAND ----------

# 結果のデータフレームはメモリに容易に収まり、「統合モデル」として保存される
model_df = features_df.groupBy('ticker').apply(train_model).toPandas()
display(model_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ビジネスロジック全体（統計モデルであれAIモデルであれ）をシンプルな`pyfunc`としてパッケージ化できます。

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel

class RiskMLFlowModel(PythonModel):

  def __init__(self, model_df):
    self.weights = dict(zip(model_df.ticker, model_df.weights))

  def _predict_record(self, ticker, xs):
    # ロジックは非常にシンプルで、シンプルな非線形特徴量と線形回帰を使用
    # それでも、sklearn、複雑なDL、単純な統計オブジェクトに関わらず、モデルはpyfuncとしてパッケージ化可能
    from utils.var_utils import non_linear_features
    from utils.var_utils import predict_non_linears
    ps = self.weights[ticker]
    fs = non_linear_features(xs)
    return predict_non_linears(ps, fs)

  def predict(self, context, model_input):
    predicted = model_input[['ticker','features']].apply(lambda x: self._predict_record(*x), axis=1)
    return predicted

# COMMAND ----------

# MAGIC %md
# MAGIC このモデルは追跡、保存、登録され、データドリフトを防ぐためにモデルのシグネチャが強制されます。

# COMMAND ----------

from mlflow.models.signature import infer_signature

# Unity Catalog モデル名을 定義
uc_model_name = "{}.{}.{}".format(
  config['database']['catalog'],
  config['database']['schema'],
  config['model']['name']
)

with mlflow.start_run(run_name='value-at-risk') as run:

  run_id = run.info.run_id

  # pyfuncモデルを作成
  python_model = RiskMLFlowModel(model_df)

  # モデルの入出力シグネチャを取得
  model_input_df  = features_df.select('ticker', 'features').limit(10).toPandas()
  model_output_df = python_model.predict(None, model_input_df)
  model_signature = infer_signature(model_input_df, model_output_df)

  # モデルをログ + Unity Catalogに直接登録（一回だけ）
  model_info = mlflow.pyfunc.log_model(
    artifact_path="model",
    python_model=python_model,
    signature=model_signature,
    code_paths=["utils"],
    registered_model_name=uc_model_name
  )

  # champion エイリアスを設定
  client = mlflow.tracking.MlflowClient()
  client.set_registered_model_alias(
    name=uc_model_name,
    alias="champion",
    version=model_info.registered_model_version
  )

  # 相関行列プロットを記録
  mlflow.log_figure(fig_corr, "factor_correlation.png")

  print(f"✅ モデル登録完了: {uc_model_name} v{model_info.registered_model_version} (champion)")

# COMMAND ----------

# MAGIC %md
# MAGIC モデルをUnity Catalogに登録することで、次のノートブック（モンテカルロシミュレーション）などの下流プロセスやバックエンドジョブで利用可能になります。

# COMMAND ----------

# MAGIC %md
# MAGIC Unity Catalogではステージの代わりにエイリアスを使用してモデルバージョンを管理します。ここでは最新モデルに`champion`エイリアスを設定し、下流プロセスで利用可能にします。

# COMMAND ----------

# Unity Catalogの登録済みモデルから直接ロード（分散実行）
model_udf = mlflow.pyfunc.spark_udf(
  model_uri='models:/{}@champion'.format(uc_model_name),
  result_type='float',
  spark=spark
)
prediction_df = features_df.withColumn('predicted', model_udf(F.struct('ticker', 'features')))
display(prediction_df)

# COMMAND ----------

# 予測値と実際のリターンを比較
# 銘柄ごとの平均二乗誤差の合計
from utils.var_udf import wsse_udf
wsse_df = prediction_df \
  .withColumn('wsse', wsse_udf(F.col('predicted'), F.col('return'))) \
  .groupBy('ticker') \
  .agg(F.sum('wsse').alias('wsse'))

# ポートフォリオ全体の平均WSSEを取得
wsse = wsse_df.select(F.avg('wsse').alias('wsse')).toPandas().iloc[0].wsse

# 各銘柄のモデル精度を平均二乗誤差でプロット
fig_wsse, ax_wsse = plt.subplots(figsize=(24, 5))
wsse_df.toPandas().plot.bar(x='ticker', y='wsse', rot=0, label=None, ax=ax_wsse)
ax_wsse.get_legend().remove()
ax_wsse.set_title("各銘柄のモデルWSSE")
plt.xticks(rotation=45)
plt.ylabel("wsse")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 予測モデルの結果（二乗誤差の合計）で以前の実験を更新できます。

# COMMAND ----------

with mlflow.start_run(run_id=run_id) as run:
  mlflow.log_metric("wsse", wsse)
  mlflow.log_figure(fig_wsse, "model_wsse.png")

# COMMAND ----------

# MAGIC %md
# MAGIC キャプチャされた実験には、独立して実行するために必要なすべてのライブラリが含まれており、完全な再現性を実現するために特定のDeltaバージョンにリンクされています。

# COMMAND ----------

# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/var/images/var_experiments.png width="1000px">

# COMMAND ----------

# MAGIC %md
# MAGIC モデルにchampionエイリアスが設定されたので、予測ロジックをシンプルなユーザー定義関数としてロードし、観測されたすべての市場条件に対して投資リターンを予測できます。

# COMMAND ----------

import numpy as np

plt.figure(figsize=(25,12))

prediction_df = features_df.withColumn('predicted', model_udf(F.struct("ticker", "features")))
df_past_1 = prediction_df.filter(F.col('ticker') == "EC").orderBy('date').toPandas()
df_past_2 = prediction_df.filter(F.col('ticker') == "EC").orderBy('date').toPandas()
plt.plot(df_past_1.date, df_past_1['return'])
plt.plot(df_past_2.date, df_past_2['predicted'], color='green', linestyle='--')

min_return = np.min(df_past_2['return'])
max_return = np.max(df_past_2['return'])

plt.ylim([min_return, max_return])
plt.title('ECの対数リターン')
plt.ylabel('対数リターン')
plt.xlabel('日付')
plt.show()

# COMMAND ----------


