# Databricks notebook source
# MAGIC %md
# MAGIC # モデル訓練 (RiskLens VaR App用)
# MAGIC 02_var_model.py をベースに、Databricks Apps からジョブキックで実行するための訓練ノートブック。

# COMMAND ----------

# MAGIC %pip install statsmodels mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# パラメータ受け取り
dbutils.widgets.text("catalog", "skotani_var")
dbutils.widgets.text("schema", "var_app")
dbutils.widgets.text("stocks_table", "market_data")
dbutils.widgets.text("indicators_table", "market_indicators")
dbutils.widgets.text("model_name", "value_at_risk")
dbutils.widgets.text("model_date", "2026-04-01")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
stocks_table = f"{catalog}.{schema}.{dbutils.widgets.get('stocks_table')}"
indicators_table = f"{catalog}.{schema}.{dbutils.widgets.get('indicators_table')}"
model_name = dbutils.widgets.get("model_name")
model_date = dbutils.widgets.get("model_date")

print(f"Catalog: {catalog}, Schema: {schema}")
print(f"Stocks: {stocks_table}, Indicators: {indicators_table}")
print(f"Model: {catalog}.{schema}.{model_name}, Cutoff: {model_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## リターンの計算

# COMMAND ----------

from pyspark.sql import functions as F, Window
import pandas as pd
import numpy as np

# 株式リターン
stock_returns_df = (
    spark.table(stocks_table)
    .filter(F.col("close").isNotNull())
    .filter(F.col("date") < model_date)
    .withColumn("log_return",
        F.log(F.col("close") / F.lag("close").over(Window.partitionBy("ticker").orderBy("date"))))
    .filter(F.col("log_return").isNotNull())
)

# 市場指標リターン
indicators_df = spark.table(indicators_table).filter(F.col("date") < model_date).orderBy("date")
indicator_returns_df = indicators_df.select(
    F.col("date"),
    F.log(F.col("SP500") / F.lag("SP500").over(Window.orderBy("date"))).alias("sp500_ret"),
    F.log(F.col("NYSE") / F.lag("NYSE").over(Window.orderBy("date"))).alias("nyse_ret"),
    F.log(F.col("OIL") / F.lag("OIL").over(Window.orderBy("date"))).alias("oil_ret"),
    F.log(F.col("TREASURY") / F.lag("TREASURY").over(Window.orderBy("date"))).alias("treasury_ret"),
    F.log(F.col("DOWJONES") / F.lag("DOWJONES").over(Window.orderBy("date"))).alias("dowjones_ret"),
).filter(F.col("sp500_ret").isNotNull())

# 特徴量結合
features_df = (
    stock_returns_df.select("date", "ticker", "log_return")
    .join(indicator_returns_df, "date")
)

market_cols = ["sp500_ret", "nyse_ret", "oil_ret", "treasury_ret", "dowjones_ret"]
market_pdf = features_df.select(market_cols).toPandas().astype(float)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 相関行列プロット

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

f_cor_pdf = market_pdf.corr(method='spearman')
fig_corr, ax_corr = plt.subplots(figsize=(11, 8))
sns.heatmap(f_cor_pdf, annot=True, ax=ax_corr, cmap='RdBu_r', center=0)
ax_corr.set_title("Market Factor Correlation")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデル訓練 (非線形特徴量 + OLS回帰)

# COMMAND ----------

import statsmodels.api as sm
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType

def non_linear_features(xs):
    fs = []
    for x in xs:
        fs.append(x)
        fs.append(np.sign(x) * x ** 2)
        fs.append(x ** 3)
        fs.append(np.sign(x) * np.sqrt(abs(x)))
    return fs

def predict_non_linears(ps, fs):
    s = ps[0]
    for i, f in enumerate(fs):
        s = s + ps[i + 1] * f
    return float(s)

# pandas UDFで銘柄ごとにOLS回帰
train_model_schema = StructType([
    StructField('ticker', StringType(), True),
    StructField('weights', ArrayType(FloatType()), True)
])

@pandas_udf(train_model_schema, PandasUDFType.GROUPED_MAP)
def train_model(group, pdf):
    X = [non_linear_features(row) for row in np.array(pdf['features'])]
    X = sm.add_constant(X, prepend=True)
    y = np.array(pdf['return'])
    model = sm.OLS(y, X).fit()
    w_df = pd.DataFrame(data=[[model.params]], columns=['weights'])
    w_df['ticker'] = group[0]
    return w_df

# 特徴量を配列にパック
features_with_array = features_df.withColumn(
    "features", F.array(*[F.col(c) for c in market_cols])
).withColumnRenamed("log_return", "return")

model_df = features_with_array.groupBy("ticker").apply(train_model).toPandas()
display(model_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow ログ & Unity Catalog 登録

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel
from mlflow.models.signature import infer_signature

mlflow.set_registry_uri("databricks-uc")
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/risklens_var')

uc_model_name = f"{catalog}.{schema}.{model_name}"

class RiskMLFlowModel(PythonModel):
    def __init__(self, model_df):
        self.weights = dict(zip(model_df.ticker, model_df.weights))

    def _predict_record(self, ticker, xs):
        ps = self.weights[ticker]
        fs = non_linear_features(xs)
        return predict_non_linears(ps, fs)

    def predict(self, context, model_input):
        predicted = model_input[['ticker', 'features']].apply(lambda x: self._predict_record(*x), axis=1)
        return predicted

python_model = RiskMLFlowModel(model_df)

# シグネチャ推論
model_input_df = features_with_array.select('ticker', 'features').limit(10).toPandas()
model_output_df = python_model.predict(None, model_input_df)
model_signature = infer_signature(model_input_df, model_output_df)

with mlflow.start_run(run_name='value-at-risk') as run:
    run_id = run.info.run_id

    # モデルをログ + Unity Catalogに登録
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=python_model,
        signature=model_signature,
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

    # パラメータ記録
    mlflow.log_param("algorithm", "non-linear-ols")
    mlflow.log_param("model_date_cutoff", model_date)
    mlflow.log_param("num_tickers", len(model_df))
    mlflow.log_param("features", "x, x^2, x^3, sqrt(|x|) per factor")

    print(f"✅ モデル登録完了: {uc_model_name} v{model_info.registered_model_version} (champion)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 予測精度 (WSSE)

# COMMAND ----------

model_udf = mlflow.pyfunc.spark_udf(
    model_uri=f'models:/{uc_model_name}@champion',
    result_type='float',
    spark=spark
)

prediction_df = features_with_array.withColumn('predicted', model_udf(F.struct('ticker', 'features')))

wsse_df = (
    prediction_df
    .withColumn('wsse', (F.col('predicted') - F.col('return')) ** 2)
    .groupBy('ticker')
    .agg(F.sum('wsse').alias('wsse'))
)

wsse = wsse_df.select(F.avg('wsse').alias('wsse')).toPandas().iloc[0].wsse

fig_wsse, ax_wsse = plt.subplots(figsize=(24, 5))
wsse_df.toPandas().plot.bar(x='ticker', y='wsse', rot=45, label=None, ax=ax_wsse)
ax_wsse.get_legend().remove()
ax_wsse.set_title("Model WSSE per Ticker")
plt.ylabel("WSSE")
plt.tight_layout()
plt.show()

with mlflow.start_run(run_id=run_id):
    mlflow.log_metric("wsse", wsse)
    mlflow.log_figure(fig_wsse, "model_wsse.png")

print(f"✅ WSSE: {wsse:.6f}")

# COMMAND ----------

# SQL回帰テーブルも更新 (App表示用)
features_with_array.createOrReplaceTempView("features_view")
indicator_returns_df.createOrReplaceTempView("ind_ret_view")

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.stock_returns AS
    SELECT ticker, date, log_return FROM ({stock_returns_df.filter(F.col("log_return").isNotNull()).createOrReplaceTempView("sr_view") or "SELECT * FROM sr_view"})
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.indicator_returns AS
    SELECT * FROM ind_ret_view
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.model_weights AS
    SELECT
        s.ticker,
        REGR_SLOPE(s.log_return, i.sp500_ret) as beta_sp500,
        REGR_SLOPE(s.log_return, i.nyse_ret) as beta_nyse,
        REGR_SLOPE(s.log_return, i.oil_ret) as beta_oil,
        REGR_SLOPE(s.log_return, i.treasury_ret) as beta_treasury,
        REGR_SLOPE(s.log_return, i.dowjones_ret) as beta_dowjones,
        AVG(s.log_return) - REGR_SLOPE(s.log_return, i.sp500_ret) * AVG(i.sp500_ret) as alpha,
        CORR(s.log_return, i.sp500_ret) as corr_sp500,
        CORR(s.log_return, i.oil_ret) as corr_oil,
        CORR(s.log_return, i.treasury_ret) as corr_treasury,
        STDDEV(s.log_return) as volatility,
        COUNT(*) as n_observations,
        MIN(s.date) as train_start,
        MAX(s.date) as train_end
    FROM {catalog}.{schema}.stock_returns s
    JOIN {catalog}.{schema}.indicator_returns i ON s.date = i.date
    WHERE s.log_return IS NOT NULL AND i.sp500_ret IS NOT NULL
    GROUP BY s.ticker
""")

print("✅ App表示用テーブルも更新完了")
