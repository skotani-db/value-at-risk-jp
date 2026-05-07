# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# pip install後にPythonプロセスを再起動し、パッケージの競合を解消
dbutils.library.restartPython()

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

import yaml
with open('config/application.yaml', 'r') as f:
  config = yaml.safe_load(f)

# COMMAND ----------

# Unity Catalog: カタログとスキーマを作成
_ = sql("CREATE CATALOG IF NOT EXISTS {}".format(config['database']['catalog']))
_ = sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(
  config['database']['catalog'],
  config['database']['schema']
))

# COMMAND ----------

# デフォルトのカタログとスキーマを設定
# 各テーブルはUnity Catalogのマネージドテーブルとして作成される
_ = sql("USE CATALOG {}".format(config['database']['catalog']))
_ = sql("USE SCHEMA {}".format(config['database']['schema']))

# COMMAND ----------

import pandas as pd
portfolio_df = pd.read_json('config/portfolio.json', orient='records')

# COMMAND ----------

import json
with open('config/indicators.json', 'r') as f:
  market_indicators = json.load(f)

# COMMAND ----------

import mlflow
# Unity Catalog対応のMLflowレジストリを使用
mlflow.set_registry_uri("databricks-uc")
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/value_at_risk'.format(username))

# COMMAND ----------

def teardown():
  _ = sql("DROP SCHEMA IF EXISTS {}.{} CASCADE".format(
    config['database']['catalog'],
    config['database']['schema']
  ))
