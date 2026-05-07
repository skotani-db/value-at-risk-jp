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

# DBTITLE 1,ダウンロードデータの検証とダミーデータ生成
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# ダウンロードしたデータを検証
stocks_table = config['database']['tables']['stocks']
stocks_df = spark.read.table(stocks_table)

total_rows = stocks_df.count()
null_rows = stocks_df.filter(F.col('close').isNull()).count()
null_ratio = null_rows / total_rows if total_rows > 0 else 1.0

print(f"総行数: {total_rows}, NULL行数: {null_rows}, NULL率: {null_ratio:.1%}")

# NULL率が50%以上ならダミーデータを生成
if null_ratio > 0.5:
    print("⚠️ ダウンロードデータが不十分です。各銘柄のもっともらしい値動きでダミーデータを生成します...")
    
    np.random.seed(42)
    tickers = portfolio_df['ticker'].tolist()
    
    # 営業日を生成 (2024-05-01 ~ 2026-05-01)
    dates = pd.bdate_range(start=y_min_date, end=y_max_date)
    
    # 各銘柄の2024年5月時点での実勢価格・ボラティリティ・ドリフト
    # (ADR/米国上場の中南米銘柄のおおよその実値に基づく)
    ticker_params = {
        # チリ
        'BCH':  {'price': 22.5,  'mu': 0.00015, 'sigma': 0.018, 'industry': 'Banks'},
        'BSAC': {'price': 19.8,  'mu': 0.00010, 'sigma': 0.020, 'industry': 'Banks'},
        'CCU':  {'price': 12.3,  'mu': 0.00005, 'sigma': 0.016, 'industry': 'Beverages'},
        'ITCB': {'price': 4.8,   'mu': -0.0001, 'sigma': 0.025, 'industry': 'Banks'},
        'ENIC': {'price': 3.2,   'mu': 0.00008, 'sigma': 0.015, 'industry': 'Electricity'},
        'SQM':  {'price': 52.0,  'mu': -0.0002, 'sigma': 0.028, 'industry': 'Chemicals'},
        # コロンビア
        'CIB':  {'price': 32.5,  'mu': 0.00012, 'sigma': 0.022, 'industry': 'Banks'},
        'EC':   {'price': 11.2,  'mu': 0.00005, 'sigma': 0.026, 'industry': 'Oil & Gas'},
        'AVAL': {'price': 2.4,   'mu': -0.0001, 'sigma': 0.020, 'industry': 'Financial Services'},
        # メキシコ
        'AMX':  {'price': 17.8,  'mu': 0.00008, 'sigma': 0.017, 'industry': 'Telecom'},
        'AMOV': {'price': 18.2,  'mu': 0.00008, 'sigma': 0.017, 'industry': 'Telecom'},
        'CX':   {'price': 7.3,   'mu': 0.00010, 'sigma': 0.024, 'industry': 'Construction'},
        'KOF':  {'price': 92.0,  'mu': 0.00012, 'sigma': 0.014, 'industry': 'Beverages'},
        'VLRS': {'price': 8.5,   'mu': 0.00005, 'sigma': 0.032, 'industry': 'Airlines'},
        'FMX':  {'price': 132.0, 'mu': 0.00010, 'sigma': 0.015, 'industry': 'Beverages'},
        'PAC':  {'price': 172.0, 'mu': 0.00015, 'sigma': 0.018, 'industry': 'Transportation'},
        'ASR':  {'price': 285.0, 'mu': 0.00012, 'sigma': 0.019, 'industry': 'Transportation'},
        'BSMX': {'price': 8.1,   'mu': 0.00010, 'sigma': 0.021, 'industry': 'Banks'},
        'SIM':  {'price': 28.5,  'mu': 0.00008, 'sigma': 0.023, 'industry': 'Metals'},
        'TV':   {'price': 3.1,   'mu': -0.0003, 'sigma': 0.030, 'industry': 'Media'},
        'IBA':  {'price': 48.0,  'mu': 0.00005, 'sigma': 0.016, 'industry': 'Food'},
        # パナマ
        'BLX':  {'price': 32.0,  'mu': 0.00015, 'sigma': 0.019, 'industry': 'Banks'},
        'CPA':  {'price': 98.0,  'mu': 0.00012, 'sigma': 0.022, 'industry': 'Airlines'},
        # ペルー
        'CPAC': {'price': 5.8,   'mu': 0.00005, 'sigma': 0.018, 'industry': 'Construction'},
        'SCCO': {'price': 108.0, 'mu': 0.00018, 'sigma': 0.025, 'industry': 'Mining'},
        'FSM':  {'price': 4.5,   'mu': 0.00010, 'sigma': 0.030, 'industry': 'Mining'},
        'BAP':  {'price': 168.0, 'mu': 0.00015, 'sigma': 0.020, 'industry': 'Banks'},
    }
    
    all_records = []
    for ticker in tickers:
        params = ticker_params.get(ticker, {'price': 50.0, 'mu': 0.0001, 'sigma': 0.020})
        n_days = len(dates)
        initial_price = params['price']
        mu = params['mu']
        sigma = params['sigma']
        
        # 幾何ブラウン運動 + 平均回帰成分でリアルな値動きを生成
        daily_returns = np.random.normal(mu, sigma, n_days)
        # レジームシフト（急騰・急落イベント）を少数追加
        n_events = np.random.randint(3, 8)
        event_days = np.random.choice(n_days, n_events, replace=False)
        daily_returns[event_days] += np.random.normal(0, sigma * 3, n_events)
        
        price_series = initial_price * np.exp(np.cumsum(daily_returns))
        
        for i, date in enumerate(dates):
            close = price_series[i]
            # 日中変動: open/high/low をリアルに生成
            intraday_vol = sigma * 0.6
            open_price = close * np.exp(np.random.normal(0, intraday_vol))
            high = max(open_price, close) * (1 + abs(np.random.normal(0, intraday_vol * 0.5)))
            low = min(open_price, close) * (1 - abs(np.random.normal(0, intraday_vol * 0.5)))
            # 出来高: 価格帯に応じたリアルな水準
            base_volume = 2_000_000 if initial_price < 20 else 800_000 if initial_price < 100 else 400_000
            volume = float(max(10000, int(np.random.lognormal(np.log(base_volume), 0.5))))
            
            all_records.append((ticker, pd.Timestamp(date), open_price, high, low, close, volume))
    
    # ダミーデータでテーブルを上書き
    dummy_pdf = pd.DataFrame(all_records, columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])
    (
        spark.createDataFrame(dummy_pdf)
        .write.format('delta').mode('overwrite')
        .saveAsTable(stocks_table)
    )
    print(f"✅ {len(tickers)}銘柄 × {len(dates)}営業日 = {len(all_records)}行のダミーデータを生成しました")
else:
    print("✅ データは正常です。ダミーデータの生成は不要です。")

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

# DBTITLE 1,市場指標データの検証とダミーデータ生成
# 市場指標データを検証
indicators_table = config['database']['tables']['indicators']
indicators_df = spark.read.table(indicators_table)

indicator_cols = [c for c in indicators_df.columns if c != 'date']
total_rows = indicators_df.count()
null_rows = indicators_df.filter(F.col(indicator_cols[0]).isNull()).count()
null_ratio = null_rows / total_rows if total_rows > 0 else 1.0

print(f"市場指標 - 総行数: {total_rows}, NULL行数: {null_rows}, NULL率: {null_ratio:.1%}")

if null_ratio > 0.5:
    print("⚠️ 市場指標データが不十分です。もっともらしいダミーデータを生成します...")
    
    np.random.seed(123)
    dates = pd.bdate_range(start=y_min_date, end=y_max_date)
    n_days = len(dates)

    # 2024年5月時点の実勢値とリアルなパラメータ
    # S&P500: 5200から緩やかに上昇トレンド
    # NYSE: S&P500と高相関
    # OIL (WTI原油): 78ドル付近、地政学リスクで高ボラティリティ
    # TREASURY (10Y利回り): 4.3%付近、平均回帰的
    # DOWJONES: S&P500と高相関だがやや低ボラ
    indicator_params = {
        'SP500':    {'start': 5205.0, 'mu': 0.00035, 'sigma': 0.011},
        'NYSE':     {'start': 18120.0,'mu': 0.00025, 'sigma': 0.009},
        'OIL':      {'start': 78.5,   'mu': 0.00005, 'sigma': 0.022},
        'TREASURY': {'start': 4.32,   'mu': 0.0,     'sigma': 0.008},
        'DOWJONES': {'start': 39170.0,'mu': 0.00030, 'sigma': 0.010},
    }

    # 相関構造を持つ乱数を生成（市場指標は互いに相関する）
    # 相関行列: SP500, NYSE, OIL, TREASURY, DOWJONES
    corr_matrix = np.array([
        [1.00, 0.92, 0.35, -0.40, 0.96],  # SP500
        [0.92, 1.00, 0.30, -0.35, 0.90],  # NYSE
        [0.35, 0.30, 1.00, 0.10,  0.32],  # OIL
        [-0.40,-0.35, 0.10, 1.00, -0.38],  # TREASURY
        [0.96, 0.90, 0.32, -0.38, 1.00],  # DOWJONES
    ])
    L = np.linalg.cholesky(corr_matrix)
    
    # 相関のある正規乱数を生成
    uncorrelated = np.random.normal(0, 1, (n_days, 5))
    correlated = uncorrelated @ L.T

    dummy_data = {'date': dates}
    for idx, col_name in enumerate(indicator_cols):
        params = indicator_params.get(col_name, {'start': 100, 'mu': 0.0002, 'sigma': 0.015})
        
        if col_name == 'TREASURY':
            # 金利は平均回帰モデル（Ornstein-Uhlenbeck）
            mean_level = 4.0  # 長期平均に回帰
            kappa = 0.02  # 回帰速度
            prices = np.zeros(n_days)
            prices[0] = params['start']
            for i in range(1, n_days):
                prices[i] = prices[i-1] + kappa * (mean_level - prices[i-1]) + params['sigma'] * correlated[i, idx]
                prices[i] = max(2.0, min(6.0, prices[i]))  # 現実的な範囲に制限
        else:
            # 株式指標は幾何ブラウン運動
            returns = params['mu'] + params['sigma'] * correlated[:, idx]
            # 市場ショックイベントを少数追加
            n_shocks = np.random.randint(2, 5)
            shock_days = np.random.choice(n_days, n_shocks, replace=False)
            returns[shock_days] += np.random.normal(0, params['sigma'] * 2.5, n_shocks)
            prices = params['start'] * np.exp(np.cumsum(returns))
        
        dummy_data[col_name] = prices

    dummy_indicators_pdf = pd.DataFrame(dummy_data)
    (
        spark.createDataFrame(dummy_indicators_pdf)
        .write.format('delta').mode('overwrite')
        .saveAsTable(indicators_table)
    )
    print(f"✅ {len(indicator_cols)}指標 × {n_days}営業日のダミーデータを生成しました")
    print(f"   S&P500: {dummy_data['SP500'][0]:.0f} -> {dummy_data['SP500'][-1]:.0f}")
    print(f"   OIL:    {dummy_data['OIL'][0]:.1f} -> {dummy_data['OIL'][-1]:.1f}")
    print(f"   10Y:    {dummy_data['TREASURY'][0]:.2f}% -> {dummy_data['TREASURY'][-1]:.2f}%")
else:
    print("✅ 市場指標データは正常です。")

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
