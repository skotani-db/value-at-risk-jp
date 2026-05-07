# Databricks notebook source
# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">
# MAGIC
# MAGIC [![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC
# MAGIC *従来のオンプレミスインフラに依存する銀行は、もはやリスクを効果的に管理することができません。銀行はレガシー技術の計算上の非効率性を捨て、市場や経済のボラティリティに迅速に対応できるアジャイルなモダンリスク管理体制を構築する必要があります。バリュー・アット・リスクのユースケースを通じて、DatabricksがどのようにFSI（金融サービス業界）のリスク管理の近代化を支援し、Delta Lake、Apache Spark、MLFlowを活用してよりアジャイルなリスク管理アプローチを採用しているかを学びます。*
# MAGIC
# MAGIC ___
# MAGIC <antoine.amend@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/databricks-industry-solutions/value-at-risk/master/images/reference_architecture.png' width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC ## VaR 入門
# MAGIC
# MAGIC VaR（バリュー・アット・リスク）は、特定の信頼区間における潜在的損失の指標です。VaR統計量は3つの要素で構成されます：期間、信頼水準、損失額（または損失率）。「来月中に95%または99%の信頼水準で、最大いくらの損失が見込まれるか？」という問いに答えるものです。VaRの計算方法には3つのアプローチがあります。
# MAGIC #
# MAGIC
# MAGIC + **ヒストリカル法**: 実際の過去のリターンを最悪から最良の順に並べ替えるシンプルな手法です。
# MAGIC + **分散共分散法**: 株式リターンが正規分布に従うと仮定し、実際のリターンの代わりに確率密度関数を使用します。
# MAGIC + **モンテカルロシミュレーション**: 将来の株価リターンのモデルを構築し、複数の仮想的なシナリオを実行する手法です。
# MAGIC
# MAGIC 以下の例では、合成商品に対するシンプルなVaR計算を示します。ボラティリティ（商品リターンの標準偏差）と時間軸（300日）が与えられています。**95%の信頼水準で、300日間に最大いくら損失する可能性があるか？**

# COMMAND ----------

# 時間軸
days = 300

# ボラティリティ
sigma = 0.04

# ドリフト（平均成長率）
mu = 0.05

# 初期価格
start_price = 10

# COMMAND ----------

import matplotlib.pyplot as plt
from utils.var_utils import generate_prices

plt.figure(figsize=(16,6))
for i in range(1, 500):
    plt.plot(generate_prices(start_price, mu, sigma, days))

plt.title('シミュレーション価格')
plt.xlabel("時間")
plt.ylabel("価格")
plt.show()

# COMMAND ----------

from utils.var_viz import plot_var
simulations = [generate_prices(start_price, mu, sigma, days)[-1] for i in range(10000)]
plot_var(simulations, 99)

# COMMAND ----------

# MAGIC %md
# MAGIC 期待ショートフォールは、VaRよりもトレーダーに対してより良いインセンティブを生み出す指標です。条件付きVaRまたはテールロスとも呼ばれます。VaRが「最悪の場合どの程度悪くなりうるか？」と問うのに対し、期待ショートフォールは「悪い事態が発生した場合、予想される損失はいくらか？」と問います。

# COMMAND ----------

from utils.var_utils import get_var
from utils.var_utils import get_shortfall

print('Var99: {}'.format(round(get_var(simulations, 99), 2)))
print('ショートフォール: {}'.format(round(get_shortfall(simulations, 99), 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. 本ノートブックのソースコードは Databricks License [https://databricks.com/db-license-source] に基づいて提供されます。含まれる、または参照されるすべてのサードパーティライブラリは、以下に記載されたライセンスに従います。
# MAGIC
# MAGIC | ライブラリ                                | 説明             | ライセンス    | ソース                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | Yfinance                               | Yahoo ファイナンス           | Apache2    | https://github.com/ranaroussi/yfinance              |
# MAGIC | tempo                                  | 時系列ライブラリ      | Databricks | https://github.com/databrickslabs/tempo             |
# MAGIC | PyYAML                                 | YAMLファイル読み込み      | MIT        | https://github.com/yaml/pyyaml                      |
