<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

*本ソリューションは2つのパートで構成されています。第1に、Delta LakeとMLflowをバリュー・アット・リスク（VaR）計算にどのように活用できるかを示します。銀行がLakehouseによる統合的なデータ分析アプローチを用いて、バックテスト、集計、シミュレーションのスケーリングを行い、リスク管理の近代化を実現する方法を解説します。第2に、オルタナティブデータを活用し、より包括的でアジャイル、かつ先見的なリスク管理・投資アプローチへの移行を紹介します。*

___
<antoine.amend@databricks.com>

___

<img src='https://raw.githubusercontent.com/databricks-industry-solutions/value-at-risk/master/images/reference_architecture.png' width=800>

___

&copy; 2022 Databricks, Inc. All rights reserved. 本ノートブックのソースコードは Databricks License [https://databricks.com/db-license-source] に基づいて提供されます。含まれる、または参照されるすべてのサードパーティライブラリは、以下に記載されたライセンスに従います。

| ライブラリ                                | 説明             | ライセンス    | ソース                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Yfinance                               | Yahoo ファイナンス           | Apache2    | https://github.com/ranaroussi/yfinance              |
| tempo                                  | 時系列ライブラリ      | Databricks | https://github.com/databrickslabs/tempo             |
| PyYAML                                 | YAMLファイル読み込み      | MIT        | https://github.com/yaml/pyyaml                      |

## 動作環境
- **Serverless Environment**: Version 4 以上
- **Classic Compute**: DBR 17.x 以降

## 実行手順
このアクセラレータを実行するには、本リポジトリをDatabricksワークスペースにクローンしてください。Databricksウェブサイトで公開されているバージョンのノートブックを実行したい場合は、`web-sync`ブランチに切り替えてください。`RUNME`ノートブックをDBR 11.0以降のランタイムで実行するクラスタにアタッチし、Run-Allで実行してください。アクセラレータパイプラインを記述するマルチステップジョブが作成され、そのリンクが提供されます。マルチステップジョブを実行して、パイプラインの動作を確認してください。ジョブの設定はRUNMEノートブックにJSON形式で記述されています。アクセラレータの実行に伴うコストはユーザーの責任となります。
