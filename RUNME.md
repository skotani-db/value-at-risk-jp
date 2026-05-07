# ユーザーガイド

本ガイドでは、バリュー・アット・リスク ソリューションアクセラレータの設定と実行方法を説明します。
過去の実装実績に基づくと、POC全体の所要期間は約10日間と見積もっています。

## [00_configure_notebook](config/configure_notebook.py)

[![POC](https://img.shields.io/badge/_-ARBITRARY_FILE-lightgray?style=for-the-badge)]()

本ソリューションは複数のノートブックで実行されますが、外部設定ファイルを使用して設定します。
各設定項目の詳細は [application.yaml](config/application.yaml) を参照してください。
この設定は [configure_notebook](config/configure_notebook.py) で使用され、以下のように各ノートブックに「注入」されます。
本POCで使用するポートフォリオは外部[ファイル](config/portfolio.json)として提供され、
設定変数 `portfolio` を通じてアクセスできます。実際のシナリオでは、
外部テーブルまたはジョブ引数から読み取ることになります。

```
%run config/configure_notebook
```

# 問題報告

問題が発生した場合は、GitHubプロジェクトに直接チケットを作成してください。
