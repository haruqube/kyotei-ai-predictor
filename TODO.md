## TODO
- [x] 過去データ取得 (2023-2024年分) — 完了: 110,745レース / 654,637結果
- [x] モデル学習・精度検証 — NDCG@1=0.690, NDCG@3=0.768
- [x] note.com記事テンプレート作成
- [ ] 予測パイプラインの自動化テスト

## 完了
- [x] プロジェクト構造作成
- [x] DB スキーマ設計
- [x] スクレイパー実装
- [x] boatrace.jpのスクレイパーをテスト・調整（HTML構造の実地確認）
- [x] 特徴量エンジニアリング（選手成績 + コース・装備特徴量）
- [x] LightGBM LambdaRankモデル
- [x] 予測・レポート生成スクリプト
- [x] 精度評価モジュール
- [x] 並列スクレイパー (init_db_parallel.py, 4ワーカー)
- [x] 高速バッチ特徴量ビルダー (builder_fast.py)

## 進行中
- 予測パイプラインの自動化テスト

## メモ
- 競艇はコース(1-6)が最大のファクター。1コース1着率は全国平均55%
- boatrace.jp HTML構造確認済み (2026-03-07 住之江R1で検証)
  - 出走表: .is-tableFixed__3rdadd > tbody.is-fs12 × 6
  - 結果: table.is-w495 > tbody × 6 (全角着順, is-boatColor枠番, span.is-fs12登番)
  - 天候: .weather1 > .weather1_bodyUnit (is-weather/is-wind/is-wave)
  - 直前情報: tbody.is-fs12 td[4] = 展示タイム
- 毎日開催のため、競馬と違って日次パイプラインが必要
- 進捗確認: python -c "from db.schema import get_connection; ..."
