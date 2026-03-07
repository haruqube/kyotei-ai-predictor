"""モデル学習スクリプト"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from rich.console import Console

from config import RESULTS_DIR, TRAIN_YEARS, TEST_YEARS
from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker

console = Console()


def train():
    """学習データセットを作成しモデルを学習"""
    builder = FeatureBuilder()
    feature_names = builder.feature_names

    # 学習データ
    console.print("[bold]学習データセット構築中...[/bold]")
    train_df = builder.build_dataset(TRAIN_YEARS[0], TRAIN_YEARS[-1])
    if train_df.empty:
        console.print("[red]学習データがありません。init_db.pyを先に実行してください。[/red]")
        return

    # 検証データ
    console.print("[bold]検証データセット構築中...[/bold]")
    val_df = builder.build_dataset(TEST_YEARS[0], TEST_YEARS[-1])

    # NaN処理
    train_df[feature_names] = train_df[feature_names].apply(pd.to_numeric, errors="coerce").fillna(0)
    train_df = train_df.dropna(subset=["finish_position"])

    X_train = train_df[feature_names]
    y_train = train_df["finish_position"]

    # レース単位のグループ
    group_train = train_df.groupby("race_id").size().tolist()

    console.print(f"学習: {len(train_df)}行, {len(group_train)}レース")
    console.print(f"特徴量: {len(feature_names)}個")

    X_val, y_val, group_val = None, None, None
    if not val_df.empty:
        val_df[feature_names] = val_df[feature_names].apply(pd.to_numeric, errors="coerce").fillna(0)
        val_df = val_df.dropna(subset=["finish_position"])
        X_val = val_df[feature_names]
        y_val = val_df["finish_position"]
        group_val = val_df.groupby("race_id").size().tolist()
        console.print(f"検証: {len(val_df)}行, {len(group_val)}レース")

    # 学習
    model = LGBMRanker()
    console.print("\n[bold]モデル学習開始...[/bold]")
    model.train(X_train, y_train, group_train, X_val, y_val, group_val)

    # 保存
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = str(RESULTS_DIR / "model_lgbm.pkl")
    model.save(model_path)
    console.print(f"[green]モデル保存: {model_path}[/green]")

    # 特徴量重要度
    importance = model.feature_importance()
    console.print("\n[bold]特徴量重要度 Top 10:[/bold]")
    for _, row in importance.head(10).iterrows():
        console.print(f"  {row['feature']}: {row['importance']:.1f}")


if __name__ == "__main__":
    train()
