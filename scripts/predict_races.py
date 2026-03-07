"""レース予測生成"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from rich.console import Console
from rich.table import Table

from config import RESULTS_DIR, VENUE_CODES
from db.schema import get_connection, insert_entry, insert_prediction, insert_race
from data.scraper import BoatraceScraper
from data.race_calendar import get_upcoming_race_ids
from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker

console = Console()


def predict_races(target_date: str | None = None):
    """レース予測を生成

    target_date: YYYYMMDD形式。指定なしなら今日・明日の全レース。
    """
    model_path = str(RESULTS_DIR / "model_lgbm.pkl")
    model = LGBMRanker()
    try:
        model.load(model_path)
    except FileNotFoundError:
        console.print("[red]モデルが見つかりません。先に train_model.py を実行してください。[/red]")
        return []

    scraper = BoatraceScraper()
    builder = FeatureBuilder()
    conn = get_connection()

    if target_date:
        race_map = {target_date: scraper.scrape_race_list(target_date)}
    else:
        race_map = get_upcoming_race_ids()

    all_predictions = []

    for date_str, race_ids in race_map.items():
        date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
        console.print(f"\n[bold blue]== {date_display} ({len(race_ids)}レース) ==[/bold blue]")

        for race_id in race_ids:
            try:
                entry_data = scraper.scrape_race_entry(race_id)
                race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                # エントリーをDBに保存
                for e in entry_data.get("entries", []):
                    if e.get("racer_id"):
                        e["race_id"] = race_id
                        insert_entry(conn, e)

                # レース情報もDBに保存
                _, jcd, rno = race_id.split("_")
                race_info = {
                    "race_id": race_id,
                    "date": race_date,
                    "venue_code": jcd,
                    "venue_name": VENUE_CODES.get(jcd, jcd),
                    "race_number": int(rno),
                    "race_name": entry_data.get("race_name"),
                    "grade": entry_data.get("grade"),
                    "distance": entry_data.get("distance", 1800),
                    "weather": entry_data.get("weather"),
                    "wind_direction": entry_data.get("wind_direction"),
                    "wind_speed": entry_data.get("wind_speed"),
                    "wave_height": entry_data.get("wave_height"),
                    "is_night": entry_data.get("is_night", 0),
                    "racer_count": len(entry_data.get("entries", [])),
                }
                insert_race(conn, race_info)
                conn.commit()

                # 特徴量生成
                df = builder.build_race_features(race_id, race_date)
                if df.empty:
                    continue

                # 予測
                feature_names = builder.feature_names
                X = df[feature_names].copy()
                for col in X.columns:
                    X[col] = pd.to_numeric(X[col], errors="coerce")
                X = X.fillna(0)
                df["predicted_score"] = model.predict(X).values
                df = df.sort_values("predicted_score", ascending=False)
                df["predicted_rank"] = range(1, len(df) + 1)

                # 印
                marks = ["◎", "○", "▲", "△", "△", ""]
                df["mark"] = [marks[i] if i < len(marks) else "" for i in range(len(df))]

                # DBに保存
                for _, row in df.iterrows():
                    insert_prediction(conn, {
                        "race_id": race_id,
                        "racer_id": row["racer_id"],
                        "predicted_score": row["predicted_score"],
                        "predicted_rank": row["predicted_rank"],
                        "mark": row["mark"],
                        "confidence": row["predicted_score"],
                    })
                conn.commit()

                # 表示
                venue_name = VENUE_CODES.get(jcd, jcd)
                race_name = entry_data.get("race_name", "")
                table = Table(title=f"{venue_name} {int(rno)}R {race_name}")
                table.add_column("印")
                table.add_column("コース")
                table.add_column("選手名")
                table.add_column("級別")
                table.add_column("全国勝率")
                table.add_column("モーター")
                table.add_column("展示T")
                table.add_column("スコア")

                for _, row in df.iterrows():
                    entry_row = conn.execute(
                        "SELECT * FROM entries WHERE race_id = ? AND racer_id = ?",
                        (race_id, row["racer_id"])
                    ).fetchone()

                    table.add_row(
                        row["mark"],
                        str(entry_row["course"] if entry_row else "?"),
                        entry_row["racer_name"] if entry_row else row["racer_id"],
                        entry_row["class"] if entry_row else "?",
                        f"{entry_row['national_win_rate']:.2f}" if entry_row and entry_row["national_win_rate"] else "?",
                        f"{entry_row['motor_2nd_rate']:.1f}%" if entry_row and entry_row["motor_2nd_rate"] else "?",
                        f"{entry_row['exhibition_time']}" if entry_row and entry_row["exhibition_time"] else "?",
                        f"{row['predicted_score']:.3f}",
                    )
                console.print(table)

                all_predictions.append({
                    "race_id": race_id,
                    "race_info": race_info,
                    "predictions": df.to_dict("records"),
                })

            except Exception as e:
                console.print(f"  [red]Error {race_id}: {e}[/red]")

    conn.close()
    console.print(f"\n[bold green]予測完了: {len(all_predictions)}レース[/bold green]")
    return all_predictions


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="レース予測")
    parser.add_argument("--date", type=str, default=None, help="対象日 (YYYYMMDD)")
    args = parser.parse_args()
    predict_races(args.date)
