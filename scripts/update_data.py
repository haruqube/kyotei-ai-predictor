"""データ更新 — 直近の結果取得 + DB更新"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from datetime import datetime, timedelta
from rich.console import Console

from config import VENUE_CODES
from db.schema import get_connection, insert_race, insert_result, insert_racer
from data.scraper import BoatraceScraper

console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def _process_race(scraper, conn, race_id, date_str):
    """1レース分の結果を取得してDBに保存"""
    data = scraper.scrape_race_result(race_id)
    if not data.get("results"):
        return

    race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    race_info = {
        "race_id": race_id,
        "date": race_date,
        "venue_code": data.get("venue_code", ""),
        "venue_name": data.get("venue_name", ""),
        "race_number": data.get("race_number", 0),
        "race_name": data.get("race_name"),
        "grade": data.get("grade"),
        "distance": data.get("distance", 1800),
        "weather": data.get("weather"),
        "wind_direction": data.get("wind_direction"),
        "wind_speed": data.get("wind_speed"),
        "wave_height": data.get("wave_height"),
        "is_night": data.get("is_night", 0),
        "racer_count": data.get("racer_count", 6),
    }
    insert_race(conn, race_info)

    for r in data.get("results", []):
        if r.get("racer_id"):
            insert_racer(conn, {
                "racer_id": r["racer_id"],
                "name": r.get("racer_name", ""),
            })
            insert_result(conn, r)


def update_recent_results(days_back: int = 7):
    """直近N日分の結果を取得してDBに保存"""
    scraper = BoatraceScraper()
    conn = get_connection()

    today = datetime.now()
    total_races = 0

    for d in range(days_back, 0, -1):
        date = today - timedelta(days=d)
        date_str = date.strftime("%Y%m%d")
        race_ids = scraper.scrape_race_list(date_str)

        if not race_ids:
            continue

        console.print(f"[blue]{date_str}: {len(race_ids)}レース[/blue]")

        for race_id in race_ids:
            try:
                _process_race(scraper, conn, race_id, date_str)
                total_races += 1
            except Exception as e:
                console.print(f"  [red]Error {race_id}: {e}[/red]")

        conn.commit()

    conn.close()
    console.print(f"[green]更新完了: {total_races}レース[/green]")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="レースデータ更新")
    parser.add_argument("--days", type=int, default=7, help="何日前まで遡るか (デフォルト: 7)")
    args = parser.parse_args()
    update_recent_results(args.days)
