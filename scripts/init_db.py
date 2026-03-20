"""DB初期化 + 過去データ一括取得"""

import sys
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

warnings.filterwarnings("ignore", category=UserWarning)

from datetime import datetime
from rich.console import Console

from config import VENUE_CODES
from db.schema import init_db, get_connection, insert_race, insert_result, insert_racer
from data.scraper import BoatraceScraper
from data.race_calendar import get_month_dates

console = Console()


def collect_past_data(start_year: int = 2023, end_year: int = 2024):
    """過去データを一括取得してDBに保存"""
    init_db()
    scraper = BoatraceScraper()
    conn = get_connection()

    total_races = 0
    total_results = 0

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            now = datetime.now()
            if year > now.year or (year == now.year and month > now.month):
                break

            console.print(f"\n[bold blue]== {year}年{month}月 ==[/bold blue]")

            dates = get_month_dates(year, month)

            for date in dates:
                race_ids = scraper.scrape_race_list(date)
                if not race_ids:
                    continue

                console.print(f"  {date}: {len(race_ids)}レース")

                for race_id in race_ids:
                    try:
                        data = scraper.scrape_race_result(race_id)
                        if not data.get("results"):
                            continue

                        race_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
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
                                total_results += 1

                        total_races += 1

                    except Exception as e:
                        console.print(f"  [red]Error {race_id}: {e}[/red]")
                        continue

                conn.commit()

    conn.close()
    console.print(f"\n[bold green]完了: {total_races}レース, {total_results}結果[/bold green]")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="過去データ一括取得")
    parser.add_argument("--start", type=int, default=2023, help="開始年")
    parser.add_argument("--end", type=int, default=2024, help="終了年")
    args = parser.parse_args()
    collect_past_data(args.start, args.end)
